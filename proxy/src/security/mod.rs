//! Shared security budgets. The cancellation Redis instance is reused, not its keys.
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::Instant;

const PREFIX: &str = "lakeon:security:v1:{proxy-security}:";
const SCRIPT: &str = include_str!("shared_budget.lua");

#[derive(Debug, thiserror::Error)]
pub(crate) enum SecurityError {
    #[error("Shared security service is unavailable; please retry later")]
    Unavailable,
    #[error("Authentication or connection rate limit exceeded; please retry later")]
    Limited,
    #[error("Connection quota or waiting queue exhausted; please retry later")]
    Full,
    #[error("Shared connection lease expired")]
    LeaseLost,
}
impl crate::error::ReportableError for SecurityError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::Limited | Self::Full => crate::error::ErrorKind::RateLimit,
            _ => crate::error::ErrorKind::Service,
        }
    }
}
impl crate::error::UserFacingError for SecurityError {}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub max_connections: u32,
    pub connection_rate: f64,
    pub connection_burst: u32,
    pub max_queued: u32,
    pub queue_timeout_ms: u64,
    pub wake_rate: f64,
    pub wake_burst: u32,
}
impl Policy {
    fn valid(&self) -> bool {
        (1..=1_000_000).contains(&self.max_connections)
            && self.connection_rate.is_finite()
            && self.connection_rate > 0.0
            && self.connection_rate <= 1_000_000.0
            && (1..=1_000_000).contains(&self.connection_burst)
            && self.max_queued <= 10_000
            && (100..=60_000).contains(&self.queue_timeout_ms)
            && self.wake_rate.is_finite()
            && self.wake_rate > 0.0
            && self.wake_rate <= 1_000_000.0
            && (1..=1_000_000).contains(&self.wake_burst)
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecurityContext {
    pub tenant_id: String,
    pub database_id: String,
    pub tenant: Policy,
    pub database: Policy,
}
#[derive(Deserialize)]
struct Reply {
    status: String,
    epoch: String,
    remaining_ms: Option<u64>,
}

pub(crate) struct SharedSecurity {
    redis: redis::Client,
    connection: Mutex<Option<MultiplexedConnection>>,
    endpoint: crate::http::Endpoint,
    token: Arc<str>,
    lease_ms: u64,
    recovery_ms: u64,
    auth_rate: u32,
    auth_burst: u32,
}
impl SharedSecurity {
    pub(crate) fn new(
        url: &str,
        endpoint: crate::http::Endpoint,
        token: Arc<str>,
        lease_ms: u64,
        auth_rate: u32,
        auth_burst: u32,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(
            (1000..=60000).contains(&lease_ms),
            "shared security lease must be 1s through 60s"
        );
        anyhow::ensure!(
            auth_rate > 0 && auth_burst > 0,
            "shared authentication budgets must be positive"
        );
        let redis = redis::Client::open(url)
            .map_err(|_| anyhow::anyhow!("invalid shared Redis configuration"))?;
        Ok(Arc::new(Self {
            redis,
            connection: Mutex::new(None),
            endpoint,
            token,
            lease_ms,
            recovery_ms: if cfg!(debug_assertions) {
                lease_ms
            } else {
                60000
            },
            auth_rate,
            auth_burst,
        }))
    }
    /// Readiness is withheld until a new Redis epoch has aged past every
    /// possible production lease. Debug builds shorten this for local E2E.
    pub(crate) async fn prepare(&self) -> Result<(), SecurityError> {
        let deadline = Instant::now() + Duration::from_millis(self.recovery_ms + 5000);
        loop {
            let reply = self.command("epoch", None, "startup", &[]).await?;
            if reply.status == "ok" {
                return Ok(());
            }
            if reply.status != "recovering" || Instant::now() >= deadline {
                return Err(SecurityError::Unavailable);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn key(kind: &str, id: &str) -> String {
        format!("{PREFIX}{kind}:{:x}", Sha256::digest(id.as_bytes()))
    }
    async fn command(
        &self,
        action: &str,
        epoch: Option<&str>,
        member: &str,
        scopes: &[Value],
    ) -> Result<Reply, SecurityError> {
        let payload = json!({"action":action,"epoch":uuid::Uuid::new_v4().to_string(),
            "expected_epoch":epoch,"member":member,"lease_ms":self.lease_ms,
            "recovery_ms":self.recovery_ms,"scopes":scopes});
        let request = async {
            let mut connection = {
                let mut slot = self.connection.lock().await;
                if slot.is_none() {
                    *slot = Some(self.redis.get_multiplexed_tokio_connection().await?);
                }
                slot.as_ref().expect("initialized Redis connection").clone()
            };
            redis::cmd("EVAL")
                .arg(SCRIPT)
                .arg(1)
                .arg(format!("{PREFIX}epoch"))
                .arg(payload.to_string())
                .query_async::<String>(&mut connection)
                .await
        };
        match tokio::time::timeout(Duration::from_millis(500), request).await {
            Ok(Ok(raw)) => serde_json::from_str(&raw).map_err(|_| SecurityError::Unavailable),
            _ => {
                crate::metrics::Metrics::get()
                    .proxy
                    .shared_security_dependency_errors_total
                    .inc();
                *self.connection.lock().await = None;
                Err(SecurityError::Unavailable)
            }
        }
    }
    fn accepted(reply: &Reply) -> Result<(), SecurityError> {
        if matches!(reply.status.as_str(), "limited" | "full") {
            crate::metrics::Metrics::get()
                .proxy
                .shared_security_rejections_total
                .inc();
        }
        match reply.status.as_str() {
            "ok" => Ok(()),
            "limited" => Err(SecurityError::Limited),
            "full" => Err(SecurityError::Full),
            "lost" | "epoch_changed" => Err(SecurityError::LeaseLost),
            _ => Err(SecurityError::Unavailable),
        }
    }
    pub(crate) async fn pre_auth(
        &self,
        ip: IpAddr,
        role: &str,
        endpoint: &str,
    ) -> Result<(), SecurityError> {
        let r = self.auth_rate;
        let b = self.auth_burst;
        let scopes = [
            json!({"key":Self::key("auth-global","global"),"rate":r as u64*100,"burst":b as u64*100}),
            json!({"key":Self::key("auth-ip",&ip.to_string()),"rate":r as u64*2,"burst":b as u64*2}),
            json!({"key":Self::key("auth-role-ip",&format!("{ip}\0{endpoint}\0{role}")),"rate":r,"burst":b}),
        ];
        Self::accepted(&self.command("rate", None, "auth", &scopes).await?)
    }
    pub(crate) async fn context(&self, endpoint: &str) -> Result<SecurityContext, SecurityError> {
        let result = async {
            let request = self
                .endpoint
                .get_path("security_context")
                .header("Authorization", format!("Bearer {}", self.token))
                .query(&[("endpointish", endpoint)])
                .build()?;
            let mut response = self.endpoint.execute(request).await?;
            anyhow::ensure!(response.status().is_success(), "security context rejected");
            anyhow::ensure!(
                response.content_length().is_none_or(|n| n <= 16384),
                "security context too large"
            );
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                anyhow::ensure!(
                    bytes.len() + chunk.len() <= 16384,
                    "security context too large"
                );
                bytes.extend_from_slice(&chunk);
            }
            let c: SecurityContext = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                !c.tenant_id.is_empty()
                    && c.tenant_id.len() <= 128
                    && !c.database_id.is_empty()
                    && c.database_id.len() <= 128
                    && c.tenant.valid()
                    && c.database.valid(),
                "invalid security context"
            );
            Ok::<_, anyhow::Error>(c)
        };
        tokio::time::timeout(Duration::from_secs(2), result)
            .await
            .map_err(|_| SecurityError::Unavailable)?
            .map_err(|_| SecurityError::Unavailable)
    }
    pub(crate) async fn wake(&self, endpoint: &str) -> Result<(), SecurityError> {
        let c = self.context(endpoint).await?;
        let scopes = [
            json!({"key":Self::key("wake-tenant",&c.tenant_id),"rate":c.tenant.wake_rate,"burst":c.tenant.wake_burst}),
            json!({"key":Self::key("wake-db",&c.database_id),"rate":c.database.wake_rate,"burst":c.database.wake_burst}),
        ];
        Self::accepted(&self.command("rate", None, "wake", &scopes).await?)
    }
    async fn acquire(
        self: &Arc<Self>,
        member: &str,
        scopes: Vec<Value>,
    ) -> Result<SessionLease, SecurityError> {
        let started = Instant::now();
        let reply = self.command("acquire", None, member, &scopes).await?;
        Self::accepted(&reply)?;
        let remaining = reply.remaining_ms.ok_or(SecurityError::Unavailable)?;
        let deadline = started + Duration::from_millis(remaining.saturating_sub(self.lease_ms / 5));
        if deadline <= Instant::now() {
            return Err(SecurityError::LeaseLost);
        }
        Ok(SessionLease {
            shared: self.clone(),
            member: member.to_owned(),
            epoch: reply.epoch,
            scopes,
            deadline,
        })
    }
    pub(crate) async fn admit(
        self: &Arc<Self>,
        c: &SecurityContext,
    ) -> Result<SessionLease, SecurityError> {
        let rate = [
            json!({"key":Self::key("connect-tenant",&c.tenant_id),"rate":c.tenant.connection_rate,"burst":c.tenant.connection_burst}),
            json!({"key":Self::key("connect-db",&c.database_id),"rate":c.database.connection_rate,"burst":c.database.connection_burst}),
        ];
        Self::accepted(&self.command("rate", None, "connect", &rate).await?)?;
        let member = uuid::Uuid::new_v4().to_string();
        let scopes = vec![
            json!({"key":Self::key("active-tenant",&c.tenant_id),"limit":c.tenant.max_connections}),
            json!({"key":Self::key("active-db",&c.database_id),"limit":c.database.max_connections}),
        ];
        match self.acquire(&member, scopes.clone()).await {
            Ok(lease) => return Ok(lease),
            Err(SecurityError::Full) => {}
            Err(e) => return Err(e),
        }
        if c.tenant.max_queued == 0 || c.database.max_queued == 0 {
            return Err(SecurityError::Full);
        }
        let waiting = vec![
            json!({"key":Self::key("queue-tenant",&c.tenant_id),"limit":c.tenant.max_queued}),
            json!({"key":Self::key("queue-db",&c.database_id),"limit":c.database.max_queued}),
        ];
        let mut queued = self.acquire(&format!("{member}:queue"), waiting).await?;
        crate::metrics::Metrics::get()
            .proxy
            .shared_security_queue_entries_total
            .inc();
        let deadline = Instant::now()
            + Duration::from_millis(c.tenant.queue_timeout_ms.min(c.database.queue_timeout_ms));
        let wait = async {
            loop {
                match self.acquire(&member, scopes.clone()).await {
                    Err(SecurityError::Full) => {}
                    result => return result,
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::time::timeout_at(deadline, queued.protect(wait))
            .await
            .map_err(|_| {
                crate::metrics::Metrics::get()
                    .proxy
                    .shared_security_queue_timeouts_total
                    .inc();
                SecurityError::Full
            })??
    }
}

pub(crate) struct SessionLease {
    shared: Arc<SharedSecurity>,
    member: String,
    epoch: String,
    scopes: Vec<Value>,
    deadline: Instant,
}
impl SessionLease {
    pub(crate) async fn protect<F: Future>(
        &mut self,
        future: F,
    ) -> Result<F::Output, SecurityError> {
        tokio::pin!(future);
        let mut ticks = tokio::time::interval(Duration::from_millis(self.shared.lease_ms / 3));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticks.tick().await;
        loop {
            tokio::select! { biased;
                _=tokio::time::sleep_until(self.deadline)=>{
                    crate::metrics::Metrics::get().proxy.shared_security_lease_expired_total.inc();
                    return Err(SecurityError::LeaseLost);
                },
                result=&mut future=>return Ok(result),
                _=ticks.tick()=>{
                    let start=Instant::now();
                    let result=tokio::time::timeout_at(self.deadline,self.shared.command("renew",Some(&self.epoch),&self.member,&self.scopes)).await;
                    match result {
                        Ok(Ok(reply))=>{
                            SharedSecurity::accepted(&reply)?;
                            self.deadline=start+Duration::from_millis(reply.remaining_ms.ok_or(SecurityError::Unavailable)?.saturating_sub(self.shared.lease_ms/5));
                        }
                        Err(_)=>return Err(SecurityError::LeaseLost),
                        Ok(Err(_))=>{} // Existing credits remain valid only until the original deadline.
                    }
                }
            }
        }
    }
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        let shared = self.shared.clone();
        let epoch = self.epoch.clone();
        let member = self.member.clone();
        let scopes = self.scopes.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                drop(
                    shared
                        .command("release", Some(&epoch), &member, &scopes)
                        .await,
                );
            });
        }
    }
}
