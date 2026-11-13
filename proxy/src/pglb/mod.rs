pub mod copy_bidirectional;
pub mod handshake;
pub mod inprocess;
pub mod passthrough;

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use smol_str::ToSmolStr;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

use crate::auth;
use crate::cancellation::{self, CancellationHandler};
use crate::config::{ProxyConfig, ProxyProtocolV2, TlsConfig};
use crate::context::RequestContext;
use crate::control_plane::messages::PoolerMode;
use crate::error::{ReportableError, UserFacingError};
use crate::lakeon_pooler::pool::{Checkout, CheckoutError, LakeonBackendPool};
use crate::lakeon_pooler::session::LakeonPooledSession;
use crate::lakeon_pooler::session::PooledBackend;
use crate::lakeon_pooler::session::write_pooler_error_to_pq_stream;
use crate::metrics::{Metrics, NumClientConnectionsGuard};
pub use crate::pglb::copy_bidirectional::ErrorSource;
use crate::pglb::handshake::{HandshakeData, HandshakeError, handshake};
use crate::pglb::passthrough::ProxyPassthrough;
use crate::protocol2::{ConnectHeader, ConnectionInfo, ConnectionInfoExtra, read_proxy_protocol};
use crate::proxy::{
    authenticate_client, connect_authenticated_client, register_backend_cancel_key,
    send_client_greeting,
};
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::Stream;
use crate::util::run_until_cancelled;

pub const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";

#[derive(Error, Debug)]
#[error("{ERR_INSECURE_CONNECTION}")]
pub struct TlsRequired;

impl ReportableError for TlsRequired {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        crate::error::ErrorKind::User
    }
}

impl UserFacingError for TlsRequired {}

pub async fn task_main(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
    cancellation_handler: Arc<CancellationHandler>,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    lakeon_data_proxy: bool,
    lakeon_pooler_max_backends_per_key: usize,
    lakeon_pooler_queue_timeout: Duration,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let connections = tokio_util::task::task_tracker::TaskTracker::new();
    let cancellations = tokio_util::task::task_tracker::TaskTracker::new();
    let lakeon_backend_pool = Arc::new(LakeonBackendPool::new(lakeon_pooler_max_backends_per_key));

    while let Some(accept_result) =
        run_until_cancelled(listener.accept(), &cancellation_token).await
    {
        let (socket, peer_addr) = accept_result?;
        // Admit before spawning or allocating per-client tasks. Closing the socket
        // immediately keeps rejected work bounded even during a connection flood.
        let Some(mut permit) = config.admission.try_acquire() else {
            Metrics::get().proxy.preauth_global_rejected_total.inc();
            continue;
        };

        let conn_gauge = Metrics::get()
            .proxy
            .client_connections
            .guard(crate::metrics::Protocol::Tcp);

        let session_id = uuid::Uuid::new_v4();
        let cancellation_handler = Arc::clone(&cancellation_handler);
        let cancellations = cancellations.clone();

        debug!(protocol = "tcp", %session_id, "accepted new TCP connection");
        let endpoint_rate_limiter2 = endpoint_rate_limiter.clone();
        let lakeon_backend_pool = lakeon_backend_pool.clone();

        connections.spawn(async move {
            let (socket, conn_info) = match config.proxy_protocol_v2 {
                ProxyProtocolV2::Required => {
                    match tokio::time::timeout_at(
                        permit.started
                            + config.handshake_timeout.min(config.authentication_timeout),
                        read_proxy_protocol(socket),
                    )
                    .await
                    {
                        Err(_) => {
                            Metrics::get().proxy.preauth_timeout_total.inc();
                            return;
                        }
                        Ok(result) => match result {
                            Err(e) => {
                                warn!("per-client task finished with an error: {e:#}");
                                return;
                            }
                            // our load balancers will not send any more data. let's just exit immediately
                            Ok((_socket, ConnectHeader::Local)) => {
                                debug!("healthcheck received");
                                return;
                            }
                            Ok((socket, ConnectHeader::Proxy(info))) => (socket, info),
                        },
                    }
                }
                // ignore the header - it cannot be confused for a postgres or http connection so will
                // error later.
                ProxyProtocolV2::Rejected => (
                    socket,
                    ConnectionInfo {
                        addr: peer_addr,
                        extra: None,
                    },
                ),
            };

            if !permit.assign_source(conn_info.addr.ip()) {
                Metrics::get().proxy.preauth_source_rejected_total.inc();
                return;
            }
            match socket.set_nodelay(true) {
                Ok(()) => {}
                Err(e) => {
                    error!(
                        "per-client task finished with an error: failed to set socket option: {e:#}"
                    );
                    return;
                }
            }

            let ctx = RequestContext::new(session_id, conn_info, crate::metrics::Protocol::Tcp);

            let res = handle_connection(
                config,
                auth_backend,
                &ctx,
                cancellation_handler,
                socket,
                ClientMode::Tcp,
                endpoint_rate_limiter2,
                conn_gauge,
                cancellations,
                lakeon_data_proxy,
                Some(lakeon_backend_pool.clone()),
                lakeon_pooler_queue_timeout,
                permit,
            )
            .instrument(ctx.span())
            .boxed()
            .await;

            match res {
                Err(e) => {
                    ctx.set_error_kind(e.get_error_kind());
                    warn!(parent: &ctx.span(), "per-client task finished with an error: {e:#}");
                }
                Ok(None) => {
                    ctx.set_success();
                }
                Ok(Some(p)) => {
                    ctx.set_success();
                    let _disconnect = ctx.log_connect();
                    match p.proxy_pass().await {
                        Ok(()) => {}
                        Err(ErrorSource::Client(e)) => {
                            warn!(
                                ?session_id,
                                "per-client task finished with an IO error from the client: {e:#}"
                            );
                        }
                        Err(ErrorSource::Compute(e)) => {
                            error!(
                                ?session_id,
                                "per-client task finished with an IO error from the compute: {e:#}"
                            );
                        }
                    }
                }
            }
        });
    }

    connections.close();
    cancellations.close();
    drop(listener);

    // Drain connections
    connections.wait().await;
    cancellations.wait().await;

    Ok(())
}

pub(crate) enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    pub fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    pub fn hostname<'a, S>(&'a self, s: &'a Stream<S>) -> Option<&'a str> {
        match self {
            ClientMode::Tcp => s.sni_hostname(),
            ClientMode::Websockets { hostname } => hostname.as_deref(),
        }
    }

    pub fn handshake_tls<'a>(&self, tls: Option<&'a TlsConfig>) -> Option<&'a TlsConfig> {
        match self {
            ClientMode::Tcp => tls,
            // TLS is None here if using websockets, because the connection is already encrypted.
            ClientMode::Websockets { .. } => None,
        }
    }
}

pub(crate) struct ProxyConnection<S> {
    inner: ProxyConnectionKind<S>,
    lease: Option<crate::security::SessionLease>,
}
pub(crate) enum ProxyConnectionKind<S> {
    Direct(ProxyPassthrough<S>),
    LakeonPooled(LakeonPooledSession<S>),
}
impl<S: AsyncRead + AsyncWrite + Unpin> ProxyConnectionKind<S> {
    async fn proxy_pass(self) -> Result<(), ErrorSource> {
        match self {
            Self::Direct(p) => p.proxy_pass().await,
            Self::LakeonPooled(p) => p.proxy_pass().await,
        }
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> ProxyConnection<S> {
    pub(crate) async fn proxy_pass(mut self) -> Result<(), ErrorSource> {
        let future = self.inner.proxy_pass();
        if let Some(lease) = self.lease.as_mut() {
            lease.protect(future).await.map_err(|_| {
                ErrorSource::Client(std::io::Error::other("shared connection lease expired"))
            })?
        } else {
            future.await
        }
    }
}

#[derive(Debug, Error)]
// almost all errors should be reported to the user, but there's a few cases where we cannot
// 1. Cancellation: we are not allowed to tell the client any cancellation statuses for security reasons
// 2. Handshake: handshake reports errors if it can, otherwise if the handshake fails due to protocol violation,
//    we cannot be sure the client even understands our error message
// 3. PrepareClient: The client disconnected, so we can't tell them anyway...
pub(crate) enum ClientRequestError {
    #[error("{0}")]
    Cancellation(#[from] cancellation::CancelError),
    #[error("{0}")]
    Security(#[from] crate::security::SecurityError),
    #[error("{0}")]
    Handshake(#[from] HandshakeError),
    #[error("{0}")]
    HandshakeTimeout(#[from] tokio::time::error::Elapsed),
    #[error("{0}")]
    PrepareClient(#[from] std::io::Error),
    #[error("{0}")]
    ReportedError(#[from] crate::stream::ReportedError),
}

impl ReportableError for ClientRequestError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ClientRequestError::Cancellation(e) => e.get_error_kind(),
            ClientRequestError::Security(e) => e.get_error_kind(),
            ClientRequestError::Handshake(e) => e.get_error_kind(),
            ClientRequestError::HandshakeTimeout(_) => crate::error::ErrorKind::RateLimit,
            ClientRequestError::ReportedError(e) => e.get_error_kind(),
            ClientRequestError::PrepareClient(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    auth_backend: &'static auth::Backend<'static, ()>,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    client: S,
    mode: ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    conn_gauge: NumClientConnectionsGuard<'static>,
    cancellations: tokio_util::task::task_tracker::TaskTracker,
    lakeon_data_proxy: bool,
    lakeon_backend_pool: Option<Arc<LakeonBackendPool>>,
    lakeon_pooler_queue_timeout: Duration,
    preauth_permit: crate::admission::Permit,
) -> Result<Option<ProxyConnection<S>>, ClientRequestError> {
    debug!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let metrics = &Metrics::get().proxy;
    let proto = ctx.protocol();
    let request_gauge = metrics.connection_requests.guard(proto);

    let tls = config.tls_config.load();
    let tls = tls.as_deref();

    let record_handshake_error = !ctx.has_private_peer_addr();
    let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Client);
    let do_handshake = handshake(ctx, client, mode.handshake_tls(tls), record_handshake_error);

    let authentication_deadline = preauth_permit.started + config.authentication_timeout;
    let handshake_deadline =
        authentication_deadline.min(tokio::time::Instant::now() + config.handshake_timeout);
    let (mut client, params) = match tokio::time::timeout_at(handshake_deadline, do_handshake)
        .await??
    {
        HandshakeData::Startup(client, params) => (client, params),
        HandshakeData::Cancel(cancel_key_data) => {
            // spawn a task to cancel the session, but don't wait for it
            cancellations.spawn({
                let cancellation_handler_clone = Arc::clone(&cancellation_handler);
                let ctx = ctx.clone();
                let cancel_span = tracing::span!(parent: None, tracing::Level::INFO, "cancel_session", session_id = ?ctx.session_id());
                cancel_span.follows_from(tracing::Span::current());
                async move {
                    cancellation_handler_clone
                        .cancel_session(
                            cancel_key_data,
                            ctx,
                            config.authentication_config.ip_allowlist_check_enabled,
                            config.authentication_config.is_vpc_acccess_proxy,
                            auth_backend.get_api(),
                        )
                        .await
                        .inspect_err(|e | debug!(error = ?e, "cancel_session failed")).ok();
                }.instrument(cancel_span)
            });

            return Ok(None);
        }
    };
    drop(pause);

    ctx.set_db_options(params.clone());

    let common_names = tls.map(|tls| &tls.common_names);

    let authenticated = tokio::time::timeout_at(
        authentication_deadline,
        authenticate_client(
            config,
            auth_backend,
            ctx,
            &mut client,
            &mode,
            endpoint_rate_limiter,
            common_names,
            &params,
        ),
    )
    .await??;
    // Authenticated sessions no longer consume the pre-authentication budget.
    drop(preauth_permit);

    let mut lease = if let Some(shared) = &config.shared_security {
        let context = match shared.context(authenticated.security_endpoint()).await {
            Ok(context) => context,
            Err(error) => return Err(client.throw_error(error, Some(ctx)).await.into()),
        };
        let admission = tokio::select! { biased;
            _=client.wait_for_admission_disconnect()=>return Ok(None),
            result=shared.admit(&context)=>result,
        };
        match admission {
            Ok(lease) => Some(lease),
            Err(error) => {
                let reason=match &error {
                    crate::security::SecurityError::Full=>"QUOTA_EXHAUSTED",
                    crate::security::SecurityError::Limited=>"RATE_LIMITED",
                    _=>"DEPENDENCY_UNAVAILABLE",
                };
                shared.audit_denied(ctx,authenticated.security_role(),Some(&context.database_id),Some(authenticated.security_endpoint()),reason);
                return Err(client.throw_error(error, Some(ctx)).await.into());
            }
        }
    } else {
        None
    };
    let finish = finish_authenticated_connection(
        config,
        ctx,
        cancellation_handler,
        client,
        params,
        authenticated,
        conn_gauge,
        request_gauge,
        lakeon_data_proxy,
        lakeon_backend_pool,
        lakeon_pooler_queue_timeout,
    );
    let inner = if let Some(guard) = lease.as_mut() {
        guard.protect(finish).await??
    } else {
        finish.await?
    };
    Ok(inner.map(|inner| ProxyConnection { inner, lease }))
}

#[allow(clippy::too_many_arguments)]
async fn finish_authenticated_connection<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    mut client: crate::stream::PqStream<Stream<S>>,
    params: crate::pqproto::StartupMessageParams,
    mut authenticated: crate::proxy::AuthenticatedClient<'_>,
    conn_gauge: NumClientConnectionsGuard<'static>,
    request_gauge: crate::metrics::NumConnectionRequestsGuard<'static>,
    lakeon_data_proxy: bool,
    lakeon_backend_pool: Option<Arc<LakeonBackendPool>>,
    lakeon_pooler_queue_timeout: Duration,
) -> Result<Option<ProxyConnectionKind<S>>, ClientRequestError> {
    if let Err(error) = authenticated.resolve_read_only(ctx).await {
        return Err(client.throw_error(error, Some(ctx)).await.into());
    }

    let lakeon_pool_key = if lakeon_data_proxy && authenticated.is_lakeon_pooler_endpoint() {
        authenticated.lakeon_pool_key(&params)
    } else {
        None
    };
    if let Some(pool_key) = &lakeon_pool_key {
        debug!(?pool_key, "Lakeon pooled endpoint selected");
    }

    let private_link_id = match ctx.extra() {
        Some(ConnectionInfoExtra::Aws { vpce_id }) => Some(vpce_id.clone()),
        Some(ConnectionInfoExtra::Azure { link_id }) => Some(link_id.to_smolstr()),
        None => None,
    };

    let mut checkout_guard: Option<crate::lakeon_pooler::pool::CheckoutGuard> = None;
    let lakeon_new_backend_reserved = if let (Some(pool_key), Some(pool)) =
        (&lakeon_pool_key, &lakeon_backend_pool)
    {
        match pool
            .checkout_or_reserve(pool_key, lakeon_pooler_queue_timeout)
            .await
        {
            Ok(Checkout::Idle(backend)) => {
                let checkout_guard = Some(crate::lakeon_pooler::pool::CheckoutGuard::new(
                    pool.as_ref().clone(),
                    pool_key.clone(),
                ));
                send_client_greeting(ctx, &config.greetings, &mut client);
                let (cancel_key_data, cancel_on_shutdown) = register_backend_cancel_key(
                    config,
                    ctx,
                    cancellation_handler,
                    backend.cancel_info.clone(),
                );
                backend
                    .startup_replay
                    .write_to(&mut client, cancel_key_data);
                let client = client.flush_and_into_inner().await?;
                return Ok(Some(ProxyConnectionKind::LakeonPooled(
                    LakeonPooledSession {
                        client_write_timeout: config.client_write_timeout,
                        client,
                        aux: backend.aux.clone(),
                        backend: Some(backend),
                        checkout_guard,
                        pool: pool.clone(),
                        pool_key: pool_key.clone(),
                        private_link_id,
                        _cancel_on_shutdown: cancel_on_shutdown,
                        _req: request_gauge,
                        _conn: conn_gauge,
                    },
                )));
            }
            Ok(Checkout::ReservedNew) => {
                checkout_guard = Some(crate::lakeon_pooler::pool::CheckoutGuard::new(
                    pool.as_ref().clone(),
                    pool_key.clone(),
                ));
                true
            }
            Err(CheckoutError::QueueTimeout) => {
                let stats = pool.stats().await;
                Metrics::get().proxy.lakeon_pooler_queue_timeout_total.inc();
                warn!(
                    ?pool_key,
                    pools = stats.pools,
                    checked_out = stats.checked_out,
                    idle = stats.idle,
                    total = stats.total,
                    queue_timeout_ms = lakeon_pooler_queue_timeout.as_millis(),
                    "Lakeon pooled endpoint timed out waiting for a backend connection"
                );
                send_client_greeting(ctx, &config.greetings, &mut client);
                write_pooler_error_to_pq_stream(
                        &mut client,
                        "Lakeon pooled endpoint waited for a backend connection but timed out; retry later or use the direct endpoint",
                    )
                    .await?;
                return Ok(None);
            }
            Err(CheckoutError::PoolFull) => {
                let stats = pool.stats().await;
                Metrics::get().proxy.lakeon_pooler_full_total.inc();
                warn!(
                    ?pool_key,
                    pools = stats.pools,
                    checked_out = stats.checked_out,
                    idle = stats.idle,
                    total = stats.total,
                    "Lakeon pooled endpoint reached per-proxy backend limit"
                );
                send_client_greeting(ctx, &config.greetings, &mut client);
                write_pooler_error_to_pq_stream(
                    &mut client,
                    "Lakeon pooled endpoint has reached the per-proxy backend connection limit; retry later or use the direct endpoint",
                )
                .await?;
                return Ok(None);
            }
        }
    } else {
        false
    };

    let connected = match connect_authenticated_client(
        config,
        ctx,
        cancellation_handler,
        &mut client,
        authenticated,
    )
    .await
    {
        Ok(connected) => connected,
        Err(e) => {
            if lakeon_new_backend_reserved
                && let (Some(pool_key), Some(pool)) = (&lakeon_pool_key, &lakeon_backend_pool)
            {
                pool.release_checked_out(pool_key).await;
                if let Some(guard) = checkout_guard.as_mut() {
                    guard.disarm();
                }
            }
            return Err(e.into());
        }
    };
    let node = connected.node;
    let cancel_on_shutdown = connected.cancel_on_shutdown;

    let client = client.flush_and_into_inner().await?;

    let pooled = lakeon_pool_key.is_some() && node.aux.pooler_mode == PoolerMode::ProxyPooled;
    if pooled {
        let pool = lakeon_backend_pool.expect("Lakeon pooled endpoint requires backend pool");
        let pool_key = lakeon_pool_key.expect("pooled endpoint has a pool key");
        let aux = node.aux.clone();
        Ok(Some(ProxyConnectionKind::LakeonPooled(
            LakeonPooledSession {
                client_write_timeout: config.client_write_timeout,
                client,
                checkout_guard: checkout_guard.take(),
                backend: Some(PooledBackend {
                    compute: node.stream.into_framed().into_inner(),
                    startup_replay: connected.startup_replay,
                    cancel_info: connected.cancel_info,
                    aux: aux.clone(),
                    _db_conn: node.guage,
                }),
                pool,
                pool_key,
                aux,
                private_link_id,

                _cancel_on_shutdown: cancel_on_shutdown,

                _req: request_gauge,
                _conn: conn_gauge,
            },
        )))
    } else {
        if lakeon_new_backend_reserved
            && let (Some(pool_key), Some(pool)) = (&lakeon_pool_key, &lakeon_backend_pool)
        {
            pool.release_checked_out(pool_key).await;
            if let Some(guard) = checkout_guard.as_mut() {
                guard.disarm();
            }
        }
        Ok(Some(ProxyConnectionKind::Direct(ProxyPassthrough {
            client_write_timeout: config.client_write_timeout,
            client,
            compute: node.stream.into_framed().into_inner(),

            aux: node.aux,
            private_link_id,

            _cancel_on_shutdown: cancel_on_shutdown,

            _req: request_gauge,
            _conn: conn_gauge,
            _db_conn: node.guage,
        })))
    }
}
