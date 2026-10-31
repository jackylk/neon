#[cfg(test)]
mod tests;

pub(crate) mod connect_auth;
pub(crate) mod connect_compute;
pub(crate) mod retry;
pub(crate) mod wake_compute;

use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::TryStreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use postgres_client::RawCancelToken;
use postgres_client::connect_raw::StartupStream;
use postgres_protocol::message::backend::Message;
use regex::Regex;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::cancellation::{CancelClosure, CancellationHandler};
use crate::compute::{ComputeConnection, PostgresError, RustlsStream};
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::lakeon_pooler::pool::PoolKey;
pub use crate::pglb::copy_bidirectional::{ErrorSource, copy_bidirectional_client_compute};
use crate::pglb::{ClientMode, ClientRequestError};
use crate::pqproto::{BeMessage, CancelKeyData, StartupMessageParams};
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::{PqStream, Stream};
use crate::types::EndpointCacheKey;
use crate::{auth, compute};

pub(crate) struct AuthenticatedClient<'a> {
    backend: auth::Backend<'a, auth::backend::ComputeUserInfo>,
    auth_info: compute::AuthInfo,
}

impl AuthenticatedClient<'_> {
    pub(crate) fn is_lakeon_pooler_endpoint(&self) -> bool {
        match &self.backend {
            auth::Backend::ControlPlane(_, user_info) => {
                user_info.endpoint.as_ref().ends_with("-pooler")
            }
            auth::Backend::Local(_) => false,
        }
    }

    pub(crate) fn lakeon_pool_key(&self, database: &str) -> Option<PoolKey> {
        match &self.backend {
            auth::Backend::ControlPlane(_, user_info) => Some(PoolKey::new(
                user_info.endpoint.normalize().as_ref(),
                "",
                database,
                user_info.user.as_ref(),
            )),
            auth::Backend::Local(_) => None,
        }
    }
}

pub(crate) struct ConnectedClient {
    pub(crate) node: ComputeConnection,
    pub(crate) cancel_on_shutdown: oneshot::Sender<Infallible>,
    pub(crate) startup_replay: StartupReplay,
    pub(crate) cancel_info: BackendCancelInfo,
}

#[derive(Clone)]
pub(crate) struct BackendCancelInfo {
    pub(crate) socket_addr: SocketAddr,
    pub(crate) cancel_token: RawCancelToken,
    pub(crate) hostname: String,
    pub(crate) user_info: auth::backend::ComputeUserInfo,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StartupReplay {
    frames: Vec<StartupReplayFrame>,
}

#[derive(Debug, Clone)]
enum StartupReplayFrame {
    Raw { tag: u8, body: Vec<u8> },
    BackendKeyData,
}

impl StartupReplay {
    fn push_raw(&mut self, tag: u8, body: Vec<u8>) {
        self.frames.push(StartupReplayFrame::Raw { tag, body });
    }

    fn push_backend_key_data(&mut self) {
        self.frames.push(StartupReplayFrame::BackendKeyData);
    }

    pub(crate) fn write_to(
        &self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
        cancel_key_data: CancelKeyData,
    ) {
        for frame in &self.frames {
            match frame {
                StartupReplayFrame::Raw { tag, body } => {
                    client.write_raw(body.len(), *tag, |buf| buf.extend_from_slice(body));
                }
                StartupReplayFrame::BackendKeyData => {
                    client.write_message(BeMessage::BackendKeyData(cancel_key_data));
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn authenticate_client<'a, S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &'static ProxyConfig,
    auth_backend: &'a auth::Backend<'static, ()>,
    ctx: &RequestContext,
    client: &mut PqStream<Stream<S>>,
    mode: &ClientMode,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    common_names: Option<&HashSet<String>>,
    params: &StartupMessageParams,
) -> Result<AuthenticatedClient<'a>, ClientRequestError> {
    let hostname = mode.hostname(client.get_ref());
    // Extract credentials which we're going to use for auth.
    let result = auth_backend
        .as_ref()
        .map(|()| auth::ComputeUserInfoMaybeEndpoint::parse(ctx, params, hostname, common_names))
        .transpose();

    let user_info = match result {
        Ok(user_info) => user_info,
        Err(e) => Err(client.throw_error(e, Some(ctx)).await)?,
    };

    let user = user_info.get_user().to_owned();
    let user_info = match user_info
        .authenticate(
            ctx,
            client,
            mode.allow_cleartext(),
            &config.authentication_config,
            endpoint_rate_limiter,
        )
        .await
    {
        Ok(auth_result) => auth_result,
        Err(e) => {
            let db = params.get("database");
            let app = params.get("application_name");
            let params_span = tracing::info_span!("", ?user, ?db, ?app);

            return Err(client
                .throw_error(e, Some(ctx))
                .instrument(params_span)
                .await)?;
        }
    };

    let (cplane, creds) = match user_info {
        auth::Backend::ControlPlane(cplane, creds) => (cplane, creds),
        auth::Backend::Local(_) => unreachable!("local proxy does not run tcp proxy service"),
    };
    let params_compat = creds.info.options.get(NeonOptions::PARAMS_COMPAT).is_some();
    let mut auth_info = compute::AuthInfo::with_auth_keys(creds.keys);
    auth_info.set_startup_params(params, params_compat);

    let backend = auth::Backend::ControlPlane(cplane, creds.info);

    Ok(AuthenticatedClient { backend, auth_info })
}

pub(crate) async fn connect_authenticated_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    client: &mut PqStream<Stream<S>>,
    authenticated: AuthenticatedClient<'_>,
) -> Result<ConnectedClient, ClientRequestError> {
    let res = connect_auth::connect_to_compute_and_auth(
        ctx,
        config,
        &authenticated.backend,
        authenticated.auth_info,
        connect_compute::TlsNegotiation::Postgres,
    )
    .await;

    let mut node = match res {
        Ok(node) => node,
        Err(e) => Err(client.throw_error(e, Some(ctx)).await)?,
    };

    send_client_greeting(ctx, &config.greetings, client);

    let auth::Backend::ControlPlane(_, user_info) = authenticated.backend else {
        unreachable!("ensured above");
    };

    let session = cancellation_handler.get_key();
    let cancel_key_data = *session.key();

    let (process_id, secret_key, startup_replay) =
        forward_compute_params_to_client(ctx, cancel_key_data, client, &mut node.stream).await?;
    let cancel_info = BackendCancelInfo {
        socket_addr: node.socket_addr,
        cancel_token: RawCancelToken {
            ssl_mode: node.ssl_mode,
            process_id,
            secret_key,
        },
        hostname: node.hostname.to_string(),
        user_info,
    };
    let cancel_on_shutdown =
        spawn_cancel_key_maintenance(config, ctx, session, cancel_info.clone());

    Ok(ConnectedClient {
        node,
        cancel_on_shutdown,
        startup_replay,
        cancel_info,
    })
}

pub(crate) fn register_backend_cancel_key(
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    cancellation_handler: Arc<CancellationHandler>,
    cancel_info: BackendCancelInfo,
) -> (CancelKeyData, oneshot::Sender<Infallible>) {
    let session = cancellation_handler.get_key();
    let cancel_key_data = *session.key();
    let cancel_on_shutdown = spawn_cancel_key_maintenance(config, ctx, session, cancel_info);
    (cancel_key_data, cancel_on_shutdown)
}

fn spawn_cancel_key_maintenance(
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    session: crate::cancellation::Session,
    cancel_info: BackendCancelInfo,
) -> oneshot::Sender<Infallible> {
    let session_id = ctx.session_id();
    let (cancel_on_shutdown, cancel) = oneshot::channel();
    tokio::spawn(async move {
        session
            .maintain_cancel_key(
                session_id,
                cancel,
                &CancelClosure {
                    socket_addr: cancel_info.socket_addr,
                    cancel_token: cancel_info.cancel_token,
                    hostname: cancel_info.hostname,
                    user_info: cancel_info.user_info,
                },
                &config.connect_to_compute,
            )
            .await;
    });
    cancel_on_shutdown
}

/// Greet the client with any useful information.
pub(crate) fn send_client_greeting(
    ctx: &RequestContext,
    greetings: &String,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) {
    // Expose session_id to clients if we have a greeting message.
    if !greetings.is_empty() {
        let session_msg = format!("{}, session_id: {}", greetings, ctx.session_id());
        client.write_message(BeMessage::NoticeResponse(session_msg.as_str()));
    }

    // Forward recorded latencies for probing requests
    if let Some(testodrome_id) = ctx.get_testodrome_id() {
        client.write_message(BeMessage::ParameterStatus {
            name: "neon.testodrome_id".as_bytes(),
            value: testodrome_id.as_bytes(),
        });

        let latency_measured = ctx.get_proxy_latency();

        client.write_message(BeMessage::ParameterStatus {
            name: "neon.cplane_latency".as_bytes(),
            value: latency_measured.cplane.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "neon.client_latency".as_bytes(),
            value: latency_measured.client.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "neon.compute_latency".as_bytes(),
            value: latency_measured.compute.as_micros().to_string().as_bytes(),
        });

        client.write_message(BeMessage::ParameterStatus {
            name: "neon.retry_latency".as_bytes(),
            value: latency_measured.retry.as_micros().to_string().as_bytes(),
        });
    }
}

pub(crate) async fn forward_compute_params_to_client(
    ctx: &RequestContext,
    cancel_key_data: CancelKeyData,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    compute: &mut StartupStream<TcpStream, RustlsStream>,
) -> Result<(i32, i32, StartupReplay), ClientRequestError> {
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut startup_replay = StartupReplay::default();

    let err = loop {
        // if the client buffer is too large, let's write out some bytes now to save some space
        client.write_if_full().await?;

        let msg = match compute.try_next().await {
            Ok(msg) => msg,
            Err(e) => break postgres_client::Error::io(e),
        };

        match msg {
            // Send our cancellation key data instead.
            Some(Message::BackendKeyData(body)) => {
                client.write_message(BeMessage::BackendKeyData(cancel_key_data));
                startup_replay.push_backend_key_data();
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            // Forward all postgres connection params to the client.
            Some(Message::ParameterStatus(body)) => {
                if let Ok(name) = body.name()
                    && let Ok(value) = body.value()
                {
                    client.write_message(BeMessage::ParameterStatus {
                        name: name.as_bytes(),
                        value: value.as_bytes(),
                    });
                    let mut replay_body = Vec::with_capacity(name.len() + value.len() + 2);
                    replay_body.extend_from_slice(name.as_bytes());
                    replay_body.push(0);
                    replay_body.extend_from_slice(value.as_bytes());
                    replay_body.push(0);
                    startup_replay.push_raw(b'S', replay_body);
                }
            }
            // Forward all notices to the client.
            Some(Message::NoticeResponse(notice)) => {
                client.write_raw(notice.as_bytes().len(), b'N', |buf| {
                    buf.extend_from_slice(notice.as_bytes());
                });
                startup_replay.push_raw(b'N', notice.as_bytes().to_vec());
            }
            Some(Message::ReadyForQuery(_)) => {
                client.write_message(BeMessage::ReadyForQuery);
                startup_replay.push_raw(b'Z', vec![b'I']);
                return Ok((process_id, secret_key, startup_replay));
            }
            Some(Message::ErrorResponse(body)) => break postgres_client::Error::db(body),
            Some(_) => break postgres_client::Error::unexpected_message(),
            None => break postgres_client::Error::closed(),
        }
    };

    Err(client
        .throw_error(PostgresError::Postgres(err), Some(ctx))
        .await)?
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct NeonOptions(Vec<(SmolStr, SmolStr)>);

impl NeonOptions {
    // proxy options:

    /// `PARAMS_COMPAT` allows opting in to forwarding all startup parameters from client to compute.
    pub const PARAMS_COMPAT: &'static str = "proxy_params_compat";

    // cplane options:

    /// `LSN` allows provisioning an ephemeral compute with time-travel to the provided LSN.
    const LSN: &'static str = "lsn";

    /// `TIMESTAMP` allows provisioning an ephemeral compute with time-travel to the provided timestamp.
    const TIMESTAMP: &'static str = "timestamp";

    /// `ENDPOINT_TYPE` allows configuring an ephemeral compute to be read_only or read_write.
    const ENDPOINT_TYPE: &'static str = "endpoint_type";

    pub(crate) fn parse_params(params: &StartupMessageParams) -> Self {
        params
            .options_raw()
            .map(Self::parse_from_iter)
            .unwrap_or_default()
    }

    pub(crate) fn parse_options_raw(options: &str) -> Self {
        Self::parse_from_iter(StartupMessageParams::parse_options_raw(options))
    }

    pub(crate) fn get(&self, key: &str) -> Option<SmolStr> {
        self.0
            .iter()
            .find_map(|(k, v)| (k == key).then_some(v))
            .cloned()
    }

    pub(crate) fn is_ephemeral(&self) -> bool {
        self.0.iter().any(|(k, _)| match &**k {
            // This is not a cplane option, we know it does not create ephemeral computes.
            Self::PARAMS_COMPAT => false,
            Self::LSN => true,
            Self::TIMESTAMP => true,
            Self::ENDPOINT_TYPE => true,
            // err on the side of caution. any cplane options we don't know about
            // might lead to ephemeral computes.
            _ => true,
        })
    }

    fn parse_from_iter<'a>(options: impl Iterator<Item = &'a str>) -> Self {
        let mut options = options
            .filter_map(neon_option)
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec();
        options.sort();
        Self(options)
    }

    pub(crate) fn get_cache_key(&self, prefix: &str) -> EndpointCacheKey {
        // prefix + format!(" {k}:{v}")
        // kinda jank because SmolStr is immutable
        std::iter::once(prefix)
            .chain(self.0.iter().flat_map(|(k, v)| [" ", &**k, ":", &**v]))
            .collect::<SmolStr>()
            .into()
    }

    /// <https://swagger.io/docs/specification/serialization/> DeepObject format
    /// `paramName[prop1]=value1&paramName[prop2]=value2&...`
    pub(crate) fn to_deep_object(&self) -> Vec<(SmolStr, SmolStr)> {
        self.0
            .iter()
            .map(|(k, v)| (format_smolstr!("options[{}]", k), v.clone()))
            .collect()
    }
}

pub(crate) fn neon_option(bytes: &str) -> Option<(&str, &str)> {
    static RE: OnceCell<Regex> = OnceCell::new();
    let re = RE.get_or_init(|| Regex::new(r"^neon_(\w+):(.+)").expect("regex should be correct"));

    let cap = re.captures(bytes)?;
    let (_, [k, v]) = cap.extract();
    Some((k, v))
}
