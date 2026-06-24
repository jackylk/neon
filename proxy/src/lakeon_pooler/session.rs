use std::convert::Infallible;
use std::io;
use std::sync::Arc;

use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

use super::pool::{LakeonBackendPool, PoolKey};
use super::protocol::{BackendTransactionStatus, FrontendAction, FrontendMessage};
use crate::compute::MaybeRustlsStream;
use crate::control_plane::messages::MetricsAuxInfo;
use crate::metrics::{
    NumClientConnectionsGuard, NumConnectionRequestsGuard, NumDbConnectionsGuard,
};
use crate::pglb::copy_bidirectional::ErrorSource;
use crate::proxy::{BackendCancelInfo, StartupReplay};
use crate::stream::Stream;

const MAX_POOLED_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

pub(crate) struct LakeonPooledSession<S> {
    pub(crate) client: Stream<S>,
    pub(crate) backend: Option<PooledBackend>,
    pub(crate) pool: Arc<LakeonBackendPool>,
    pub(crate) pool_key: PoolKey,

    pub(crate) aux: MetricsAuxInfo,
    pub(crate) private_link_id: Option<SmolStr>,

    pub(crate) _cancel_on_shutdown: tokio::sync::oneshot::Sender<Infallible>,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) _conn: NumClientConnectionsGuard<'static>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> LakeonPooledSession<S> {
    pub(crate) async fn proxy_pass(mut self) -> Result<(), ErrorSource> {
        debug!(
            endpoint_id = %self.aux.endpoint_id,
            branch_id = %self.aux.branch_id,
            private_link_id = ?self.private_link_id,
            "performing Lakeon transaction-aware pooled proxy pass"
        );
        let mut backend = self.backend.take().expect("pooled backend is present");
        let result = proxy_transaction_aware(self.client, &mut backend.compute).await;
        if matches!(result, Ok(BackendTransactionStatus::Idle)) {
            self.pool.release_idle(self.pool_key, backend).await;
        }
        result.map(|_| ())
    }
}

pub(crate) struct PooledBackend {
    pub(crate) compute: MaybeRustlsStream,
    pub(crate) startup_replay: StartupReplay,
    pub(crate) cancel_info: BackendCancelInfo,
    pub(crate) aux: MetricsAuxInfo,
    pub(crate) _db_conn: NumDbConnectionsGuard<'static>,
}

async fn proxy_transaction_aware(
    mut client: impl AsyncRead + AsyncWrite + Unpin,
    mut compute: impl AsyncRead + AsyncWrite + Unpin,
) -> Result<BackendTransactionStatus, ErrorSource> {
    let mut last_status = BackendTransactionStatus::Idle;
    loop {
        let Some(frontend) = read_frame(&mut client).await.map_err(ErrorSource::Client)? else {
            return Ok(last_status);
        };

        let message = FrontendMessage::new(frontend.tag, &frontend.body);
        match message.action() {
            FrontendAction::Terminate => {
                if last_status == BackendTransactionStatus::Idle {
                    return Ok(last_status);
                }
                return Err(ErrorSource::Client(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "client terminated while transaction was active",
                )));
            }
            FrontendAction::UnsupportedSessionState => {
                write_pooler_error(
                    &mut client,
                    "Lakeon pooled endpoint does not support session state for this command; use the direct endpoint",
                )
                .await
                .map_err(ErrorSource::Client)?;
                continue;
            }
            FrontendAction::FlushesTransactionBoundary => {
                compute
                    .write_all(&frontend.to_wire())
                    .await
                    .map_err(ErrorSource::Compute)?;
                last_status = forward_backend_until_ready(&mut compute, &mut client).await?;
            }
            FrontendAction::SimpleQuery
            | FrontendAction::StartsTransaction
            | FrontendAction::EndsTransaction => {
                compute
                    .write_all(&frontend.to_wire())
                    .await
                    .map_err(ErrorSource::Compute)?;
                last_status = forward_backend_until_ready(&mut compute, &mut client).await?;
            }
        }
    }
}

async fn forward_backend_until_ready(
    compute: &mut (impl AsyncRead + Unpin),
    client: &mut (impl AsyncWrite + Unpin),
) -> Result<BackendTransactionStatus, ErrorSource> {
    loop {
        let Some(backend) = read_frame(compute).await.map_err(ErrorSource::Compute)? else {
            return Err(ErrorSource::Compute(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compute closed before ReadyForQuery",
            )));
        };
        let status = backend
            .ready_for_query_status()
            .map(BackendTransactionStatus::from_ready_for_query_status);
        client
            .write_all(&backend.to_wire())
            .await
            .map_err(ErrorSource::Client)?;

        if let Some(status) = status {
            return Ok(status);
        }
    }
}

struct PgFrame {
    tag: u8,
    body: Vec<u8>,
}

impl PgFrame {
    fn to_wire(&self) -> Vec<u8> {
        let len = u32::try_from(self.body.len() + 4).expect("frame length already checked");
        let mut frame = Vec::with_capacity(self.body.len() + 5);
        frame.push(self.tag);
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&self.body);
        frame
    }

    fn ready_for_query_status(&self) -> Option<u8> {
        (self.tag == b'Z' && self.body.len() == 1).then_some(self.body[0])
    }
}

async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Option<PgFrame>> {
    let mut header = [0_u8; 5];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let tag = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if !(4..=MAX_POOLED_MESSAGE_BYTES).contains(&len) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid postgres message length {len}"),
        ));
    }

    let body_len = usize::try_from(len - 4).expect("message length is bounded");
    let mut body = vec![0_u8; body_len];
    stream.read_exact(&mut body).await?;
    Ok(Some(PgFrame { tag, body }))
}

async fn write_pooler_error(
    client: &mut (impl AsyncWrite + Unpin),
    message: &str,
) -> io::Result<()> {
    let mut body = Vec::with_capacity(18 + message.len());
    body.extend_from_slice(b"SERROR\0");
    body.extend_from_slice(b"C0A000\0");
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    let error = PgFrame { tag: b'E', body };
    client.write_all(&error.to_wire()).await?;

    let ready = PgFrame {
        tag: b'Z',
        body: vec![b'I'],
    };
    client.write_all(&ready.to_wire()).await?;
    client.flush().await
}
