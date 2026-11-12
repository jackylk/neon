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
use crate::stream::{PqStream, Stream};

const MAX_POOLED_MESSAGE_BYTES: u32 = 16 * 1024 * 1024;

pub(crate) struct LakeonPooledSession<S> {
    pub(crate) client: Stream<S>,
    pub(crate) client_write_timeout: Option<std::time::Duration>,
    pub(crate) backend: Option<PooledBackend>,
    pub(crate) pool: Arc<LakeonBackendPool>,
    pub(crate) pool_key: PoolKey,

    pub(crate) aux: MetricsAuxInfo,
    pub(crate) private_link_id: Option<SmolStr>,

    pub(crate) _cancel_on_shutdown: tokio::sync::oneshot::Sender<Infallible>,

    pub(crate) _req: NumConnectionRequestsGuard<'static>,
    pub(crate) checkout_guard: Option<super::pool::CheckoutGuard>,
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
        let mut checkout_guard = self.checkout_guard.take();
        let mut backend = self.backend.take().expect("pooled backend is present");
        let result = proxy_transaction_aware(
            crate::client_write_guard::ClientWriteGuard::new(self.client, self.client_write_timeout),
            &mut backend.compute,
        ).await;
        if matches!(result, Ok(BackendTransactionStatus::Idle))
            && reset_backend(&mut backend.compute).await.is_ok()
        {
            self.pool.release_idle(self.pool_key, backend).await;
        } else {
            self.pool.release_checked_out(&self.pool_key).await;
        }
        if let Some(guard) = checkout_guard.as_mut() {
            guard.disarm();
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

async fn reset_backend(compute: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> io::Result<()> {
    compute
        .write_all(
            &PgFrame {
                tag: b'Q',
                body: b"DISCARD ALL\0".to_vec(),
            }
            .to_wire(),
        )
        .await?;
    compute.flush().await?;
    let mut failed = false;
    loop {
        let frame = read_frame(compute).await?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "backend closed during reset")
        })?;
        failed |= frame.tag == b'E';
        if let Some(status) = frame.ready_for_query_status() {
            return if !failed && status == b'I' {
                Ok(())
            } else {
                Err(io::Error::other("backend reset failed"))
            };
        }
    }
}

async fn proxy_transaction_aware(
    mut client: impl AsyncRead + AsyncWrite + Unpin,
    mut compute: impl AsyncRead + AsyncWrite + Unpin,
) -> Result<BackendTransactionStatus, ErrorSource> {
    let mut last_status = BackendTransactionStatus::Idle;
    let mut extended_failed = false;
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
                if frontend.tag == b'P' {
                    // A locally rejected Parse never reached PostgreSQL. Closing prevents
                    // pipelined Bind/Execute from running a previous unnamed statement.
                    return Err(ErrorSource::Client(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "unsupported pooled Parse; use the direct endpoint",
                    )));
                }
                continue;
            }
            FrontendAction::ExtendedProtocol => {
                compute
                    .write_all(&frontend.to_wire())
                    .await
                    .map_err(ErrorSource::Compute)?;
                if frontend.tag != b'H' && !extended_failed {
                    // Parsing/binding can require a reply before the client sends Sync. Flush is
                    // not a transaction boundary; never wait for ReadyForQuery at these stages.
                    last_status = BackendTransactionStatus::Unknown;
                    compute
                        .write_all(
                            &PgFrame {
                                tag: b'H',
                                body: vec![],
                            }
                            .to_wire(),
                        )
                        .await
                        .map_err(ErrorSource::Compute)?;
                    compute.flush().await.map_err(ErrorSource::Compute)?;
                    extended_failed =
                        forward_extended_reply(&mut compute, &mut client, frontend.tag).await?;
                }
            }
            FrontendAction::FlushesTransactionBoundary => {
                extended_failed = false;
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

async fn forward_extended_reply(
    compute: &mut (impl AsyncRead + Unpin),
    client: &mut (impl AsyncWrite + Unpin),
    request: u8,
) -> Result<bool, ErrorSource> {
    loop {
        let Some(frame) = read_frame(compute).await.map_err(ErrorSource::Compute)? else {
            return Err(ErrorSource::Compute(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compute closed during extended query",
            )));
        };
        let complete = frame.tag == b'E'
            || match request {
                b'P' => frame.tag == b'1',
                b'B' => frame.tag == b'2',
                b'D' => matches!(frame.tag, b'T' | b'n'),
                b'E' => matches!(frame.tag, b'C' | b's' | b'I'),
                b'C' => frame.tag == b'3',
                _ => false,
            };
        client
            .write_all(&frame.to_wire())
            .await
            .map_err(ErrorSource::Client)?;
        client.flush().await.map_err(ErrorSource::Client)?;
        if complete {
            return Ok(frame.tag == b'E');
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

pub(crate) async fn write_pooler_error(
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

pub(crate) async fn write_pooler_error_to_pq_stream<S: AsyncWrite + Unpin>(
    client: &mut PqStream<S>,
    message: &str,
) -> io::Result<()> {
    client.write_raw(18 + message.len(), b'E', |buf| {
        buf.extend_from_slice(b"SERROR\0");
        buf.extend_from_slice(b"C0A000\0");
        buf.push(b'M');
        buf.extend_from_slice(message.as_bytes());
        buf.push(0);
        buf.push(0);
    });
    client.write_raw(1, b'Z', |buf| buf.push(b'I'));
    client.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn rejected_parse_never_executes_previous_statement() {
        let (mut client, proxy_client) = tokio::io::duplex(4096);
        let (proxy_compute, mut server) = tokio::io::duplex(4096);
        let task = tokio::spawn(proxy_transaction_aware(proxy_client, proxy_compute));
        for (tag, body) in [
            (b'P', b"\0SET timezone='UTC'\0\0\0".to_vec()),
            (b'B', vec![]),
            (b'E', vec![]),
            (b'S', vec![]),
        ] {
            client
                .write_all(&PgFrame { tag, body }.to_wire())
                .await
                .unwrap();
        }
        assert!(
            timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(read_frame(&mut server).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn parse_flush_is_answered_before_frontend_sync() {
        let (mut client, proxy_client) = tokio::io::duplex(4096);
        let (proxy_compute, mut server) = tokio::io::duplex(4096);
        let task = tokio::spawn(proxy_transaction_aware(proxy_client, proxy_compute));
        let backend = tokio::spawn(async move {
            assert_eq!(read_frame(&mut server).await.unwrap().unwrap().tag, b'P');
            assert_eq!(read_frame(&mut server).await.unwrap().unwrap().tag, b'H');
            server
                .write_all(
                    &PgFrame {
                        tag: b'1',
                        body: vec![],
                    }
                    .to_wire(),
                )
                .await
                .unwrap();
            loop {
                let f = read_frame(&mut server).await.unwrap().unwrap();
                if f.tag == b'S' {
                    server
                        .write_all(
                            &PgFrame {
                                tag: b'Z',
                                body: vec![b'I'],
                            }
                            .to_wire(),
                        )
                        .await
                        .unwrap();
                    break;
                }
                assert_eq!(f.tag, b'H');
            }
        });
        client
            .write_all(
                &PgFrame {
                    tag: b'P',
                    body: b"\0SELECT $1::int\0\0\0".to_vec(),
                }
                .to_wire(),
            )
            .await
            .unwrap();
        client
            .write_all(
                &PgFrame {
                    tag: b'H',
                    body: vec![],
                }
                .to_wire(),
            )
            .await
            .unwrap();
        let reply = timeout(Duration::from_millis(250), read_frame(&mut client))
            .await
            .expect("Parse/Flush deadlocked waiting for frontend Sync")
            .unwrap()
            .unwrap();
        assert_eq!(reply.tag, b'1');
        client
            .write_all(
                &PgFrame {
                    tag: b'S',
                    body: vec![],
                }
                .to_wire(),
            )
            .await
            .unwrap();
        assert_eq!(read_frame(&mut client).await.unwrap().unwrap().tag, b'Z');
        client
            .write_all(
                &PgFrame {
                    tag: b'X',
                    body: vec![],
                }
                .to_wire(),
            )
            .await
            .unwrap();
        assert_eq!(task.await.unwrap().unwrap(), BackendTransactionStatus::Idle);
        backend.await.unwrap();
    }
}
