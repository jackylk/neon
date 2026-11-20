//! Bound an output backpressure episode without timing database execution.
//! A partial write must not let a trickle reader restart the budget. Only a
//! completed flush ends an episode, so normal independent replies get fresh budgets.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(crate) struct ClientWriteGuard<S> {
    inner: S,
    timeout: Option<Duration>,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl<S> ClientWriteGuard<S> {
    pub(crate) fn new(inner: S, timeout: Option<Duration>) -> Self {
        Self {
            inner,
            timeout,
            deadline: None,
        }
    }
    fn stalled<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        use std::future::Future;
        if let Some(timeout) = self.timeout {
            let deadline = self
                .deadline
                .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
            if deadline.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "client output backpressure budget exceeded",
                )));
            }
        }
        Poll::Pending
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for ClientWriteGuard<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ClientWriteGuard<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.deadline.is_some() {
            if let Poll::Ready(error) = self.stalled(cx) {
                return Poll::Ready(error);
            }
        }
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Pending => self.stalled(cx),
            ready => ready,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.deadline.is_some() {
            if let Poll::Ready(error) = self.stalled(cx) {
                return Poll::Ready(error);
            }
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Pending => self.stalled(cx),
            ready => {
                self.deadline = None;
                ready
            }
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_shutdown(cx) {
            Poll::Pending => self.stalled(cx),
            ready => {
                self.deadline = None;
                ready
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn non_reading_client_times_out() {
        let (writer, _non_reader) = tokio::io::duplex(16);
        let mut guarded = ClientWriteGuard::new(writer, Some(Duration::from_millis(30)));
        let result =
            tokio::time::timeout(Duration::from_millis(300), guarded.write_all(&[1; 64])).await;
        assert_eq!(
            result
                .expect("client stall must terminate")
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }
    #[tokio::test]
    async fn waiting_for_query_output_is_not_a_write_stall() {
        let (writer, mut reader) = tokio::io::duplex(16);
        let mut guarded = ClientWriteGuard::new(writer, Some(Duration::from_millis(20)));
        tokio::time::sleep(Duration::from_millis(60)).await;
        guarded.write_all(b"ok").await.unwrap();
        let mut output = [0; 2];
        reader.read_exact(&mut output).await.unwrap();
        assert_eq!(&output, b"ok");
    }
    #[tokio::test]
    async fn trickle_progress_does_not_extend_a_backpressure_episode() {
        let (writer, mut reader) = tokio::io::duplex(1);
        let mut guarded = ClientWriteGuard::new(writer, Some(Duration::from_millis(60)));
        let drain = tokio::spawn(async move {
            let mut byte = [0];
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                reader.read_exact(&mut byte).await.unwrap();
            }
        });
        let error = guarded.write_all(&[1; 8]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drain.abort();
    }
    #[tokio::test]
    async fn flushed_messages_have_independent_budgets() {
        let (writer, mut reader) = tokio::io::duplex(16);
        let mut guarded = ClientWriteGuard::new(writer, Some(Duration::from_millis(30)));
        for _ in 0..4 {
            guarded.write_all(b"ok").await.unwrap();
            guarded.flush().await.unwrap();
            let mut output = [0; 2];
            reader.read_exact(&mut output).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    #[tokio::test]
    async fn disabled_guard_preserves_backpressure() {
        let (writer, _reader) = tokio::io::duplex(1);
        let mut guarded = ClientWriteGuard::new(writer, None);
        assert!(
            tokio::time::timeout(Duration::from_millis(40), guarded.write_all(&[1; 2]))
                .await
                .is_err()
        );
    }
}
