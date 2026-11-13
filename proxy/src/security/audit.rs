use std::{sync::{Arc, OnceLock},time::Duration};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Default)]
pub(super) struct AuditQueue(OnceLock<mpsc::Sender<Value>>);
impl AuditQueue {
    pub(super) fn emit(&self,endpoint:crate::http::Endpoint,token:Arc<str>,event:Value) {
        let Ok(runtime)=tokio::runtime::Handle::try_current() else {
            crate::metrics::Metrics::get().proxy.security_audit_dropped_total.inc();return;
        };
        let sender=self.0.get_or_init(|| {
            let (sender,mut receiver)=mpsc::channel::<Value>(1024);
            runtime.spawn(async move {
                while let Some(first)=receiver.recv().await {
                    let mut batch=vec![first];
                    while batch.len()<32 {match receiver.try_recv(){Ok(event)=>batch.push(event),Err(_)=>break}}
                    let mut delivered=false;
                    for _ in 0..2 {
                        let send=async {
                            let request=endpoint.request_with_url(http::Method::POST,|url|{url.path_segments_mut().push("security_events");})
                                .header("Authorization",format!("Bearer {token}")).json(&batch).build().ok()?;
                            endpoint.execute(request).await.ok().filter(|r|r.status().is_success())
                        };
                        if matches!(tokio::time::timeout(Duration::from_secs(2),send).await,Ok(Some(_))) {delivered=true;break;}
                        crate::metrics::Metrics::get().proxy.security_audit_send_failures_total.inc();
                    }
                    if !delivered {for _ in &batch {crate::metrics::Metrics::get().proxy.security_audit_dropped_total.inc();}}
                }
            });sender
        });
        if sender.try_send(event).is_err(){crate::metrics::Metrics::get().proxy.security_audit_dropped_total.inc();}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn endpoint(port:u16)->crate::http::Endpoint {
        crate::http::Endpoint::new(format!("http://127.0.0.1:{port}/proxy").parse().unwrap(),crate::http::new_client())
    }
    #[tokio::test]
    async fn full_queue_drops_without_waiting_for_collector() {
        let (sender,_receiver)=mpsc::channel(1);
        sender.try_send(serde_json::json!({"id":"first"})).unwrap();
        let queue=AuditQueue(OnceLock::from(sender));
        let target=endpoint(1);
        tokio::time::timeout(Duration::from_millis(100),async {
            for _ in 0..1000 {queue.emit(target.clone(),Arc::from("test-only"),serde_json::json!({"id":"overflow"}));}
        }).await.unwrap();
        assert_eq!(queue.0.get().unwrap().capacity(),0);
    }
    #[tokio::test]
    async fn stalled_collector_has_only_two_bounded_attempts() {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port=listener.local_addr().unwrap().port();
        let count=Arc::new(std::sync::atomic::AtomicUsize::new(0));let observed=count.clone();
        let server=tokio::spawn(async move {
            let mut sockets=Vec::new();
            loop {let (socket,_)=listener.accept().await.unwrap();sockets.push(socket);observed.fetch_add(1,std::sync::atomic::Ordering::SeqCst);}
        });
        let queue=AuditQueue::default();
        queue.emit(endpoint(port),Arc::from("test-only"),serde_json::json!({"id":"bounded"}));
        tokio::time::sleep(Duration::from_millis(4600)).await;
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst),2);
        server.abort();
    }
}
