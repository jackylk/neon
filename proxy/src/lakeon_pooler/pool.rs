use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::time;

use super::session::PooledBackend;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PoolKey {
    endpoint_id: String,
    branch_id: String,
    database: String,
    role: String,
}

#[derive(Clone)]
pub(crate) struct LakeonBackendPool {
    inner: Arc<Mutex<PoolInner>>,
    max_backends_per_key: usize,
}

impl LakeonBackendPool {
    pub(crate) fn new(max_backends_per_key: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner::default())),
            max_backends_per_key,
        }
    }

    pub(crate) async fn checkout_or_reserve(
        &self,
        key: &PoolKey,
        queue_timeout: Duration,
    ) -> Result<Checkout<PooledBackend>, CheckoutError> {
        loop {
            let notify = {
                let mut inner = self.inner.lock().await;
                match inner.checkout_or_reserve(key.clone(), self.max_backends_per_key) {
                    Ok(checkout) => return Ok(checkout),
                    Err(CheckoutError::PoolFull) => inner.notify_for(key),
                    Err(CheckoutError::QueueTimeout) => {
                        unreachable!("queue timeout is not emitted while holding the pool lock")
                    }
                }
            };

            if time::timeout(queue_timeout, notify.notified())
                .await
                .is_err()
            {
                return Err(CheckoutError::QueueTimeout);
            }
        }
    }

    pub(crate) async fn release_idle(&self, key: PoolKey, backend: PooledBackend) {
        let mut inner = self.inner.lock().await;
        inner.release_idle(key, backend);
    }

    pub(crate) async fn release_checked_out(&self, key: &PoolKey) {
        let mut inner = self.inner.lock().await;
        inner.release_checked_out(key);
    }

    pub(crate) async fn stats(&self) -> PoolStats {
        let inner = self.inner.lock().await;
        stats_for_pools(inner.pools.values())
    }
}

impl Default for LakeonBackendPool {
    fn default() -> Self {
        Self::new(20)
    }
}

impl PoolKey {
    pub(crate) fn new(endpoint_id: &str, branch_id: &str, database: &str, role: &str) -> Self {
        Self {
            endpoint_id: endpoint_id.to_owned(),
            branch_id: branch_id.to_owned(),
            database: database.to_owned(),
            role: role.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckoutError {
    PoolFull,
    QueueTimeout,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Checkout<T> {
    Idle(T),
    ReservedNew,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PoolStats {
    pub(crate) pools: usize,
    pub(crate) checked_out: usize,
    pub(crate) idle: usize,
    pub(crate) total: usize,
}

#[derive(Default)]
struct PoolInner {
    pools: HashMap<PoolKey, BackendSet<PooledBackend>>,
}

fn stats_for_pools<'a, T>(pools: impl Iterator<Item = &'a BackendSet<T>>) -> PoolStats
where
    T: 'a,
{
    let mut stats = PoolStats::default();
    for pool in pools {
        stats.pools += 1;
        stats.checked_out += pool.checked_out;
        stats.idle += pool.idle.len();
    }
    stats.total = stats.checked_out + stats.idle;
    stats
}

impl PoolInner {
    fn checkout_or_reserve(
        &mut self,
        key: PoolKey,
        max_backends_per_key: usize,
    ) -> Result<Checkout<PooledBackend>, CheckoutError> {
        let pool = self.pools.entry(key).or_default();
        if let Some(backend) = pool.idle.pop_front() {
            pool.checked_out += 1;
            return Ok(Checkout::Idle(backend));
        }

        if pool.checked_out + pool.idle.len() >= max_backends_per_key {
            return Err(CheckoutError::PoolFull);
        }

        pool.checked_out += 1;
        Ok(Checkout::ReservedNew)
    }

    fn notify_for(&mut self, key: &PoolKey) -> Arc<Notify> {
        self.pools.entry(key.clone()).or_default().notify.clone()
    }

    fn release_idle(&mut self, key: PoolKey, backend: PooledBackend) {
        let pool = self.pools.entry(key).or_default();
        pool.checked_out = pool.checked_out.saturating_sub(1);
        pool.idle.push_back(backend);
        pool.notify.notify_one();
    }

    fn release_checked_out(&mut self, key: &PoolKey) {
        if let Some(pool) = self.pools.get_mut(key) {
            pool.checked_out = pool.checked_out.saturating_sub(1);
            pool.notify.notify_one();
        }
    }
}

#[derive(Debug)]
#[cfg(test)]
pub(crate) struct TransactionPool<T> {
    max_backends_per_key: usize,
    pools: HashMap<PoolKey, BackendSet<T>>,
}

#[derive(Debug)]
struct BackendSet<T> {
    idle: VecDeque<T>,
    checked_out: usize,
    notify: Arc<Notify>,
}

impl<T> Default for BackendSet<T> {
    fn default() -> Self {
        Self {
            idle: VecDeque::new(),
            checked_out: 0,
            notify: Arc::new(Notify::new()),
        }
    }
}

#[cfg(test)]
impl<T> TransactionPool<T> {
    pub(crate) fn new(max_backends_per_key: usize) -> Self {
        Self {
            max_backends_per_key,
            pools: HashMap::new(),
        }
    }

    pub(crate) fn checkout_or_insert(
        &mut self,
        key: PoolKey,
        create: impl FnOnce() -> T,
    ) -> Result<T, CheckoutError> {
        let pool = self.pools.entry(key).or_default();
        if let Some(backend) = pool.idle.pop_front() {
            pool.checked_out += 1;
            return Ok(backend);
        }

        if pool.checked_out >= self.max_backends_per_key {
            return Err(CheckoutError::PoolFull);
        }

        pool.checked_out += 1;
        Ok(create())
    }

    pub(crate) fn checkout_or_reserve(
        &mut self,
        key: PoolKey,
    ) -> Result<Checkout<T>, CheckoutError> {
        let pool = self.pools.entry(key).or_default();
        if let Some(backend) = pool.idle.pop_front() {
            pool.checked_out += 1;
            return Ok(Checkout::Idle(backend));
        }

        if pool.checked_out + pool.idle.len() >= self.max_backends_per_key {
            return Err(CheckoutError::PoolFull);
        }

        pool.checked_out += 1;
        Ok(Checkout::ReservedNew)
    }

    pub(crate) fn release_idle(&mut self, key: PoolKey, backend: T) {
        let pool = self.pools.entry(key).or_default();
        pool.checked_out = pool.checked_out.saturating_sub(1);
        pool.idle.push_back(backend);
    }

    pub(crate) fn release_checked_out(&mut self, key: &PoolKey) {
        if let Some(pool) = self.pools.get_mut(key) {
            pool.checked_out = pool.checked_out.saturating_sub(1);
        }
    }

    pub(crate) fn total_backends(&self) -> usize {
        self.pools
            .values()
            .map(|pool| pool.checked_out + pool.idle.len())
            .sum()
    }

    pub(crate) fn stats(&self) -> PoolStats {
        stats_for_pools(self.pools.values())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Checkout, CheckoutError, LakeonBackendPool, PoolKey, TransactionPool};

    #[test]
    fn checkout_reuses_idle_backend_for_same_key() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let mut pool = TransactionPool::new(2);

        let first = pool
            .checkout_or_insert(key.clone(), || "backend-1")
            .unwrap();
        pool.release_idle(key.clone(), first);

        let second = pool.checkout_or_insert(key, || "backend-2").unwrap();

        assert_eq!(second, "backend-1");
        assert_eq!(pool.total_backends(), 1);
    }

    #[test]
    fn checkout_rejects_when_pool_is_full_for_key() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let mut pool: TransactionPool<&str> = TransactionPool::new(1);

        let _first = pool
            .checkout_or_insert(key.clone(), || "backend-1")
            .unwrap();

        assert_eq!(
            pool.checkout_or_insert(key, || "backend-2"),
            Err(CheckoutError::PoolFull)
        );
    }

    #[test]
    fn checkout_waits_when_pool_is_full_then_reserves_after_release() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let mut pool: TransactionPool<&str> = TransactionPool::new(1);

        let first = pool.checkout_or_reserve(key.clone()).unwrap();
        assert_eq!(first, Checkout::ReservedNew);

        assert_eq!(
            pool.checkout_or_reserve(key.clone()),
            Err(CheckoutError::PoolFull)
        );

        pool.release_checked_out(&key);

        let second = pool.checkout_or_reserve(key).unwrap();
        assert_eq!(second, Checkout::ReservedNew);
    }

    #[tokio::test]
    async fn backend_pool_waiter_reserves_after_checked_out_backend_is_released() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let pool = LakeonBackendPool::new(1);

        assert!(matches!(
            pool.checkout_or_reserve(&key, Duration::from_secs(1))
                .await
                .unwrap(),
            Checkout::ReservedNew
        ));

        let waiting_pool = pool.clone();
        let waiting_key = key.clone();
        let waiter = tokio::spawn(async move {
            waiting_pool
                .checkout_or_reserve(&waiting_key, Duration::from_secs(1))
                .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        pool.release_checked_out(&key).await;

        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            Checkout::ReservedNew
        ));
    }

    #[tokio::test]
    async fn backend_pool_waiter_times_out_when_no_backend_is_released() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let pool = LakeonBackendPool::new(0);

        assert!(matches!(
            pool.checkout_or_reserve(&key, Duration::from_millis(10))
                .await,
            Err(CheckoutError::QueueTimeout)
        ));
    }

    #[test]
    fn different_keys_have_independent_limits() {
        let mut pool = TransactionPool::new(1);

        let _first = pool
            .checkout_or_insert(
                PoolKey::new("endpoint", "branch-a", "db", "role"),
                || "backend-a",
            )
            .unwrap();
        let second = pool
            .checkout_or_insert(
                PoolKey::new("endpoint", "branch-b", "db", "role"),
                || "backend-b",
            )
            .unwrap();

        assert_eq!(second, "backend-b");
        assert_eq!(pool.total_backends(), 2);
    }

    #[test]
    fn checked_out_backend_counts_against_pool_limit_until_released() {
        let key = PoolKey::new("endpoint", "branch", "db", "role");
        let mut pool = TransactionPool::new(1);

        let first = pool
            .checkout_or_insert(key.clone(), || "backend-1")
            .unwrap();

        assert_eq!(
            pool.checkout_or_insert(key.clone(), || "backend-2"),
            Err(CheckoutError::PoolFull)
        );

        pool.release_checked_out(&key);

        let second = pool.checkout_or_insert(key, || "backend-2").unwrap();
        assert_eq!(first, "backend-1");
        assert_eq!(second, "backend-2");
        assert_eq!(pool.total_backends(), 1);
    }

    #[test]
    fn stats_report_active_idle_total_and_pool_count() {
        let key_a = PoolKey::new("endpoint", "branch-a", "db", "role");
        let key_b = PoolKey::new("endpoint", "branch-b", "db", "role");
        let mut pool = TransactionPool::new(2);

        let first = pool
            .checkout_or_insert(key_a.clone(), || "backend-a")
            .unwrap();
        pool.release_idle(key_a.clone(), first);
        let _second = pool.checkout_or_insert(key_b, || "backend-b").unwrap();

        let stats = pool.stats();

        assert_eq!(stats.pools, 2);
        assert_eq!(stats.idle, 1);
        assert_eq!(stats.checked_out, 1);
        assert_eq!(stats.total, 2);
    }
}
