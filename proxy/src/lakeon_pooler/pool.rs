use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::Mutex;

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

    pub(crate) async fn checkout_idle(&self, key: &PoolKey) -> Option<PooledBackend> {
        let mut inner = self.inner.lock().await;
        let pool = inner.pools.get_mut(key)?;
        let backend = pool.idle.pop_front()?;
        pool.checked_out += 1;
        Some(backend)
    }

    pub(crate) async fn try_reserve_new(&self, key: &PoolKey) -> Result<(), CheckoutError> {
        let mut inner = self.inner.lock().await;
        inner.try_reserve_new(key.clone(), self.max_backends_per_key)
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
    fn try_reserve_new(
        &mut self,
        key: PoolKey,
        max_backends_per_key: usize,
    ) -> Result<(), CheckoutError> {
        let pool = self.pools.entry(key).or_default();
        if pool.checked_out + pool.idle.len() >= max_backends_per_key {
            return Err(CheckoutError::PoolFull);
        }

        pool.checked_out += 1;
        Ok(())
    }

    fn release_idle(&mut self, key: PoolKey, backend: PooledBackend) {
        let pool = self.pools.entry(key).or_default();
        pool.checked_out = pool.checked_out.saturating_sub(1);
        pool.idle.push_back(backend);
    }

    fn release_checked_out(&mut self, key: &PoolKey) {
        if let Some(pool) = self.pools.get_mut(key) {
            pool.checked_out = pool.checked_out.saturating_sub(1);
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
}

impl<T> Default for BackendSet<T> {
    fn default() -> Self {
        Self {
            idle: VecDeque::new(),
            checked_out: 0,
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
    use super::{CheckoutError, PoolKey, TransactionPool};

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
        let mut pool = TransactionPool::new(1);

        let _first = pool
            .checkout_or_insert(key.clone(), || "backend-1")
            .unwrap();

        assert_eq!(
            pool.checkout_or_insert(key, || "backend-2"),
            Err(CheckoutError::PoolFull)
        );
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
        let _second = pool
            .checkout_or_insert(key_b, || "backend-b")
            .unwrap();

        let stats = pool.stats();

        assert_eq!(stats.pools, 2);
        assert_eq!(stats.idle, 1);
        assert_eq!(stats.checked_out, 1);
        assert_eq!(stats.total, 2);
    }
}
