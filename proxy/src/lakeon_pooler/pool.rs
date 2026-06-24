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

#[derive(Default, Clone)]
pub(crate) struct LakeonBackendPool {
    idle: Arc<Mutex<HashMap<PoolKey, VecDeque<PooledBackend>>>>,
}

impl LakeonBackendPool {
    pub(crate) async fn checkout_idle(&self, key: &PoolKey) -> Option<PooledBackend> {
        let mut idle = self.idle.lock().await;
        idle.get_mut(key).and_then(VecDeque::pop_front)
    }

    pub(crate) async fn release_idle(&self, key: PoolKey, backend: PooledBackend) {
        let mut idle = self.idle.lock().await;
        idle.entry(key).or_default().push_back(backend);
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
#[cfg(test)]
pub(crate) enum CheckoutError {
    PoolFull,
}

#[derive(Debug)]
#[cfg(test)]
pub(crate) struct TransactionPool<T> {
    max_backends_per_key: usize,
    pools: HashMap<PoolKey, BackendSet<T>>,
}

#[derive(Debug)]
#[cfg(test)]
struct BackendSet<T> {
    idle: VecDeque<T>,
    checked_out: usize,
}

#[cfg(test)]
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

    pub(crate) fn total_backends(&self) -> usize {
        self.pools
            .values()
            .map(|pool| pool.checked_out + pool.idle.len())
            .sum()
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
}
