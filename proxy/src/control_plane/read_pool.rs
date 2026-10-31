//! Per-process, bounded routing state. No backend connection or SQL is moved between sessions.
use serde::Deserialize;

pub(crate) const MAX_READ_CANDIDATES: usize = 256;
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct Candidate {
    pub(crate) endpoint_id: String,
    pub(crate) address: String,
    pub(crate) generation: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReadPool {
    pub(crate) pool_key: String,
    pub(crate) candidates: Vec<Candidate>,
    pub(crate) fallback: Option<Candidate>,
}

#[derive(Default)]
struct Cursor {
    members: Vec<Candidate>,
    next: usize,
}

pub(crate) struct ReadPoolRouter {
    seed: u64,
    failed: moka::sync::Cache<(String, String), ()>,
    cursors: moka::sync::Cache<String, Arc<Mutex<Cursor>>>,
}

impl ReadPoolRouter {
    pub(crate) fn new() -> Self {
        Self::with_seed(rand::random())
    }
    fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            failed: moka::sync::Cache::builder()
                .max_capacity(4096)
                .time_to_live(Duration::from_secs(5))
                .build(),
            cursors: moka::sync::Cache::builder()
                .max_capacity(4096)
                .time_to_idle(Duration::from_secs(600))
                .build(),
        }
    }

    pub(crate) fn mark_failed(&self, endpoint: &str, generation: &str) {
        if endpoint.starts_with("ep_") {
            self.failed.insert((endpoint.into(), generation.into()), ());
        }
    }
    pub(crate) fn failed_ids(&self, pool: &ReadPool) -> Vec<String> {
        pool.candidates
            .iter()
            .filter(|c| {
                self.failed
                    .contains_key(&(c.endpoint_id.clone(), c.generation.clone()))
            })
            .map(|c| c.endpoint_id.clone())
            .collect()
    }

    pub(crate) fn select(&self, pool: &ReadPool) -> io::Result<Candidate> {
        fn invalid() -> io::Error {
            io::Error::new(io::ErrorKind::InvalidData, "invalid read pool metadata")
        }
        fn valid(c: &Candidate) -> bool {
            !c.endpoint_id.is_empty() && !c.address.is_empty() && !c.generation.is_empty()
        }
        if pool.pool_key.is_empty() || pool.candidates.len() > MAX_READ_CANDIDATES {
            return Err(invalid());
        }
        if pool.candidates.is_empty() {
            return pool
                .fallback
                .as_ref()
                .filter(|c| valid(c))
                .cloned()
                .ok_or_else(invalid);
        }
        let mut members = pool.candidates.clone();
        let mut ids = HashSet::new();
        if members
            .iter()
            .any(|c| !valid(c) || !c.endpoint_id.starts_with("ep_") || !ids.insert(&c.endpoint_id))
        {
            return Err(invalid());
        }
        members.retain(|c| {
            !self
                .failed
                .contains_key(&(c.endpoint_id.clone(), c.generation.clone()))
        });
        if members.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "no reachable read candidates",
            ));
        }
        members.sort_by(|a, b| a.endpoint_id.cmp(&b.endpoint_id));
        let cursor = self.cursors.get_with(pool.pool_key.clone(), || {
            Arc::new(Mutex::new(Cursor::default()))
        });
        let mut cursor = cursor
            .lock()
            .map_err(|_| io::Error::other("read routing lock poisoned"))?;
        if cursor.members != members {
            let mut hash = DefaultHasher::new();
            self.seed.hash(&mut hash);
            pool.pool_key.hash(&mut hash);
            members.hash(&mut hash);
            cursor.next = (hash.finish() % members.len() as u64) as usize;
            cursor.members = members;
        }
        let selected = cursor.members[cursor.next].clone();
        cursor.next = (cursor.next + 1) % cursor.members.len();
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn pool() -> ReadPool {
        ReadPool {
            pool_key: "db:branch".into(),
            candidates: (0..3)
                .map(|i| Candidate {
                    endpoint_id: format!("ep_{i}"),
                    address: format!("10.0.0.{}:55433", i + 1),
                    generation: format!("1:uid_{i}"),
                })
                .collect(),
            fallback: None,
        }
    }
    #[test]
    fn every_proxy_is_balanced_even_with_unequal_traffic() {
        let p = pool();
        let mut total = [0; 3];
        for (seed, traffic) in [1, 2, 9, 100, 1000, 9999].into_iter().enumerate() {
            let rr = ReadPoolRouter::with_seed(seed as u64);
            let mut counts = [0; 3];
            for _ in 0..traffic {
                let c = rr.select(&p).unwrap();
                let i = c.endpoint_id[3..].parse::<usize>().unwrap();
                counts[i] += 1;
                total[i] += 1;
            }
            assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
        }
        assert!(total.iter().max().unwrap() - total.iter().min().unwrap() <= 6);
    }
    #[test]
    fn proxy_starts_are_not_synchronized_at_first_candidate() {
        let p = pool();
        let starts: std::collections::HashSet<_> = (0..32)
            .map(|seed| {
                ReadPoolRouter::with_seed(seed)
                    .select(&p)
                    .unwrap()
                    .endpoint_id
            })
            .collect();
        assert_eq!(starts.len(), 3);
    }
    #[test]
    fn concurrent_connections_have_exact_round_robin_distribution() {
        let rr = Arc::new(ReadPoolRouter::with_seed(42));
        let p = Arc::new(pool());
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let rr = rr.clone();
                let p = p.clone();
                std::thread::spawn(move || {
                    let mut counts = [0; 3];
                    for _ in 0..1500 {
                        let c = rr.select(&p).unwrap();
                        counts[c.endpoint_id[3..].parse::<usize>().unwrap()] += 1;
                    }
                    counts
                })
            })
            .collect();
        let mut total = [0; 3];
        for j in jobs {
            for (i, n) in j.join().unwrap().into_iter().enumerate() {
                total[i] += n;
            }
        }
        assert_eq!(total, [4000; 3]);
    }
    #[test]
    fn membership_updates_and_reordered_metadata_do_not_leave_stale_candidates() {
        let rr = ReadPoolRouter::with_seed(3);
        let mut p = pool();
        rr.select(&p).unwrap();
        p.candidates.retain(|c| c.endpoint_id != "ep_0");
        for _ in 0..20 {
            assert_ne!(rr.select(&p).unwrap().endpoint_id, "ep_0");
        }
        p.candidates.reverse();
        let mut seen = Vec::new();
        for _ in 0..2 {
            seen.push(rr.select(&p).unwrap().endpoint_id);
        }
        seen.sort();
        assert_eq!(seen, ["ep_1", "ep_2"]);
        p.candidates[0].generation = "2:new-pod".into();
        assert!(rr.select(&p).is_ok());
    }
    #[test]
    fn fallback_only_applies_to_a_valid_empty_pool() {
        let rr = ReadPoolRouter::with_seed(1);
        let mut p = pool();
        p.candidates.clear();
        assert!(rr.select(&p).is_err());
        p.fallback = Some(Candidate {
            endpoint_id: "db".into(),
            address: "10.0.0.9:55433".into(),
            generation: "primary:uid".into(),
        });
        assert_eq!(rr.select(&p).unwrap().endpoint_id, "db");
        p.pool_key.clear();
        assert!(rr.select(&p).is_err());
    }
    #[test]
    fn invalid_metadata_is_not_silently_treated_as_fallback() {
        let rr = ReadPoolRouter::with_seed(1);
        let mut p = pool();
        p.candidates[0].generation.clear();
        assert!(rr.select(&p).is_err());
        let mut p = pool();
        p.candidates.push(p.candidates[0].clone());
        assert!(rr.select(&p).is_err());
    }
    #[test]
    fn failed_target_is_excluded_until_generation_changes() {
        let rr = ReadPoolRouter::with_seed(1);
        let mut p = pool();
        rr.mark_failed("ep_0", "1:uid_0");
        for _ in 0..15 {
            assert_ne!(rr.select(&p).unwrap().endpoint_id, "ep_0");
        }
        p.candidates[0].generation = "2:new-pod".into();
        let selected: std::collections::HashSet<_> =
            (0..3).map(|_| rr.select(&p).unwrap().endpoint_id).collect();
        assert_eq!(selected.len(), 3);
    }
}
