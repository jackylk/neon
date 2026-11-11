//! Bounds work before a PostgreSQL client has authenticated.
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

/// Per-process pre-authentication budget. Only active sources are retained.
#[derive(Clone)]
pub(crate) struct Admission(Arc<Inner>);
struct Inner {
    max_total: usize,
    max_per_source: usize,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    total: usize,
    sources: HashMap<IpAddr, usize>,
}

pub(crate) struct Permit {
    inner: Arc<Inner>,
    source: Option<IpAddr>,
    pub(crate) started: tokio::time::Instant,
}
impl Admission {
    pub(crate) fn new(max_total: usize, max_per_source: usize) -> Self {
        assert!(
            max_total > 0 && max_per_source > 0,
            "admission limits must be positive"
        );
        Self(Arc::new(Inner {
            max_total,
            max_per_source,
            state: Mutex::new(State::default()),
        }))
    }
    pub(crate) fn try_acquire(&self) -> Option<Permit> {
        let mut state = self.0.state.lock().expect("admission lock poisoned");
        if state.total >= self.0.max_total {
            return None;
        }
        state.total += 1;
        Some(Permit {
            inner: self.0.clone(),
            source: None,
            started: tokio::time::Instant::now(),
        })
    }
}
impl Permit {
    /// Called once, after the trusted transport header has supplied the source.
    pub(crate) fn assign_source(&mut self, source: IpAddr) -> bool {
        if self.source.is_some() {
            return false;
        }
        let mut state = self.inner.state.lock().expect("admission lock poisoned");
        let count = state.sources.entry(source).or_default();
        if *count >= self.inner.max_per_source {
            return false;
        }
        *count += 1;
        self.source = Some(source);
        true
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("admission lock poisoned");
        state.total -= 1;
        if let Some(source) = self.source {
            let count = state
                .sources
                .get_mut(&source)
                .expect("assigned source missing");
            *count -= 1;
            if *count == 0 {
                state.sources.remove(&source);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    fn ip(n: u8) -> IpAddr {
        Ipv4Addr::new(192, 0, 2, n).into()
    }
    #[test]
    fn global_capacity_is_reclaimed_on_drop() {
        let limits = Admission::new(2, 2);
        let a = limits.try_acquire().unwrap();
        let b = limits.try_acquire().unwrap();
        assert!(limits.try_acquire().is_none());
        drop(a);
        assert!(limits.try_acquire().is_some());
        drop(b);
    }
    #[test]
    fn source_capacity_is_independent_and_reclaimed() {
        let limits = Admission::new(4, 1);
        let mut a = limits.try_acquire().unwrap();
        assert!(a.assign_source(ip(1)));
        let mut b = limits.try_acquire().unwrap();
        assert!(!b.assign_source(ip(1)));
        assert!(b.assign_source(ip(2)));
        drop(a);
        let mut c = limits.try_acquire().unwrap();
        assert!(c.assign_source(ip(1)));
    }
    #[test]
    fn source_assignment_cannot_be_repeated() {
        let limits = Admission::new(2, 1);
        let mut a = limits.try_acquire().unwrap();
        assert!(a.assign_source(ip(1)));
        assert!(!a.assign_source(ip(2)));
        drop(a);
        assert!(limits.try_acquire().unwrap().assign_source(ip(1)));
    }
    #[test]
    fn concurrent_global_admission_never_exceeds_limit() {
        let limits = Admission::new(8, 2);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(32));
        let workers: Vec<_> = (0..32)
            .map(|_| {
                let limits = limits.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let permit = limits.try_acquire();
                    barrier.wait();
                    let accepted = permit.is_some();
                    barrier.wait();
                    accepted
                })
            })
            .collect();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| w.join().unwrap())
                .filter(|accepted| *accepted)
                .count(),
            8
        );
        assert!(limits.try_acquire().is_some());
    }
    #[test]
    fn invalid_limits_are_rejected() {
        assert!(std::panic::catch_unwind(|| Admission::new(0, 1)).is_err());
        assert!(std::panic::catch_unwind(|| Admission::new(1, 0)).is_err());
    }
}
