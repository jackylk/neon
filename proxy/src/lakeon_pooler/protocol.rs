#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendTransactionStatus {
    Idle,
    InTransaction,
    FailedTransaction,
    Unknown,
}

impl BackendTransactionStatus {
    pub(crate) const fn from_ready_for_query_status(status: u8) -> Self {
        match status {
            b'I' => Self::Idle,
            b'T' => Self::InTransaction,
            b'E' => Self::FailedTransaction,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrontendAction {
    SimpleQuery,
    StartsTransaction,
    EndsTransaction,
    FlushesTransactionBoundary,
    Terminate,
    UnsupportedSessionState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrontendMessage<'a> {
    tag: u8,
    body: &'a [u8],
}

impl<'a> FrontendMessage<'a> {
    pub(crate) const fn new(tag: u8, body: &'a [u8]) -> Self {
        Self { tag, body }
    }

    #[cfg(test)]
    pub(crate) const fn tagged(tag: u8) -> Self {
        Self { tag, body: &[] }
    }

    #[cfg(test)]
    pub(crate) fn simple_query(sql: &'a str) -> Self {
        Self {
            tag: b'Q',
            body: sql.as_bytes(),
        }
    }

    pub(crate) fn action(self) -> FrontendAction {
        match self.tag {
            b'Q' => classify_simple_query(self.body),
            b'S' => FrontendAction::FlushesTransactionBoundary,
            b'X' => FrontendAction::Terminate,
            b'P' | b'B' | b'D' | b'E' | b'H' => FrontendAction::SimpleQuery,
            b'd' | b'c' | b'f' => FrontendAction::UnsupportedSessionState,
            _ => FrontendAction::UnsupportedSessionState,
        }
    }
}

fn classify_simple_query(body: &[u8]) -> FrontendAction {
    let Ok(sql) = std::str::from_utf8(body) else {
        return FrontendAction::UnsupportedSessionState;
    };
    let normalized = sql.trim_matches('\0').trim_start().to_ascii_lowercase();
    let first = normalized
        .split(|c: char| c.is_ascii_whitespace() || c == ';')
        .next()
        .unwrap_or_default();

    match first {
        "begin" | "start" => FrontendAction::StartsTransaction,
        "commit" | "rollback" | "end" => FrontendAction::EndsTransaction,
        "listen" | "notify" | "prepare" | "execute" | "deallocate" | "set" | "copy" => {
            FrontendAction::UnsupportedSessionState
        }
        _ => FrontendAction::SimpleQuery,
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendTransactionStatus, FrontendAction, FrontendMessage};

    #[test]
    fn ready_for_query_idle_releases_backend() {
        assert_eq!(
            BackendTransactionStatus::from_ready_for_query_status(b'I'),
            BackendTransactionStatus::Idle
        );
    }

    #[test]
    fn ready_for_query_transaction_keeps_backend_checked_out() {
        assert_eq!(
            BackendTransactionStatus::from_ready_for_query_status(b'T'),
            BackendTransactionStatus::InTransaction
        );
        assert_eq!(
            BackendTransactionStatus::from_ready_for_query_status(b'E'),
            BackendTransactionStatus::FailedTransaction
        );
    }

    #[test]
    fn simple_query_begin_checks_out_backend_until_ready_idle() {
        let query = FrontendMessage::simple_query(" begin ");

        assert_eq!(query.action(), FrontendAction::StartsTransaction);
    }

    #[test]
    fn simple_query_commit_can_release_on_next_ready_idle() {
        let query = FrontendMessage::simple_query("commit");

        assert_eq!(query.action(), FrontendAction::EndsTransaction);
    }

    #[test]
    fn session_state_sql_is_rejected_for_pooled_mvp() {
        for sql in [
            "listen channel",
            "notify channel",
            "prepare stmt as select 1",
            "set search_path = public",
            "copy t from stdin",
        ] {
            assert_eq!(
                FrontendMessage::simple_query(sql).action(),
                FrontendAction::UnsupportedSessionState
            );
        }
    }

    #[test]
    fn extended_protocol_sync_waits_for_ready_for_query() {
        assert_eq!(
            FrontendMessage::tagged(b'S').action(),
            FrontendAction::FlushesTransactionBoundary
        );
    }
}
