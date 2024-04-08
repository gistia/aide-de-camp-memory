use aide_de_camp::core::{Bytes, DateTime, Xid};

#[derive(Debug, Clone)]
pub struct JobRow {
    pub jid: Xid,
    pub job_type: String,
    pub payload: Bytes,
    pub retries: u32,
    pub priority: i8,
    pub scheduled_at: DateTime,
    pub started_at: Option<DateTime>,
}
