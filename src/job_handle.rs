use std::{collections::HashMap, sync::Arc};

use aide_de_camp::core::{job_handle::JobHandle, queue::QueueError, Bytes, Xid};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::types::JobRow;

pub struct MemoryJobHandle {
    jobs: Arc<RwLock<HashMap<Xid, JobRow>>>,
    dead_jobs: Arc<RwLock<Vec<JobRow>>>,
    row: JobRow,
}

impl MemoryJobHandle {
    pub(crate) fn new(
        jobs: Arc<RwLock<HashMap<Xid, JobRow>>>,
        dead_jobs: Arc<RwLock<Vec<JobRow>>>,
        row: JobRow,
    ) -> Self {
        Self {
            jobs,
            dead_jobs,
            row,
        }
    }
}

#[async_trait]
impl JobHandle for MemoryJobHandle {
    fn id(&self) -> Xid {
        self.row.jid
    }

    fn job_type(&self) -> &str {
        &self.row.job_type
    }

    fn payload(&self) -> Bytes {
        self.row.payload.clone()
    }

    fn retries(&self) -> u32 {
        self.row.retries
    }

    async fn complete(mut self) -> Result<(), QueueError> {
        let jid = self.id();
        let mut jobs = self.jobs.write().await;
        jobs.remove(&jid);
        Ok(())
    }

    async fn fail(mut self) -> Result<(), QueueError> {
        let jid = self.id();
        let mut jobs = self.jobs.write().await;

        let job = jobs.get_mut(&jid).ok_or(QueueError::JobNotFound(jid))?;
        job.started_at = None;

        Ok(())
    }

    async fn dead_queue(mut self) -> Result<(), QueueError> {
        let jid = self.id();

        let mut jobs = self.jobs.write().await;
        let removed_job = jobs.remove(&jid);

        if let Some(removed_job) = removed_job {
            let mut dead_jobs = self.dead_jobs.write().await;
            dead_jobs.push(removed_job);
        }

        Ok(())
    }
}
