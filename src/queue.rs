use std::{collections::HashMap, sync::Arc};

use aide_de_camp::core::{
    job_processor::JobProcessor,
    new_xid,
    queue::{Queue, QueueError},
    DateTime, Xid,
};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use tokio::sync::RwLock;
use tracing::instrument;

use crate::{job_handle::MemoryJobHandle, types::JobRow};

pub struct MemoryQueue {
    jobs: Arc<RwLock<HashMap<Xid, JobRow>>>,
    dead_jobs: Arc<RwLock<Vec<JobRow>>>,
    bincode_config: bincode::config::Configuration,
}

impl MemoryQueue {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            dead_jobs: Arc::new(RwLock::new(Vec::new())),
            bincode_config: bincode::config::standard(),
        }
    }
}

#[async_trait]
impl Queue for MemoryQueue {
    type JobHandle = MemoryJobHandle;

    #[instrument(skip_all, err, ret, fields(job_type = J::name(), payload_size))]
    async fn schedule_at<J>(
        &self,
        payload: J::Payload,
        scheduled_at: DateTime,
        priority: i8,
    ) -> Result<Xid, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: Encode,
    {
        let payload = bincode::encode_to_vec(&payload, self.bincode_config)?;
        let jid = new_xid();
        let job_type = J::name().to_string();

        tracing::Span::current().record("payload_size", payload.len());

        let mut jobs = self.jobs.write().await;

        jobs.insert(
            jid,
            JobRow {
                jid,
                job_type,
                payload: payload.into(),
                retries: 0,
                priority,
                scheduled_at,
                started_at: None,
            },
        );

        Ok(jid)
    }

    #[instrument(skip_all, err)]
    async fn poll_next_with_instant(
        &self,
        job_types: &[&str],
        now: DateTime,
    ) -> Result<Option<Self::JobHandle>, QueueError> {
        let mut jobs = self.jobs.write().await;

        let job = jobs
            .values_mut()
            .filter(|j| {
                j.started_at == None
                    && j.scheduled_at <= now
                    && job_types.contains(&j.job_type.as_str())
            })
            .max_by(|a, b| a.priority.cmp(&b.priority));

        if let Some(job) = job {
            job.started_at = Some(now);
            job.retries += 1;

            Ok(Some(MemoryJobHandle::new(
                Arc::clone(&self.jobs),
                Arc::clone(&self.dead_jobs),
                job.clone(),
            )))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, err)]
    async fn cancel_job(&self, job_id: Xid) -> Result<(), QueueError> {
        let mut jobs = self.jobs.write().await;

        let job = jobs.get(&job_id);

        if let Some(job) = job {
            if job.started_at.is_none() {
                jobs.remove(&job_id);
                Ok(())
            } else {
                Err(QueueError::JobNotFound(job_id))
            }
        } else {
            Err(QueueError::JobNotFound(job_id))
        }
    }

    #[allow(clippy::or_fun_call)]
    #[instrument(skip_all, err)]
    async fn unschedule_job<J>(&self, job_id: Xid) -> Result<J::Payload, QueueError>
    where
        J: JobProcessor + 'static,
        J::Payload: Decode,
    {
        let job_type = J::name();
        let mut jobs = self.jobs.write().await;

        let job = jobs.get(&job_id);

        if let Some(job) = job {
            if job.started_at.is_none() && job.job_type == job_type {
                let (decoded, _) = bincode::decode_from_slice(&job.payload, self.bincode_config)?;
                jobs.remove(&job_id);
                Ok(decoded)
            } else {
                Err(QueueError::JobNotFound(job_id))
            }
        } else {
            Err(QueueError::JobNotFound(job_id))
        }
    }
}
