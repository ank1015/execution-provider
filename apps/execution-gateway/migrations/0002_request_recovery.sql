ALTER TABLE jobs
    ADD COLUMN response_recovery boolean NOT NULL DEFAULT false,
    ADD COLUMN recovery_expires_at timestamptz;

CREATE INDEX jobs_recovery_expires_idx ON jobs(recovery_expires_at)
    WHERE status IN ('dispatching','waiting_response');
