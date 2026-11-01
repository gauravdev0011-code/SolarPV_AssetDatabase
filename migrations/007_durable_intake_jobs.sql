-- Durable runtime controls for jobs. The Phase 1 job table and its state
-- constraint remain unchanged; this linked row adds lease/retry mechanics.
CREATE TABLE IF NOT EXISTS ingestion_job_runtime (
    job_id TEXT PRIMARY KEY REFERENCES ingestion_jobs(job_id) ON DELETE RESTRICT,
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN (
        'queued', 'running', 'retry_wait', 'succeeded', 'failed_terminal',
        'cancel_requested', 'cancelled', 'reconciliation_required'
    )),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    retry_of_job_id TEXT REFERENCES ingestion_job_runtime(job_id) ON DELETE RESTRICT,
    available_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (state IN ('running', 'cancel_requested')
         AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
         AND lease_expires_at IS NOT NULL)
        OR
        (state NOT IN ('running', 'cancel_requested')
         AND lease_owner IS NULL AND lease_token IS NULL
         AND lease_expires_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_ingestion_job_runtime_claim
    ON ingestion_job_runtime(state, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_job_runtime_lease
    ON ingestion_job_runtime(state, lease_expires_at);

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_runtime_starts_queued
BEFORE INSERT ON ingestion_job_runtime
WHEN NEW.state <> 'queued' OR NEW.attempt_count <> 0
BEGIN
    SELECT RAISE(ABORT, 'new ingestion job runtime must start queued');
END;

CREATE TRIGGER IF NOT EXISTS trg_source_file_intake_transition
BEFORE UPDATE OF state ON source_files
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'receiving' AND NEW.state IN ('quarantined', 'intake_rejected'))
    OR (OLD.state = 'quarantined' AND NEW.state IN ('available', 'intake_rejected'))
    OR (OLD.state = 'available' AND NEW.state = 'deletion_pending')
    OR (OLD.state = 'deletion_pending' AND NEW.state = 'deleted')
)
BEGIN
    SELECT RAISE(ABORT, 'invalid source file intake state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_runtime_transition
BEFORE UPDATE OF state ON ingestion_job_runtime
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'queued' AND NEW.state IN ('running', 'cancelled'))
    OR (OLD.state = 'running' AND NEW.state IN (
        'retry_wait', 'succeeded', 'failed_terminal', 'cancel_requested',
        'reconciliation_required'
    ))
    OR (OLD.state = 'retry_wait' AND NEW.state IN ('running', 'cancelled'))
    OR (OLD.state = 'cancel_requested' AND NEW.state IN (
        'cancelled', 'succeeded', 'failed_terminal'
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid ingestion job runtime state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_attempt_count_monotonic
BEFORE UPDATE OF attempt_count ON ingestion_job_runtime
WHEN NEW.attempt_count < OLD.attempt_count
BEGIN
    SELECT RAISE(ABORT, 'ingestion job attempt count cannot decrease');
END;
