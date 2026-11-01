-- Persist upload sessions before bytes are streamed so crash recovery can own
-- and classify staging files without reserving a source fingerprint early.
CREATE TABLE IF NOT EXISTS intake_uploads (
    intake_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    original_name TEXT NOT NULL,
    staging_key TEXT NOT NULL UNIQUE,
    storage_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN (
        'receiving', 'staged', 'completed', 'duplicate', 'rejected',
        'reconciliation_required'
    )),
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    sha256 TEXT,
    media_type TEXT,
    source_file_id TEXT REFERENCES source_files(source_file_id) ON DELETE RESTRICT,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (state IN ('receiving', 'staged') AND lease_owner IS NOT NULL
         AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state NOT IN ('receiving', 'staged') AND lease_owner IS NULL
         AND lease_token IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (state <> 'staged' OR (sha256 IS NOT NULL AND size_bytes > 0)),
    CHECK (state NOT IN ('completed', 'duplicate') OR source_file_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_intake_upload_recovery
    ON intake_uploads(state, lease_expires_at);

CREATE TRIGGER IF NOT EXISTS trg_intake_upload_transition
BEFORE UPDATE OF state ON intake_uploads
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'receiving' AND NEW.state IN ('staged', 'rejected'))
    OR (OLD.state = 'staged' AND NEW.state IN (
        'completed', 'duplicate', 'rejected', 'reconciliation_required'
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid intake upload state transition');
END;

-- Backfill legacy job rows into runtime state. The temporary table limits the
-- mapping update to rows inserted by this migration if the script is replayed.
CREATE TEMP TABLE migration_008_new_job_ids (job_id TEXT PRIMARY KEY);
INSERT INTO migration_008_new_job_ids (job_id)
SELECT job.job_id FROM ingestion_jobs AS job
WHERE NOT EXISTS (
    SELECT 1 FROM ingestion_job_runtime AS runtime WHERE runtime.job_id = job.job_id
);

DROP TRIGGER IF EXISTS trg_ingestion_job_runtime_transition;
INSERT INTO ingestion_job_runtime (job_id)
SELECT job_id FROM migration_008_new_job_ids;

UPDATE ingestion_job_runtime
SET state = (
        SELECT CASE
            WHEN job.state IN ('running', 'cancel_requested')
                THEN 'reconciliation_required'
            ELSE job.state
        END
        FROM ingestion_jobs AS job
        WHERE job.job_id = ingestion_job_runtime.job_id
    ),
    attempt_count = (
        SELECT max(job.attempt_count, coalesce((
            SELECT max(attempt.attempt_number)
            FROM job_attempts AS attempt
            WHERE attempt.job_id = job.job_id
        ), 0))
        FROM ingestion_jobs AS job
        WHERE job.job_id = ingestion_job_runtime.job_id
    ),
    last_error_code = (
        SELECT CASE WHEN job.state IN ('running', 'cancel_requested')
            THEN 'legacy_job_requires_reconciliation' ELSE job.error_code END
        FROM ingestion_jobs AS job
        WHERE job.job_id = ingestion_job_runtime.job_id
    ),
    last_error_message = (
        SELECT CASE WHEN job.state IN ('running', 'cancel_requested')
            THEN 'Legacy active job had no trustworthy worker lease.'
            ELSE job.error_message END
        FROM ingestion_jobs AS job
        WHERE job.job_id = ingestion_job_runtime.job_id
    )
WHERE job_id IN (SELECT job_id FROM migration_008_new_job_ids);

-- Older public repository methods could update the legacy job row without its
-- runtime row. Preserve both records and route any disagreement to explicit
-- reconciliation instead of silently choosing one lifecycle as authoritative.
UPDATE ingestion_job_runtime
SET state = 'reconciliation_required',
    lease_owner = NULL,
    lease_token = NULL,
    lease_expires_at = NULL,
    last_error_code = 'legacy_runtime_state_mismatch',
    last_error_message = 'Legacy and runtime job states disagreed during migration.',
    updated_at = CURRENT_TIMESTAMP
WHERE job_id NOT IN (SELECT job_id FROM migration_008_new_job_ids)
  AND EXISTS (
      SELECT 1 FROM ingestion_jobs AS job
      WHERE job.job_id = ingestion_job_runtime.job_id
        AND NOT (
            (ingestion_job_runtime.state = 'reconciliation_required'
             AND job.state = 'failed_terminal')
            OR ingestion_job_runtime.state = job.state
        )
  );

-- Attempts are historical facts. Keep the maximum recorded in either legacy
-- job metadata, runtime metadata, or the attempt ledger.
UPDATE ingestion_job_runtime
SET attempt_count = max(
    attempt_count,
    coalesce((SELECT job.attempt_count FROM ingestion_jobs AS job
              WHERE job.job_id = ingestion_job_runtime.job_id), 0),
    coalesce((SELECT max(attempt.attempt_number) FROM job_attempts AS attempt
              WHERE attempt.job_id = ingestion_job_runtime.job_id), 0)
);

-- Keep the compatibility columns aligned with the runtime lifecycle. The
-- legacy schema represents reconciliation_required as failed_terminal.
UPDATE ingestion_jobs
SET state = (SELECT CASE WHEN runtime.state = 'reconciliation_required'
                         THEN 'failed_terminal' ELSE runtime.state END
             FROM ingestion_job_runtime AS runtime
             WHERE runtime.job_id = ingestion_jobs.job_id),
    attempt_count = (SELECT runtime.attempt_count FROM ingestion_job_runtime AS runtime
                     WHERE runtime.job_id = ingestion_jobs.job_id),
    lease_expires_at = (SELECT runtime.lease_expires_at
                        FROM ingestion_job_runtime AS runtime
                        WHERE runtime.job_id = ingestion_jobs.job_id),
    error_code = CASE WHEN (SELECT runtime.state FROM ingestion_job_runtime AS runtime
                            WHERE runtime.job_id = ingestion_jobs.job_id)
                           = 'reconciliation_required'
                      THEN coalesce(error_code, 'legacy_job_requires_reconciliation')
                      ELSE error_code END,
    error_message = CASE WHEN (SELECT runtime.state FROM ingestion_job_runtime AS runtime
                               WHERE runtime.job_id = ingestion_jobs.job_id)
                              = 'reconciliation_required'
                         THEN coalesce(error_message,
                              'Job requires explicit reconciliation after migration.')
                         ELSE error_message END,
    updated_at = CURRENT_TIMESTAMP
WHERE EXISTS (SELECT 1 FROM ingestion_job_runtime AS runtime
              WHERE runtime.job_id = ingestion_jobs.job_id);

DROP TABLE migration_008_new_job_ids;

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

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_legacy_state_consistent
BEFORE UPDATE OF state ON ingestion_jobs
WHEN EXISTS (
    SELECT 1 FROM ingestion_job_runtime AS runtime
    WHERE runtime.job_id = OLD.job_id
      AND NOT (
          NEW.state = runtime.state
          OR (runtime.state = 'reconciliation_required'
              AND NEW.state = 'failed_terminal')
      )
)
BEGIN
    SELECT RAISE(ABORT, 'legacy job state must match runtime lifecycle');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_legacy_attempts_consistent
BEFORE UPDATE OF attempt_count ON ingestion_jobs
WHEN EXISTS (
    SELECT 1 FROM ingestion_job_runtime AS runtime
    WHERE runtime.job_id = OLD.job_id AND NEW.attempt_count <> runtime.attempt_count
)
BEGIN
    SELECT RAISE(ABORT, 'legacy job attempt count must match runtime lifecycle');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_legacy_lease_consistent
BEFORE UPDATE OF lease_expires_at ON ingestion_jobs
WHEN EXISTS (
    SELECT 1 FROM ingestion_job_runtime AS runtime
    WHERE runtime.job_id = OLD.job_id
      AND NEW.lease_expires_at IS NOT runtime.lease_expires_at
)
BEGIN
    SELECT RAISE(ABORT, 'legacy job lease must match runtime lifecycle');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_runtime_no_delete
BEFORE DELETE ON ingestion_job_runtime
BEGIN
    SELECT RAISE(ABORT, 'job runtime lifecycle must be retained');
END;
