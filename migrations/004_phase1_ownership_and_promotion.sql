-- Tighten ownership and promotion evidence without rewriting earlier migrations.

ALTER TABLE promotion_operations ADD COLUMN already_imported_before INTEGER
    NOT NULL DEFAULT 0 CHECK (already_imported_before IN (0, 1));

-- Existing successful duplicate outcomes are known to have observed a prior
-- imported_files row. For unfinished legacy operations, a present marker is
-- treated as pre-existing evidence at migration time.
UPDATE promotion_operations
SET already_imported_before = CASE
    WHEN outcome = 'already_imported' THEN 1
    WHEN state IN ('queued', 'running', 'retryable_failure', 'reconciliation_required')
         AND EXISTS (
             SELECT 1 FROM imported_files
             WHERE imported_files.source_hash = promotion_operations.source_hash
         ) THEN 1
    ELSE 0
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_insert_pending
BEFORE INSERT ON approvals
WHEN NEW.state <> 'pending'
  OR NEW.decision_by IS NOT NULL
  OR NEW.decision_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'new approvals must start pending without a decision');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_workspace_insert
BEFORE INSERT ON ingestion_jobs
WHEN NOT EXISTS (
    SELECT 1 FROM source_files AS file
    WHERE file.source_file_id = NEW.source_file_id
      AND file.workspace_id = NEW.workspace_id
)
BEGIN
    SELECT RAISE(ABORT, 'job and source file must belong to the same workspace');
END;

CREATE TRIGGER IF NOT EXISTS trg_ingestion_job_workspace_update
BEFORE UPDATE OF workspace_id, source_file_id ON ingestion_jobs
WHEN NOT EXISTS (
    SELECT 1 FROM source_files AS file
    WHERE file.source_file_id = NEW.source_file_id
      AND file.workspace_id = NEW.workspace_id
)
 OR EXISTS (
    SELECT 1 FROM extraction_artifacts AS artifact
    WHERE artifact.job_id = OLD.job_id
      AND (artifact.workspace_id <> NEW.workspace_id
           OR artifact.source_file_id <> NEW.source_file_id)
)
BEGIN
    SELECT RAISE(ABORT, 'job ownership cannot invalidate its source or artifacts');
END;

CREATE TRIGGER IF NOT EXISTS trg_source_file_workspace_update
BEFORE UPDATE OF workspace_id ON source_files
WHEN NEW.workspace_id <> OLD.workspace_id
 AND (
     EXISTS (SELECT 1 FROM ingestion_jobs AS job
             WHERE job.source_file_id = OLD.source_file_id
               AND job.workspace_id <> NEW.workspace_id)
     OR EXISTS (SELECT 1 FROM extraction_artifacts AS artifact
                WHERE artifact.source_file_id = OLD.source_file_id
                  AND artifact.workspace_id <> NEW.workspace_id)
     OR EXISTS (
         SELECT 1 FROM dataset_sources AS source
         JOIN dataset_versions AS version
           ON version.dataset_version_id = source.dataset_version_id
         JOIN datasets AS dataset ON dataset.dataset_id = version.dataset_id
         WHERE source.source_file_id = OLD.source_file_id
           AND dataset.workspace_id <> NEW.workspace_id
     )
)
BEGIN
    SELECT RAISE(ABORT, 'source workspace change would invalidate linked records');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_workspace_update
BEFORE UPDATE OF workspace_id ON datasets
WHEN NEW.workspace_id <> OLD.workspace_id
 AND (
     EXISTS (
         SELECT 1 FROM dataset_versions AS version
         JOIN dataset_sources AS source
           ON source.dataset_version_id = version.dataset_version_id
         JOIN source_files AS file ON file.source_file_id = source.source_file_id
         WHERE version.dataset_id = OLD.dataset_id
           AND file.workspace_id <> NEW.workspace_id
     )
     OR EXISTS (
         SELECT 1 FROM dataset_versions AS version
         JOIN interpretations AS interpretation
           ON interpretation.dataset_version_id = version.dataset_version_id
         WHERE version.dataset_id = OLD.dataset_id
           AND interpretation.workspace_id <> NEW.workspace_id
     )
)
BEGIN
    SELECT RAISE(ABORT, 'dataset workspace change would invalidate linked records');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_version_owner_immutable
BEFORE UPDATE OF dataset_id ON dataset_versions
WHEN NEW.dataset_id <> OLD.dataset_id
 AND (
     EXISTS (SELECT 1 FROM dataset_sources
             WHERE dataset_version_id = OLD.dataset_version_id)
     OR EXISTS (SELECT 1 FROM approvals
                WHERE dataset_version_id = OLD.dataset_version_id)
)
BEGIN
    SELECT RAISE(ABORT, 'dataset version owner cannot change after linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_artifact_lineage_immutable
BEFORE UPDATE OF workspace_id, source_file_id, job_id ON extraction_artifacts
WHEN EXISTS (
    SELECT 1 FROM dataset_sources
    WHERE artifact_id = OLD.artifact_id
      AND (source_file_id <> NEW.source_file_id)
)
 OR EXISTS (
    SELECT 1 FROM source_locations AS location
    JOIN interpretations AS interpretation
      ON interpretation.source_location_id = location.source_location_id
    WHERE location.artifact_id = OLD.artifact_id
      AND interpretation.workspace_id <> NEW.workspace_id
)
BEGIN
    SELECT RAISE(ABORT, 'artifact update would invalidate source lineage');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_imported_evidence
BEFORE INSERT ON promotion_operations
WHEN NEW.already_imported_before <> CASE WHEN EXISTS (
    SELECT 1 FROM imported_files WHERE source_hash = NEW.source_hash
) THEN 1 ELSE 0 END
BEGIN
    SELECT RAISE(ABORT, 'promotion duplicate evidence must match the import marker');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_duplicate_evidence_immutable
BEFORE UPDATE OF already_imported_before ON promotion_operations
WHEN NEW.already_imported_before < OLD.already_imported_before
  OR (OLD.already_imported_before = 0 AND NEW.already_imported_before = 1
      AND (OLD.state <> 'queued' OR NOT EXISTS (
          SELECT 1 FROM imported_files WHERE source_hash = OLD.source_hash
      )))
BEGIN
    SELECT RAISE(ABORT, 'promotion duplicate evidence cannot be changed');
END;

CREATE TRIGGER IF NOT EXISTS trg_promotion_success_evidence
BEFORE UPDATE OF state, outcome, result_json ON promotion_operations
WHEN NEW.state = 'succeeded'
 AND (
     NOT EXISTS (
         SELECT 1 FROM imported_files WHERE source_hash = NEW.source_hash
     )
     OR (NEW.outcome = 'imported' AND NEW.already_imported_before <> 0)
     OR (NEW.outcome = 'already_imported' AND NEW.already_imported_before <> 1)
 )
BEGIN
    SELECT RAISE(ABORT, 'promotion success lacks matching import evidence');
END;

CREATE TRIGGER IF NOT EXISTS trg_imported_marker_referenced_immutable
BEFORE DELETE ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE source_hash = OLD.source_hash AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'import marker is required by a successful promotion');
END;

CREATE TRIGGER IF NOT EXISTS trg_imported_marker_hash_immutable
BEFORE UPDATE OF source_hash ON imported_files
WHEN NEW.source_hash <> OLD.source_hash
 AND EXISTS (
     SELECT 1 FROM promotion_operations WHERE source_hash = OLD.source_hash
 )
BEGIN
    SELECT RAISE(ABORT, 'import marker hash is referenced by a promotion');
END;

-- Domain tables have no source-file/provenance columns. These links prove that
-- a target record exists and was linked while promotion ran, but cannot prove
-- that arbitrary pre-existing domain rows originated from this source.
