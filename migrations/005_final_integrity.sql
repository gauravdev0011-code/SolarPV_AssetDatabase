-- Preserve trustworthy completed promotions and quarantine ambiguous history.

-- Migration 004 inferred this flag for unfinished rows from the marker's state
-- at migration time. That cannot establish historical ordering. Rebuild the
-- transition and evidence guards while reconciling those rows below.
DROP TRIGGER IF EXISTS trg_promotion_transition;
DROP TRIGGER IF EXISTS trg_promotion_duplicate_evidence_immutable;

UPDATE promotion_operations
SET state = CASE
        WHEN state IN ('queued', 'running', 'retryable_failure', 'reconciliation_required')
            THEN 'reconciliation_required'
        WHEN state = 'succeeded'
         AND outcome = 'imported'
         AND result_json IS NOT NULL
         AND already_imported_before = 0
         AND CASE WHEN json_valid(result_json) = 1
                  THEN json_type(result_json) ELSE '' END = 'object'
         AND EXISTS (
             SELECT 1 FROM imported_files
             WHERE imported_files.source_hash = promotion_operations.source_hash
         ) THEN 'succeeded'
        WHEN state = 'succeeded'
         AND outcome = 'already_imported'
         AND result_json IS NOT NULL
         AND already_imported_before = 1
         AND CASE WHEN json_valid(result_json) = 1
                  THEN json_type(result_json) ELSE '' END = 'object'
         AND EXISTS (
             SELECT 1 FROM imported_files
             WHERE imported_files.source_hash = promotion_operations.source_hash
         ) THEN 'succeeded'
        WHEN state = 'succeeded' THEN 'reconciliation_required'
        ELSE state
    END,
    already_imported_before = CASE
        WHEN state = 'succeeded'
         AND outcome = 'already_imported'
         AND result_json IS NOT NULL
         AND already_imported_before = 1
         AND CASE WHEN json_valid(result_json) = 1
                  THEN json_type(result_json) ELSE '' END = 'object'
         AND EXISTS (
             SELECT 1 FROM imported_files
             WHERE imported_files.source_hash = promotion_operations.source_hash
         ) THEN 1
        ELSE 0
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

CREATE TRIGGER IF NOT EXISTS trg_promotion_transition
BEFORE UPDATE OF state ON promotion_operations
WHEN NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'queued' AND NEW.state IN (
        'running', 'cancelled', 'retryable_failure', 'terminal_failure',
        'reconciliation_required'
    ))
    OR (OLD.state = 'running' AND NEW.state IN (
        'succeeded', 'retryable_failure', 'terminal_failure',
        'reconciliation_required'
    ))
    OR (OLD.state = 'retryable_failure' AND NEW.state IN (
        'queued', 'reconciliation_required'
    ))
    OR (OLD.state = 'reconciliation_required' AND NEW.state IN (
        'succeeded', 'retryable_failure', 'terminal_failure'
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid promotion operation state transition');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_promotion_immutable
BEFORE UPDATE ON promotion_operations
WHEN OLD.state = 'succeeded'
 AND (
     NEW.promotion_id IS NOT OLD.promotion_id
     OR NEW.approval_id IS NOT OLD.approval_id
     OR NEW.source_hash IS NOT OLD.source_hash
     OR NEW.source_name IS NOT OLD.source_name
     OR NEW.state IS NOT OLD.state
     OR NEW.outcome IS NOT OLD.outcome
     OR NEW.result_json IS NOT OLD.result_json
     OR NEW.error_code IS NOT OLD.error_code
     OR NEW.error_message IS NOT OLD.error_message
     OR NEW.created_at IS NOT OLD.created_at
     OR NEW.dataset_id IS NOT OLD.dataset_id
     OR NEW.dataset_version_id IS NOT OLD.dataset_version_id
     OR NEW.request_fingerprint IS NOT OLD.request_fingerprint
     OR NEW.attempt_count IS NOT OLD.attempt_count
     OR NEW.already_imported_before IS NOT OLD.already_imported_before
 )
BEGIN
    SELECT RAISE(ABORT, 'successful promotion evidence is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_promotion_json
BEFORE UPDATE OF state, outcome, result_json ON promotion_operations
WHEN NEW.state = 'succeeded'
 AND CASE WHEN json_valid(NEW.result_json) = 1
          THEN json_type(NEW.result_json) ELSE '' END <> 'object'
BEGIN
    SELECT RAISE(ABORT, 'successful promotion result must be a JSON object');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_promotion_no_delete
BEFORE DELETE ON promotion_operations
WHEN OLD.state = 'succeeded'
BEGIN
    SELECT RAISE(ABORT, 'successful promotion evidence must be retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_promotion_no_replace
BEFORE INSERT ON promotion_operations
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE promotion_id = NEW.promotion_id AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'successful promotion evidence must be retained');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_domain_link_insert
BEFORE INSERT ON promotion_domain_records
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE promotion_id = NEW.promotion_id AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'successful promotion links are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_domain_link_update
BEFORE UPDATE ON promotion_domain_records
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE promotion_id = OLD.promotion_id AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'successful promotion links are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_domain_link_delete
BEFORE DELETE ON promotion_domain_records
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE promotion_id = OLD.promotion_id AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'successful promotion links are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_import_marker_immutable
BEFORE UPDATE ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE source_hash = OLD.source_hash AND state = 'succeeded'
)
BEGIN
    SELECT RAISE(ABORT, 'import marker referenced by a successful promotion is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_successful_import_marker_no_replace
BEFORE INSERT ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations
    WHERE source_hash = NEW.source_hash AND state = 'succeeded'
)
 AND EXISTS (
    SELECT 1 FROM imported_files WHERE source_hash = NEW.source_hash
 )
BEGIN
    SELECT RAISE(ABORT, 'import marker referenced by a successful promotion is immutable');
END;

DROP TRIGGER IF EXISTS trg_dataset_version_owner_immutable;
CREATE TRIGGER trg_dataset_version_owner_immutable
BEFORE UPDATE OF dataset_id ON dataset_versions
WHEN NEW.dataset_id <> OLD.dataset_id
 AND (
     EXISTS (SELECT 1 FROM dataset_sources
             WHERE dataset_version_id = OLD.dataset_version_id)
     OR EXISTS (SELECT 1 FROM approvals
                WHERE dataset_version_id = OLD.dataset_version_id)
     OR EXISTS (SELECT 1 FROM interpretations
                WHERE dataset_version_id = OLD.dataset_version_id)
     OR EXISTS (SELECT 1 FROM validation_runs
                WHERE dataset_version_id = OLD.dataset_version_id)
)
BEGIN
    SELECT RAISE(ABORT, 'dataset version owner cannot change after linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_source_fingerprint_immutable_after_linkage
BEFORE UPDATE OF sha256 ON source_files
WHEN NEW.sha256 <> OLD.sha256
 AND (
     EXISTS (SELECT 1 FROM ingestion_jobs
             WHERE source_file_id = OLD.source_file_id)
     OR EXISTS (SELECT 1 FROM extraction_artifacts
                WHERE source_file_id = OLD.source_file_id)
     OR EXISTS (SELECT 1 FROM dataset_sources
                WHERE source_file_id = OLD.source_file_id)
)
BEGIN
    SELECT RAISE(ABORT, 'source fingerprint cannot change after linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_source_location_artifact_immutable
BEFORE UPDATE OF artifact_id ON source_locations
WHEN NEW.artifact_id <> OLD.artifact_id
 AND (
     EXISTS (SELECT 1 FROM interpretations
             WHERE source_location_id = OLD.source_location_id)
     OR EXISTS (SELECT 1 FROM promotion_domain_records
                WHERE source_location_id = OLD.source_location_id)
)
BEGIN
    SELECT RAISE(ABORT, 'source location artifact cannot change after linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_interpretation_update
BEFORE UPDATE OF dataset_version_id, source_file_id, artifact_id ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM source_locations AS location
    JOIN interpretations AS interpretation
      ON interpretation.source_location_id = location.source_location_id
    WHERE location.artifact_id = OLD.artifact_id
      AND interpretation.dataset_version_id = OLD.dataset_version_id
      AND (
          NEW.dataset_version_id <> OLD.dataset_version_id
          OR NEW.source_file_id <> OLD.source_file_id
          OR NEW.artifact_id IS NOT OLD.artifact_id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'dataset source cannot change after interpretation linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_dataset_source_interpretation_delete
BEFORE DELETE ON dataset_sources
WHEN EXISTS (
    SELECT 1 FROM source_locations AS location
    JOIN interpretations AS interpretation
      ON interpretation.source_location_id = location.source_location_id
    WHERE location.artifact_id = OLD.artifact_id
      AND interpretation.dataset_version_id = OLD.dataset_version_id
)
BEGIN
    SELECT RAISE(ABORT, 'dataset source cannot be deleted after interpretation linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_validation_run_version_immutable
BEFORE UPDATE OF dataset_version_id ON validation_runs
WHEN NEW.dataset_version_id <> OLD.dataset_version_id
BEGIN
    SELECT RAISE(ABORT, 'validation run dataset version is immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_validation_run_outcome_immutable_after_approval
BEFORE UPDATE OF outcome ON validation_runs
WHEN NEW.outcome IS NOT OLD.outcome
 AND EXISTS (
     SELECT 1 FROM approvals
     WHERE validation_run_id = OLD.validation_run_id
 )
BEGIN
    SELECT RAISE(ABORT, 'validation outcome is immutable after approval linkage');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_relationship_insert
BEFORE INSERT ON approvals
WHEN NOT EXISTS (
    SELECT 1 FROM validation_runs AS validation
    WHERE validation.validation_run_id = NEW.validation_run_id
      AND validation.dataset_version_id = NEW.dataset_version_id
)
BEGIN
    SELECT RAISE(ABORT, 'approval version and validation run must agree');
END;

CREATE TRIGGER IF NOT EXISTS trg_approval_relationship_update
BEFORE UPDATE OF dataset_version_id, validation_run_id ON approvals
WHEN NOT EXISTS (
    SELECT 1 FROM validation_runs AS validation
    WHERE validation.validation_run_id = NEW.validation_run_id
      AND validation.dataset_version_id = NEW.dataset_version_id
)
BEGIN
    SELECT RAISE(ABORT, 'approval version and validation run must agree');
END;

-- The source_locations.artifact_id trigger above freezes a parent key once it
-- is used by an interpretation or promotion link. Dataset/validation parent
-- identity is similarly frozen after dependent rows exist. All historical
-- domain rows, markers, results, and links are retained during reconciliation.
