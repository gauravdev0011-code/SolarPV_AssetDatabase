-- Validate stored promotion results and freeze artifact source identity once
-- source locations become interpreted or promoted provenance.

-- Migration 005 can recognize valid JSON syntax but cannot establish that the
-- object describes its promotion. Quarantine completed rows with inconsistent
-- result identity while retaining their result, links, marker, and domain rows.
DROP TRIGGER IF EXISTS trg_promotion_transition;
DROP TRIGGER IF EXISTS trg_successful_promotion_immutable;
DROP TRIGGER IF EXISTS trg_successful_promotion_json;

UPDATE promotion_operations
SET state = 'reconciliation_required'
WHERE state = 'succeeded'
  AND CASE
      WHEN json_valid(result_json) = 1
      THEN CASE WHEN json_type(result_json) = 'object' THEN CASE WHEN (
          json_type(result_json, '$.promotion_id') = 'text'
          AND json_extract(result_json, '$.promotion_id') = promotion_id
          AND json_type(result_json, '$.outcome') = 'text'
          AND json_extract(result_json, '$.outcome') = outcome
          AND json_type(result_json, '$.source_name') = 'text'
          AND json_extract(result_json, '$.source_name') = source_name
          AND json_type(result_json, '$.assets') = 'integer'
          AND CAST(json_extract(result_json, '$.assets') AS INTEGER) >= 0
          AND json_type(result_json, '$.inspections') = 'integer'
          AND CAST(json_extract(result_json, '$.inspections') AS INTEGER) >= 0
          AND json_type(result_json, '$.observations') = 'integer'
          AND CAST(json_extract(result_json, '$.observations') AS INTEGER) >= 0
          AND json_type(result_json, '$.maintenance') = 'integer'
          AND CAST(json_extract(result_json, '$.maintenance') AS INTEGER) >= 0
          AND json_type(result_json, '$.message') = 'text'
          AND (
              (outcome = 'imported'
               AND already_imported_before = 0
               AND json_extract(result_json, '$.status') = 'imported'
               AND json_extract(result_json, '$.message') = 'Import completed successfully.'
               AND CAST(json_extract(result_json, '$.assets') AS INTEGER) = (
                   SELECT COUNT(*) FROM promotion_domain_records
                   WHERE promotion_id = promotion_operations.promotion_id
                     AND record_type = 'asset'
               )
               AND CAST(json_extract(result_json, '$.inspections') AS INTEGER) = (
                   SELECT COUNT(*) FROM promotion_domain_records
                   WHERE promotion_id = promotion_operations.promotion_id
                     AND record_type = 'inspection'
               )
               AND CAST(json_extract(result_json, '$.observations') AS INTEGER) = (
                   SELECT COUNT(*) FROM promotion_domain_records
                   WHERE promotion_id = promotion_operations.promotion_id
                     AND record_type = 'observation'
               )
               AND CAST(json_extract(result_json, '$.maintenance') AS INTEGER) = (
                   SELECT COUNT(*) FROM promotion_domain_records
                   WHERE promotion_id = promotion_operations.promotion_id
                     AND record_type = 'maintenance'
               ))
              OR
              (outcome = 'already_imported'
               AND already_imported_before = 1
               AND json_extract(result_json, '$.status') = 'skipped'
               AND json_extract(result_json, '$.message') = 'Identical source content has already been imported.'
               AND json_extract(result_json, '$.assets') = 0
               AND json_extract(result_json, '$.inspections') = 0
               AND json_extract(result_json, '$.observations') = 0
               AND json_extract(result_json, '$.maintenance') = 0
               AND NOT EXISTS (
                   SELECT 1 FROM promotion_domain_records
                   WHERE promotion_id = promotion_operations.promotion_id
               ))
          )
      ) THEN 0 ELSE 1 END ELSE 1 END
      ELSE 1
  END = 1;

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
 AND CASE
     WHEN json_valid(NEW.result_json) = 1
     THEN CASE WHEN json_type(NEW.result_json) = 'object' THEN CASE WHEN (
         json_type(NEW.result_json, '$.promotion_id') = 'text'
         AND json_extract(NEW.result_json, '$.promotion_id') = NEW.promotion_id
         AND json_type(NEW.result_json, '$.outcome') = 'text'
         AND json_extract(NEW.result_json, '$.outcome') = NEW.outcome
         AND json_type(NEW.result_json, '$.source_name') = 'text'
         AND json_extract(NEW.result_json, '$.source_name') = NEW.source_name
         AND json_type(NEW.result_json, '$.assets') = 'integer'
         AND CAST(json_extract(NEW.result_json, '$.assets') AS INTEGER) >= 0
         AND json_type(NEW.result_json, '$.inspections') = 'integer'
         AND CAST(json_extract(NEW.result_json, '$.inspections') AS INTEGER) >= 0
         AND json_type(NEW.result_json, '$.observations') = 'integer'
         AND CAST(json_extract(NEW.result_json, '$.observations') AS INTEGER) >= 0
         AND json_type(NEW.result_json, '$.maintenance') = 'integer'
         AND CAST(json_extract(NEW.result_json, '$.maintenance') AS INTEGER) >= 0
         AND json_type(NEW.result_json, '$.message') = 'text'
         AND (
             (NEW.outcome = 'imported'
              AND NEW.already_imported_before = 0
              AND json_extract(NEW.result_json, '$.status') = 'imported'
              AND json_extract(NEW.result_json, '$.message') = 'Import completed successfully.'
              AND CAST(json_extract(NEW.result_json, '$.assets') AS INTEGER) = (
                  SELECT COUNT(*) FROM promotion_domain_records
                  WHERE promotion_id = NEW.promotion_id AND record_type = 'asset'
              )
              AND CAST(json_extract(NEW.result_json, '$.inspections') AS INTEGER) = (
                  SELECT COUNT(*) FROM promotion_domain_records
                  WHERE promotion_id = NEW.promotion_id AND record_type = 'inspection'
              )
              AND CAST(json_extract(NEW.result_json, '$.observations') AS INTEGER) = (
                  SELECT COUNT(*) FROM promotion_domain_records
                  WHERE promotion_id = NEW.promotion_id AND record_type = 'observation'
              )
              AND CAST(json_extract(NEW.result_json, '$.maintenance') AS INTEGER) = (
                  SELECT COUNT(*) FROM promotion_domain_records
                  WHERE promotion_id = NEW.promotion_id AND record_type = 'maintenance'
              ))
             OR
             (NEW.outcome = 'already_imported'
              AND NEW.already_imported_before = 1
              AND json_extract(NEW.result_json, '$.status') = 'skipped'
              AND json_extract(NEW.result_json, '$.message') = 'Identical source content has already been imported.'
              AND json_extract(NEW.result_json, '$.assets') = 0
              AND json_extract(NEW.result_json, '$.inspections') = 0
              AND json_extract(NEW.result_json, '$.observations') = 0
              AND json_extract(NEW.result_json, '$.maintenance') = 0
              AND NOT EXISTS (
                  SELECT 1 FROM promotion_domain_records
                  WHERE promotion_id = NEW.promotion_id
              ))
         )
     ) THEN 0 ELSE 1 END ELSE 1 END ELSE 1 END
BEGIN
    SELECT RAISE(ABORT, 'successful promotion result does not match its operation');
END;

CREATE TRIGGER IF NOT EXISTS trg_artifact_source_immutable_after_lineage
BEFORE UPDATE OF source_file_id ON extraction_artifacts
WHEN NEW.source_file_id IS NOT OLD.source_file_id
 AND EXISTS (
     SELECT 1 FROM source_locations AS location
     WHERE location.artifact_id = OLD.artifact_id
       AND (
           EXISTS (
               SELECT 1 FROM interpretations AS interpretation
               WHERE interpretation.source_location_id = location.source_location_id
           )
           OR EXISTS (
               SELECT 1 FROM promotion_domain_records AS link
               WHERE link.source_location_id = location.source_location_id
           )
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'artifact source cannot change after lineage linkage');
END;
