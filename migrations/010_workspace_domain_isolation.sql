-- Add workspace ownership to domain rows and make legacy external IDs
-- unique within, rather than across, workspaces. Existing records retain IDs
-- and are assigned to the only workspace that existed before migration 010.

ALTER TABLE workspaces ADD COLUMN description TEXT;
ALTER TABLE workspaces ADD COLUMN updated_at TEXT;
UPDATE workspaces SET updated_at = created_at WHERE updated_at IS NULL;

CREATE TABLE sites (
    site_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    location_text TEXT,
    latitude REAL,
    longitude REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((latitude IS NULL AND longitude IS NULL)
        OR (latitude IS NOT NULL AND longitude IS NOT NULL)),
    CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
    UNIQUE (workspace_id, site_id),
    UNIQUE (workspace_id, name)
);
CREATE INDEX idx_sites_workspace_name ON sites(workspace_id, name, site_id);

-- Imported content is workspace-specific. Preserve the historical marker as
-- a default-workspace marker and replace the former global hash primary key.
DROP TRIGGER IF EXISTS trg_promotion_duplicate_evidence_immutable;
DROP TRIGGER IF EXISTS trg_promotion_imported_evidence;
DROP TRIGGER IF EXISTS trg_promotion_success_consistent;
DROP TRIGGER IF EXISTS trg_promotion_success_evidence;
DROP TRIGGER IF EXISTS trg_successful_import_marker_immutable;
DROP TRIGGER IF EXISTS trg_successful_import_marker_no_replace;
DROP TRIGGER IF EXISTS trg_imported_marker_hash_immutable;
DROP TRIGGER IF EXISTS trg_imported_marker_referenced_immutable;
DROP TRIGGER IF EXISTS trg_promotion_domain_record_consistent;

CREATE TABLE imported_files_v10 (
    workspace_id TEXT NOT NULL DEFAULT 'default'
        REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    source_hash TEXT NOT NULL,
    source_name TEXT NOT NULL,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, source_hash)
);
INSERT INTO imported_files_v10(workspace_id, source_hash, source_name, imported_at)
SELECT 'default', source_hash, source_name, imported_at FROM imported_files;
DROP TABLE imported_files;
ALTER TABLE imported_files_v10 RENAME TO imported_files;
CREATE INDEX idx_imported_files_hash ON imported_files(source_hash, workspace_id);

CREATE TABLE assets_v10 (
    asset_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default'
        REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    site_id TEXT,
    external_id TEXT,
    asset_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    location TEXT,
    data_origin TEXT NOT NULL DEFAULT 'unknown'
        CHECK (data_origin IN ('real', 'synthetic', 'unknown')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(trim(asset_type)) > 0),
    CHECK (length(trim(name)) > 0),
    CHECK (external_id IS NULL OR length(trim(external_id)) > 0),
    UNIQUE (workspace_id, external_id),
    UNIQUE (workspace_id, asset_id),
    FOREIGN KEY (workspace_id, site_id)
        REFERENCES sites(workspace_id, site_id) ON DELETE RESTRICT
);
INSERT INTO assets_v10(
    asset_id, workspace_id, site_id, external_id, asset_type, name,
    description, location, data_origin, created_at, updated_at
)
SELECT asset_id, 'default', NULL, external_id, asset_type, name,
    description, location, 'unknown', created_at, updated_at FROM assets;

CREATE TABLE inspections_v10 (
    inspection_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default'
        REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    asset_id INTEGER NOT NULL,
    inspected_at TEXT NOT NULL,
    inspection_method TEXT,
    data_source TEXT,
    data_origin TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'completed',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(trim(inspected_at)) > 0),
    CHECK (data_origin IN ('real', 'synthetic', 'unknown')),
    CHECK (status IN ('planned', 'in_progress', 'completed', 'cancelled')),
    UNIQUE (workspace_id, inspection_id),
    FOREIGN KEY (workspace_id, asset_id)
        REFERENCES assets_v10(workspace_id, asset_id) ON DELETE RESTRICT
);
INSERT INTO inspections_v10(
    inspection_id, workspace_id, asset_id, inspected_at, inspection_method,
    data_source, data_origin, status, notes, created_at
)
SELECT inspection_id, 'default', asset_id, inspected_at, inspection_method,
       data_source, data_origin, status, notes, created_at FROM inspections;

CREATE TABLE observations_v10 (
    observation_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default'
        REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    inspection_id INTEGER NOT NULL,
    observation_type TEXT NOT NULL,
    value REAL,
    unit TEXT,
    finding TEXT,
    data_quality TEXT NOT NULL DEFAULT 'unknown',
    source_reference TEXT,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(trim(observation_type)) > 0),
    CHECK (data_quality IN ('good', 'suspect', 'invalid', 'unknown')),
    CHECK (value IS NOT NULL OR finding IS NOT NULL),
    FOREIGN KEY (workspace_id, inspection_id)
        REFERENCES inspections_v10(workspace_id, inspection_id) ON DELETE RESTRICT
);
INSERT INTO observations_v10(
    observation_id, workspace_id, inspection_id, observation_type, value,
    unit, finding, data_quality, source_reference, recorded_at
)
SELECT observation_id, 'default', inspection_id, observation_type, value,
       unit, finding, data_quality, source_reference, recorded_at FROM observations;

CREATE TABLE maintenance_records_v10 (
    maintenance_id INTEGER PRIMARY KEY,
    workspace_id TEXT NOT NULL DEFAULT 'default'
        REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    asset_id INTEGER NOT NULL,
    performed_at TEXT,
    maintenance_type TEXT NOT NULL,
    description TEXT NOT NULL,
    performed_by TEXT,
    data_origin TEXT NOT NULL DEFAULT 'unknown',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(trim(maintenance_type)) > 0),
    CHECK (length(trim(description)) > 0),
    CHECK (data_origin IN ('real', 'synthetic', 'unknown')),
    FOREIGN KEY (workspace_id, asset_id)
        REFERENCES assets_v10(workspace_id, asset_id) ON DELETE RESTRICT
);
INSERT INTO maintenance_records_v10(
    maintenance_id, workspace_id, asset_id, performed_at, maintenance_type,
    description, performed_by, data_origin, created_at
)
SELECT maintenance_id, 'default', asset_id, performed_at, maintenance_type,
       description, performed_by, data_origin, created_at FROM maintenance_records;

-- Drop children first so the migration can replace the parent table without
-- disabling foreign-key enforcement. IDs and historical values are copied.
DROP TABLE observations;
DROP TABLE inspections;
DROP TABLE maintenance_records;
DROP TABLE assets;
ALTER TABLE assets_v10 RENAME TO assets;
ALTER TABLE inspections_v10 RENAME TO inspections;
ALTER TABLE observations_v10 RENAME TO observations;
ALTER TABLE maintenance_records_v10 RENAME TO maintenance_records;

CREATE INDEX idx_assets_type ON assets(asset_type);
CREATE INDEX idx_assets_workspace ON assets(workspace_id, asset_id);
CREATE INDEX idx_assets_workspace_external ON assets(workspace_id, external_id);
CREATE INDEX idx_inspections_asset_date ON inspections(asset_id, inspected_at);
CREATE INDEX idx_inspections_workspace_date
    ON inspections(workspace_id, inspected_at, inspection_id);
CREATE INDEX idx_observations_inspection ON observations(inspection_id);
CREATE INDEX idx_observations_type ON observations(observation_type);
CREATE INDEX idx_observations_workspace_type_date
    ON observations(workspace_id, observation_type, recorded_at, observation_id);
CREATE INDEX idx_maintenance_asset_date
    ON maintenance_records(asset_id, performed_at);
CREATE INDEX idx_maintenance_workspace_date
    ON maintenance_records(workspace_id, performed_at, maintenance_id);
CREATE INDEX idx_datasets_workspace ON datasets(workspace_id, created_at, dataset_id);
CREATE INDEX idx_sources_workspace ON source_files(workspace_id, created_at, source_file_id);
CREATE INDEX idx_jobs_workspace ON ingestion_jobs(workspace_id, created_at, job_id);

-- Migration 009's registration guard was written for its then-only default
-- workspace. Replace that policy with matching workspace ownership checks.
DROP TRIGGER IF EXISTS trg_dataset_registration_request_matches_version;
CREATE TRIGGER trg_dataset_registration_request_matches_version
BEFORE INSERT ON dataset_version_requests
WHEN NOT EXISTS (
    SELECT 1 FROM datasets dataset
    JOIN dataset_versions version ON version.dataset_id = dataset.dataset_id
    JOIN ingestion_jobs job ON job.job_id = NEW.job_id
    JOIN source_files file ON file.source_file_id = job.source_file_id
    WHERE dataset.dataset_id = NEW.dataset_id
      AND dataset.workspace_id = NEW.workspace_id
      AND version.dataset_version_id = NEW.dataset_version_id
      AND version.source_job_id = NEW.job_id
      AND job.workspace_id = NEW.workspace_id
      AND file.workspace_id = NEW.workspace_id
)
BEGIN
    SELECT RAISE(ABORT, 'dataset registration request does not match its workspace version');
END;

CREATE TRIGGER trg_domain_workspace_immutable_assets
BEFORE UPDATE OF workspace_id ON assets
WHEN NEW.workspace_id IS NOT OLD.workspace_id
BEGIN SELECT RAISE(ABORT, 'asset workspace ownership is immutable'); END;
CREATE TRIGGER trg_domain_workspace_immutable_inspections
BEFORE UPDATE OF workspace_id ON inspections
WHEN NEW.workspace_id IS NOT OLD.workspace_id
BEGIN SELECT RAISE(ABORT, 'inspection workspace ownership is immutable'); END;
CREATE TRIGGER trg_domain_workspace_immutable_observations
BEFORE UPDATE OF workspace_id ON observations
WHEN NEW.workspace_id IS NOT OLD.workspace_id
BEGIN SELECT RAISE(ABORT, 'observation workspace ownership is immutable'); END;
CREATE TRIGGER trg_domain_workspace_immutable_maintenance
BEFORE UPDATE OF workspace_id ON maintenance_records
WHEN NEW.workspace_id IS NOT OLD.workspace_id
BEGIN SELECT RAISE(ABORT, 'maintenance workspace ownership is immutable'); END;
CREATE TRIGGER trg_site_workspace_immutable
BEFORE UPDATE OF workspace_id ON sites
WHEN NEW.workspace_id IS NOT OLD.workspace_id
BEGIN SELECT RAISE(ABORT, 'site workspace ownership is immutable'); END;

-- Promotion result evidence is scoped through its approved dataset workspace.
CREATE TRIGGER trg_promotion_imported_evidence
BEFORE INSERT ON promotion_operations
WHEN NEW.already_imported_before <> CASE WHEN EXISTS (
    SELECT 1 FROM imported_files marker
    JOIN datasets dataset ON dataset.workspace_id = marker.workspace_id
    WHERE dataset.dataset_id = NEW.dataset_id
      AND marker.source_hash = NEW.source_hash
) THEN 1 ELSE 0 END
BEGIN SELECT RAISE(ABORT, 'promotion duplicate evidence must match its workspace marker'); END;

CREATE TRIGGER trg_promotion_duplicate_evidence_immutable
BEFORE UPDATE OF already_imported_before ON promotion_operations
WHEN NEW.already_imported_before < OLD.already_imported_before
 OR (OLD.already_imported_before = 0 AND NEW.already_imported_before = 1
     AND (OLD.state <> 'queued' OR NOT EXISTS (
         SELECT 1 FROM imported_files marker
         JOIN datasets dataset ON dataset.workspace_id = marker.workspace_id
         WHERE dataset.dataset_id = OLD.dataset_id
           AND marker.source_hash = OLD.source_hash
     )))
BEGIN SELECT RAISE(ABORT, 'promotion duplicate evidence cannot be changed'); END;

CREATE TRIGGER trg_promotion_success_consistent
BEFORE UPDATE OF state, outcome, result_json ON promotion_operations
WHEN NEW.state = 'succeeded'
 AND (NEW.outcome IS NULL OR NEW.result_json IS NULL
      OR NOT EXISTS (
          SELECT 1 FROM imported_files marker
          JOIN datasets dataset ON dataset.workspace_id = marker.workspace_id
          WHERE dataset.dataset_id = NEW.dataset_id
            AND marker.source_hash = NEW.source_hash
      ))
BEGIN SELECT RAISE(ABORT, 'successful promotion requires a workspace import marker'); END;

CREATE TRIGGER trg_promotion_success_evidence
BEFORE UPDATE OF state, outcome, result_json ON promotion_operations
WHEN NEW.state = 'succeeded'
 AND (NOT EXISTS (
         SELECT 1 FROM imported_files marker
         JOIN datasets dataset ON dataset.workspace_id = marker.workspace_id
         WHERE dataset.dataset_id = NEW.dataset_id
           AND marker.source_hash = NEW.source_hash
     )
     OR (NEW.outcome = 'imported' AND NEW.already_imported_before <> 0)
     OR (NEW.outcome = 'already_imported' AND NEW.already_imported_before <> 1))
BEGIN SELECT RAISE(ABORT, 'promotion success lacks matching workspace import evidence'); END;

CREATE TRIGGER trg_successful_import_marker_immutable
BEFORE UPDATE ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE dataset.workspace_id = OLD.workspace_id
      AND operation.source_hash = OLD.source_hash AND operation.state = 'succeeded'
)
BEGIN SELECT RAISE(ABORT, 'import marker referenced by a successful promotion is immutable'); END;

CREATE TRIGGER trg_successful_import_marker_no_replace
BEFORE INSERT ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE dataset.workspace_id = NEW.workspace_id
      AND operation.source_hash = NEW.source_hash AND operation.state = 'succeeded'
)
 AND EXISTS (SELECT 1 FROM imported_files marker
             WHERE marker.workspace_id = NEW.workspace_id
               AND marker.source_hash = NEW.source_hash)
BEGIN SELECT RAISE(ABORT, 'import marker referenced by a successful promotion is immutable'); END;

CREATE TRIGGER trg_imported_marker_hash_immutable
BEFORE UPDATE OF workspace_id, source_hash ON imported_files
WHEN (NEW.workspace_id IS NOT OLD.workspace_id OR NEW.source_hash IS NOT OLD.source_hash)
 AND EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE dataset.workspace_id = OLD.workspace_id
      AND operation.source_hash = OLD.source_hash
)
BEGIN SELECT RAISE(ABORT, 'import marker identity is referenced by a promotion'); END;

CREATE TRIGGER trg_imported_marker_referenced_immutable
BEFORE DELETE ON imported_files
WHEN EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE dataset.workspace_id = OLD.workspace_id
      AND operation.source_hash = OLD.source_hash AND operation.state = 'succeeded'
)
BEGIN SELECT RAISE(ABORT, 'import marker is required by a successful promotion'); END;

CREATE TRIGGER trg_promotion_domain_record_workspace
BEFORE INSERT ON promotion_domain_records
WHEN NOT EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE operation.promotion_id = NEW.promotion_id
      AND ((NEW.record_type = 'asset' AND EXISTS (
                SELECT 1 FROM assets WHERE asset_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'inspection' AND EXISTS (
                SELECT 1 FROM inspections WHERE inspection_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'observation' AND EXISTS (
                SELECT 1 FROM observations WHERE observation_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'maintenance' AND EXISTS (
                SELECT 1 FROM maintenance_records WHERE maintenance_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id)))
)
BEGIN SELECT RAISE(ABORT, 'promotion record and dataset workspaces must match'); END;

CREATE TRIGGER trg_promotion_domain_record_workspace_update
BEFORE UPDATE ON promotion_domain_records
WHEN NOT EXISTS (
    SELECT 1 FROM promotion_operations operation
    JOIN datasets dataset ON dataset.dataset_id = operation.dataset_id
    WHERE operation.promotion_id = NEW.promotion_id
      AND ((NEW.record_type = 'asset' AND EXISTS (
                SELECT 1 FROM assets WHERE asset_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'inspection' AND EXISTS (
                SELECT 1 FROM inspections WHERE inspection_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'observation' AND EXISTS (
                SELECT 1 FROM observations WHERE observation_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id))
        OR (NEW.record_type = 'maintenance' AND EXISTS (
                SELECT 1 FROM maintenance_records WHERE maintenance_id = NEW.record_id
                  AND workspace_id = dataset.workspace_id)))
)
BEGIN SELECT RAISE(ABORT, 'promotion record and dataset workspaces must match'); END;

CREATE TRIGGER trg_promotion_domain_record_consistent
BEFORE INSERT ON promotion_domain_records
WHEN NOT (
     (NEW.record_type = 'asset' AND EXISTS (SELECT 1 FROM assets WHERE asset_id = NEW.record_id))
  OR (NEW.record_type = 'inspection' AND EXISTS (SELECT 1 FROM inspections WHERE inspection_id = NEW.record_id))
  OR (NEW.record_type = 'observation' AND EXISTS (SELECT 1 FROM observations WHERE observation_id = NEW.record_id))
  OR (NEW.record_type = 'maintenance' AND EXISTS (SELECT 1 FROM maintenance_records WHERE maintenance_id = NEW.record_id))
)
 OR NOT EXISTS (
     SELECT 1 FROM promotion_operations AS operation
     JOIN approvals AS approval ON approval.approval_id = operation.approval_id
     JOIN datasets AS dataset ON dataset.dataset_id = operation.dataset_id
     JOIN dataset_sources AS source ON source.dataset_version_id = approval.dataset_version_id
     JOIN source_files AS file ON file.source_file_id = source.source_file_id
     WHERE operation.promotion_id = NEW.promotion_id
       AND operation.state = 'running'
       AND dataset.workspace_id = file.workspace_id
       AND file.sha256 = operation.source_hash
       AND (
           NEW.source_location_id IS NULL
           OR EXISTS (
               SELECT 1 FROM source_locations AS location
               JOIN extraction_artifacts AS artifact ON artifact.artifact_id = location.artifact_id
               WHERE location.source_location_id = NEW.source_location_id
                 AND artifact.source_file_id = file.source_file_id
           )
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'promotion domain record lineage is invalid');
END;
