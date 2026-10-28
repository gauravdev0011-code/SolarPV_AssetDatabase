
-- Initial database schema for the Solar PV Asset Database.
-- All timestamps are stored as ISO 8601 text.
-- Real and synthetic research data must be clearly distinguished.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS assets (
    asset_id INTEGER PRIMARY KEY,
    external_id TEXT UNIQUE,
    asset_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    location TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (length(trim(asset_type)) > 0),
    CHECK (length(trim(name)) > 0),
    CHECK (external_id IS NULL OR length(trim(external_id)) > 0)
);

CREATE TABLE IF NOT EXISTS inspections (
    inspection_id INTEGER PRIMARY KEY,
    asset_id INTEGER NOT NULL,
    inspected_at TEXT NOT NULL,
    inspection_method TEXT,
    data_source TEXT,
    data_origin TEXT NOT NULL DEFAULT 'unknown',
    status TEXT NOT NULL DEFAULT 'completed',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
        ON DELETE RESTRICT,
    CHECK (length(trim(inspected_at)) > 0),
    CHECK (data_origin IN ('real', 'synthetic', 'unknown')),
    CHECK (status IN ('planned', 'in_progress', 'completed', 'cancelled'))
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id INTEGER PRIMARY KEY,
    inspection_id INTEGER NOT NULL,
    observation_type TEXT NOT NULL,
    value REAL,
    unit TEXT,
    finding TEXT,
    data_quality TEXT NOT NULL DEFAULT 'unknown',
    source_reference TEXT,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (inspection_id) REFERENCES inspections(inspection_id)
        ON DELETE RESTRICT,
    CHECK (length(trim(observation_type)) > 0),
    CHECK (data_quality IN ('good', 'suspect', 'invalid', 'unknown')),
    CHECK (
        value IS NOT NULL
        OR finding IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS maintenance_records (
    maintenance_id INTEGER PRIMARY KEY,
    asset_id INTEGER NOT NULL,
    performed_at TEXT,
    maintenance_type TEXT NOT NULL,
    description TEXT NOT NULL,
    performed_by TEXT,
    data_origin TEXT NOT NULL DEFAULT 'unknown',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
        ON DELETE RESTRICT,
    CHECK (length(trim(maintenance_type)) > 0),
    CHECK (length(trim(description)) > 0),
    CHECK (data_origin IN ('real', 'synthetic', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_assets_type
    ON assets(asset_type);

CREATE INDEX IF NOT EXISTS idx_inspections_asset_date
    ON inspections(asset_id, inspected_at);

CREATE INDEX IF NOT EXISTS idx_observations_inspection
    ON observations(inspection_id);

CREATE INDEX IF NOT EXISTS idx_observations_type
    ON observations(observation_type);

CREATE INDEX IF NOT EXISTS idx_maintenance_asset_date
    ON maintenance_records(asset_id, performed_at);
