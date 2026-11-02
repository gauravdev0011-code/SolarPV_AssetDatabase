"""Bounded, workspace-scoped read models for dashboard API responses."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from uuid import uuid4

from solarpv_db.database import DEFAULT_DATABASE, get_connection
from solarpv_db.workspaces import WorkspaceError, WorkspaceNotFound


MAX_PAGE_SIZE = 100
MAX_OFFSET = 10_000_000
MAX_DATE_WINDOW_DAYS = 3650


class DashboardError(ValueError):
    """Invalid dashboard query or site request."""


class DashboardNotFound(KeyError):
    """Requested workspace resource is absent from the selected workspace."""


class DashboardService:
    def __init__(self, database_path: str | Path = DEFAULT_DATABASE) -> None:
        self.database_path = database_path

    def overview(self, workspace_id: str, *, start: str | None = None,
                 end: str | None = None) -> dict:
        start, end = _date_window(start, end)
        bounds_sql, bounds = _date_predicate("inspected_at", start, end)
        maintenance_sql, maintenance_bounds = _date_predicate("performed_at", start, end)
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            assets = connection.execute(
                "SELECT COUNT(*) FROM assets WHERE workspace_id = ? AND data_origin <> 'synthetic'",
                (workspace_id,),
            ).fetchone()[0]
            sample_assets = connection.execute(
                "SELECT COUNT(*) FROM assets WHERE workspace_id = ? AND data_origin = 'synthetic'",
                (workspace_id,),
            ).fetchone()[0]
            inspections = connection.execute(
                f"""SELECT COUNT(*) FROM inspections
                    WHERE workspace_id = ? AND data_origin <> 'synthetic' {bounds_sql}""",
                (workspace_id, *bounds),
            ).fetchone()[0]
            inspected_assets = connection.execute(
                f"""SELECT COUNT(DISTINCT asset_id) FROM inspections
                    WHERE workspace_id = ? AND data_origin <> 'synthetic'
                      AND status = 'completed' {bounds_sql}""",
                (workspace_id, *bounds),
            ).fetchone()[0]
            observations = connection.execute(
                f"""SELECT COUNT(*) FROM observations observation
                    JOIN inspections inspection USING (workspace_id, inspection_id)
                    JOIN assets asset ON asset.workspace_id = inspection.workspace_id
                                     AND asset.asset_id = inspection.asset_id
                    WHERE observation.workspace_id = ?
                      AND inspection.data_origin <> 'synthetic'
                      AND asset.data_origin <> 'synthetic'
                      AND date(inspection.inspected_at) IS NOT NULL
                      AND { _qualified_date_predicate('inspection.inspected_at', start, end)[0] }""",
                (workspace_id, *_qualified_date_predicate("inspection.inspected_at", start, end)[1]),
            ).fetchone()[0]
            maintenance = connection.execute(
                f"""SELECT COUNT(*) FROM maintenance_records
                    WHERE workspace_id = ? AND data_origin <> 'synthetic'
                      AND performed_at IS NOT NULL {maintenance_sql}""",
                (workspace_id, *maintenance_bounds),
            ).fetchone()[0]
            undated_maintenance = connection.execute(
                "SELECT COUNT(*) FROM maintenance_records WHERE workspace_id = ? "
                "AND data_origin <> 'synthetic' AND performed_at IS NULL", (workspace_id,)
            ).fetchone()[0]
            latest = connection.execute(
                f"""SELECT MAX(inspected_at) FROM inspections
                    WHERE workspace_id = ? AND data_origin <> 'synthetic'
                      AND status = 'completed' {bounds_sql}""",
                (workspace_id, *bounds),
            ).fetchone()[0]
            located = connection.execute(
                "SELECT COUNT(*) FROM assets WHERE workspace_id = ? AND data_origin <> 'synthetic' "
                "AND location IS NOT NULL AND length(trim(location)) > 0", (workspace_id,)
            ).fetchone()[0]
            provenance = self._provenance_counts(connection, workspace_id)
        coverage = None if assets == 0 else inspected_assets / assets
        return {
            "workspace_id": workspace_id,
            "data_mode": "live",
            "date_window": {"start": start, "end": end},
            "metrics": {
                "total_assets": _metric(assets, definition="Non-synthetic assets in this workspace; not date-filtered."),
                "inspected_assets": _metric(inspected_assets, denominator=assets,
                    definition="Distinct non-synthetic assets with a completed inspection in the selected date window."),
                "inspection_coverage": _metric(coverage, denominator=assets, unit="ratio",
                    definition="Inspected assets divided by non-synthetic workspace assets; null when the denominator is zero."),
                "inspection_count": _metric(inspections,
                    definition="Non-synthetic inspection records of any status dated in the selected window."),
                "observation_count": _metric(observations,
                    definition="Observations attached to non-synthetic assets and inspections dated in the selected window."),
                "maintenance_count": _metric(maintenance,
                    definition="Non-synthetic maintenance records with performed_at in the selected date window."),
                "undated_maintenance_count": _metric(undated_maintenance,
                    definition="Non-synthetic maintenance records without performed_at; excluded from date-window counts."),
                "latest_inspection_at": _metric(latest,
                    definition="Latest completed, non-synthetic inspection timestamp in the selected window."),
                "condition_distribution": _unavailable(
                    "No persisted condition classification or approved classification rule exists."),
                "open_findings": _unavailable(
                    "Observation findings have no open/closed lifecycle in the current schema."),
                "priority_actions": _unavailable(
                    "Maintenance records have no priority field or approved ranking rule."),
                "assets_with_location": _metric(located, denominator=assets,
                    definition="Assets with non-empty recorded location text; no location is inferred."),
                "assets_missing_location": _metric(assets - located, denominator=assets,
                    definition="Non-synthetic assets without recorded location text."),
                "synthetic_records_excluded": _metric(sample_assets,
                    definition="Synthetic assets excluded from live workspace metrics."),
            },
            "provenance": _provenance_summary(provenance),
        }

    def list_assets(self, workspace_id: str, *, limit: int = 50, offset: int = 0,
                    asset_type: str | None = None, search: str | None = None,
                    site_id: str | None = None) -> dict:
        _page(limit, offset)
        clauses = ["asset.workspace_id = ?", "asset.data_origin <> 'synthetic'"]
        values: list[object] = [workspace_id]
        if asset_type is not None:
            asset_type = _filter_text(asset_type, "asset_type")
            clauses.append("asset.asset_type = ?")
            values.append(asset_type)
        if search is not None and search.strip():
            query = _escape_like(search.strip())
            clauses.append("(asset.external_id LIKE ? ESCAPE '\\' OR asset.name LIKE ? ESCAPE '\\')")
            values.extend((f"%{query}%", f"%{query}%"))
        if site_id is not None:
            clauses.append("asset.site_id = ?")
            values.append(_filter_text(site_id, "site_id"))
        where = " AND ".join(clauses)
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            if site_id is not None:
                _require_site(connection, workspace_id, site_id)
            rows = connection.execute(
                f"""SELECT asset.*, site.name AS site_name,
                    (SELECT COUNT(*) FROM inspections inspection
                     WHERE inspection.workspace_id = asset.workspace_id
                       AND inspection.asset_id = asset.asset_id
                       AND inspection.data_origin <> 'synthetic') AS inspection_count
                    FROM assets asset LEFT JOIN sites site
                      ON site.workspace_id = asset.workspace_id AND site.site_id = asset.site_id
                    WHERE {where}
                    ORDER BY asset.external_id IS NULL, asset.external_id, asset.asset_id
                    LIMIT ? OFFSET ?""", (*values, limit, offset),
            ).fetchall()
            total = connection.execute(f"SELECT COUNT(*) FROM assets asset WHERE {where}", values).fetchone()[0]
        return _page_result([dict(row) for row in rows], total, limit, offset)

    def get_asset(self, workspace_id: str, asset_id: int) -> dict:
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            row = connection.execute(
                """SELECT asset.*, site.name AS site_name, site.location_text AS site_location,
                    site.latitude, site.longitude,
                    (SELECT COUNT(*) FROM inspections i WHERE i.workspace_id=asset.workspace_id
                     AND i.asset_id=asset.asset_id AND i.data_origin <> 'synthetic') AS inspection_count,
                    (SELECT MAX(i.inspected_at) FROM inspections i WHERE i.workspace_id=asset.workspace_id
                     AND i.asset_id=asset.asset_id AND i.data_origin <> 'synthetic') AS latest_inspection_at
                   FROM assets asset LEFT JOIN sites site
                     ON site.workspace_id=asset.workspace_id AND site.site_id=asset.site_id
                   WHERE asset.workspace_id=? AND asset.asset_id=?
                     AND asset.data_origin <> 'synthetic'""", (workspace_id, asset_id)
            ).fetchone()
        if row is None:
            raise DashboardNotFound("Asset not found in this workspace.")
        return dict(row)

    def list_inspections(self, workspace_id: str, *, limit: int = 50, offset: int = 0,
                         start: str | None = None, end: str | None = None,
                         asset_id: int | None = None, status: str | None = None) -> dict:
        _page(limit, offset)
        start, end = _date_window(start, end)
        date_sql, date_values = _qualified_date_predicate("inspection.inspected_at", start, end)
        clauses = ["inspection.workspace_id = ?", "inspection.data_origin <> 'synthetic'",
                   "asset.data_origin <> 'synthetic'", date_sql]
        values: list[object] = [workspace_id, *date_values]
        if asset_id is not None:
            if not isinstance(asset_id, int) or asset_id < 1:
                raise DashboardError("asset_id must be a positive integer.")
            clauses.append("inspection.asset_id = ?")
            values.append(asset_id)
        if status is not None:
            if status not in {"planned", "in_progress", "completed", "cancelled"}:
                raise DashboardError("status is not a supported inspection status.")
            clauses.append("inspection.status = ?")
            values.append(status)
        where = " AND ".join(clauses)
        base = "FROM inspections inspection JOIN assets asset ON asset.workspace_id=inspection.workspace_id AND asset.asset_id=inspection.asset_id"
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            rows = connection.execute(
                f"SELECT inspection.*, asset.external_id, asset.name AS asset_name, asset.asset_type {base} "
                f"WHERE {where} ORDER BY date(inspection.inspected_at) DESC, inspection.inspection_id DESC LIMIT ? OFFSET ?",
                (*values, limit, offset),
            ).fetchall()
            total = connection.execute(f"SELECT COUNT(*) {base} WHERE {where}", values).fetchone()[0]
        return _page_result([dict(row) for row in rows], total, limit, offset)

    def get_inspection(self, workspace_id: str, inspection_id: int) -> dict:
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            row = connection.execute(
                """SELECT inspection.*, asset.external_id, asset.name AS asset_name,
                          asset.asset_type
                   FROM inspections inspection JOIN assets asset
                     ON asset.workspace_id=inspection.workspace_id AND asset.asset_id=inspection.asset_id
                   WHERE inspection.workspace_id=? AND inspection.inspection_id=?
                     AND inspection.data_origin <> 'synthetic'
                     AND asset.data_origin <> 'synthetic'""", (workspace_id, inspection_id)
            ).fetchone()
            if row is None:
                raise DashboardNotFound("Inspection not found in this workspace.")
            observations = connection.execute(
                """SELECT observation_id, observation_type, value, unit, finding,
                          data_quality, source_reference, recorded_at
                   FROM observations WHERE workspace_id=? AND inspection_id=?
                   ORDER BY recorded_at, observation_id LIMIT 100""",
                (workspace_id, inspection_id),
            ).fetchall()
        result = dict(row)
        result["observations"] = [dict(item) for item in observations]
        return result

    def list_asset_inspections(self, workspace_id: str, asset_id: int, *,
                               limit: int = 50, offset: int = 0) -> dict:
        _page(limit, offset)
        asset = self.get_asset(workspace_id, asset_id)
        return self.list_inspections(workspace_id, limit=limit, offset=offset,
                                     asset_id=asset["asset_id"])

    def list_trend(self, workspace_id: str, asset_id: int, *, metric: str,
                   start: str | None = None, end: str | None = None,
                   limit: int = 500) -> dict:
        if not 1 <= limit <= 1000:
            raise DashboardError("limit must be between 1 and 1000.")
        metric = _filter_text(metric, "metric")
        start, end = _date_window(start, end)
        date_sql, date_values = _qualified_date_predicate("inspection.inspected_at", start, end)
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            if connection.execute(
                "SELECT 1 FROM assets WHERE workspace_id=? AND asset_id=? AND data_origin <> 'synthetic'",
                (workspace_id, asset_id),
            ).fetchone() is None:
                raise DashboardNotFound("Asset not found in this workspace.")
            rows = connection.execute(
                f"""SELECT observation.observation_id, observation.observation_type AS metric,
                          observation.value, observation.unit, observation.data_quality,
                          observation.finding, observation.source_reference,
                          inspection.inspection_id, inspection.inspected_at AS timestamp,
                          inspection.data_source,
                          CASE WHEN observation.value IS NULL THEN 'missing'
                               WHEN observation.data_quality = 'invalid' THEN 'invalid'
                               WHEN observation.data_quality = 'suspect' THEN 'suspect'
                               ELSE 'recorded' END AS reading_status
                   FROM observations observation
                   JOIN inspections inspection USING (workspace_id, inspection_id)
                   WHERE observation.workspace_id=? AND inspection.asset_id=?
                     AND observation.observation_type=?
                     AND inspection.data_origin <> 'synthetic'
                     AND inspection.status='completed' AND {date_sql}
                   ORDER BY date(inspection.inspected_at), inspection.inspected_at,
                            inspection.inspection_id, observation.observation_id
                   LIMIT ?""",
                (workspace_id, asset_id, metric, *date_values, limit),
            ).fetchall()
        return {"workspace_id": workspace_id, "asset_id": asset_id, "metric": metric,
                "date_window": {"start": start, "end": end}, "items": [dict(row) for row in rows],
                "limit": limit, "truncated": len(rows) == limit,
                "provenance": "observed measurements only; no deterioration rule is applied"}

    def list_maintenance(self, workspace_id: str, *, limit: int = 50, offset: int = 0,
                         asset_id: int | None = None, start: str | None = None,
                         end: str | None = None) -> dict:
        _page(limit, offset)
        start, end = _date_window(start, end)
        date_sql, date_values = _qualified_date_predicate("maintenance.performed_at", start, end)
        clauses = ["maintenance.workspace_id=?", "maintenance.data_origin <> 'synthetic'", date_sql]
        values: list[object] = [workspace_id, *date_values]
        if asset_id is not None:
            if not isinstance(asset_id, int) or asset_id < 1:
                raise DashboardError("asset_id must be a positive integer.")
            clauses.append("maintenance.asset_id=?")
            values.append(asset_id)
        where = " AND ".join(clauses)
        base = "FROM maintenance_records maintenance JOIN assets asset ON asset.workspace_id=maintenance.workspace_id AND asset.asset_id=maintenance.asset_id"
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            rows = connection.execute(
                f"SELECT maintenance.*, asset.external_id, asset.name AS asset_name {base} "
                f"WHERE {where} ORDER BY date(maintenance.performed_at) DESC, maintenance.maintenance_id DESC LIMIT ? OFFSET ?",
                (*values, limit, offset),
            ).fetchall()
            total = connection.execute(f"SELECT COUNT(*) {base} WHERE {where}", values).fetchone()[0]
        return _page_result([dict(row) for row in rows], total, limit, offset)

    def list_sites(self, workspace_id: str, *, limit: int = 50, offset: int = 0) -> dict:
        _page(limit, offset)
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            rows = connection.execute(
                """SELECT site.*,
                    (SELECT COUNT(*) FROM assets asset WHERE asset.workspace_id=site.workspace_id
                     AND asset.site_id=site.site_id AND asset.data_origin <> 'synthetic') AS asset_count
                   FROM sites site WHERE site.workspace_id=?
                   ORDER BY site.name, site.site_id LIMIT ? OFFSET ?""",
                (workspace_id, limit, offset),
            ).fetchall()
            total = connection.execute("SELECT COUNT(*) FROM sites WHERE workspace_id=?", (workspace_id,)).fetchone()[0]
        return _page_result([dict(row) for row in rows], total, limit, offset)

    def get_site(self, workspace_id: str, site_id: str) -> dict:
        with get_connection(self.database_path) as connection:
            _require_workspace(connection, workspace_id)
            result = self._site(connection, workspace_id, site_id)
            result["asset_count"] = connection.execute(
                "SELECT COUNT(*) FROM assets WHERE workspace_id=? AND site_id=? "
                "AND data_origin <> 'synthetic'", (workspace_id, site_id)
            ).fetchone()[0]
            return result

    def create_site(self, workspace_id: str, *, name: str, location_text: str | None,
                    latitude: float | None, longitude: float | None) -> dict:
        name = _filter_text(name, "name", maximum=160)
        location_text = _optional_text(location_text, 500)
        _coordinates(latitude, longitude)
        site_id = uuid4().hex
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            _require_workspace(connection, workspace_id)
            connection.execute(
                """INSERT INTO sites(site_id,workspace_id,name,location_text,latitude,longitude)
                   VALUES(?,?,?,?,?,?)""",
                (site_id, workspace_id, name, location_text, latitude, longitude),
            )
            return self._site(connection, workspace_id, site_id)

    def update_site(self, workspace_id: str, site_id: str, *, name: str | None = None,
                    location_text: str | None = None, latitude: float | None = None,
                    longitude: float | None = None) -> dict:
        fields: dict[str, object] = {}
        if name is not None:
            fields["name"] = _filter_text(name, "name", maximum=160)
        if location_text is not None:
            fields["location_text"] = _optional_text(location_text, 500)
        if latitude is not None or longitude is not None:
            _coordinates(latitude, longitude)
            fields["latitude"], fields["longitude"] = latitude, longitude
        if not fields:
            raise DashboardError("Provide at least one site field to update.")
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            _require_workspace(connection, workspace_id)
            if connection.execute("SELECT 1 FROM sites WHERE workspace_id=? AND site_id=?",
                                  (workspace_id, site_id)).fetchone() is None:
                raise DashboardNotFound("Site not found in this workspace.")
            assignments = ",".join(f"{field}=?" for field in fields)
            connection.execute(f"UPDATE sites SET {assignments}, updated_at=CURRENT_TIMESTAMP "
                                "WHERE workspace_id=? AND site_id=?",
                                (*fields.values(), workspace_id, site_id))
            return self._site(connection, workspace_id, site_id)

    def assign_asset_site(self, workspace_id: str, asset_id: int, site_id: str | None) -> dict:
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            _require_workspace(connection, workspace_id)
            if site_id is not None:
                _require_site(connection, workspace_id, site_id)
            cursor = connection.execute(
                "UPDATE assets SET site_id=?, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE workspace_id=? AND asset_id=? AND data_origin <> 'synthetic'",
                (site_id, workspace_id, asset_id),
            )
            if cursor.rowcount != 1:
                raise DashboardNotFound("Asset not found in this workspace.")
        return self.get_asset(workspace_id, asset_id)

    def _site(self, connection: sqlite3.Connection, workspace_id: str, site_id: str) -> dict:
        row = connection.execute(
            "SELECT * FROM sites WHERE workspace_id=? AND site_id=?", (workspace_id, site_id)
        ).fetchone()
        if row is None:
            raise DashboardNotFound("Site not found in this workspace.")
        return dict(row)

    @staticmethod
    def _provenance_counts(connection: sqlite3.Connection, workspace_id: str) -> dict[str, int]:
        specs = [
            ("assets", "asset", "asset_id", "asset.data_origin", "asset"),
            ("inspections", "inspection", "inspection_id", "inspection.data_origin", "inspection"),
            ("observations", "observation", "observation_id", "inspection.data_origin", "observation"),
            ("maintenance_records", "maintenance", "maintenance_id", "maintenance.data_origin", "maintenance"),
        ]
        selects = []
        params: list[object] = []
        for table, record_type, key, origin, alias in specs:
            if table == "observations":
                join = " JOIN inspections inspection ON inspection.workspace_id=observation.workspace_id AND inspection.inspection_id=observation.inspection_id"
            elif table == "maintenance_records":
                join = ""
            elif table == "inspections":
                join = ""
            else:
                join = ""
            approved = (
                "EXISTS (SELECT 1 FROM promotion_domain_records link "
                "JOIN promotion_operations operation ON operation.promotion_id=link.promotion_id "
                "JOIN approvals approval ON approval.approval_id=operation.approval_id "
                "JOIN datasets dataset ON dataset.dataset_id=operation.dataset_id "
                f"WHERE link.record_type='{record_type}' AND link.record_id={alias}.{key} "
                "AND dataset.workspace_id=" + f"{alias}.workspace_id "
                "AND operation.state='succeeded' AND approval.state='approved')"
            )
            select = (
                f"SELECT CASE WHEN {approved} THEN 'approved_live' "
                f"WHEN {origin}='synthetic' THEN 'sample' ELSE 'operational_unverified' END AS category "
                f"FROM {table} {alias}{join} WHERE {alias}.workspace_id=?"
            )
            selects.append(select)
            params.append(workspace_id)
        rows = connection.execute(
            "SELECT category, COUNT(*) AS count FROM (" + " UNION ALL ".join(selects) + ") GROUP BY category",
            params,
        ).fetchall()
        return {row["category"]: row["count"] for row in rows}


def _require_workspace(connection: sqlite3.Connection, workspace_id: str) -> None:
    if connection.execute("SELECT 1 FROM workspaces WHERE workspace_id=?", (workspace_id,)).fetchone() is None:
        raise WorkspaceNotFound(workspace_id)


def _require_site(connection: sqlite3.Connection, workspace_id: str, site_id: str) -> None:
    if connection.execute("SELECT 1 FROM sites WHERE workspace_id=? AND site_id=?",
                          (workspace_id, site_id)).fetchone() is None:
        raise DashboardNotFound("Site not found in this workspace.")


def _date_window(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    parsed = []
    for value, name in ((start, "start"), (end, "end")):
        if value is None:
            parsed.append(None)
            continue
        if not isinstance(value, str):
            raise DashboardError(f"{name} must be an ISO date in YYYY-MM-DD form.")
        try:
            normalized = date.fromisoformat(value).isoformat()
        except ValueError as error:
            raise DashboardError(f"{name} must be an ISO date in YYYY-MM-DD form.") from error
        if normalized != value:
            raise DashboardError(f"{name} must be an ISO date in YYYY-MM-DD form.")
        parsed.append(normalized)
    start, end = parsed
    if start and end and start > end:
        raise DashboardError("start must be on or before end.")
    if start and end and (date.fromisoformat(end) - date.fromisoformat(start)).days > MAX_DATE_WINDOW_DAYS:
        raise DashboardError("Date windows cannot exceed ten years.")
    return start, end


def _date_predicate(column: str, start: str | None, end: str | None) -> tuple[str, list[object]]:
    parts, values = [], []
    if start:
        parts.append(f"date({column}) >= ?")
        values.append(start)
    if end:
        parts.append(f"date({column}) <= ?")
        values.append(end)
    return (" AND " + " AND ".join(parts) if parts else ""), values


def _qualified_date_predicate(column: str, start: str | None, end: str | None) -> tuple[str, list[object]]:
    parts, values = [], []
    if start:
        parts.append(f"date({column}) >= ?")
        values.append(start)
    if end:
        parts.append(f"date({column}) <= ?")
        values.append(end)
    return (" AND ".join(parts) if parts else "1=1"), values


def _metric(value, *, denominator=None, unit: str | None = None, definition: str) -> dict:
    return {"value": value, "denominator": denominator, "unit": unit,
            "available": True, "definition": definition}


def _unavailable(reason: str) -> dict:
    return {"value": None, "denominator": None, "unit": None,
            "available": False, "unavailable_reason": reason}


def _provenance_summary(counts: dict[str, int]) -> dict:
    categories = [key for key, count in counts.items() if count]
    classification = "none" if not categories else categories[0] if len(categories) == 1 else "mixed"
    return {"classification": classification, "domain_record_counts": counts,
            "approval_is_proven_only_by_successful_promotion_links": True}


def _page(limit: int, offset: int) -> None:
    if not isinstance(limit, int) or not 1 <= limit <= MAX_PAGE_SIZE:
        raise DashboardError(f"limit must be between 1 and {MAX_PAGE_SIZE}.")
    if not isinstance(offset, int) or not 0 <= offset <= MAX_OFFSET:
        raise DashboardError(f"offset must be between 0 and {MAX_OFFSET}.")


def _page_result(items: list[dict], total: int, limit: int, offset: int) -> dict:
    return {"items": items, "total": total, "limit": limit, "offset": offset}


def _filter_text(value: str, field: str, maximum: int = 200) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise DashboardError(f"{field} must be non-empty and at most {maximum} characters.")
    return value.strip()


def _optional_text(value: str | None, maximum: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value.strip()) > maximum:
        raise DashboardError(f"Text must be at most {maximum} characters.")
    return value.strip() or None


def _coordinates(latitude: float | None, longitude: float | None) -> None:
    if (latitude is None) != (longitude is None):
        raise DashboardError("Latitude and longitude must be supplied together.")
    if latitude is not None and (not -90 <= latitude <= 90 or not -180 <= longitude <= 180):
        raise DashboardError("Coordinates are outside valid latitude/longitude ranges.")


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
