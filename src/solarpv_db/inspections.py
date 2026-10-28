
"""Operations for recording and retrieving inspections."""

from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection


VALID_STATUSES = {
    "planned",
    "in_progress",
    "completed",
    "cancelled",
}

VALID_DATA_ORIGINS = {"real", "synthetic", "unknown"}


def create_inspection(
    asset_id: int,
    inspected_at: str,
    inspection_method: str | None = None,
    data_source: str | None = None,
    data_origin: str = "unknown",
    status: str = "completed",
    notes: str | None = None,
    database_path: str | Path = DEFAULT_DATABASE,
) -> int:
    """Record an inspection and return its ID."""
    if not inspected_at or not inspected_at.strip():
        raise ValueError("Inspection date and time cannot be empty.")

    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid inspection status: {status}")

    if data_origin not in VALID_DATA_ORIGINS:
        raise ValueError(f"Invalid data origin: {data_origin}")

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO inspections (
                asset_id,
                inspected_at,
                inspection_method,
                data_source,
                data_origin,
                status,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                inspected_at.strip(),
                inspection_method,
                data_source,
                data_origin,
                status,
                notes,
            ),
        )
        return cursor.lastrowid


def get_inspection(
    inspection_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
) -> dict | None:
    """Return an inspection by ID, or None if it does not exist."""
    with get_connection(database_path) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM inspections
            WHERE inspection_id = ?
            """,
            (inspection_id,),
        ).fetchone()

    return dict(row) if row is not None else None


def list_inspections(
    asset_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
) -> list[dict]:
    """Return an asset's inspections, newest first."""
    with get_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM inspections
            WHERE asset_id = ?
            ORDER BY inspected_at DESC, inspection_id DESC
            """,
            (asset_id,),
        ).fetchall()

    return [dict(row) for row in rows]
