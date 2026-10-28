
"""Operations for recording and retrieving asset maintenance."""

from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection

VALID_DATA_ORIGINS = {"real", "synthetic", "unknown"}


def create_maintenance_record(
    asset_id: int,
    performed_at: str,
    maintenance_type: str,
    description: str,
    performed_by: str | None = None,
    data_origin: str = "unknown",
    database_path: str | Path = DEFAULT_DATABASE,
) -> int:
    """Create a maintenance record and return its ID."""
    if not performed_at or not performed_at.strip():
        raise ValueError("Maintenance date cannot be empty.")

    if not maintenance_type or not maintenance_type.strip():
        raise ValueError("Maintenance type cannot be empty.")

    if not description or not description.strip():
        raise ValueError("Maintenance description cannot be empty.")

    if data_origin not in VALID_DATA_ORIGINS:
        raise ValueError(f"Invalid data origin: {data_origin}")

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO maintenance_records (
                asset_id,
                performed_at,
                maintenance_type,
                description,
                performed_by,
                data_origin
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                asset_id,
                performed_at.strip(),
                maintenance_type.strip(),
                description.strip(),
                performed_by,
                data_origin,
            ),
        )

        return cursor.lastrowid


def get_maintenance_record(
    maintenance_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
) -> dict | None:
    """Retrieve one maintenance record by ID."""
    with get_connection(database_path) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM maintenance_records
            WHERE maintenance_id = ?
            """,
            (maintenance_id,),
        ).fetchone()

        return dict(row) if row else None


def list_maintenance_records(
    asset_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
) -> list[dict]:
    """List an asset's maintenance history, newest first."""
    with get_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM maintenance_records
            WHERE asset_id = ?
            ORDER BY performed_at DESC, maintenance_id DESC
            """,
            (asset_id,),
        ).fetchall()

        return [dict(row) for row in rows]
