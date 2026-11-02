
"""Operations for recording and retrieving inspection observations."""

import math
from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection

VALID_DATA_QUALITIES = {"good", "suspect", "invalid", "unknown"}


def create_observation(
    inspection_id: int,
    observation_type: str,
    value: float | None = None,
    unit: str | None = None,
    finding: str | None = None,
    data_quality: str = "unknown",
    source_reference: str | None = None,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
) -> int:
    """Create an observation and return its ID."""
    observation_type = observation_type.strip()

    if not observation_type:
        raise ValueError("Observation type cannot be empty.")

    if value is not None and not math.isfinite(value):
        raise ValueError("Observation value must be a finite number.")

    if value is None and (finding is None or not finding.strip()):
        raise ValueError(
            "An observation must contain a measurement or a finding."
        )

    if data_quality not in VALID_DATA_QUALITIES:
        raise ValueError(f"Invalid data quality: {data_quality}")

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO observations (
                workspace_id,
                inspection_id,
                observation_type,
                value,
                unit,
                finding,
                data_quality,
                source_reference
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                inspection_id,
                observation_type,
                value,
                unit,
                finding,
                data_quality,
                source_reference,
            ),
        )

        return cursor.lastrowid


def get_observation(
    observation_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
) -> dict | None:
    """Retrieve one observation by ID."""
    with get_connection(database_path) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM observations
            WHERE observation_id = ? AND workspace_id = ?
            """,
            (observation_id, workspace_id),
        ).fetchone()

        return dict(row) if row else None


def list_observations(
    inspection_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
) -> list[dict]:
    """List observations belonging to an inspection."""
    with get_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM observations
            WHERE inspection_id = ? AND workspace_id = ?
            ORDER BY observation_id
            """,
            (inspection_id, workspace_id),
        ).fetchall()

        return [dict(row) for row in rows]
