
"""Operations for managing solar assets."""

from pathlib import Path

from solarpv_db.database import DEFAULT_DATABASE, get_connection

VALID_DATA_ORIGINS = {"real", "synthetic", "unknown"}
_UNSET = object()


def create_asset(
    asset_type: str,
    name: str,
    external_id: str | None = None,
    description: str | None = None,
    location: str | None = None,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
    site_id: str | None = None,
    data_origin: str = "unknown",
) -> int:
    """Create an asset and return its internal ID."""
    asset_type = asset_type.strip()
    name = name.strip()

    if not asset_type:
        raise ValueError("Asset type cannot be empty.")

    if not name:
        raise ValueError("Asset name cannot be empty.")

    if external_id is not None:
        external_id = external_id.strip()
        if not external_id:
            raise ValueError("External ID cannot be empty.")
    if data_origin not in VALID_DATA_ORIGINS:
        raise ValueError(f"Invalid data origin: {data_origin}")

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO assets (
                external_id, asset_type, name, description, location
                , workspace_id, site_id, data_origin
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (external_id, asset_type, name, description, location,
             workspace_id, site_id, data_origin),
        )
        return cursor.lastrowid


def get_asset(
    asset_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
) -> dict | None:
    """Return one asset by internal ID, or None if it does not exist."""
    with get_connection(database_path) as connection:
        row = connection.execute(
            "SELECT * FROM assets WHERE asset_id = ? AND workspace_id = ?",
            (asset_id, workspace_id),
        ).fetchone()

    return dict(row) if row is not None else None


def list_assets(
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    workspace_id: str = "default",
) -> list[dict]:
    """Return all assets ordered by internal ID."""
    with get_connection(database_path) as connection:
        rows = connection.execute(
            "SELECT * FROM assets WHERE workspace_id = ? ORDER BY asset_id",
            (workspace_id,),
        ).fetchall()

    return [dict(row) for row in rows]


def update_asset(
    asset_id: int,
    database_path: str | Path = DEFAULT_DATABASE,
    *,
    asset_type: str | None = None,
    name: str | None = None,
    external_id: str | None = None,
    description: str | None = None,
    location: str | None = None,
    site_id: str | None | object = _UNSET,
    data_origin: str | None = None,
    workspace_id: str = "default",
) -> bool:
    """Update supplied asset fields without changing its internal ID."""
    fields = {}

    if asset_type is not None:
        asset_type = asset_type.strip()
        if not asset_type:
            raise ValueError("Asset type cannot be empty.")
        fields["asset_type"] = asset_type

    if name is not None:
        name = name.strip()
        if not name:
            raise ValueError("Asset name cannot be empty.")
        fields["name"] = name

    if external_id is not None:
        external_id = external_id.strip()
        if not external_id:
            raise ValueError("External ID cannot be empty.")
        fields["external_id"] = external_id

    if description is not None:
        fields["description"] = description

    if location is not None:
        fields["location"] = location

    if site_id is not _UNSET:
        fields["site_id"] = site_id

    if data_origin is not None:
        if data_origin not in VALID_DATA_ORIGINS:
            raise ValueError(f"Invalid data origin: {data_origin}")
        fields["data_origin"] = data_origin

    if not fields:
        raise ValueError("Provide at least one field to update.")

    fields["updated_at"] = None

    columns = []
    values = []

    for column, value in fields.items():
        if column == "updated_at":
            columns.append(
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
            )
        else:
            columns.append(f"{column} = ?")
            values.append(value)

    values.extend((asset_id, workspace_id))

    with get_connection(database_path) as connection:
        cursor = connection.execute(
            f"UPDATE assets SET {', '.join(columns)} WHERE asset_id = ? AND workspace_id = ?",
            values,
        )
        return cursor.rowcount > 0

