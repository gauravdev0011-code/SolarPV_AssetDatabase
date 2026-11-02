"""Workspace lifecycle operations for the local single-operator service."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

from solarpv_db.database import DEFAULT_DATABASE, get_connection


DEFAULT_WORKSPACE_ID = "default"
MAX_WORKSPACE_NAME = 160
MAX_WORKSPACE_DESCRIPTION = 1000


class WorkspaceError(ValueError):
    """Invalid workspace request."""


class WorkspaceNotFound(KeyError):
    """Workspace ID does not exist."""


class WorkspaceConflict(RuntimeError):
    """Workspace update conflicts with an existing record."""


class WorkspaceService:
    def __init__(self, database_path: str | Path = DEFAULT_DATABASE) -> None:
        self.database_path = database_path

    def create(self, *, name: str, description: str | None = None) -> dict:
        name = _text(name, "name", MAX_WORKSPACE_NAME)
        description = _optional_text(description, MAX_WORKSPACE_DESCRIPTION)
        workspace_id = uuid4().hex
        try:
            with get_connection(self.database_path) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO workspaces(workspace_id,name,description,updated_at) "
                    "VALUES(?,?,?,CURRENT_TIMESTAMP)",
                    (workspace_id, name, description),
                )
                return self._get(connection, workspace_id)
        except sqlite3.IntegrityError as error:
            raise WorkspaceConflict("Workspace could not be created.") from error

    def list(self, *, limit: int = 50, offset: int = 0) -> dict:
        _page(limit, offset)
        with get_connection(self.database_path) as connection:
            rows = connection.execute(
                "SELECT * FROM workspaces ORDER BY created_at, workspace_id LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            total = connection.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0]
        return {"items": [_workspace_dict(row) for row in rows], "total": total,
                "limit": limit, "offset": offset}

    def get(self, workspace_id: str) -> dict:
        with get_connection(self.database_path) as connection:
            return self._get(connection, workspace_id)

    def update(
        self, workspace_id: str, *, name: str | None = None,
        description: str | None = None,
    ) -> dict:
        fields: dict[str, str | None] = {}
        if name is not None:
            fields["name"] = _text(name, "name", MAX_WORKSPACE_NAME)
        if description is not None:
            fields["description"] = _optional_text(description, MAX_WORKSPACE_DESCRIPTION)
        if not fields:
            raise WorkspaceError("Provide name or description to update.")
        with get_connection(self.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM workspaces WHERE workspace_id = ?", (workspace_id,)
            ).fetchone() is None:
                raise WorkspaceNotFound(workspace_id)
            assignments = ", ".join(f"{field} = ?" for field in fields)
            connection.execute(
                f"UPDATE workspaces SET {assignments}, updated_at = CURRENT_TIMESTAMP "
                "WHERE workspace_id = ?",
                (*fields.values(), workspace_id),
            )
            return self._get(connection, workspace_id)

    @staticmethod
    def _get(connection: sqlite3.Connection, workspace_id: str) -> dict:
        row = connection.execute(
            "SELECT * FROM workspaces WHERE workspace_id = ?", (workspace_id,)
        ).fetchone()
        if row is None:
            raise WorkspaceNotFound(workspace_id)
        return _workspace_dict(row)


def _workspace_dict(row: sqlite3.Row) -> dict:
    return {"workspace_id": row["workspace_id"], "name": row["name"],
            "description": row["description"], "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "workspace_kind": "live", "is_default": row["workspace_id"] == DEFAULT_WORKSPACE_ID}


def _text(value: str, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise WorkspaceError(f"{field} must be non-empty and at most {maximum} characters.")
    return value.strip()


def _optional_text(value: str | None, maximum: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value.strip()) > maximum:
        raise WorkspaceError(f"description must be at most {maximum} characters.")
    return value.strip() or None


def _page(limit: int, offset: int) -> None:
    if not isinstance(limit, int) or not 1 <= limit <= 100:
        raise WorkspaceError("limit must be between 1 and 100.")
    if not isinstance(offset, int) or not 0 <= offset <= 10_000_000:
        raise WorkspaceError("offset must be between 0 and 10000000.")
