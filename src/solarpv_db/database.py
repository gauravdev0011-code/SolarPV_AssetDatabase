"""Database connection and initialization for Solar PV Asset Database."""

import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FILE = PROJECT_ROOT / "migrations" / "001_initial_schema.sql"
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "solarpv.db"


class ClosingConnection(sqlite3.Connection):
    """SQLite connection that closes after its context manager exits."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def get_connection(
    database_path: str | Path = DEFAULT_DATABASE,
) -> sqlite3.Connection:
    """Open a SQLite connection with foreign-key checks enabled.

    The connection commits or rolls back its transaction on context-manager
    exit and is then closed, including when an exception occurs.
    """
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path, factory=ClosingConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")

    return connection


def initialize_database(
    database_path: str | Path = DEFAULT_DATABASE,
) -> None:
    """Create the database tables and indexes if they do not exist."""
    if not SCHEMA_FILE.is_file():
        raise FileNotFoundError(
            f"Database schema not found: {SCHEMA_FILE}"
        )

    schema = SCHEMA_FILE.read_text(encoding="utf-8")

    with get_connection(database_path) as connection:
        connection.executescript(schema)
