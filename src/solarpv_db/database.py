"""Database connection and initialization for Solar PV Asset Database."""

import hashlib
import re
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"
SCHEMA_FILE = MIGRATIONS_DIR / "001_initial_schema.sql"
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "solarpv.db"
_MIGRATION_PATTERN = re.compile(r"^(\d{3,})_([a-z0-9_]+)\.sql$")


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
    """Apply pending numbered SQL migrations transactionally.

    Databases created before migration tracking are recognized by applying the
    original idempotent initial schema, then recording it as the baseline.
    Existing rows are never copied, dropped, or rewritten by this process.
    """
    _require_sqlite_json_support()
    migrations = _get_migrations()
    with get_connection(database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        migration_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'schema_migrations'"
        ).fetchone()
        applied_rows = (
            connection.execute(
                "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
            ).fetchall()
            if migration_table is not None
            else []
        )
        applied_versions = [row["version"] for row in applied_rows]
        latest_version = applied_versions[-1] if applied_versions else 0
        database_version = connection.execute("PRAGMA user_version").fetchone()[0]
        if database_version != latest_version:
            raise RuntimeError(
                "SQLite user_version does not match migration history: "
                f"{database_version} != {latest_version}."
            )
        if latest_version > migrations[-1][0]:
            raise RuntimeError(
                f"Database migration version {latest_version} is newer than "
                f"available migrations ({migrations[-1][0]})."
            )
        if applied_versions != list(range(1, latest_version + 1)):
            raise RuntimeError("Migration history is not contiguous from version 1.")
        for row in applied_rows:
            applied_migration = migrations[row["version"] - 1]
            if (
                row["name"] != applied_migration[1]
                or row["checksum"] != applied_migration[2]
            ):
                raise RuntimeError(
                    f"Applied migration {row['version']} does not match "
                    "the migration file; refusing to continue."
                )
        if latest_version == 0:
            _validate_untracked_database(connection, migrations[0][3])
        connection.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        connection.commit()

        for version, name, checksum, sql in migrations:
            connection.execute("BEGIN IMMEDIATE")
            try:
                applied_rows = connection.execute(
                    "SELECT version, name, checksum FROM schema_migrations "
                    "ORDER BY version"
                ).fetchall()
                applied_versions = [row["version"] for row in applied_rows]
                latest_version = applied_versions[-1] if applied_versions else 0
                database_version = connection.execute(
                    "PRAGMA user_version"
                ).fetchone()[0]
                if database_version != latest_version:
                    raise RuntimeError(
                        "SQLite user_version does not match migration history: "
                        f"{database_version} != {latest_version}."
                    )
                if latest_version > migrations[-1][0]:
                    raise RuntimeError(
                        f"Database migration version {latest_version} is newer "
                        f"than available migrations ({migrations[-1][0]})."
                    )
                expected_versions = list(range(1, latest_version + 1))
                if applied_versions != expected_versions:
                    raise RuntimeError(
                        "Migration history is not contiguous from version 1."
                    )
                for row in applied_rows:
                    applied_migration = migrations[row["version"] - 1]
                    if (
                        row["name"] != applied_migration[1]
                        or row["checksum"] != applied_migration[2]
                    ):
                        raise RuntimeError(
                            f"Applied migration {row['version']} does not match "
                            "the migration file; refusing to continue."
                        )

                if latest_version >= version:
                    connection.commit()
                    continue
                if version != latest_version + 1:
                    raise RuntimeError(
                        f"Migration sequence gap: expected {latest_version + 1}, "
                        f"found {version}."
                    )
                if latest_version == 0:
                    _validate_untracked_database(connection, migrations[0][3])

                _execute_sql_script(connection, sql)
                connection.execute(
                    "INSERT INTO schema_migrations (version, name, checksum) "
                    "VALUES (?, ?, ?)",
                    (version, name, checksum),
                )
                connection.execute(f"PRAGMA user_version = {version}")
                connection.commit()
            except Exception:
                if connection.in_transaction:
                    connection.rollback()
                raise


def _require_sqlite_json_support() -> None:
    """Fail before database changes if SQLite JSON functions are unavailable."""
    try:
        with sqlite3.connect(":memory:") as connection:
            result = connection.execute(
                "SELECT json_valid('{}'), json_type('{}')"
            ).fetchone()
    except sqlite3.OperationalError as error:
        raise RuntimeError(
            "SQLite JSON functions json_valid() and json_type() are required "
            "for database migrations and promotion integrity."
        ) from error
    if result != (1, "object"):
        raise RuntimeError(
            "SQLite JSON functions json_valid() and json_type() are required "
            "for database migrations and promotion integrity."
        )


def _execute_sql_script(connection: sqlite3.Connection, script: str) -> None:
    """Execute complete SQL statements inside the caller's transaction."""
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            if statement.strip():
                connection.execute(statement)
            statement = ""
    trailing_lines = [line.strip() for line in statement.splitlines()]
    if any(line and not line.startswith("--") for line in trailing_lines):
        raise RuntimeError("Migration contains an incomplete SQL statement.")


def _get_migrations() -> list[tuple[int, str, str, str]]:
    """Read and validate the ordered migration files without modifying them."""
    if not MIGRATIONS_DIR.is_dir():
        raise FileNotFoundError(f"Migration directory not found: {MIGRATIONS_DIR}")

    migrations = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = _MIGRATION_PATTERN.fullmatch(path.name)
        if match is None:
            raise RuntimeError(f"Invalid migration filename: {path.name}")
        version = int(match.group(1))
        name = match.group(2)
        sql = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        migrations.append((version, name, checksum, sql))

    if not migrations or migrations[0][0] != 1:
        raise FileNotFoundError(f"Initial schema migration not found: {SCHEMA_FILE}")
    if [item[0] for item in migrations] != list(range(1, migrations[-1][0] + 1)):
        raise RuntimeError("Migration versions must be contiguous, starting at 1.")
    return migrations


def _validate_untracked_database(
    connection: sqlite3.Connection, initial_schema: str
) -> None:
    """Compare legacy schema objects with an in-memory copy of migration 001."""
    actual_objects = _schema_objects(connection)
    reference = sqlite3.connect(":memory:")
    try:
        reference.execute("PRAGMA foreign_keys = ON")
        reference.row_factory = sqlite3.Row
        _execute_sql_script(reference, initial_schema)
        expected_objects = _schema_objects(reference)
    finally:
        reference.close()

    relevant_actual = set(expected_objects).intersection(actual_objects)
    if not relevant_actual:
        return
    missing = set(expected_objects) - set(actual_objects)
    incompatible = {
        key for key in set(expected_objects).intersection(actual_objects)
        if _normalize_schema_sql(expected_objects[key])
        != _normalize_schema_sql(actual_objects[key])
    }
    if missing or incompatible:
        details = []
        if missing:
            details.append("missing objects: " + ", ".join(sorted(missing)))
        if incompatible:
            details.append("incompatible objects: " + ", ".join(sorted(incompatible)))
        raise RuntimeError(
            "Untracked Solar PV schema is incompatible with migration 001 ("
            + "; ".join(details)
            + "); refusing to baseline it."
        )
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise RuntimeError(
            "Untracked Solar PV database contains foreign-key violations; "
            "refusing to baseline it."
        )


def _schema_objects(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute(
        """SELECT type, name, sql FROM sqlite_master
           WHERE type IN ('table', 'index')
             AND name NOT LIKE 'sqlite_%'
           ORDER BY type, name"""
    ).fetchall()
    return {f"{row['type']}:{row['name']}": row["sql"] or "" for row in rows}


def _normalize_schema_sql(sql: str) -> str:
    """Normalize insignificant whitespace for schema DDL comparisons."""
    return " ".join(sql.split()).casefold()
