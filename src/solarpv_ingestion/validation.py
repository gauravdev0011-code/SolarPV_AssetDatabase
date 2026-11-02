"""Bounded deterministic validation for registered CSV and JSON datasets."""

from __future__ import annotations

import csv
import io
import json
import math
import re
from datetime import date, datetime
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from solarpv_ingestion.processing import InvalidInputError, _reject_constant
from solarpv_ingestion.storage import LocalArtifactStorage

VALIDATION_RULE_SET = "solarpv.dataset-rules-v1"
MAX_VALIDATION_ROWS = 1_000_000
MAX_VALIDATION_COLUMNS = 500
MAX_VALIDATION_CELL_CHARS = 1_000_000
MAX_ERROR_DETAILS = 1_000
MAX_RESULT_BYTES = 1_000_000


def read_records(
    storage: LocalArtifactStorage,
    storage_key: str,
    filename: str,
    *,
    max_json_bytes: int,
    max_rows: int = MAX_VALIDATION_ROWS,
    max_columns: int = MAX_VALIDATION_COLUMNS,
    max_cell_chars: int = MAX_VALIDATION_CELL_CHARS,
) -> tuple[list[str], Iterator[tuple[int, dict[str, object]]]]:
    """Return headers and a streaming record iterator; JSON is byte bounded."""
    suffix = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if suffix == "csv":
        raw = storage.open_read(storage_key)
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="strict", newline="")
        try:
            reader = csv.reader(text, strict=True)
            headers = next(reader, None)
            if not headers:
                text.close()
                raise InvalidInputError("CSV must include a header row.")
            _validate_headers(headers, max_columns)
        except (UnicodeDecodeError, csv.Error, StopIteration) as error:
            text.close()
            raise InvalidInputError("CSV header is malformed or is not valid UTF-8.") from error

        def csv_rows() -> Iterator[tuple[int, dict[str, object]]]:
            try:
                for record_number, row in enumerate(reader, start=1):
                    number = reader.line_num
                    if record_number > max_rows:
                        raise InvalidInputError(f"Dataset exceeds the {max_rows}-row validation limit.")
                    if len(row) != len(headers):
                        raise InvalidInputError(
                            f"CSV row {number} has {len(row)} fields; expected {len(headers)}."
                        )
                    if any(len(value) > max_cell_chars for value in row):
                        raise InvalidInputError(f"CSV row {number} contains a field exceeding the size limit.")
                    yield number, dict(zip(headers, row, strict=True))
            except (UnicodeDecodeError, csv.Error) as error:
                raise InvalidInputError(f"Malformed CSV: {error}") from error
            finally:
                text.close()

        return headers, csv_rows()

    if suffix != "json":
        raise InvalidInputError("Only CSV and JSON datasets are supported.")
    try:
        with storage.open_read(storage_key) as stream:
            content = stream.read(max_json_bytes + 1)
        if len(content) > max_json_bytes:
            raise InvalidInputError(f"JSON exceeds the {max_json_bytes}-byte validation limit.")
        document = json.loads(content.decode("utf-8-sig"), parse_constant=_reject_constant)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        if isinstance(error, InvalidInputError):
            raise
        raise InvalidInputError(f"Malformed JSON: {error}") from error

    records = document if isinstance(document, list) else (
        document["records"] if isinstance(document, dict) and isinstance(document.get("records"), list)
        else [document]
    )
    if not all(isinstance(record, dict) for record in records):
        raise InvalidInputError("JSON dataset records must be objects.")
    if len(records) > max_rows:
        raise InvalidInputError(f"Dataset exceeds the {max_rows}-row validation limit.")
    headers: list[str] = []
    for record in records:
        for key in record:
            if not isinstance(key, str):
                raise InvalidInputError("JSON record field names must be strings.")
            if key not in headers:
                headers.append(key)
                if len(headers) > max_columns:
                    raise InvalidInputError(f"Dataset exceeds the {max_columns}-column validation limit.")
    _validate_headers(headers, max_columns)

    def json_rows() -> Iterator[tuple[int, dict[str, object]]]:
        for index, record in enumerate(records, start=1):
            yield index, record

    return headers, json_rows()


def preview_records(
    storage: LocalArtifactStorage,
    storage_key: str,
    filename: str,
    *,
    limit: int,
    max_json_bytes: int,
) -> dict:
    if not 1 <= limit <= 100:
        raise ValueError("Preview limit must be between 1 and 100.")
    headers, rows = read_records(
        storage, storage_key, filename, max_json_bytes=max_json_bytes,
        max_rows=MAX_VALIDATION_ROWS,
    )
    samples = []
    try:
        for _, record in rows:
            samples.append(record)
            if len(samples) == limit:
                break
    finally:
        close = getattr(rows, "close", None)
        if close is not None:
            close()
    return {"columns": headers, "rows": samples, "returned_rows": len(samples), "limit": limit}


def validate_records(
    storage: LocalArtifactStorage,
    storage_key: str,
    filename: str,
    mappings: list[dict],
    *,
    max_json_bytes: int,
) -> dict:
    headers, rows = read_records(
        storage, storage_key, filename, max_json_bytes=max_json_bytes,
    )
    errors: list[dict] = []
    warnings: list[dict] = []
    error_count = 0
    warning_count = 0

    def add(target: list, kind: str, row: int | None, field: str | None, code: str, message: str) -> None:
        nonlocal error_count, warning_count
        if kind == "error":
            error_count += 1
            if len(errors) < MAX_ERROR_DETAILS:
                errors.append({"row": row, "field": field, "code": code, "message": message})
        else:
            warning_count += 1
            if len(warnings) < MAX_ERROR_DETAILS:
                warnings.append({"row": row, "field": field, "code": code, "message": message})

    if not mappings:
        add(warnings, "warning", None, None, "mapping_not_defined",
            "No column mapping is defined; the dataset was checked for readable structure only.")
    mapped = {item["source_column"] for item in mappings}
    for mapping in mappings:
        if mapping["source_column"] not in headers:
            add(errors, "error", None, mapping["source_column"], "source_column_missing",
                "Mapped source column does not exist in the dataset.")
    for name in headers:
        if name not in mapped:
            add(warnings, "warning", None, name, "unmapped_column", "Source column has no target mapping.")

    row_count = 0
    try:
        for row_number, record in rows:
            row_count += 1
            for mapping in mappings:
                field = mapping["source_column"]
                if field not in record:
                    continue
                value = record[field]
                empty = value is None or value == ""
                if empty and mapping["is_required"]:
                    add(errors, "error", row_number, field, "required_value_missing",
                        "Required value is empty or missing.")
                    continue
                if empty:
                    continue
                issue = _type_issue(value, mapping)
                if issue:
                    add(errors, "error", row_number, field, issue,
                        f"Value does not match expected type '{mapping['expected_type']}'.")
    except InvalidInputError as error:
        add(errors, "error", None, None, "malformed_dataset", str(error)[:300])
    finally:
        close = getattr(rows, "close", None)
        if close is not None:
            close()

    outcome = "failed" if error_count else "warnings" if warning_count else "passed"
    return {
        "rule_set_version": VALIDATION_RULE_SET,
        "outcome": outcome,
        "row_count": row_count,
        "error_count": error_count,
        "warning_count": warning_count,
        "error_details_truncated": error_count > len(errors),
        "warning_details_truncated": warning_count > len(warnings),
        "errors": errors,
        "warnings": warnings,
        "limits": {
            "max_rows": MAX_VALIDATION_ROWS,
            "max_columns": MAX_VALIDATION_COLUMNS,
            "max_cell_chars": MAX_VALIDATION_CELL_CHARS,
            "max_error_details": MAX_ERROR_DETAILS,
        },
    }


def _validate_headers(headers: list[str], maximum: int) -> None:
    if len(headers) > maximum:
        raise InvalidInputError(f"Dataset exceeds the {maximum}-column validation limit.")
    if len(set(headers)) != len(headers) or any(not name.strip() or len(name) > 500 for name in headers):
        raise InvalidInputError("Dataset columns must be unique, non-empty, and at most 500 characters.")


def _type_issue(value: object, mapping: dict) -> str | None:
    expected = mapping["expected_type"]
    text = str(value)
    if expected == "string":
        return None if isinstance(value, str) else "invalid_type"
    if expected == "integer":
        return None if isinstance(value, int) and not isinstance(value, bool) or (
            isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip())
        ) else "invalid_integer"
    if expected == "number":
        try:
            number = float(text)
            return None if math.isfinite(number) else "invalid_number"
        except (TypeError, ValueError):
            return "invalid_number"
    if expected == "boolean":
        return None if (isinstance(value, bool) or (isinstance(value, str) and value.strip().lower() in {"true", "false"})) else "invalid_boolean"
    if expected == "date":
        try:
            date.fromisoformat(text)
            return None
        except ValueError:
            return "invalid_date"
    if expected == "datetime":
        try:
            parsed = datetime.strptime(text, mapping["timestamp_format"]) if mapping.get("timestamp_format") else datetime.fromisoformat(text.replace("Z", "+00:00"))
            timezone = mapping.get("timezone")
            if timezone:
                ZoneInfo(timezone)
                if parsed.tzinfo is None:
                    return "timezone_missing"
            return None
        except (ValueError, TypeError, ZoneInfoNotFoundError):
            return "invalid_datetime"
    return "invalid_type_definition"
