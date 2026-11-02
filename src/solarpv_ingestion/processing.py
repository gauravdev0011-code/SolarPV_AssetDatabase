"""Bounded, deterministic profiling for generic CSV and JSON uploads."""

import csv
import json
from collections import Counter
from typing import Callable

from solarpv_ingestion.models import SourceFile
from solarpv_ingestion.storage import LocalArtifactStorage


class InvalidInputError(ValueError):
    """The uploaded content is malformed or exceeds a processing limit."""


PARSER_ID = "solarpv.generic-profile"
PARSER_VERSION = "1.0.0"
RESULT_SCHEMA_VERSION = "1"


def profile_source(
    source: SourceFile,
    storage: LocalArtifactStorage,
    *,
    max_json_bytes: int = 5 * 1024 * 1024,
    max_rows: int = 1_000_000,
    max_columns: int = 500,
    max_cell_chars: int = 1_000_000,
    cancelled: Callable[[], bool] = lambda: False,
) -> dict:
    suffix = source.original_name.rsplit(".", 1)[-1].lower() if "." in source.original_name else ""
    if suffix == "csv":
        profile = _profile_csv(source, storage, max_rows, max_columns, max_cell_chars, cancelled)
        detected_format = "csv"
    elif suffix == "json":
        profile = _profile_json(source, storage, max_json_bytes, cancelled)
        detected_format = "json"
    else:
        raise InvalidInputError("Only .csv and .json files are supported.")
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "source": {
            "source_file_id": source.source_file_id,
            "sha256": source.sha256,
            "original_name": source.original_name,
            "size_bytes": source.size_bytes,
        },
        "processing": {"parser_id": PARSER_ID, "parser_version": PARSER_VERSION},
        "format": detected_format,
        "profile": profile,
    }


def _profile_csv(source, storage, max_rows, max_columns, max_cell_chars, cancelled):
    try:
        with storage.open_read(source.storage_key) as raw:
            import io
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="strict", newline="")
            reader = csv.reader(text, strict=True)
            try:
                headers = next(reader)
            except StopIteration:
                raise InvalidInputError("CSV must include a header row.") from None
            if not headers or len(headers) > max_columns:
                raise InvalidInputError(f"CSV must contain between 1 and {max_columns} columns.")
            if any(not name.strip() or len(name) > 500 for name in headers):
                raise InvalidInputError("CSV column names must be non-empty and at most 500 characters.")
            if len(set(headers)) != len(headers):
                raise InvalidInputError("CSV column names must be unique.")
            types = [Counter() for _ in headers]
            row_count = 0
            for row in reader:
                if cancelled():
                    raise ProcessingCancelled
                row_count += 1
                if row_count > max_rows:
                    raise InvalidInputError(f"CSV exceeds the {max_rows} data-row limit.")
                if len(row) != len(headers):
                    raise InvalidInputError(f"CSV row {reader.line_num} has {len(row)} fields; expected {len(headers)}.")
                for index, value in enumerate(row):
                    if len(value) > max_cell_chars:
                        raise InvalidInputError(f"CSV row {reader.line_num} contains a field exceeding the size limit.")
                    types[index][_classify(value)] += 1
            return {
                "row_count": row_count,
                "column_count": len(headers),
                "columns": [{"name": name, "observed_types": dict(types[index])} for index, name in enumerate(headers)],
                "encoding": "utf-8",
            }
    except UnicodeDecodeError as error:
        raise InvalidInputError("CSV is not valid UTF-8 text.") from error
    except csv.Error as error:
        raise InvalidInputError(f"Malformed CSV: {error}") from error


def _classify(value: str) -> str:
    if value == "":
        return "empty"
    try:
        int(value)
        return "integer"
    except ValueError:
        pass
    try:
        float(value)
        return "number"
    except ValueError:
        return "text"


def _profile_json(source, storage, max_bytes, cancelled):
    if source.size_bytes > max_bytes:
        raise InvalidInputError(f"JSON exceeds the {max_bytes}-byte processing limit.")
    try:
        content = storage.read_bytes(source.storage_key)
        if len(content) > max_bytes:
            raise InvalidInputError(f"JSON exceeds the {max_bytes}-byte processing limit.")
        document = json.loads(content.decode("utf-8-sig"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise InvalidInputError(f"Malformed JSON: {error}") from error
    stats = {"nodes": 0, "max_depth": 0, "types": Counter(), "object_keys": Counter(), "array_items": 0}
    _walk_json(document, stats, 0, cancelled)
    return {
        "root_type": _json_type(document),
        "record_count": (
            len(document) if isinstance(document, list) and all(isinstance(item, dict) for item in document)
            else len(document["records"]) if isinstance(document, dict)
            and isinstance(document.get("records"), list)
            and all(isinstance(item, dict) for item in document["records"])
            else 1 if isinstance(document, dict) else None
        ),
        "node_count": stats["nodes"],
        "max_depth": stats["max_depth"],
        "value_types": dict(stats["types"]),
        "top_level_keys": sorted(document.keys())[:200] if isinstance(document, dict) else [],
        "array_items": stats["array_items"],
    }


def _walk_json(value, stats, depth, cancelled):
    stats["nodes"] += 1
    if stats["nodes"] % 1000 == 0 and cancelled():
        raise ProcessingCancelled
    stats["max_depth"] = max(stats["max_depth"], depth)
    if stats["nodes"] > 1_000_000 or depth > 100:
        raise InvalidInputError("JSON exceeds the structural complexity limit.")
    stats["types"][_json_type(value)] += 1
    if isinstance(value, dict):
        for key, item in value.items():
            stats["object_keys"][key] += 1
            _walk_json(item, stats, depth + 1, cancelled)
    elif isinstance(value, list):
        stats["array_items"] += len(value)
        for item in value:
            _walk_json(item, stats, depth + 1, cancelled)


def _json_type(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    return "array"


def _reject_constant(value):
    raise ValueError(f"Non-standard JSON constant {value} is not allowed.")


class ProcessingCancelled(Exception):
    """Raised when cancellation is observed during processing."""
