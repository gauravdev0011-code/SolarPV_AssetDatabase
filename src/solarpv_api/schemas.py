"""Small response serializers for the ingestion API."""

from dataclasses import asdict


def source_response(source, *, duplicate: bool | None = None) -> dict:
    result = {
        "source_file_id": source.source_file_id,
        "workspace_id": source.workspace_id,
        "original_name": source.original_name,
        "media_type": source.media_type,
        "size_bytes": source.size_bytes,
        "sha256": source.sha256,
        "state": source.state.value,
    }
    if duplicate is not None:
        result["duplicate"] = duplicate
    return result


def job_response(status) -> dict:
    result = asdict(status)
    result["state"] = status.state.value
    result["result_available"] = status.artifact_id is not None and status.state.value == "succeeded"
    result.pop("artifact_id", None)
    return result
