"""Versioned routes for local file intake and processing results."""

import hashlib
import json
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request

from solarpv_ingestion.intake import UnsafeUploadError
from solarpv_ingestion.storage import UploadTooLargeError
from solarpv_api.schemas import job_response, source_response as serialize_source_response
from solarpv_db.workspaces import WorkspaceNotFound

router = APIRouter()


@router.post("/uploads", status_code=202)
async def upload_file(request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        request.app.state.workspaces.get(workspace_id)
    except WorkspaceNotFound as error:
        raise HTTPException(404, "Workspace not found.") from error
    content_type = request.headers.get("content-type", "").lower()
    if not content_type.startswith("multipart/form-data"):
        raise HTTPException(415, "Upload must use multipart/form-data.")
    try:
        form = await request.form(max_files=1, max_fields=1)
    except Exception as error:
        raise HTTPException(400, "Invalid multipart upload.") from error
    files = [(key, value) for key, value in form.multi_items() if hasattr(value, "filename")]
    if len(files) != 1 or files[0][0] != "file" or len(form.multi_items()) != 1:
        raise HTTPException(400, "Provide exactly one file field named 'file'.")
    upload = files[0][1]
    filename = upload.filename or ""
    suffix = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if suffix not in {"csv", "json"}:
        await upload.close()
        raise HTTPException(415, "Only CSV and JSON uploads are supported.")
    try:
        accepted = request.app.state.intake.accept(
            upload.file, workspace_id=workspace_id, original_filename=filename,
            media_type=upload.content_type,
        )
    except UploadTooLargeError as error:
        raise HTTPException(413, str(error)) from error
    except (UnsafeUploadError, ValueError) as error:
        raise HTTPException(400, str(error)) from error
    finally:
        await upload.close()

    job, reused = request.app.state.queue.create_or_get_job(
        job_id=uuid4().hex, workspace_id=accepted.source_file.workspace_id,
        source_file_id=accepted.source_file.source_file_id,
        idempotency_key=f"profile-v1:{accepted.source_file.source_file_id}",
    )
    return {
        "source": serialize_source_response(accepted.source_file, duplicate=accepted.duplicate),
        "job_id": job.job_id,
        "job_state": job.state.value,
        "job_reused": reused,
    }


@router.get("/uploads/{source_file_id}")
def get_upload(source_file_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    source = request.app.state.repository.get_source_file(source_file_id, workspace_id=workspace_id)
    if source is None:
        raise HTTPException(404, "Source file not found.")
    return serialize_source_response(source)


@router.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    status = request.app.state.repository.get_job_status(job_id, workspace_id=workspace_id)
    if status is None:
        raise HTTPException(404, "Job not found.")
    return job_response(status)


@router.get("/jobs/{job_id}/result")
def get_result(job_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    artifact = request.app.state.repository.get_job_result(job_id, workspace_id=workspace_id)
    if artifact is None:
        status = request.app.state.repository.get_job_status(job_id, workspace_id=workspace_id)
        if status is None:
            raise HTTPException(404, "Job not found.")
        raise HTTPException(409, "Processing result is not available.")
    try:
        content = request.app.state.storage.read_bytes(artifact.storage_key)
        if hashlib.sha256(content).hexdigest() != artifact.checksum:
            raise ValueError("checksum mismatch")
        result = json.loads(content)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise HTTPException(500, "Stored processing result failed integrity verification.") from error
    return {
        "job_id": job_id,
        "source_file_id": artifact.source_file_id,
        "artifact_id": artifact.artifact_id,
        "parser_id": artifact.parser_id,
        "parser_version": artifact.parser_version,
        "schema_version": artifact.schema_version,
        "checksum": artifact.checksum,
        "result": result,
    }


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        runtime = request.app.state.queue.request_cancellation(job_id, workspace_id=workspace_id)
    except KeyError as error:
        raise HTTPException(404, "Job not found.") from error
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return {"job_id": job_id, "state": runtime.state.value}


def _workspace_for_route(request: Request, path_workspace_id: str) -> str:
    """Keep legacy routes pinned to default; scoped routes use their path ID."""
    if request.url.path.startswith("/api/v1/workspaces/"):
        return path_workspace_id
    return "default"
