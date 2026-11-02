"""Dataset management API routes for the existing SOLARIS application."""

import json
import sqlite3

from fastapi import APIRouter, HTTPException, Request

from solarpv_ingestion.datasets import DatasetConflict, DatasetError, DatasetNotFound

router = APIRouter()


@router.post("/datasets", status_code=201)
async def create_dataset(request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.create_dataset(
            name=body.get("name"), job_id=body.get("job_id"),
            idempotency_key=body.get("idempotency_key"), workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.post("/datasets/{dataset_id}/versions", status_code=201)
async def create_version(dataset_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.create_version(
            dataset_id, job_id=body.get("job_id"), idempotency_key=body.get("idempotency_key"),
            workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets")
def list_datasets(request: Request, limit: int = 50, offset: int = 0,
                  workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.list_datasets(limit=limit, offset=offset, workspace_id=workspace_id)
    except DatasetError as error:
        raise HTTPException(422, str(error)) from error


@router.get("/datasets/{dataset_id}")
def get_dataset(dataset_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_dataset(dataset_id, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets/{dataset_id}/versions")
def list_versions(dataset_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return {"items": request.app.state.datasets.list_versions(dataset_id, workspace_id=workspace_id)}
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets/{dataset_id}/versions/{version_id}")
def get_version(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_version(dataset_id, version_id, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets/{dataset_id}/versions/{version_id}/profile")
def get_profile(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_profile(dataset_id, version_id, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets/{dataset_id}/versions/{version_id}/preview")
def get_preview(dataset_id: str, version_id: str, request: Request, limit: int = 20,
                workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_preview(dataset_id, version_id, limit=limit, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


@router.put("/datasets/{dataset_id}/versions/{version_id}/mapping", status_code=201)
async def save_mapping(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.save_mapping(
            dataset_id, version_id, columns=body.get("columns"),
            idempotency_key=body.get("idempotency_key"), workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/datasets/{dataset_id}/versions/{version_id}/mapping")
def get_mapping(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return {"mapping": request.app.state.datasets.get_mapping(dataset_id, version_id, workspace_id=workspace_id)}
    except Exception as error:
        _raise_dataset_error(error)


@router.post("/datasets/{dataset_id}/versions/{version_id}/validate", status_code=201)
async def validate_dataset(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.validate(
            dataset_id, version_id, idempotency_key=body.get("idempotency_key"), workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/validation-runs/{validation_run_id}")
def get_validation(validation_run_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_validation(validation_run_id, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


@router.post("/datasets/{dataset_id}/versions/{version_id}/approvals", status_code=201)
async def create_approval(dataset_id: str, version_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.create_approval(
            dataset_id, version_id, validation_run_id=body.get("validation_run_id"),
            idempotency_key=body.get("idempotency_key"), workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.post("/approvals/{approval_id}/decision")
async def decide_approval(approval_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    body = await _body(request)
    try:
        return request.app.state.datasets.decide_approval(
            approval_id, decision=body.get("decision"),
            reviewer_identity=body.get("reviewer_identity"), workspace_id=workspace_id,
        )
    except Exception as error:
        _raise_dataset_error(error)


@router.get("/approvals/{approval_id}")
def get_approval(approval_id: str, request: Request, workspace_id: str = "default"):
    workspace_id = _workspace_for_route(request, workspace_id)
    try:
        return request.app.state.datasets.get_approval(approval_id, workspace_id=workspace_id)
    except Exception as error:
        _raise_dataset_error(error)


async def _body(request: Request) -> dict:
    try:
        body = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise HTTPException(400, "Request body must be valid JSON.") from error
    if not isinstance(body, dict):
        raise HTTPException(422, "Request body must be a JSON object.")
    return body


def _raise_dataset_error(error: Exception):
    if isinstance(error, DatasetNotFound):
        raise HTTPException(404, "Dataset resource not found.") from error
    if isinstance(error, DatasetConflict):
        raise HTTPException(409, str(error)) from error
    if isinstance(error, (DatasetError, ValueError)):
        raise HTTPException(422, str(error)) from error
    if isinstance(error, sqlite3.IntegrityError):
        raise HTTPException(409, "Dataset operation conflicts with persisted integrity constraints.") from error
    raise error


def _workspace_for_route(request: Request, path_workspace_id: str) -> str:
    """Keep legacy routes pinned to default; scoped routes use their path ID."""
    if request.url.path.startswith("/api/v1/workspaces/"):
        return path_workspace_id
    return "default"
