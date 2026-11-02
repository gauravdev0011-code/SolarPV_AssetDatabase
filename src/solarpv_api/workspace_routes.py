"""Workspace and explicit site metadata routes."""

from fastapi import APIRouter, HTTPException, Request

from solarpv_db.dashboard import DashboardError, DashboardNotFound
from solarpv_db.workspaces import (
    WorkspaceConflict,
    WorkspaceError,
    WorkspaceNotFound,
)

router = APIRouter(prefix="/api/v1")


@router.post("/workspaces", status_code=201)
async def create_workspace(request: Request):
    body = await _body(request)
    try:
        return request.app.state.workspaces.create(
            name=body.get("name"), description=body.get("description"),
        )
    except (WorkspaceError, WorkspaceConflict) as error:
        raise HTTPException(422, str(error)) from error


@router.get("/workspaces")
def list_workspaces(request: Request, limit: int = 50, offset: int = 0):
    try:
        return request.app.state.workspaces.list(limit=limit, offset=offset)
    except WorkspaceError as error:
        raise HTTPException(422, str(error)) from error


@router.get("/workspaces/{workspace_id}")
def get_workspace(workspace_id: str, request: Request):
    try:
        return request.app.state.workspaces.get(workspace_id)
    except WorkspaceNotFound as error:
        raise HTTPException(404, "Workspace not found.") from error


@router.patch("/workspaces/{workspace_id}")
async def update_workspace(workspace_id: str, request: Request):
    body = await _body(request)
    try:
        return request.app.state.workspaces.update(
            workspace_id, name=body.get("name"), description=body.get("description"),
        )
    except WorkspaceNotFound as error:
        raise HTTPException(404, "Workspace not found.") from error
    except WorkspaceError as error:
        raise HTTPException(422, str(error)) from error


@router.post("/workspaces/{workspace_id}/sites", status_code=201)
async def create_site(workspace_id: str, request: Request):
    body = await _body(request)
    try:
        return request.app.state.dashboard.create_site(
            workspace_id, name=body.get("name"), location_text=body.get("location_text"),
            latitude=body.get("latitude"), longitude=body.get("longitude"),
        )
    except WorkspaceNotFound as error:
        raise HTTPException(404, "Workspace not found.") from error
    except (DashboardError, ValueError, TypeError) as error:
        raise HTTPException(422, str(error)) from error


@router.patch("/workspaces/{workspace_id}/sites/{site_id}")
async def update_site(workspace_id: str, site_id: str, request: Request):
    body = await _body(request)
    try:
        return request.app.state.dashboard.update_site(
            workspace_id, site_id, name=body.get("name"),
            location_text=body.get("location_text"), latitude=body.get("latitude"),
            longitude=body.get("longitude"),
        )
    except (WorkspaceNotFound, DashboardNotFound) as error:
        raise HTTPException(404, str(error)) from error
    except (DashboardError, ValueError, TypeError) as error:
        raise HTTPException(422, str(error)) from error


@router.put("/workspaces/{workspace_id}/assets/{asset_id}/site")
async def assign_asset_site(workspace_id: str, asset_id: int, request: Request):
    body = await _body(request)
    try:
        return request.app.state.dashboard.assign_asset_site(
            workspace_id, asset_id, body.get("site_id"),
        )
    except (WorkspaceNotFound, DashboardNotFound) as error:
        raise HTTPException(404, str(error)) from error
    except (DashboardError, ValueError, TypeError) as error:
        raise HTTPException(422, str(error)) from error


async def _body(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception as error:
        raise HTTPException(400, "Request body must be valid JSON.") from error
    if not isinstance(body, dict):
        raise HTTPException(400, "Request body must be a JSON object.")
    return body
