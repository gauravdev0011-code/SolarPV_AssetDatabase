"""Workspace-scoped Solar PV dashboard read API."""

from fastapi import APIRouter, HTTPException, Request

from solarpv_db.dashboard import DashboardError, DashboardNotFound
from solarpv_db.workspaces import WorkspaceNotFound

router = APIRouter(prefix="/api/v1/workspaces/{workspace_id}")


@router.get("/dashboard/overview")
def overview(workspace_id: str, request: Request, start: str | None = None,
             end: str | None = None):
    return _call(lambda: request.app.state.dashboard.overview(
        workspace_id, start=start, end=end,
    ))


@router.get("/assets")
def list_assets(workspace_id: str, request: Request, limit: int = 50,
                offset: int = 0, asset_type: str | None = None,
                search: str | None = None, site_id: str | None = None):
    return _call(lambda: request.app.state.dashboard.list_assets(
        workspace_id, limit=limit, offset=offset, asset_type=asset_type,
        search=search, site_id=site_id,
    ))


@router.get("/assets/{asset_id}")
def get_asset(workspace_id: str, asset_id: int, request: Request):
    return _call(lambda: request.app.state.dashboard.get_asset(workspace_id, asset_id))


@router.get("/assets/{asset_id}/inspections")
def list_asset_inspections(workspace_id: str, asset_id: int, request: Request,
                           limit: int = 50, offset: int = 0):
    return _call(lambda: request.app.state.dashboard.list_asset_inspections(
        workspace_id, asset_id, limit=limit, offset=offset,
    ))


@router.get("/inspections")
def list_inspections(workspace_id: str, request: Request, limit: int = 50,
                     offset: int = 0, start: str | None = None, end: str | None = None,
                     asset_id: int | None = None, status: str | None = None):
    return _call(lambda: request.app.state.dashboard.list_inspections(
        workspace_id, limit=limit, offset=offset, start=start, end=end,
        asset_id=asset_id, status=status,
    ))


@router.get("/inspections/{inspection_id}")
def get_inspection(workspace_id: str, inspection_id: int, request: Request):
    return _call(lambda: request.app.state.dashboard.get_inspection(
        workspace_id, inspection_id,
    ))


@router.get("/assets/{asset_id}/trends")
def list_asset_trend(workspace_id: str, asset_id: int, request: Request,
                     metric: str, start: str | None = None, end: str | None = None,
                     limit: int = 500):
    return _call(lambda: request.app.state.dashboard.list_trend(
        workspace_id, asset_id, metric=metric, start=start, end=end, limit=limit,
    ))


@router.get("/maintenance")
def list_maintenance(workspace_id: str, request: Request, limit: int = 50,
                     offset: int = 0, asset_id: int | None = None,
                     start: str | None = None, end: str | None = None):
    return _call(lambda: request.app.state.dashboard.list_maintenance(
        workspace_id, limit=limit, offset=offset, asset_id=asset_id,
        start=start, end=end,
    ))


@router.get("/sites")
def list_sites(workspace_id: str, request: Request, limit: int = 50,
               offset: int = 0):
    return _call(lambda: request.app.state.dashboard.list_sites(
        workspace_id, limit=limit, offset=offset,
    ))


@router.get("/sites/{site_id}")
def get_site(workspace_id: str, site_id: str, request: Request):
    return _call(lambda: request.app.state.dashboard.get_site(workspace_id, site_id))


def _call(operation):
    try:
        return operation()
    except (WorkspaceNotFound, DashboardNotFound) as error:
        raise HTTPException(404, str(error)) from error
    except DashboardError as error:
        raise HTTPException(422, str(error)) from error
