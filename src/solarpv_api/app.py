"""FastAPI application factory for the local-first ingestion service."""

import json
from contextlib import asynccontextmanager

from fastapi import FastAPI

from solarpv_api.config import Settings
from solarpv_api.routes import router
from solarpv_ingestion.intake import FileIntakeService
from solarpv_ingestion.jobs import JobQueue
from solarpv_ingestion.repository import IngestionRepository
from solarpv_ingestion.storage import LocalArtifactStorage


class UploadRequestLimitMiddleware:
    """Bound actual ASGI request bytes before multipart parsing can spool them."""

    def __init__(self, app, max_request_bytes: int):
        self.app = app
        self.max_request_bytes = max_request_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") != "/api/v1/uploads" or scope.get("method") != "POST":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        length = headers.get(b"content-length")
        if length:
            try:
                if int(length) > self.max_request_bytes:
                    await self._reject(send)
                    return
            except ValueError:
                await self._reject(send)
                return
        total = 0
        exceeded = False

        async def limited_receive():
            nonlocal total, exceeded
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.max_request_bytes:
                    exceeded = True
                    return {"type": "http.disconnect"}
            return message

        async def guarded_send(message):
            if exceeded:
                return
            await send(message)

        try:
            await self.app(scope, limited_receive, guarded_send)
        except Exception:
            if not exceeded:
                raise
        if exceeded:
            await self._reject(send)

    @staticmethod
    async def _reject(send):
        body = json.dumps({"detail": "Upload request exceeds the configured size limit."}).encode()
        await send({"type": "http.response.start", "status": 413,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


def create_app(settings: Settings | None = None) -> FastAPI:
    configured = settings or Settings.from_environment()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from solarpv_db.database import initialize_database

        initialize_database(configured.database_path)
        storage = LocalArtifactStorage(configured.storage_path)
        app.state.settings = configured
        app.state.storage = storage
        app.state.repository = IngestionRepository(configured.database_path)
        app.state.queue = JobQueue(configured.database_path)
        app.state.intake = FileIntakeService(
            database_path=configured.database_path, storage=storage,
            max_file_bytes=configured.max_file_bytes,
        )
        yield

    app = FastAPI(title="SOLARIS Ingestion API", version="1.0.0", lifespan=lifespan)
    app.state.settings = configured
    app.include_router(router)
    # Multipart framing and form fields need bounded overhead in addition to file bytes.
    app.add_middleware(UploadRequestLimitMiddleware, max_request_bytes=configured.max_file_bytes + 1024 * 1024)
    return app


app = create_app()
