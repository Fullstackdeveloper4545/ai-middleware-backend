import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from .routers import api as api_router
from .routers import auth as auth_router


def create_app() -> FastAPI:
    app = FastAPI(title="AI Middleware Backend")

    origins_env = os.getenv("CORS_ORIGINS", "")
    if origins_env:
        origins = [o.strip() for o in origins_env.split(",") if o.strip()]
    else:
        origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False if "*" in origins else True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(auth_router.router)
    app.include_router(api_router.router)

    @app.get("/")
    def root() -> dict:
        return {"status": "ok"}

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.head("/")
    def root_head() -> Response:
        return Response(status_code=200)

    return app


app = create_app()
