from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from backend.api.routes import api_router
from backend.api.websocket.websocket_service import router as websocket_router
from config.logging_config import get_logger, setup_logging

# from config.settings import get_settings
from config.settings.app_config import get_legacy_settings

# Configurar logging avançado
setup_logging(log_level="INFO", log_dir="logs", json_logs=False)
logger = get_logger()

# Carregar configurações
settings = get_legacy_settings()


def create_application() -> FastAPI:
    app = FastAPI(
        title="EVAonline",
        version="1.0.0",
        openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
        docs_url=f"{settings.API_V1_PREFIX}/docs",
        redoc_url=f"{settings.API_V1_PREFIX}/redoc",
    )

    # Configurar CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Adicionar middleware Prometheus
    from backend.api.middleware.prometheus import PrometheusMiddleware

    app.add_middleware(PrometheusMiddleware)

    # Montar rotas
    app.include_router(api_router, prefix=settings.API_V1_PREFIX)
    app.include_router(websocket_router)

    # Configurar métricas Prometheus
    Instrumentator().instrument(app).expose(app, endpoint="/metrics")

    # Servir arquivos estáticos do frontend
    from fastapi.staticfiles import StaticFiles
    from pathlib import Path

    assets_dir = Path("assets")
    if assets_dir.exists():
        app.mount(
            "/frontend/assets", StaticFiles(directory="assets"), name="assets"
        )

    # Add root endpoint
    @app.get("/")
    async def root():
        """Redirect to Dash frontend."""
        return {
            "message": "EVAonline API",
            "frontend": "http://localhost:8050",
            "docs": "/docs",
        }

    return app


def mount_dash(app: FastAPI) -> FastAPI:
    """Dash will run separately on port 8050."""
    logger.info("ℹ️ Dash will run separately on port 8050")
    return app


# Criar aplicação FastAPI primeiro
app = create_application()

# Montar Dash POR ÚLTIMO (após todas as rotas da API estarem registradas)
app = mount_dash(app)

if __name__ == "__main__":
    import uvicorn

    # uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
