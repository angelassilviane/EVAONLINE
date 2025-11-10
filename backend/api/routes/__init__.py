from fastapi import APIRouter

from backend.api.routes.climate_sources import router as climate_sources_router
from backend.api.routes.eto_routes import eto_router
from backend.api.routes.health import router as health_router

# ============================================================================
# API SIMPLIFICADA - 8 endpoints essenciais
# ============================================================================

# Criar router principal
api_router = APIRouter()

# Health checks (3 endpoints)
api_router.include_router(health_router)

# ETo calculation + favorites (5 endpoints)
api_router.include_router(eto_router)

# Climate sources discovery (1 endpoint)
api_router.include_router(climate_sources_router)
