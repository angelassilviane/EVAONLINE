"""
ETo Calculation Routes
"""

import time
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from loguru import logger

from backend.database.connection import get_db
from backend.database.models.user_favorites import UserFavorites

eto_router = APIRouter(prefix="/internal/eto", tags=["ETo"])


# ============================================================================
# SCHEMAS
# ============================================================================


class EToCalculationRequest(BaseModel):
    """Request para cálculo ETo."""

    lat: float
    lng: float
    start_date: str
    end_date: str
    sources: Optional[str] = "auto"
    period_type: Optional[str] = "dashboard"  # historical, dashboard, forecast
    elevation: Optional[float] = None
    estado: Optional[str] = None
    cidade: Optional[str] = None


class LocationInfoRequest(BaseModel):
    """Request para informações de localização."""

    lat: float
    lng: float


class FavoriteRequest(BaseModel):
    """Request para favoritos."""

    user_id: str = "default"
    name: str
    lat: float
    lng: float
    cidade: Optional[str] = None
    estado: Optional[str] = None


# ============================================================================
# ENDPOINTS ESSENCIAIS (5)
# ============================================================================


@eto_router.post("/calculate")
async def calculate_eto(
    request: EToCalculationRequest, db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    ✅ Cálculo ETo principal (configurável).

    Suporta:
    - Múltiplas fontes de dados
    - Auto-detecção de melhor fonte
    - Fusão de dados (Kalman)
    - Cache automático

    Modos de operação (period_type):
    - historical: 1-90 dias (apenas NASA POWER e OpenMeteo Archive)
    - dashboard: 7-30 dias (todas as APIs disponíveis)
    - forecast: hoje até hoje+5d (apenas APIs de previsão)
    """
    try:
        from backend.core.eto_calculation.eto_services import (
            EToProcessingService,
        )
        from datetime import datetime, timedelta

        # 0. Validar period_type e período
        start = datetime.strptime(request.start_date, "%Y-%m-%d").date()
        end = datetime.strptime(request.end_date, "%Y-%m-%d").date()
        today = datetime.now().date()
        period_days = (end - start).days + 1
        period_type = request.period_type or "dashboard"

        if period_type == "historical":
            # Histórico: 1-90 dias, apenas NASA e Archive
            if period_days < 1 or period_days > 90:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Histórico: período deve ser 1-90 dias "
                        f"(atual: {period_days})"
                    ),
                )
            if end >= today:
                raise HTTPException(
                    status_code=400,
                    detail="Histórico: período deve ser no passado",
                )
            # Forçar apenas fontes históricas
            if request.sources == "auto" or not request.sources:
                request.sources = "openmeteo_archive,nasa_power"

        elif period_type == "dashboard":
            # Dashboard: 7-30 dias
            if period_days < 7 or period_days > 30:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Dashboard: período deve ser 7-30 dias "
                        f"(atual: {period_days})"
                    ),
                )

        elif period_type == "forecast":
            # Forecast: hoje até hoje+5d
            if start < today:
                raise HTTPException(
                    status_code=400,
                    detail="Forecast: data inicial deve ser >= hoje",
                )
            if end > today + timedelta(days=5):
                raise HTTPException(
                    status_code=400,
                    detail="Forecast: data final deve ser <= hoje + 5 dias",
                )

        # 1. Auto-seleção de fontes
        # data_download.py classifica automaticamente como:
        # - historical (start <= today-30d)
        # - current (passado recente 7-30 dias)
        # - forecast (end > today)
        if request.sources == "auto" or not request.sources:
            selected_source = "data fusion"
            logger.info(
                f"Auto-seleção ativada: {period_type} em "
                f"({request.lat}, {request.lng})"
            )
        else:
            selected_source = request.sources
            logger.info(f"Fontes especificadas: {selected_source}")

        # 2. Preparar string de fontes para download
        database = selected_source

        # 3. Obter elevação (se não fornecida)
        elevation = request.elevation
        if elevation is None:
            logger.info(
                f"Elevação não fornecida para ({request.lat}, {request.lng}), "
                f"será obtida via API"
            )

        # 4. Executar cálculo ETo
        service = EToProcessingService(db_session=db)
        result = await service.process_location(
            latitude=request.lat,
            longitude=request.lng,
            start_date=request.start_date,
            end_date=request.end_date,
            elevation=elevation,
            include_recomendations=False,
            database=database,
        )

        # 5. Retornar resultados
        return {
            "status": "success",
            "data": result.get("eto_data", []),
            "statistics": result.get("statistics", {}),
            "source": selected_source,
            "database_used": database,
            "warnings": result.get("warnings", []),
            "location": {
                "lat": request.lat,
                "lng": request.lng,
                "elevation_m": elevation,
            },
            "period": {
                "start": request.start_date,
                "end": request.end_date,
            },
            "timestamp": time.time(),
        }

    except ValueError as ve:
        raise HTTPException(
            status_code=400, detail=f"Formato de data inválido: {str(ve)}"
        )
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"ETo calculation failed: {str(e)}"
        )


@eto_router.post("/location-info")
async def get_location_info(request: LocationInfoRequest) -> Dict[str, Any]:
    """
    ✅ Informações de localização (timezone, elevação).
    """
    try:
        # TODO: Implementar busca real de timezone e elevação
        # Por enquanto, retorna estrutura básica
        return {
            "status": "success",
            "location": {
                "lat": request.lat,
                "lng": request.lng,
                "timezone": "America/Sao_Paulo",  # Placeholder
                "elevation_m": None,  # Placeholder
            },
            "timestamp": time.time(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get location info: {str(e)}"
        )


@eto_router.post("/favorites/add")
async def add_favorite(
    request: FavoriteRequest, db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    ✅ Adicionar favorito.
    """
    try:
        # Verificar duplicata
        existing = (
            db.query(UserFavorites)
            .filter_by(
                user_id=request.user_id, lat=request.lat, lng=request.lng
            )
            .first()
        )

        if existing:
            return {
                "status": "exists",
                "message": "Favorito já existe",
                "favorite_id": existing.id,
            }

        # Criar novo favorito
        favorite = UserFavorites(
            user_id=request.user_id,
            name=request.name,
            lat=request.lat,
            lng=request.lng,
            cidade=request.cidade,
            estado=request.estado,
        )
        db.add(favorite)
        db.commit()
        db.refresh(favorite)

        return {
            "status": "success",
            "message": "Favorito adicionado",
            "favorite_id": favorite.id,
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to add favorite: {str(e)}"
        )


@eto_router.get("/favorites/list")
async def list_favorites(
    user_id: str = "default", db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    ✅ Listar favoritos do usuário.
    """
    try:
        favorites = (
            db.query(UserFavorites)
            .filter_by(user_id=user_id)
            .order_by(UserFavorites.created_at.desc())
            .all()
        )

        return {
            "status": "success",
            "total": len(favorites),
            "favorites": [
                {
                    "id": f.id,
                    "name": f.name,
                    "lat": f.lat,
                    "lng": f.lng,
                    "cidade": f.cidade,
                    "estado": f.estado,
                    "created_at": f.created_at.isoformat(),
                }
                for f in favorites
            ],
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list favorites: {str(e)}"
        )


@eto_router.delete("/favorites/remove/{favorite_id}")
async def remove_favorite(
    favorite_id: int, user_id: str = "default", db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    ✅ Remover favorito.
    """
    try:
        favorite = (
            db.query(UserFavorites)
            .filter_by(id=favorite_id, user_id=user_id)
            .first()
        )

        if not favorite:
            raise HTTPException(
                status_code=404, detail="Favorito não encontrado"
            )

        db.delete(favorite)
        db.commit()

        return {
            "status": "success",
            "message": "Favorito removido",
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500, detail=f"Failed to remove favorite: {str(e)}"
        )
