"""
Módulo para cálculo da Evapotranspiração de Referência (ETo) usando o método FAO-56 Penman-Monteith.

REFATORAÇÃO FASE 3:
- Este módulo agora funciona como WRAPPER para compatibilidade
- Lógica de cálculo movida para: EToCalculationService (eto_services.py)
- Orquestração movida para: EToProcessingService (eto_services.py)

Benefícios:
- Código testável e modular
- Responsabilidades bem-definidas
- 100% backward compatible
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from backend.core.eto_calculation.eto_services import (
    EToCalculationService,
    EToProcessingService,
)
from backend.infrastructure.celery.celery_config import celery_app as app

# Configuração do logging
logger.add(
    "./logs/eto_calculator.log",
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)

# Constantes
MATOPIBA_BOUNDS = {
    "lat_min": -14.5,
    "lat_max": -2.5,
    "lng_min": -50.0,
    "lng_max": -41.5,
}


# ============================================================================
# WRAPPERS PARA COMPATIBILIDADE (FASE 3 REFACTORING)
# ============================================================================


def calculate_eto(
    weather_df: pd.DataFrame, elevation: float, latitude: float
) -> Tuple[pd.DataFrame, List[str]]:
    """
    WRAPPER: Calcula ETo para DataFrame completo.

    Delegado para: EToCalculationService (eto_services.py)

    Args:
        weather_df: DataFrame com dados climáticos.
        elevation: Elevação em metros.
        latitude: Latitude em graus (-90 a 90).

    Returns:
        Tuple contendo (DataFrame com ETo, lista de avisos/erros)
    """
    warnings = []
    try:
        service = EToCalculationService()

        # Processar cada linha
        et0_results = []
        for idx, row in weather_df.iterrows():
            measurements = row.to_dict()
            measurements["latitude"] = latitude
            measurements["longitude"] = 0  # Padrão para compatibilidade
            measurements["date"] = (
                str(idx.date()) if hasattr(idx, "date") else str(idx)
            )
            measurements["elevation_m"] = elevation

            result = service.calculate_et0(measurements)
            et0_results.append(result["et0_mm_day"])

        weather_df["ETo"] = et0_results

        result_columns = [
            "T2M_MAX",
            "T2M_MIN",
            "RH2M",
            "WS2M",
            "ALLSKY_SFC_SW_DWN",
            "PRECTOTCORR",
            "ETo",
        ]

        logger.info("Cálculo de ETo concluído com sucesso")
        return weather_df[result_columns], warnings

    except Exception as e:
        msg = f"Erro no cálculo de ETo: {str(e)}"
        warnings.append(msg)
        logger.error(msg)
        raise


@app.task(
    bind=True,
    name="backend.core.eto_calculation.eto_calculation.calculate_eto_pipeline",
)
async def calculate_eto_pipeline(
    self,
    lat: float,
    lng: float,
    elevation: float,
    database: str,
    d_inicial: str,
    d_final: str,
    estado: Optional[str] = None,
    cidade: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    WRAPPER: Pipeline completo para cálculo de ETo.

    Delegado para: EToProcessingService (eto_services.py)

    Args:
        lat: Latitude (-90 a 90)
        lng: Longitude (-180 a 180)
        elevation: Elevação em metros
        database: Base de dados ('nasa_power' ou 'openmeteo_forecast')
        d_inicial: Data inicial (YYYY-MM-DD)
        d_final: Data final (YYYY-MM-DD)
        estado: Estado para modo MATOPIBA
        cidade: Cidade para modo MATOPIBA

    Returns:
        Tuple (dict com dados de ETo, lista de avisos/erros)
    """
    warnings = []
    try:
        # Validar coordenadas
        if not (-90 <= lat <= 90):
            raise ValueError("Latitude deve estar entre -90 e 90 graus")
        if not (-180 <= lng <= 180):
            raise ValueError("Longitude deve estar entre -180 e 180 graus")

        # Validar database
        valid_databases = ["nasa_power", "openmeteo_forecast"]
        if database not in valid_databases:
            raise ValueError(f"Base de dados inválida. Use: {valid_databases}")

        # Validar datas
        try:
            start = datetime.strptime(d_inicial, "%Y-%m-%d")
            end = datetime.strptime(d_final, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Formato de data inválido. Use: YYYY-MM-DD")

        # Validar período
        hoje = datetime.now()
        um_ano_atras = hoje - timedelta(days=365)
        amanha = hoje + timedelta(days=1)

        if start < um_ano_atras:
            raise ValueError(
                "Data inicial não pode ser anterior a 1 ano atrás"
            )
        if end > amanha:
            raise ValueError("Data final não pode ser posterior a amanhã")
        if end < start:
            raise ValueError("Data final deve ser posterior à data inicial")

        period_days = (end - start).days + 1
        if period_days < 7 or period_days > 15:
            raise ValueError("O período deve ser entre 7 e 15 dias")

        # Validar modo MATOPIBA
        is_matopiba = database == "openmeteo_forecast"
        if is_matopiba:
            if not (estado and cidade):
                raise ValueError(
                    "Estado e cidade são obrigatórios para o modo MATOPIBA"
                )
            if not (
                MATOPIBA_BOUNDS["lat_min"] <= lat <= MATOPIBA_BOUNDS["lat_max"]
                and MATOPIBA_BOUNDS["lng_min"]
                <= lng
                <= MATOPIBA_BOUNDS["lng_max"]
            ):
                warnings.append(
                    "Coordenadas fora da região típica do MATOPIBA"
                )

        # Usar EToProcessingService para orquestração
        service = EToProcessingService()
        result = await service.process_location(
            latitude=lat,
            longitude=lng,
            start_date=d_inicial,
            end_date=d_final,
            elevation=elevation,
            database=database,
            include_recomendations=True,
        )

        if "error" in result:
            raise ValueError(result["error"])

        # Converter para formato compatível
        return {"data": result.get("et0_series", [])}, warnings

    except Exception as e:
        msg = f"Erro no pipeline de ETo: {str(e)}"
        warnings.append(msg)
        logger.error(msg)
        return {}, warnings
