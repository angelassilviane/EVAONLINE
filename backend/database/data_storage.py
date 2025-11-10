"""
Funções para armazenamento de dados climáticos no banco de dados.

Suporta múltiplas fontes de dados (NASA POWER, Open-Meteo, MET Norway,
NWS Forecast, NWS Stations) com harmonização automática de variáveis.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy.exc import SQLAlchemyError

from backend.database.connection import get_db_context
from backend.database.models import APIVariables, ClimateData


# ==============================================================================
# MAPEAMENTO DE HARMONIZAÇÃO
# ==============================================================================


def get_variable_mapping(source_api: str) -> Dict[str, str]:
    """
    Obtém mapeamento de variáveis da API para nomes padronizados.

    Args:
        source_api: Nome da API fonte

    Returns:
        Dict com {variable_name: standard_name}

    Examples:
        >>> get_variable_mapping('nasa_power')
        {
            'T2M_MAX': 'temp_max_c',
            'T2M_MIN': 'temp_min_c',
            ...
        }
    """
    with get_db_context() as db:
        variables = (
            db.query(APIVariables)
            .filter(APIVariables.source_api == source_api)
            .all()
        )

        mapping = {var.variable_name: var.standard_name for var in variables}

    return mapping


def harmonize_data(
    raw_data: Dict[str, Any], source_api: str
) -> Dict[str, Any]:
    """
    Harmoniza dados de uma API para formato padronizado.

    Args:
        raw_data: Dados originais da API
        source_api: Nome da API fonte

    Returns:
        Dict com dados em formato padronizado

    Examples:
        # NASA POWER
        >>> harmonize_data(
        ...     {'T2M_MAX': 28.5, 'RH2M': 65.0},
        ...     'nasa_power'
        ... )
        {'temp_max_c': 28.5, 'humidity_percent': 65.0}

        # Open-Meteo
        >>> harmonize_data(
        ...     {'temperature_2m_max': 28.5, 'relative_humidity_2m_mean': 65.0},
        ...     'openmeteo_archive'
        ... )
        {'temp_max_c': 28.5, 'humidity_percent': 65.0}
    """
    try:
        mapping = get_variable_mapping(source_api)
        harmonized = {}

        for api_var, value in raw_data.items():
            if api_var in mapping:
                std_var = mapping[api_var]
                harmonized[std_var] = value
            else:
                # Mantém variável não mapeada com prefixo
                harmonized[f"unmapped_{api_var}"] = value

        return harmonized

    except Exception as e:
        logger.warning(
            f"Erro ao harmonizar dados de {source_api}: {e}. "
            f"Retornando dados originais."
        )
        return raw_data


# ==============================================================================
# SALVAMENTO DE DADOS - MODELO MODERNO (ClimateData)
# ==============================================================================


def save_climate_data(
    data: List[Dict[str, Any]], source_api: str, auto_harmonize: bool = True
) -> int:
    """
    Salva dados climáticos no banco usando modelo ClimateData (JSONB).

    Args:
        data: Lista de dicionários com dados climáticos
        source_api: Nome da API fonte
        auto_harmonize: Se True, harmoniza dados automaticamente

    Returns:
        Número de registros salvos

    Structure esperada de cada item em data:
        {
            'latitude': -23.55,
            'longitude': -46.63,
            'elevation': 760.0,  # Opcional
            'timezone': 'America/Sao_Paulo',  # Opcional
            'date': datetime(2020, 1, 1),
            'raw_data': {...},  # Dados originais da API
            'eto_mm_day': 4.5,  # Opcional (calculado)
            'eto_method': 'penman_monteith',  # Opcional
            'quality_flags': {...},  # Opcional
            'processing_metadata': {...}  # Opcional
        }

    Examples:
        # NASA POWER
        >>> data = [{
        ...     'latitude': -23.55,
        ...     'longitude': -46.63,
        ...     'elevation': 760.0,
        ...     'date': datetime(2020, 1, 1),
        ...     'raw_data': {
        ...         'T2M_MAX': 28.5,
        ...         'T2M_MIN': 18.2,
        ...         'RH2M': 65.0
        ...     },
        ...     'eto_mm_day': 4.5
        ... }]
        >>> save_climate_data(data, 'nasa_power')
        1

        # Open-Meteo
        >>> data = [{
        ...     'latitude': -23.55,
        ...     'longitude': -46.63,
        ...     'date': datetime(2020, 1, 1),
        ...     'raw_data': {
        ...         'temperature_2m_max': 28.5,
        ...         'temperature_2m_min': 18.2
        ...     }
        ... }]
        >>> save_climate_data(data, 'openmeteo_archive')
        1
    """
    try:
        with get_db_context() as db:
            climate_objects = []

            for d in data:
                # Harmoniza dados se solicitado
                harmonized = None
                if auto_harmonize and "raw_data" in d:
                    harmonized = harmonize_data(d["raw_data"], source_api)

                # Cria objeto ClimateData
                climate_obj = ClimateData(
                    source_api=source_api,
                    latitude=d["latitude"],
                    longitude=d["longitude"],
                    elevation=d.get("elevation"),
                    timezone=d.get("timezone"),
                    date=d["date"],
                    raw_data=d.get("raw_data", {}),
                    harmonized_data=harmonized,
                    eto_mm_day=d.get("eto_mm_day"),
                    eto_method=d.get("eto_method", "penman_monteith"),
                    quality_flags=d.get("quality_flags"),
                    processing_metadata=d.get("processing_metadata"),
                )
                climate_objects.append(climate_obj)

            # Inserção bulk
            if climate_objects:
                db.add_all(climate_objects)
                db.commit()
                count = len(climate_objects)
                logger.info(
                    f"✅ Salvos {count} registros de {source_api} "
                    f"no PostgreSQL (ClimateData)"
                )
                return count
            else:
                logger.warning("Nenhum dado para salvar")
                return 0

    except SQLAlchemyError as e:
        logger.error(f"❌ Erro SQLAlchemy ao salvar dados: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao salvar dados climáticos: {e}")
        raise


# ==============================================================================
# QUERIES E UTILITÁRIOS
# ==============================================================================


def get_climate_data(
    latitude: float,
    longitude: float,
    start_date: datetime,
    end_date: datetime,
    source_api: Optional[str] = None,
) -> List[ClimateData]:
    """
    Busca dados climáticos no banco.

    Args:
        latitude: Latitude
        longitude: Longitude
        start_date: Data inicial
        end_date: Data final
        source_api: Filtro opcional por API

    Returns:
        Lista de objetos ClimateData
    """
    with get_db_context() as db:
        query = db.query(ClimateData).filter(
            ClimateData.latitude == latitude,
            ClimateData.longitude == longitude,
            ClimateData.date >= start_date,
            ClimateData.date <= end_date,
        )

        if source_api:
            query = query.filter(ClimateData.source_api == source_api)

        results = query.order_by(ClimateData.date).all()

    return results


def check_data_exists(
    latitude: float, longitude: float, date: datetime, source_api: str
) -> bool:
    """
    Verifica se já existem dados para localização/data/fonte.

    Args:
        latitude: Latitude
        longitude: Longitude
        date: Data
        source_api: API fonte

    Returns:
        True se dados existem, False caso contrário
    """
    with get_db_context() as db:
        count = (
            db.query(ClimateData)
            .filter(
                ClimateData.latitude == latitude,
                ClimateData.longitude == longitude,
                ClimateData.date == date,
                ClimateData.source_api == source_api,
            )
            .count()
        )

    return count > 0
