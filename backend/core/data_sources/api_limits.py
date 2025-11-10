"""
Limites de datas e validações para cada fonte de dados climáticos.

PADRONIZAÇÃO EVA (2025):
========================

DADOS HISTÓRICOS (type='historical'):
- Início: 01/01/1990 (todas as APIs)
- NASA POWER: delay de 2-7 dias para dados atuais
- Open-Meteo Archive: delay de 2 dias para dados atuais
- Limite de download: 90 dias por requisição (celery assíncrono)

DADOS ATUAIS/FORECAST (type='forecast'):
- Open-Meteo Forecast: hoje até hoje+5d
- MET Norway: hoje até hoje+5d
- NWS Forecast: hoje até hoje+5d
- Padrão EVA: 5 dias de forecast

DADOS REALTIME (type='current'):
- NWS Stations: ontem até hoje (observações em tempo real)
"""

from datetime import date, timedelta
from typing import Dict, Tuple

# ==============================================================================
# CONSTANTES GLOBAIS - PADRONIZAÇÃO EVA
# ==============================================================================

# Data padrão de início para dados históricos (TODAS AS APIs)
HISTORICAL_START_DATE = date(1990, 1, 1)

# Limites de dias para downloads históricos assíncronos
HISTORICAL_MIN_DAYS = 1  # Mínimo: 1 dia
HISTORICAL_MAX_DAYS = 90  # Máximo: 90 dias (3 meses) por requisição

# Limites de dias para dashboard em tempo real (dados atuais)
REALTIME_MIN_DAYS = 7  # Mínimo: 7 dias para análise estatística
REALTIME_MAX_DAYS = 30  # Máximo: 30 dias para dashboard tempo real

# Forecast padronizado (todas as APIs de previsão)
FORECAST_DAYS_LIMIT = 5  # Padronização EVA: 5 dias de forecast

# Threshold para classificação automática de tipo de requisição
REQUEST_TYPE_THRESHOLD_DAYS = 30  # > 30 dias do passado = histórico


# ==============================================================================
# LIMITES DE DATAS POR API
# ==============================================================================

API_DATE_LIMITS: Dict[str, Dict] = {
    # ==========================================================================
    # DADOS HISTÓRICOS (type='historical') - PADRONIZAÇÃO EVA
    # Início: 01/01/1990 para todas as APIs
    # Download assíncrono via Celery (limite: 90 dias por requisição)
    # ==========================================================================
    "nasa_power": {
        "start": date(1981, 1, 1),  # API real desde 1981
        # ⚠️ DELAY: hoje - 7 dias (processamento MERRA-2/CERES)
        "end_offset_days": -7,
        "type": "historical",
        # NASA POWER - Dados históricos globais (MERRA-2 + CERES)
        "description": "NASA POWER - Dados históricos globais",
        "coverage": "global",
        "min_padrao": HISTORICAL_START_DATE,  # ✅ PADRÃO EVA: 1990-01-01
        "notes": [
            "Delay de 2-7 dias para dados atuais (processamento MERRA-2)",
            "Resolução: 0.5° × 0.625° (MERRA-2), 1° × 1° (CERES)",
            "Community 'ag' obrigatória para dados agroclimáticos",
            "Radiação solar em MJ/m²/dia (pronta para ETo)",
        ],
    },
    "openmeteo_archive": {
        "start": date(1940, 1, 1),  # API real desde 1940
        # ⚠️ DELAY: hoje - 2 dias (processamento ERA5)
        "end_offset_days": -2,
        "type": "historical",
        # Open-Meteo Archive - Dados históricos globais (ERA5-Land)
        "description": "Open-Meteo Archive - Dados históricos globais",
        "coverage": "global",
        "min_padrao": HISTORICAL_START_DATE,  # ✅ PADRÃO EVA: 1990-01-01
        "notes": [
            "Delay de 2 dias para dados atuais (processamento ERA5)",
            "Resolução: ~11km (0.1°)",
            "ETo FAO-56 pré-calculado disponível",
            "Código aberto: github.com/open-meteo/open-meteo",
        ],
    },
    # ==========================================================================
    # DADOS ATUAIS/FORECAST (type='forecast') - PADRONIZAÇÃO EVA
    # Período: hoje até hoje+5 dias (TODAS AS APIs)
    # ==========================================================================
    "openmeteo_forecast": {
        "start_offset_days": 0,  # ✅ hoje (modo forecast puro)
        "end_offset_days": FORECAST_DAYS_LIMIT,  # ✅ hoje + 5 dias (PADRÃO EVA)
        "type": "forecast",
        "description": "Open-Meteo Forecast - Previsão global (5 dias)",
        "coverage": "global",
        "notes": [
            "Padronizado para 5 dias (API suporta até +16d)",
            "Resolução: ~11km (0.1°)",
            "ETo FAO-56 pré-calculado disponível",
            "Atualização: horária",
        ],
    },
    "met_norway": {
        "start_offset_days": 0,  # ✅ hoje
        "end_offset_days": FORECAST_DAYS_LIMIT,  # ✅ hoje + 5 dias (PADRÃO EVA)
        "type": "forecast",
        "description": "MET Norway - Forecast nórdico alta qualidade (5 dias)",
        "coverage": "nordic",  # Noruega, Suécia, Finlândia, Dinamarca
        "notes": [
            "User-Agent obrigatório",
            "Cache via header 'Expires'",
            "Rate limit: <20 req/s",
            "Dados horários agregados para diários",
        ],
    },
    "nws_forecast": {
        "start_offset_days": 0,  # ✅ hoje
        "end_offset_days": FORECAST_DAYS_LIMIT,  # ✅ hoje + 5 dias (PADRÃO EVA)
        "type": "forecast",
        "description": "NWS Forecast - Previsão USA (5 dias)",
        "coverage": "usa",
        "notes": [
            "Padronizado para 5 dias (API fornece até 7d)",
            "Cobertura: USA Continental + territórios",
            "User-Agent obrigatório",
            "Rate limit: ~5 req/s",
        ],
    },
    # ==========================================================================
    # DADOS REALTIME (type='current') - Observações em tempo real
    # Período: ontem até hoje
    # ==========================================================================
    "nws_stations": {
        "start_offset_days": -1,  # ✅ ontem
        "end_offset_days": 0,  # ✅ hoje
        "type": "current",
        "description": "NWS Stations - Observações em tempo real USA",
        "coverage": "usa",
        "notes": [
            "Dados de estações meteorológicas (MADIS)",
            "Atraso: até 20 minutos",
            "Cobertura: USA Continental + territórios",
            "Precipitação <10mm pode ser arredondada para zero",
        ],
    },
}


# ==============================================================================
# FUNÇÕES DE VALIDAÇÃO
# ==============================================================================


def get_api_date_range(source_api: str) -> Tuple[date, date]:
    """
    Retorna range de datas válido para a API.

    Args:
        source_api: Nome da API ('nasa_power', 'openmeteo_archive', etc.)

    Returns:
        Tupla (data_início, data_fim) válidas para a API

    Raises:
        ValueError: Se a API não for reconhecida

    Examples:
        >>> get_api_date_range('nasa_power')
        (datetime.date(1990, 1, 1), datetime.date(2025, 11, 4))

        >>> get_api_date_range('openmeteo_forecast')
        (datetime.date(2025, 10, 7), datetime.date(2025, 11, 22))
    """
    if source_api not in API_DATE_LIMITS:
        raise ValueError(
            f"API '{source_api}' não reconhecida. "
            f"APIs disponíveis: {list(API_DATE_LIMITS.keys())}"
        )

    limits = API_DATE_LIMITS[source_api]
    today = date.today()

    # Determina data de início
    if "start" in limits:
        # Para históricos: usa min_padrao (1990) se existir
        start_date = limits.get("min_padrao", limits["start"])
    else:
        # Para forecast/current: calcula a partir de hoje
        start_date = today + timedelta(days=limits["start_offset_days"])

    # Determina data de fim
    end_date = today + timedelta(days=limits["end_offset_days"])

    return start_date, end_date


def validate_dates_for_source(
    source_api: str,
    start_date: date,
    end_date: date,
    raise_exception: bool = True,
) -> bool:
    """
    Valida se datas são compatíveis com a API.

    Args:
        source_api: Nome da API
        start_date: Data inicial solicitada
        end_date: Data final solicitada
        raise_exception: Se True, lança exceção; se False, retorna bool

    Returns:
        True se datas são válidas, False caso contrário

    Raises:
        ValueError: Se datas forem inválidas (apenas se raise_exception=True)

    Examples:
        >>> validate_dates_for_source(
        ...     'nasa_power',
        ...     date(2020, 1, 1),
        ...     date(2020, 12, 31)
        ... )
        True

        >>> validate_dates_for_source(
        ...     'nasa_power',
        ...     date(1970, 1, 1),  # Antes de 1990
        ...     date(2020, 12, 31)
        ... )
        ValueError: Data inicial deve ser >= 01/01/1990
    """
    api_start, api_end = get_api_date_range(source_api)
    limits = API_DATE_LIMITS[source_api]

    # Valida data inicial
    if start_date < api_start:
        msg = (
            f"{source_api}: Data inicial deve ser >= "
            f"{api_start.strftime('%d/%m/%Y')}. "
            f"(Padronizamos início histórico em 1990 para garantir "
            f"dados de NASA POWER e Open-Meteo Archive)"
        )
        if raise_exception:
            raise ValueError(msg)
        return False

    # Valida data final
    if end_date > api_end:
        offset = limits.get("end_offset_days", 0)
        msg = (
            f"{source_api}: Data final deve ser <= "
            f"{api_end.strftime('%d/%m/%Y')}. "
            f"(Dados disponíveis até hoje{offset:+d} dias)"
        )
        if raise_exception:
            raise ValueError(msg)
        return False

    # Valida ordem das datas
    if start_date > end_date:
        msg = "Data inicial deve ser anterior à data final"
        if raise_exception:
            raise ValueError(msg)
        return False

    return True


def validate_period_duration(
    start_date: date, end_date: date, is_historical: bool = False
) -> None:
    """
    Valida duração do período conforme tipo de requisição.

    Args:
        start_date: Data inicial
        end_date: Data final
        is_historical: Se True, valida como histórico; se False, como real-time

    Raises:
        ValueError: Se duração for inválida

    Examples:
        # Real-time: mínimo 7 dias, máximo 30 dias
        >>> validate_period_duration(
        ...     date.today() - timedelta(days=7),
        ...     date.today(),
        ...     is_historical=False
        ... )
        # OK

        # Histórico: máximo 90 dias
        >>> validate_period_duration(
        ...     date(2020, 1, 1),
        ...     date(2020, 6, 1),
        ...     is_historical=True
        ... )
        ValueError: Limite de 90 dias para downloads históricos
    """
    days = (end_date - start_date).days + 1

    if is_historical:
        # Validação para processamento assíncrono
        if days > HISTORICAL_MAX_DAYS:
            raise ValueError(
                f"Limite de {HISTORICAL_MAX_DAYS} dias (3 meses) para "
                f"downloads históricos. Período solicitado: {days} dias. "
                f"Para períodos maiores, faça múltiplas requisições."
            )
    else:
        # Validação para dashboard em tempo real
        if days < REALTIME_MIN_DAYS:
            raise ValueError(
                f"Período mínimo: {REALTIME_MIN_DAYS} dias para "
                f"análise estatística adequada. "
                f"Período solicitado: {days} dias."
            )

        if days > REALTIME_MAX_DAYS:
            raise ValueError(
                f"Período máximo para tempo real: {REALTIME_MAX_DAYS} dias. "
                f"Período solicitado: {days} dias. "
                f"Para períodos maiores, use download histórico assíncrono."
            )


def classify_request_type(start_date: date, end_date: date) -> str:
    """
    Classifica automaticamente o tipo de requisição.

    Args:
        start_date: Data inicial
        end_date: Data final

    Returns:
        'historical' ou 'current'

    Logic:
        - Se data_inicial > hoje - 30 dias: 'current' (tempo real)
        - Caso contrário: 'historical' (processamento assíncrono)

    Examples:
        >>> classify_request_type(date(2020, 1, 1), date(2020, 12, 31))
        'historical'

        >>> classify_request_type(
        ...     date.today() - timedelta(days=7),
        ...     date.today()
        ... )
        'current'
    """
    days_from_today = (date.today() - start_date).days

    if days_from_today > REQUEST_TYPE_THRESHOLD_DAYS:
        return "historical"
    else:
        return "current"


def get_available_sources_for_period(
    start_date: date, end_date: date
) -> Dict[str, list]:
    """
    Retorna fontes disponíveis para o período solicitado.

    Args:
        start_date: Data inicial
        end_date: Data final

    Returns:
        Dict com 'historical', 'forecast' e 'current'

    Examples:
        >>> get_available_sources_for_period(
        ...     date(2020, 1, 1),
        ...     date(2020, 12, 31)
        ... )
        {
            'historical': ['nasa_power', 'openmeteo_archive'],
            'forecast': [],
            'current': []
        }
    """
    available = {"historical": [], "forecast": [], "current": []}

    for api_name, limits in API_DATE_LIMITS.items():
        try:
            if validate_dates_for_source(
                api_name, start_date, end_date, raise_exception=False
            ):
                api_type = limits["type"]
                available[api_type].append(api_name)
        except Exception:
            continue

    return available


def get_api_info(source_api: str) -> Dict:
    """
    Retorna informações sobre uma API.

    Args:
        source_api: Nome da API

    Returns:
        Dicionário com informações da API

    Examples:
        >>> get_api_info('nasa_power')
        {
            'name': 'nasa_power',
            'description': 'NASA POWER - Dados históricos globais',
            'type': 'historical',
            'coverage': 'global',
            'start_date': datetime.date(1990, 1, 1),
            'end_date': datetime.date(2025, 11, 4)
        }
    """
    if source_api not in API_DATE_LIMITS:
        raise ValueError(f"API '{source_api}' não reconhecida")

    limits = API_DATE_LIMITS[source_api]
    start_date, end_date = get_api_date_range(source_api)

    return {
        "name": source_api,
        "description": limits["description"],
        "type": limits["type"],
        "coverage": limits["coverage"],
        "start_date": start_date,
        "end_date": end_date,
    }
