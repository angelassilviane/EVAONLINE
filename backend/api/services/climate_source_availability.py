"""
Serviço de disponibilidade de fontes de dados climáticos.

Regras EVAonline (3 modos de operação):
1. HISTORICAL_EMAIL: 1-90 dias, end ≤ hoje-30d (email, escolha livre)
   - NASA POWER: 1990 → hoje (sem delay, evita overlap via validação)
   - Open-Meteo Archive: 1990 → hoje-2d (com delay próprio)
2. DASHBOARD_CURRENT: [7,14,21,30] dias, end=hoje fixo (web, dropdown)
   - NASA POWER: hoje-29d → hoje (30 dias máx, sem delay)
   - Open-Meteo Archive: hoje-29d → hoje-2d (com delay 2d)
   - Open-Meteo Forecast: hoje-1d → hoje (preenche gap)
3. DASHBOARD_FORECAST: 6 dias fixo (hoje → hoje+5d), previsões web
   SE USA: radio button "Fusão das Bases" OU "Dados reais de estações"
   - Fusão (padrão):
     * Open-Meteo Forecast: hoje → hoje+5d
     * MET Norway: hoje → hoje+5d (temp+humidity, global)
     * NWS Forecast: hoje → hoje+5d (USA, previsão)
   - Estações (alternativa USA):
     * NWS Stations: apenas hoje (tempo real)

Responsabilidades:
1. Define limites temporais de cada API por modo
2. Filtra APIs por contexto (data + local + tipo)
3. Determina variáveis disponíveis por região
4. Retorna quais APIs funcionam para um pedido específico

IMPORTANTE: Detecção geográfica delega para GeographicUtils
(SINGLE SOURCE OF TRUTH para bounding boxes USA, Nordic, etc)
"""

from datetime import date, timedelta
from enum import StrEnum
from typing import Any
from loguru import logger

from backend.api.services.geographic_utils import GeographicUtils


class OperationMode(StrEnum):  # StrEnum para compatibilidade Pydantic v2
    """Enum para os 3 modos de operação EVAonline."""

    HISTORICAL_EMAIL = "historical_email"
    DASHBOARD_CURRENT = "dashboard_current"
    DASHBOARD_FORECAST = "dashboard_forecast"


class ClimateSourceAvailability:
    """Determina disponibilidade de APIs baseado em contexto."""

    # Constante centralizada para data de início histórica (evita duplicação)
    HISTORICAL_START_DATE = date(1990, 1, 1)

    # Limites temporais das APIs (padronizados EVA)
    API_LIMITS: dict[str, dict[str, Any]] = {
        # Histórico
        "nasa_power": {
            "type": "historical",
            "start_date": HISTORICAL_START_DATE,  # Referência centralizada
            "end_date_offset": 0,  # hoje (sem delay)
            "coverage": "global",
        },
        "openmeteo_archive": {
            "type": "historical",
            "start_date": HISTORICAL_START_DATE,  # 1990-01-01
            "end_date_offset": -30,  # hoje-30d (HISTORICAL_EMAIL)
            "coverage": "global",
        },
        # Previsão/Recent
        "openmeteo_forecast": {
            "type": "forecast",
            "start_date_offset": -1,  # hoje-1d (DASHBOARD_FORECAST)
            "end_date_offset": +5,  # hoje+5d
            "coverage": "global",
        },
        "met_norway": {
            "type": "forecast",
            "start_date_offset": 0,  # hoje
            "end_date_offset": +5,  # hoje+5d
            "coverage": "global",
            # Global: temp+humidity apenas | Nordic: +wind+precipitation
            "regional_variables": True,
        },
        "nws_forecast": {
            "type": "forecast",
            "start_date_offset": 0,  # hoje
            "end_date_offset": +5,  # hoje+5d
            "coverage": "usa",
        },
        "nws_stations": {
            "type": "realtime",
            "start_date_offset": -1,  # hoje-1d
            "end_date_offset": 0,  # agora
            "coverage": "usa",
        },
    }

    @classmethod
    def _parse_date(cls, date_input: date | str) -> date:
        """Parser privado e validado para datas (ISO ou date)."""
        try:
            if isinstance(date_input, str):
                return date.fromisoformat(
                    date_input
                )  # Mais robusto que strptime
            return date_input
        except ValueError as e:
            logger.error(f"Data inválida '{date_input}': {e}")
            raise ValueError("Formato de data inválido: use YYYY-MM-DD") from e

    @classmethod
    def get_available_sources(
        cls,
        start_date: date | str,
        end_date: date | str,
        lat: float,
        lon: float,
    ) -> dict[str, dict[str, Any]]:
        """
        Determina quais APIs estão disponíveis para o contexto fornecido.

        NOTA: NÃO valida período (min/max dias).
        Apenas verifica:
        1. Cobertura geográfica (USA, Nordic, Global)
        2. Limites temporais (cada API tem seus próprios limites)

        Validação de modo (1-90 dias, etc) é responsabilidade de
        climate_validation.py::validate_request_mode()

        Args:
            start_date: Data inicial (date ou YYYY-MM-DD)
            end_date: Data final (date ou YYYY-MM-DD)
            lat: Latitude
            lon: Longitude

        Returns:
            Dict com APIs disponíveis e suas características:
            {
                "nasa_power": {
                    "available": True,
                    "variables": ["all"],
                    "reason": "..."
                },
                ...
            }
        """
        # Parser validado (com raise se inválido)
        start_parsed = cls._parse_date(start_date)
        end_parsed = cls._parse_date(end_date)

        # Validação básica: start <= end
        if start_parsed > end_parsed:
            raise ValueError(
                f"start_date ({start_parsed}) deve ser <= "
                f"end_date ({end_parsed})"
            )

        today = date.today()  # Sempre use today() para consistência
        result: dict[str, dict[str, Any]] = {}

        # Verificar localização (com log bind para contexto)
        in_usa = GeographicUtils.is_in_usa(lat, lon)
        in_nordic = GeographicUtils.is_in_nordic(lat, lon)

        logger.bind(
            start=start_parsed,
            end=end_parsed,
            lat=lat,
            lon=lon,
            usa=in_usa,
            nordic=in_nordic,
        ).debug("Verificando disponibilidade de fontes climáticas")

        # Avaliar cada API
        for api_name, limits in cls.API_LIMITS.items():
            available = True
            reasons: list[str] = []  # Lista para juntar motivos
            variables: list[str] = []

            # 1. Verificar cobertura geográfica
            if limits["coverage"] == "usa" and not in_usa:
                available = False
                reasons.append("Não disponível fora dos EUA")

            # 2. Verificar limites temporais
            if available:
                api_type = limits["type"]
                if api_type == "historical":
                    # APIs históricas: verificar limites absolutos
                    api_start = limits["start_date"]
                    api_end = today + timedelta(days=limits["end_date_offset"])

                    if start_parsed < api_start:
                        available = False
                        reasons.append(f"Data inicial anterior a {api_start}")
                    if end_parsed > api_end:
                        available = False
                        reasons.append(f"Data final posterior a {api_end}")

                elif api_type in ["forecast", "realtime"]:
                    # APIs de forecast/realtime: verificar offsets
                    api_start = today + timedelta(
                        days=limits.get("start_date_offset", 0)
                    )
                    api_end = today + timedelta(
                        days=limits.get("end_date_offset", 0)
                    )

                    logger.bind(api=api_name).debug(
                        f"Range API: {api_start} to {api_end}, "
                        f"solicitado: {start_parsed} to {end_parsed}"
                    )

                    if start_parsed < api_start:
                        available = False
                        reasons.append(f"Data inicial anterior a {api_start}")
                    if end_parsed > api_end:
                        available = False
                        reasons.append(f"Data final posterior a {api_end}")

            # 3. Determinar variáveis disponíveis (se disponível)
            if available:
                if api_name == "met_norway" and limits.get(
                    "regional_variables"
                ):
                    if in_nordic:
                        variables = [
                            "temperature_2m_mean",
                            "relative_humidity_2m_mean",
                            "wind_speed_10m_mean",
                            "precipitation_sum",
                        ]
                        reasons.append(
                            "Região Nordic: temp+humidity+wind+precip"
                        )
                    else:
                        variables = [
                            "temperature_2m_mean",
                            "relative_humidity_2m_mean",
                        ]
                        reasons.append("Global: temp+humidity apenas")
                else:
                    variables = ["all"]
                    reasons.append("Todas as variáveis disponíveis")

            # Adicionar ao resultado
            result[api_name] = {
                "available": available,
                "variables": variables,
                "type": api_type,
                "coverage": limits["coverage"],
                "reason": (
                    " | ".join(reasons)
                    if reasons
                    else "Disponível sem restrições"
                ),
            }

        return result

    @classmethod
    def get_compatible_sources_list(
        cls,
        start_date: date | str,
        end_date: date | str,
        lat: float,
        lon: float,
    ) -> list[str]:
        """
        Retorna lista de APIs disponíveis (apenas nomes).

        Args:
            start_date: Data inicial
            end_date: Data final
            lat: Latitude
            lon: Longitude

        Returns:
            Lista de nomes das APIs disponíveis
        """
        available = cls.get_available_sources(start_date, end_date, lat, lon)
        return [
            api_name
            for api_name, info in available.items()
            if info["available"]
        ]

    @classmethod
    def is_source_available(
        cls,
        source_id: str,
        mode: OperationMode | str,
        start_date: date | str,
        end_date: date | str,
    ) -> bool:
        """
        Verifica se uma fonte específica está disponível para o contexto.

        Args:
            source_id: ID da fonte (ex: 'nasa_power', 'openmeteo_archive')
            mode: Modo de operação
            start_date: Data inicial
            end_date: Data final

        Returns:
            True se a fonte está disponível
        """
        try:
            # Converter mode para OperationMode se necessário
            if isinstance(mode, str):
                mode = OperationMode(mode)

            # Obter limites da API para o contexto
            limits = cls.get_api_date_limits_for_context(mode)

            if source_id not in limits:
                return False

            api_limits = limits[source_id]

            # Verificar se as datas estão dentro dos limites
            start_parsed = cls._parse_date(start_date)
            end_parsed = cls._parse_date(end_date)

            min_date = api_limits["start_date"]
            max_date = api_limits["end_date"]

            return (
                min_date <= start_parsed <= max_date
                and min_date <= end_parsed <= max_date
            )

        except Exception:
            return False

    @classmethod
    def get_api_date_limits_for_context(
        cls,
        context: str | OperationMode,
        today: date | None = None,
    ) -> dict[str, dict[str, Any]]:
        """
        Retorna limites de data específicos por API e contexto.

        Regras EVAonline (ref: 15/11/2025 - Atualizado para data atual):

        HISTORICAL_EMAIL (dados antigos, período LIVRE, entrega email):
        - NASA POWER: 1990-01-01 → hoje (SEM delay)
        - Open-Meteo Archive: 1990-01-01 → hoje-2d (com delay 2d)
        - Usuário escolhe LIVREMENTE as datas (qualquer ano desde 1990)
        - Restrições: mínimo 1 dia, máximo 90 dias de período
        - Validação: end ≤ hoje-30d (conforme climate_validation.py)

        Exemplos válidos (ref 15/11/2025):
        - 18/07/2025 → 15/10/2025 (89 dias, 2025)
        - 01/05/2013 → 29/07/2013 (90 dias, 2013)
        - 01/01/1990 → 31/01/1990 (31 dias, 1990)

        DASHBOARD_CURRENT (período DROPDOWN, dados recentes, web):
        - Fim SEMPRE: hoje (15/11/2025)
        - Início calculado: hoje - (dias-1) para incluir hoje
        - NASA POWER: hoje-29d → hoje (SEM delay, cobertura completa)
        - Open-Meteo Archive: hoje-29d → hoje-2d (com delay 2d)
        - Open-Meteo Forecast: hoje-30d → hoje (preenche gap Archive)

        Exemplos válidos:
        - 17/10/2025 → 15/11/2025 (30 dias: hoje-29d a hoje)
        - 09/11/2025 → 15/11/2025 (7 dias: hoje-6d a hoje)

        DASHBOARD_FORECAST (próximos 5 dias FIXO, web):
        - Período: 6 dias total fixo (hoje até hoje+5d, inclusive)
        - SE USA: radio button com 2 opções:
          A) "Fusão das Bases" (padrão):
             * Open-Meteo Forecast: hoje → hoje+5d
             * MET Norway: hoje → hoje+5d (temp+humidity)
             * NWS Forecast: hoje → hoje+5d (se solicitação <20h)
          B) "Dados reais de estações" (alternativa):
             * NWS Stations: apenas hoje (tempo real)
             * Mostra quantas estações encontradas perto
             * Todas variáveis disponíveis
             * Botão download

        Exemplo (ref 15/11/2025):
        - 15/11/2025 → 20/11/2025 (6 dias: 15,16,17,18,19,20)

        Args:
            context: Contexto da requisição
            today: Data de referência (default: hoje)

        Returns:
            Dict com limites por API:
            {
                "nasa_power": {
                    "start_date": date(1990, 1, 1),
                    "end_date": date(2025, 10, 15),
                    "reason": "..."
                },
                ...
            }
        """
        if today is None:
            today = date.today()

        # Normalizar context para string
        context_str = (
            context.value if isinstance(context, OperationMode) else context
        )

        limits: dict[str, dict[str, Any]] = {}

        if context_str == OperationMode.HISTORICAL_EMAIL.value:
            # Historical_email: APIs fornecem de 1990 até hoje-30 dias
            # Usuário escolhe LIVREMENTE dentro deste range
            # Restrições: 1-90 dias de período
            # Limite hoje-30d evita overlap com dashboard_current
            historical_end = today - timedelta(days=30)
            common_reason = (
                f"Historical email: 1990 to {historical_end} "
                "(user free choice, 1-90 days period)"
            )

            limits["nasa_power"] = {
                "start_date": cls.HISTORICAL_START_DATE,
                "end_date": historical_end,
                "min_period_days": 1,
                "max_period_days": 90,
                "reason": common_reason,
            }

            limits["openmeteo_archive"] = {
                "start_date": cls.HISTORICAL_START_DATE,
                "end_date": historical_end,
                "min_period_days": 1,
                "max_period_days": 90,
                "reason": common_reason,
            }

        elif context_str == OperationMode.DASHBOARD_CURRENT.value:
            # Dashboard_current: [7,14,21,30] dias dropdown, end=hoje fixo
            # NASA POWER: sem delay, end = hoje
            # Open-Meteo Archive: delay 2d, end = hoje-2d
            # Open-Meteo Forecast: cobre gap dos últimos dias
            # NOTA: start_date varia conforme período selecionado
            dashboard_start_30d = today - timedelta(days=29)  # 30 dias max
            archive_end = today - timedelta(days=2)

            limits["nasa_power"] = {
                "start_date": dashboard_start_30d,
                "end_date": today,
                "reason": (
                    f"Dashboard current: {dashboard_start_30d} to {today} "
                    "(no delay, complete coverage)"
                ),
            }

            limits["openmeteo_archive"] = {
                "start_date": dashboard_start_30d,
                "end_date": archive_end,
                "reason": (
                    f"Dashboard current: {dashboard_start_30d} to "
                    f"{archive_end} (2-day delay)"
                ),
            }

            limits["openmeteo_forecast"] = {
                "start_date": today - timedelta(days=30),
                "end_date": today,
                "reason": (
                    f"Dashboard current: {today - timedelta(days=30)} to "
                    f"{today} (fills archive gap, recent data)"
                ),
            }

        elif context_str == OperationMode.DASHBOARD_FORECAST.value:
            # Dashboard_forecast: próximos 5 dias fixo (hoje → hoje+5d)
            # Período fixo: 6 dias total (inclusive)
            forecast_end = today + timedelta(days=5)
            common_reason = (
                f"Dashboard forecast: {today} to {forecast_end} "
                "(6 days fixed period)"
            )

            limits["openmeteo_forecast"] = {
                "start_date": today,
                "end_date": forecast_end,
                "reason": common_reason,
            }

            limits["met_norway"] = {
                "start_date": today,
                "end_date": forecast_end,
                "reason": common_reason,
            }

            limits["nws_forecast"] = {
                "start_date": today,
                "end_date": forecast_end,
                "reason": f"{common_reason} (USA only)",
                "coverage": "usa",
            }

            # NWS Stations: opção alternativa para USA
            # (radio button: "Fusão" vs "Estações reais")
            limits["nws_stations"] = {
                "start_date": today,
                "end_date": today,
                "reason": (
                    f"Dashboard forecast: {today} only "
                    "(realtime stations, USA only, alternative mode)"
                ),
                "coverage": "usa",
            }

        return limits
