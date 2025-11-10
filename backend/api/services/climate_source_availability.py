"""
Serviço de disponibilidade de fontes de dados climáticos.

Regras EVA:
- Histórico web: 7-30 dias (NASA POWER, Open-Meteo Archive)
- Histórico email: 7-90 dias (NASA POWER, Open-Meteo Archive)
- Previsão Dashboard (tempo real): 7-30 dias (todas as APIs exceto histórico puro)

Responsabilidades
1. Define limites temporais de cada API (1990, hoje-30d, hoje+5d)
2. Detecta região geográfica (USA, Nordic ou Global)
3. Filtra APIs por contexto (data + local + tipo)
4. Determina variáveis disponíveis por região
5. Retorna quais APIs funcionam para um pedido específico
"""

from datetime import datetime, date, timedelta
from typing import Literal
from loguru import logger


class ClimateSourceAvailability:
    """Determina disponibilidade de APIs baseado em contexto."""

    # Limites temporais das APIs (padronizados EVA)
    API_LIMITS = {
        # Histórico
        "nasa_power": {
            "type": "historical",
            "start_date": datetime(1990, 1, 1).date(),  # NASA: 1990-01-01
            "end_date_offset": -2,  # hoje-2d
            "coverage": "global",
        },
        "openmeteo_archive": {
            "type": "historical",
            "start_date": datetime(1990, 1, 1).date(),  # Archive: 1990-01-01
            "end_date_offset": -2,  # hoje-2d
            "coverage": "global",
        },
        # Previsão/Recent
        "openmeteo_forecast": {
            "type": "forecast",
            "start_date_offset": -30,  # hoje-30d
            "end_date_offset": +5,  # hoje+5d
            "coverage": "global",
        },
        "met_norway": {
            "type": "forecast",
            "start_date_offset": 0,  # hoje
            "end_date_offset": +5,  # hoje+5d
            "coverage": "global",
            "regional_variables": True,  # precipitation só Nordic
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

    # Bounding box para USA continental
    USA_BBOX = {
        "lon_min": -125.0,  # West Coast
        "lon_max": -66.0,  # East Coast
        "lat_min": 24.0,  # South Florida
        "lat_max": 49.0,  # Canadian border
    }

    # Bounding box para região Nordic (MET Norway alta qualidade)
    NORDIC_BBOX = {
        "lon_min": 4.0,  # West Denmark
        "lon_max": 31.0,  # East Finland/Baltics
        "lat_min": 54.0,  # South Denmark
        "lat_max": 71.5,  # North Norway
    }

    @classmethod
    def is_in_usa(cls, lat: float, lon: float) -> bool:
        """Verifica se coordenadas estão nos EUA continental."""
        bbox = cls.USA_BBOX
        return (
            bbox["lon_min"] <= lon <= bbox["lon_max"]
            and bbox["lat_min"] <= lat <= bbox["lat_max"]
        )

    @classmethod
    def is_in_nordic(cls, lat: float, lon: float) -> bool:
        """Verifica se coordenadas estão na região Nordic."""
        bbox = cls.NORDIC_BBOX
        return (
            bbox["lon_min"] <= lon <= bbox["lon_max"]
            and bbox["lat_min"] <= lat <= bbox["lat_max"]
        )

    @classmethod
    def get_available_sources(
        cls,
        start_date: date | str,
        end_date: date | str,
        lat: float,
        lon: float,
        data_type: Literal["historical", "forecast"] = "forecast",
    ) -> dict[str, dict]:
        """
        Determina quais APIs estão disponíveis para o contexto fornecido.

        Args:
            start_date: Data inicial (date ou YYYY-MM-DD)
            end_date: Data final (date ou YYYY-MM-DD)
            lat: Latitude
            lon: Longitude
            data_type: Tipo de dado ("historical" ou "forecast")

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
        # Converter strings para date se necessário
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        today = datetime.now().date()
        result = {}

        # Verificar localização
        in_usa = cls.is_in_usa(lat, lon)
        in_nordic = cls.is_in_nordic(lat, lon)

        logger.debug(
            f"Checking availability: {start_date} to {end_date}, "
            f"location: ({lat}, {lon}), "
            f"USA: {in_usa}, Nordic: {in_nordic}, "
            f"type: {data_type}"
        )

        # Avaliar cada API
        for api_name, limits in cls.API_LIMITS.items():
            available = True
            reason = []
            variables = []

            # 1. Verificar cobertura geográfica
            if limits["coverage"] == "usa" and not in_usa:
                available = False
                reason.append("Não disponível fora dos EUA")

            # 2. Verificar compatibilidade de tipo
            api_type = limits["type"]

            if data_type == "historical":
                # Modo histórico: só aceita APIs históricas
                if api_type != "historical":
                    available = False
                    reason.append("Não é fonte histórica")
            elif data_type == "forecast":
                # Modo previsão: aceita forecast e realtime
                # Também aceita histórico se período todo no passado
                if api_type == "historical":
                    # Histórico pode servir se período está no passado
                    if end_date < today:
                        # Período todo no passado - histórico OK
                        pass
                    else:
                        # Tem futuro - histórico não serve
                        available = False
                        reason.append(
                            "Fonte histórica não cobre período futuro"
                        )
                elif api_type == "realtime":
                    # Realtime só serve para dados recentes/atuais
                    pass

            # 3. Verificar limites temporais
            if available and api_type == "historical":
                # APIs históricas: verificar limites absolutos
                api_start = limits["start_date"]
                api_end = today + timedelta(days=limits["end_date_offset"])

                if start_date < api_start:
                    available = False
                    reason.append(f"Data inicial anterior a {api_start}")

                if end_date > api_end:
                    available = False
                    reason.append(f"Data final posterior a {api_end}")

            elif available and api_type in ["forecast", "realtime"]:
                # APIs de forecast/realtime: verificar offsets
                api_start = today + timedelta(days=limits["start_date_offset"])
                api_end = today + timedelta(days=limits["end_date_offset"])

                logger.debug(
                    f"{api_name}: range {api_start} to {api_end}, "
                    f"requested {start_date} to {end_date}"
                )

                if start_date < api_start:
                    available = False
                    reason.append(f"Data inicial anterior a {api_start}")

                if end_date > api_end:
                    available = False
                    reason.append(f"Data final posterior a {api_end}")

            # 4. Determinar variáveis disponíveis
            if available:
                if api_name == "met_norway":
                    if in_nordic:
                        variables = [
                            "air_temperature",
                            "relative_humidity",
                            "wind_speed",
                            "precipitation_amount",
                        ]
                        reason.append("Região Nordic: todas as variáveis")
                    else:
                        variables = [
                            "air_temperature",
                            "relative_humidity",
                            "wind_speed",
                        ]
                        reason.append("Fora Nordic: sem precipitation_amount")
                else:
                    variables = ["all"]
                    reason.append("Todas as variáveis disponíveis")

            # Adicionar ao resultado
            result[api_name] = {
                "available": available,
                "variables": variables,
                "type": api_type,
                "coverage": limits["coverage"],
                "reason": " | ".join(reason) if reason else "Disponível",
            }

        return result

    @classmethod
    def get_compatible_sources_list(
        cls,
        start_date: date | str,
        end_date: date | str,
        lat: float,
        lon: float,
        data_type: Literal["historical", "forecast"] = "forecast",
    ) -> list[str]:
        """
        Retorna lista de APIs disponíveis (apenas nomes).

        Args:
            start_date: Data inicial
            end_date: Data final
            lat: Latitude
            lon: Longitude
            data_type: Tipo de dado

        Returns:
            Lista de nomes das APIs disponíveis
        """
        available = cls.get_available_sources(
            start_date, end_date, lat, lon, data_type
        )
        return [
            api_name
            for api_name, info in available.items()
            if info["available"]
        ]

    @classmethod
    def validate_period_for_mode(
        cls,
        start_date: date | str,
        end_date: date | str,
        mode: Literal["web", "email"],
        data_type: Literal["historical", "forecast"],
    ) -> tuple[bool, str]:
        """
        Valida período baseado no modo de operação.

        Args:
            start_date: Data inicial
            end_date: Data final
            mode: Modo ("web" ou "email")
            data_type: Tipo de dado

        Returns:
            Tupla (válido, mensagem)
        """
        # Converter strings
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        period_days = (end_date - start_date).days + 1

        # Mínimo sempre 7 dias
        if period_days < 7:
            return False, f"Período mínimo: 7 dias (atual: {period_days})"

        # Máximo depende do modo
        if mode == "web":
            max_days = 30
            if period_days > max_days:
                return (
                    False,
                    f"Período máximo para interface web: {max_days} dias "
                    f"(atual: {period_days}). "
                    f"Use download por email para períodos maiores.",
                )
        elif mode == "email" and data_type == "historical":
            max_days = 90
            if period_days > max_days:
                return (
                    False,
                    f"Período máximo para histórico por email: "
                    f"{max_days} dias (atual: {period_days})",
                )
        else:
            # Forecast por email: mesmo limite de web (30 dias)
            max_days = 30
            if period_days > max_days:
                return (
                    False,
                    f"Período máximo: {max_days} dias "
                    f"(atual: {period_days})",
                )

        return True, f"Período válido: {period_days} dias"


# Instância singleton
climate_source_availability = ClimateSourceAvailability()
