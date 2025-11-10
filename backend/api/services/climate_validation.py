"""
Serviço de validação centralizado para dados climáticos.

Responsabilidades:
1. Valida coordenadas (-90 a 90, -180 a 180)
2. Valida formato de datas (YYYY-MM-DD)
3. Valida período (7-30 dias) para dashboard online em tempo real
4. Valida período (7-90 dias) para requisições históricas enviadas por e-mail
5. Valida variáveis climáticas
6. Valida nome de fonte (string)
"""

from datetime import datetime
from typing import Any

from loguru import logger


class ClimateValidationService:
    """Centraliza validações de coordenadas e datas climáticas."""

    # Constantes de validação
    LAT_MIN, LAT_MAX = -90.0, 90.0
    LON_MIN, LON_MAX = -180.0, 180.0

    # Limites de período da aplicação EVA
    MIN_PERIOD_DAYS = 7  # Mínimo de 7 dias (todas as operações)
    MAX_PERIOD_DAYS = 30  # Máximo de 30 dias (interface web)

    # Variáveis válidas (padronizadas para todas as APIs)
    VALID_CLIMATE_VARIABLES = {
        # Temperatura
        "temperature_2m",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        # Umidade
        "relative_humidity_2m",
        "relative_humidity_2m_max",
        "relative_humidity_2m_min",
        "relative_humidity_2m_mean",
        # Vento (IMPORTANTE: todas as APIs fornecem a 2m após conversão)
        "wind_speed_2m",
        "wind_speed_2m_mean",
        "wind_speed_2m_ms",
        # Precipitação
        "precipitation",
        "precipitation_sum",
        # Radiação solar
        "solar_radiation",
        "shortwave_radiation_sum",
        # Evapotranspiração
        "evapotranspiration",
        "et0_fao_evapotranspiration",
        # Outras
        "pressure_msl",
    }

    # Fontes válidas (todas as 6 APIs implementadas)
    VALID_SOURCES = {
        # Global - Dados Históricos
        "openmeteo_archive",  # Histórico (1990-01-01 → hoje-2d)
        "nasa_power",  # Histórico (1990-01-01 → hoje-2d)
        # Global - Previsão/Recent
        "openmeteo_forecast",  # Recent+Forecast (hoje-30d → hoje+5d)
        "met_norway",  # Previsão (hoje → hoje+5d)
        # USA Continental - Previsão
        "nws_forecast",  # Previsão (hoje → hoje+5d)
        "nws_stations",  # Observações tempo real (hoje-1d → agora)
    }

    # NOTA: Limites temporais detalhados (start_date, end_date_offset)
    # estão em climate_source_availability.py (fonte única da verdade)
    # Este módulo apenas valida FORMATO e PERÍODO (7-30 dias)

    @staticmethod
    def validate_coordinates(
        lat: float, lon: float, location_name: str = "Location"
    ) -> tuple[bool, dict[str, Any]]:
        """
        Valida coordenadas geográficas.

        Args:
            lat: Latitude
            lon: Longitude
            location_name: Nome do local (para mensagens de erro)

        Returns:
            Tupla (válido, detalhes)
        """
        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return False, {"error": "Invalid coordinates format"}

        errors = []

        lat_min = ClimateValidationService.LAT_MIN
        lat_max = ClimateValidationService.LAT_MAX
        lon_min = ClimateValidationService.LON_MIN
        lon_max = ClimateValidationService.LON_MAX

        if not lat_min <= lat <= lat_max:
            errors.append(f"Latitude {lat} out of range ({lat_min}~{lat_max})")

        if not lon_min <= lon <= lon_max:
            errors.append(
                f"Longitude {lon} out of range " f"({lon_min}~{lon_max})"
            )

        if errors:
            logger.warning(
                f"Coordinate validation failed "
                f"for {location_name}: {errors}"
            )
            return False, {"errors": errors}

        logger.debug(f"Coordinates validated: {location_name} ({lat}, {lon})")
        return True, {"lat": lat, "lon": lon, "valid": True}

    @staticmethod
    def validate_date_range(
        start_date: str, end_date: str, allow_future: bool = False
    ) -> tuple[bool, dict[str, Any]]:
        """
        Valida intervalo de datas.

        Args:
            start_date: Data inicial (YYYY-MM-DD)
            end_date: Data final (YYYY-MM-DD)
            allow_future: Se permite datas futuras

        Returns:
            Tupla (válido, detalhes)
        """
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as e:
            return False, {"error": f"Invalid date format: {e}"}

        errors = []
        today = datetime.now().date()

        if start > end:
            errors.append(f"Start date {start} > end date {end}")

        if not allow_future:
            if start > today:
                errors.append(f"Start date {start} is in the future")
            if end > today:
                errors.append(f"End date {end} is in the future")

        # Verificar período EVAonline: mínimo 7 dias, máximo 30 dias
        period_days = (end - start).days + 1  # +1 para incluir ambos os dias
        min_days = ClimateValidationService.MIN_PERIOD_DAYS
        max_days = ClimateValidationService.MAX_PERIOD_DAYS

        if period_days < min_days:
            errors.append(
                f"Period too short: {period_days} days "
                f"(minimum {min_days} days required)"
            )

        if period_days > max_days:
            errors.append(
                f"Period too long: {period_days} days "
                f"(maximum {max_days} days allowed)"
            )

        if errors:
            logger.warning(f"Date range validation failed: {errors}")
            return False, {"errors": errors}

        logger.debug(
            f"Date range validated: {start} to {end} ({period_days} days)"
        )
        return True, {
            "start": start,
            "end": end,
            "period_days": period_days,
            "valid": True,
        }

    @staticmethod
    def validate_variables(variables: list) -> tuple[bool, dict[str, Any]]:
        """
        Valida lista de variáveis climáticas.

        Args:
            variables: Lista de variáveis desejadas

        Returns:
            Tupla (válido, detalhes)
        """
        if not variables:
            return False, {"error": "At least one variable is required"}

        invalid_vars = (
            set(variables) - ClimateValidationService.VALID_CLIMATE_VARIABLES
        )

        if invalid_vars:
            logger.warning(f"Invalid climate variables: {invalid_vars}")
            return False, {
                "error": f"Invalid variables: {invalid_vars}",
                "valid_options": list(
                    ClimateValidationService.VALID_CLIMATE_VARIABLES
                ),
            }

        logger.debug(f"Variables validated: {variables}")
        return True, {"variables": variables, "valid": True}

    @staticmethod
    def validate_source(source: str) -> tuple[bool, dict[str, Any]]:
        """
        Valida fonte de dados.

        Args:
            source: Nome da fonte

        Returns:
            Tupla (válido, detalhes)
        """
        if source not in ClimateValidationService.VALID_SOURCES:
            logger.warning(f"Invalid source: {source}")
            return False, {
                "error": f"Invalid source: {source}",
                "valid_options": list(ClimateValidationService.VALID_SOURCES),
            }

        logger.debug(f"Source validated: {source}")
        return True, {"source": source, "valid": True}

    @staticmethod
    def validate_all(
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        variables: list,
        source: str = "openmeteo_forecast",
        allow_future: bool = False,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Valida todos os parâmetros de uma vez.

        Args:
            lat, lon: Coordenadas
            start_date, end_date: Intervalo de datas
            variables: Variáveis climáticas
            source: Fonte de dados
            allow_future: Permite datas futuras

        Returns:
            Tupla (válido, detalhes)
        """
        validations = [
            (
                "coordinates",
                ClimateValidationService.validate_coordinates(lat, lon),
            ),
            (
                "date_range",
                ClimateValidationService.validate_date_range(
                    start_date, end_date, allow_future
                ),
            ),
            (
                "variables",
                ClimateValidationService.validate_variables(variables),
            ),
            ("source", ClimateValidationService.validate_source(source)),
        ]

        errors = {}
        details = {}

        for name, (valid, detail) in validations:
            if not valid:
                errors[name] = detail
            else:
                details[name] = detail

        if errors:
            logger.warning(f"Validation errors: {errors}")
            return False, {"errors": errors, "details": details}

        logger.info(f"All validations passed for ({lat}, {lon})")
        return True, {"all_valid": True, "details": details}


# Instância singleton
climate_validation_service = ClimateValidationService()
