"""
Open-Meteo Forecast API Client - Recent + Future Climate Data.

API: https://api.open-meteo.com/v1/forecast

Cobertura: Global

Período: (hoje - 25 dias) até (hoje + 5 dias)
Total: 30 dias (25 passado + 5 futuro)

Resolução: Diária (agregada de dados horários)

Licença: CC BY 4.0 (atribuição obrigatória)

Open-Meteo is open-source
Source code is available on GitHub under the GNU Affero General
Public Licence Version 3 AGPLv3 or any later version.
GitHub Open-Meteo: https://github.com/open-meteo/open-meteo

Variables (10):
- Temperature: max, mean, min (°C)
- Relative Humidity: max, mean, min (%)
- Wind Speed: mean at 10m (m/s)
- Shortwave Radiation: sum (MJ/m²)
- Precipitation: sum (mm)
- ET0 FAO Evapotranspiration (mm)

CACHE STRATEGY (Nov 2025):
- Redis cache via ClimateCache (opcional)
- Fallback: requests_cache local (se Redis não disponível)
- TTL dinâmico:
  * Forecast (futuro): 1h
  * Recent (passado): 6h
"""

from datetime import datetime, timedelta
from typing import Any, Dict

import openmeteo_requests
import requests_cache
from loguru import logger
from retry_requests import retry


class OpenMeteoForecastConfig:
    """Configuration for Open-Meteo Forecast API."""

    # API URL
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    # Timeline constraints
    MAX_PAST_DAYS = 25  # Pode voltar até 25 dias (hoje - 25d)
    MAX_FUTURE_DAYS = 5  # Pode prever até 5 dias (hoje + 5d)

    # Cache TTL (dados atualizam diariamente)
    CACHE_TTL = 3600 * 6  # 6 hours

    # 10 Climate variables
    DAILY_VARIABLES = [
        "temperature_2m_max",
        "temperature_2m_mean",
        "temperature_2m_min",
        "relative_humidity_2m_max",
        "relative_humidity_2m_mean",
        "relative_humidity_2m_min",
        "wind_speed_10m_mean",
        "shortwave_radiation_sum",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
    ]

    # Network settings
    TIMEOUT = 30
    RETRY_ATTEMPTS = 5
    BACKOFF_FACTOR = 0.2


class OpenMeteoForecastClient:
    """
    Client for Open-Meteo Forecast API (recent + future data).

    Supports Redis cache (via ClimateCache) with fallback to local cache.
    """

    def __init__(self, cache: Any | None = None, cache_dir: str = ".cache"):
        """
        Initialize Forecast client with caching and retry logic.

        Args:
            cache: Optional ClimateCache instance (Redis)
            cache_dir: Directory for fallback requests_cache
        """
        self.config = OpenMeteoForecastConfig()
        self.cache = cache  # Redis cache (opcional)
        self._setup_client(cache_dir)

        cache_type = "Redis" if cache else "Local"
        logger.info(
            f"OpenMeteoForecastClient initialized ({cache_type} cache, "
            f"-30d to +5d)"
        )

    def _setup_client(self, cache_dir: str):
        """Setup requests cache and retry session."""
        cache_session = requests_cache.CachedSession(
            cache_dir, expire_after=self.config.CACHE_TTL
        )
        retry_session = retry(
            cache_session,
            retries=self.config.RETRY_ATTEMPTS,
            backoff_factor=self.config.BACKOFF_FACTOR,
        )
        self.client = openmeteo_requests.Client(session=retry_session)  # type: ignore[arg-type]  # noqa: E501
        logger.debug(f"Cache dir: {cache_dir}, TTL: 6 hours")

    async def get_climate_data(
        self,
        lat: float,
        lng: float,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Get recent/future climate data from Forecast API.

        Uses Redis cache if available, with TTL based on data type:
        - Forecast (futuro): TTL 1h
        - Recent (passado): TTL 6h
        """
        # 1. Validate inputs
        self._validate_inputs(lat, lng, start_date, end_date)

        # Ajustar datas para limites da API
        from datetime import datetime, timedelta

        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        today = datetime.now().date()

        # Forecast API: (hoje - 25d) até (hoje + 5d)
        min_date = today - timedelta(days=25)
        max_date = today + timedelta(days=5)

        if start_dt.date() < min_date:
            logger.warning(
                f"Ajustando start_date de {start_date} para {min_date}"
            )
            start_date = min_date.isoformat()

        if end_dt.date() > max_date:
            logger.warning(f"Ajustando end_date de {end_date} para {max_date}")
            end_date = max_date.isoformat()

        # 2. Try Redis cache first (if available)
        if self.cache:
            cache_key = self._get_cache_key(lat, lng, start_date, end_date)
            cached_data = await self.cache.get(cache_key)

            if cached_data:
                logger.info(
                    f"✅ Cache HIT (Redis): OpenMeteo Forecast "
                    f"({lat:.4f}, {lng:.4f}) - {len(cached_data.get('climate_data', {}).get('dates', []))} days cached"
                )
                return cached_data

        # 3. Prepare API parameters
        # OpenMeteo Forecast API: usa past_days e forecast_days
        from datetime import datetime, timedelta

        # Recalcular datas após ajustes
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        today_date = datetime.now().date()

        # Calcular past_days e forecast_days
        if start_dt.date() <= today_date:
            past_days = (today_date - start_dt.date()).days
        else:
            past_days = 0

        if end_dt.date() >= today_date:
            forecast_days = (end_dt.date() - today_date).days + 1
        else:
            forecast_days = 0

        # Limites da API (corrigidos conforme documentação)
        past_days = min(past_days, 25)
        forecast_days = min(forecast_days, 5)

        params = {
            "latitude": lat,
            "longitude": lng,
            "past_days": past_days,
            "forecast_days": forecast_days,
            "daily": self.config.DAILY_VARIABLES,
            "models": "best_match",
            "timezone": "auto",
            "wind_speed_unit": "ms",
        }

        logger.info(
            f"⚠️ Cache MISS: Forecast API past_days={past_days}, "
            f"forecast_days={forecast_days} | ({lat:.4f}, {lng:.4f})"
        )
        logger.info(
            f"Requested: {start_date} to {end_date} → "
            f"API params: past_days={past_days}, forecast_days={forecast_days}"
        )
        logger.info(f"Full API params: {params}")

        # 4. Fetch data from Forecast API
        try:
            responses = self.client.weather_api(
                self.config.BASE_URL, params=params
            )
            response = responses[0]  # Single location

            # 5. Extract location metadata
            location = {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "elevation": response.Elevation(),
                "timezone": response.Timezone(),
                "timezone_abbreviation": response.TimezoneAbbreviation(),
                "utc_offset_seconds": response.UtcOffsetSeconds(),
            }

            # 6. Extract climate data
            daily = response.Daily()
            # Handle scalar (single day) vs array timestamps
            time_data = daily.Time()  # type: ignore

            logger.debug(f"Raw time_data type: {type(time_data)}")
            logger.debug(f"Raw time_data value: {time_data}")
            logger.debug(f"Has tolist: {hasattr(time_data, 'tolist')}")

            if hasattr(time_data, "tolist"):
                timestamps = time_data.tolist()  # type: ignore
            else:
                timestamps = [int(time_data)]

            logger.debug(f"Timestamps extracted: {len(timestamps)} items")
            logger.debug(f"First 3 timestamps: {timestamps[:3]}")

            dates = [datetime.fromtimestamp(ts) for ts in timestamps]

            logger.info(
                f"✅ API returned {len(dates)} days: "
                f"{dates[0].date()} to {dates[-1].date()} | "
                f"Elevation: {location['elevation']}m"
            )

            climate_data = {"dates": dates}

            # Map variables to data
            for i, var_name in enumerate(self.config.DAILY_VARIABLES):
                try:
                    values = daily.Variables(i).ValuesAsNumpy()  # type: ignore
                    # Handle scalar values (single day) vs arrays
                    if hasattr(values, "tolist"):
                        climate_data[var_name] = values.tolist()  # type: ignore  # noqa: E501
                    else:
                        # Scalar value - wrap in list
                        climate_data[var_name] = [float(values)]  # type: ignore  # noqa: E501
                except Exception as e:
                    logger.warning(f"Variable {var_name} not available: {e}")
                    climate_data[var_name] = [None] * len(dates)  # type: ignore  # noqa: E501

            # Convert wind from 10m to 2m for FAO-56 PM equation
            if "wind_speed_10m_mean" in climate_data:
                wind_10m = climate_data["wind_speed_10m_mean"]  # type: ignore
                wind_2m = [
                    self.convert_wind_10m_to_2m(w) if w is not None else None  # type: ignore  # noqa: E501
                    for w in wind_10m  # type: ignore
                ]
                climate_data["wind_speed_2m_mean"] = wind_2m  # type: ignore
                logger.debug(
                    f"✅ Converted wind 10m→2m: {len(wind_2m)} values"
                )

            # 7. Add metadata
            metadata = {
                "api": "forecast",
                "url": self.config.BASE_URL,
                "data_points": len(dates),
                "cache_ttl_hours": self._get_ttl_hours(start_date, end_date),
            }

            result = {
                "location": location,
                "climate_data": climate_data,
                "metadata": metadata,
            }

            logger.info(
                f"✅ Forecast: {len(dates)} days | "
                f"Elevation: {location['elevation']:.0f}m"
            )

            # 8. Save to Redis cache (if available)
            if self.cache:
                ttl = self._get_ttl_seconds(start_date, end_date)
                cache_key = self._get_cache_key(lat, lng, start_date, end_date)
                await self.cache.set(cache_key, result, ttl=ttl)
                logger.debug(f"💾 Cached with TTL {ttl}s")

            return result

        except Exception as e:
            logger.error(f"Forecast API error: {str(e)}")
            raise

    @staticmethod
    def convert_wind_10m_to_2m(wind_10m: float | None) -> float | None:
        """
        Converte velocidade do vento de 10m para 2m usando perfil logarítmico.

        Fórmula FAO-56 (Allen et al., 1998):
        u2 = uz × (4.87) / ln(67.8 × z - 5.42)

        Para z=10m: u2 ≈ u10 × 0.748

        Referência: FAO Irrigation and Drainage Paper 56, Chapter 3, Eq 47

        Args:
            wind_10m: Velocidade do vento a 10m (m/s)

        Returns:
            Velocidade do vento a 2m (m/s) ou None
        """
        if wind_10m is None:
            return None
        return wind_10m * 0.748

    def _get_cache_key(
        self, lat: float, lng: float, start_date: str, end_date: str
    ) -> str:
        """Generate Redis cache key."""
        lat_rounded = round(lat, 2)
        lng_rounded = round(lng, 2)
        return (
            f"climate:openmeteo:forecast:{lat_rounded}:{lng_rounded}:"
            f"{start_date}:{end_date}"
        )

    def _get_ttl_seconds(self, start_date: str, end_date: str) -> int:
        """
        Calculate TTL based on data type.

        - Forecast (futuro): 1h (dados mudam frequentemente)
        - Recent (passado): 6h (dados mais estáveis)
        """
        today = datetime.now().date()
        end = datetime.fromisoformat(end_date).date()

        if end > today:
            # Forecast data (futuro)
            return 3600  # 1 hour
        else:
            # Recent data (passado)
            return 3600 * 6  # 6 hours

    def _get_ttl_hours(self, start_date: str, end_date: str) -> int:
        """Get TTL in hours for metadata."""
        return self._get_ttl_seconds(start_date, end_date) // 3600

    def _validate_inputs(
        self, lat: float, lng: float, start_date: str, end_date: str
    ):
        """
        Validate coordinate and date range inputs.

        Raises:
            ValueError: Invalid inputs
        """
        # Coordinates
        if not -90 <= lat <= 90:
            msg = f"Invalid latitude: {lat}. Must be -90 to 90"
            raise ValueError(msg)
        if not -180 <= lng <= 180:
            msg = f"Invalid longitude: {lng}. Must be -180 to 180"
            raise ValueError(msg)

        # Date format
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
        except ValueError:
            msg = "Dates must be in YYYY-MM-DD format"
            raise ValueError(msg)

        # Date logic
        if start > end:
            msg = "start_date must be <= end_date"
            raise ValueError(msg)

        # Forecast constraints
        today = datetime.now().date()
        min_date = today - timedelta(days=self.config.MAX_PAST_DAYS)
        max_date = today + timedelta(days=self.config.MAX_FUTURE_DAYS)

        if start.date() < min_date:
            msg = (
                f"Forecast API: start_date must be >= {min_date} "
                f"(hoje - 25 dias). Use Archive API para dados "
                f"mais antigos."
            )
            raise ValueError(msg)

        if end.date() > max_date:
            msg = (
                f"Forecast API: end_date must be <= {max_date} "
                f"(hoje + 5 dias)"
            )
            raise ValueError(msg)

    @staticmethod
    def get_info() -> Dict[str, Any]:
        """
        Get information about Forecast API.

        Returns:
            Dict with API metadata
        """
        today = datetime.now().date()
        min_date = today - timedelta(days=90)
        max_date = today + timedelta(days=5)

        return {
            "api": "Open-Meteo Forecast",
            "url": "https://api.open-meteo.com/v1/forecast",
            "coverage": "Global",
            "period": f"{min_date} até {max_date}",
            "resolution": "Diária (agregada de horária)",
            "license": "CC BY 4.0",
            "attribution": "Weather data by Open-Meteo.com (CC BY 4.0)",
            "cache_ttl": "6 horas (dados atualizam diariamente)",
        }


# Factory helper
def create_forecast_client(
    cache: Any | None = None, cache_dir: str = ".cache"
) -> OpenMeteoForecastClient:
    """
    Factory function to create Forecast client.

    Args:
        cache: Optional ClimateCache instance (Redis)
        cache_dir: Fallback cache directory

    Returns:
        Configured OpenMeteoForecastClient
    """
    return OpenMeteoForecastClient(cache=cache, cache_dir=cache_dir)
