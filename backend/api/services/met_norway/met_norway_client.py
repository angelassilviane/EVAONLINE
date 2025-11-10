"""
MET Norway Locationforecast 2.0 Client with hourly-to-daily aggregation.
Usar somente "MET Norway" para MET Norway LocationForecast 2.0.

Documentation:
- https://api.met.no/weatherapi/locationforecast/2.0/documentation
- https://docs.api.met.no/doc/locationforecast/datamodel.html
- https://api.met.no/doc/locationforecast/datamodel

IMPORTANT:
- Locationforecast is GLOBAL (works anywhere)
- Returns HOURLY data that must be aggregated to daily
- No separate daily endpoint - aggregation done in backend
- 5-day forecast limit (standardized)

License: CC-BY 4.0 - Attribution required in all visualizations

Variable (from MET Norway JSON):
Instant values (hourly snapshots):
- air_temperature: Air temperature (°C)
- relative_humidity: Relative humidity (%)
- wind_speed: Wind speed at 10m (m/s)

Next 1 hour:
- precipitation_amount: Hourly precipitation (mm)

Next 6 hours:
- air_temperature_max: Maximum temperature (°C)
- air_temperature_min: Minimum temperature (°C)
- precipitation_amount: 6-hour precipitation (mm)

"""

import os
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import numpy as np
from loguru import logger
from pydantic import BaseModel, Field


class METNorwayConfig(BaseModel):
    """MET Norway API configuration."""

    # Base URL (GLOBAL, not just Europe)
    base_url: str = os.getenv(
        "MET_NORWAY_URL",
        "https://api.met.no/weatherapi/locationforecast/2.0/compact",
    )

    # Request timeout
    timeout: int = 30

    # Retry configuration
    retry_attempts: int = 3
    retry_delay: float = 1.0

    # User-Agent required (MET Norway requires identification)
    user_agent: str = os.getenv(
        "MET_NORWAY_USER_AGENT",
        "EVAonline/1.0 (https://github.com/angelassilviane/EVAONLINE)",
    )


class METNorwayDailyData(BaseModel):
    """Daily aggregated data from MET Norway API.

    Field names match our standardized output schema, but are calculated
    from MET Norway's API variable names:
    - temp_max/min: from air_temperature_max/min (next_6_hours)
    - temp_mean: calculated from hourly air_temperature (instant)
    - humidity_mean: calculated from hourly relative_humidity (instant)
    - precipitation_sum: calculated from precipitation_amount (next_1_hours)
    - wind_speed_2m_mean: converted from wind_speed (10m) using FAO-56 formula
    """

    date: datetime = Field(..., description="Date of record")
    temp_max: float | None = Field(
        None, description="Maximum temperature (°C) - from air_temperature_max"
    )
    temp_min: float | None = Field(
        None, description="Minimum temperature (°C) - from air_temperature_min"
    )
    temp_mean: float | None = Field(
        None, description="Mean temperature (°C) - from hourly air_temperature"
    )
    humidity_mean: float | None = Field(
        None,
        description="Mean relative humidity (%) - from hourly relative_humidity",
    )
    precipitation_sum: float | None = Field(
        None,
        description="Total precipitation (mm/day) - from precipitation_amount",
    )
    wind_speed_2m_mean: float | None = Field(
        None,
        description="Mean wind speed at 2m (m/s) - converted from 10m using FAO-56",
    )
    source: str = Field(default="met_norway", description="Data source")


class METNorwayCacheMetadata(BaseModel):
    """Metadata for cached MET Norway responses."""

    last_modified: str | None = Field(
        None, description="Last-Modified header from API (RFC 1123 format)"
    )
    expires: datetime | None = Field(
        None, description="Expiration timestamp (parsed from Expires header)"
    )
    data: list[METNorwayDailyData] = Field(
        ..., description="Cached forecast data"
    )


class METNorwayClient:
    """
    MET Norway client with
    GLOBAL coverage and DAILY data support.

    Regional Quality Strategy:
    - Nordic Region (NO, SE, FI, DK, Baltics): High-quality precipitation
      with radar + crowdsourced bias-correction (1km MET Nordic)
    - Rest of World: Temperature and humidity only (9km ECMWF base)
      Precipitation has lower quality without post-processing
    """

    # Nordic region bounding box (MET Nordic 1km dataset coverage)
    # Norway, Denmark, Sweden, Finland, Baltic countries
    NORDIC_BBOX = {
        "lon_min": 4.0,  # West Denmark
        "lon_max": 31.0,  # East Finland/Baltics
        "lat_min": 54.0,  # South Denmark
        "lat_max": 71.5,  # North Norway
    }

    # Daily variables available from MET Norway
    # Note: Solar radiation NOT available - use other APIs
    # API variable names (from MET Norway JSON):
    #   - air_temperature (instant, hourly)
    #   - air_temperature_max (next_6_hours)
    #   - air_temperature_min (next_6_hours)
    #   - relative_humidity (instant, hourly)
    #   - precipitation_amount (next_1_hours, next_6_hours)
    #   - wind_speed (instant, hourly)
    DAILY_VARIABLES_FOR_ETO = [
        "air_temperature_max",
        "air_temperature_min",
        "air_temperature_mean",  # Calculated from hourly air_temperature
        "precipitation_sum",  # Calculated from precipitation_amount
        "relative_humidity_mean",  # Calculated from hourly relative_humidity
    ]

    # Variables for Nordic region (high quality with bias correction)
    NORDIC_VARIABLES = [
        "air_temperature_max",
        "air_temperature_min",
        "air_temperature_mean",
        "precipitation_sum",  # High quality: radar + crowdsourced
        "relative_humidity_mean",
    ]

    # Variables for rest of world (basic ECMWF, skip precipitation)
    GLOBAL_VARIABLES = [
        "air_temperature_max",
        "air_temperature_min",
        "air_temperature_mean",
        "relative_humidity_mean",
        # precipitation_sum excluded: use Open-Meteo for better quality
    ]

    def __init__(
        self,
        config: METNorwayConfig | None = None,
        cache: Any | None = None,
    ):
        """
        Initialize MET Norway client (GLOBAL).

        Args:
            config: Custom configuration (optional)
            cache: ClimateCacheService (optional)
        """
        self.config = config or METNorwayConfig()

        # Required headers
        headers = {
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
        }

        self.client = httpx.AsyncClient(
            timeout=self.config.timeout, headers=headers
        )
        self.cache = cache

    async def close(self):
        """Close HTTP connection."""
        await self.client.aclose()

    @staticmethod
    def convert_wind_10m_to_2m(wind_10m: float | None) -> float | None:
        """
        Convert wind speed from 10m height to 2m height using FAO-56 formula.

        The FAO-56 Penman-Monteith equation requires wind speed at 2m height.
        MET Norway reports wind at 10m (standard meteorological height).

        Formula (FAO-56 Irrigation and Drainage Paper 56, Eq. 47):
            u2 = uz × [4.87 / ln(67.8 × z - 5.42)]

        For z=10m:
            u2 = u10 × [4.87 / ln(67.8 × 10 - 5.42)]
            u2 = u10 × [4.87 / ln(672.58)]
            u2 = u10 × [4.87 / 6.511]
            u2 = u10 × 0.748

        Args:
            wind_10m: Wind speed at 10m height (m/s), or None if missing

        Returns:
            Wind speed at 2m height (m/s), or None if input is None

        Reference:
            Allen, R.G., Pereira, L.S., Raes, D., Smith, M., 1998.
            Crop evapotranspiration - Guidelines for computing crop water requirements.
            FAO Irrigation and Drainage Paper 56. Food and Agriculture Organization of
            the United Nations, Rome. Chapter 3, Equation 47.
        """
        if wind_10m is None:
            return None
        return wind_10m * 0.748

    @staticmethod
    def _round_coordinates(lat: float, lon: float) -> tuple[float, float]:
        """
        Round coordinates to 4 decimal places as required by MET Norway API.

        From API documentation:
        "Most forecast models are fairly coarse, e.g. using a 1km resolution
        grid. This means there is no need to send requests with any higher
        resolution coordinates. For this reason you should never use more
        than 4 decimals in lat/lon coordinates."

        Args:
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees

        Returns:
            Tuple of (rounded_lat, rounded_lon)
        """
        return round(lat, 4), round(lon, 4)

    @staticmethod
    def _parse_rfc1123_date(date_str: str | None) -> datetime | None:
        """
        Parse RFC 1123 date format from HTTP headers.

        Args:
            date_str: Date string in RFC 1123 format
                     (e.g., "Tue, 16 Jun 2020 12:13:49 GMT")

        Returns:
            Parsed datetime or None if parsing fails
        """
        if not date_str:
            return None
        try:
            return parsedate_to_datetime(date_str)
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None

    @classmethod
    def is_in_nordic_region(cls, lat: float, lon: float) -> bool:
        """
        Check if coordinates are in Nordic region (MET Nordic 1km dataset).

        The MET Nordic dataset provides high-quality weather data with:
        - 1km resolution (vs 9km global)
        - Hourly updates (vs 4x/day global)
        - Extensive post-processing with radar and crowdsourced data
        - Bias correction for precipitation using radar + Netatmo stations

        Coverage: Norway, Denmark, Sweden, Finland, Baltic countries

        Args:
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees

        Returns:
            True if in Nordic region (high quality), False otherwise
        """
        bbox = cls.NORDIC_BBOX
        return (
            bbox["lon_min"] <= lon <= bbox["lon_max"]
            and bbox["lat_min"] <= lat <= bbox["lat_max"]
        )

    @classmethod
    def get_recommended_variables(cls, lat: float, lon: float) -> list[str]:
        """
        Get recommended variables based on location quality.

        Strategy:
        - Nordic region: Include precipitation (radar + bias-corrected)
        - Rest of world: Exclude precipitation (use Open-Meteo instead)

        Args:
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees

        Returns:
            List of recommended variable names for the location
        """
        if cls.is_in_nordic_region(lat, lon):
            logger.debug(
                f"📍 Location ({lat}, {lon}) in NORDIC region: "
                f"Using high-quality precipitation (1km + radar)"
            )
            return cls.NORDIC_VARIABLES
        else:
            logger.debug(
                f"📍 Location ({lat}, {lon}) OUTSIDE Nordic region: "
                f"Skipping precipitation (use Open-Meteo)"
            )
            return cls.GLOBAL_VARIABLES

    async def get_daily_forecast(
        self,
        lat: float,
        lon: float,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        timezone: str | None = None,
        variables: list[str] | None = None,
    ) -> list[METNorwayDailyData]:
        """
        Fetch DAILY weather forecast with pre-calculated aggregations.

        Implements MET Norway API best practices:
        - Coordinates rounded to 4 decimals (cache efficiency)
        - If-Modified-Since headers (avoid unnecessary downloads)
        - Dynamic TTL based on Expires header
        - Status code 203/429 handling

        Performs hourly-to-daily aggregation of MET Norway data including:
        - Temperature extremes and means
        - Humidity
        - Precipitation sums

        Note: Solar radiation and wind speed are NOT provided.
        Use other APIs (Open-Meteo) for radiation and wind data.

        Args:
            lat: Latitude in decimal degrees (-90 to 90)
            lon: Longitude in decimal degrees (-180 to 180)
            start_date: Start date (default: today)
            end_date: End date (default: start + 5 days)
            timezone: Timezone name (e.g., 'America/Sao_Paulo')
            variables: List of variables to fetch (default: all for ETo)

        Returns:
            List of daily aggregated weather records

        Raises:
            ValueError: Invalid coordinates or date range exceeds 5-day limit
        """
        # Validations
        if not (-90 <= lat <= 90):
            msg = f"Invalid latitude: {lat}"
            raise ValueError(msg)
        if not (-180 <= lon <= 180):
            msg = f"Invalid longitude: {lon}"
            raise ValueError(msg)

        # Round coordinates to 4 decimals (API best practice)
        lat, lon = self._round_coordinates(lat, lon)

        # Forecast horizon validation (5 days - standardized)
        now = datetime.now()
        max_forecast_date = now + timedelta(days=5)
        if end_date and end_date > max_forecast_date:
            msg = (
                f"MET Norway standardized to 5-day "
                f"forecast. Requested date: {end_date}, "
                f"limit: {max_forecast_date}"
            )
            raise ValueError(msg)

        # Default dates
        if not start_date:
            start_date = datetime.now()
        if not end_date:
            end_date = start_date + timedelta(days=5)

        if start_date > end_date:
            msg = "start_date must be <= end_date"
            raise ValueError(msg)

        # Default variables (optimized for location quality)
        if not variables:
            variables = self.get_recommended_variables(lat, lon)

        # Check if we're in Nordic region for logging
        in_nordic = self.is_in_nordic_region(lat, lon)
        region_label = "NORDIC (1km)" if in_nordic else "GLOBAL (9km)"

        logger.info(
            f"📍 MET Norway ({region_label}): "
            f"lat={lat}, lon={lon}, variables={len(variables)}"
        )

        # Build cache key
        vars_str = "_".join(sorted(variables)) if variables else "default"
        cache_key = (
            f"met_lf_{lat}_{lon}_{start_date.date()}_"
            f"{end_date.date()}_{vars_str}"
        )
        cache_metadata_key = f"{cache_key}_metadata"

        # 1. Check cache and expiration
        if self.cache:
            cached_metadata = await self.cache.get(cache_metadata_key)
            if cached_metadata:
                # Check if data has expired
                if (
                    cached_metadata.expires
                    and datetime.now() < cached_metadata.expires
                ):
                    logger.info("🎯 Cache HIT (not expired): " "MET Norway")
                    return cached_metadata.data

                # Data expired - try conditional request with If-Modified-Since
                logger.info(
                    "Cache expired, checking with If-Modified-Since..."
                )
                last_modified = cached_metadata.last_modified
            else:
                last_modified = None
        else:
            last_modified = None

        # 2. Fetch from API (with conditional request if possible)
        logger.info("Querying MET Norway API...")

        # Request parameters
        params: dict[str, float | str] = {
            "lat": lat,
            "lon": lon,
        }

        if timezone:
            params["timezone"] = timezone
            logger.warning(
                "Timezone parameter may affect date boundaries. "
                "Ensure proper handling in aggregation."
            )

        # Request with retry
        for attempt in range(self.config.retry_attempts):
            try:
                logger.debug(
                    f"MET Norway request "
                    f"(attempt {attempt + 1}/{self.config.retry_attempts}): "
                    f"lat={lat}, lon={lon}"
                )

                # Add If-Modified-Since header if we have cached data
                headers = {}
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
                    logger.debug(f"Using If-Modified-Since: {last_modified}")

                response = await self.client.get(
                    self.config.base_url, params=params, headers=headers
                )

                # Handle 304 Not Modified
                if response.status_code == 304:
                    logger.info("✅ 304 Not Modified: Using cached data")
                    if self.cache:
                        cached_metadata = await self.cache.get(
                            cache_metadata_key
                        )
                        if cached_metadata:
                            # Update expiration time
                            expires_header = response.headers.get("Expires")
                            if expires_header:
                                new_expires = self._parse_rfc1123_date(
                                    expires_header
                                )
                                cached_metadata.expires = new_expires
                                # Re-cache with updated expiration
                                ttl = self._calculate_ttl(new_expires)
                                await self.cache.set(
                                    cache_metadata_key,
                                    cached_metadata,
                                    ttl=ttl,
                                )
                            return cached_metadata.data
                    # Fallback if cache unavailable
                    return []

                # Handle 203 Non-Authoritative (deprecated/beta)
                if response.status_code == 203:
                    logger.warning(
                        "⚠️ 203 Non-Authoritative Information: "
                        "This product version is deprecated or in beta. "
                        "Check documentation for updates."
                    )

                # Handle 429 Too Many Requests (rate limiting)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "60")
                    logger.error(
                        f"❌ 429 Too Many Requests: Rate limit exceeded. "
                        f"Retry after {retry_after}s. "
                        f"Consider reducing request frequency."
                    )
                    raise httpx.HTTPStatusError(
                        f"Rate limited (429). Retry after {retry_after}s",
                        request=response.request,
                        response=response,
                    )

                response.raise_for_status()

                # Extract headers
                last_modified_header = response.headers.get("Last-Modified")
                expires_header = response.headers.get("Expires")

                logger.debug(
                    f"Response headers - "
                    f"Last-Modified: {last_modified_header}, "
                    f"Expires: {expires_header}"
                )

                # Parse expires timestamp
                expires_dt = self._parse_rfc1123_date(expires_header)

                # Process response
                data = response.json()
                parsed_data = self._parse_daily_response(
                    data, variables, start_date, end_date
                )

                logger.info(
                    f"MET Norway: " f"{len(parsed_data)} days retrieved"
                )

                # 3. Save to cache with metadata
                if self.cache and parsed_data:
                    # Create metadata object
                    metadata = METNorwayCacheMetadata(
                        last_modified=last_modified_header,
                        expires=expires_dt,
                        data=parsed_data,
                    )

                    # Calculate TTL from Expires header (with fallback)
                    ttl = self._calculate_ttl(expires_dt)

                    # Save to cache
                    await self.cache.set(cache_metadata_key, metadata, ttl=ttl)
                    logger.debug(
                        f"Cache SAVE: MET Norway"
                        f"(TTL: {ttl}s, expires: {expires_dt})"
                    )

                return parsed_data

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Don't retry on rate limiting
                    raise
                logger.warning(
                    f"MET Norway request failed "
                    f"(attempt {attempt + 1}): {e}"
                )
                if attempt == self.config.retry_attempts - 1:
                    raise
                await self._delay_retry()

            except httpx.HTTPError as e:
                logger.warning(
                    f"MET Norway request failed "
                    f"(attempt {attempt + 1}): {e}"
                )
                if attempt == self.config.retry_attempts - 1:
                    raise
                await self._delay_retry()

        return []

    @staticmethod
    def _calculate_ttl(expires: datetime | None) -> int:
        """
        Calculate cache TTL from Expires header.

        Args:
            expires: Expiration datetime from Expires header

        Returns:
            TTL in seconds (default: 3600 if no Expires header)
        """
        if not expires:
            # Default to 1 hour if no Expires header
            return 3600

        now = (
            datetime.now(expires.tzinfo) if expires.tzinfo else datetime.now()
        )
        ttl_seconds = int((expires - now).total_seconds())

        # Ensure TTL is positive and reasonable
        if ttl_seconds <= 0:
            return 60  # Minimum 1 minute
        if ttl_seconds > 86400:  # Max 24 hours
            return 86400

        return ttl_seconds

    def _parse_daily_response(
        self,
        data: dict,
        variables: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> list[METNorwayDailyData]:
        """
        Process MET Norway API response.

        Aggregates hourly data to daily with proper handling of:
        - Instant values (mean aggregation with NaN handling)
        - Precipitation (sum with 1h priority, 6h fallback)
        - Temperature extremes (6h projections preferred)
        - Derived variables (wind_speed_2m)

        Args:
            data: API response JSON
            variables: Requested variable names (for validation)
            start_date: Start of period
            end_date: End of period

        Returns:
            List of daily aggregated records
        """
        result = []

        try:
            # Extract geometry for calculations
            geometry = data.get("geometry", {})
            coordinates = geometry.get("coordinates", [])

            if len(coordinates) >= 2:
                api_lon, api_lat = coordinates[0], coordinates[1]
                api_elevation = (
                    coordinates[2] if len(coordinates) > 2 else None
                )
                logger.debug(
                    f"API geometry: lat={api_lat}, lon={api_lon}, "
                    f"elev={api_elevation}m"
                )
            else:
                api_lat, api_lon, api_elevation = None, None, None
                logger.warning("No geometry in API response")

            timeseries = data.get("properties", {}).get("timeseries", [])

            if not timeseries:
                logger.warning("MET Norway: no data")
                return []

            # Group data by day
            daily_data = {}

            for entry in timeseries:
                try:
                    # Parse timestamp
                    time_str = entry.get("time")
                    if not time_str:
                        continue

                    dt = datetime.fromisoformat(
                        time_str.replace("Z", "+00:00")
                    )
                    date_key = dt.date()

                    # Filter by period
                    if start_date and dt.date() < start_date.date():
                        continue
                    if end_date and dt.date() > end_date.date():
                        continue

                    # Initialize day if needed
                    if date_key not in daily_data:
                        daily_data[date_key] = {
                            "temp_values": [],
                            "humidity_values": [],
                            "wind_speed_values": [],
                            "precipitation_1h": [],
                            "precipitation_6h": [],
                            "temp_max_6h": [],
                            "temp_min_6h": [],
                            "count": 0,
                        }

                    day_data = daily_data[date_key]

                    # Extract instant values (REAL variable names from API)
                    # Use .get() for safe NaN/missing key handling
                    instant = (
                        entry.get("data", {})
                        .get("instant", {})
                        .get("details", {})
                    )

                    temp = instant.get("air_temperature")
                    if temp is not None:
                        day_data["temp_values"].append(temp)

                    humidity = instant.get("relative_humidity")
                    if humidity is not None:
                        day_data["humidity_values"].append(humidity)

                    wind_speed = instant.get("wind_speed")
                    if wind_speed is not None:
                        day_data["wind_speed_values"].append(wind_speed)

                    # Extract next_1_hours precipitation (PRIORITY)
                    next_1h = (
                        entry.get("data", {})
                        .get("next_1_hours", {})
                        .get("details", {})
                    )
                    precip_1h = next_1h.get("precipitation_amount")
                    if precip_1h is not None:
                        day_data["precipitation_1h"].append(precip_1h)

                    # Extract next_6_hours (fallback for precipitation)
                    next_6h = (
                        entry.get("data", {})
                        .get("next_6_hours", {})
                        .get("details", {})
                    )

                    precip_6h = next_6h.get("precipitation_amount")
                    if precip_6h is not None:
                        day_data["precipitation_6h"].append(precip_6h)

                    temp_max_6h = next_6h.get("air_temperature_max")
                    if temp_max_6h is not None:
                        day_data["temp_max_6h"].append(temp_max_6h)

                    temp_min_6h = next_6h.get("air_temperature_min")
                    if temp_min_6h is not None:
                        day_data["temp_min_6h"].append(temp_min_6h)

                    day_data["count"] += 1

                except Exception as e:
                    logger.warning(f"Error processing hourly entry: {e}")
                    continue

            # Aggregate daily data with derived variables
            for date_key, day_values in daily_data.items():
                try:
                    # Calculate aggregations with NaN handling
                    temp_mean = (
                        float(np.nanmean(day_values["temp_values"]))
                        if day_values["temp_values"]
                        else None
                    )

                    # Temp max/min: prefer next_6h, fallback to instant
                    # Use max/min of 6h forecasts (not mean) for daily extremes
                    temp_max = (
                        float(np.nanmax(day_values["temp_max_6h"]))
                        if day_values["temp_max_6h"]
                        else (
                            float(np.nanmax(day_values["temp_values"]))
                            if day_values["temp_values"]
                            else None
                        )
                    )
                    temp_min = (
                        float(np.nanmin(day_values["temp_min_6h"]))
                        if day_values["temp_min_6h"]
                        else (
                            float(np.nanmin(day_values["temp_values"]))
                            if day_values["temp_values"]
                            else None
                        )
                    )

                    humidity_mean = (
                        float(np.nanmean(day_values["humidity_values"]))
                        if day_values["humidity_values"]
                        else None
                    )

                    # Wind speed: calculate mean from 10m values,
                    # then convert to 2m
                    wind_10m_mean = (
                        float(np.nanmean(day_values["wind_speed_values"]))
                        if day_values["wind_speed_values"]
                        else None
                    )
                    wind_2m_mean = self.convert_wind_10m_to_2m(wind_10m_mean)

                    if wind_2m_mean is not None:
                        logger.debug(
                            f"✅ Converted wind 10m→2m for {date_key}: "
                            f"{wind_10m_mean:.2f} → {wind_2m_mean:.2f} m/s"
                        )

                    # Precipitation: prioritize 1h sum, fallback to 6h sum
                    if day_values["precipitation_1h"]:
                        # Best: sum of hourly values
                        precipitation_sum = float(
                            np.sum(day_values["precipitation_1h"])
                        )
                    elif day_values["precipitation_6h"]:
                        # Fallback: sum 6h values (each is 6h accumulation)
                        # Total = sum of all 6h periods in the day
                        precipitation_sum = float(
                            np.sum(day_values["precipitation_6h"])
                        )
                        logger.debug(
                            f"Using 6h precipitation fallback for {date_key}: "
                            f"{len(day_values['precipitation_6h'])} periods"
                        )
                    else:
                        precipitation_sum = 0.0

                    # Create daily record
                    daily_record = METNorwayDailyData(
                        date=date_key,
                        temp_max=temp_max,
                        temp_min=temp_min,
                        temp_mean=temp_mean,
                        humidity_mean=humidity_mean,
                        precipitation_sum=precipitation_sum,
                        wind_speed_2m_mean=wind_2m_mean,
                    )

                    result.append(daily_record)

                except Exception as e:
                    logger.warning(f"Error aggregating day {date_key}: {e}")
                    continue

            # Sort by date
            result.sort(key=lambda x: x.date)

            logger.info(
                f"MET Norway parsed: {len(result)} days "
                f"from {len(timeseries)} hourly entries"
            )
            return result

        except Exception as e:
            logger.error(
                f"❌ Error processing MET Norway response: {e}",
                exc_info=True,
            )
            msg = f"Invalid MET Norway response: {e}"
            raise ValueError(msg) from e

    async def _delay_retry(self):
        """Wait before retry attempt."""
        import asyncio

        await asyncio.sleep(self.config.retry_delay)

    async def health_check(self) -> bool:
        """
        Check if MET Norway API is accessible.

        Returns:
            True if API responds successfully, False otherwise
        """
        try:
            # Brasília, Brazil (testing with GLOBAL coordinates)
            params: dict[str, float | str] = {
                "lat": -15.7939,
                "lon": -47.8828,
            }

            response = await self.client.get(
                self.config.base_url, params=params
            )
            response.raise_for_status()

            logger.info("MET Norway health check: OK")
            return True

        except Exception as e:
            logger.error(f"MET Norway health check failed: {e}")
            return False

    def get_attribution(self) -> str:
        """
        Return CC-BY 4.0 attribution text.

        Returns:
            Attribution text as required by license
        """
        return "Weather data from MET Norway (CC BY 4.0)"

    def get_coverage_info(self) -> dict[str, Any]:
        """
        Return geographic coverage information.

        Returns:
            dict: Coverage information with regional quality tiers
        """
        return {
            "region": "GLOBAL",
            "bbox": {
                "lon_min": -180,
                "lat_min": -90,
                "lon_max": 180,
                "lat_max": 90,
            },
            "description": (
                "Global coverage with regional quality optimization"
            ),
            "forecast_horizon": "5 days ahead (standardized)",
            "data_type": "Forecast (no historical data)",
            "update_frequency": "Updated every 6 hours",
            "quality_tiers": {
                "nordic": {
                    "region": "Norway, Denmark, Sweden, Finland, Baltics",
                    "bbox": self.NORDIC_BBOX,
                    "resolution": "1 km",
                    "model": "MEPS 2.5km + downscaling",
                    "updates": "Hourly",
                    "post_processing": "Extensive (radar + crowdsourced)",
                    "variables": (
                        "Temperature, Humidity, Precipitation (high quality)"
                    ),
                    "precipitation_quality": (
                        "High (radar + Netatmo bias correction)"
                    ),
                },
                "global": {
                    "region": "Rest of World",
                    "resolution": "9 km",
                    "model": "ECMWF",
                    "updates": "4x per day",
                    "post_processing": "Minimal",
                    "variables": "Temperature, Humidity only",
                    "precipitation_quality": "Lower (use Open-Meteo instead)",
                },
            },
        }

    @classmethod
    def get_data_availability_info(cls) -> dict[str, Any]:
        """
        Return data availability information.

        Returns:
            dict: Information about temporal coverage and limitations
        """
        return {
            "data_start_date": None,  # Forecast only
            "max_historical_years": 0,
            "forecast_horizon_days": 5,
            "description": "Forecast data only, global coverage",
            "coverage": "Global",
            "update_frequency": "Every 6 hours",
        }


# Factory function
def create_met_norway_client(
    cache: Any | None = None,
) -> METNorwayClient:
    """
    Factory function to create MET Norway client
    (GLOBAL coverage).

    Args:
        cache: Optional ClimateCacheService instance

    Returns:
        Configured METNorwayClient instance
    """
    return METNorwayClient(cache=cache)
