"""
Adapter síncrono para MET Norway 2.0.

GLOBAL com dados DIÁRIOS e ESTRATÉGIA REGIONAL.

Este adapter permite usar o cliente assíncrono MET Norway
em código síncrono, facilitando a integração com data_download.py.

Características:
GLOBAL (qualquer coordenada do mundo)
Dados DIÁRIOS agregados de dados horários
ESTRATÉGIA REGIONAL para qualidade otimizada:
   - Nordic (NO/SE/FI/DK/Baltics): Temp + Humidity + Precipitation
     (1km MET Nordic, radar + crowdsourced bias correction)
   - Rest of World: Temp + Humidity only
     (9km ECMWF, skip precipitation - use Open-Meteo instead)
Variáveis otimizadas para ETo FAO-56
Sem limite de cobertura

Licença: CC-BY 4.0 - Exibir em todas as visualizações com dados MET Norway
"""

import asyncio
from datetime import datetime
from typing import Any

from loguru import logger

from .met_norway_client import (
    METNorwayDailyData,
    METNorwayClient,
    METNorwayConfig,
)


class METNorwaySyncAdapter:
    """
    Adapter síncrono para MET Norway.
    Usar somente "MET Norway" para MET Norway.
    """

    def __init__(
        self,
        config: METNorwayConfig | None = None,
        cache: Any | None = None,
    ):
        """
        Inicializa adapter GLOBAL do MET Norway.
        """
        self.config = config or METNorwayConfig()
        self.cache = cache
        logger.info("🌍 METNorwaySyncAdapter initialized (GLOBAL)")

    def get_daily_data_sync(
        self,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
        timezone: str | None = None,
    ) -> list[METNorwayDailyData]:
        """
        Busca dados DIÁRIOS de forma síncrona com estratégia regional.
        IMPORTANTE - ESTRATÉGIA REGIONAL:
        - NORDIC Region (NO/SE/FI/DK/Baltics):
          * Variables: temp_max, temp_min, temp_mean, humidity_mean,
                       precipitation_sum (HIGH QUALITY)
          * Quality: 1km resolution, radar + Netatmo bias correction

        - GLOBAL Region (rest of world):
          * Variables: temp_max, temp_min, temp_mean, humidity_mean
                       (NO precipitation - use Open-Meteo instead)
          * Quality: 9km ECMWF, minimal post-processing

        O cliente interno detecta automaticamente a região e filtra
        as variáveis apropriadas. A precipitação só é retornada para
        a região Nordic onde tem alta qualidade com radar.
        """
        logger.debug(
            f"🌍 MET Norway Sync request (GLOBAL): "
            f"lat={lat}, lon={lon}, "
            f"dates={start_date.date()} to {end_date.date()}"
        )

        # Executa função assíncrona de forma síncrona
        return asyncio.run(
            self._async_get_daily_data(
                lat=lat,
                lon=lon,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
            )
        )

    async def _async_get_daily_data(
        self,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
        timezone: str | None = None,
    ) -> list[METNorwayDailyData]:
        """
        Método assíncrono interno (GLOBAL com estratégia regional).
        """
        client = METNorwayClient(config=self.config, cache=self.cache)

        try:
            # Validações básicas (sem limitação geográfica!)
            if not (-90 <= lat <= 90):
                msg = f"Latitude inválida: {lat}"
                raise ValueError(msg)
            if not (-180 <= lon <= 180):
                msg = f"Longitude inválida: {lon}"
                raise ValueError(msg)

            # Log região detectada
            is_nordic = client.is_in_nordic_region(lat, lon)
            region_label = (
                "NORDIC (1km + radar)" if is_nordic else "GLOBAL (9km ECMWF)"
            )

            logger.info(
                f"📡 Consultando MET Norway API: "
                f"({lat}, {lon}) - {region_label}"
            )

            # Buscar dados DIÁRIOS (agregados de horários)
            # Cliente automaticamente filtra variáveis por região
            daily_data = await client.get_daily_forecast(
                lat=lat,
                lon=lon,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
                # variables=None usa get_recommended_variables(lat, lon)
                variables=None,
            )

            if not daily_data:
                logger.warning("⚠️  MET Norway retornou dados vazios")
                return []

            logger.info(
                f"✅ MET Norway: {len(daily_data)} dias "
                f"obtidos (de {start_date.date()} a {end_date.date()})"
            )

            return daily_data

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados MET Norway: {e}")
            raise

        finally:
            await client.close()

    def health_check_sync(self) -> bool:
        """
        Health check síncrono (testa com coordenada GLOBAL).

        Returns:
            bool: True se API está acessível
        """
        return asyncio.run(self._async_health_check())

    async def _async_health_check(self) -> bool:
        """
        Health check assíncrono interno.

        Testa com coordenadas de Brasília (Brasil) para validar
        que é realmente GLOBAL.
        """
        client = METNorwayClient(config=self.config, cache=self.cache)

        try:
            # Teste com Brasília (fora da Europa, prova que é GLOBAL!)
            is_healthy = await client.health_check()

            if is_healthy:
                logger.info("🏥 MET Norway health check: ✅ OK (GLOBAL)")
            else:
                logger.error("🏥 MET Norway health check: ❌ FAIL")

            return is_healthy

        except Exception as e:
            logger.error(f"🏥 MET Norway health check failed: {e}")
            return False

        finally:
            await client.close()

    def get_coverage_info(self) -> dict:
        """
        Retorna informações sobre cobertura GLOBAL com qualidade regional.

        Returns:
            dict: Informações de cobertura com quality tiers
        """
        return {
            "adapter": "METNorwaySyncAdapter",
            "coverage": "GLOBAL with regional quality optimization",
            "bbox": {
                "lon_min": -180,
                "lat_min": -90,
                "lon_max": 180,
                "lat_max": 90,
            },
            "quality_tiers": {
                "nordic": {
                    "region": "Norway, Denmark, Sweden, Finland, Baltics",
                    "bbox": {
                        "lon_min": 4.0,
                        "lon_max": 31.0,
                        "lat_min": 54.0,
                        "lat_max": 71.5,
                    },
                    "resolution": "1 km",
                    "model": "MEPS 2.5km + MET Nordic downscaling",
                    "updates": "Hourly",
                    "post_processing": (
                        "Extensive (radar + Netatmo crowdsourced)"
                    ),
                    "variables": [
                        "air_temperature_max",
                        "air_temperature_min",
                        "air_temperature_mean",
                        "relative_humidity_mean",
                        "precipitation_sum",
                    ],
                    "precipitation_quality": (
                        "Very High (radar + bias correction)"
                    ),
                },
                "global": {
                    "region": "Rest of World",
                    "resolution": "9 km",
                    "model": "ECMWF IFS",
                    "updates": "4x per day",
                    "post_processing": "Minimal",
                    "variables": [
                        "air_temperature_max",
                        "air_temperature_min",
                        "air_temperature_mean",
                        "relative_humidity_mean",
                    ],
                    "precipitation_quality": (
                        "Lower (use Open-Meteo instead)"
                    ),
                    "note": (
                        "Precipitation excluded - "
                        "use Open-Meteo for better global quality"
                    ),
                },
            },
            "data_type": "Forecast only (no historical data)",
            "forecast_horizon": "Up to 5 days ahead (standardized)",
            "update_frequency": "Every 6 hours",
            "license": "CC-BY 4.0 (attribution required)",
            "attribution": "Weather data from MET Norway",
        }
