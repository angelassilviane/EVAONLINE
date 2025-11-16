"""
Sync Adapter para Open-Meteo Archive API.

Converte chamadas assíncronas do OpenMeteoArchiveClient em métodos síncronos
para compatibilidade com Celery tasks.

API: https://archive-api.open-meteo.com/v1/archive
Cobertura: Global
Período: 1940-01-01 até hoje-30 dias (modo historical_email)
Licença: CC BY 4.0 (atribuição obrigatória)

Variables (10):
- Temperature: max, mean, min (°C)
- Relative Humidity: max, mean, min (%)
- Wind Speed: mean at 10m (m/s)
- Shortwave Radiation: sum (MJ/m²)
- Precipitation: sum (mm)
- ET0 FAO Evapotranspiration (mm)

CACHE STRATEGY (Nov 2025):
- Redis cache via ClimateCache (recomendado)
- Fallback: requests_cache local
- TTL: 24h (dados históricos estáveis)
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from loguru import logger

from .openmeteo_archive_client import (
    OpenMeteoArchiveClient,
)


class OpenMeteoArchiveSyncAdapter:
    """
    Adapter síncrono para Open-Meteo Archive API.

    Historical data: 1940-01-01 até hoje-30 dias (modo historical_email)
    Models: best_match (melhor modelo disponível)
    Variables: 10 variáveis climáticas (T, RH, Wind, Solar, Precip, ET0)
    Wind unit: m/s (metros por segundo)
    Cache: Redis compartilhado (TTL 24h)
    """

    def __init__(self, cache: Any | None = None, cache_dir: str = ".cache"):
        """
        Inicializa adapter síncrono.

        Args:
            cache: Optional ClimateCache instance (Redis)
            cache_dir: Diretório para fallback cache (TTL: 24h)

        Features:
            - Historical data: 1940 até hoje-30d (modo historical_email)
            - Best match model: Seleciona melhor modelo disponível
            - 10 climate variables com unidades padronizadas
            - Redis cache compartilhado entre workers
        """
        self.cache = cache  # Redis cache (opcional)
        self.cache_dir = cache_dir

        cache_type = "Redis" if cache else "Local"
        logger.info(
            f"OpenMeteoArchiveSyncAdapter initialized ({cache_type} cache, "
            f"1940 to today-30d)"
        )

    def get_daily_data_sync(
        self,
        lat: float,
        lon: float,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
    ) -> List[Dict[str, Any]]:
        """
        Baixa dados históricos de forma SÍNCRONA.

        Args:
            lat: Latitude (-90 a 90)
            lon: Longitude (-180 a 180)
            start_date: Data inicial (str ou datetime)
            end_date: Data final (str ou datetime)

        Returns:
            Lista de dicionários com dados diários

        Example:
            >>> adapter = OpenMeteoArchiveSyncAdapter()
            >>> data = adapter.get_daily_data_sync(
            ...     lat=-15.7939, lon=-47.8828,
            ...     start_date='2024-01-01',
            ...     end_date='2024-01-07'
            ... )
        """
        # Convert strings to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # Executar async de forma segura
        try:
            # Tentar obter loop existente
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Loop já está rodando (contexto de servidor async)
                # Criar nova task no loop existente
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._async_get_data(lat, lon, start_date, end_date),
                    )
                    return future.result()
            else:
                # Loop existe mas não está rodando
                return loop.run_until_complete(
                    self._async_get_data(lat, lon, start_date, end_date)
                )
        except RuntimeError:
            # Nenhum loop, criar um novo
            return asyncio.run(
                self._async_get_data(lat, lon, start_date, end_date)
            )

    async def _async_get_data(
        self,
        lat: float,
        lon: float,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Implementação async interna.

        Usa best_match model e wind_speed_unit=ms para consistência.
        """
        try:
            client = OpenMeteoArchiveClient(
                cache=self.cache, cache_dir=self.cache_dir
            )

            response = await client.get_climate_data(
                lat=lat,
                lng=lon,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            # Extrair dados do response
            daily_data = response["climate_data"]
            dates = pd.to_datetime(daily_data["dates"])

            # Converter para lista de dicionários
            records = []
            for i, date in enumerate(dates):
                record = {"date": date.date()}

                # Adicionar todas as variáveis disponíveis
                for key, values in daily_data.items():
                    if key != "dates" and isinstance(values, list):
                        record[key] = values[i] if i < len(values) else None

                records.append(record)

            logger.info(
                f"Archive: obtidos {len(records)} registros diários "
                f"para ({lat:.4f}, {lon:.4f}) | "
                f"10 variáveis climáticas"
            )
            return records

        except Exception as e:
            logger.error(f"Archive: erro ao baixar dados: {str(e)}")
            raise

    def health_check_sync(self) -> bool:
        """
        Verifica se Archive API está acessível (síncrono).

        Testa com coordenadas de Brasília e data histórica
        segura (1 ano atrás).
        Valida resposta com best_match model.

        Returns:
            True se API está funcionando, False caso contrário
        """
        return asyncio.run(self._async_health_check())

    async def _async_health_check(self) -> bool:
        """
        Implementação async do health check.

        Testa: Brasília, 1 ano atrás, best_match model.
        """
        try:
            client = OpenMeteoArchiveClient(
                cache=self.cache, cache_dir=self.cache_dir
            )

            # Testar com coordenadas de referência (Brasília)
            # Usar data histórica segura (1 ano atrás)
            test_date = (datetime.now() - timedelta(days=365)).date()
            response = await client.get_climate_data(
                lat=-15.7939,
                lng=-47.8828,
                start_date=str(test_date),
                end_date=str(test_date),
            )

            has_data = "climate_data" in response
            has_dates = "dates" in response.get("climate_data", {})
            return has_data and has_dates

        except Exception as e:
            logger.error(f"Archive: health check falhou: {str(e)}")
            return False

    @staticmethod
    def get_info() -> Dict[str, Any]:
        """
        Retorna informações sobre a fonte de dados Archive API.

        Inclui: coverage, period, variables, license, model, units.

        Returns:
            Dicionário com metadados completos da fonte
        """
        return OpenMeteoArchiveClient.get_info()
