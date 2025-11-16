# backend/api/services/climate_factory.py
"""
Factory centralizada para criação de clientes climáticos
com injeção de dependências.

Responsabilidades principais:
- Garantir singleton único do ClimateCacheService (Redis)
- Injetar cache automaticamente onde necessário
- Padronizar criação de todos os clientes climáticos
- Fornecer cleanup seguro e centralizado (async + sync)
- Evitar múltiplas conexões Redis ou HTTP clients desnecessários
"""

from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from backend.infrastructure.cache.climate_cache import (
        ClimateCacheService,
    )


@lru_cache(maxsize=1)
def get_climate_cache_service() -> ClimateCacheService:
    """
    Singleton lazy do serviço de cache Redis.

    Usa @lru_cache em vez de variável de classe mutável para
    garantir thread-safety e evitar estado global mutável.
    """
    from backend.infrastructure.cache.climate_cache import (
        ClimateCacheService,
    )

    service = ClimateCacheService(prefix="climate")
    logger.info("ClimateCacheService singleton criado (Redis)")
    return service


class ClimateClientFactory:
    """
    Factory oficial para todos os clientes climáticos do EVAonline.

    Uso recomendado em todo o projeto:
        client = ClimateClientFactory.create_met_norway()
        data = await client.get_daily_data(...)
    """

    @staticmethod
    def create_nasa_power():
        """
        Cria cliente NASA POWER com cache Redis.

        Returns:
            Cliente para dados históricos globais (1990–hoje)
        """
        from .nasa_power.nasa_power_client import NASAPowerClient

        client = NASAPowerClient(cache=get_climate_cache_service())
        logger.debug("NASAPowerClient criado com cache Redis")
        return client

    @staticmethod
    def create_met_norway():
        """
        Cria cliente MET Norway com cache Redis.

        Returns:
            Cliente para região nórdica (1km) e forecast global (9km)
        """
        from .met_norway.met_norway_client import METNorwayClient

        client = METNorwayClient(cache=get_climate_cache_service())
        logger.debug("METNorwayClient criado com cache Redis")
        return client

    @staticmethod
    def create_nws():
        """
        Cria cliente NWS Forecast com cache interno.

        Returns:
            Cliente para previsão oficial NOAA (EUA continental apenas)
        """
        from .nws_forecast.nws_forecast_client import NWSForecastClient

        client = NWSForecastClient()  # Cache interno próprio
        logger.debug("NWSForecastClient criado (cache interno)")
        return client

    @staticmethod
    def create_nws_stations():
        """
        Cria cliente NWS Stations com cache Redis.

        Returns:
            Cliente para observações tempo real (EUA apenas)
        """
        from .nws_stations.nws_stations_client import NWSStationsClient

        client = NWSStationsClient(cache=get_climate_cache_service())
        logger.debug("NWSStationsClient criado com cache Redis")
        return client

    @staticmethod
    def create_openmeteo_forecast(
        cache_dir: str = ".cache/openmeteo_forecast",
    ):
        """
        Cria cliente Open-Meteo Forecast com cache local.

        Args:
            cache_dir: Diretório para cache em disco

        Returns:
            Cliente para dados recentes + previsão (-30d a +5d)
        """
        from .openmeteo_forecast.openmeteo_forecast_client import (
            OpenMeteoForecastClient,
        )

        client = OpenMeteoForecastClient(cache_dir=cache_dir)
        logger.debug(
            "OpenMeteoForecastClient criado (cache local: {})", cache_dir
        )
        return client

    @staticmethod
    def create_openmeteo_archive(
        cache_dir: str = ".cache/openmeteo_archive",
    ):
        """
        Cria cliente Open-Meteo Archive com cache Redis.

        Args:
            cache_dir: Diretório para cache em disco (fallback)

        Returns:
            Cliente para dados históricos (1940–hoje-2d)
        """
        from .openmeteo_archive.openmeteo_archive_client import (
            OpenMeteoArchiveClient,
        )

        client = OpenMeteoArchiveClient(
            cache=get_climate_cache_service(), cache_dir=cache_dir
        )
        logger.debug(
            "OpenMeteoArchiveClient criado com cache Redis + local: {}",
            cache_dir,
        )
        return client

    @staticmethod
    def create_openmeteo(cache_dir: str = ".cache/openmeteo_forecast"):
        """
        Alias mantido por compatibilidade com código antigo.

        Sempre retorna o Forecast (o mais usado no dashboard).
        """
        return ClimateClientFactory.create_openmeteo_forecast(cache_dir)

    @classmethod
    async def close_all(cls) -> None:
        """
        Fecha TODAS as conexões abertas de forma segura.

        Chamado no shutdown da aplicação (FastAPI lifespan,
        Celery worker, etc).
        """
        # Fecha Redis (única conexão global)
        cache_service = get_climate_cache_service()
        if hasattr(cache_service, "redis") and cache_service.redis is not None:
            try:
                await cache_service.redis.close()
                logger.info(
                    "Conexão Redis (ClimateCacheService) "
                    "fechada com sucesso"
                )
            except Exception as e:
                logger.error(f"Erro ao fechar Redis: {e}")

        # Limpa o singleton (importante para testes e reinícios)
        get_climate_cache_service.cache_clear()

        # Nota: httpx.AsyncClient é criado por request
        # cada cliente já tem seu próprio .aclose() se necessário
        logger.info("ClimateClientFactory: cleanup completo")

    @classmethod
    def close_all_sync(cls) -> None:
        """
        Versão síncrona segura para contextos não-async.

        Para uso em scripts, testes, Celery sync tasks.
        """
        try:
            loop = asyncio.get_running_loop()
            # Loop já rodando: cria task e NÃO aguarda
            # (impossível await em contexto sync)
            task = loop.create_task(cls.close_all())
            logger.debug(
                "Cleanup agendado como task em loop existente: %s", task
            )
        except RuntimeError:
            # Sem loop: cria temporário e executa completamente
            asyncio.run(cls.close_all())
