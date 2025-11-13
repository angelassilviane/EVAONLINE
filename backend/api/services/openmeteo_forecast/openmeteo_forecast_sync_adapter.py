"""
Sync Adapter para Open-Meteo Forecast API.

Converte chamadas assíncronas do OpenMeteoForecastClient em métodos síncronos
para compatibilidade com Celery tasks e scripts legados.

API: https://api.open-meteo.com/v1/forecast
Cobertura: Global
Período: (hoje - 25 dias) até (hoje + 5 dias) = 30 dias totais
Resolução: Diária (agregada de dados horários)
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
- TTL dinâmico: 1h (forecast), 6h (recent)
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from loguru import logger

from backend.api.services.openmeteo_forecast.openmeteo_forecast_client import (
    OpenMeteoForecastClient,
)


class OpenMeteoForecastSyncAdapter:
    """
    Adapter síncrono para Open-Meteo Forecast API.

    Suporta Redis cache (via ClimateCache) com fallback para cache local.
    """

    def __init__(self, cache: Any | None = None, cache_dir: str = ".cache"):
        """
        Inicializa adapter síncrono.

        Args:
            cache: Optional ClimateCache instance (Redis)
            cache_dir: Diretório para fallback cache (TTL: 6 horas)

        Features:
            - Recent data: hoje - 30 dias
            - Forecast data: hoje + 5 dias (padronizado)
            - Best match model: Seleciona melhor modelo disponível
            - 10 climate variables com unidades padronizadas
            - Redis cache compartilhado entre workers
        """
        self.cache = cache  # Redis cache (opcional)
        self.cache_dir = cache_dir

        cache_type = "Redis" if cache else "Local"
        logger.info(
            f"OpenMeteoForecastSyncAdapter initialized ({cache_type} cache, "
            f"-25d to +5d = 30d total)"
        )

    def get_data_sync(
        self,
        lat: float,
        lon: float,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
    ) -> List[Dict[str, Any]]:
        """
        Baixa dados recentes/futuros de forma SÍNCRONA.
        """
        # Convert strings to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # Validação - Forecast API: -25d to +5d (30 dias totais)
        today = datetime.now().date()
        min_date = today - timedelta(days=25)
        max_date = today + timedelta(days=5)

        if start_date.date() < min_date:
            logger.warning(
                f"Forecast: ajustando start_date para {min_date} "
                f"(limite: hoje - 25 dias)"
            )
            start_date = datetime.combine(min_date, datetime.min.time())

        if end_date.date() > max_date:
            logger.warning(
                f"Forecast: ajustando end_date para {max_date} "
                f"(limite: hoje + 5 dias padronizado)"
            )
            end_date = datetime.combine(max_date, datetime.min.time())

        # Executar async de forma segura (igual Archive adapter)
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

        Usa best_match model, past_days=30, e wind_speed_unit=ms.
        """
        try:
            client = OpenMeteoForecastClient(
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
                f"✅ Forecast: {len(records)} registros diários "
                f"para ({lat:.4f}, {lon:.4f}) | "
                f"10 variáveis climáticas"
            )
            return records

        except Exception as e:
            logger.error(f"❌ Forecast: erro ao baixar dados: {str(e)}")
            raise

    def get_forecast_sync(
        self,
        lat: float,
        lon: float,
        days: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Baixa previsões futuras de forma SÍNCRONA.
        """
        if not 1 <= days <= 5:
            msg = "Forecast: dias deve ser entre 1 e 5"
            logger.error(msg)
            raise ValueError(msg)

        # Calcular período
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days - 1)

        return self.get_data_sync(lat, lon, start_date, end_date)

    def health_check_sync(self) -> bool:
        """
        Verifica se Forecast API está acessível (síncrono).
        """
        try:
            # Tentar obter loop existente
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Loop já está rodando
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run, self._async_health_check()
                    )
                    return future.result()
            else:
                # Loop existe mas não está rodando
                return loop.run_until_complete(self._async_health_check())
        except RuntimeError:
            # Nenhum loop, criar um novo
            return asyncio.run(self._async_health_check())

    async def _async_health_check(self) -> bool:
        """
        Implementação async do health check.

        Testa: Brasília, data atual, best_match model.
        """
        try:
            client = OpenMeteoForecastClient(
                cache=self.cache, cache_dir=self.cache_dir
            )

            # Testar com coordenadas de referência (Brasília)
            # Usar data atual (Forecast API sempre tem)
            today = datetime.now().date()
            response = await client.get_climate_data(
                lat=-15.7939,
                lng=-47.8828,
                start_date=str(today),
                end_date=str(today),
            )

            has_data = "climate_data" in response
            has_dates = "dates" in response.get("climate_data", {})
            return has_data and has_dates

        except Exception as e:
            logger.error(f"Forecast: health check falhou: {str(e)}")
            return False

    @staticmethod
    def get_info() -> Dict[str, Any]:
        """
        Retorna informações sobre a fonte de dados Forecast API.

        Inclui: coverage, period, variables, license, model, units.

        Returns:
            Dicionário com metadados completos da fonte
        """
        return OpenMeteoForecastClient.get_info()
