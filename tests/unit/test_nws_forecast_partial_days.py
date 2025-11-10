"""
Testes para o filtro de dias parciais no NWS Forecast Client

Cenários testados:
1. Dia com < 20 horas deve ser descartado (ex: 7 horas)
2. Dia com >= 20 horas deve ser aceito (ex: 22 horas)
3. Dia com 24 horas deve ser aceito (dia completo)
4. Threshold de 20 horas garante ~83% de cobertura mínima
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
from backend.api.services.nws_forecast.nws_forecast_client import (
    NWSForecastClient,
    NWSHourlyData,
)


@pytest.fixture
def mock_client():
    """Fixture com cliente mockado."""
    with patch(
        "backend.api.services.nws_forecast.nws_forecast_client.httpx.AsyncClient"
    ):
        client = NWSForecastClient()
        yield client


def create_hourly_data(date: datetime, hours: int) -> list[NWSHourlyData]:
    """
    Cria dados horários mockados.

    Args:
        date: Data base
        hours: Quantidade de horas a gerar

    Returns:
        Lista de NWSHourlyData mockado
    """
    hourly_data = []
    for i in range(hours):
        timestamp = date + timedelta(hours=i)
        hourly_data.append(
            NWSHourlyData(
                timestamp=timestamp.isoformat(),
                temp_celsius=15.0 + i * 0.5,
                humidity_percent=60.0,
                wind_speed_ms=2.0,
                wind_speed_2m_ms=1.5,
                precip_mm=0.0,
                probability_precip_percent=10.0,
                short_forecast="Clear",
            )
        )
    return hourly_data


@pytest.mark.asyncio
async def test_partial_day_with_less_than_20_hours_is_discarded(mock_client):
    """
    Teste: Dia com < 20 horas deve ser descartado.

    Cenário: Usuário consulta às 17:00, restam apenas 7 horas do dia.
    Resultado esperado: Dia atual descartado, retorna apenas dias futuros.
    """
    # Arrange: 7 horas hoje + 24 horas amanhã
    today = datetime(2025, 11, 9, 17, 0, 0)
    tomorrow = datetime(2025, 11, 10, 0, 0, 0)

    hourly_data = create_hourly_data(
        today, 7
    ) + create_hourly_data(  # Hoje: 17:00-23:00 = 7h
        tomorrow, 24
    )  # Amanhã: 00:00-23:00 = 24h

    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data
    ):
        # Act
        result = await mock_client.get_daily_forecast_data(39.7392, -104.9903)

        # Assert
        assert len(result) == 1, "Deve retornar apenas 1 dia (amanhã)"
        assert (
            result[0].date.date() == tomorrow.date()
        ), "Deve ser o dia de amanhã"
        assert len(result[0].hourly_data) == 24, "Deve ter 24 horas completas"


@pytest.mark.asyncio
async def test_partial_day_with_20_or_more_hours_is_accepted(mock_client):
    """
    Teste: Dia com >= 20 horas deve ser aceito.

    Cenário: Usuário consulta às 04:00, restam 20 horas do dia.
    Resultado esperado: Dia atual aceito (threshold = 20h = 83% do dia).
    """
    # Arrange: 20 horas hoje + 24 horas amanhã
    today = datetime(2025, 11, 9, 4, 0, 0)
    tomorrow = datetime(2025, 11, 10, 0, 0, 0)

    hourly_data = create_hourly_data(
        today, 20
    ) + create_hourly_data(  # Hoje: 04:00-23:00 = 20h
        tomorrow, 24
    )  # Amanhã: 00:00-23:00 = 24h

    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data
    ):
        # Act
        result = await mock_client.get_daily_forecast_data(39.7392, -104.9903)

        # Assert
        assert len(result) == 2, "Deve retornar 2 dias (hoje + amanhã)"
        assert (
            result[0].date.date() == today.date()
        ), "Primeiro dia deve ser hoje"
        assert len(result[0].hourly_data) == 20, "Hoje deve ter 20 horas"
        assert (
            result[1].date.date() == tomorrow.date()
        ), "Segundo dia deve ser amanhã"
        assert len(result[1].hourly_data) == 24, "Amanhã deve ter 24 horas"


@pytest.mark.asyncio
async def test_full_day_with_24_hours_is_always_accepted(mock_client):
    """
    Teste: Dia com 24 horas deve sempre ser aceito.

    Cenário: Consulta retorna dias futuros completos.
    Resultado esperado: Todos os dias aceitos.
    """
    # Arrange: 3 dias completos
    day1 = datetime(2025, 11, 10, 0, 0, 0)
    day2 = datetime(2025, 11, 11, 0, 0, 0)
    day3 = datetime(2025, 11, 12, 0, 0, 0)

    hourly_data = (
        create_hourly_data(day1, 24)
        + create_hourly_data(day2, 24)
        + create_hourly_data(day3, 24)
    )

    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data
    ):
        # Act
        result = await mock_client.get_daily_forecast_data(39.7392, -104.9903)

        # Assert
        assert len(result) == 3, "Deve retornar 3 dias completos"
        for i, day_result in enumerate(result):
            assert (
                len(day_result.hourly_data) == 24
            ), f"Dia {i+1} deve ter 24 horas"


@pytest.mark.asyncio
async def test_threshold_edge_cases(mock_client):
    """
    Teste: Casos limítrofes do threshold de 20 horas.

    Verifica comportamento exato no limite de aceitação.
    """
    # Arrange: Testar 19h (rejeitar) e 20h (aceitar)
    base_date = datetime(2025, 11, 9, 0, 0, 0)

    # Caso 1: 19 horas (deve ser rejeitado)
    hourly_data_19 = create_hourly_data(base_date, 19)
    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data_19
    ):
        result_19 = await mock_client.get_daily_forecast_data(
            39.7392, -104.9903
        )
        assert (
            len(result_19) == 0
        ), "19 horas deve ser rejeitado (< 20 threshold)"

    # Caso 2: 20 horas (deve ser aceito)
    hourly_data_20 = create_hourly_data(base_date, 20)
    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data_20
    ):
        result_20 = await mock_client.get_daily_forecast_data(
            39.7392, -104.9903
        )
        assert (
            len(result_20) == 1
        ), "20 horas deve ser aceito (>= 20 threshold)"
        assert (
            len(result_20[0].hourly_data) == 20
        ), "Deve ter exatamente 20 horas"


@pytest.mark.asyncio
async def test_statistics_are_unbiased_with_full_days(mock_client):
    """
    Teste: Estatísticas são corretas com dias completos.

    Cenário: Verifica que temp_max/min/mean são calculadas corretamente.
    Expectativa: Dados de 24h representam o dia inteiro (madrugada + dia).
    """
    # Arrange: Dia completo com variação de temperatura
    base_date = datetime(2025, 11, 10, 0, 0, 0)
    hourly_data = []

    # Simular variação real de temperatura ao longo do dia
    temps = (
        [5.0] * 6  # Madrugada fria (00:00-05:00)
        + [10.0, 15.0, 20.0, 25.0, 30.0, 28.0]  # Aquecendo
        + [26.0, 24.0, 22.0, 20.0, 18.0, 16.0]  # Esfriando
        + [14.0, 12.0, 10.0, 8.0, 7.0, 6.0]  # Noite
    )

    for i, temp in enumerate(temps):
        timestamp = base_date + timedelta(hours=i)
        hourly_data.append(
            NWSHourlyData(
                timestamp=timestamp.isoformat(),
                temp_celsius=temp,
                humidity_percent=60.0,
                wind_speed_ms=2.0,
                wind_speed_2m_ms=1.5,
                precip_mm=0.0,
                probability_precip_percent=10.0,
                short_forecast="Clear",
            )
        )

    with patch.object(
        mock_client, "get_forecast_data", return_value=hourly_data
    ):
        # Act
        result = await mock_client.get_daily_forecast_data(39.7392, -104.9903)

        # Assert
        assert len(result) == 1, "Deve retornar 1 dia"
        day = result[0]

        assert (
            day.temp_max_celsius == 30.0
        ), "Temp máxima deve ser 30°C (pico do dia)"
        assert (
            day.temp_min_celsius == 5.0
        ), "Temp mínima deve ser 5°C (madrugada)"
        assert (
            14.0 < day.temp_mean_celsius < 15.0
        ), "Temp média deve estar entre 14-15°C"
