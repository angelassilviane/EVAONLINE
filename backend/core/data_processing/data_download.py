from datetime import datetime, timedelta
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from celery import shared_task
from loguru import logger


def classify_request_type(data_inicial: datetime, data_final: datetime) -> str:
    """
    Classifica requisição: histórico, atual ou forecast.

    Regras:
    - Se data_final > hoje → FORECAST
    - Se data_inicial <= hoje - 30 dias → HISTÓRICO
    - Senão → ATUAL

    Args:
        data_inicial: Data inicial da requisição
        data_final: Data final da requisição

    Returns:
        "historical", "current" ou "forecast"
    """
    today = datetime.now().date()
    start_date = (
        data_inicial.date() if hasattr(data_inicial, "date") else data_inicial
    )
    end_date = data_final.date() if hasattr(data_final, "date") else data_final
    threshold = today - timedelta(days=30)

    if end_date > today:
        return "forecast"
    elif start_date <= threshold:
        return "historical"
    else:
        return "current"


@shared_task
def download_weather_data(
    data_source: Union[str, list],
    data_inicial: str,
    data_final: str,
    longitude: float,
    latitude: float,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Baixa dados meteorológicos das fontes especificadas para as coordenadas
    e período.

    Fontes suportadas:
    - "nasa_power": NASA POWER (global, 1981+, domínio público)
    - "openmeteo_archive": Open-Meteo Archive (global, 1940+, CC BY 4.0)
    - "openmeteo_forecast": Open-Meteo Forecast (global, 16d, CC BY 4.0)
    - "met_norway": MET Norway Locationforecast
      (global, real-time, CC BY 4.0)
    - "nws_forecast": NWS Forecast (USA, previsões, domínio público)
    - "nws_stations": NWS Stations (USA, estações, domínio público)
    - "data fusion": Fusiona múltiplas fontes disponíveis (Kalman Ensemble)

    A validação dinâmica verifica automaticamente quais fontes estão disponíveis
    para as coordenadas específicas, rejeitando fontes indisponíveis.

    Args:
        data_source: Fonte de dados (str ou list de fontes)
        data_inicial: Data inicial no formato YYYY-MM-DD
        data_final: Data final no formato YYYY-MM-DD
        longitude: Longitude (-180 a 180)
        latitude: Latitude (-90 a 90)
    """
    # Import moved here to avoid circular imports during module initialization
    from backend.api.services.climate_source_manager import (
        ClimateSourceManager,
    )

    logger.info(
        f"Iniciando download - Fonte: {data_source}, "
        f"Período: {data_inicial} a {data_final}, "
        f"Coord: ({latitude}, {longitude})"
    )
    warnings_list = []

    # Validação das coordenadas
    if not (-90 <= latitude <= 90):
        msg = "Latitude deve estar entre -90 e 90 graus"
        logger.error(msg)
        raise ValueError(msg)
    if not (-180 <= longitude <= 180):
        msg = "Longitude deve estar entre -180 e 180 graus"
        logger.error(msg)
        raise ValueError(msg)

    # Validação das datas
    try:
        data_inicial_formatted = pd.to_datetime(data_inicial)
        data_final_formatted = pd.to_datetime(data_final)
    except ValueError:
        msg = "As datas devem estar no formato 'AAAA-MM-DD'"
        logger.error(msg)
        raise ValueError(msg)

    # Verifica ordem das datas
    if data_final_formatted < data_inicial_formatted:
        msg = "A data final deve ser posterior à data inicial"
        logger.error(msg)
        raise ValueError(msg)

    # Verifica período mínimo (validações específicas por fonte depois)
    period_days = (data_final_formatted - data_inicial_formatted).days + 1
    if period_days < 1:
        msg = "O período deve ter pelo menos 1 dia"
        logger.error(msg)
        raise ValueError(msg)

    # Data atual
    current_date = pd.to_datetime(datetime.now().date())

    # Classificar tipo de requisição (histórico vs atual vs forecast)
    request_type = classify_request_type(
        data_inicial_formatted, data_final_formatted
    )
    logger.info(f"Tipo de requisição: {request_type}")

    # Verifica se é uma data válida (não futura para dados históricos/atual)
    if request_type in ["historical", "current"]:
        if data_inicial_formatted > current_date:
            msg = (
                "A data inicial não pode ser futura para dados "
                f"históricos ou atuais. Data atual: "
                f"{current_date.strftime('%Y-%m-%d')}"
            )
            logger.error(msg)
            raise ValueError(msg)

    # Validações de período por tipo de requisição
    if request_type == "historical" and not (1 <= period_days <= 90):
        msg = "Dados históricos: período entre 1 e 90 dias"
        logger.error(msg)
        raise ValueError(msg)

    if request_type == "current" and not (7 <= period_days <= 30):
        msg = "Dados atuais: período entre 7 e 30 dias"
        logger.error(msg)
        raise ValueError(msg)

    if request_type == "forecast":
        forecast_max = current_date + pd.Timedelta(days=5)
        if data_final_formatted > forecast_max:
            msg = "Dados forecast: período máximo até hoje + 5 dias"
            logger.error(msg)
            raise ValueError(msg)
        # Permitir start até today - 25d para OpenMeteo
        min_start = current_date - pd.Timedelta(days=25)
        if data_inicial_formatted < min_start:
            msg = "Dados forecast: data inicial deve ser >= " "hoje - 25 dias"
            logger.error(msg)
            raise ValueError(msg)

    # Validação dinâmica de fontes disponíveis para a localização
    source_manager = ClimateSourceManager()
    available_sources = source_manager.get_available_sources_for_location(
        lat=latitude,
        lon=longitude,
        exclude_non_commercial=True,  # Exclui fontes não-comerciais do mapa
    )

    # Filtrar apenas fontes disponíveis para esta localização
    available_source_ids = [
        source_id
        for source_id, meta in available_sources.items()
        if meta["available"]
    ]

    logger.info(
        "Fontes disponíveis para (%s, %s): %s",
        latitude,
        longitude,
        available_source_ids,
    )

    # Validação da fonte de dados (aceita str ou list, case-insensitive)
    valid_sources = [
        "openmeteo_archive",
        "openmeteo_forecast",
        "nasa_power",
        "nws_forecast",
        "nws_stations",
        "met_norway",
        "data fusion",
    ]

    # Normalize input to list of lower-case strings
    if isinstance(data_source, list):
        requested = [str(s).lower() for s in data_source]
    else:
        # Suportar string com múltiplas fontes separadas por vírgula
        data_source_str = str(data_source).lower()
        if "," in data_source_str:
            requested = [s.strip() for s in data_source_str.split(",")]
        else:
            requested = [data_source_str]

    # Validate requested sources
    for req in requested:
        if req not in valid_sources:
            msg = f"Fonte inválida: {req}. Use: " f"{', '.join(valid_sources)}"
            logger.error(msg)
            raise ValueError(msg)

        # Para fontes específicas, verificar se estão disponíveis na localização
        if req != "data fusion" and req not in available_source_ids:
            available_list = (
                ", ".join(available_source_ids)
                if available_source_ids
                else "nenhuma"
            )
            msg = (
                f"Fonte '{req}' não disponível para as coordenadas "
                f"({latitude}, {longitude}). "
                f"Fontes disponíveis: {available_list}"
            )
            logger.error(msg)
            raise ValueError(msg)

    # Define sources to query based on request_type and availability
    if "data fusion" in requested:
        # Data Fusion combina múltiplas fontes com Kalman Ensemble
        # Selecionar fontes baseadas no tipo de requisição
        if request_type == "historical":
            possible_sources = ["nasa_power", "openmeteo_archive"]
        elif request_type == "current":
            possible_sources = [
                "openmeteo_archive",
                "nasa_power",
                "met_norway",
                "nws_forecast",
                "openmeteo_forecast",
                "nws_stations",
            ]
        elif request_type == "forecast":
            possible_sources = [
                "openmeteo_forecast",
                "met_norway",
                "nws_forecast",
            ]

        sources = [
            src for src in possible_sources if src in available_source_ids
        ]

        if not sources:
            msg = (
                f"Nenhuma fonte disponível para {request_type} nas coordenadas "
                f"({latitude}, {longitude})."
            )
            logger.error(msg)
            raise ValueError(msg)

        logger.info(
            f"Data Fusion selecionada para {request_type}, coletando de {len(sources)} fontes disponíveis: {sources}"
        )
    else:
        sources = [req for req in requested if req in available_source_ids]
        logger.info(f"Fonte(s) selecionada(s): {sources}")

    weather_data_sources: List[pd.DataFrame] = []
    for source in sources:
        logger.info(f"📥 Processando fonte: {source}")

        # Validações específicas por fonte de dados e ajuste de datas
        data_final_adjusted = data_final_formatted

        if source == "nasa_power":
            # NASA POWER: dados históricos desde 1981, sem dados futuros
            # Padrão: >= 1990-01-01
            nasa_start_limit = pd.to_datetime("1990-01-01")
            if data_inicial_formatted < nasa_start_limit:
                msg = "NASA POWER: data inicial deve ser >= 1990-01-01"
                logger.error(msg)
                raise ValueError(msg)
            if data_final_formatted > current_date:
                warnings_list.append(
                    "NASA POWER: truncando para data atual (sem dados futuros)"
                )
                data_final_adjusted = current_date

        elif source == "openmeteo_archive":
            # Open-Meteo Archive: dados históricos desde 1940, até today - 2d
            # Padrão: >= 1990-01-01
            oma_start_limit = pd.to_datetime("1990-01-01")
            if data_inicial_formatted < oma_start_limit:
                msg = "Open-Meteo Archive: data inicial deve ser >= 1990-01-01"
                logger.error(msg)
                raise ValueError(msg)
            max_date = current_date - pd.Timedelta(days=2)
            if data_final_formatted > max_date:
                warnings_list.append(
                    "Open-Meteo Archive: truncando para today - 2d (consolidação)"
                )
                data_final_adjusted = max_date

        elif source == "openmeteo_forecast":
            # Open-Meteo Forecast: (hoje - 25d) até (hoje + 5d)
            # Total: 30 dias (25 passado + 5 futuro)
            min_date = current_date - pd.Timedelta(days=25)
            if data_inicial_formatted < min_date:
                msg = (
                    f"Open-Meteo Forecast: data inicial deve ser >= "
                    f"{min_date.strftime('%Y-%m-%d')} "
                    "(máximo 25 dias no passado)"
                )
                logger.error(msg)
                raise ValueError(msg)

            forecast_limit = current_date + pd.Timedelta(days=5)
            if data_final_formatted > forecast_limit:
                msg = (
                    "Open-Meteo Forecast: data final deve ser <= "
                    "hoje + 5 dias"
                )
                logger.error(msg)
                raise ValueError(msg)

        elif source == "met_norway":
            # MET Norway: apenas forecast, hoje até hoje + 5 dias
            # Padrão EVAonline: data atual + 5 dias
            if data_inicial_formatted < current_date:
                msg = (
                    "MET Norway: data inicial deve ser >= hoje (sem histórico)"
                )
                logger.error(msg)
                raise ValueError(msg)

            forecast_limit = current_date + pd.Timedelta(days=5)
            if data_final_formatted > forecast_limit:
                msg = "MET Norway: data final deve ser <= hoje + 5 dias"
                logger.error(msg)
                raise ValueError(msg)

        elif source == "nws_forecast":
            # NWS Forecast: previsão de hoje até hoje + 5 dias
            # IMPORTANTE: Descarta dias incompletos (<20h) para evitar viés
            # Dia atual só é incluído se tiver >20 horas de dados
            if data_inicial_formatted < current_date:
                msg = (
                    "NWS Forecast: data inicial deve ser >= hoje "
                    "(sem histórico)"
                )
                logger.error(msg)
                raise ValueError(msg)

            forecast_limit = current_date + pd.Timedelta(days=5)
            if data_final_formatted > forecast_limit:
                msg = "NWS Forecast: data final deve ser <= hoje + 5 dias"
                logger.error(msg)
                raise ValueError(msg)

        elif source == "nws_stations":
            # NWS Stations: observações em tempo real
            # Dados de estações meteorológicas: (hoje - 1 dia) até hoje
            # NOTA: Delay de até 20 minutos é normal (MADIS processing)
            min_date = current_date - pd.Timedelta(days=1)
            if data_inicial_formatted < min_date:
                msg = (
                    f"NWS Stations: data inicial deve ser >= "
                    f"{min_date.strftime('%Y-%m-%d')} "
                    "(observações em tempo real: máximo 1 dia passado)"
                )
                logger.error(msg)
                raise ValueError(msg)

            if data_final_formatted > current_date:
                msg = (
                    "NWS Stations: data final deve ser <= hoje "
                    "(sem previsão, apenas observações reais)"
                )
                logger.error(msg)
                raise ValueError(msg)

        # Adjust end date for NASA POWER (no future data)
        data_final_adjusted = (
            min(data_final_formatted, current_date)
            if source == "nasa_power"
            else data_final_formatted
        )
        if (
            data_final_adjusted < data_final_formatted
            and source == "nasa_power"
        ):
            warnings_list.append(
                f"NASA POWER data truncated to "
                f"{data_final_adjusted.strftime('%Y-%m-%d')} "
                "as it does not provide future data."
            )

        # Download data
        # Inicializa variáveis
        weather_df = None

        try:
            if source == "nasa_power":
                # Usa novo cliente assíncrono via adapter síncrono
                from backend.api.services import NASAPowerSyncAdapter

                adapter = NASAPowerSyncAdapter()

                # Baixa dados via novo cliente (aceita datetime)
                nasa_data = adapter.get_daily_data_sync(
                    lat=latitude,
                    lon=longitude,
                    start_date=data_inicial_formatted,
                    end_date=data_final_adjusted,
                )

                # Converte para DataFrame pandas - variáveis NASA POWER
                data_records = []
                for record in nasa_data:
                    data_records.append(
                        {
                            "date": record.date,
                            # Variáveis NASA POWER nativas
                            "T2M_MAX": record.temp_max,
                            "T2M_MIN": record.temp_min,
                            "T2M": record.temp_mean,
                            "RH2M": record.humidity,
                            "WS2M": record.wind_speed,
                            "ALLSKY_SFC_SW_DWN": record.solar_radiation,
                            "PRECTOTCORR": record.precipitation,
                        }
                    )

                weather_df = pd.DataFrame(data_records)
                weather_df["date"] = pd.to_datetime(weather_df["date"])
                weather_df.set_index("date", inplace=True)

                logger.info(
                    f"✅ NASA POWER: {len(nasa_data)} registros diários para ({latitude}, {longitude})"
                )

            elif source == "openmeteo_archive":
                # Open-Meteo Archive (histórico desde 1950)
                from backend.api.services import OpenMeteoArchiveSyncAdapter

                adapter = OpenMeteoArchiveSyncAdapter()

                # Busca dados via novo adapter síncrono
                openmeteo_data = adapter.get_data_sync(
                    lat=latitude,
                    lon=longitude,
                    start_date=data_inicial_formatted,
                    end_date=data_final_adjusted,
                )

                if not openmeteo_data:
                    msg = (
                        f"Open-Meteo Archive: Nenhum dado "
                        f"para ({latitude}, {longitude})"
                    )
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                # Converte para DataFrame - TODAS as variáveis Open-Meteo
                weather_df = pd.DataFrame(openmeteo_data)
                weather_df["date"] = pd.to_datetime(weather_df["date"])
                weather_df.set_index("date", inplace=True)

                # Harmonizar variáveis OpenMeteo → NASA format para ETo
                # ETo: T2M_MAX, T2M_MIN, T2M_MEAN, RH2M, WS2M,
                #      ALLSKY_SFC_SW_DWN, PRECTOTCORR
                harmonization = {
                    "temperature_2m_max": "T2M_MAX",
                    "temperature_2m_min": "T2M_MIN",
                    "temperature_2m_mean": "T2M",
                    "relative_humidity_2m_mean": "RH2M",
                    "wind_speed_2m_mean": "WS2M",
                    "shortwave_radiation_sum": "ALLSKY_SFC_SW_DWN",
                    "precipitation_sum": "PRECTOTCORR",
                }

                for openmeteo_var, nasa_var in harmonization.items():
                    if openmeteo_var in weather_df.columns:
                        weather_df[nasa_var] = weather_df[openmeteo_var]

                logger.info(
                    f"✅ Open-Meteo Archive: {len(openmeteo_data)} registros diários para ({latitude}, {longitude})"
                )

            elif source == "openmeteo_forecast":
                # Open-Meteo Forecast (previsão + recent: -30d a +5d)
                from backend.api.services import OpenMeteoForecastSyncAdapter

                adapter = OpenMeteoForecastSyncAdapter()

                # Busca dados via adapter síncrono (aceita start/end date)
                forecast_data = adapter.get_data_sync(
                    lat=latitude,
                    lon=longitude,
                    start_date=data_inicial_formatted,
                    end_date=data_final_formatted,
                )

                if not forecast_data:
                    msg = (
                        f"Open-Meteo Forecast: Nenhum dado "
                        f"para ({latitude}, {longitude})"
                    )
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                # Converte para DataFrame - TODAS as variáveis Open-Meteo
                weather_df = pd.DataFrame(forecast_data)
                weather_df["date"] = pd.to_datetime(weather_df["date"])
                weather_df.set_index("date", inplace=True)

                # Harmonizar variáveis OpenMeteo → NASA format para ETo
                # ETo: T2M_MAX, T2M_MIN, T2M_MEAN, RH2M, WS2M,
                # ALLSKY_SFC_SW_DWN, PRECTOTCORR
                harmonization = {
                    "temperature_2m_max": "T2M_MAX",
                    "temperature_2m_min": "T2M_MIN",
                    "temperature_2m_mean": "T2M_MEAN",  # MEAN not T2M!
                    "relative_humidity_2m_mean": "RH2M",
                    "wind_speed_2m_mean": "WS2M",
                    "shortwave_radiation_sum": "ALLSKY_SFC_SW_DWN",
                    "precipitation_sum": "PRECTOTCORR",
                }

                # Renomear colunas existentes
                for openmeteo_var, nasa_var in harmonization.items():
                    if openmeteo_var in weather_df.columns:
                        weather_df[nasa_var] = weather_df[openmeteo_var]
                        logger.debug(
                            f"Harmonized: {openmeteo_var} → {nasa_var}"
                        )

                logger.info(
                    f"✅ Open-Meteo Forecast: {len(forecast_data)} registros diários para ({latitude}, {longitude})"
                )

            elif source == "met_norway":
                # MET Norway Locationforecast (Europa/Global, real-time,
                # horários convertidos em diários)
                from backend.api.services import (
                    METNorwayLocationForecastSyncAdapter as Adapter,
                )

                adapter = Adapter()

                # Verificar se está na cobertura (Europa/Global)
                if not adapter.health_check_sync():
                    msg = "MET Norway Locationforecast: Verificação falhou"
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                try:
                    # Busca dados via novo adapter síncrono
                    met_data = adapter.get_daily_data_sync(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )

                    if not met_data:
                        msg = (
                            f"MET Norway Locationforecast: Nenhum dado "
                            f"para ({latitude}, {longitude})"
                        )
                        logger.warning(msg)
                        warnings_list.append(msg)
                        continue

                    # Obter variáveis recomendadas para a região
                    from backend.api.services import (
                        METNorwayLocationForecastClient,
                    )

                    recommended_vars = METNorwayLocationForecastClient.get_recommended_variables(  # noqa: E501
                        latitude, longitude
                    )

                    # Verificar se precipitação deve ser incluída
                    include_precipitation = (
                        "precipitation_sum" in recommended_vars
                    )

                    # Log da estratégia regional
                    if include_precipitation:
                        region_info = (
                            "NORDIC (1km + radar): "
                            "Incluindo precipitação (alta qualidade)"
                        )
                    else:
                        region_info = (
                            "GLOBAL (9km ECMWF): "
                            "Excluindo precipitação (usar Open-Meteo)"
                        )

                    logger.info(f"MET Norway Locationforecast - {region_info}")

                    # Converte para DataFrame - FILTRA variáveis por região
                    data_records = []
                    for record in met_data:
                        record_dict = {
                            "date": record.date,
                            # Temperaturas (sempre incluídas)
                            "temperature_2m_max": record.temp_max,
                            "temperature_2m_min": record.temp_min,
                            "temperature_2m_mean": record.temp_mean,
                            # Umidade (sempre incluída)
                            "relative_humidity_2m_mean": (
                                record.humidity_mean
                            ),
                        }

                        # Precipitação: apenas para região Nordic
                        if include_precipitation:
                            record_dict["precipitation_sum"] = (
                                record.precipitation_sum
                            )
                        # Else: omitir precipitação (será None ou ignorada)

                        data_records.append(record_dict)

                    weather_df = pd.DataFrame(data_records)
                    weather_df["date"] = pd.to_datetime(weather_df["date"])
                    weather_df.set_index("date", inplace=True)

                    # Adicionar atribuição CC-BY 4.0 aos warnings
                    warnings_list.append(
                        "📌 Dados MET Norway: CC-BY 4.0 - Atribuição requerida"  # noqa: E501
                    )

                    # Log de variáveis incluídas
                    logger.info(
                        "MET Norway Locationforecast: %d registros (%s, %s), "
                        "variáveis: %s",
                        len(met_data),
                        latitude,
                        longitude,
                        list(weather_df.columns),
                    )

                except ValueError as e:
                    msg = (
                        f"MET Norway Locationforecast: fora da cobertura - "
                        f"{str(e)}"
                    )
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

            elif source == "nws_forecast":
                # NWS Forecast (USA, previsões)
                from backend.api.services import NWSDailyForecastSyncAdapter

                adapter = NWSDailyForecastSyncAdapter()

                # Verificar se está na cobertura (USA Continental)
                if not adapter.health_check_sync():
                    msg = "NWS Forecast: Verificação falhou"
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                try:
                    # Busca dados via adapter síncrono
                    nws_forecast_data = adapter.get_daily_data_sync(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )

                    if not nws_forecast_data:
                        msg = (
                            f"NWS Forecast: Nenhum dado para "
                            f"({latitude}, {longitude})"
                        )
                        logger.warning(msg)
                        warnings_list.append(msg)
                        continue

                    # Converte para DataFrame - variáveis NWS Forecast
                    data_records = []
                    for record in nws_forecast_data:
                        data_records.append(
                            {
                                "date": record.date,
                                # Temperaturas
                                "temperature_2m_max": record.temp_max,
                                "temperature_2m_min": record.temp_min,
                                "temperature_2m_mean": record.temp_mean,
                                # Umidade
                                "relative_humidity_2m_mean": (
                                    record.humidity_mean
                                ),
                                # Vento
                                "wind_speed_10m_max": record.wind_speed_max,
                                "wind_speed_10m_mean": record.wind_speed_mean,
                                # Precipitação
                                "precipitation_sum": record.precipitation_sum,
                            }
                        )

                    weather_df = pd.DataFrame(data_records)
                    weather_df["date"] = pd.to_datetime(weather_df["date"])
                    weather_df.set_index("date", inplace=True)

                    logger.info(
                        "NWS Forecast: %d registros (%s, %s)",
                        len(nws_forecast_data),
                        latitude,
                        longitude,
                    )

                except ValueError as e:
                    msg = f"NWS Forecast: fora da cobertura USA - {str(e)}"
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

            elif source == "nws_stations":
                # NWS Stations (USA, estações)
                from backend.api.services import NWSStationsSyncAdapter

                adapter = NWSStationsSyncAdapter()

                # Verificar se está na cobertura (USA Continental)
                if not adapter.health_check_sync():
                    msg = "NWS Stations: Verificação falhou"
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                try:
                    # Busca dados via novo adapter síncrono
                    nws_data = adapter.get_daily_data_sync(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )

                    if not nws_data:
                        msg = (
                            f"NWS Stations: Nenhum dado para "
                            f"({latitude}, {longitude})"
                        )
                        logger.warning(msg)
                        warnings_list.append(msg)
                        continue

                    # Converte para DataFrame - variáveis disponíveis do NWS
                    data_records = []
                    for record in nws_data:
                        data_records.append(
                            {
                                "date": record.date,
                                # Temperaturas
                                "temp_celsius": record.temp_mean,
                                # Umidade
                                "humidity_percent": record.humidity,
                                # Vento
                                "wind_speed_ms": record.wind_speed,
                                # Precipitação
                                "precipitation_mm": record.precipitation,
                            }
                        )

                    weather_df = pd.DataFrame(data_records)
                    weather_df["date"] = pd.to_datetime(weather_df["date"])
                    weather_df.set_index("date", inplace=True)

                    logger.info(
                        "NWS Stations: %d registros (%s, %s)",
                        len(nws_data),
                        latitude,
                        longitude,
                    )

                except ValueError as e:
                    msg = f"NWS Stations: fora da cobertura USA - {str(e)}"
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

        except Exception as e:
            logger.error(
                f"{source}: erro ao baixar dados: {str(e)}",
                exc_info=True,  # Mostra traceback completo
            )
            warnings_list.append(f"{source}: erro ao baixar dados: {str(e)}")
            continue

        # Valida DataFrame
        if weather_df is None or weather_df.empty:
            msg = (
                f"Nenhum dado obtido de {source} para "
                f"({latitude}, {longitude}) "
                f"entre {data_inicial} e {data_final}"
            )
            logger.warning(msg)
            warnings_list.append(msg)
            continue

        # Não padronizar colunas - preservar nomes nativos das APIs
        # Cada API retorna suas próprias variáveis específicas
        # Validação será feita em data_preprocessing.py com limits apropriados
        weather_df = weather_df.replace(-999.00, np.nan)
        weather_df = weather_df.dropna(how="all", subset=weather_df.columns)

        # Verifica quantidade de dados
        dias_retornados = (
            weather_df.index.max() - weather_df.index.min()
        ).days + 1
        if dias_retornados < period_days:
            msg = (
                f"{source}: obtidos {dias_retornados} dias "
                f"(solicitados: {period_days})"
            )
            warnings_list.append(msg)

        # Verifica dados faltantes
        perc_faltantes = weather_df.isna().mean() * 100
        nomes_variaveis = {
            # NASA POWER
            "ALLSKY_SFC_SW_DWN": "Radiação Solar (MJ/m²/dia)",
            "PRECTOTCORR": "Precipitação Total (mm)",
            "T2M_MAX": "Temperatura Máxima (°C)",
            "T2M_MIN": "Temperatura Mínima (°C)",
            "T2M": "Temperatura Média (°C)",
            "RH2M": "Umidade Relativa (%)",
            "WS2M": "Velocidade do Vento (m/s)",
            # Open-Meteo (Archive & Forecast)
            "temperature_2m_max": "Temperatura Máxima (°C)",
            "temperature_2m_min": "Temperatura Mínima (°C)",
            "temperature_2m_mean": "Temperatura Média (°C)",
            "relative_humidity_2m_max": "Umidade Relativa Máxima (%)",
            "relative_humidity_2m_min": "Umidade Relativa Mínima (%)",
            "relative_humidity_2m_mean": "Umidade Relativa Média (%)",
            "wind_speed_10m_mean": "Velocidade Média do Vento (m/s)",
            "wind_speed_10m_max": "Velocidade Máxima do Vento (m/s)",
            "shortwave_radiation_sum": "Radiação Solar (MJ/m²/dia)",
            "precipitation_sum": "Precipitação Total (mm)",
            "et0_fao_evapotranspiration": "ETo FAO-56 (mm/dia)",
            # NWS Stations
            "temp_celsius": "Temperatura (°C)",
            "humidity_percent": "Umidade Relativa (%)",
            "wind_speed_ms": "Velocidade do Vento (m/s)",
            "precipitation_mm": "Precipitação (mm)",
        }

        for nome_var, porcentagem in perc_faltantes.items():
            if porcentagem > 25:
                var_portugues = nomes_variaveis.get(
                    str(nome_var), str(nome_var)
                )
                msg = (
                    f"{source}: {porcentagem:.1f}% faltantes em "
                    f"{var_portugues}. Será feita imputação."
                )
                warnings_list.append(msg)

        weather_data_sources.append(weather_df)
        logger.debug("%s: DataFrame obtido\n%s", source, weather_df)

    # Consolidar dados (fusão Kalman feita em eto_services.py)
    if not weather_data_sources:
        msg = "Nenhuma fonte forneceu dados válidos"
        logger.error(msg)
        raise ValueError(msg)

    # Se múltiplas fontes, concatenar para processamento posterior
    if len(weather_data_sources) > 1:
        logger.info(
            f"Concatenando {len(weather_data_sources)} fontes "
            f"para processamento"
        )
        weather_data = pd.concat(weather_data_sources, axis=0)
        # Remover duplicatas de datas, mantendo primeira ocorrência
        weather_data = weather_data[
            ~weather_data.index.duplicated(keep="first")
        ]
    else:
        weather_data = weather_data_sources[0]

    # Validação final - aceitar todas as variáveis das APIs
    # Não mais restringir apenas às variáveis NASA POWER
    # Validação física será feita em data_preprocessing.py

    logger.info("Dados finais obtidos com sucesso")
    logger.debug("DataFrame final:\n%s", weather_data)
    return weather_data, warnings_list
