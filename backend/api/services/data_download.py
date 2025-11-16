from datetime import datetime
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from loguru import logger

# Imports de módulos de validação canônicos
try:
    from backend.api.services.climate_validation import (
        ClimateValidationService,
    )
    from backend.api.services.climate_source_manager import (
        ClimateSourceManager,
    )
    from backend.api.services.climate_factory import ClimateClientFactory
except ImportError:
    from ...api.services.climate_validation import (
        ClimateValidationService,
    )
    from ...api.services.climate_source_manager import (
        ClimateSourceManager,
    )
    from ...api.services.climate_factory import ClimateClientFactory


async def download_weather_data(
    data_source: Union[str, list],
    data_inicial: str,
    data_final: str,
    longitude: float,
    latitude: float,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Baixa dados meteorológicos das fontes especificadas para as coordenadas
    e período.

    Integração completa com módulos de validação e seleção:
    - climate_validation.py: Valida coordenadas, datas e modo
    - climate_source_manager.py: Seleciona fontes por local e modo
    - climate_factory.py: Cria clientes com cache injetado

    Fontes suportadas:
    - "nasa_power": NASA POWER (global, 1990+, domínio público)
    - "openmeteo_archive": Open-Meteo Archive (global, 1990+, CC BY 4.0)
    - "openmeteo_forecast": Open-Meteo Forecast (global, hoje±5d, CC BY 4.0)
    - "met_norway": MET Norway Locationforecast (global, CC BY 4.0)
    - "nws_forecast": NWS Forecast (USA, previsões, domínio público)
    - "nws_stations": NWS Stations (USA, estações, domínio público)
    - "data fusion": Fusiona múltiplas fontes disponíveis (Kalman Ensemble)

    Args:
        data_source: Fonte de dados (str ou list de fontes)
        data_inicial: Data inicial no formato YYYY-MM-DD
        data_final: Data final no formato YYYY-MM-DD
        longitude: Longitude (-180 a 180)
        latitude: Latitude (-90 a 90)
    """
    logger.info(
        f"Iniciando download - Fonte: {data_source}, "
        f"Período: {data_inicial} a {data_final}, "
        f"Coord: ({latitude}, {longitude})"
    )
    warnings_list = []

    # ✅ 1. VALIDAÇÃO DE COORDENADAS
    coord_valid, coord_details = ClimateValidationService.validate_coordinates(
        lat=latitude, lon=longitude, location_name="Download Request"
    )
    if not coord_valid:
        msg = f"Coordenadas inválidas: {coord_details.get('errors')}"
        logger.error(msg)
        raise ValueError(msg)

    # ✅ 2. VALIDAÇÃO DE FORMATO DE DATAS
    date_valid, date_details = ClimateValidationService.validate_date_range(
        start_date=data_inicial,
        end_date=data_final,
        allow_future=True,  # Permite forecast
    )
    if not date_valid:
        msg = f"Datas inválidas: {date_details.get('errors')}"
        logger.error(msg)
        raise ValueError(msg)

    # Converter para pandas datetime para cálculos
    data_inicial_formatted = pd.to_datetime(data_inicial)
    data_final_formatted = pd.to_datetime(data_final)
    period_days = date_details["period_days"]

    # ✅ 3. DETECÇÃO DE MODO (usando módulo oficial)
    detected_mode, error = ClimateValidationService.detect_mode_from_dates(
        data_inicial, data_final
    )
    if not detected_mode:
        warnings_list.append(f"Modo não detectado: {error}")
        # Usar modo padrão baseado nas datas
        today = datetime.now().date()
        end_date_obj = pd.to_datetime(data_final).date()
        if end_date_obj > today:
            detected_mode = "dashboard_forecast"
        else:
            detected_mode = "dashboard_current"
        logger.warning(
            f"Usando modo padrão: {detected_mode} (datas não se encaixam "
            f"perfeitamente nos modos)"
        )

    # ✅ 4. VALIDAÇÃO DE MODO E PERÍODO
    mode_valid, mode_details = ClimateValidationService.validate_request_mode(
        mode=detected_mode,
        start_date=data_inicial,
        end_date=data_final,
    )
    if not mode_valid:
        # Adicionar warnings mas não falhar (pode ser requisição manual)
        mode_errors = mode_details.get("errors", [])
        warnings_list.extend(
            [f"Aviso de modo {detected_mode}: {err}" for err in mode_errors]
        )
        logger.warning(
            f"Validação de modo {detected_mode} com warnings: {mode_errors}"
        )

    logger.info(f"Modo detectado: {detected_mode}")

    # ✅ 5. SELEÇÃO INTELIGENTE DE FONTES (usando ClimateSourceManager)
    source_manager = ClimateSourceManager()

    # Normalizar entrada de data_source
    if isinstance(data_source, list):
        requested_sources = [str(s).lower() for s in data_source]
    else:
        data_source_str = str(data_source).lower()
        if "," in data_source_str:
            requested_sources = [s.strip() for s in data_source_str.split(",")]
        else:
            requested_sources = [data_source_str]

    # Usar método específico para data_download
    if "data fusion" in requested_sources:
        # Data Fusion: usar seleção automática baseada em modo e localização
        try:
            source_result = source_manager.get_sources_for_data_download(
                lat=latitude,
                lon=longitude,
                start_date=pd.to_datetime(data_inicial).date(),
                end_date=pd.to_datetime(data_final).date(),
                mode=detected_mode,
                preferred_sources=None,  # Usar todas disponíveis
            )
            sources = source_result["sources"]
            warnings_list.extend(source_result["warnings"])

            logger.info(
                f"Data Fusion {detected_mode}: {len(sources)} fontes "
                f"selecionadas - {sources}"
            )
        except ValueError as e:
            msg = f"Erro na seleção de fontes para Data Fusion: {str(e)}"
            logger.error(msg)
            raise ValueError(msg)
    else:
        # Fonte(s) específica(s): validar disponibilidade
        try:
            source_result = source_manager.get_sources_for_data_download(
                lat=latitude,
                lon=longitude,
                start_date=pd.to_datetime(data_inicial).date(),
                end_date=pd.to_datetime(data_final).date(),
                mode=detected_mode,
                preferred_sources=requested_sources,
            )
            sources = source_result["sources"]
            warnings_list.extend(source_result["warnings"])

            # Validar que todas as fontes solicitadas estão disponíveis
            unavailable = set(requested_sources) - set(sources)
            if unavailable:
                msg = (
                    f"Fontes indisponíveis para ({latitude}, {longitude}): "
                    f"{unavailable}"
                )
                logger.error(msg)
                raise ValueError(msg)

            logger.info(f"Fontes específicas selecionadas: {sources}")
        except ValueError as e:
            msg = f"Erro na validação de fontes: {str(e)}"
            logger.error(msg)
            raise ValueError(msg)

    if not sources:
        msg = "Nenhuma fonte disponível para esta requisição"
        logger.error(msg)
        raise ValueError(msg)

    weather_data_sources: List[pd.DataFrame] = []
    for source in sources:
        logger.info(f"📥 Processando fonte: {source}")

        # ✅ Validações de limites temporais delegadas aos clientes
        # Cada cliente (adapter) valida seus próprios limites internamente
        # NÃO há necessidade de validar aqui (duplicação removida)
        # Limites canônicos em: climate_source_availability.py
        data_final_adjusted = data_final_formatted

        # 🔄 Download data
        # NOTA: Validações de limites temporais são feitas pelos
        # próprios clientes/adapters. Cada API conhece seus limites
        # e valida internamente.
        # Inicializa variáveis
        weather_df = None

        try:
            if source == "nasa_power":
                # Usar factory para garantir cache Redis injetado
                client = ClimateClientFactory.create_nasa_power()
                try:
                    nasa_data = await client.get_daily_data(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )
                finally:
                    await client.close()

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
                    f"✅ NASA POWER: {len(nasa_data)} registros diários "
                    f"para ({latitude}, {longitude})"
                )

            elif source == "openmeteo_archive":
                # Open-Meteo Archive (histórico desde 1950)
                from backend.api.services.openmeteo_archive.openmeteo_archive_sync_adapter import (
                    OpenMeteoArchiveSyncAdapter,
                )

                client = OpenMeteoArchiveSyncAdapter()
                openmeteo_data = client.get_daily_data_sync(
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
                # ETo: T2M_MAX, T2M_MIN, T2M (mean), RH2M, WS2M,
                #      ALLSKY_SFC_SW_DWN, PRECTOTCORR
                harmonization = {
                    "temperature_2m_max": "T2M_MAX",
                    "temperature_2m_min": "T2M_MIN",
                    "temperature_2m_mean": "T2M",  # NASA usa T2M para média
                    "relative_humidity_2m_mean": "RH2M",
                    "wind_speed_2m_mean": "WS2M",
                    "shortwave_radiation_sum": "ALLSKY_SFC_SW_DWN",
                    "precipitation_sum": "PRECTOTCORR",
                }

                for openmeteo_var, nasa_var in harmonization.items():
                    if openmeteo_var in weather_df.columns:
                        weather_df[nasa_var] = weather_df[openmeteo_var]

                logger.info(
                    f"✅ Open-Meteo Archive: {len(openmeteo_data)} "
                    f"registros diários para ({latitude}, {longitude})"
                )

            elif source == "openmeteo_forecast":
                # Open-Meteo Forecast (previsão + recent: -30d a +5d)
                client = ClimateClientFactory.create_openmeteo_forecast()
                try:
                    forecast_data = await client.get_daily_data(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_formatted,
                    )
                finally:
                    await client.close()

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
                # ETo: T2M_MAX, T2M_MIN, T2M (mean), RH2M, WS2M,
                # ALLSKY_SFC_SW_DWN, PRECTOTCORR
                harmonization = {
                    "temperature_2m_max": "T2M_MAX",
                    "temperature_2m_min": "T2M_MIN",
                    "temperature_2m_mean": "T2M",  # NASA usa T2M para média
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
                    f"✅ Open-Meteo Forecast: {len(forecast_data)} "
                    f"registros diários para ({latitude}, {longitude})"
                )

            elif source == "met_norway":
                # MET Norway Locationforecast (Global, async)
                client = ClimateClientFactory.create_met_norway()
                try:
                    met_data = await client.get_daily_forecast(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )
                finally:
                    await client.close()

                if not met_data:
                    msg = (
                        f"MET Norway: Nenhum dado "
                        f"para ({latitude}, {longitude})"
                    )
                    logger.warning(msg)
                    warnings_list.append(msg)
                    continue

                # Obter variáveis recomendadas para a região
                from backend.api.services import METNorwayClient

                recommended_vars = METNorwayClient.get_recommended_variables(
                    latitude, longitude
                )

                # Verificar se precipitação deve ser incluída
                include_precipitation = "precipitation_sum" in recommended_vars

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

                logger.info(f"MET Norway - {region_info}")

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
                        "relative_humidity_2m_mean": (record.humidity_mean),
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
                    "MET Norway: %d registros (%s, %s), " "variáveis: %s",
                    len(met_data),
                    latitude,
                    longitude,
                    list(weather_df.columns),
                )

            elif source == "nws_forecast":
                # NWS Forecast (USA, previsões)
                client = ClimateClientFactory.create_nws()
                try:
                    nws_forecast_data = await client.get_daily_forecast(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )
                finally:
                    await client.close()

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

            elif source == "nws_stations":
                # NWS Stations (USA, estações)
                client = ClimateClientFactory.create_nws_stations()
                try:
                    nws_data = await client.get_daily_data(
                        lat=latitude,
                        lon=longitude,
                        start_date=data_inicial_formatted,
                        end_date=data_final_adjusted,
                    )
                finally:
                    await client.close()

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
            # MET Norway
            # (mesmas variáveis do Open-Meteo, pois são harmonizadas)
            # As variáveis já estão listadas acima na seção Open-Meteo
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

    # Consolidar dados (fusão Kalman será feita em eto_services.py)
    if not weather_data_sources:
        msg = "Nenhuma fonte forneceu dados válidos"
        logger.error(msg)
        raise ValueError(msg)

    # Se múltiplas fontes, concatenar TODAS as medições
    # A fusão Kalman em eto_services.py aplicará pesos inteligentes
    if len(weather_data_sources) > 1:
        logger.info(
            f"Concatenando {len(weather_data_sources)} fontes "
            f"(fusão Kalman será aplicada em eto_services.py)"
        )
        weather_data = pd.concat(weather_data_sources, axis=0)
        # MANTER duplicatas de datas - cada linha representa 1 fonte
        # Fusão Kalman processará todas as medições
        logger.info(
            f"Total de {len(weather_data)} medições de "
            f"{len(weather_data_sources)} fontes para fusão"
        )
    else:
        weather_data = weather_data_sources[0]

    # Validação final - aceitar todas as variáveis das APIs
    # Não mais restringir apenas às variáveis NASA POWER
    # Validação física será feita em data_preprocessing.py

    logger.info("Dados finais obtidos com sucesso")
    logger.debug("DataFrame final:\n%s", weather_data)
    return weather_data, warnings_list
