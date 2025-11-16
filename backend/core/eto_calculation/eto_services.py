"""
Serviços modulares para cálculo de Evapotranspiração (ETo).

Este módulo implementa a separação de responsabilidades:
- EToCalculationService: Cálculo FAO-56 Penman-Monteith puro (sem I/O)
- EToProcessingService: Orquestração completa do pipeline
  (download → fusion → ETo)

Benefícios:
- Testabilidade: Cada serviço pode ser testado isoladamente
- Reutilização: EToCalculationService pode ser usado em outros contextos
- Manutenibilidade: Responsabilidades claras e bem-definidas
"""

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pandas import Timestamp
from loguru import logger

from backend.api.services.data_download import download_weather_data
from backend.core.data_processing.data_preprocessing import preprocessing
from backend.core.data_processing.kalman_ensemble import KalmanEnsembleStrategy
from backend.api.services.nws_stations.station_finder import StationFinder
from backend.api.services.opentopo import OpenTopoClient
from backend.api.services.weather_utils import (
    ElevationUtils,
    WeatherValidationUtils,
)
from backend.api.services.geographic_utils import GeographicUtils
from config.logging_config import log_execution_time


class EToCalculationService:
    """
    Serviço para cálculo FAO-56 Penman-Monteith puro.

    Responsabilidades:
    - Validação de entrada (dados meteorológicos)
    - Cálculos matemáticos FAO-56
    - Detecção de anomalias
    - SEM I/O (sem PostgreSQL, Redis, downloads)

    Use este serviço quando precisar:
    - Calcular ETo isoladamente
    - Testar lógica de cálculo
    - Reutilizar em outros contextos
    """

    # Constantes FAO-56
    STEFAN_BOLTZMANN = 4.903e-9  # MJ K⁻⁴ m⁻² dia⁻¹
    ALBEDO = 0.23  # Albedo de referência
    MATOPIBA_BOUNDS = {
        "lat_min": -14.5,
        "lat_max": -2.5,
        "lng_min": -50.0,
        "lng_max": -41.5,
    }

    def __init__(self):
        """Inicializa o serviço de cálculo ETo."""
        self.logger = logger

    def _validate_measurements(self, measurements: Dict[str, float]) -> bool:
        """
        Valida presença e valores razoáveis das variáveis climáticas.

        Args:
            measurements: Dict com variáveis climáticas

        Returns:
            True se validado

        Raises:
            ValueError: Se alguma variável obrigatória está ausente
        """
        required_vars = [
            "T2M_MAX",
            "T2M_MIN",
            "T2M_MEAN",
            "RH2M",
            "WS2M",
            "PRECTOTCORR",
            "ALLSKY_SFC_SW_DWN",
            "latitude",
            "longitude",
            "date",
            "elevation_m",
        ]

        missing_vars = [
            var for var in required_vars if var not in measurements
        ]

        if missing_vars:
            raise ValueError(
                f"Variáveis obrigatórias ausentes: {', '.join(missing_vars)}"
            )

        # Validar ranges usando utilitários centralizados
        lat = measurements["latitude"]
        lon = measurements["longitude"]

        # Validar coordenadas
        if not GeographicUtils.is_valid_coordinate(lat, lon):
            raise ValueError(f"Coordenadas inválidas: lat={lat}, lon={lon}")

        # Validar elevação
        elevation = measurements["elevation_m"]
        if elevation < -500 or elevation > 9000:
            raise ValueError(
                f"Elevação {elevation}m fora do range válido (-500 a 9000m)"
            )

        # Validar variáveis meteorológicas
        if not WeatherValidationUtils.is_valid_temperature(
            measurements["T2M_MAX"]
        ):
            raise ValueError(f"T2M_MAX inválida: {measurements['T2M_MAX']}°C")
        if not WeatherValidationUtils.is_valid_temperature(
            measurements["T2M_MIN"]
        ):
            raise ValueError(f"T2M_MIN inválida: {measurements['T2M_MIN']}°C")
        if not WeatherValidationUtils.is_valid_humidity(measurements["RH2M"]):
            raise ValueError(
                f"Umidade relativa inválida: {measurements['RH2M']}%"
            )
        if not WeatherValidationUtils.is_valid_wind_speed(
            measurements["WS2M"]
        ):
            raise ValueError(
                f"Velocidade do vento inválida: {measurements['WS2M']} m/s"
            )
        if measurements["T2M_MAX"] < measurements["T2M_MIN"]:
            raise ValueError(
                f"T2M_MAX ({measurements['T2M_MAX']}°C) < "
                f"T2M_MIN ({measurements['T2M_MIN']}°C)"
            )

        return True

    def calculate_et0(
        self,
        measurements: Dict[str, float],
        method: str = "pm",
        elevation_factors: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Calcula ET0 diária usando FAO-56 Penman-Monteith.

        Args:
            measurements: Dict com 12 variáveis climáticas:
                - T2M_MAX: Temperatura máxima (°C)
                - T2M_MIN: Temperatura mínima (°C)
                - T2M_MEAN: Temperatura média (°C)
                - RH2M: Umidade relativa (%)
                - WS2M: Velocidade do vento a 2m (m/s)
                - PRECTOTCORR: Precipitação (mm)
                - ALLSKY_SFC_SW_DWN: Radiação solar (MJ/m²/dia)
                - PS: Pressão atmosférica (kPa)
                - latitude: Latitude (°)
                - longitude: Longitude (°)
                - date: Data (YYYY-MM-DD)
                - elevation_m: Elevação (m)
            method: Método de cálculo ('pm' para Penman-Monteith)

        Returns:
            Dict com:
            {
                'et0_mm_day': float,      # ET0 diária (mm/dia)
                'quality': str,           # 'high' ou 'low'
                'method': str,            # Método usado
                'components': {           # Componentes do cálculo
                    'Ra': float,          # Radiação extraterrestre
                    'Rn': float,          # Radiação net
                    'slope': float,       # Declividade da curva de vapor
                    'gamma': float        # Constante psicrométrica
                }
            }
        """
        try:
            # 1. Validação
            self._validate_measurements(measurements)

            # 2. Extração de variáveis
            T_max = measurements["T2M_MAX"]
            T_min = measurements["T2M_MIN"]
            T_mean = measurements["T2M_MEAN"]
            RH_mean = measurements["RH2M"]
            u2 = measurements["WS2M"]
            Rs = measurements["ALLSKY_SFC_SW_DWN"]  # MJ/m²/dia
            z = measurements["elevation_m"]
            lat = measurements["latitude"]
            date_str: str = str(measurements["date"])

            # Usar fatores de elevação pré-calculados ou calcular
            if elevation_factors:
                P = elevation_factors.get("pressure", 101.3)
            else:
                # Usar utilitário centralizado (FAO-56 Eq. 7)
                P = ElevationUtils.calculate_atmospheric_pressure(z)

            # 3. Cálculos intermediários FAO-56

            # 3a. Saturação de vapor
            es_T_max = self._saturation_vapor_pressure(T_max)
            es_T_min = self._saturation_vapor_pressure(T_min)
            es = (es_T_max + es_T_min) / 2

            # 3b. Pressão de vapor atual
            ea = (RH_mean / 100.0) * es

            # 3c. Déficit de pressão de vapor
            Vpd = es - ea

            # 3d. Declinação solar e ângulo solar
            N = self._day_of_year(date_str)
            delta = self._solar_declination(N)

            # 3e. Radiação extraterrestre (Ra)
            Ra = self._extraterrestrial_radiation(lat, N, delta)

            # 3f. Radiação net (aproximação)
            # Rn = 0.77 * Rs (simplificação se Rn_long não disponível)
            Rn_sw = (1 - self.ALBEDO) * Rs  # Radiação net de ondas curtas
            Rn_lw = 0.23  # Aproximação para Rn de ondas longas
            Rn = Rn_sw - (Rn_lw * Rs)  # Simplificação

            # 3g. Calor do solo (assume zero para períodos diários)
            G = 0

            # 3h. Declividade da curva de vapor (Δ)
            slope = self._vapor_pressure_slope(T_mean)

            # 3i. Constante psicrométrica (γ)
            if elevation_factors:
                gamma = elevation_factors.get("gamma", 0.665e-3 * P)
            else:
                # Usar utilitário centralizado (FAO-56 Eq. 8)
                gamma = ElevationUtils.calculate_psychrometric_constant(z)

            # 4. Penman-Monteith (FAO-56 Eq. 6)
            Cn = 900  # Coeficiente para ETo
            Cd = 0.34  # Coeficiente para ETo

            numerator = (
                0.408 * slope * (Rn - G)
                + gamma * (Cn / (T_mean + 273)) * u2 * Vpd
            )
            denominator = slope + gamma * (1 + Cd * u2)

            if denominator == 0:
                ET0 = np.nan
                quality = "low"
            else:
                ET0 = numerator / denominator

            # 5. Validação de qualidade
            quality = "high"
            if ET0 < 0 or ET0 > 15 or np.isnan(ET0):  # Sanity checks
                quality = "low"
                if np.isnan(ET0):
                    ET0 = 0

            return {
                "et0_mm_day": round(max(0, ET0), 2),
                "quality": quality,
                "method": method,
                "components": {
                    "Ra": round(Ra, 2),
                    "Rn": round(Rn, 2),
                    "slope": round(slope, 4),
                    "gamma": round(gamma, 4),
                    "Vpd": round(Vpd, 2),
                },
            }

        except Exception as e:
            self.logger.error(f"Erro no cálculo de ETo: {str(e)}")
            return {
                "et0_mm_day": 0,
                "quality": "low",
                "method": method,
                "components": {},
                "error": str(e),
            }

    def _saturation_vapor_pressure(self, T: float) -> float:
        """
        Pressão de saturação de vapor (FAO-56 Eq. 11).

        Args:
            T: Temperatura em °C

        Returns:
            Pressão de saturação em kPa
        """
        return 0.6108 * math.exp((17.27 * T) / (T + 237.3))

    def _vapor_pressure_slope(self, T: float) -> float:
        """
        Declividade da curva de vapor de pressão (FAO-56 Eq. 13).

        Args:
            T: Temperatura média em °C

        Returns:
            Declividade em kPa/°C
        """
        exp_term = (17.27 * T) / (T + 237.3)
        return (4098 * 0.6108 * math.exp(exp_term)) / ((T + 237.3) ** 2)

    def _solar_declination(self, N: int) -> float:
        """
        Declinação solar (FAO-56 Eq. 34).

        Args:
            N: Dia do ano (1-365/366)

        Returns:
            Declinação solar em radianos
        """
        b = 2 * math.pi * (N - 1) / 365.0
        return 0.409 * math.sin(b - 1.39)

    def _extraterrestrial_radiation(
        self, lat: float, N: int, delta: float
    ) -> float:
        """
        Radiação extraterrestre (FAO-56 Eq. 21).

        Args:
            lat: Latitude em graus
            N: Dia do ano (1-365/366)
            delta: Declinação solar em radianos

        Returns:
            Radiação extraterrestre em MJ/m²/dia
        """
        phi = math.radians(lat)
        dr = 1 + 0.033 * math.cos(2 * math.pi * N / 365.0)

        omega_s = math.acos(-math.tan(phi) * math.tan(delta))

        Ra = (
            (24 * 60 / math.pi)
            * 0.0820
            * dr
            * (
                omega_s * math.sin(phi) * math.sin(delta)
                + math.cos(phi) * math.cos(delta) * math.sin(omega_s)
            )
        )

        return max(0, Ra)  # Ra nunca deve ser negativo

    def _day_of_year(self, date_str: str) -> int:
        """
        Calcula o dia do ano.

        Args:
            date_str: Data em formato YYYY-MM-DD

        Returns:
            Dia do ano (1-366)
        """
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date.timetuple().tm_yday

    def detect_anomalies(
        self, et0: float, historical_normal: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        Detecta anomalias comparando com histórico.

        Args:
            et0: Valor de ETo calculado (mm/dia)
            historical_normal: Dict com 'mean', 'std_dev' do histórico

        Returns:
            Dict com detecção de anomalia:
            {
                'is_anomaly': bool,
                'z_score': float,
                'deviation_percent': float
            }

        Example:
            >>> historical = {'mean': 3.8, 'std_dev': 0.6}
            >>> service.detect_anomalies(4.2, historical)
            {'is_anomaly': False, 'z_score': 0.67, 'deviation_percent': 10.5}
        """
        if not historical_normal:
            return {
                "is_anomaly": False,
                "z_score": None,
                "deviation_percent": None,
            }

        mean = historical_normal.get("mean", 0)
        std = historical_normal.get("std_dev", 1)

        if std == 0 or mean == 0:
            return {"is_anomaly": False, "z_score": 0, "deviation_percent": 0}

        z_score = (et0 - mean) / std
        is_anomaly = abs(z_score) > 2.5  # 2.5 standard deviations
        deviation_percent = (et0 - mean) / mean * 100

        return {
            "is_anomaly": is_anomaly,
            "z_score": round(z_score, 2),
            "deviation_percent": round(deviation_percent, 1),
        }


class EToProcessingService:
    """
    Serviço para orquestração do pipeline ETo completo.

    Responsabilidades:
    - Orquestra: download → preprocessing → fusion → ETo
    - Gerencia cache (Redis)
    - Gerencia histórico (PostgreSQL)
    - Gera recomendações agrícolas

    Use este serviço quando precisar:
    - Pipeline completo de download até ETo
    - Integração com histórico
    - Recomendações agrícolas
    """

    # Constantes
    MATOPIBA_BOUNDS = {
        "lat_min": -14.5,
        "lat_max": -2.5,
        "lng_min": -50.0,
        "lng_max": -41.5,
    }

    def __init__(self, db_session=None, redis_client=None, s3_client=None):
        """
        Inicializa o serviço de processamento ETo.

        Args:
            db_session: Sessão SQLAlchemy (para histórico)
            redis_client: Cliente Redis (para cache)
            s3_client: Cliente S3 (opcional)
        """
        self.db_session = db_session
        self.redis_client = redis_client
        self.s3_client = s3_client
        self.et0_calc = EToCalculationService()
        self.kalman = KalmanEnsembleStrategy(db_session, redis_client)
        self.station_finder = StationFinder(db_session)
        self.logger = logger

    @log_execution_time
    async def process_location(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        elevation: Optional[float] = None,
        include_recomendations: bool = True,
        database: str = "nasa_power",
        use_precise_elevation: bool = True,
    ) -> Dict[str, Any]:
        """
        Processa localidade completa: download → elevação → fusion → ETo.

        NEW: Integração com OpenTopoData para elevação precisa (1m de
        resolução) vs Open-Meteo (~7-30m).

        Args:
            latitude: Latitude (-90 a 90)
            longitude: Longitude (-180 a 180)
            start_date: Data inicial (YYYY-MM-DD)
            end_date: Data final (YYYY-MM-DD)
            elevation: Elevação em metros (opcional, será obtida de
                       OpenTopoData se não fornecida)
            include_recomendations: Se deve gerar recomendações
            database: Base de dados ('nasa_power' ou outro)
            use_precise_elevation: Se True, tenta OpenTopoData (default: True)

        Returns:
            Dict com resultado completo:
            {
                'location': {'lat': float, 'lon': float},
                'elevation': {
                    'value': float,
                    'source': 'opentopo' | 'openmeteo' | 'input',
                    'openmeteo': float (if available),
                    'opentopo': float (if available),
                    'difference_m': float,
                },
                'elevation_factors': {
                    'pressure': float,
                    'gamma': float,
                    'solar_factor': float,
                },
                'period': {'start': str, 'end': str},
                'et0_series': [
                    {
                        'date': '2024-09-01',
                        'et0_mm_day': 4.5,
                        'quality': 'high',
                        'anomaly': {'is_anomaly': False, 'z_score': 0.3}
                    },
                    ...
                ],
                'summary': {...},
                'recomendations': [...]  # If include_recomendations=True
            }
        """
        topo_client = None

        try:
            # ═══════════════════════════════════════════════════════════
            # ESTRATÉGIA DE ELEVAÇÃO OTIMIZADA (Nov 2025)
            # ═══════════════════════════════════════════════════════════
            # PRIORIDADE DE ELEVAÇÃO:
            # 1. Input do usuário (se fornecido)
            # 2. OpenTopo (~1m) - Máxima precisão
            # 3. Open-Meteo Archive/Forecast (~7-30m) - Fallback
            # 4. Default (0m) - Último recurso
            #
            # ESTRATÉGIA DE REQUISIÇÕES (economiza limite diário):
            # - Baixar dados climáticos PRIMEIRO (Open-Meteo grátis)
            # - Tentar OpenTopo (se user não deu input)
            # - Se OpenTopo falhar, usar elevation do Open-Meteo
            # ═══════════════════════════════════════════════════════════

            elevation_archive = None  # Open-Meteo Archive elevation
            elevation_forecast = None  # Open-Meteo Forecast elevation
            elevation_opentopo = None  # OpenTopo elevation
            final_elevation = None
            elevation_source = None

            # 1️⃣ BAIXAR DADOS CLIMÁTICOS PRIMEIRO
            # (Open-Meteo já retorna elevation sem custo extra)
            logger.info("📊 Etapa 1: Baixar dados climáticos")

            weather_data, download_warnings = await download_weather_data(
                database, start_date, end_date, longitude, latitude
            )

            if weather_data is None or weather_data.empty:
                raise ValueError("Falha ao obter dados meteorológicos")

            # Extrair elevação do Open-Meteo (Archive ou Forecast)
            if hasattr(weather_data, "attrs"):
                elevation_meteo = weather_data.attrs.get("elevation")

                # Identificar fonte específica (archive vs forecast)
                if "archive" in database.lower():
                    elevation_archive = elevation_meteo
                    if elevation_archive:
                        logger.info(
                            f"   ✅ Open-Meteo Archive elevation: "
                            f"{elevation_archive:.1f}m (fallback disponível)"
                        )
                elif "forecast" in database.lower() or database == "openmeteo":
                    elevation_forecast = elevation_meteo
                    if elevation_forecast:
                        logger.info(
                            f"   ✅ Open-Meteo Forecast elevation: "
                            f"{elevation_forecast:.1f}m (fallback disponível)"
                        )

            # 2️⃣ OBTER ELEVAÇÃO PRECISA (OpenTopo)
            # Só tenta se user não forneceu input
            logger.info("🗻 Etapa 2: Obter elevação precisa")

            if elevation is None and use_precise_elevation:
                logger.info(
                    "   🎯 Tentando OpenTopo para máxima precisão (~1m)..."
                )
                try:
                    topo_client = OpenTopoClient()
                    topo_location = await topo_client.get_elevation(
                        latitude, longitude
                    )

                    if topo_location:
                        elevation_opentopo = topo_location.elevation
                        logger.info(
                            f"   ✅ OpenTopo: {elevation_opentopo:.1f}m "
                            f"({topo_location.dataset}, precisão: ~1m)"
                        )
                    else:
                        logger.warning("   ⚠️  OpenTopo retornou None")

                except Exception as e:
                    logger.warning(
                        f"   ⚠️  OpenTopo falhou: {e} "
                        f"(usando fallback Open-Meteo)"
                    )

            # 3️⃣ DECISÃO DE ELEVAÇÃO (Cascata de Fallback)
            logger.info("🔧 Etapa 3: Determinar elevação final")

            # Prioridade 1: Input do usuário
            if elevation is not None:
                final_elevation = elevation
                elevation_source = "input"
                logger.info(
                    f"   📍 Usando elevação do INPUT: {final_elevation:.1f}m"
                )

            # Prioridade 2: OpenTopo (~1m) - Máxima precisão
            elif elevation_opentopo is not None:
                final_elevation = elevation_opentopo
                elevation_source = "opentopo"
                logger.info(
                    f"   🎯 Usando elevação OPENTOPO: "
                    f"{final_elevation:.1f}m (precisão: ~1m)"
                )

            # Prioridade 3: Open-Meteo Archive (~7-30m) - Fallback
            elif elevation_archive is not None:
                final_elevation = elevation_archive
                elevation_source = "openmeteo_archive"
                logger.info(
                    f"   🌐 Usando elevação OPEN-METEO ARCHIVE (fallback): "
                    f"{final_elevation:.1f}m (precisão: ~7-30m)"
                )

            # Prioridade 4: Open-Meteo Forecast (~7-30m) - Fallback
            elif elevation_forecast is not None:
                final_elevation = elevation_forecast
                elevation_source = "openmeteo_forecast"
                logger.info(
                    f"   🌐 Usando elevação OPEN-METEO FORECAST (fallback): "
                    f"{final_elevation:.1f}m (precisão: ~7-30m)"
                )

            # Prioridade 5: Default (0m - nível do mar)
            else:
                final_elevation = 0.0
                elevation_source = "default"
                logger.warning(
                    "   ⚠️ Nenhuma elevação disponível - "
                    "assumindo nível do mar (0m)"
                )

            # Análise de impacto (se múltiplas fontes disponíveis)
            if elevation_opentopo and (
                elevation_archive or elevation_forecast
            ):
                elevation_meteo_best = (
                    elevation_archive
                    if elevation_archive
                    else elevation_forecast
                )
                impact = ElevationUtils.compare_elevation_impact(
                    elevation_opentopo, elevation_meteo_best
                )
                logger.info(
                    f"   💡 Diferença OpenTopo vs Open-Meteo: "
                    f"{impact['elevation_diff_m']:.1f}m | "
                    f"Impacto ETo: {impact['eto_impact_pct']:.3f}% "
                    f"({impact['recommendation']})"
                )

            # 4️⃣ CALCULAR FATORES DE CORREÇÃO POR ELEVAÇÃO
            logger.info("⚙️ Etapa 4: Calcular fatores de correção de elevação")

            elevation_factors = ElevationUtils.get_elevation_correction_factor(
                final_elevation
            )

            logger.info(f"   Pressão: {elevation_factors['pressure']:.2f} kPa")
            logger.info(
                f"   Gamma (Y): {elevation_factors['gamma']:.5f} kPa/°C"
            )
            logger.info(
                f"   Solar factor: {elevation_factors['solar_factor']:.4f}"
            )

            # 5️⃣ PRÉ-PROCESSAMENTO
            logger.info("📈 Etapa 5: Pré-processamento de dados")

            weather_data, preprocessing_warnings = preprocessing(
                weather_data, latitude
            )

            # Adicionar elevação ao DataFrame
            weather_data["elevation_m"] = final_elevation

            # 6️⃣ FUSÃO (Kalman com histórico)
            logger.info("🔀 Etapa 6: Fusão de dados (Kalman Ensemble)")

            weather_data_fused, fusion_warnings = await self._fuse_data(
                weather_data, latitude, longitude
            )

            # 7️⃣ CÁLCULO DE ETo PARA CADA DIA
            logger.info("✅ Etapa 7: Calcular ETo (FAO-56)")

            et0_series = []
            raw_data_list = []  # Para salvar no banco

            for idx, row in weather_data_fused.iterrows():
                measurements = row.to_dict()
                measurements["latitude"] = latitude
                measurements["longitude"] = longitude
                # Converter idx para string de data
                if isinstance(idx, Timestamp):
                    date_str_val = str(idx.date())
                else:
                    date_str_val = str(idx)[:10]
                measurements["date"] = date_str_val

                # Usar elevação e fatores pré-calculados
                et0_result = self.et0_calc.calculate_et0(
                    measurements,
                    elevation_factors=elevation_factors,
                )

                # 5. Detecção anomalia (com histórico)
                historical = await self._get_historical_et0_normal(
                    latitude, longitude, date_str_val
                )
                anomaly = self.et0_calc.detect_anomalies(
                    et0_result["et0_mm_day"], historical
                )

                et0_data = {
                    "date": date_str_val,
                    "et0_mm_day": et0_result["et0_mm_day"],
                    "quality": et0_result["quality"],
                    "anomaly": anomaly,
                }
                et0_series.append(et0_data)

                # Preparar dados para salvamento
                raw_data_list.append(
                    {
                        "date": measurements["date"],
                        "raw_data": measurements,
                        "eto_result": et0_result,
                    }
                )

            # 8️⃣ SALVAR NO BANCO PostgreSQL
            logger.info("💾 Etapa 8: Salvar dados no banco")

            if self.db_session and et0_series:
                await self._save_to_database(
                    latitude=latitude,
                    longitude=longitude,
                    elevation=final_elevation,
                    elevation_source=elevation_source,
                    elevation_opentopo=elevation_opentopo,
                    elevation_archive=elevation_archive,
                    elevation_forecast=elevation_forecast,
                    source_api=database,
                    raw_data_list=raw_data_list,
                )

            # 9️⃣ RECOMENDAÇÕES (agrícolas)
            logger.info("📋 Etapa 9: Gerar recomendações")

            recomendations = None
            if include_recomendations:
                recomendations = self._generate_recomendations(et0_series)

            logger.info("✅ Processamento completo!")

            return {
                "location": {"lat": latitude, "lon": longitude},
                "elevation": {
                    "value": final_elevation,
                    "source": elevation_source,
                    "opentopo": elevation_opentopo,
                    "archive": elevation_archive,
                    "forecast": elevation_forecast,
                    "difference_m": (
                        abs(
                            elevation_opentopo
                            - (elevation_archive or elevation_forecast)
                        )
                        if elevation_opentopo
                        and (elevation_archive or elevation_forecast)
                        else None
                    ),
                },
                "elevation_factors": elevation_factors,
                "period": {"start": start_date, "end": end_date},
                "et0_series": et0_series,
                "eto_data": et0_series,  # Compatibilidade com fmt antigo
                "summary": self._summarize_series(et0_series),
                "statistics": (self._summarize_series(et0_series)),  # Alias
                "recomendations": recomendations,
            }

        except Exception as e:
            self.logger.error(f"❌ Erro ao processar localidade: {str(e)}")
            return {"error": str(e)}

        finally:
            if topo_client:
                await topo_client.close()

    async def _fuse_data(
        self, weather_data: pd.DataFrame, latitude: float, longitude: float
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        Funde dados usando Kalman Ensemble com histórico.

        Args:
            weather_data: DataFrame com dados brutos
            latitude: Latitude
            longitude: Longitude

        Returns:
            Tuple com (DataFrame fusionado, lista de warnings)
        """
        warnings = []
        try:
            # Processar cada registro individualmente
            fused_records = []

            for _, row in weather_data.iterrows():
                # Converter linha para dict
                current_measurements = row.to_dict()

                # Usar wrapper síncrono se disponível
                if hasattr(self.kalman, "auto_fuse_sync"):
                    result = self.kalman.auto_fuse_sync(
                        latitude, longitude, current_measurements
                    )
                else:
                    # Fallback para async
                    result = await self.kalman.auto_fuse(
                        latitude, longitude, current_measurements
                    )

                fused_records.append(result)

            # Criar DataFrame preservando o índice datetime original
            fused_df = pd.DataFrame(fused_records, index=weather_data.index)
            return fused_df, warnings

        except Exception as e:
            warnings.append(f"Erro na fusão de dados: {str(e)}")
            self.logger.warning(f"Fusão falhou, usando dados brutos: {str(e)}")
            return weather_data, warnings

    async def _get_historical_et0_normal(
        self, latitude: float, longitude: float, date_str: str
    ) -> Optional[Dict[str, float]]:
        """
        Busca normal histórica de ET0 para detectar anomalias.

        Args:
            latitude: Latitude
            longitude: Longitude
            date_str: Data (YYYY-MM-DD)

        Returns:
            Dict com 'mean' e 'std_dev' ou None
        """
        try:
            # Buscar cidade estudada próxima
            if hasattr(self.station_finder, "find_studied_city_sync"):
                city_data = self.station_finder.find_studied_city_sync(
                    latitude, longitude, max_distance_km=10
                )
            else:
                city_data = await self.station_finder.find_studied_city(
                    latitude, longitude, max_distance_km=10
                )

            if not city_data or "monthly_data" not in city_data:
                return None

            # Extrair mês
            month = int(date_str.split("-")[1])
            monthly_data = city_data.get("monthly_data", {})

            month_key = f"month_{month}"
            if month_key not in monthly_data:
                return None

            month_stats = monthly_data[month_key]
            return {
                "mean": month_stats.get("mean_et0", 0),
                "std_dev": month_stats.get("std_et0", 1),
            }

        except Exception as e:
            self.logger.debug(f"Não foi possível obter histórico: {str(e)}")
            return None

    async def _save_to_database(
        self,
        latitude: float,
        longitude: float,
        elevation: Optional[float],
        elevation_source: Optional[str] = None,
        elevation_opentopo: Optional[float] = None,
        elevation_archive: Optional[float] = None,
        elevation_forecast: Optional[float] = None,
        source_api: str = "nasa_power",
        raw_data_list: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Salva dados climáticos e ETo no banco PostgreSQL.

        Args:
            latitude: Latitude
            longitude: Longitude
            elevation: Elevação em metros
            source_api: Nome da API fonte
            raw_data_list: Lista com dados brutos e resultados
        """
        try:
            if not raw_data_list:
                return

            from backend.database.models.climate_data import ClimateData
            from datetime import datetime as dt

            saved_count = 0

            for data_item in raw_data_list:
                try:
                    date_str = data_item["date"]
                    date_obj = dt.strptime(date_str, "%Y-%m-%d")

                    # Verificar se já existe
                    existing = (
                        self.db_session.query(ClimateData)
                        .filter_by(
                            source_api=source_api,
                            latitude=latitude,
                            longitude=longitude,
                            date=date_obj,
                        )
                        .first()
                    )

                    if existing:
                        # Atualizar registro existente
                        # Converter NaN para None (PostgreSQL JSONB)
                        import math

                        raw_data_clean = {}
                        for k, v in data_item["raw_data"].items():
                            if isinstance(v, float) and math.isnan(v):
                                raw_data_clean[k] = None
                            else:
                                raw_data_clean[k] = v

                        existing.raw_data = raw_data_clean
                        existing.eto_mm_day = data_item["eto_result"][
                            "et0_mm_day"
                        ]
                        existing.eto_method = data_item["eto_result"]["method"]
                        existing.quality_flags = {
                            "quality": data_item["eto_result"]["quality"]
                        }
                        existing.elevation = elevation
                        existing.elevation_metadata = {
                            "source": elevation_source,
                            "opentopo": elevation_opentopo,
                            "archive": elevation_archive,
                            "forecast": elevation_forecast,
                        }
                        existing.updated_at = dt.utcnow()
                    else:
                        # Criar novo registro
                        # Converter NaN para None (PostgreSQL JSONB)
                        import math

                        raw_data_clean = {}
                        for k, v in data_item["raw_data"].items():
                            if isinstance(v, float) and math.isnan(v):
                                raw_data_clean[k] = None
                            else:
                                raw_data_clean[k] = v

                        climate_record = ClimateData(
                            source_api=source_api,
                            latitude=latitude,
                            longitude=longitude,
                            elevation=elevation,
                            date=date_obj,
                            raw_data=raw_data_clean,  # Sem NaN
                            harmonized_data=None,
                            eto_mm_day=data_item["eto_result"]["et0_mm_day"],
                            eto_method=data_item["eto_result"]["method"],
                            quality_flags={
                                "quality": data_item["eto_result"]["quality"]
                            },
                            elevation_metadata={
                                "source": elevation_source,
                                "opentopo": elevation_opentopo,
                                "archive": elevation_archive,
                                "forecast": elevation_forecast,
                            },
                            processing_metadata={
                                "components": data_item["eto_result"].get(
                                    "components", {}
                                ),
                                "processed_at": dt.utcnow().isoformat(),
                            },
                        )
                        self.db_session.add(climate_record)

                    saved_count += 1

                except Exception as e:
                    self.logger.warning(
                        f"Erro ao salvar registro {date_str}: {str(e)}"
                    )
                    continue

            # Commit de todos os registros
            self.db_session.commit()
            self.logger.info(
                f"✅ {saved_count} registros salvos no banco "
                f"(fonte: {source_api})"
            )

        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar no banco: {str(e)}")
            self.db_session.rollback()
            # Não falhar a requisição por erro de salvamento

    def _generate_recomendations(self, et0_series: List[Dict]) -> List[str]:
        """
        Gera recomendações agrícolas baseadas em ET0.

        Args:
            et0_series: Série de dados ETo

        Returns:
            Lista de recomendações
        """
        recs = []

        if not et0_series:
            return recs

        # Calcular estatísticas
        values = [d["et0_mm_day"] for d in et0_series]
        mean_et0 = sum(values) / len(values)
        total_et0 = sum(values)
        anomalies = [d for d in et0_series if d["anomaly"]["is_anomaly"]]

        # Gerar recomendações
        if mean_et0 > 6:
            recs.append("⚠️ ET0 alta: Aumentar irrigação (+ 150% ETo normal)")
        elif mean_et0 < 2:
            recs.append("✓ ET0 baixa: Reduzir irrigação (- 50% ETo normal)")
        else:
            recs.append(f"✓ ET0 normal: ~{mean_et0:.1f} mm/dia")

        if len(anomalies) > len(et0_series) * 0.3:
            recs.append(
                f"⚠️ Anomalias em {len(anomalies)} dias - "
                "revisar dados climáticos"
            )

        total_irrigation = total_et0 * 1.1  # Coeficiente de cultura = 1.1
        recs.append(
            f"💧 Irrigação estimada: {total_irrigation:.1f} mm para o período"
        )

        return recs

    def _summarize_series(self, et0_series: List[Dict]) -> Dict[str, float]:
        """
        Cria resumo estatístico da série ETo.

        Args:
            et0_series: Série de dados ETo

        Returns:
            Dict com estatísticas
        """
        if not et0_series:
            return {}

        values = [d["et0_mm_day"] for d in et0_series]

        return {
            "total_days": len(et0_series),
            "et0_total_mm": round(sum(values), 1),
            "et0_mean_mm_day": round(sum(values) / len(values), 2),
            "et0_max_mm_day": round(max(values), 2),
            "et0_min_mm_day": round(min(values), 2),
            "anomaly_count": sum(
                1 for d in et0_series if d["anomaly"]["is_anomaly"]
            ),
        }

    @log_execution_time
    async def process_location_with_sources(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        sources: List[str],
        elevation: Optional[float] = None,
        estado: Optional[str] = None,
        cidade: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Processa localidade com múltiplas fontes:
        download → preprocessing → fusion → eto.

        Esta é a implementação da lógica de fusão configurável
        que estava no endpoint /calculate.

        Args:
            latitude: Latitude (-90 a 90)
            longitude: Longitude (-180 a 180)
            start_date: Data inicial (YYYY-MM-DD)
            end_date: Data final (YYYY-MM-DD)
            sources: Lista de fontes climáticas a usar
            elevation: Elevação em metros (opcional)
            estado: Estado/região (metadata)
            cidade: Cidade (metadata)

        Returns:
            Dict com resultado completo no formato do endpoint /calculate
        """
        try:
            from backend.api.services.climate_source_manager import (
                ClimateSourceManager,
            )
            from backend.api.services.data_download import (
                download_weather_data,
            )
            from backend.core.data_processing.data_preprocessing import (
                preprocessing,
            )

            # 1. Baixar dados de cada fonte selecionada
            all_weather_data = []
            fusion_warnings = []

            for source_id in sources:
                try:
                    self.logger.info(
                        f"📥 Baixando dados de {source_id} para "
                        f"({latitude}, {longitude})"
                    )
                    weather_data, warnings = await download_weather_data(
                        source_id, start_date, end_date, longitude, latitude
                    )

                    if weather_data is not None and not weather_data.empty:
                        # Adicionar metadados da fonte
                        weather_data["source"] = source_id
                        all_weather_data.append(weather_data)
                        if warnings:
                            fusion_warnings.extend(warnings)
                        self.logger.info(
                            f"✅ Dados de {source_id}: "
                            f"{len(weather_data)} registros"
                        )
                    else:
                        self.logger.warning(f"⚠️ Sem dados de {source_id}")
                        fusion_warnings.append(
                            f"Sem dados disponíveis de {source_id}"
                        )

                except Exception as e:
                    self.logger.error(
                        f"❌ Erro ao baixar {source_id}: {str(e)}"
                    )
                    fusion_warnings.append(f"Erro em {source_id}: {str(e)}")

            if not all_weather_data:
                raise ValueError(
                    "Nenhum dado climático disponível das fontes selecionadas."
                )

            # 2. ✅ CORREÇÃO: Combinar dados por data para fusão Kalman
            import pandas as pd

            combined_data = pd.concat(all_weather_data, ignore_index=False)

            # Garantir que date é datetime e está como índice
            if combined_data.index.name != "date":
                if "date" not in combined_data.columns:
                    raise ValueError(
                        "Coluna 'date' ausente nos dados climáticos"
                    )
                combined_data["date"] = pd.to_datetime(combined_data["date"])
                combined_data.set_index("date", inplace=True)

            # 3. Preprocessing (normalização, outlier detection)
            # Detectar região baseada na latitude (Brasil: -35 a 5 lat)
            region = "brazil" if -35 <= latitude <= 5 else "global"
            combined_data, preprocessing_warnings = preprocessing(
                combined_data, latitude, region=region
            )
            fusion_warnings.extend(preprocessing_warnings)

            # 4. ✅ CORREÇÃO: Fusão Kalman POR DIA (múltiplas fontes)
            self.logger.info(
                f"🔬 Iniciando fusão Kalman para "
                f"{len(combined_data.index.unique())} dias..."
            )

            fused_records = []

            # Agrupar por data (múltiplas linhas por data = múltiplas fontes)
            for date_key, group in combined_data.groupby(level=0):
                # Se apenas 1 fonte, sem necessidade de fusão
                if len(group) == 1:
                    single_record = group.iloc[0].to_dict()
                    fused_records.append(
                        {
                            "date": date_key,
                            **single_record,
                            "fusion_applied": False,
                        }
                    )
                    continue

                # Aplicar Kalman para múltiplas fontes
                try:
                    # Variáveis climáticas para fusão
                    climate_vars = [
                        "temperature_2m_max",
                        "temperature_2m_min",
                        "temperature_2m_mean",
                        "relative_humidity_2m_mean",
                        "wind_speed_2m_mean",
                        "shortwave_radiation_sum",
                        "precipitation_sum",
                    ]

                    # Preparar dados de cada fonte separadamente
                    # para fusão real
                    stations_data = []
                    for idx, row in group.iterrows():
                        station_measurement = {}
                        for var in climate_vars:
                            if var in row.index and not pd.isna(row[var]):
                                station_measurement[var] = row[var]
                        if station_measurement:
                            stations_data.append(station_measurement)

                    if len(stations_data) > 1:
                        # ✅ FUSÃO REAL: múltiplas fontes via Kalman
                        fused_result = self.kalman.auto_fuse_sync(
                            latitude,
                            longitude,
                            current_measurements={},
                            # Não usado quando stations_data é fornecido
                            stations_data=stations_data,
                            distance_weights=None,
                            # Pesos iguais para todas as fontes
                        )

                        fused_records.append(
                            {
                                "date": date_key,
                                **fused_result,
                                "fusion_applied": True,
                                "fusion_strategy": self.kalman.fusion.fusion_strategy,
                                "sources_count": len(stations_data),
                            }
                        )
                    else:
                        # Apenas 1 fonte válida - usar diretamente
                        fused_records.append(
                            {
                                "date": date_key,
                                **stations_data[0],
                                "fusion_applied": False,
                                "sources_count": 1,
                            }
                        )

                except Exception as e:
                    self.logger.error(f"Fusão falhou para {date_key}: {e}")
                    # Fallback: usar primeira fonte disponível
                    fallback_data = {}
                    for var in climate_vars:
                        if var in group.columns:
                            values = group[var].dropna().tolist()
                            if values:
                                fallback_data[var] = values[
                                    0
                                ]  # Primeira fonte

                    fused_records.append(
                        {
                            "date": date_key,
                            **fallback_data,
                            "fusion_applied": False,
                            "fusion_error": str(e),
                            "sources_count": len(group),
                        }
                    )

            # Criar DataFrame fusionado
            weather_data_fused = pd.DataFrame(fused_records)
            weather_data_fused.set_index("date", inplace=True)

            self.logger.info(
                f"✅ Fusão concluída: {len(weather_data_fused)} dias"
            )

            # 5. Calcular ETo para cada dia
            et0_series = []

            for idx, row in weather_data_fused.iterrows():
                try:
                    measurements = row.to_dict()
                    measurements["latitude"] = latitude
                    measurements["longitude"] = longitude
                    # Converter idx para string de data
                    if isinstance(idx, Timestamp):
                        date_str_val = str(idx.date())
                    else:
                        date_str_val = str(idx)[:10]
                    measurements["date"] = date_str_val
                    if elevation:
                        measurements["elevation_m"] = elevation

                    # Debug: verificar chaves disponíveis antes do mapeamento
                    # self.logger.warning(
                    #     f"Medições disponíveis para {idx}: "
                    #     f"{list(measurements.keys())}"
                    # )

                    # ✅ CORREÇÃO: Mapear nomes de colunas da fusão
                    # para nomes esperados por calculate_et0
                    column_mapping = {
                        "temperature_2m_max": "T2M_MAX",
                        "temperature_2m_min": "T2M_MIN",
                        "temperature_2m_mean": "T2M_MEAN",
                        "relative_humidity_2m_mean": "RH2M",
                        "wind_speed_2m_mean": "WS2M",
                        "shortwave_radiation_sum": "ALLSKY_SFC_SW_DWN",
                        "precipitation_sum": "PRECTOTCORR",
                        # NASA POWER usa "T2M" para média, calculate_et0 espera "T2M_MEAN"
                        "T2M": "T2M_MEAN",
                    }

                    # Aplicar mapeamento
                    for old_col, new_col in column_mapping.items():
                        if old_col in measurements:
                            measurements[new_col] = measurements.pop(old_col)

                    # Debug: verificar após mapeamento
                    # self.logger.warning(
                    #     f"Após mapeamento para {idx}: "
                    #     f"{list(measurements.keys())}"
                    # )

                    # Debug: verificar se elevation_m está presente
                    if "elevation_m" not in measurements:
                        self.logger.warning(
                            f"Elevation ausente para {idx}, tentando obter..."
                        )
                        if elevation:
                            measurements["elevation_m"] = elevation
                        else:
                            self.logger.error(
                                f"Elevation não disponível para {idx}"
                            )
                            continue

                    et0_result = self.et0_calc.calculate_et0(measurements)

                    et0_series.append(
                        {
                            "date": measurements["date"],
                            "et0_mm_day": et0_result["et0_mm_day"],
                            "quality": et0_result["quality"],
                            "anomaly": {
                                "is_anomaly": False,
                                "z_score": 0.0,
                            },  # Placeholder - será implementado com histórico
                        }
                    )

                except Exception as e:
                    self.logger.warning(
                        f"Erro no cálculo ETo para {idx}: {str(e)}"
                    )
                    continue

            if not et0_series:
                raise ValueError("Falha no cálculo de ETo para todos os dias.")

            # 6. Calcular métrica de irrigação
            irrigation_mm = round(
                sum(d["et0_mm_day"] for d in et0_series) * 1.1, 1
            )

            # 7. Criar resultado no formato esperado pelo endpoint
            result = {
                "location": {"lat": latitude, "lon": longitude},
                "period": {"start": start_date, "end": end_date},
                "et0_series": et0_series,
                "summary": {
                    "total_days": len(et0_series),
                    "et0_total_mm": round(
                        sum(d["et0_mm_day"] for d in et0_series), 1
                    ),
                    "et0_mean_mm_day": round(
                        sum(d["et0_mm_day"] for d in et0_series)
                        / len(et0_series),
                        2,
                    ),
                    "et0_max_mm_day": round(
                        max(d["et0_mm_day"] for d in et0_series), 2
                    ),
                    "et0_min_mm_day": round(
                        min(d["et0_mm_day"] for d in et0_series), 2
                    ),
                    "anomaly_count": 0,
                },
                "recomendations": [
                    (
                        f"💧 Irrigação estimada: {irrigation_mm} "
                        f"mm para o período"
                    ),
                    (
                        f"✅ Cálculo baseado em {len(sources)} "
                        f"fontes fusionadas"
                    ),
                ],
            }

            # 7. Adicionar metadados da fusão
            source_manager = ClimateSourceManager()
            fusion_weights = source_manager.get_fusion_weights(
                sources, latitude, longitude
            )

            # 8. Formatar resposta final
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            return {
                "task_id": f"configurable_{timestamp}",
                "status": "completed",
                "data": result,
                "sources_used": sources,
                "fusion_metadata": {
                    "strategy": "configurable_fusion",
                    "sources_selected": sources,
                    "sources_available": len(
                        source_manager.get_available_sources_for_location(
                            latitude, longitude
                        )
                    ),
                    "fusion_weights": fusion_weights,
                    "quality_score": min(1.0, len(sources) / 7.0),
                    "last_updated": datetime.now().isoformat(),
                },
                "message": (
                    f"Cálculo de ETo concluído. "
                    f"Usadas {len(sources)} fontes: "
                    f"{', '.join(sources)}."
                ),
            }

        except Exception as e:
            self.logger.error(
                f"Erro no processamento com múltiplas fontes: {str(e)}"
            )
            raise
