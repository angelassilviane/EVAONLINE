"""
Kalman Ensemble Filter com duas estratégias:
1. Kalman Adaptado (com dados históricos) - mais preciso
2. Kalman Simples (sem histórico) - robusto para novas localidades

"""

import json
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

warnings.filterwarnings("ignore")


@dataclass
class KalmanState:
    """Estado interno do filtro Kalman"""

    posterior_estimate: float = 0.0
    posterior_error_estimate: float = 1.0
    process_variance: float = 1e-4
    measurement_variance: float = 0.1
    history: List[float] = field(default_factory=list)
    timestamps: List[datetime] = field(default_factory=list)


class SimpleKalmanFilter:
    """
    Filtro Kalman Simples
    - Não depende de dados históricos
    - Perfeito para novas localidades
    - Lida bem com dados incompletos
    - Auto-ajustável com o tempo
    """

    def __init__(
        self,
        process_variance: float = 1e-4,
        measurement_variance: float = 0.1,
        initial_value: float = 0.0,
    ):
        """
        Args:
            process_variance: Variância do processo (quanta incerteza há no modelo)
            measurement_variance: Variância da medição (quanta confiança na medição)
            initial_value: Estimativa inicial
        """
        if process_variance <= 0:
            raise ValueError("process_variance must be positive")
        if measurement_variance <= 0:
            raise ValueError("measurement_variance must be positive")

        self.state = KalmanState(
            posterior_estimate=initial_value,
            posterior_error_estimate=1.0,
            process_variance=process_variance,
            measurement_variance=measurement_variance,
        )

    def update(
        self, measurement: float, timestamp: Optional[datetime] = None
    ) -> float:
        """
        Atualiza o filtro com uma nova medição

        Etapas:
        1. Predição (a priori)
        2. Atualização (correção)

        Args:
            measurement: Valor medido (pode ser NaN)
            timestamp: Timestamp opcional da medição
        """
        if not isinstance(
            measurement, (int, float, type(None))
        ) and not pd.isna(measurement):
            raise TypeError("measurement must be a number or NaN")

        current_time = timestamp or datetime.now()

        if pd.isna(measurement):
            # Mesmo com NaN, adicionar ao histórico para consistência temporal
            self.state.history.append(self.state.posterior_estimate)
            self.state.timestamps.append(current_time)
            return self.state.posterior_estimate

        # Predição: usar estimativa anterior
        priori_estimate = self.state.posterior_estimate
        priori_error_estimate = (
            self.state.posterior_error_estimate + self.state.process_variance
        )

        # Atualização (Correção): combinar predição com medição
        kalman_gain = priori_error_estimate / (
            priori_error_estimate + self.state.measurement_variance
        )

        self.state.posterior_estimate = priori_estimate + kalman_gain * (
            measurement - priori_estimate
        )

        self.state.posterior_error_estimate = (
            1 - kalman_gain
        ) * priori_error_estimate

        # Guardar histórico
        self.state.history.append(self.state.posterior_estimate)
        self.state.timestamps.append(current_time)

        return self.state.posterior_estimate

    def get_state(self) -> Dict[str, float]:
        """Retorna o estado atual do filtro"""
        return {
            "estimate": self.state.posterior_estimate,
            "error_estimate": self.state.posterior_error_estimate,
            "history_length": len(self.state.history),
        }


class AdaptiveKalmanFilter:
    """
    Filtro Kalman Adaptado
    - Usa dados históricos (normais climáticas)
    - Mais preciso que Kalman simples
    - Varianças ajustadas com base no histórico
    """

    def __init__(
        self,
        monthly_normal: Optional[Dict[str, float]] = None,
        historical_std: Optional[float] = None,
        station_confidence: float = 0.8,
    ):
        """
        Args:
            monthly_normal: Valor normal do mês (de histórico)
            historical_std: Desvio padrão histórico do mês
            station_confidence: Confiança na estação (0-1)
        """
        if station_confidence < 0 or station_confidence > 1:
            raise ValueError("station_confidence must be between 0 and 1")
        if historical_std is not None and historical_std <= 0:
            raise ValueError("historical_std must be positive")

        self.monthly_normal = monthly_normal or 0.0
        self.historical_std = historical_std or 1.0
        self.station_confidence = station_confidence

        # Varianças adaptadas ao histórico
        # Maior confiança = menor measurement_variance
        process_var = (historical_std**2) * 0.01  # 1% do histórico
        measurement_var = (historical_std**2) * (1 - station_confidence)

        self.state = KalmanState(
            posterior_estimate=self.monthly_normal,
            posterior_error_estimate=historical_std,
            process_variance=max(process_var, 1e-5),
            measurement_variance=max(measurement_var, 0.01),
        )
        logger.debug(
            f"AdaptiveKalmanFilter initialized: "
            f"normal={self.monthly_normal}, "
            f"std={self.historical_std:.2f}, "
            f"confidence={station_confidence:.2f}"
        )

    def update(
        self,
        measurement: float,
        weight: float = 1.0,
        timestamp: Optional[datetime] = None,
    ) -> float:
        """
        Atualiza com medição ponderada

        Args:
            measurement: Valor medido (pode ser NaN)
            weight: Peso da medição (para múltiplas estações)
            timestamp: Timestamp opcional da medição
        """
        if not isinstance(
            measurement, (int, float, type(None))
        ) and not pd.isna(measurement):
            raise TypeError("measurement must be a number or NaN")
        if weight <= 0:
            raise ValueError("weight must be positive")

        current_time = timestamp or datetime.now()

        if pd.isna(measurement):
            # Mesmo com NaN, adicionar ao histórico para consistência temporal
            self.state.history.append(self.state.posterior_estimate)
            self.state.timestamps.append(current_time)
            return self.state.posterior_estimate

        # Predição
        priori_estimate = self.state.posterior_estimate
        priori_error_estimate = (
            self.state.posterior_error_estimate + self.state.process_variance
        )

        # Ganho de Kalman (adaptativo)
        kalman_gain = priori_error_estimate / (
            priori_error_estimate + self.state.measurement_variance / weight
        )

        # Atualização
        self.state.posterior_estimate = priori_estimate + kalman_gain * (
            measurement - priori_estimate
        )

        self.state.posterior_error_estimate = (
            1 - kalman_gain
        ) * priori_error_estimate

        self.state.history.append(self.state.posterior_estimate)
        self.state.timestamps.append(current_time)

        return self.state.posterior_estimate

    def get_state(self) -> Dict[str, Any]:
        """Retorna estado com intervalo de confiança"""
        return {
            "estimate": self.state.posterior_estimate,
            "error_estimate": self.state.posterior_error_estimate,
            "confidence_interval_95": (
                self.state.posterior_estimate
                - 1.96 * self.state.posterior_error_estimate,
                self.state.posterior_estimate
                + 1.96 * self.state.posterior_error_estimate,
            ),
            "history_length": len(self.state.history),
            "monthly_normal": self.monthly_normal,
        }


class ClimateKalmanFusion:
    """
    Orquestrador de fusão Kalman
    - Decide entre Kalman Simples ou Adaptado
    - Gerencia múltiplas variáveis climáticas
    - Funde dados de múltiplas estações
    """

    def __init__(self):
        self.filters: Dict[str, Any] = {}
        self.fusion_strategy: str = "unknown"  # simple ou adaptive

    def fuse_simple(
        self,
        current_measurements: Dict[str, float],
        station_confidence: float = 0.8,
    ) -> Dict[str, Any]:
        """
        Fusão Kalman Simples (sem histórico)

        Ideal para:
        - Novas localidades
        - Dados incompletos
        - Estações remotas
        """
        self.fusion_strategy = "simple"
        # Copiar todas as variáveis originais
        fused_data = current_measurements.copy()

        for variable, value in current_measurements.items():
            if value is None or pd.isna(value):
                fused_data[f"{variable}_quality"] = "missing"
                continue

            # Pular variáveis não numéricas
            if not isinstance(value, (int, float)):
                continue

            # Criar filtro se não existir
            if variable not in self.filters:
                measurement_variance = 0.5 - (
                    station_confidence * 0.4
                )  # 0.1 a 0.5
                self.filters[variable] = SimpleKalmanFilter(
                    measurement_variance=measurement_variance
                )
                logger.debug(
                    f"SimpleKalmanFilter created for {variable} "
                    f"(confidence={station_confidence})"
                )

            # Aplicar filtro
            filtered_value = self.filters[variable].update(value)
            fused_data[variable] = filtered_value
            fused_data[f"{variable}_raw"] = value
            fused_data[f"{variable}_quality"] = "simple_kalman"

        return fused_data

    def fuse_adaptive(
        self,
        current_measurements: Dict[str, float],
        monthly_normals: Dict[str, float],
        historical_stds: Dict[str, float],
        station_weights: Optional[Dict[str, float]] = None,
        station_confidence: float = 0.8,
    ) -> Dict[str, Any]:
        """
        Fusão Kalman Adaptada (com histórico)

        Ideal para:
        - Localidades com histórico climático
        - Maior precisão requerida
        - Análise de anomalias
        """
        self.fusion_strategy = "adaptive"
        # Copiar todas as variáveis originais
        fused_data = current_measurements.copy()

        if station_weights is None:
            station_weights = dict.fromkeys(current_measurements.keys(), 1.0)

        for variable, value in current_measurements.items():
            if value is None or pd.isna(value):
                fused_data[f"{variable}_quality"] = "missing"
                continue

            # Pular variáveis não numéricas
            if not isinstance(value, (int, float)):
                continue

            normal = monthly_normals.get(variable, 0.0)
            std = historical_stds.get(variable, 1.0)

            # Criar filtro se não existir
            if variable not in self.filters:
                self.filters[variable] = AdaptiveKalmanFilter(
                    monthly_normal=normal,
                    historical_std=std,
                    station_confidence=station_confidence,
                )
                logger.debug(
                    f"AdaptiveKalmanFilter created for {variable} "
                    f"(normal={normal:.2f}, std={std:.2f})"
                )

            # Aplicar filtro com peso
            weight = station_weights.get(variable, 1.0)
            filtered_value = self.filters[variable].update(
                value, weight=weight
            )

            fused_data[variable] = filtered_value
            fused_data[f"{variable}_raw"] = value
            fused_data[f"{variable}_anomaly"] = filtered_value - normal
            fused_data[f"{variable}_quality"] = "adaptive_kalman"

        return fused_data

    def fuse_multiple_stations(
        self,
        stations_data: List[Dict[str, float]],
        distance_weights: Optional[List[float]] = None,
        has_historical_data: bool = False,
        monthly_normals: Optional[Dict[str, float]] = None,
        historical_stds: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Funde dados de múltiplas estações

        Args:
            stations_data: Lista de dicts com dados de cada estação
            distance_weights: Pesos inversamente proporcionais à distância
            has_historical_data: Se usar Kalman Adaptado
            monthly_normals: Normais mensais para Kalman Adaptado
            historical_stds: Desvios padrão para Kalman Adaptado
        """
        if not stations_data:
            logger.warning("No station data provided")
            return {}

        # Calcular médias iniciais
        initial_estimates = {}
        weighted_estimates = {}
        n_stations = len(stations_data)

        if distance_weights is None:
            distance_weights = [1.0 / n_stations] * n_stations

        # Normalizar pesos
        total_weight = sum(distance_weights)
        distance_weights = [w / total_weight for w in distance_weights]

        logger.debug(
            f"Fusing {n_stations} stations with weights: {distance_weights}"
        )

        for station_idx, station in enumerate(stations_data):
            weight = distance_weights[station_idx]

            for key, value in station.items():
                if not isinstance(value, (int, float)) or pd.isna(value):
                    continue

                if key not in initial_estimates:
                    initial_estimates[key] = []
                    weighted_estimates[key] = 0.0

                initial_estimates[key].append(value)
                weighted_estimates[key] += value * weight

        # Aplicar Kalman à fusão
        fused_result = {}

        if has_historical_data and monthly_normals:
            for variable in weighted_estimates:
                if pd.isna(weighted_estimates[variable]):
                    continue

                if variable not in self.filters:
                    normal = monthly_normals.get(variable, 0.0)
                    std = (
                        historical_stds.get(variable, 1.0)
                        if historical_stds
                        else 1.0
                    )
                    self.filters[variable] = AdaptiveKalmanFilter(
                        monthly_normal=normal,
                        historical_std=std,
                        station_confidence=min(n_stations * 0.3, 0.95),
                    )

                fused_value = self.filters[variable].update(
                    weighted_estimates[variable]
                )
                fused_result[variable] = fused_value
        else:
            for variable in weighted_estimates:
                if variable not in self.filters:
                    self.filters[variable] = SimpleKalmanFilter()

                fused_value = self.filters[variable].update(
                    weighted_estimates[variable]
                )
                fused_result[variable] = fused_value

        return fused_result

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Retorna estado de todos os filtros"""
        return {
            var: filter_obj.get_state()
            for var, filter_obj in self.filters.items()
        }

    def reset(self, variable: Optional[str] = None):
        """Reset de um filtro ou de todos"""
        if variable:
            if variable in self.filters:
                del self.filters[variable]
        else:
            self.filters.clear()
        logger.info(
            f"Kalman filters reset: {'all' if variable is None else variable}"
        )


class KalmanEnsembleStrategy:
    """
    Estratégia inteligente de fusão:
    - Se tem histórico na DB → Kalman Adaptado
    - Se não tem → Kalman Simples
    """

    def __init__(self, db_session=None, redis_client=None):
        self.db_session = db_session
        self.redis = redis_client
        self.fusion = ClimateKalmanFusion()

        # Importar aqui para evitar circular imports
        if db_session:
            from backend.core.data_processing.station_finder import (
                StationFinder,
            )

            self.station_finder = StationFinder(db_session)
        else:
            self.station_finder = None

        logger.info("KalmanEnsembleStrategy initialized")

    async def auto_fuse(
        self,
        latitude: float,
        longitude: float,
        current_measurements: Dict[str, float],
        stations_data: Optional[List[Dict[str, float]]] = None,
        distance_weights: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        """
        Estratégia automática de fusão

        1. Verifica se há histórico da localidade na DB (27 cidades estudadas)
        2. Se sim → Kalman Adaptado (com histórico)
        3. Se não → Kalman Simples (robusto para novas localidades)

        Args:
            latitude: Latitude do ponto de interesse
            longitude: Longitude do ponto de interesse
            current_measurements: Dict com dados das 4 APIs
            stations_data: Opcional, para múltiplas estações
            distance_weights: Pesos das estações

        Returns:
            Dict com dados fusionados e qualidade
        """
        logger.info(f"Auto fusion for ({latitude:.4f}, {longitude:.4f})")

        # 1️⃣ Buscar histórico do PostgreSQL
        has_history, monthly_normals, historical_stds = (
            await self._get_historical_data(latitude, longitude)
        )

        # 2️⃣ Decidir estratégia
        if has_history and monthly_normals:
            logger.info("✅ Using Adaptive Kalman (with historical data)")

            # Determinar mês atual ou usar média anual
            current_month = datetime.now().month
            month_normals = monthly_normals.get(current_month, {})
            month_stds = historical_stds.get(current_month, {})

            return self.fusion.fuse_adaptive(
                current_measurements=current_measurements,
                monthly_normals=month_normals,
                historical_stds=month_stds,
                station_confidence=0.85,  # Próximo aos 27 estudos
            )
        else:
            logger.info("⚠️ Using Simple Kalman (no historical data)")

            if stations_data:
                return self.fusion.fuse_multiple_stations(
                    stations_data=stations_data,
                    distance_weights=distance_weights,
                    has_historical_data=False,
                )
            else:
                return self.fusion.fuse_simple(current_measurements)

    async def _get_historical_data(
        self, latitude: float, longitude: float
    ) -> Tuple[bool, Dict[int, Dict[str, float]], Dict[int, Dict[str, float]]]:
        """
        Busca dados históricos do PostgreSQL com cache Redis.

        Estratégia:
        1. Verificar Redis: key = f"climate_history:{lat:.2f}:{lon:.2f}"
        2. Se hit (24h TTL): retornar
        3. Se miss:
           a) Query ao PostgreSQL via StationFinder
           b) Buscar cidade estudada próxima (max 10km)
           c) Extrair monthly_normals e historical_stds
           d) Cachear em Redis
           e) Retornar

        Returns:
            Tuple[has_history, monthly_normals, historical_stds]
            onde:
            - monthly_normals: {1: {eto_normal: 5.2, precip_normal: 120}, ...}
            - historical_stds: {1: {eto_std: 1.2, precip_std: 45}, ...}
        """
        if not self.station_finder and not self.redis:
            logger.debug("No DB/Redis session, returning empty history")
            return False, {}, {}

        # 1️⃣ Tentar Redis cache (24h TTL)
        cache_key = f"climate_history:{latitude:.2f}:{longitude:.2f}"

        if self.redis:
            try:
                cached = self.redis.get(cache_key)
                if cached:
                    logger.debug(f"✅ Redis HIT: {cache_key}")
                    data = json.loads(cached)
                    return (
                        data.get("has_history"),
                        data.get("monthly_normals", {}),
                        data.get("historical_stds", {}),
                    )
            except Exception as e:
                logger.warning(f"Redis cache error (continuando): {e}")

        # 2️⃣ Query ao PostgreSQL
        if not self.station_finder:
            logger.warning("No StationFinder available for DB query")
            return False, {}, {}

        try:
            logger.debug(
                f"🔍 Buscando histórico no PostgreSQL: ({latitude:.4f}, {longitude:.4f})"
            )

            city_data = await self.station_finder.find_studied_city(
                target_lat=latitude,
                target_lon=longitude,
                max_distance_km=10.0,
            )

            if not city_data:
                logger.info(
                    f"No studied city found within 10km of ({latitude:.4f}, {longitude:.4f})"
                )
                return False, {}, {}

            logger.info(
                f"✅ Found studied city: {city_data.get('city_name')} "
                f"({city_data.get('distance_km', 'N/A'):.1f}km away)"
            )

            # 3️⃣ Extrair monthly_normals e historical_stds
            monthly_normals = self._extract_monthly_normals(city_data)
            historical_stds = self._extract_historical_stds(city_data)

            if not monthly_normals or not historical_stds:
                logger.warning(
                    f"Incomplete historical data for {city_data.get('city_name')}"
                )
                return False, {}, {}

            # 4️⃣ Cachear em Redis (24h)
            if self.redis:
                try:
                    cache_data = {
                        "has_history": True,
                        "monthly_normals": monthly_normals,
                        "historical_stds": historical_stds,
                        "city_name": city_data.get("city_name"),
                        "distance_km": city_data.get("distance_km"),
                        "timestamp": datetime.now().isoformat(),
                    }
                    self.redis.setex(
                        cache_key, 86400, json.dumps(cache_data)
                    )  # 24 horas
                    logger.debug(f"💾 Cached: {cache_key}")
                except Exception as e:
                    logger.warning(f"Failed to cache in Redis: {e}")

            return True, monthly_normals, historical_stds

        except Exception as e:
            logger.error(f"❌ Error getting historical data: {e}")
            return False, {}, {}

    def _extract_monthly_normals(
        self, city_data: Dict
    ) -> Dict[int, Dict[str, float]]:
        """
        Extrai normais mensais do city_data.

        Returns:
            {1: {eto_normal: 5.2, precip_normal: 120}, ...}
        """
        monthly_normals = {}

        try:
            # Esperado em city_data: monthly_data = {1: {...}, 2: {...}, ...}
            monthly_data = city_data.get("monthly_data", {})

            for month, data in monthly_data.items():
                if not isinstance(data, dict):
                    continue
                month_int = int(month) if isinstance(month, str) else month

                monthly_normals[month_int] = {
                    "eto_normal": data.get("eto_normal"),
                    "eto_daily_mean": data.get("eto_daily_mean"),
                    "eto_daily_std": data.get("eto_daily_std"),
                    "precip_normal": data.get("precip_normal"),
                    "precip_daily_mean": data.get("precip_daily_mean"),
                    "precip_daily_std": data.get("precip_daily_std"),
                    "rain_probability": data.get("rain_probability"),
                }

        except Exception as e:
            logger.warning(f"Error extracting monthly normals: {e}")

        return monthly_normals

    def _extract_historical_stds(
        self, city_data: Dict
    ) -> Dict[int, Dict[str, float]]:
        """
        Extrai desvios padrão históricos do city_data.

        Returns:
            {1: {eto_std: 1.2, precip_std: 45}, ...}
        """
        historical_stds = {}

        try:
            monthly_data = city_data.get("monthly_data", {})

            for month, data in monthly_data.items():
                if not isinstance(data, dict):
                    continue
                month_int = int(month) if isinstance(month, str) else month

                historical_stds[month_int] = {
                    "eto_std": data.get("eto_daily_std", 0.1),
                    "precip_std": data.get("precip_daily_std", 1.0),
                    "wind_std": data.get("wind_speed_std", 0.5),
                    "humidity_std": data.get("humidity_std", 5.0),
                }

        except Exception as e:
            logger.warning(f"Error extracting historical stds: {e}")

        return historical_stds

    def auto_fuse_sync(
        self,
        latitude: float,
        longitude: float,
        current_measurements: Dict[str, float],
        stations_data: Optional[List[Dict[str, float]]] = None,
        distance_weights: Optional[List[float]] = None,
    ) -> Dict[str, Any]:
        """
        Wrapper síncrono para auto_fuse() - compatível com código síncrono.

        Detecta se já há event loop rodando e usa abordagem apropriada.

        Args:
            latitude: Latitude do ponto de interesse
            longitude: Longitude do ponto de interesse
            current_measurements: Dict com dados das 4 APIs
            stations_data: Opcional, para múltiplas estações
            distance_weights: Pesos das estações

        Returns:
            Dict com dados fusionados e qualidade

        Example:
            >>> kalman = KalmanEnsembleStrategy(db_session, redis_client)
            >>> result = kalman.auto_fuse_sync(
            ...     latitude=-15.7939,
            ...     longitude=-47.8828,
            ...     current_measurements={'temperature_max': 28.5, ...}
            ... )
        """
        import asyncio

        try:
            # Verificar se já há event loop rodando
            asyncio.get_running_loop()
            # Se há loop rodando, usar ThreadPoolExecutor
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    asyncio.run,
                    self.auto_fuse(
                        latitude,
                        longitude,
                        current_measurements,
                        stations_data,
                        distance_weights,
                    ),
                )
                return future.result()

        except RuntimeError:
            # Não há event loop rodando, usar asyncio.run normalmente
            return asyncio.run(
                self.auto_fuse(
                    latitude,
                    longitude,
                    current_measurements,
                    stations_data,
                    distance_weights,
                )
            )
