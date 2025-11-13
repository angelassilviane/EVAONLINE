"""
Station Finder com PostGIS
- Busca por raio usando índices espaciais
- Cálculo de pesos por distância
- Integração com dados históricos
"""

from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session


class StationFinder:
    """
    Localizador de estações com suporte a PostGIS

    Métodos:
    - find_stations_in_radius: Busca por raio (usando índice espacial)
    - get_weighted_climate_data: Dados ponderados por distância
    - find_studied_city: Busca cidade com histórico na DB
    """

    def __init__(self, db_session: Optional[Session] = None):
        self.db_session = db_session
        logger.info("StationFinder initialized with PostGIS support")

    async def find_stations_in_radius(
        self,
        target_lat: float,
        target_lon: float,
        radius_km: float = 50,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Encontra estações dentro de um raio usando PostGIS

        Query otimizada com índice GIST:
        - Usa ST_DWithin para busca rápida
        - Ordena por distância
        - Retorna metadados completos

        Args:
            target_lat: Latitude do alvo
            target_lon: Longitude do alvo
            radius_km: Raio de busca em km
            limit: Máximo de estações

        Returns:
            Lista de estações ordenadas por distância
        """
        if not self.db_session:
            logger.warning("No DB session provided, using fallback")
            return []

        try:
            query = text(
                """
            SELECT
                id,
                station_code,
                station_name,
                latitude,
                longitude,
                elevation_m,
                country,
                data_source,
                data_available_from,
                data_available_to,
                variables_available,
                ST_Distance(
                    location::geography,
                    ST_Point(:lon, :lat)::geography
                ) / 1000.0 AS distance_km,
                ST_Distance(
                    location::geography,
                    ST_Point(:lon, :lat)::geography
                ) / 1000.0 / :radius_km AS proximity_weight
            FROM climate_history.weather_stations
            WHERE ST_DWithin(
                location::geography,
                ST_Point(:lon, :lat)::geography,
                :radius_m
            )
            ORDER BY distance_km ASC
            LIMIT :limit
            """
            )

            result = self.db_session.execute(
                query,
                {
                    "lat": target_lat,
                    "lon": target_lon,
                    "radius_m": radius_km * 1000,  # Converter para metros
                    "radius_km": radius_km,
                    "limit": limit,
                },
            )

            stations = []
            for row in result:
                station = {
                    "id": row[0],
                    "station_code": row[1],
                    "station_name": row[2],
                    "latitude": row[3],
                    "longitude": row[4],
                    "elevation_m": row[5],
                    "country": row[6],
                    "data_source": row[7],
                    "data_available_from": row[8],
                    "data_available_to": row[9],
                    "variables_available": row[10],
                    "distance_km": row[11],
                    "proximity_weight": row[12],
                }
                stations.append(station)

            logger.info(
                f"Found {len(stations)} stations within {radius_km}km "
                f"of ({target_lat}, {target_lon})"
            )
            return stations

        except Exception as e:
            logger.error(f"Error finding stations: {e}")
            return []

    async def find_studied_city(
        self,
        target_lat: float,
        target_lon: float,
        max_distance_km: float = 10,
    ) -> Optional[Dict[str, Any]]:
        """
        Busca se há uma cidade estudada próxima à coordenada.

        Retorna dados históricos (normais mensais) se encontrar, caso contrário None

        Args:
            target_lat: Latitude do alvo
            target_lon: Longitude do alvo
            max_distance_km: Distância máxima para considerar "próxima"

        Returns:
            Dict com dados da cidade e seus normais mensais, ou None
            Formato:
            {
                "id": 1,
                "city_name": "Piracicaba",
                "distance_km": 2.5,
                "monthly_data": {
                    1: {
                        "eto_normal": 5.2,
                        "eto_daily_mean": 5.3,
                        "eto_daily_std": 1.2,
                        "precip_normal": 120.5,
                        "precip_daily_std": 15.3,
                        "rain_probability": 0.65,
                    },
                    ...
                }
            }
        """
        if not self.db_session:
            logger.warning("No DB session for city lookup")
            return None

        try:
            # 1 Buscar cidade próxima
            query = text(
                """
            SELECT
                id,
                city_name,
                country,
                state,
                latitude,
                longitude,
                elevation,
                timezone,
                data_sources,
                reference_periods,
                ST_Distance(
                    location::geography,
                    ST_Point(:lon, :lat)::geography
                ) / 1000.0 AS distance_km
            FROM climate_history.studied_cities
            WHERE ST_DWithin(
                location::geography,
                ST_Point(:lon, :lat)::geography,
                :max_distance_m
            )
            ORDER BY distance_km ASC
            LIMIT 1
            """
            )

            result = self.db_session.execute(
                query,
                {
                    "lat": target_lat,
                    "lon": target_lon,
                    "max_distance_m": max_distance_km * 1000,
                },
            ).first()

            if not result:
                logger.info(
                    f"No studied city found within {max_distance_km}km "
                    f"of ({target_lat}, {target_lon})"
                )
                return None

            city_id = result[0]
            city_name = result[1]
            distance_km = result[10]

            logger.info(
                f"Found studied city '{city_name}' at distance {distance_km:.2f}km"
            )

            # 2 Buscar todos os normais mensais desta cidade
            normals_query = text(
                """
            SELECT
                month,
                eto_normal,
                eto_daily_mean,
                eto_daily_std,
                eto_p95,
                eto_p99,
                precip_normal,
                precip_daily_mean,
                precip_daily_std,
                precip_p95,
                rain_probability,
                period_key
            FROM climate_history.monthly_climate_normals
            WHERE city_id = :city_id
            ORDER BY period_key DESC, month ASC
            """
            )

            normals_result = self.db_session.execute(
                normals_query, {"city_id": city_id}
            ).fetchall()

            # Agrupar por mês (usar o período mais recente)
            monthly_data = {}
            seen_months = set()

            for row in normals_result:
                month = row[0]
                if month not in seen_months:  # Usar período mais recente
                    monthly_data[month] = {
                        "eto_normal": row[1],
                        "eto_daily_mean": row[2],
                        "eto_daily_std": row[3],
                        "eto_p95": row[4],
                        "eto_p99": row[5],
                        "precip_normal": row[6],
                        "precip_daily_mean": row[7],
                        "precip_daily_std": row[8],
                        "precip_p95": row[9],
                        "rain_probability": row[10],
                        "period_key": row[11],
                    }
                    seen_months.add(month)

            # Construir resposta
            city_data = {
                "id": city_id,
                "city_name": city_name,
                "country": result[2],
                "state": result[3],
                "latitude": result[4],
                "longitude": result[5],
                "elevation_m": result[6],
                "timezone": result[7],
                "data_sources": result[8],
                "reference_periods": result[9],
                "distance_km": distance_km,
                "monthly_data": monthly_data,
            }

            logger.info(
                f" Loaded {len(monthly_data)} months of historical data for {city_name}"
            )
            return city_data

        except Exception as e:
            logger.error(f"Error finding studied city: {e}")
            return None

    async def get_monthly_normals(
        self,
        city_id: int,
        month: int,
        period_key: Optional[str] = None,
    ) -> Optional[Dict[str, float]]:
        """
        Busca normais climáticas mensais de uma cidade

        Args:
            city_id: ID da cidade
            month: Mês (1-12)
            period_key: Período específico (ex: "1991-2020"), usa último se None

        Returns:
            Dict com normais ou None se não encontrado
        """
        if not self.db_session:
            return None

        try:
            # Se período não especificado, usar o mais recente
            if period_key is None:
                query = text(
                    """
                SELECT
                    eto_normal, precip_normal, rain_probability,
                    eto_daily_std, precip_daily_std, eto_p95, precip_p95,
                    period_key
                FROM climate_history.monthly_climate_normals
                WHERE city_id = :city_id AND month = :month
                ORDER BY period_key DESC
                LIMIT 1
                """
                )
            else:
                query = text(
                    """
                SELECT
                    eto_normal, precip_normal, rain_probability,
                    eto_daily_std, precip_daily_std, eto_p95, precip_p95,
                    period_key
                FROM climate_history.monthly_climate_normals
                WHERE city_id = :city_id AND month = :month AND period_key = :period_key
                LIMIT 1
                """
                )

            result = self.db_session.execute(
                query,
                {
                    "city_id": city_id,
                    "month": month,
                    "period_key": period_key,
                },
            ).first()

            if result:
                return {
                    "eto_normal": result[0],
                    "precip_normal": result[1],
                    "rain_probability": result[2],
                    "eto_daily_std": result[3],
                    "precip_daily_std": result[4],
                    "eto_p95": result[5],
                    "precip_p95": result[6],
                    "period_key": result[7],
                }
            return None

        except Exception as e:
            logger.error(f"Error fetching monthly normals: {e}")
            return None

    async def get_weighted_climate_data(
        self,
        target_lat: float,
        target_lon: float,
        radius_km: float = 50,
        stations_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, float]:
        """
        Calcula dados climáticos ponderados pela distância

        Peso = 1 / (distância + 0.1)
        Normaliza para que soma dos pesos = 1

        Args:
            target_lat: Latitude
            target_lon: Longitude
            radius_km: Raio de busca
            stations_data: Lista de estações (se None, busca na DB)

        Returns:
            Dict com dados ponderados
        """
        if stations_data is None:
            stations_data = await self.find_stations_in_radius(
                target_lat, target_lon, radius_km
            )

        if not stations_data:
            logger.warning("No stations available for weighting")
            return {}

        weighted_data = {}
        total_weight = 0.0

        for station in stations_data:
            distance_km = station.get("distance_km", float("inf"))

            # Evitar divisão por zero
            if distance_km == 0:
                distance_km = 0.001

            # Peso inversamente proporcional à distância
            weight = 1.0 / (distance_km + 0.1)
            total_weight += weight

            # Acumular dados ponderados
            for key, value in station.items():
                if isinstance(value, (int, float)) and key not in [
                    "distance_km",
                    "proximity_weight",
                    "latitude",
                    "longitude",
                    "id",
                    "elevation_m",
                ]:
                    if key not in weighted_data:
                        weighted_data[key] = 0.0
                    weighted_data[key] += value * weight

        # Normalizar pesos
        for key in weighted_data:
            weighted_data[key] /= total_weight

        logger.info(
            f"Weighted climate data calculated from {len(stations_data)} "
            f"stations (total_weight={total_weight:.2f})"
        )
        return weighted_data

    async def get_nearby_stations_for_city(
        self,
        city_id: int,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Busca estações pré-calculadas próximas de uma cidade

        Args:
            city_id: ID da cidade estudada
            limit: Máximo de estações

        Returns:
            Lista de estações com weights pré-calculados
        """
        if not self.db_session:
            return []

        try:
            query = text(
                """
            SELECT
                ws.id,
                ws.station_code,
                ws.station_name,
                ws.latitude,
                ws.longitude,
                ws.elevation_m,
                ws.country,
                ws.data_source,
                cns.distance_km,
                cns.proximity_weight,
                cns.confidence_score,
                ws.variables_available
            FROM climate_history.city_nearby_stations cns
            JOIN climate_history.weather_stations ws ON ws.id = cns.station_id
            WHERE cns.city_id = :city_id
            ORDER BY cns.distance_km ASC
            LIMIT :limit
            """
            )

            result = self.db_session.execute(
                query, {"city_id": city_id, "limit": limit}
            )

            stations = []
            for row in result:
                stations.append(
                    {
                        "id": row[0],
                        "station_code": row[1],
                        "station_name": row[2],
                        "latitude": row[3],
                        "longitude": row[4],
                        "elevation_m": row[5],
                        "country": row[6],
                        "data_source": row[7],
                        "distance_km": row[8],
                        "proximity_weight": row[9],
                        "confidence_score": row[10],
                        "variables_available": row[11],
                    }
                )

            logger.info(
                f"Found {len(stations)} nearby stations for city {city_id}"
            )
            return stations

        except Exception as e:
            logger.error(f"Error fetching nearby stations: {e}")
            return []

    def find_studied_city_sync(
        self,
        target_lat: float,
        target_lon: float,
        max_distance_km: float = 10,
    ) -> Optional[Dict[str, Any]]:
        """
        Wrapper síncrono para find_studied_city() - compatível com código síncrono.

        Usa asyncio.run() internamente para executar coroutine.

        Args:
            target_lat: Latitude do alvo
            target_lon: Longitude do alvo
            max_distance_km: Distância máxima para considerar "próxima"

        Returns:
            Dict com dados da cidade e seus normais mensais, ou None

        Example:
            >>> finder = StationFinder(db_session)
            >>> city = finder.find_studied_city_sync(-15.7939, -47.8828)
            >>> if city:
            ...     print(f"Encontrada: {city['city_name']} a {city['distance_km']:.1f}km")
        """
        import asyncio

        try:
            # Tentar executar diretamente com asyncio.run
            return asyncio.run(
                self.find_studied_city(target_lat, target_lon, max_distance_km)
            )
        except RuntimeError as e:
            if "already running" in str(e):
                # Se já há event loop rodando, criar nova task
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.find_studied_city(
                            target_lat, target_lon, max_distance_km
                        ),
                    )
                    return future.result()
            else:
                raise

    def find_stations_in_radius_sync(
        self,
        target_lat: float,
        target_lon: float,
        radius_km: float = 50,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Wrapper síncrono para find_stations_in_radius() - compatível com código síncrono.

        Usa asyncio.run() internamente para executar coroutine.

        Args:
            target_lat: Latitude do alvo
            target_lon: Longitude do alvo
            radius_km: Raio de busca em km
            limit: Máximo de estações

        Returns:
            Lista de estações ordenadas por distância

        Example:
            >>> finder = StationFinder(db_session)
            >>> stations = finder.find_stations_in_radius_sync(-15.7939, -47.8828, 50)
            >>> print(f"Encontradas {len(stations)} estações")
        """
        import asyncio

        try:
            # Tentar executar diretamente com asyncio.run
            return asyncio.run(
                self.find_stations_in_radius(
                    target_lat, target_lon, radius_km, limit
                )
            )
        except RuntimeError as e:
            if "already running" in str(e):
                # Se já há event loop rodando, criar nova task
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.find_stations_in_radius(
                            target_lat, target_lon, radius_km, limit
                        ),
                    )
                    return future.result()
            else:
                raise
