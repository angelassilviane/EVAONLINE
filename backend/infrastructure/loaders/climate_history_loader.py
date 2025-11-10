"""
Climate History Data Loader
- Importa dados históricos dos CSVs/JSONs para PostgreSQL
- Processa normais climáticas mensais
- Calcula pesos de proximidade para estações
"""

import json
import warnings
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

warnings.filterwarnings("ignore")


class ClimateHistoryLoader:
    """Carregador de dados históricos climáticos"""

    def __init__(
        self,
        db_session: Session,
        reports_dir: str = "data/historical/cities",
    ):
        self.db_session = db_session
        self.reports_dir = Path(reports_dir)
        logger.info(f"ClimateHistoryLoader initialized with {reports_dir}")

    def load_city_from_json(self, json_path: Path) -> dict[str, Any] | None:
        """
        Carrega dados de uma cidade a partir de JSON

        Estrutura esperada:
        {
            "city": "Piracicaba_SP",
            "reference_period": ["1991-01-01", "2020-12-31"],
            "reference_period_key": "1991-2020",
            "latitude": -22.7191,
            "longitude": -47.6383,
            "elevation_m": 546,
            "climate_normals_all_periods": {...}
        }
        """
        try:
            with open(json_path, encoding="utf-8") as f:
                data = json.load(f)

            return data

        except Exception as e:
            logger.error(f"Error loading JSON {json_path}: {e}")
            return None

    def parse_city_info(
        self, json_data: dict
    ) -> tuple[str, str, str, float, float, float | None]:
        """
        Extrai informações básicas da cidade

        Returns: city_name, country, state, lat, lon, elevation
        """
        city_full = json_data.get("city", "Unknown")

        # Parsear city_full (formato: "CityName_StateCountry" ou "CityName_Country")
        parts = city_full.split("_")

        if len(parts) >= 2:
            city_name = parts[0]
            state_province = parts[1] if len(parts) == 2 else parts[1]
            country = parts[-1] if len(parts) > 2 else "Unknown"
        else:
            city_name = city_full
            state_province = None
            country = "Unknown"

        # Tentar extrair coords do JSON
        latitude = json_data.get("latitude")
        longitude = json_data.get("longitude")
        elevation = json_data.get("elevation_m")

        if latitude is None or longitude is None:
            logger.warning(f"Missing coordinates for {city_full}")
            return city_name, country, state_province, 0.0, 0.0, None

        return (
            city_name,
            country,
            state_province,
            latitude,
            longitude,
            elevation,
        )

    def insert_studied_city(
        self,
        json_data: dict,
    ) -> int | None:
        """
        Insere cidade estudada no BD

        Returns: city_id se sucesso, None se falha
        """
        try:
            city_name, country, state, lat, lon, elevation = (
                self.parse_city_info(json_data)
            )

            # Verificar se já existe
            existing = self.db_session.execute(
                text(
                    """
                SELECT id FROM climate_history.studied_cities
                WHERE city_name = :city_name AND country = :country
                LIMIT 1
                """
                ),
                {"city_name": city_name, "country": country},
            ).first()

            if existing:
                logger.info(f"City {city_name}, {country} already exists")
                return existing[0]

            # Referência de período
            json_data.get("reference_period", [])
            reference_key = json_data.get("reference_period_key", "unknown")

            # Inserir cidade com PostGIS
            insert_stmt = text(
                """
            INSERT INTO climate_history.studied_cities
            (city_name, country, state_province, latitude, longitude, elevation_m,
             location, reference_periods, created_at, updated_at)
            VALUES
            (:city_name, :country, :state, :lat, :lon, :elevation,
             ST_Point(:lon, :lat), :reference_periods::jsonb,
             now(), now())
            RETURNING id
            """
            )

            result = self.db_session.execute(
                insert_stmt,
                {
                    "city_name": city_name,
                    "country": country,
                    "state": state,
                    "lat": lat,
                    "lon": lon,
                    "elevation": elevation,
                    "reference_periods": f'{{"periods": ["{reference_key}"]}}',
                },
            ).scalar()
            self.db_session.commit()

            logger.info(f"Inserted city {city_name} (ID: {result})")
            return result

        except Exception as e:
            logger.error(f"Error inserting city: {e}")
            self.db_session.rollback()
            return None

    def insert_monthly_normals(
        self,
        city_id: int,
        json_data: dict,
    ) -> int:
        """
        Insere normais mensais para uma cidade

        Returns: número de registros inseridos
        """
        try:
            normals_data = json_data.get("climate_normals_all_periods", {})
            period_key = json_data.get("reference_period_key", "unknown")

            inserted_count = 0

            for _period_str, periods_data in normals_data.items():
                monthly_data = periods_data.get("monthly", {})

                for month_str, month_data in monthly_data.items():
                    try:
                        month = int(month_str)

                        # Preparar dados do mês
                        normal_data = {
                            "city_id": city_id,
                            "period_key": period_key,
                            "month": month,
                            "eto_normal": month_data.get("normal"),
                            "eto_daily_mean": month_data.get("daily_mean"),
                            "eto_daily_median": month_data.get("daily_median"),
                            "eto_daily_std": month_data.get("daily_std"),
                            "eto_p01": month_data.get("p01"),
                            "eto_p05": month_data.get("p05"),
                            "eto_p10": month_data.get("p10"),
                            "eto_p25": month_data.get("p25"),
                            "eto_p75": month_data.get("p75"),
                            "eto_p90": month_data.get("p90"),
                            "eto_p95": month_data.get("p95"),
                            "eto_p99": month_data.get("p99"),
                            "eto_abs_min": month_data.get("abs_min"),
                            "eto_abs_max": month_data.get("abs_max"),
                            "precip_normal": month_data.get("precip_normal"),
                            "precip_daily_mean": month_data.get(
                                "precip_daily_mean"
                            ),
                            "precip_daily_median": month_data.get(
                                "precip_daily_median"
                            ),
                            "precip_daily_std": month_data.get(
                                "precip_daily_std"
                            ),
                            "precip_p95": month_data.get("precip_p95"),
                            "precip_p99": month_data.get("precip_p99"),
                            "precip_max": month_data.get("precip_max"),
                            "rain_days": month_data.get("rain_days"),
                            "dry_days": month_data.get("dry_days"),
                            "rain_probability": month_data.get(
                                "rain_probability"
                            ),
                            "precip_intensity": month_data.get(
                                "precip_intensity"
                            ),
                            "n_days": month_data.get("n_days"),
                        }

                        # Verificar se já existe
                        check_query = text(
                            """
                        SELECT id FROM climate_history.monthly_climate_normals
                        WHERE city_id = :city_id AND period_key = :period_key
                        AND month = :month
                        LIMIT 1
                        """
                        )
                        existing = self.db_session.execute(
                            check_query,
                            {
                                "city_id": city_id,
                                "period_key": period_key,
                                "month": month,
                            },
                        ).first()

                        if not existing:
                            # Inserir registro de forma segura
                            # Usar SQLAlchemy insert para evitar SQL injection
                            from sqlalchemy import MetaData, Table

                            # Criar tabela metadata dinamicamente
                            metadata = MetaData()
                            monthly_normals = Table(
                                "monthly_climate_normals",
                                metadata,
                                schema="climate_history",
                                autoload_with=self.db_session.bind,
                            )

                            # Preparar dados com timestamps
                            insert_data = normal_data.copy()
                            insert_data["created_at"] = text("now()")
                            insert_data["updated_at"] = text("now()")

                            insert_stmt = monthly_normals.insert().values(
                                **insert_data
                            )
                            self.db_session.execute(insert_stmt)
                            inserted_count += 1

                    except Exception as e:
                        logger.warning(
                            f"Error processing month {month_str}: {e}"
                        )
                        continue

            self.db_session.commit()
            logger.info(
                f"Inserted {inserted_count} monthly normals for city {city_id}"
            )
            return inserted_count

        except Exception as e:
            logger.error(f"Error inserting monthly normals: {e}")
            self.db_session.rollback()
            return 0

    def load_all_cities(self) -> int:
        """
        Carrega todas as cidades do diretório reports/cities

        Returns: número de cidades carregadas
        """
        json_files = list(self.reports_dir.glob("report_*.json"))
        logger.info(f"Found {len(json_files)} JSON reports to load")

        loaded_count = 0

        for json_file in json_files:
            try:
                # Carregar JSON
                json_data = self.load_city_from_json(json_file)
                if not json_data:
                    continue

                # Inserir cidade
                city_id = self.insert_studied_city(json_data)
                if not city_id:
                    continue

                # Inserir normais mensais
                self.insert_monthly_normals(city_id, json_data)

                loaded_count += 1
                logger.info(f"✓ Loaded {json_file.name}")

            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}")
                continue

        logger.info(f"\n✓ Successfully loaded {loaded_count} cities")
        return loaded_count


# Script para executar no terminal
if __name__ == "__main__":
    """
    Uso:
    python -m backend.infrastructure.loaders.climate_history_loader
    """
    from backend.database.connection import get_db_context

    def main():
        with get_db_context() as session:
            loader = ClimateHistoryLoader(session)
            loader.load_all_cities()

    main()
