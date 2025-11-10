"""
Complete climate infrastructure with 6 APIs support.

Revision ID: 001_climate_6apis
Revises:
Create Date: 2025-11-08

Esta migração CONSOLIDA toda a infraestrutura com suporte às 6 APIs:

CLIMATE APIs (6 fontes) - LIMITES PADRONIZADOS EVA:
1. nasa_power - NASA POWER (histórico: 1990-01-01 → hoje-2d)
2. openmeteo_archive - Open-Meteo Archive (histórico: 1990-01-01 → hoje-2d)
3. openmeteo_forecast - Open-Meteo Forecast (previsão: hoje-30d → hoje+5d)
4. met_norway - MET Norway (previsão: hoje → hoje+5d)
5. nws_forecast - NWS Forecast (previsão USA: hoje → hoje+5d)
6. nws_stations - NWS Stations (realtime USA: hoje-1d → hoje)

Tabelas criadas:
- Schema climate_history
- climate_data (dados multi-API com JSONB, 15 colunas)
- api_variables (metadados das APIs com source_api)
- Tabelas do climate_history (studied_cities, monthly_climate_normals, etc.)
- Tabelas administrativas (admin_users, cache, visitor_stats, eto_results)

ATENÇÃO: Esta migration também carrega os dados históricos de referência
do diretório data/historical/ automaticamente.

Inclui todos os ajustes das migrations 004, 005, 006 e 007 consolidados.
"""

import csv
import json
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geography
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "001_climate_6apis"
down_revision = None
branch_labels = None
depends_on = None


def _load_historical_data():
    """Carrega dados históricos de referência do diretório data/historical/."""
    print("\n📚 Carregando dados históricos de referência...")

    # Localizar diretório de dados
    migration_dir = Path(__file__).parent.parent.parent
    data_dir = migration_dir / "data" / "historical"

    if not data_dir.exists():
        print(
            "⚠️  Diretório data/historical/ não encontrado. Pulando carga de dados históricos."
        )
        return

    conn = op.get_bind()

    # Carregar CSVs de sumário
    summary_dir = data_dir / "summary"
    cities_csv = summary_dir / "cities_summary.csv"
    annual_csv = summary_dir / "annual_normals_comparison.csv"

    if not cities_csv.exists() or not annual_csv.exists():
        print("⚠️  CSVs de sumário não encontrados. Pulando carga.")
        return

    # 1. Carregar dados de cidades
    cities_data = {}
    with open(cities_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_key = row["city"]
            cities_data[city_key] = {
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "elevation": float(row["alt"]),
                "total_records": int(row["total_records"]),
                "data_period": row["data_period"],
                "variables": row["variables"],
                "completeness": float(row["completeness"]),
                "eto_mean": float(row["eto_mean"]),
                "eto_std": float(row["eto_std"]),
                "eto_max": float(row["eto_max"]),
                "eto_min": float(row["eto_min"]),
                "eto_p99": float(row["eto_p99"]),
                "eto_p01": float(row["eto_p01"]),
            }

    # 2. Carregar normais mensais
    normals_data = {}
    with open(annual_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_key = row["city"]
            period = row["period"]
            if city_key not in normals_data:
                normals_data[city_key] = {}
            if period not in normals_data[city_key]:
                normals_data[city_key][period] = {
                    "eto_normal": float(row["eto_normal_mm_day"]),
                    "precip_normal": float(row["precip_normal_mm_year"]),
                }

    # 3. Inserir cidades e dados
    cities_inserted = 0
    for city_key, city_info in cities_data.items():
        # Parse nome da cidade
        parts = city_key.split("_")

        # Lista de estados brasileiros (UF) para diferenciar de outros países
        brazilian_states = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        }

        # Mapeamento de cidades internacionais para estado/província
        international_cities = {
            "Addis_Ababa_Ethiopia": ("Addis Ababa", "Capital", "Ethiopia"),
            "Des_Moines_IA": ("Des Moines", "Iowa", "USA"),
            "Fresno_CA": ("Fresno", "California", "USA"),
            "Hanoi_Vietnam": ("Hanoi", "Hanoi", "Vietnam"),
            "Krasnodar_Russia": ("Krasnodar", "Krasnodar Krai", "Russia"),
            "Ludhiana_Punjab": ("Ludhiana", "Punjab", "India"),
            "Mendoza_Argentina": ("Mendoza", "Mendoza", "Argentina"),
            "Polokwane_Limpopo": ("Polokwane", "Limpopo", "South Africa"),
            "Seville_Spain": ("Seville", "Andalusia", "Spain"),
            "Wagga_Wagga_Australia": (
                "Wagga Wagga",
                "New South Wales",
                "Australia",
            ),
        }

        # Verifica se é cidade internacional (lookup direto)
        if city_key in international_cities:
            city_name, state, country = international_cities[city_key]

        # Verifica formato: Cidade_UF (brasileiro)
        elif len(parts) >= 2 and len(parts[-1]) == 2 and parts[-1].isupper():
            state = parts[-1]
            city_name = (
                "_".join(parts[:-1])
                if len(parts) == 2
                else " ".join(parts[:-1])
            )

            # Determinar país pelo estado
            if state in brazilian_states:
                country = "Brasil"
            else:
                # Fallback genérico
                country = "Unknown"
                state = None

        else:
            city_name = city_key
            state = None
            country = "Unknown"

        # Inserir cidade
        result = conn.execute(
            sa.text(
                """
                INSERT INTO climate_history.studied_cities
                (city_name, state, country, latitude, longitude, elevation,
                 location, data_sources, reference_periods)
                VALUES
                (:city, :state, :country, :lat, :lon, :elev,
                 ST_SetSRID(ST_MakePoint(:lon2, :lat2), 4326),
                 cast(:sources as jsonb), cast(:periods as jsonb))
                ON CONFLICT (city_name, latitude, longitude) DO NOTHING
                RETURNING id
            """
            ),
            {
                "city": city_name,
                "state": state,
                "country": country,
                "lat": city_info["lat"],
                "lon": city_info["lon"],
                "elev": city_info["elevation"],
                "lon2": city_info["lon"],
                "lat2": city_info["lat"],
                "sources": json.dumps(
                    ["historical_eto_reports", "csv_summary"]
                ),
                "periods": json.dumps(
                    {
                        period: True
                        for period in normals_data.get(city_key, {}).keys()
                    }
                ),
            },
        )
        row = result.fetchone()
        if row:
            city_id = row[0]
            cities_inserted += 1

            # Inserir estatísticas
            conn.execute(
                sa.text(
                    """
                    INSERT INTO climate_history.city_statistics
                    (city_id, total_records, data_period, variables, completeness,
                     eto_mean, eto_std, eto_max, eto_min, eto_p99, eto_p01)
                    VALUES
                    (:city_id, :total_records, :data_period, :variables, :completeness,
                     :eto_mean, :eto_std, :eto_max, :eto_min, :eto_p99, :eto_p01)
                    ON CONFLICT (city_id) DO NOTHING
                """
                ),
                {
                    "city_id": city_id,
                    "total_records": city_info["total_records"],
                    "data_period": city_info["data_period"],
                    "variables": city_info["variables"],
                    "completeness": city_info["completeness"],
                    "eto_mean": city_info["eto_mean"],
                    "eto_std": city_info["eto_std"],
                    "eto_max": city_info["eto_max"],
                    "eto_min": city_info["eto_min"],
                    "eto_p99": city_info["eto_p99"],
                    "eto_p01": city_info["eto_p01"],
                },
            )

            # Inserir normais mensais (simplificadas - apenas anuais)
            if city_key in normals_data:
                for period, period_data in normals_data[city_key].items():
                    # Distribuir ETo anual pelos meses (aproximação)
                    eto_daily = period_data["eto_normal"]
                    precip_monthly = period_data["precip_normal"] / 12

                    for month in range(1, 13):
                        conn.execute(
                            sa.text(
                                """
                                INSERT INTO climate_history.monthly_climate_normals
                                (city_id, period_key, month, eto_mm_day, precipitation_mm)
                                VALUES
                                (:city_id, :period, :month, :eto, :precip)
                                ON CONFLICT (city_id, period_key, month) DO NOTHING
                            """
                            ),
                            {
                                "city_id": city_id,
                                "period": period,
                                "month": month,
                                "eto": eto_daily,
                                "precip": precip_monthly,
                            },
                        )

    print(f"✅ {cities_inserted} cidades carregadas com dados históricos")


def upgrade() -> None:
    """Cria toda a infraestrutura do zero com suporte às 6 APIs."""

    print("\n" + "=" * 80)
    print("🚀 CRIANDO INFRAESTRUTURA COMPLETA - 6 APIs DE CLIMA")
    print("=" * 80)

    # ========================================
    # 1. CRIAR SCHEMA CLIMATE_HISTORY
    # ========================================
    print("\n📁 Criando schema climate_history...")
    op.execute("CREATE SCHEMA IF NOT EXISTS climate_history")
    print("✅ Schema criado")

    # ========================================
    # 2. TABELAS ADMINISTRATIVAS (PUBLIC)
    # ========================================
    print("\n👤 Criando tabelas administrativas...")

    # Tabela de administradores
    op.create_table(
        "admin_users",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("username", sa.String(100), nullable=False, unique=True),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean, default=True, nullable=False),
        sa.Column("is_superuser", sa.Boolean, default=False, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("last_login", sa.DateTime, nullable=True),
    )

    # Tabela de cache de usuário
    op.create_table(
        "user_cache",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("cache_key", sa.String(255), nullable=False),
        sa.Column("cache_data", postgresql.JSONB, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime, nullable=True),
    )
    op.create_index(
        "idx_user_cache_user_key", "user_cache", ["user_id", "cache_key"]
    )
    op.create_index("idx_user_cache_expires", "user_cache", ["expires_at"])

    # Tabela de favoritos
    op.create_table(
        "user_favorites",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("city_name", sa.String(255), nullable=False),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("idx_user_favorites_user", "user_favorites", ["user_id"])

    # Tabela de estatísticas de visitantes
    op.create_table(
        "visitor_stats",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("session_id", sa.String(255), nullable=False),
        sa.Column("ip_address", sa.String(50), nullable=True),
        sa.Column("user_agent", sa.String(500), nullable=True),
        sa.Column("page_visited", sa.String(255), nullable=False),
        sa.Column(
            "timestamp",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_visitor_stats_session", "visitor_stats", ["session_id"]
    )
    op.create_index(
        "idx_visitor_stats_timestamp", "visitor_stats", ["timestamp"]
    )

    print("✅ Tabelas administrativas criadas")

    # ========================================
    # 3. TABELA PRINCIPAL CLIMATE_DATA (15 COLUNAS)
    # ========================================
    print("\n🌡️  Criando tabela climate_data (6 APIs, 15 colunas)...")

    op.create_table(
        "climate_data",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "source_api",
            sa.String(50),
            nullable=False,
            comment=(
                "Fonte dos dados: nasa_power, openmeteo_archive, "
                "openmeteo_forecast, met_norway, nws_forecast, nws_stations"
            ),
        ),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
        sa.Column(
            "elevation",
            sa.Float,
            nullable=True,
            comment="Elevação em metros (crucial para ETo)",
        ),
        sa.Column(
            "timezone",
            sa.String(50),
            nullable=True,
            comment="Timezone IANA (ex: America/Sao_Paulo)",
        ),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column(
            "raw_data",
            postgresql.JSONB,
            nullable=False,
            comment="Dados brutos da API original",
        ),
        sa.Column(
            "harmonized_data",
            postgresql.JSONB,
            nullable=True,
            comment="Dados normalizados para formato padrão",
        ),
        sa.Column(
            "eto_mm_day",
            sa.Float,
            nullable=True,
            comment="ETo calculado em mm/dia",
        ),
        sa.Column(
            "eto_method",
            sa.String(50),
            nullable=True,
            comment="Método: penman_monteith, hargreaves, etc.",
        ),
        sa.Column(
            "quality_flags",
            postgresql.JSONB,
            nullable=True,
            comment="Flags de qualidade dos dados",
        ),
        sa.Column(
            "processing_metadata",
            postgresql.JSONB,
            nullable=True,
            comment="Metadados sobre o processamento",
        ),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "source_api",
            "latitude",
            "longitude",
            "date",
            name="uq_climate_data_location_date",
        ),
    )

    # Índices para climate_data
    op.create_index("idx_climate_data_source", "climate_data", ["source_api"])
    op.create_index(
        "idx_climate_data_location", "climate_data", ["latitude", "longitude"]
    )
    op.create_index("idx_climate_data_date", "climate_data", ["date"])
    op.create_index(
        "idx_climate_data_source_date", "climate_data", ["source_api", "date"]
    )

    print("✅ climate_data criada - 15 colunas, 6 APIs suportadas")

    # ========================================
    # 4. TABELA API_VARIABLES (METADADOS)
    # ========================================
    print("\n📋 Criando tabela api_variables...")

    op.create_table(
        "api_variables",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "source_api",
            sa.String(50),
            nullable=False,
            comment="Nome da API fonte",
        ),
        sa.Column(
            "variable_name",
            sa.String(100),
            nullable=False,
            comment="Nome da variável na API",
        ),
        sa.Column(
            "standard_name",
            sa.String(100),
            nullable=True,
            comment="Nome padronizado CF ou interno",
        ),
        sa.Column(
            "description",
            sa.Text,
            nullable=True,
            comment="Descrição da variável",
        ),
        sa.Column(
            "unit",
            sa.String(50),
            nullable=True,
            comment="Unidade de medida",
        ),
        sa.Column(
            "temporal_resolution",
            sa.String(50),
            nullable=True,
            comment="Resolução temporal (hourly, daily, etc.)",
        ),
        sa.Column(
            "is_required_for_eto",
            sa.Boolean,
            nullable=False,
            default=False,
            comment="Se é necessária para cálculo de ETo",
        ),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source_api",
            "variable_name",
            name="uq_api_variables_source_var",
        ),
    )

    # Índices para api_variables
    op.create_index(
        "idx_api_variables_source", "api_variables", ["source_api"]
    )
    op.create_index(
        "idx_api_variables_standard", "api_variables", ["standard_name"]
    )
    op.create_index(
        "idx_api_variables_eto",
        "api_variables",
        ["source_api", "is_required_for_eto"],
    )

    print("✅ api_variables criada")

    # ========================================
    # 5. TABELAS DO CLIMATE_HISTORY
    # ========================================
    print("\n🏛️  Criando tabelas climate_history...")

    # Studied Cities - Cidades com dados históricos climáticos
    op.create_table(
        "studied_cities",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("city_name", sa.String(255), nullable=False),
        sa.Column(
            "state", sa.String(100), nullable=True
        ),  # Opcional para cidades globais
        sa.Column("country", sa.String(100), nullable=False),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
        sa.Column("elevation", sa.Float, nullable=True),
        sa.Column(
            "location",
            Geography(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column("timezone", sa.String(100), nullable=True),
        sa.Column(
            "data_sources",
            postgresql.JSONB,
            nullable=True,
            comment="Fontes de dados disponíveis para esta cidade",
        ),
        sa.Column(
            "reference_periods",
            postgresql.JSONB,
            nullable=True,
            comment="Períodos de referência disponíveis (1961-1990, etc.)",
        ),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "city_name", "latitude", "longitude", name="uq_city_location"
        ),
        schema="climate_history",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_studied_cities_location "
        "ON climate_history.studied_cities USING gist (location)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_studied_cities_name "
        "ON climate_history.studied_cities (city_name)"
    )

    # Monthly Climate Normals
    op.create_table(
        "monthly_climate_normals",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "city_id",
            sa.Integer,
            sa.ForeignKey("climate_history.studied_cities.id"),
            nullable=False,
        ),
        sa.Column(
            "period_key",
            sa.String(20),
            nullable=False,
            comment="Período de referência (1961-1990, 1981-2010, 1991-2020)",
        ),
        sa.Column("month", sa.Integer, nullable=False),
        sa.Column(
            "eto_mm_day",
            sa.Float,
            nullable=True,
            comment="ETo médio diário em mm/dia",
        ),
        sa.Column("temp_max_avg", sa.Float, nullable=True),
        sa.Column("temp_min_avg", sa.Float, nullable=True),
        sa.Column("precipitation_mm", sa.Float, nullable=True),
        sa.Column("humidity_percent", sa.Float, nullable=True),
        sa.Column("wind_speed_ms", sa.Float, nullable=True),
        sa.Column("solar_radiation_mjm2", sa.Float, nullable=True),
        sa.Column(
            "eto_mm_month",
            sa.Float,
            nullable=True,
            comment="ETo total mensal em mm/mês",
        ),
        sa.Column("data_source", sa.String(100), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "city_id", "period_key", "month", name="uq_city_period_month"
        ),
        schema="climate_history",
    )
    op.create_index(
        "idx_monthly_normals_city",
        "monthly_climate_normals",
        ["city_id"],
        schema="climate_history",
    )
    op.create_index(
        "idx_monthly_normals_period",
        "monthly_climate_normals",
        ["period_key"],
        schema="climate_history",
    )

    # City Statistics - Estatísticas históricas por cidade
    op.create_table(
        "city_statistics",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "city_id",
            sa.Integer,
            sa.ForeignKey("climate_history.studied_cities.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("total_records", sa.Integer, nullable=True),
        sa.Column("data_period", sa.String(50), nullable=True),
        sa.Column("variables", sa.String(255), nullable=True),
        sa.Column("completeness", sa.Float, nullable=True),
        # Estatísticas de ETo
        sa.Column("eto_mean", sa.Float, nullable=True),
        sa.Column("eto_std", sa.Float, nullable=True),
        sa.Column("eto_max", sa.Float, nullable=True),
        sa.Column("eto_min", sa.Float, nullable=True),
        sa.Column("eto_p99", sa.Float, nullable=True),
        sa.Column("eto_p01", sa.Float, nullable=True),
        # Contadores de extremos
        sa.Column("eto_high_extremes_count", sa.Integer, nullable=True),
        sa.Column("eto_low_extremes_count", sa.Integer, nullable=True),
        sa.Column("eto_extreme_frequency", sa.Float, nullable=True),
        sa.Column("precip_extremes_count", sa.Integer, nullable=True),
        sa.Column("precip_extreme_frequency", sa.Float, nullable=True),
        sa.Column("precip_max_value", sa.Float, nullable=True),
        sa.Column("precip_dry_spell_max", sa.Integer, nullable=True),
        sa.Column("precip_wet_spell_max", sa.Integer, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        schema="climate_history",
    )
    op.create_index(
        "idx_city_statistics_city",
        "city_statistics",
        ["city_id"],
        schema="climate_history",
    )

    # Weather Stations
    op.create_table(
        "weather_stations",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("station_code", sa.String(50), nullable=False, unique=True),
        sa.Column("station_name", sa.String(255), nullable=False),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
        sa.Column("elevation", sa.Float, nullable=True),
        sa.Column(
            "location",
            Geography(geometry_type="POINT", srid=4326),
            nullable=True,
        ),
        sa.Column("station_type", sa.String(50), nullable=True),
        sa.Column("data_provider", sa.String(100), nullable=True),
        sa.Column("is_active", sa.Boolean, default=True, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema="climate_history",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_weather_stations_location "
        "ON climate_history.weather_stations USING gist (location)"
    )

    # City Nearby Stations (junction table)
    op.create_table(
        "city_nearby_stations",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "city_id",
            sa.Integer,
            sa.ForeignKey("climate_history.studied_cities.id"),
            nullable=False,
        ),
        sa.Column(
            "station_id",
            sa.Integer,
            sa.ForeignKey("climate_history.weather_stations.id"),
            nullable=False,
        ),
        sa.Column("distance_km", sa.Float, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "city_id",
            "station_id",
            name="uq_city_station",
        ),
        schema="climate_history",
    )
    op.create_index(
        "idx_city_nearby_city",
        "city_nearby_stations",
        ["city_id"],
        schema="climate_history",
    )
    op.create_index(
        "idx_city_nearby_station",
        "city_nearby_stations",
        ["station_id"],
        schema="climate_history",
    )

    print("✅ Tabelas climate_history criadas")

    # ========================================
    # 6. TABELA ETO_RESULTS (PUBLIC)
    # ========================================
    print("\n💧 Criando tabela eto_results...")

    op.create_table(
        "eto_results",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("latitude", sa.Float, nullable=False),
        sa.Column("longitude", sa.Float, nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("eto_mm_day", sa.Float, nullable=False),
        sa.Column("method", sa.String(50), nullable=False),
        sa.Column("source_api", sa.String(50), nullable=False),
        sa.Column("input_data", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_eto_results_location", "eto_results", ["latitude", "longitude"]
    )
    op.create_index("idx_eto_results_date", "eto_results", ["date"])
    op.create_index("idx_eto_results_source", "eto_results", ["source_api"])

    print("✅ eto_results criada")

    # ========================================
    # 7. CARREGAR DADOS HISTÓRICOS
    # ========================================
    _load_historical_data()

    print("\n" + "=" * 80)
    print("✅ INFRAESTRUTURA COMPLETA CRIADA COM SUCESSO!")
    print("=" * 80)
    print("\n📊 Resumo:")
    print("   • Schema: climate_history")
    print("   • Tabela principal: climate_data (15 colunas)")
    print("   • APIs suportadas: 6 (nasa_power, openmeteo_archive,")
    print("     openmeteo_forecast, met_norway, nws_forecast, nws_stations)")
    print("   • Tabelas climate_history: 5")
    print("   • Tabelas administrativas: 4")
    print("   • Total de tabelas: 11")
    print("\n🎉 Sistema pronto para receber dados das 6 APIs!")
    print("=" * 80 + "\n")


def downgrade() -> None:
    """Remove toda a infraestrutura."""

    print("\n❌ REMOVENDO TODA A INFRAESTRUTURA...")

    # Drop tables in reverse order (respecting foreign keys)
    op.drop_table("eto_results")
    op.drop_table("city_statistics", schema="climate_history")
    op.drop_table("city_nearby_stations", schema="climate_history")
    op.drop_table("weather_stations", schema="climate_history")
    op.drop_table("monthly_climate_normals", schema="climate_history")
    op.drop_table("studied_cities", schema="climate_history")
    op.drop_table("api_variables")
    op.drop_table("climate_data")
    op.drop_table("visitor_stats")
    op.drop_table("user_favorites")
    op.drop_table("user_cache")
    op.drop_table("admin_users")

    # Drop schema
    op.execute("DROP SCHEMA IF EXISTS climate_history CASCADE")

    print("✅ Todas as tabelas removidas")
