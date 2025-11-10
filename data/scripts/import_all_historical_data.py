"""
Script COMPLETO para importar TODOS os dados históricos do diretório data/historical/.

Importa:
1. Todas as 27 cidades (com coordenadas dos CSVs)
2. Normais mensais de todos os períodos
3. Estatísticas completas
4. Análise de extremos

Usage:
    uv run python data/scripts/import_all_historical_data.py
"""

import csv
import io
import json
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy import text

# Fix encoding para Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Adicionar raiz do projeto ao path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.database.connection import get_db_context


def parse_city_name(city_name: str) -> tuple[str, Optional[str], str]:
    """Extrai cidade, estado e país do nome no CSV."""
    parts = city_name.split("_")

    # Cidades brasileiras: Cidade_UF
    if len(parts) >= 2 and len(parts[-1]) == 2 and parts[-1].isupper():
        city = "_".join(parts[:-1])
        state = parts[-1]
        return city, state, "Brasil"

    # Cidades internacionais: Cidade_País
    if len(parts) >= 2:
        country = parts[-1]
        city = " ".join(parts[:-1])
        return city, None, country

    return city_name, None, "Unknown"


def load_cities_from_summary(csv_path: Path) -> dict:
    """Carrega dados completos das cidades do cities_summary.csv."""
    cities = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_name, state, country = parse_city_name(row["city"])

            # Tratar valores faltantes ou inválidos
            try:
                lat = (
                    float(row["lat"])
                    if row.get("lat") and row["lat"] != ""
                    else None
                )
                lon = (
                    float(row["lon"])
                    if row.get("lon") and row["lon"] != ""
                    else None
                )
                alt = (
                    float(row["alt"])
                    if row.get("alt") and row["alt"] != ""
                    else None
                )
                # Tentar converter total_records
                try:
                    total_records = int(row["total_records"])
                except (ValueError, KeyError):
                    print(
                        f"   ⚠️  Erro ao ler total_records para {row.get('city', 'UNKNOWN')}: '{row.get('total_records')}'"
                    )
                    total_records = 0

                variables = []
                if row.get("variables"):
                    variables = json.loads(row["variables"].replace("'", '"'))

                cities[row["city"]] = {
                    "city_name": city_name,
                    "state_province": state,
                    "country": country,
                    "latitude": lat if lat and lat != 0.0 else None,
                    "longitude": lon if lon and lon != 0.0 else None,
                    "elevation_m": alt if alt and alt != 0.0 else None,
                    "total_records": total_records,
                    "data_period": row.get("data_period", ""),
                    "variables": variables,
                    "completeness": (
                        float(row["completeness"])
                        if row.get("completeness")
                        else 0.0
                    ),
                    "eto_mean": (
                        float(row["eto_mean"]) if row.get("eto_mean") else None
                    ),
                    "eto_std": (
                        float(row["eto_std"]) if row.get("eto_std") else None
                    ),
                    "eto_max": (
                        float(row["eto_max"]) if row.get("eto_max") else None
                    ),
                    "eto_min": (
                        float(row["eto_min"]) if row.get("eto_min") else None
                    ),
                    "eto_p99": (
                        float(row["eto_p99"]) if row.get("eto_p99") else None
                    ),
                    "eto_p01": (
                        float(row["eto_p01"]) if row.get("eto_p01") else None
                    ),
                }
            except (ValueError, KeyError) as e:
                city = row.get("city", "unknown")
                print(f"   ⚠️  Erro ao processar {city}: {e}")
                continue
    return cities


def load_extremes_data(csv_path: Path) -> dict:
    """Carrega análise de extremos."""
    extremes = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            extremes[row["city"]] = {
                "eto_high_extremes_count": int(row["eto_high_extremes_count"]),
                "eto_low_extremes_count": int(row["eto_low_extremes_count"]),
                "eto_extreme_frequency": float(row["eto_extreme_frequency"]),
                "precip_extremes_count": int(row["precip_extremes_count"]),
                "precip_extreme_frequency": float(
                    row["precip_extreme_frequency"]
                ),  # noqa: E501
                "precip_max_value": float(row["precip_max_value"]),
                "precip_dry_spell_max": int(row["precip_dry_spell_max"]),
                "precip_wet_spell_max": int(row["precip_wet_spell_max"]),
            }
    return extremes


def load_annual_normals(csv_path: Path) -> dict:
    """Carrega normais anuais de todos os períodos."""
    normals = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["city"]
            period = row["period"]

            if city not in normals:
                normals[city] = {}

            normals[city][period] = {
                "eto_annual": float(row["eto_normal_mm_day"]),
                "precip_annual": float(row["precip_normal_mm_year"]),
                "valid_years": int(row["valid_years"]),
                "completeness": float(row["completeness"]),
            }
    return normals


def load_monthly_normals_from_json(json_path: Path) -> dict:
    """Extrai normais mensais de um JSON."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    monthly_normals = {}

    if "climate_normals_all_periods" not in data:
        return {}

    periods = data["climate_normals_all_periods"]

    # Processar cada período
    for period_key, period_data in periods.items():
        if "monthly" not in period_data:
            continue

        monthly_normals[period_key] = []

        for month_str, month_data in period_data["monthly"].items():
            monthly_normals[period_key].append(
                {
                    "month": int(month_str),
                    "eto_mm_day": month_data.get("normal"),
                    "precipitation_mm": month_data.get("precip_normal"),
                    "rain_days": month_data.get("rain_days"),
                    "temp_mean": None,  # Não disponível nos JSONs
                }
            )

    return monthly_normals


def insert_or_update_city(
    conn, city_key: str, city_data: dict
) -> Optional[int]:  # noqa: E501
    """Insere ou atualiza cidade."""
    # Se não tem coordenadas, pular
    if not city_data["latitude"] or not city_data["longitude"]:
        print(f"   ⚠️  {city_key}: sem coordenadas, pulando")
        return None

    query = text(
        """
        INSERT INTO climate_history.studied_cities
        (city_name, state, country, latitude, longitude, elevation, location)
        VALUES
        (:city, :state, :country, :lat, :lon, :elev,
         ST_SetSRID(ST_MakePoint(:lon2, :lat2), 4326))
        ON CONFLICT (city_name, latitude, longitude)
        DO UPDATE SET
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            elevation = EXCLUDED.elevation
        RETURNING id
    """
    )

    params = {
        "city": city_data["city_name"],
        "state": city_data["state_province"],
        "country": city_data["country"],
        "lat": city_data["latitude"],
        "lon": city_data["longitude"],
        "elev": city_data["elevation_m"],
        "lon2": city_data["longitude"],
        "lat2": city_data["latitude"],
        "tz": None,
        "sources": json.dumps(["historical_eto_reports", "csv_summary"]),
        "periods": json.dumps(
            {"1961-1990": True, "1981-2010": True, "1991-2020": True}
        ),  # noqa: E501
    }

    try:
        result = conn.execute(query, params)
        city_id_row = result.fetchone()
        city_id = city_id_row[0] if city_id_row else None
        print(f"   ✅ {city_key}: ID={city_id}")
        return city_id
    except Exception as e:
        print(f"   ❌ {city_key}: {e}")
        return None


def insert_statistics(
    conn, city_id: int, city_data: dict, extremes_data: dict
):  # noqa: E501
    """Insere estatísticas da cidade."""
    query = text(
        """
        INSERT INTO climate_history.city_statistics
        (city_id, total_records, data_period, variables, completeness,
         eto_mean, eto_std, eto_max, eto_min, eto_p99, eto_p01,
         eto_high_extremes_count, eto_low_extremes_count, eto_extreme_frequency,
         precip_extremes_count, precip_extreme_frequency, precip_max_value,
         precip_dry_spell_max, precip_wet_spell_max)
        VALUES
        (:city_id, :total_records, :data_period, :variables, :completeness,
         :eto_mean, :eto_std, :eto_max, :eto_min, :eto_p99, :eto_p01,
         :eto_high_ext, :eto_low_ext, :eto_ext_freq,
         :precip_ext, :precip_ext_freq, :precip_max,
         :dry_spell, :wet_spell)
        ON CONFLICT (city_id) DO UPDATE SET
            total_records = EXCLUDED.total_records,
            eto_mean = EXCLUDED.eto_mean,
            eto_std = EXCLUDED.eto_std
    """
    )

    params = {
        "city_id": city_id,
        "total_records": city_data["total_records"],
        "data_period": city_data["data_period"],
        "variables": city_data["variables"],
        "completeness": city_data["completeness"],
        "eto_mean": city_data["eto_mean"],
        "eto_std": city_data["eto_std"],
        "eto_max": city_data["eto_max"],
        "eto_min": city_data["eto_min"],
        "eto_p99": city_data["eto_p99"],
        "eto_p01": city_data["eto_p01"],
        "eto_high_ext": extremes_data.get("eto_high_extremes_count"),
        "eto_low_ext": extremes_data.get("eto_low_extremes_count"),
        "eto_ext_freq": extremes_data.get("eto_extreme_frequency"),
        "precip_ext": extremes_data.get("precip_extremes_count"),
        "precip_ext_freq": extremes_data.get("precip_extreme_frequency"),
        "precip_max": extremes_data.get("precip_max_value"),
        "dry_spell": extremes_data.get("precip_dry_spell_max"),
        "wet_spell": extremes_data.get("precip_wet_spell_max"),
    }

    conn.execute(query, params)


def insert_monthly_normals(
    conn, city_id: int, period_key: str, normals: list
):  # noqa: E501
    """Insere normais mensais."""
    query = text(
        """
        INSERT INTO climate_history.monthly_climate_normals
        (city_id, period_key, month, eto_mm_day, precipitation_mm)
        VALUES
        (:city_id, :period, :month, :eto, :precip)
        ON CONFLICT (city_id, period_key, month)
        DO UPDATE SET
            eto_mm_day = EXCLUDED.eto_mm_day,
            precipitation_mm = EXCLUDED.precipitation_mm
    """
    )

    for normal in normals:
        params = {
            "city_id": city_id,
            "period": period_key,
            "month": normal["month"],
            "eto": normal.get("eto_mm_day"),
            "precip": normal.get("precipitation_mm"),
        }
        conn.execute(query, params)


def main():
    """Função principal."""
    print("\n" + "=" * 80)
    print("🌍 IMPORTAÇÃO COMPLETA DE DADOS HISTÓRICOS")
    print("=" * 80)

    # Caminhos dos arquivos
    base_path = project_root / "data" / "historical"
    summary_path = base_path / "summary" / "cities_summary.csv"
    extremes_path = base_path / "summary" / "extremes_analysis.csv"
    annual_path = base_path / "summary" / "annual_normals_comparison.csv"
    reports_dir = base_path / "cities"

    # Carregar dados dos CSVs
    print("\n📂 Carregando CSVs...")
    cities_data = load_cities_from_summary(summary_path)
    print(f"   ✅ {len(cities_data)} cidades em cities_summary.csv")

    extremes_data = load_extremes_data(extremes_path)
    print(f"   ✅ {len(extremes_data)} cidades em extremes_analysis.csv")

    annual_normals = load_annual_normals(annual_path)
    print(
        f"   ✅ {len(annual_normals)} cidades em annual_normals_comparison.csv"
    )  # noqa: E501

    with get_db_context() as conn:
        cities_imported = 0
        cities_skipped = 0
        normals_imported = 0

        print("\n💾 Processando cidades...")

        for city_key, city_data in cities_data.items():
            print(f"\n📍 {city_key}")

            # 1. Inserir/atualizar cidade
            city_id = insert_or_update_city(conn, city_key, city_data)

            if not city_id:
                cities_skipped += 1
                continue

            cities_imported += 1

            # 2. Inserir estatísticas
            extremes = extremes_data.get(city_key, {})
            insert_statistics(conn, city_id, city_data, extremes)
            print("      ✅ Estatísticas inseridas")

            # 3. Inserir normais mensais dos JSONs (se existir)
            json_filename = f"report_{city_key}.json"
            json_path = reports_dir / json_filename

            if json_path.exists():
                monthly_data = load_monthly_normals_from_json(json_path)

                for period_key, normals in monthly_data.items():
                    insert_monthly_normals(conn, city_id, period_key, normals)
                    normals_imported += len(normals)

                print(
                    f"      ✅ Normais mensais: {len(monthly_data)} períodos"
                )  # noqa: E501
            else:
                print(f"      ⚠️  JSON não encontrado: {json_filename}")

            # Commit incremental
            conn.commit()

        # Validação final
        print("\n" + "=" * 80)
        print("🔍 VALIDAÇÃO FINAL")
        print("=" * 80)

        result = conn.execute(
            text("SELECT COUNT(*) FROM climate_history.studied_cities")
        )
        cities_count_row = result.fetchone()
        cities_count = cities_count_row[0] if cities_count_row else 0
        print(f"  📍 Total de cidades: {cities_count}")

        result = conn.execute(
            text("SELECT COUNT(*) FROM climate_history.city_statistics")
        )
        stats_count_row = result.fetchone()
        stats_count = stats_count_row[0] if stats_count_row else 0
        print(f"  📊 Total de estatísticas: {stats_count}")

        result = conn.execute(
            text(
                """
            SELECT period_key, COUNT(DISTINCT city_id) as cities,
                   COUNT(*) as records
            FROM climate_history.monthly_climate_normals
            GROUP BY period_key
            ORDER BY period_key
        """
            )
        )
        print("\n  📅 Normais mensais por período:")
        for row in result:
            print(f"     - {row[0]}: {row[1]} cidades, {row[2]} registros")

    print("\n" + "=" * 80)
    print("📊 RESUMO FINAL")
    print("=" * 80)
    print(f"  ✅ Cidades importadas: {cities_imported}")
    print(f"  ⚠️  Cidades sem coordenadas: {cities_skipped}")
    print(f"  ✅ Normais mensais: {normals_imported}")
    print("\n" + "=" * 80)
    print("✅ IMPORTAÇÃO CONCLUÍDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
