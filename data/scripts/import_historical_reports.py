"""
Script para importar dados históricos de data/historical/cities/*.json
para as tabelas climate_history.studied_cities e monthly_climate_normals.

Usage:
    uv run python data/scripts/import_historical_reports.py
"""

# Fix encoding para Windows
import io
import csv
import json
import sys
from pathlib import Path
from typing import Optional
from sqlalchemy import text
from backend.database.connection import get_db_context

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Adicionar raiz do projeto ao path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def load_cities_database() -> dict:
    """Carrega worldcities_with_elevation.csv para lookup."""
    cities_db = {}
    csv_path = project_root / "data" / "csv" / "worldcities_with_elevation.csv"

    if not csv_path.exists():
        print(f"⚠️  Arquivo não encontrado: {csv_path}")
        return cities_db

    with open(csv_path, "r", encoding="utf-8") as f:
        # Pular a primeira linha (cabeçalho)
        next(f)
        reader = csv.DictReader(
            f,
            fieldnames=["city", "lat", "lng", "country", "iso3", "elevation"],
        )
        for row in reader:
            city_key = row["city"].lower().strip()
            cities_db[city_key] = {
                "city": row["city"],
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "country": row["country"],
                "iso3": row["iso3"],
                "elevation": (
                    float(row["elevation"]) if row["elevation"] else None
                ),
            }

    print(f"📍 Carregadas {len(cities_db)} cidades do banco de dados")
    return cities_db


def parse_city_name(filename: str) -> tuple[str, Optional[str], str]:
    """Extrai cidade, estado e país do nome do arquivo."""
    name_part = filename.replace("report_", "").replace(".json", "")
    parts = name_part.split("_")

    # Cidades brasileiras: Cidade_UF
    if len(parts) == 2 and len(parts[-1]) == 2:
        return parts[0], parts[-1], "Brasil"

    # Cidades internacionais: última parte é o país
    if len(parts) >= 2:
        country = parts[-1]
        city = " ".join(parts[:-1])
        return city, None, country

    return name_part, None, "Unknown"


def find_city_coords(
    city_name: str,
    state: Optional[str],
    country: str,
    cities_db: dict,
) -> Optional[dict]:
    """Procura coordenadas no banco de dados de cidades."""
    # Normalizar para busca
    search_key = city_name.lower().strip()

    # Busca exata
    if search_key in cities_db:
        return cities_db[search_key]

    # Remover acentos e caracteres especiais
    search_variations = [
        search_key.replace(" ", ""),
        search_key.replace("_", " "),
        search_key.replace("_", ""),
    ]

    for variation in search_variations:
        if variation in cities_db:
            return cities_db[variation]

    return None


def import_city_report(json_path: Path, cities_db: dict) -> dict:
    """Importa dados de um único relatório JSON."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    city_name, state, country = parse_city_name(json_path.name)

    # Buscar coordenadas
    coords = find_city_coords(city_name, state, country, cities_db)

    if coords:
        latitude = coords["lat"]
        longitude = coords["lng"]
        elevation = coords["elevation"]
    else:
        print(f"   ⚠️  Coordenadas não encontradas para {city_name}")
        latitude = None
        longitude = None
        elevation = None

    # Extrair normais climatológicas do período 1991-2020
    monthly_normals = []
    if "climate_normals_all_periods" in data:
        periods = data["climate_normals_all_periods"]
        if "1991-2020" in periods:
            period_data = periods["1991-2020"]
            if "monthly" in period_data:
                for month_str, month_data in period_data["monthly"].items():
                    monthly_normals.append(
                        {
                            "month": int(month_str),
                            "eto_mean": month_data.get("normal"),
                            "eto_std": month_data.get("daily_std"),
                            "eto_max": month_data.get("abs_max"),
                            "eto_min": month_data.get("abs_min"),
                            "precipitation_mm": month_data.get(
                                "precip_normal"
                            ),
                            "rain_days": month_data.get("rain_days"),
                        }
                    )

    return {
        "city_name": city_name,
        "state": state,
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "elevation_m": elevation,
        "timezone": None,  # Não disponível nos JSONs
        "data_sources": ["historical_eto_reports"],
        "reference_periods": {"1991-2020": True},
        "monthly_normals": monthly_normals,
    }


def insert_city(conn, city_data: dict) -> Optional[int]:
    """Insere ou atualiza cidade na tabela studied_cities."""
    if not city_data["latitude"] or not city_data["longitude"]:
        print(f"   ⚠️  Pulando {city_data['city_name']} (sem coordenadas)")
        return None

    query = text(
        """
        INSERT INTO climate_history.studied_cities
        (city_name, state, country, latitude, longitude,
         elevation_m, location, timezone, data_sources, reference_periods)
        VALUES
        (:city, :state, :country, :lat, :lon, :elev,
         ST_SetSRID(ST_MakePoint(:lon2, :lat2), 4326),
         :tz, cast(:sources as jsonb), cast(:periods as jsonb))
        ON CONFLICT (city_name, latitude, longitude)
        DO UPDATE SET
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            elevation_m = EXCLUDED.elevation_m,
            timezone = EXCLUDED.timezone,
            data_sources = EXCLUDED.data_sources,
            reference_periods = EXCLUDED.reference_periods,
            updated_at = NOW()
        RETURNING id
    """
    )

    params = {
        "city": city_data["city_name"],
        "state": city_data["state"],
        "country": city_data["country"],
        "lat": city_data["latitude"],
        "lon": city_data["longitude"],
        "elev": city_data["elevation_m"],
        "lon2": city_data["longitude"],
        "lat2": city_data["latitude"],
        "tz": city_data["timezone"],
        "sources": json.dumps(city_data["data_sources"]),
        "periods": json.dumps(city_data["reference_periods"]),
    }

    try:
        result = conn.execute(query, params)
        city_id_row = result.fetchone()
        return city_id_row[0] if city_id_row else None
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        return None


def insert_monthly_normals(conn, city_id: int, normals: list) -> int:
    """Insere normais mensais na tabela monthly_climate_normals."""
    if not normals:
        return 0

    query = text(
        """
        INSERT INTO climate_history.monthly_climate_normals
        (city_id, period_key, month, precipitation_mm, eto_mm_day)
        VALUES
        (:city_id, :period, :month, :precip, :eto)
        ON CONFLICT (city_id, period_key, month)
        DO UPDATE SET
            precipitation_mm = EXCLUDED.precipitation_mm,
            eto_mm_day = EXCLUDED.eto_mm_day
    """
    )

    inserted = 0
    for normal in normals:
        params = {
            "city_id": city_id,
            "period": "1991-2020",
            "month": normal["month"],
            "precip": normal.get("precipitation_mm"),
            "eto": normal.get("eto_mean"),
        }

        try:
            result = conn.execute(query, params)
            inserted += 1
            print(
                f"      [DEBUG] Mês {normal['month']}: ETO={params['eto']}, Precip={params['precip']}"
            )  # noqa: E501
        except Exception as e:
            print(f"   ⚠️  Erro ao inserir mês {normal['month']}: {e}")

    return inserted


def main():
    """Função principal."""
    print("\n" + "=" * 80)
    print("🌍 IMPORTAÇÃO DE DADOS HISTÓRICOS CLIMÁTICOS")
    print("=" * 80)

    # Carregar banco de dados de cidades
    cities_db = load_cities_database()

    # Encontrar todos os JSONs
    reports_dir = project_root / "data" / "historical" / "cities"
    json_files = sorted(reports_dir.glob("report_*.json"))

    print(f"\n📁 Encontrados {len(json_files)} arquivos JSON")
    print("=" * 80)

    cities_imported = 0
    normals_imported = 0
    errors = []

    with get_db_context() as conn:
        for json_file in json_files:
            try:
                print(f"📄 Processando: {json_file.name}")

                # Importar dados do JSON
                city_data = import_city_report(json_file, cities_db)

                # Inserir cidade
                city_id = insert_city(conn, city_data)

                if city_id:
                    print(f"   ✅ Cidade inserida (ID: {city_id})")
                    cities_imported += 1

                    # Debug: mostrar quantos normais foram extraídos
                    normals_count = len(city_data["monthly_normals"])
                    print(f"   DEBUG: {normals_count} normais extraídas")

                    # Inserir normais mensais
                    inserted = insert_monthly_normals(
                        conn, city_id, city_data["monthly_normals"]
                    )
                    if inserted > 0:
                        print(f"   ✅ {inserted} normais mensais inseridas")
                        normals_imported += inserted

                    # Commit incremental
                    conn.commit()
                    print(
                        f"   💾 COMMIT realizado para {city_data['city_name']}"
                    )  # noqa: E501
                else:
                    errors.append(json_file.name)

            except Exception as e:
                # Rollback em caso de erro
                conn.rollback()
                print(f"   ❌ Erro geral: {e}")
                errors.append(json_file.name)

        # Validação
        print("\n" + "=" * 80)
        print("🔍 VALIDANDO DADOS SALVOS")
        print("=" * 80)

        result = conn.execute(
            text("SELECT COUNT(*) FROM climate_history.studied_cities")
        )
        cities_count_row = result.fetchone()
        cities_count = cities_count_row[0] if cities_count_row else 0
        print(f"  📍 Total de cidades: {cities_count}")

        result = conn.execute(
            text(
                "SELECT COUNT(*) "
                "FROM climate_history.monthly_climate_normals"
            )
        )
        normals_count_row = result.fetchone()
        normals_count = normals_count_row[0] if normals_count_row else 0
        print(f"  📊 Total de normais mensais: {normals_count}")

        # Top 5 cidades com mais normais
        result = conn.execute(
            text(
                """
            SELECT c.city_name, c.country, COUNT(n.id) as normal_count
            FROM climate_history.studied_cities c
            LEFT JOIN climate_history.monthly_climate_normals n
                ON c.id = n.city_id
            GROUP BY c.id, c.city_name, c.country
            ORDER BY normal_count DESC
            LIMIT 5
        """
            )
        )

        print("\n  📋 Top 5 cidades com mais normais:")
        for row in result:
            print(f"     - {row[0]} ({row[1]}): {row[2]} meses")

    # Relatório final
    print("\n" + "=" * 80)
    print("📊 RELATÓRIO DE IMPORTAÇÃO")
    print("=" * 80)
    print(f"  ✅ Cidades importadas: {cities_imported}/{len(json_files)}")
    print(f"  ✅ Normais mensais: {normals_imported}")

    if errors:
        print(f"\n  ⚠️  Erros encontrados: {len(errors)}")
        for error_file in errors:
            print(f"     - {error_file}")

    print("\n" + "=" * 80)
    print("✅ IMPORTAÇÃO CONCLUÍDA!")
    print("=" * 80)


if __name__ == "__main__":
    main()
