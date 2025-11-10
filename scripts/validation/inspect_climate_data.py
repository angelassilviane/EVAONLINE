"""
Inspeciona os dados climáticos armazenados no banco de dados.

Mostra os valores reais das variáveis para cada API.

Usage:
    uv run python scripts/validation/inspect_climate_data.py
"""

import sys
from pathlib import Path

# Adicionar raiz ao path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from backend.database.connection import get_db_context
from backend.database.models import ClimateData
from sqlalchemy import func


def print_separator(title=""):
    """Print formatted separator."""
    if title:
        print(f"\n{'=' * 80}")
        print(f"{title.center(80)}")
        print("=" * 80)
    else:
        print("-" * 80)


def format_json_compact(data, max_items=5):
    """Format JSON data in a compact readable way."""
    if not data:
        return "null"

    if isinstance(data, dict):
        items = list(data.items())[:max_items]
        formatted = []
        for key, value in items:
            if isinstance(value, (int, float)):
                formatted.append(f"{key}: {value}")
            elif isinstance(value, str) and len(value) < 50:
                formatted.append(f"{key}: '{value}'")
            elif isinstance(value, dict):
                formatted.append(f"{key}: {{...}}")
            elif isinstance(value, list):
                formatted.append(f"{key}: [{len(value)} items]")
            else:
                formatted.append(f"{key}: {type(value).__name__}")

        result = ", ".join(formatted)
        if len(data) > max_items:
            result += f", ... (+{len(data) - max_items} more)"
        return f"{{{result}}}"

    return str(data)[:100]


def inspect_by_api():
    """Mostra dados agrupados por API."""
    print_separator("📊 DADOS CLIMÁTICOS POR API")

    with get_db_context() as db:
        # Get all APIs with data
        apis = db.query(ClimateData.source_api).distinct().all()
        apis = [api[0] for api in apis]

        print(f"\n🔍 Encontradas {len(apis)} APIs com dados:")
        for api in apis:
            print(f"   • {api}")

        # Inspect each API
        for api_name in sorted(apis):
            print_separator(f"API: {api_name.upper()}")

            records = (
                db.query(ClimateData)
                .filter(ClimateData.source_api == api_name)
                .order_by(ClimateData.date)
                .all()
            )

            print(f"\n📈 Total de registros: {len(records)}")

            for i, record in enumerate(records, 1):
                print(f"\n📅 Registro {i}/{len(records)}")
                print(f"   Data: {record.date}")
                print(
                    f"   Localização: ({record.latitude:.4f}, {record.longitude:.4f})"
                )
                print(f"   Elevação: {record.elevation}m")
                print(f"   Timezone: {record.timezone}")

                # Raw data - show key variables
                print(f"\n   🌡️  RAW DATA:")
                if record.raw_data:
                    raw = record.raw_data

                    # Temperature variables
                    temp_vars = {
                        k: v
                        for k, v in raw.items()
                        if "temp" in k.lower()
                        or "tmax" in k.lower()
                        or "tmin" in k.lower()
                    }
                    if temp_vars:
                        print(f"      Temperatura:")
                        for key, val in temp_vars.items():
                            print(f"         {key}: {val}")

                    # Precipitation
                    precip_vars = {
                        k: v
                        for k, v in raw.items()
                        if "precip" in k.lower()
                        or "rain" in k.lower()
                        or "prectotcorr" in k.lower()
                    }
                    if precip_vars:
                        print(f"      Precipitação:")
                        for key, val in precip_vars.items():
                            print(f"         {key}: {val}")

                    # Wind
                    wind_vars = {
                        k: v
                        for k, v in raw.items()
                        if "wind" in k.lower() or "ws" in k.lower()
                    }
                    if wind_vars:
                        print(f"      Vento:")
                        for key, val in wind_vars.items():
                            print(f"         {key}: {val}")

                    # Humidity
                    humidity_vars = {
                        k: v
                        for k, v in raw.items()
                        if "humidity" in k.lower()
                        or "rh" in k.lower()
                        or "dewpoint" in k.lower()
                    }
                    if humidity_vars:
                        print(f"      Umidade:")
                        for key, val in humidity_vars.items():
                            print(f"         {key}: {val}")

                    # Radiation/Solar
                    solar_vars = {
                        k: v
                        for k, v in raw.items()
                        if "solar" in k.lower()
                        or "radiation" in k.lower()
                        or "allsky" in k.lower()
                    }
                    if solar_vars:
                        print(f"      Radiação Solar:")
                        for key, val in solar_vars.items():
                            print(f"         {key}: {val}")

                    # ETo if present
                    eto_vars = {
                        k: v
                        for k, v in raw.items()
                        if "et0" in k.lower()
                        or "eto" in k.lower()
                        or "evapotranspiration" in k.lower()
                    }
                    if eto_vars:
                        print(f"      Evapotranspiração:")
                        for key, val in eto_vars.items():
                            print(f"         {key}: {val}")

                    # Other variables
                    other_keys = (
                        set(raw.keys())
                        - set(temp_vars.keys())
                        - set(precip_vars.keys())
                        - set(wind_vars.keys())
                        - set(humidity_vars.keys())
                        - set(solar_vars.keys())
                        - set(eto_vars.keys())
                    )
                    if other_keys:
                        print(
                            f"      Outras variáveis: {', '.join(sorted(other_keys))}"
                        )

                # Harmonized data
                print(f"\n   ✨ HARMONIZED DATA:")
                if record.harmonized_data:
                    harm = record.harmonized_data
                    print(f"      {format_json_compact(harm, max_items=10)}")
                else:
                    print(f"      (não disponível)")

                # ETo
                if record.eto_mm_day is not None:
                    print(
                        f"\n   💧 ETo: {record.eto_mm_day:.2f} mm/dia ({record.eto_method})"
                    )
                else:
                    print(f"\n   💧 ETo: não calculado")

                # Quality flags
                if record.quality_flags:
                    print(
                        f"\n   🏷️  Quality Flags: {format_json_compact(record.quality_flags)}"
                    )

                print(f"\n   📝 Criado em: {record.created_at}")

                if i < len(records):
                    print_separator()


def show_summary_statistics():
    """Mostra estatísticas resumidas."""
    print_separator("📊 ESTATÍSTICAS RESUMIDAS")

    with get_db_context() as db:
        # Total records
        total = db.query(ClimateData).count()
        print(f"\n📈 Total de registros: {total}")

        # By API
        api_counts = (
            db.query(
                ClimateData.source_api,
                func.count(ClimateData.id).label("count"),
                func.min(ClimateData.date).label("min_date"),
                func.max(ClimateData.date).label("max_date"),
            )
            .group_by(ClimateData.source_api)
            .all()
        )

        print("\n📋 Resumo por API:")
        for api, count, min_date, max_date in api_counts:
            print(f"\n   {api}:")
            print(f"      Registros: {count}")
            print(f"      Período: {min_date} → {max_date}")

        # ETo coverage
        with_eto = (
            db.query(ClimateData)
            .filter(ClimateData.eto_mm_day.isnot(None))
            .count()
        )
        print(
            f"\n💧 Registros com ETo: {with_eto}/{total} ({100*with_eto/total:.1f}%)"
        )

        # Harmonization coverage
        with_harmonized = (
            db.query(ClimateData)
            .filter(ClimateData.harmonized_data.isnot(None))
            .count()
        )
        print(
            f"✨ Registros harmonizados: {with_harmonized}/{total} ({100*with_harmonized/total:.1f}%)"
        )

        # Unique locations
        unique_locations = (
            db.query(ClimateData.latitude, ClimateData.longitude)
            .distinct()
            .count()
        )
        print(f"📍 Localizações únicas: {unique_locations}")


def main():
    """Main execution."""
    print("=" * 80)
    print("🔍 INSPEÇÃO DE DADOS CLIMÁTICOS NO BANCO DE DADOS".center(80))
    print("=" * 80)

    # Summary first
    show_summary_statistics()

    # Detailed by API
    inspect_by_api()

    print_separator("✅ INSPEÇÃO COMPLETA")
    print()


if __name__ == "__main__":
    main()
