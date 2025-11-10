"""
Inspeciona as tabelas do banco de dados PostgreSQL
Mostra a estrutura das tabelas.
"""

from backend.database.connection import get_db_context
from sqlalchemy import text


def show_tables():
    """Mostra todas as tabelas no banco."""
    print("\n" + "=" * 80)
    print("📊 TABELAS NO BANCO DE DADOS")
    print("=" * 80)

    with get_db_context() as db:
        # Lista todas as tabelas
        result = db.execute(
            text(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
                """
            )
        )
        tables = [row[0] for row in result]

        print(f"\n📋 Total de tabelas: {len(tables)}\n")
        for table in tables:
            print(f"   • {table}")

        # Para cada tabela, mostra estrutura e contagem
        for table in tables:
            print("\n" + "=" * 80)
            print(f"📑 Tabela: {table}")
            print("=" * 80)

            # Contagem de registros
            count_result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = count_result.scalar()
            print(f"\n📊 Total de registros: {count}")

            # Estrutura das colunas
            columns_result = db.execute(
                text(
                    f"""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                    """
                )
            )

            print(f"\n📋 Estrutura das colunas:\n")
            for col in columns_result:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                default = f" DEFAULT {col[3]}" if col[3] else ""
                print(f"   • {col[0]:<30} {col[1]:<20} {nullable}{default}")

            # Se for climate_data, mostra exemplo de raw_data
            if table == "climate_data" and count > 0:
                print("\n📝 Exemplo de raw_data (primeiro registro):\n")
                sample = db.execute(
                    text(
                        """
                        SELECT
                            source_api,
                            date,
                            raw_data::text
                        FROM climate_data
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    )
                ).fetchone()

                if sample:
                    print(f"   Source: {sample[0]}")
                    print(f"   Date: {sample[1]}")
                    print(
                        f"   Raw Data: {sample[2][:200]}..."
                    )  # Se for api_variables, mostra amostra
            if table == "api_variables" and count > 0:
                print("\n📝 Amostra de mapeamentos (5 registros):\n")
                samples = db.execute(
                    text(
                        """
                        SELECT 
                            source_api, 
                            variable_name, 
                            standard_name,
                            unit
                        FROM api_variables 
                        ORDER BY source_api, variable_name
                        LIMIT 5
                        """
                    )
                ).fetchall()

                for sample in samples:
                    print(
                        f"   {sample[0]:<20} | {sample[1]:<30} → {sample[2]:<30} | {sample[3]}"
                    )


if __name__ == "__main__":
    show_tables()
