from sqlalchemy import create_engine, text
import os

# Construir DATABASE_URL do .env
db_url = os.getenv(
    "DATABASE_URL",
    "postgresql://evaonline:evaonline@localhost:5432/evaonline_dev",
)
engine = create_engine(db_url)

with engine.connect() as conn:
    result = conn.execute(
        text(
            """
        SELECT city_name, state, country 
        FROM climate_history.studied_cities 
        WHERE city_name IN ('Des_Moines', 'Fresno', 'Addis_Ababa', 'Balsas', 'Rosario')
        ORDER BY city_name
    """
        )
    )

    print(f"{'City':<20} {'State':<10} {'Country':<15}")
    print("=" * 50)
    for row in result:
        state = row[1] if row[1] else "NULL"
        print(f"{row[0]:<20} {state:<10} {row[2]:<15}")
