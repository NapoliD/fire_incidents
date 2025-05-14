import logging
from sqlalchemy import text

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseSchema:
    """
    Clase para gestionar el esquema de la base de datos:
      - Elimina tablas existentes (con DROP IF EXISTS CASCADE).
      - Crea tablas de dimensiones, staging y hechos de forma idempotente.
      - Envuelve todo en una transacción.
    """
    def __init__(self, engine, schema: str = None):
        self.engine = engine
        self.schema = schema

    def create_schema(self):
        """Recrea el esquema completo de la DW eliminando y volviendo a crear tablas."""
        logger.info("Iniciando verificación/recreación del esquema de base de datos")
        with self.engine.begin() as conn:
            prefix = f"{self.schema}." if self.schema else ""

            # 1) DROP tablas en orden inverso de dependencias
            for table in [
                'fact_fire_incident',
                'stg_fire_incident',
                'dim_action_taken',
                'dim_location',
                'dim_call_type',
                'dim_prime_situation',
                'dim_station_area',
                'dim_battalion',
                'dim_date_time'
            ]:
                conn.execute(text(f"DROP TABLE IF EXISTS {prefix}{table} CASCADE;"))
                logger.info(f"Tabla {prefix}{table} eliminada (si existía)")

            # 2) CREATE tablas dimensionales
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_date_time (
                    date_key DATE PRIMARY KEY,
                    year INTEGER,
                    quarter INTEGER,
                    month INTEGER,
                    day INTEGER,
                    day_of_week VARCHAR(9),
                    hour INTEGER,
                    minute INTEGER
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_battalion (
                    battalion_id SERIAL PRIMARY KEY,
                    battalion_name VARCHAR(20) UNIQUE NOT NULL
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_station_area (
                    station_id SERIAL PRIMARY KEY,
                    station_area VARCHAR(20) UNIQUE NOT NULL
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_call_type (
                    call_type_id SERIAL PRIMARY KEY,
                    call_type VARCHAR(100) UNIQUE NOT NULL
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_prime_situation (
                    prime_situation_id SERIAL PRIMARY KEY,
                    description VARCHAR(200) UNIQUE NOT NULL
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_location (
                    location_key SERIAL PRIMARY KEY,
                    address VARCHAR(200) UNIQUE NOT NULL,
                    block VARCHAR(100),
                    street VARCHAR(100),
                    intersection_dirs VARCHAR(100),
                    neighborhood VARCHAR(100),
                    supervisor_district VARCHAR(50),
                    city VARCHAR(50),
                    zip_code VARCHAR(10),
                    latitude DECIMAL(9,6),
                    longitude DECIMAL(9,6)
                );
            """))
            conn.execute(text(f"""
                CREATE TABLE {prefix}dim_action_taken (
                    action_taken_id SERIAL PRIMARY KEY,
                    action_description VARCHAR(200) UNIQUE NOT NULL
                );
            """))

            # 3) CREATE tabla de staging
            conn.execute(text(f"""
                CREATE TABLE {prefix}stg_fire_incident (
                    incident_number VARCHAR(20),
                    call_number VARCHAR(20),
                    incident_datetime TIMESTAMP,
                    call_date DATE,
                    call_time TIME,
                    address VARCHAR(200),
                    block VARCHAR(100),
                    street VARCHAR(100),
                    intersection_directions VARCHAR(100),
                    battalion VARCHAR(20),
                    station_area VARCHAR(20),
                    call_type VARCHAR(100),
                    prime_situation VARCHAR(200),
                    actions_taken TEXT[],
                    neighborhood_district VARCHAR(100),
                    supervisor_district VARCHAR(50),
                    city VARCHAR(50),
                    zip_code VARCHAR(10),
                    latitude DECIMAL(9,6),
                    longitude DECIMAL(9,6),
                    number_units_responding INTEGER,
                    unit_type VARCHAR(100),
                    property_loss DECIMAL(12,2),
                    acres_burned DECIMAL(8,2),
                    civilian_injuries INTEGER,
                    firefighter_injuries INTEGER,
                    civilian_fatalities INTEGER,
                    firefighter_fatalities INTEGER
                );
            """))

            # 4) CREATE tabla de hechos
            conn.execute(text(f"""
                CREATE TABLE {prefix}fact_fire_incident (
                    incident_id BIGINT PRIMARY KEY,
                    call_number VARCHAR(20),
                    date_time_key DATE REFERENCES {prefix}dim_date_time(date_key),
                    location_key INTEGER REFERENCES {prefix}dim_location(location_key),
                    battalion_id INTEGER REFERENCES {prefix}dim_battalion(battalion_id),
                    station_id INTEGER REFERENCES {prefix}dim_station_area(station_id),
                    call_type_id INTEGER REFERENCES {prefix}dim_call_type(call_type_id),
                    prime_situation_id INTEGER REFERENCES {prefix}dim_prime_situation(prime_situation_id),
                    action_taken_id INTEGER REFERENCES {prefix}dim_action_taken(action_taken_id),
                    number_units_responding INTEGER,
                    units_detail VARCHAR(100),
                    property_loss_amount DECIMAL(12,2),
                    acres_burned DECIMAL(8,2),
                    civilian_injuries INTEGER,
                    firefighter_injuries INTEGER,
                    civilian_fatalities INTEGER,
                    firefighter_fatalities INTEGER
                );
            """))

        logger.info("Esquema de base de datos recreado con éxito")
