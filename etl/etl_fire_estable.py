import pandas as pd
from sqlalchemy import create_engine, text, ARRAY, String
import os
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de la base de datos y archivo CSV
db_uri_default = "postgresql://postgres:postgres@localhost:5432/sf_fire_incidents"
csv_path_default = "c:/Users/napol/OneDrive/Escritorio/challege/Fire_Incidents_20250513.csv"
DB_URI = os.getenv("FIRE_DW_DB_URI", db_uri_default)
CSV_PATH = os.getenv("FIRE_CSV_PATH", csv_path_default)

from database_schema import DatabaseSchema


def connect_to_db():
    """Establece la conexión a la base de datos."""
    if not DB_URI:
        raise ValueError("Database URI not set in environment variables")
    return create_engine(DB_URI)


def create_database_schema(engine):
    """Reinicia el esquema de la base de datos y las tablas."""
    db_schema = DatabaseSchema(engine)
    db_schema.create_schema()


def run_etl():
    """Función principal del proceso ETL."""
    logger.info("Iniciando proceso ETL")
    try:
        engine = connect_to_db()
        logger.info(f"Conectado a la base de datos: {engine.url}")

        # Reiniciar esquema/tablas
        create_database_schema(engine)

        # 1) CARGA Y TRANSFORMACIÓN DE CSV
        logger.info(f"Cargando datos desde: {CSV_PATH}")
        dtypes = {
            "Station Area": str,
            "Battalion": str,
            "Supervisor District": str,
            "zipcode": str,
            "block": str,
            "Ignition Cause": str,
            "Ignition Factor Primary": str,
            "Ignition Factor Secondary": str
        }
        na_values = ["", "NA", "N/A", "null", "NULL", "nan", "NaN"]
        try:
            df = pd.read_csv(CSV_PATH, dtype=dtypes, na_values=na_values, low_memory=False).head(10000)
        except Exception as e:
            logger.warning(f"Carga avanzada falló: {e}, usando modo sencillo")
            df = pd.read_csv(CSV_PATH, low_memory=False).head(10000)
        logger.info(f"Cargados {len(df)} registros")

        # Renombrar columnas según esquema
        rename_map = {
            "Incident Number": "incident_number",
            "Address": "address",
            "Incident Date": "call_date",
            "Alarm DtTm": "call_time",
            "Call Number": "call_number",
            "Battalion": "battalion",
            "Station Area": "station_area",
            "Primary Situation": "prime_situation",
            "Action Taken Primary": "action_taken_primary",
            "Action Taken Secondary": "action_taken_secondary",
            "Action Taken Other": "action_taken_other",
            "City": "city",
            "zipcode": "zip_code",
            "point": "geo_point",
            "Estimated Property Loss": "property_loss",
            "Acres Burned": "acres_burned",
            "Civilian Injuries": "civilian_injuries",
            "Firefighter Injuries": "firefighter_injuries",
            "Civilian Fatalities": "civilian_fatalities",
            "Fire Fatalities": "firefighter_fatalities",
            "Suppression Units": "number_units_responding",
            "First Unit On Scene": "unit_type",
            "neighborhood_district": "neighborhood_district",
            "Supervisor District": "supervisor_district"
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Fechas y horas
        for col in ["call_date", "call_time"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Extraer latitud/longitud
        if 'geo_point' in df.columns:
            def extract_coords(pt):
                try:
                    coords = pt.split('(')[1].split(')')[0].split()
                    return float(coords[0]), float(coords[1])
                except:
                    return 0.0, 0.0
            coords = df['geo_point'].astype(str).map(extract_coords)
            df['longitude'] = coords.map(lambda x: x[0])
            df['latitude'] = coords.map(lambda x: x[1])
        elif {'latitude', 'longitude'}.issubset(df.columns):
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)

        # Dirección: block, street, intersection_directions
        if 'address' in df.columns:
            df['address'] = df['address'].fillna('')
            has_int = df['address'].str.contains('/', na=False)
            # Filas sin intersección
            no_int = ~has_int
            def split_addr(a):
                parts = a.split(' ', 1)
                return (parts[0], parts[1]) if parts[0].isdigit() and len(parts) > 1 else ('', a)
            split = df.loc[no_int, 'address'].map(split_addr)
            df.loc[no_int, 'block'] = split.map(lambda x: x[0])
            df.loc[no_int, 'street'] = split.map(lambda x: x[1])
            # Filas con intersección
            df.loc[has_int, 'block'] = ''
            df.loc[has_int, 'street'] = df.loc[has_int, 'address']
            df.loc[has_int, 'intersection_directions'] = 'INTERSEC'

        # Acciones tomadas
        action_cols = [c for c in ['action_taken_primary', 'action_taken_secondary', 'action_taken_other'] if c in df.columns]
        df['actions_taken'] = df.apply(
            lambda r: [str(r[c]) for c in action_cols if pd.notna(r[c]) and str(r[c]).strip()], axis=1
        )

        # Unificar datetime
        if {'call_date', 'call_time'}.issubset(df.columns):
            df['incident_datetime'] = pd.to_datetime(
                df['call_date'].dt.strftime('%Y-%m-%d') + ' ' + df['call_time'].dt.strftime('%H:%M:%S'),
                errors='coerce'
            )
        else:
            df['incident_datetime'] = pd.to_datetime(df.get('call_date'), errors='coerce')

        # Eliminar nulos y duplicados clave
        df = df.dropna(subset=['incident_number'])
        df = df.drop_duplicates(subset=['incident_number'])

        # Asegurar columnas requeridas
        required = [
            'incident_number', 'call_number', 'incident_datetime', 'address', 'block', 'street',
            'intersection_directions', 'battalion', 'station_area', 'prime_situation', 'actions_taken',
            'neighborhood_district', 'supervisor_district', 'city', 'zip_code', 'latitude', 'longitude',
            'number_units_responding', 'unit_type', 'property_loss', 'acres_burned',
            'civilian_injuries', 'firefighter_injuries', 'civilian_fatalities', 'firefighter_fatalities'
        ]
        for col in required:
            if col not in df.columns:
                df[col] = None

        # Cast numéricos
        num_map = {
            'property_loss': 'float64', 'acres_burned': 'float64',
            'civilian_injuries': 'int64', 'firefighter_injuries': 'int64',
            'civilian_fatalities': 'int64', 'firefighter_fatalities': 'int64',
            'number_units_responding': 'int64'
        }
        for col, dtype in num_map.items():
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(dtype)

        staging_df = df[required]
        logger.info("Transformación de datos completada con deduplicación")

        # 2) CARGA EN BASE DE DATOS
        with engine.begin() as conn:
            staging_df.to_sql(
                'stg_fire_incident', con=conn, if_exists='replace', index=False,
                dtype={'actions_taken': ARRAY(String)}
            )
            logger.info("Tabla de staging creada")

            # Actualizar dimensiones y hechos...
            # (resto de los inserts igual)

        logger.info("ETL completado con éxito ✔️")
        return True

    except Exception as e:
        logger.error(f"Error en el proceso ETL: {e}")
        raise

if __name__ == "__main__":
    run_etl()
