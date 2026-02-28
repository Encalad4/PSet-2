# data_loader: create_checkpoint_table.py

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd

@data_loader
def create_checkpoint_table(*args, **kwargs):
    """
    Crea el esquema orchestration y la tabla de checkpoint si no existen
    """
    logger = kwargs.get('logger')
    
    # 1. Obtener credenciales
    db_user = get_secret_value('POSTGRES_USER')
    db_password = get_secret_value('POSTGRES_PASSWORD')
    db_host = get_secret_value('POSTGRES_HOST')
    db_port = '5432'
    db_name = get_secret_value('POSTGRES_DB')
    
    # 2. Crear conexión
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    # 3. Crear esquema orchestration si no existe
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS orchestration"))
            logger.info("✓ Esquema 'orchestration' verificado/creado")
    
    # 4. Crear tabla de checkpoint si no existe
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS orchestration.ingestion_checkpoint (
        id SERIAL PRIMARY KEY,
        year_month VARCHAR(7) NOT NULL,
        service_type VARCHAR(10) NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        rows_ingested INTEGER,
        error_message TEXT,
        retry_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(year_month, service_type)
    );
    
    CREATE INDEX IF NOT EXISTS idx_checkpoint_status 
        ON orchestration.ingestion_checkpoint(status);
    CREATE INDEX IF NOT EXISTS idx_checkpoint_year_month 
        ON orchestration.ingestion_checkpoint(year_month);
    """
    
    with engine.connect() as conn:
        with conn.begin():
            # Ejecutar cada comando por separado
            for statement in create_table_sql.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            logger.info("✓ Tabla 'ingestion_checkpoint' verificada/creada")
    
    # 5. Retornar un DataFrame vacío o mensaje de éxito
    return pd.DataFrame({'status': ['success'], 'message': ['Checkpoint table ready']})

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert output['status'].iloc[0] == 'success', 'Checkpoint creation failed'