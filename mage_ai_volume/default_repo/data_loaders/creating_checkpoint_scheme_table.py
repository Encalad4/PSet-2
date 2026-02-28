# data_loader: orchestrate_ingestion/check_pending.py

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import itertools

@data_loader
def check_pending(*args, **kwargs):
    """
    Genera lista de parámetros pendientes basado en checkpoint
    """
    logger = kwargs.get('logger')
    
    
    db_user = get_secret_value('POSTGRES_USER')
    db_password = get_secret_value('POSTGRES_PASSWORD')
    db_host = get_secret_value('POSTGRES_HOST')
    db_port = '5432'
    db_name = get_secret_value('POSTGRES_DB')
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    
    with engine.connect() as conn:
        with conn.begin():
            
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS orchestration"))
            logger.info("✓ Esquema 'orchestration' verificado")
            
           
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
            """
            conn.execute(text(create_table_sql))
            logger.info("✓ Tabla 'ingestion_checkpoint' verificada")
    
   
    years = range(2022, 2026) 
    months = range(1, 13)
    services = ['yellow','green']
    
    all_combinations = []
    for year, month, service in itertools.product(years, months, services):
        year_month = f"{year}-{month:02d}"
        all_combinations.append({
            'year_month': year_month,
            'service_type': service
        })
    
    
    with engine.connect() as conn:
        completed = pd.read_sql(
            """
            SELECT year_month, service_type 
            FROM orchestration.ingestion_checkpoint 
            WHERE status = 'completed'
            """, 
            conn
        )
    
    
    completed_set = set(zip(completed['year_month'], completed['service_type']))
    
    
    pending = []
    for combo in all_combinations:
        key = (combo['year_month'], combo['service_type'])
        if key not in completed_set:
            pending.append(combo)
    
    logger.info(f"Total combinaciones: {len(all_combinations)}")
    logger.info(f"Ya completadas: {len(completed_set)}")
    logger.info(f"Pendientes: {len(pending)}")
    
   
    if pending:
        logger.info("Primeras 5 pendientes:")
        for p in pending[:5]:
            logger.info(f"  - {p['service_type']} {p['year_month']}")
    
    return pd.DataFrame(pending)

@test
def test_output(output, *args) -> None:
    assert output is not None, 'El output es undefined'