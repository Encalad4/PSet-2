if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import gc

@data_exporter
def create_partitioned_table(*args, **kwargs):
    """
    Creates partitioned dim_payment_type table in PostgreSQL using Mage Secrets
    """
    logger = kwargs.get('logger')
    
    # 1. CONEXIÓN DIRECTA
    db_user = get_secret_value('POSTGRES_USER')
    db_password = get_secret_value('POSTGRES_PASSWORD')
    db_host = get_secret_value('POSTGRES_HOST')
    db_port = '5432'
    db_name = get_secret_value('POSTGRES_DB')
    
    # Crear conexión
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    schema_name = 'analytics_gold'
    
    logger.info("Starting creation of partitioned dim_payment_type table")
    
    # 2. CREAR SCHEMA SI NO EXISTE
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        logger.info(f"Schema {schema_name} verificado/creado")
    
    # 3. CREAR TABLA PARTICIONADA
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.dim_payment_type (
        payment_type_key INTEGER,
        payment_type_name VARCHAR(50),
        partition_group VARCHAR(20)
    ) PARTITION BY LIST (partition_group);
    
    -- Create partitions
    CREATE TABLE IF NOT EXISTS {schema_name}.dim_payment_type_cash PARTITION OF {schema_name}.dim_payment_type
    FOR VALUES IN ('cash');
    
    CREATE TABLE IF NOT EXISTS {schema_name}.dim_payment_type_card PARTITION OF {schema_name}.dim_payment_type
    FOR VALUES IN ('card');
    
    CREATE TABLE IF NOT EXISTS {schema_name}.dim_payment_type_flex PARTITION OF {schema_name}.dim_payment_type
    FOR VALUES IN ('flex');
    
    CREATE TABLE IF NOT EXISTS {schema_name}.dim_payment_type_other PARTITION OF {schema_name}.dim_payment_type
    FOR VALUES IN ('other/unknown');
    """
    
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(create_table_sql))
        logger.info("Successfully created partitioned table gold.dim_payment_type")
    except Exception as e:
        logger.error(f"Error creating partitioned table: {str(e)}")
        raise
    
    gc.collect()
    return {"table_created": f"{schema_name}.dim_payment_type"}