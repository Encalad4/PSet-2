if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
import pandas as pd
from sqlalchemy import create_engine, text
import os
import gc
@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta datos de zonas a PostgreSQL
    - Reemplaza todos los datos cada vez que se ejecuta
    """
    logger = kwargs.get('logger')
    
    
    db_user = get_secret_value('POSTGRES_USER')
    db_password = get_secret_value('POSTGRES_PASSWORD')
    db_host = get_secret_value('POSTGRES_HOST')
    db_port = '5432'
    db_name = get_secret_value('POSTGRES_DB')
    
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    schema_name = 'raw'
    table_name = 'taxi_zones'
    
   
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        logger.info(f"Schema {schema_name} verificado/creado")
    
    
    with engine.connect() as conn:
        with conn.begin():
            
            dtype_mapping = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'datetime64[ns]': 'TIMESTAMP',
                'bool': 'BOOLEAN'
            }
            
            
            columns = []
            for col_name, col_type in data.dtypes.items():
                sql_type = dtype_mapping.get(str(col_type), 'TEXT')
                columns.append(f'"{col_name}" {sql_type}')
            
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    {', '.join(columns)}
                )
            """
            conn.execute(text(create_table_sql))
            logger.info(f"Tabla {schema_name}.{table_name} verificada/creada")
    
    
    with engine.connect() as conn:
        with conn.begin():
            delete_sql = text(f"DELETE FROM {schema_name}.{table_name}")
            result = conn.execute(delete_sql)
        logger.info(f"Eliminados {result.rowcount} registros existentes")
    
    
    data.to_sql(
        table_name,
        con=engine,
        schema=schema_name,
        if_exists='append', 
        index=False,
        chunksize=10000
    )
    
    logger.info(f"Insertadas {len(data)} filas en {schema_name}.{table_name}")
    gc.collect()
    return {"status": "success", "rows_inserted": len(data)}