if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
import pandas as pd
from sqlalchemy import create_engine, text
import gc

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta datos a PostgreSQL con idempotencia:
    - Usa checkpoint para decidir si eliminar datos existentes
    - Versión optimizada para memoria usando streaming
    """
    logger = kwargs.get('logger')
    
    
    year_month = kwargs.get("year_month")
    service_type = kwargs.get("service_type") 

    current_date = datetime.now()
    current_year_month = current_date.strftime("%Y-%m")
    
    if not year_month:
        year_month = current_year_month
    if not service_type:
        service_type = "yellow"
    
    if not year_month or not service_type:
        raise ValueError("Se requieren year_month y service_type")
     
    
    logger.info(f"Procesando datos para {service_type} - {year_month}")
    
    if service_type == 'yellow':
        logger.info("Renombrando columnas de Yellow Taxi (tpep_ -> lpep_)")
        rename_map = {
            'tpep_pickup_datetime': 'lpep_pickup_datetime',
            'tpep_dropoff_datetime': 'lpep_dropoff_datetime'
        }
        existing_renames = {k: v for k, v in rename_map.items() if k in data.columns}
        if existing_renames:
            data.rename(columns=existing_renames, inplace=True)

        if 'Airport_fee' in data.columns:
            data.drop(columns=['Airport_fee'], inplace=True)
        elif 'airport_fee' in data.columns:
            data.drop(columns=['airport_fee'], inplace=True)

        
        missing_cols = ['trip_type', 'ehail_fee']
        for col in missing_cols:
            if col not in data.columns:
                data[col] = None
            
    elif service_type == 'green':
        logger.info("Datos de Green Taxi, columnas ya en formato lpep_")
    
   
    try:
        db_user = get_secret_value('POSTGRES_USER')
        db_password = get_secret_value('POSTGRES_PASSWORD')
        db_host = get_secret_value('POSTGRES_HOST')
        db_port = '5432'
        db_name = get_secret_value('POSTGRES_DB')
        
        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(connection_string)
        logger.info("✓ Conexión a base de datos establecida")
    except Exception as e:
        logger.error(f"❌ Error conectando a base de datos: {e}")
        raise Exception(f"Error de conexión a BD: {e}")
    
    schema_name = 'raw' 
    table_name = 'taxi_trips'
    
    
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        logger.info(f"✓ Schema {schema_name} verificado/creado")
    
    
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
            
            if 'ingest_ts' not in data.columns:
                columns.append('"ingest_ts" TIMESTAMP')
            if 'source_month' not in data.columns:
                columns.append('"source_month" TEXT')
            if 'service_type' not in data.columns:
                columns.append('"service_type" TEXT')
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    {', '.join(columns)}
                )
            """
            conn.execute(text(create_table_sql))
            logger.info(f"✓ Tabla {schema_name}.{table_name} verificada/creada")
    
    
    should_delete = False
    
   
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT status, completed_at 
                FROM orchestration.ingestion_checkpoint 
                WHERE year_month = :month AND service_type = :service
            """),
            {"month": year_month, "service": service_type}
        ).first()
        
        if result:
            status = result[0]
            completed_at = result[1]
            
            if status == 'completed':
                logger.info(f"✓ Checkpoint indica que {service_type}-{year_month} ya fue completado en {completed_at}")
                logger.info(f"  Eliminando datos existentes por idempotencia...")
                should_delete = True
            elif status == 'processing':
                logger.info(f"  Checkpoint indica que {service_type}-{year_month} está en procesamiento")
                logger.info(f"  Eliminando datos existentes para comenzar de nuevo...")
                should_delete = True
            elif status == 'failed':
                logger.info(f" Checkpoint indica que {service_type}-{year_month} falló anteriormente")
                logger.info(f"  Eliminando datos existentes para reintentar...")
                should_delete = True
        else:
            logger.info(f"No hay checkpoint para {service_type}-{year_month}, insertando datos nuevos")
   
    if should_delete:
        with engine.connect() as conn:
            with conn.begin():
                delete_sql = text(f"""
                    DELETE FROM {schema_name}.{table_name} 
                    WHERE source_month = :month AND service_type = :service
                """)
                result = conn.execute(delete_sql, {"month": year_month, "service": service_type})
            logger.info(f"✓ Eliminados {result.rowcount} registros existentes para {service_type} - {year_month}")
    else:
        logger.info(f"→ No es necesario eliminar datos existentes")
    
    
    with engine.connect() as conn:
        with conn.begin():
            
            conn.execute(
                text("""
                    INSERT INTO orchestration.ingestion_checkpoint 
                    (year_month, service_type, status, started_at, retry_count)
                    VALUES (:month, :service, 'processing', NOW(), 1)
                    ON CONFLICT (year_month, service_type) 
                    DO UPDATE SET 
                        status = 'processing',
                        started_at = NOW(),
                        retry_count = orchestration.ingestion_checkpoint.retry_count + 1,
                        error_message = NULL,
                        updated_at = NOW()
                """),
                {
                    "month": year_month, 
                    "service": service_type
                }
            )
    logger.info(f"✓ Checkpoint actualizado a 'processing' para {service_type}-{year_month}")
    

    rows_inserted = 0
    chunksize = 25000
    
    columns = data.columns.tolist()
    
    placeholders = ', '.join([f':{col}' for col in columns])
    column_names = ', '.join([f'"{col}"' for col in columns])
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} ({column_names})
        VALUES ({placeholders})
    """
    
    total_rows = len(data)
    logger.info(f"Iniciando inserción de {total_rows} filas en chunks de {chunksize}")
    
    with engine.connect() as conn:
        with conn.begin():
            for start_idx in range(0, total_rows, chunksize):
                end_idx = min(start_idx + chunksize, total_rows)
                
                chunk = data.iloc[start_idx:end_idx]
                
                chunk_records = []
                for _, row in chunk.iterrows():
                    record = {}
                    for col in columns:
                        value = row[col]
                        if pd.isna(value):
                            record[col] = None
                        elif isinstance(value, pd.Timestamp):
                            record[col] = value.isoformat()
                        else:
                            record[col] = value
                    chunk_records.append(record)
                
                conn.execute(text(insert_query), chunk_records)
                
                rows_inserted += len(chunk)
                logger.info(f"Progreso: {rows_inserted}/{total_rows} filas insertadas ({rows_inserted/total_rows*100:.1f}%)")
                
                del chunk_records
                del chunk
                gc.collect()
    
    logger.info(f"✓ Insertadas {rows_inserted} filas en total")
    
   
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text("""
                        UPDATE orchestration.ingestion_checkpoint 
                        SET status = 'completed',
                            completed_at = NOW(),
                            rows_ingested = :rows,
                            updated_at = NOW()
                        WHERE year_month = :month AND service_type = :service
                    """),
                    {
                        "month": year_month, 
                        "service": service_type,
                        "rows": rows_inserted
                    }
                )
        logger.info(f"Checkpoint actualizado a 'completed'")
    except Exception as e:
        logger.error(f"Error actualizando checkpoint a 'completed': {e}")
        raise
    
    gc.collect()
    return {"status": "success", "rows_inserted": rows_inserted}