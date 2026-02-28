if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
from datetime import datetime 
 
@data_loader
def load_data(*args, **kwargs):
    logger = kwargs.get('logger')
    
    
    year_month = kwargs.get("year_month")
    service_type = kwargs.get("service_type")
    
    if not year_month:
        year_month = "2025-01"
    if not service_type:
        service_type = "green"
    
    logger.info(f"=== Procesando {service_type} - {year_month} ===")
    
    
    db_user = get_secret_value('POSTGRES_USER')
    db_password = get_secret_value('POSTGRES_PASSWORD')
    db_host = get_secret_value('POSTGRES_HOST')
    db_port = '5432'
    db_name = get_secret_value('POSTGRES_DB')
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    
    
    with engine.connect() as conn:
        with conn.begin():
            
            result = conn.execute(
                text("""
                    SELECT status FROM orchestration.ingestion_checkpoint 
                    WHERE year_month = :month AND service_type = :service
                """),
                {"month": year_month, "service": service_type}
            ).first()
            
            if result:
                
                if result[0] != 'completed':
                    conn.execute(
                        text("""
                            UPDATE orchestration.ingestion_checkpoint 
                            SET status = 'started', 
                                started_at = NOW(),
                                retry_count = retry_count + 1,
                                updated_at = NOW()
                            WHERE year_month = :month AND service_type = :service
                        """),
                        {"month": year_month, "service": service_type}
                    )
                    logger.info(f"✓ Checkpoint actualizado: {service_type} - {year_month} -> processing")
                else:
                    logger.info(f"⚠️ {service_type} - {year_month} ya está COMPLETADO")
            else:
                
                conn.execute(
                    text("""
                        INSERT INTO orchestration.ingestion_checkpoint 
                        (year_month, service_type, status, started_at)
                        VALUES (:month, :service, 'started', NOW())
                    """),
                    {"month": year_month, "service": service_type}
                )
                logger.info(f"✓ Nuevo checkpoint creado: {service_type} - {year_month}")

    
    url_taxi_zones = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    logger.info(' Inicio de descarga de zonas')
    try:
        zones_csv = pd.read_csv(url_taxi_zones)
        logger.info(f"Se descargaron las zonas exitosamente, con shape {zones_csv.shape}")
    except Exception as e:
        logger.error(f"Error en la descarga de zonas: {e}")
        
        
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    text("""
                        UPDATE orchestration.ingestion_checkpoint 
                        SET status = 'failed', 
                            error_message = :error,
                            updated_at = NOW()
                        WHERE year_month = :month AND service_type = :service
                    """),
                    {
                        "month": year_month, 
                        "service": service_type,
                        "error": str(e)[:200]
                    }
                )
        raise e

    return zones_csv


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'