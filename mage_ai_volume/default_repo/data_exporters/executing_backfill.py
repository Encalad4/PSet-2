# data_exporter: orchestrate_ingestion/trigger_pipelines.py

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
from mage_ai.orchestration.triggers.api import trigger_pipeline
import time
import gc
@data_exporter
def trigger_pipelines(pending_df, *args, **kwargs):
    """
    Ejecuta ingest_bronze para cada combinación pendiente
    """ 
    logger = kwargs.get('logger')
    
    if pending_df.empty:
        logger.info("🎉 No hay combinaciones pendientes. Todo completado!")
        return {"status": "complete", "triggered": 0}
    
    results = {
        'success': [],
        'failed': [],
        'skipped': []
    } 
    
    
    pending_list = pending_df.to_dict('records')
    
    logger.info(f"Iniciando ejecución de {len(pending_list)} pipelines...")
    
    for i, combo in enumerate(pending_list, 1):
        year_month = combo['year_month']
        service_type = combo['service_type']
        
        logger.info(f"[{i}/{len(pending_list)}] Ejecutando: {service_type} - {year_month}")
        
        try:
           
            trigger_pipeline(
                'ingest_bronze', 
                variables={
                    'year_month': year_month,
                    'service_type': service_type
                },
                check_status=True,  
                error_on_failure=False, 
                poll_interval=30,  
                poll_timeout=7200, 
                verbose=True
            )
            
            results['success'].append(f"{service_type}-{year_month}")
            logger.info(f"Completado: {service_type} - {year_month}")
            
            
            time.sleep(5)
            gc.collect()
        except Exception as e:
            results['failed'].append(f"{service_type}-{year_month}")
            logger.error(f"Falló: {service_type} - {year_month}: {str(e)}")
    
    
    logger.info("="*50)
    logger.info("RESUMEN FINAL:")
    logger.info(f"Exitosos: {len(results['success'])}")
    logger.info(f"Fallidos: {len(results['failed'])}")
    logger.info("="*50)
    
    return results