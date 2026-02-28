if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test 
 
import io
import pandas as pd
import requests
from datetime import datetime


@data_loader
def load_data(data, *args, **kwargs):
    logger = kwargs.get('logger')
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    year_month = kwargs.get("year_month")
    service_type = kwargs.get("service_type")

    current_date = datetime.now()
    current_year_month = current_date.strftime("%Y-%m")
    
    if not year_month:
        year_month = current_year_month

    if not service_type:
        service_type = "yellow"


    url_taxi_generic_link = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    trips_url = url_taxi_generic_link + f"{service_type}_tripdata_{year_month}.parquet"


    
    try:
        logger.info(f"Iniciando descarga de {trips_url}")
        response = requests.get(trips_url)
        response.raise_for_status()

        trips_parquet = pd.read_parquet(io.BytesIO(response.content))
        trips_parquet['ingest_ts'] = pd.Timestamp.now()
        trips_parquet['source_month'] = year_month
        trips_parquet['service_type'] = service_type 
        logger.info("Descarga exitosa")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al descargar {trips_url}: {e}")


    column_list = trips_parquet.columns.tolist()
    logger.info(f"Número de columnas: {len(column_list)}")
    logger.info(f"Nombres: {column_list}")
    return trips_parquet


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
