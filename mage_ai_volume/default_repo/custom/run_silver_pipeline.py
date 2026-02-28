if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage_ai.orchestration.triggers.api import trigger_pipeline

@custom
def trigger_dbt_pipeline(*args, **kwargs):
    """
    Trigger the dbt_build_silver pipeline after ingest_bronze completes
    """
    
    trigger_pipeline(
        'dbt_build_silver', 
        check_status=False,    
        error_on_failure=False, 
        verbose=True,
        schedule_name='run_after_ingest_bronze',         
    )
    
    return {"status": "dbt_build_silver triggered successfully"}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'status' in output, 'Status not found in output'