{{ config(
    materialized='table',
    post_hook=[
        "{% if execute and not flags.FULL_REFRESH %} 
            DO $$ 
            BEGIN 
                
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname = 'dim_service_type_yellow'
                ) THEN
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_service_type_new (
                        service_type_key INTEGER,
                        service_type_name VARCHAR(20),
                        service_type_code VARCHAR(10),
                        partition_group VARCHAR(10)
                    ) PARTITION BY LIST (partition_group);
                    
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_service_type_yellow 
                        PARTITION OF {{ this.schema }}.dim_service_type_new 
                        FOR VALUES IN ('yellow');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_service_type_green 
                        PARTITION OF {{ this.schema }}.dim_service_type_new 
                        FOR VALUES IN ('green');
                    
                    
                    INSERT INTO {{ this.schema }}.dim_service_type_new
                    SELECT * FROM {{ this }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.dim_service_type_new RENAME TO dim_service_type;
                    
                    RAISE NOTICE 'Successfully created partitioned dim_service_type';
                ELSE
                    RAISE NOTICE 'dim_service_type already partitioned, skipping';
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

WITH service_types AS (
    SELECT * FROM (VALUES
        (1, 'Yellow Taxi', 'yellow', 'yellow'),
        (2, 'Green Taxi',  'green',  'green')
    ) AS t(service_type_key, service_type_name, service_type_code, partition_group)
)

SELECT
    service_type_key,
    service_type_name,
    service_type_code,
    partition_group
FROM service_types
ORDER BY service_type_key