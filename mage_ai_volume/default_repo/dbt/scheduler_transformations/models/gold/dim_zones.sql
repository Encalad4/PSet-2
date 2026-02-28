{{ config(
    materialized='table',
    post_hook=[
        "{% if execute and not flags.FULL_REFRESH %} 
            DO $$ 
            DECLARE
                num_partitions INTEGER := 4;
                i INTEGER;
                partition_name text;
            BEGIN
                
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname LIKE 'dim_zone_part_%'
                    LIMIT 1
                ) THEN
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_zone_new (
                        zone_key INTEGER,
                        borough VARCHAR(50),
                        zone_name VARCHAR(100),
                        service_zone VARCHAR(50)
                    ) PARTITION BY HASH (zone_key);
                    
                   
                    FOR i IN 0..(num_partitions - 1) LOOP
                        partition_name := 'dim_zone_part_' || i;
                        
                        EXECUTE format('
                            CREATE TABLE IF NOT EXISTS {{ this.schema }}.%I 
                            PARTITION OF {{ this.schema }}.dim_zone_new
                            FOR VALUES WITH (MODULUS %s, REMAINDER %s)',
                            partition_name,
                            num_partitions,
                            i
                        );
                    END LOOP;
                    
                    
                    INSERT INTO {{ this.schema }}.dim_zone_new
                    SELECT 
                        location_id AS zone_key,
                        borough,
                        zone_name,
                        service_zone
                    FROM {{ ref('stg_zones') }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.dim_zone_new RENAME TO dim_zones;
                    
                    RAISE NOTICE 'Successfully created HASH partitioned dim_zone with % partitions', num_partitions;
                ELSE
                    RAISE NOTICE 'dim_zone already partitioned, skipping';
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

SELECT 
    location_id AS zone_key,
    COALESCE(borough, 'Unknown Borough') AS borough,
    COALESCE(zone_name, 'Unknown Zone') AS zone_name,
    COALESCE(service_zone, 'Unknown') AS service_zone
FROM {{ ref('stg_zones') }}