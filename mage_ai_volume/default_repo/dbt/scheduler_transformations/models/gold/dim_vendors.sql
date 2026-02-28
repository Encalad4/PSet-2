{{ config(
    materialized='table',
    post_hook=[
        "{% if execute and not flags.FULL_REFRESH %} 
            DO $$ 
            BEGIN 
                
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname = 'dim_vendor_creative'
                ) THEN
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_new (
                        vendor_key INTEGER,
                        vendor_id INTEGER,
                        vendor_name VARCHAR(100),
                        vendor_code VARCHAR(10),
                        vendor_group VARCHAR(50),
                        description TEXT,
                        partition_group VARCHAR(20)
                    ) PARTITION BY LIST (partition_group);
                    
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_creative 
                        PARTITION OF {{ this.schema }}.dim_vendor_new 
                        FOR VALUES IN ('creative');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_curb 
                        PARTITION OF {{ this.schema }}.dim_vendor_new 
                        FOR VALUES IN ('curb');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_myle 
                        PARTITION OF {{ this.schema }}.dim_vendor_new 
                        FOR VALUES IN ('myle');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_helix 
                        PARTITION OF {{ this.schema }}.dim_vendor_new 
                        FOR VALUES IN ('helix');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_vendor_other 
                        PARTITION OF {{ this.schema }}.dim_vendor_new 
                        FOR VALUES IN ('other');
                    
                    
                    INSERT INTO {{ this.schema }}.dim_vendor_new
                    SELECT * FROM {{ this }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.dim_vendor_new RENAME TO dim_vendors;
                    
                    RAISE NOTICE 'Successfully created partitioned dim_vendor';
                ELSE
                    RAISE NOTICE 'dim_vendor already partitioned, skipping';
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

WITH vendors AS (
    SELECT * FROM (VALUES
        (1, 1, 'Creative Mobile Technologies, LLC', 'CMT', 'Creative', 'Creative Mobile Technologies - TPEP provider', 'creative'),
        (2, 2, 'Curb Mobility, LLC', 'CURB', 'Curb', 'Curb Mobility (formerly Verifone)', 'curb'),
        (3, 6, 'Myle Technologies Inc', 'MYLE', 'Myle', 'Myle Technologies Inc', 'myle'),
        (4, 7, 'Helix', 'HELIX', 'Helix', 'Helix', 'helix'),
        (5, 99, 'Other', 'OTH', 'Other', 'Other or unknown vendors', 'other')
    ) AS t(vendor_key, vendor_id, vendor_name, vendor_code, vendor_group, description, partition_group)
)

SELECT
    vendor_key,
    vendor_id,
    vendor_name,
    vendor_code,
    vendor_group,
    description,
    partition_group
FROM vendors
ORDER BY vendor_key