{{ config(
    materialized='table',
    post_hook=[
        "{% if execute and not flags.FULL_REFRESH %} 
            DO $$ 
            BEGIN 
                -- Check if table is already partitioned
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname = 'dim_payment_type_cash'
                ) THEN
                    -- Convert to partitioned table
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_payment_type_new (
                        payment_type_key INTEGER,
                        payment_type_name VARCHAR(50),
                        partition_group VARCHAR(20)
                    ) PARTITION BY LIST (partition_group);
                    
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_payment_type_cash 
                        PARTITION OF {{ this.schema }}.dim_payment_type_new 
                        FOR VALUES IN ('cash');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_payment_type_card 
                        PARTITION OF {{ this.schema }}.dim_payment_type_new 
                        FOR VALUES IN ('card');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_payment_type_flex 
                        PARTITION OF {{ this.schema }}.dim_payment_type_new 
                        FOR VALUES IN ('flex');
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_payment_type_other 
                        PARTITION OF {{ this.schema }}.dim_payment_type_new 
                        FOR VALUES IN ('other/unknown');
                    
                    
                    INSERT INTO {{ this.schema }}.dim_payment_type_new
                    SELECT * FROM {{ this }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.dim_payment_type_new RENAME TO dim_payment_type;
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

WITH payment_types AS (
    SELECT * FROM (VALUES
        (0, 'Flex Fare',    'flex'),
        (1, 'Credit Card',  'card'),
        (2, 'Cash',         'cash'),
        (3, 'No Charge',    'other/unknown'),
        (4, 'Dispute',      'other/unknown'),
        (5, 'Unknown',      'other/unknown'),
        (6, 'Voided Trip',  'other/unknown')
    ) AS t(payment_type_key, payment_type_name, partition_group)
)

SELECT
    payment_type_key,
    payment_type_name,
    partition_group
FROM payment_types