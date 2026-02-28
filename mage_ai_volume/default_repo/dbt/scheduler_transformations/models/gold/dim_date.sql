{{ config(
    materialized='table',
    post_hook=[
        "{% if execute and not flags.FULL_REFRESH %} 
            DO $$ 
            DECLARE
                start_date date;
                end_date date;
                current_date_val date;
                partition_name text;
            BEGIN
                
                IF NOT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname LIKE 'dim_date_%'
                    LIMIT 1
                ) THEN
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.dim_date_new (
                        date_key INTEGER,
                        date DATE,
                        year INTEGER,
                        month INTEGER,
                        month_name VARCHAR(20),
                        day INTEGER,
                        day_of_week INTEGER,
                        day_name VARCHAR(20),
                        year_month VARCHAR(7)
                    ) PARTITION BY RANGE (date);
                    
                    
                    SELECT MIN(date), MAX(date) 
                    INTO start_date, end_date
                    FROM {{ this }};
                    
                    
                    current_date_val := DATE_TRUNC('month', start_date);
                    
                    WHILE current_date_val <= end_date LOOP
                        partition_name := 'dim_date_' || TO_CHAR(current_date_val, 'YYYY_MM');
                        
                        EXECUTE format('
                            CREATE TABLE IF NOT EXISTS {{ this.schema }}.%I 
                            PARTITION OF {{ this.schema }}.dim_date_new
                            FOR VALUES FROM (%L) TO (%L)',
                            partition_name,
                            current_date_val,
                            current_date_val + INTERVAL '1 month'
                        );
                        
                        current_date_val := current_date_val + INTERVAL '1 month';
                    END LOOP;
                    
                    
                    INSERT INTO {{ this.schema }}.dim_date_new
                    SELECT 
                        date_key,
                        date,
                        year,
                        month,
                        month_name,
                        day,
                        day_of_week,
                        day_name,
                        year_month
                    FROM {{ this }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.dim_date_new RENAME TO dim_date;
                    
                    RAISE NOTICE 'Successfully created RANGE partitioned dim_date with monthly partitions from % to %', start_date, end_date;
                ELSE
                    RAISE NOTICE 'dim_date already partitioned, skipping';
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

WITH date_spine AS (
    SELECT generate_series(
        '2022-01-01'::DATE,
        '2025-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS full_date 
)

SELECT
    
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    
    
    full_date AS date,
    
    
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    
    
    TO_CHAR(full_date, 'YYYY-MM') AS year_month
    
FROM date_spine
ORDER BY full_date