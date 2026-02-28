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
                partition_exists boolean;
            BEGIN
                
                SELECT EXISTS (
                    SELECT 1 
                    FROM pg_class 
                    WHERE relname LIKE 'fct_trips_%'
                    LIMIT 1
                ) INTO partition_exists;
                
                IF NOT partition_exists THEN
                    
                    CREATE TABLE IF NOT EXISTS {{ this.schema }}.fct_trips_new (
                        
                        trip_key VARCHAR(32),
                        
                        
                        pickup_date DATE,
                        pickup_date_key INTEGER,
                        dropoff_date_key INTEGER,
                        
                        
                        service_type_key INTEGER,
                        vendor_key INTEGER,
                        pu_zone_key INTEGER,
                        do_zone_key INTEGER,
                        payment_type_key INTEGER,
                        
                        
                        passenger_count INTEGER,
                        trip_distance NUMERIC(10,2),
                        fare_amount NUMERIC(10,2),
                        tip_amount NUMERIC(10,2),
                        tolls_amount NUMERIC(10,2),
                        total_amount NUMERIC(10,2),
                        trip_time_minutes NUMERIC(10,2),
                        
                       
                        tip_percentage NUMERIC(10,4),
                        speed_mph NUMERIC(10,2),
                        
                        
                        pickup_location_id INTEGER,
                        dropoff_location_id INTEGER,
                        payment_type_id INTEGER,
                        vendor_id INTEGER,
                        
                       
                        source_month VARCHAR(7),
                        service_type VARCHAR(10),
                        ingest_ts TIMESTAMP
                        
                    ) PARTITION BY RANGE (pickup_date);
                    
                   
                    SELECT MIN(pickup_date), MAX(pickup_date) 
                    INTO start_date, end_date
                    FROM {{ this }};
                    
                    
                    current_date_val := DATE_TRUNC('month', start_date);
                    
                    WHILE current_date_val <= end_date LOOP
                        partition_name := 'fct_trips_' || TO_CHAR(current_date_val, 'YYYY_MM');
                        
                        EXECUTE format('
                            CREATE TABLE IF NOT EXISTS {{ this.schema }}.%I 
                            PARTITION OF {{ this.schema }}.fct_trips_new
                            FOR VALUES FROM (%L) TO (%L)',
                            partition_name,
                            current_date_val,
                            current_date_val + INTERVAL '1 month'
                        );
                        
                        RAISE NOTICE 'Created partition: % for dates % to %', 
                            partition_name, 
                            current_date_val, 
                            current_date_val + INTERVAL '1 month';
                        
                        current_date_val := current_date_val + INTERVAL '1 month';
                    END LOOP;
                    
                   
                    INSERT INTO {{ this.schema }}.fct_trips_new 
                    SELECT 
                        trip_key,
                        pickup_date,
                        pickup_date_key,
                        dropoff_date_key,
                        service_type_key,
                        vendor_key,
                        pu_zone_key,
                        do_zone_key,
                        payment_type_key,
                        passenger_count,
                        trip_distance,
                        fare_amount,
                        tip_amount,
                        tolls_amount,
                        total_amount,
                        trip_time_minutes,
                        tip_percentage,
                        speed_mph,
                        pickup_location_id,
                        dropoff_location_id,
                        payment_type_id,
                        vendor_id,
                        source_month,
                        service_type,
                        ingest_ts
                    FROM {{ this }};
                    
                    
                    DROP TABLE IF EXISTS {{ this }} CASCADE;
                    ALTER TABLE {{ this.schema }}.fct_trips_new RENAME TO fct_trips;
                    
                    RAISE NOTICE 'Successfully created RANGE partitioned fct_trips with monthly partitions from % to %', start_date, end_date;
                ELSE
                    RAISE NOTICE 'fct_trips already partitioned, skipping';
                END IF;
            END $$;
        {% endif %}"
    ]
) }}

WITH cleaned_trips AS (
    SELECT 
        
        COALESCE(t."VendorID"::INTEGER, 99) AS vendor_id_clean,
        COALESCE(t.payment_type_id, 5) AS payment_type_id_clean,
        
        
        t.pickup_ts,
        t.dropoff_ts,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.service_type,
        t.source_month,
        t.ingest_ts,
        
        
        COALESCE(t.passenger_count, 0) AS passenger_count,
        COALESCE(t.trip_distance, 0) AS trip_distance,
        COALESCE(t.fare_amount, 0) AS fare_amount,
        COALESCE(t.tip_amount, 0) AS tip_amount,
        COALESCE(t.tolls_amount, 0) AS tolls_amount,
        COALESCE(t.total_amount, 0) AS total_amount,
        COALESCE(t.trip_time_minutes, 0) AS trip_time_minutes
        
    FROM {{ ref('int_taxi_trips_zones_joined') }} t
    
    
    WHERE t.pickup_ts IS NOT NULL 
      AND t.dropoff_ts IS NOT NULL
      AND t.pickup_location_id IS NOT NULL
      AND t.dropoff_location_id IS NOT NULL
      AND t.pickup_ts <= t.dropoff_ts
      AND COALESCE(t.trip_distance, 0) >= 0
      AND COALESCE(t.total_amount, 0) >= 0
      AND COALESCE(t.fare_amount, 0) >= 0
      AND COALESCE(t.tip_amount, 0) >= 0
      AND COALESCE(t.tolls_amount, 0) >= 0
      AND COALESCE(t.passenger_count, 0) >= 0
      AND COALESCE(t.trip_time_minutes, 0) >= 0
      
      -- Business logic filters
      AND COALESCE(t.trip_distance, 0) > 0
      AND COALESCE(t.trip_time_minutes, 0) > 0
      AND COALESCE(t.total_amount, 0) > 0
      
      -- CRITICAL: Validate source_month matches pickup timestamp month
      AND TO_CHAR(t.pickup_ts, 'YYYY-MM') = t.source_month
),

joined_keys AS (
    SELECT 
        c.*,
        
        
        c.pickup_ts::DATE AS pickup_date,
        
        
        TO_CHAR(c.pickup_ts, 'YYYYMMDD')::INTEGER AS pickup_date_key,
        TO_CHAR(c.dropoff_ts, 'YYYYMMDD')::INTEGER AS dropoff_date_key,
        
       
        
        
        CASE c.service_type
            WHEN 'yellow' THEN 1
            WHEN 'green' THEN 2
            ELSE 2  
        END AS service_type_key,
        
        
        COALESCE(v.vendor_key, 5) AS vendor_key,  
        
       
        z_pu.zone_key AS pu_zone_key,
        z_do.zone_key AS do_zone_key,
        
       
        COALESCE(pt.payment_type_key, 5) AS payment_type_key, 
        

        c.vendor_id_clean AS vendor_id,
        c.payment_type_id_clean AS payment_type_id,
        
        
        CASE 
            WHEN c.fare_amount > 0 THEN c.tip_amount / c.fare_amount
            ELSE 0 
        END AS tip_percentage,
        
        CASE 
            WHEN c.trip_time_minutes > 0 THEN (c.trip_distance / (c.trip_time_minutes / 60))
            ELSE 0 
        END AS speed_mph
        
    FROM cleaned_trips c
    
    
    LEFT JOIN {{ ref('dim_vendors') }} v 
        ON c.vendor_id_clean = v.vendor_id
    
    LEFT JOIN {{ ref('dim_payment_type') }} pt 
        ON c.payment_type_id_clean = pt.payment_type_key
    
    LEFT JOIN {{ ref('dim_zones') }} z_pu 
        ON c.pickup_location_id = z_pu.zone_key
    
    LEFT JOIN {{ ref('dim_zones') }} z_do 
        ON c.dropoff_location_id = z_do.zone_key
)

SELECT 
    
    DENSE_RANK() OVER (
        ORDER BY 
            vendor_id,
            pickup_ts,
            dropoff_ts,
            pickup_location_id,
            dropoff_location_id,
            passenger_count,
            COALESCE(trip_distance, 0),
            COALESCE(total_amount, 0),
            service_type,
            source_month
    ) AS trip_key,
    
    pickup_date,
    pickup_date_key,
    dropoff_date_key,
    service_type_key,
    vendor_key,
    pu_zone_key,
    do_zone_key,
    payment_type_key,
    
    
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_time_minutes,
    
    
    ROUND(tip_percentage::NUMERIC, 4) AS tip_percentage,
    ROUND(speed_mph::NUMERIC, 2) AS speed_mph,
    
    
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    vendor_id,
    
   
    source_month,
    service_type,
    ingest_ts
    
FROM joined_keys


WHERE pu_zone_key IS NOT NULL 
  AND do_zone_key IS NOT NULL
  AND vendor_key IS NOT NULL
  AND payment_type_key IS NOT NULL