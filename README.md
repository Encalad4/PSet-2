# PSet-2

## Project Overview

This project implements an end-to-end data pipeline for NYC TLC Trip Record Data (Yellow and Green taxis) using modern data engineering tools. The pipeline follows the medallion architecture (Bronze-Silver-Gold) and is fully orchestrated with Mage, with data transformations powered by dbt, and stored in PostgreSQL.

### Key Technologies
- **Orchestration**: Mage AI
- **Data Transformation**: dbt (with dbt-postgres)
- **Database**: PostgreSQL (Docker)
- **Containerization**: Docker + Docker Compose
- **Analysis**: Jupyter Notebook

---

## 1. Architecture

### Medallion Architecture Implementation

```
┌─────────────────────────────────────────────────┐
│            ORCHESTRATION LAYER                   │
│  Schema: orchestration                           │
│  ┌─────────────────────────────────────────────┐ │
│  │ ingestion_checkpoint                         │ │
│  │ • Tracks all ingestion jobs                  │ │
│  │ • Fields: year_month, service_type, status,  │ │
│  │   rows_ingested, retry_count, error_message  │ │
│  │ • Enables idempotency & backfill coordination│ │
│  └─────────────────────────────────────────────┘ │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│               BRONZE LAYER                       │
│  Schema: analytics_bronze                         │
│  ┌─────────────────────────────────────────────┐ │
│  │ stg_taxi_trips_bronze                        │ │
│  │ • Raw trip data from source                  │ │
│  │ • VendorID, pickup_ts, dropoff_ts            │ │
│  │ • passenger_count, trip_distance             │ │
│  │ • pickup/dropoff_location_id                 │ │
│  │ • rate_code_id, payment_type_id              │ │
│  │ • fare/tip/tolls/total_amount                │ │
│  │ • trip_type                                   │ │
│  │ • METADATA: ingest_ts, source_month,         │ │
│  │   service_type, trip_time_minutes            │ │
│  └─────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────┐ │
│  │ stg_zones                                    │ │
│  │ • Taxi zone lookup data                      │ │
│  │ • location_id, borough, zone_name,           │ │
│  │   service_zone                               │ │
│  └─────────────────────────────────────────────┘ │
└─────────────────────┬───────────────────────────┘
                      │ dbt run --select silver
                      ▼
┌─────────────────────────────────────────────────┐
│               SILVER LAYER                       │
│  Schema: analytics_silver                        │
│  Materialization: VIEWS                          │
│  ┌─────────────────────────────────────────────┐ │
│  │ int_taxi_trips_zones_joined                  │ │
│  │ • Joins trips with zones for enrichment      │ │
│  │ • Data quality rules applied:                │ │
│  │   - pickup/dropoff_ts NOT NULL               │ │
│  │   - pickup_ts <= dropoff_ts                  │ │
│  │   - trip_distance >= 0                       │ │
│  │   - total_amount >= 0                        │ │
│  │   - Valid location IDs                       │ │
│  │ • Enriched with zone info for both           │ │
│  │   pickup and dropoff locations               │ │
│  └─────────────────────────────────────────────┘ │
└─────────────────────┬───────────────────────────┘
                      │ dbt run --select gold
                      ▼
┌─────────────────────────────────────────────────┐
│                GOLD LAYER                        │
│  Schema: analytics_gold                          │
│  Materialization: TABLES (partitioned)           │
│                                                   │
│  ┌─────────────────────────────────────────────┐ │
│  │              STAR SCHEMA                     │ │
│  │                                              │ │
│  │         ┌───────────────┐                    │ │
│  │         │   dim_date    │                    │ │
│  │         │ date_key      │                    │ │
│  │         │ date, year    │                    │ │
│  │         │ month, day    │                    │ │
│  │         │ month_name    │                    │ │
│  │         │ day_name      │                    │ │
│  │         └───────┬───────┘                    │ │
│  │                 │                            │ │
│  │                 ▼                            │ │
│  │  ┌───────────────┐  ┌───────────────┐       │ │
│  │  │   dim_zones   │  │   fct_trips   │       │ │
│  │  │   (pickup)    │◄─┤(RANGE PART)   ├─┐     │ │
│  │  │ zone_key      │  │ trip_key      │ │     │ │
│  │  │ borough       │  │ pickup_date   │ │     │ │
│  │  │ zone_name     │  │ pu_zone_key   │ │     │ │
│  │  │ service_zone  │  │ do_zone_key   │ │     │ │
│  │  └───────────────┘  │ service_key   │ │     │ │
│  │                     │ vendor_key    │ │     │ │
│  │  ┌───────────────┐  │ payment_key   │ │     │ │
│  │  │   dim_zones   │  │ passenger_cnt │ │     │ │
│  │  │   (dropoff)   │◄─┤ trip_distance │ │     │ │
│  │  │ zone_key      │  │ fare_amount   │ │     │ │
│  │  │ borough       │  │ tip_amount    │ │     │ │
│  │  │ zone_name     │  │ total_amount  │ │     │ │
│  │  │ service_zone  │  │ tip_percentage│ │     │ │
│  │  └───────────────┘  │ speed_mph     │ │     │ │
│  │                     │ source_month  │ │     │ │
│  │                     └───────┬───────┘ │     │ │
│  │                             │         │     │ │
│  │          ┌──────────────────┼─────────┼─────┘ │
│  │          ▼                  ▼         ▼       │ │
│  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│  │  │dim_service_type│ │dim_payment_type││ dim_vendors    │
│  │  │(LIST PART)    │ │(LIST PART)    ││               │
│  │  │service_key    │ │payment_key     ││ vendor_key    │
│  │  │service_name   │ │payment_name    ││ vendor_id     │
│  │  │service_code   │ │partition_group ││ vendor_name   │
│  │  │partition_group│ └───────────────┘│ vendor_code   │
│  │  └───────────────┘                   │ vendor_group  │
│  │                                     │ description   │
│  │                                     └───────────────┘
│  └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

---

## Layer Details

### Orchestration Layer (`orchestration`)

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `ingestion_checkpoint` | `year_month`, `service_type`, `status`, `rows_ingested`, `retry_count`, `error_message` | Tracks all ingestion jobs for idempotency and monitoring |

---

### Bronze Layer (`analytics_bronze`)

#### `stg_taxi_trips_bronze`
Raw trip data with metadata:
- **Source fields**: `VendorID`, `pickup_ts`, `dropoff_ts`, `passenger_count`, `trip_distance`, `pickup_location_id`, `dropoff_location_id`, `rate_code_id`, `payment_type_id`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `trip_type`
- **Metadata**: `ingest_ts`, `source_month`, `service_type`, `trip_time_minutes`

#### `stg_zones`
Zone lookup data:
- `location_id`, `borough`, `zone_name`, `service_zone`

---

### Silver Layer (`analytics_silver`) - *Materialized as VIEWS*

#### `int_taxi_trips_zones_joined`
Enriched trips with quality checks:
- All bronze fields preserved
- Quality rules enforced (non-null timestamps, valid ranges)
- Enriched with pickup/dropoff borough and zone names

---

### Gold Layer (`analytics_gold`) - *Materialized as TABLES*

#### Fact Table: `fct_trips` *(RANGE partitioned by pickup_date)*
- **Keys**: `trip_key`, `pickup_date_key`, `dropoff_date_key`, `service_type_key`, `vendor_key`, `pu_zone_key`, `do_zone_key`, `payment_type_key`
- **Measures**: `passenger_count`, `trip_distance`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`
- **Calculated**: `tip_percentage`, `speed_mph`, `trip_time_minutes`
- **Metadata**: `pickup_date`, `source_month`, `service_type`, `ingest_ts`

#### Dimension Tables

| Table  | Key Columns |
|-------|-------------|
| `dim_date` | `date_key`, `date`, `year`, `month`, `month_name`, `day`, `day_of_week`, `day_name`, `year_month` |
| `dim_zones` | `zone_key`, `borough`, `zone_name`, `service_zone` |
| `dim_service_type` | `service_type_key`, `service_type_name`, `service_type_code`, `partition_group` |
| `dim_payment_type` | `payment_type_key`, `payment_type_name`, `partition_group` |
| `dim_vendors` | `vendor_key`, `vendor_id`, `vendor_name`, `vendor_code`, `vendor_group`, `description` |

---

## Partitioning Strategy

| Table | Partition Type | Partition Key | Details |
|-------|----------------|---------------|---------|
| `fct_trips` | RANGE | `pickup_date` | Monthly partitions (2024-01, 2024-02, etc.) |
| `dim_zones` | HASH | `zone_key` | 4 partitions (modulus 4) |
| `dim_service_type` | LIST | `partition_group` | Partitions: 'yellow', 'green' |
| `dim_payment_type` | LIST | `partition_group` | Partitions: 'cash', 'card', 'other/unknown' |
---
## Docker compose
```yml
services:
  postgres:
    container_name: postgres_container_pst_2
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - ./postgres_volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
  
  mage:
    container_name: mage_container_pst_2
    image: mageai/mageai:latest
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "6789:6789"
    environment:
      MAGE_DATABASE_CONNECTION_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}        
    volumes:
      - ./mage_ai_volume:/home/src

  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: pgadmin_container_pst_2
    environment:
      PGADMIN_DEFAULT_EMAIL: ${PG_ADMIN_EMAIL}
      PGADMIN_DEFAULT_PASSWORD: ${PG_ADMIN_PASSWORD}
    ports:
      - "8080:80"
    volumes:
      - ./pgadmin_volume:/var/lib/pgadmin

    depends_on:
      - postgres


      
```
---
## Mage secrets
![Mage Secrets](./evidence/mage_secrets.png)

---

## `ingest_bronze`
This evidence shows that the pipeline dinamically inspects the ingestion_checkpoint table to see if the data exists, and it it exists and it is on a middle stage (meaning it didn't finish correctly), it deletes the data and then uploads it, and if it didn't exist, it just uploads the data avoiding duplicates. 
![Evidence idempotence](./evidence/idempotencia_ingest_bronze.png)
---
### Tabla de Cobertura Implementada

![Tabla de Cobertura](./evidence/tabla_de_cobertura_1.png)
![Tabla de Cobertura 2](./evidence/coverage_table.png)

---

## `dbt_build_silver` y `dbt_build_gold`
```yml
models:
  scheduler_transformations:
    bronze:
      +materialized: table
      +schema: bronze
    silver:
      +materialized: view
      +schema: silver
    gold:
      +materialized: table
      +schema: gold
```
![materialized views silver](./evidence/materialized_silver.png)
![materialized table gold](./evidence/materialized_gold.png)

---

## Quality Checks
```yml
version: 2

models:
  - name: fct_trips
    description: 'Fact table containing taxi trips with one row per trip'
    columns:
      - name: trip_key
        description: 'Surrogate key for the trip'
        tests:
          - unique
          - not_null
      
      - name: pickup_date_key
        description: 'Foreign key to dim_date for pickup'
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      
      - name: dropoff_date_key
        description: 'Foreign key to dim_date for dropoff'
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      
      - name: service_type_key
        description: 'Foreign key to dim_service_type'
        tests:
          - not_null
          - relationships:
              to: ref('dim_service_type')
              field: service_type_key
      
      - name: vendor_key
        description: 'Foreign key to dim_vendors'
        tests:
          - not_null
          - relationships:
              to: ref('dim_vendors')
              field: vendor_key
      
      - name: pu_zone_key
        description: 'Foreign key to dim_zones for pickup location'
        tests:
          - not_null
          - relationships:
              to: ref('dim_zones')
              field: zone_key
      
      - name: do_zone_key
        description: 'Foreign key to dim_zones for dropoff location'
        tests:
          - not_null
          - relationships:
              to: ref('dim_zones')
              field: zone_key
      
      - name: payment_type_key
        description: 'Foreign key to dim_payment_type'
        tests:
          - not_null
          - relationships:
              to: ref('dim_payment_type')
              field: payment_type_key
      
      - name: passenger_count
        description: 'Number of passengers'
        tests:
          - not_null
          - accepted_values:
              values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      
      - name: trip_distance
        description: 'Trip distance in miles'
        tests:
          - not_null
      
      - name: fare_amount
        description: 'Fare amount'
        tests:
          - not_null
      
      - name: tip_amount
        description: 'Tip amount'
        tests:
          - not_null
      
      - name: tolls_amount
        description: 'Tolls amount'
        tests:
          - not_null
      
      - name: total_amount
        description: 'Total amount'
        tests:
          - not_null
      
      - name: trip_time_minutes
        description: 'Trip duration in minutes'
        tests:
          - not_null
      
      - name: source_month
        description: 'Source month in YYYY-MM format'
        tests:
          - not_null
      
      - name: service_type
        description: 'Type of taxi service'
        tests:
          - not_null
          - accepted_values:
              values: ['yellow', 'green']
              
  - name: dim_date
    description: 'Date dimension'
    columns:
      - name: date_key
        description: 'Surrogate key in YYYYMMDD format'
        tests:
          - unique
          - not_null
      - name: date
        tests:
          - unique
          - not_null

  - name: dim_zones
    description: 'Zone dimension with HASH partitioning'
    columns:
      - name: zone_key
        description: 'Surrogate key'
        tests:
          - unique
          - not_null
      - name: borough
        tests:
          - not_null
      - name: zone_name
        tests:
          - not_null

  - name: dim_service_type
    description: 'Service type dimension with LIST partitioning'
    columns:
      - name: service_type_key
        tests:
          - unique
          - not_null
      - name: service_type_code
        tests:
          - not_null
          - accepted_values:
              values: ['yellow', 'green']

  - name: dim_payment_type
    description: 'Payment type dimension with LIST partitioning'
    columns:
      - name: payment_type_key
        tests:
          - unique
          - not_null
      - name: payment_type_name
        tests:
          - not_null
      - name: partition_group
        tests:
          - not_null
          - accepted_values:
              values: ['cash', 'card', 'flex', 'other/unknown']

  - name: dim_vendors
    description: 'Vendor dimension with LIST partitioning'
    columns:
      - name: vendor_key
        tests:
          - unique
          - not_null
      - name: vendor_id
        tests:
          - unique
          - not_null
      - name: vendor_name
        tests:
          - not_null
      - name: partition_group
        tests:
          - not_null
          - accepted_values:
              values: ['creative', 'curb', 'myle', 'helix', 'other']
```
![Test pass 1](./evidence/test_run_dim_payment_type.png)


---

## Particionamiento

### **RANGE** en `fct_trips`
![RANGE fct_trips](./evidence/d+_fct_trips.png)
![RANGE fct_trips](./evidence/partition_list_fct_trips.png)
### **HASH** en `dim_zone`
![RANGE fct_trips](./evidence/d+_dim_zones.png)
### **LIST** en `dim_service_type`
![RANGE fct_trips](./evidence/d+_dim_service_type.png)
### **LIST** en `dim_payment_type`
![RANGE fct_trips](./evidence/d+_dim_payment_type.png)

---
## Partition Pruning
### fct_trips
![RANGE fct_trips](./evidence/partition_pruning_1_fct_trips.png)
---
![RANGE fct_trips](./evidence/partition_pruning_2_fct_trips.png)

### dim_zones
![partition prunning dim zones](./evidence/partition_pruning_dim_zones.png)

### dim_payment_type

![partition prunning dim payment type](./evidence/partition_pruning_dim_payment_typee.png)

### dim_service_type

![partition prunning dim service type](./evidence/partition_pruning_dim_service_type.png)

---

### tests
![dbt tests for fct_trips](./evidence/test_fct_trips.png)

### Notebook answers
To see the code, results and interpretation for each businness question, press [here](./queries.ipynb)

Here are some of the questions and their answers.

### Viajes por mes (2024)
```python

query_q1 = """
SELECT 
    d.year_month,
    d.month_name,
    COUNT(*) AS trip_count
FROM analytics_gold.fct_trips f
JOIN analytics_gold.dim_date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year_month, d.month_name, d.month
ORDER BY d.month;
"""

try:
    df_q1 = pd.read_sql(query_q1, engine)
    print("Question 1 - Viajes por mes (2024)")
    print("=" * 50)
    print(f"Total rows: {len(df_q1)}")
    print("\nResults:")
    display(df_q1)
    

except Exception as e:
    print(f" Error: {e}")
```

### Octubre fue el mes con mayor cantidad de viajes en 2024 (3.74M), mientras que agosto registró la menor actividad (2.92M). Se observa un patrón estacional con picos en mayo y octubre, y una caída durante los meses de verano (julio-agosto).


## 8. Ingreso total (total_amount) por mes.
```python
query_q8 = """
SELECT 
    d.year_month,
    d.month_name,
    SUM(f.total_amount) AS total_revenue
FROM analytics_gold.fct_trips f
JOIN analytics_gold.dim_date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year_month, d.month_name, d.month
ORDER BY d.month;
"""

try:
    df_q8 = pd.read_sql(query_q8, engine)
    print(" Question 8 - Ingreso total (total_amount) por mes")
    print("=" * 50)
    print(f"Total rows: {len(df_q8)}")
    print("\n Results:")
    display(df_q8)
except Exception as e:
    print(f" Error: {e}")
```
### Octubre fue el mes con mayor ingreso ($109.5M), seguido por Mayo ($106.3M) y Septiembre ($104.1M). Los meses de verano (julio-agosto) muestran una caída significativa en ingresos, consistente con la menor cantidad de viajes en ese período. El ingreso mensual promedio en 2024 fue de aproximadamente $96M. La tendencia sigue el mismo patrón que los viajes, con un crecimiento hacia fin de año y una recuperación notable en septiembre-octubre.


## 16. Distancia promedio por mes
```python
query_q16 = """
SELECT 
    d.year_month,
    d.month_name,
    ROUND(AVG(f.trip_distance)::NUMERIC, 2) AS avg_distance_miles
FROM analytics_gold.fct_trips f
JOIN analytics_gold.dim_date d ON f.pickup_date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year_month, d.month_name, d.month
ORDER BY d.month;
"""

try:
    df_q16 = pd.read_sql(query_q16, engine)
    print(" Question 16 - Distancia promedio por mes")
    print("=" * 50)
    print(f"Total rows: {len(df_q16)}")
    print("\n Results:")
    display(df_q16)
except Exception as e:
    print(f" Error: {e}")
```

### La distancia de los viajes muestra una marcada estacionalidad. Los primeros meses del año (enero-febrero) tienen los viajes más cortos (4.2 millas), mientras que septiembre registra la mayor distancia promedio (6 millas), un incremento del 42% respecto a febrero.

### Esto puede significar:

### Viajes a aeropuertos: El aumento en distancia coincide con meses de mayor actividad de aeropuertos (primavera-otoño)

### Turismo: Visitantes que realizan viajes más largos desde/hacia aeropuertos y zonas turísticas

### Clima: Mejores condiciones climáticas en primavera y otoño que facilitan viajes más largos

### La distancia promedio se mantiene elevada (5.2-5.6 millas) durante prácticamente todo el año excepto el primer bimestre, indicando que los viajes cortos dominan solo en temporada baja de invierno.


---

## Triggers

This trigger runs once a week, and tries to fetch the current month-year from the data.  
![Trigger configuration](./evidence/trigger_configuration.png)

And this is a list of all the available triggers. 
It is important to note that the backfill trigger runs only once when prompted. And it looks for the missing data in the database and downloads it. So if it is run with an empty database, it will create all the schemas and tables and fetch the data. 
![Trigger configuration](./evidence/trigger_evidence.png)


### Logs
![Logs de funcionamiento](./evidence/Backfill%20performing.png)



### 

# Checklist del Proyecto

## Infraestructura y Configuración
- [X] Docker Compose configurado para levantar:
  - [X] PostgreSQL
  - [X] Mage
- [X] Credenciales manejadas de forma segura:
  - [X] Mage Secrets configuradas
  - [X] Archivo `.env` con variables sensibles (no versionado)
  - [X] Solo `.env.example` incluido en el repositorio

## Pipeline de Ingesta
- [X] Pipeline `ingest_bronze`:
  - [X] Ejecución mensual configurada
  - [X] Idempotente (puede ejecutarse múltiples veces sin duplicar datos)
  - [X] Tabla de cobertura implementada para tracking de datos ingestados

## Modelado con dbt (ejecutado desde Mage)
- [X] `dbt_build_silver`:
  - [X] Materializado como **views**
- [X] `dbt_build_gold`:
  - [X] Materializado como **tables**
  - [X] Esquema estrella completo implementado:
    - [X] Tabla de hechos (`fct_trips`)
    - [X] Dimensiones correspondientes
- [X] `quality_checks`:
  - [X] Tests de dbt implementados y pasando desde Mage

## Particionamiento en PostgreSQL
- [X] Particionamiento **RANGE** en `fct_trips`
- [X] Particionamiento **HASH** en `dim_zone`
- [X] Particionamiento **LIST** en `dim_service_type`
- [X] Particionamiento **LIST** en `dim_payment_type`

## Documentación en README
- [X] Comando `\d+` para cada tabla particionada
- [X] Comando `EXPLAIN (ANALYZE, BUFFERS)` para consultas que demuestren:
  - [X] Partition pruning efectivo en `fct_trips`
  - [X] Partition pruning efectivo en dimensiones con HASH/LIST

## Validaciones y Análisis
- [X] `dbt test` ejecutado desde Mage y pasando exitosamente
- [X] Notebook con respuestas a **20 preguntas de negocio**:
  - [X] Consultas realizadas exclusivamente sobre esquema `gold.*`
  - [X] Respuestas validadas y documentadas

## Triggers
- [X] Triggers configurados en base de datos
- [X] Evidencia de triggers funcionando (logs, capturas, o ejemplos)