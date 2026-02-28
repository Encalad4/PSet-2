-- Docs: https://docs.mage.ai/guides/sql-blocks
DROP TABLE IF EXISTS analytics_gold.dim_payment_type CASCADE;

CREATE TABLE gold.dim_payment_type (
    payment_type_key  INTEGER NOT NULL PRIMARY KEY,
    payment_type_name TEXT    NOT NULL,
    partition_group   TEXT    NOT NULL
) PARTITION BY LIST (partition_group);

-- Crear particiones por grupo
CREATE TABLE gold.dim_payment_type_cash
    PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('cash');

CREATE TABLE gold.dim_payment_type_card
    PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('card');

CREATE TABLE gold.dim_payment_type_flex
    PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('flex');

CREATE TABLE gold.dim_payment_type_other
    PARTITION OF gold.dim_payment_type
    FOR VALUES IN ('other/unknown');