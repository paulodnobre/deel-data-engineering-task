-- Analytics Schema DDL
-- Purpose: Create dimensional (star) schema for analytics layer
-- Date: 2026-04-29
-- Idempotent: All CREATE statements use IF NOT EXISTS; safe to re-run

-- ============================================================================
-- SCHEMA SETUP
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Create analytics_owner role for future automation
CREATE ROLE IF NOT EXISTS analytics_owner NOLOGIN;
GRANT USAGE ON SCHEMA analytics TO analytics_owner;
GRANT CREATE ON SCHEMA analytics TO analytics_owner;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    delivery_date DATE NOT NULL PRIMARY KEY,
    day_of_week VARCHAR(10),
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_quarter VARCHAR(5),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id BIGINT NOT NULL PRIMARY KEY,
    product_name VARCHAR(500),
    barcode VARCHAR(26),
    unity_price DECIMAL,
    is_active BOOLEAN DEFAULT TRUE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id BIGINT NOT NULL PRIMARY KEY,
    customer_name VARCHAR(500),
    customer_address VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Order
CREATE TABLE IF NOT EXISTS analytics.dim_order (
    order_id BIGINT NOT NULL PRIMARY KEY,
    order_date DATE,
    status VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.fct_order_items (
    order_item_key BIGSERIAL NOT NULL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    delivery_date DATE NOT NULL,
    quantity_pending INTEGER NOT NULL,
    is_open BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Keys
    CONSTRAINT fk_fct_order FOREIGN KEY (order_id) REFERENCES analytics.dim_order(order_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fct_product FOREIGN KEY (product_id) REFERENCES analytics.dim_product(product_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fct_customer FOREIGN KEY (customer_id) REFERENCES analytics.dim_customer(customer_id) ON DELETE RESTRICT,
    CONSTRAINT fk_fct_date FOREIGN KEY (delivery_date) REFERENCES analytics.dim_date(delivery_date) ON DELETE RESTRICT,

    -- Check Constraints
    CONSTRAINT ck_quantity_positive CHECK (quantity_pending >= 0),
    CONSTRAINT ck_is_open_boolean CHECK (is_open IN (TRUE, FALSE))
);

-- ============================================================================
-- INDEXES ON FACT TABLE
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_fct_delivery_date ON analytics.fct_order_items(delivery_date);
CREATE INDEX IF NOT EXISTS idx_fct_order_id ON analytics.fct_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fct_product_id ON analytics.fct_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_fct_customer_id ON analytics.fct_order_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_fct_is_open ON analytics.fct_order_items(is_open);

-- ============================================================================
-- INDEXES ON DIMENSION TABLES (Natural Keys)
-- ============================================================================

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_product_natural ON analytics.dim_product(product_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer_natural ON analytics.dim_customer(customer_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_order_natural ON analytics.dim_order(order_id);

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_owner;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA analytics TO analytics_owner;

-- ============================================================================
-- NOTES
-- ============================================================================
--
-- Idempotency: All CREATE statements use IF NOT EXISTS. Safe to re-run.
--
-- Foreign Keys: ON DELETE RESTRICT prevents accidental dimension deletion.
--               Out-of-order CDC events handled by deferred constraint checking.
--
-- SCD2: is_current column added to dimensions for future upgrade (not used in MVP).
