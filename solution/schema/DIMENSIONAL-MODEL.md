# Dimensional Schema Design — Analytics Platform

**Date:** 2026-04-29 | **Status:** APPROVED | **Version:** 1.0

---

## Star Schema Overview

The analytics layer uses a classic **star schema** architecture optimized for OLAP queries. The fact table `fct_order_items` contains one row per source order item, joined to four dimension tables (`dim_order`, `dim_product`, `dim_customer`, `dim_date`). This design supports the four analytic queries without requiring pre-aggregation.

**Why Star Schema?**
- Denormalized dimensions enable fast joins with selective aggregations
- Single fact table grain (order_item) matches source granularity
- Separate schema (`analytics`) isolates analytical workload from transactional source
- Natural keys on dimensions enable idempotent upserts during load

---

## Fact Table: `fct_order_items`

**Grain:** One row per `order_item` from source (`operations.order_items`)

**Rationale:** The source order_items table is the finest granularity of business event. Fact table grain matches this directly, enabling accurate aggregations across all dimensions. This supports pending items SUM, open order counts, and time-based analysis.

**Columns:**

| Column | Type | Constraint | Purpose |
|--------|------|-----------|---------|
| `order_item_key` | BIGSERIAL | PK | Surrogate primary key |
| `order_id` | BIGINT | FK → dim_order, NOT NULL | Links to order dimension |
| `product_id` | BIGINT | FK → dim_product, NOT NULL | Links to product dimension |
| `customer_id` | BIGINT | FK → dim_customer, NOT NULL | Links to customer dimension |
| `delivery_date` | DATE | FK → dim_date, NOT NULL | Links to date dimension; used for GROUP BY |
| `quantity_pending` | INTEGER | NOT NULL, CHECK >= 0 | Sum of quantities (normalized from source typo) |
| `is_open` | BOOLEAN | NOT NULL | Cached flag: TRUE if order.status <> 'COMPLETED' |
| `created_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Row creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Last update timestamp |

**Indexes:**
- `idx_fct_delivery_date` — Supports GROUP BY delivery_date
- `idx_fct_order_id` — Supports JOINs and filtering
- `idx_fct_product_id` — Supports product-level aggregations
- `idx_fct_customer_id` — Supports customer-level aggregations
- `idx_fct_is_open` — Supports WHERE is_open = TRUE filters
- Plus PK and FK indexes (implicit)

---

## Dimension Tables

### `dim_order`

| Column | Type | Purpose |
|--------|------|---------|
| `order_id` | BIGINT PK | Natural key from source |
| `order_date` | DATE | Order creation date |
| `status` | VARCHAR(50) | Order status (PENDING, PROCESSING, COMPLETED, REPROCESSING) |
| `created_at` | TIMESTAMP | Dim creation time |
| `updated_at` | TIMESTAMP | Dim update time |

Provides clean denormalized access to order-level attributes. Status cached here for fast filtering.

---

### `dim_product`

| Column | Type | Purpose |
|--------|------|---------|
| `product_id` | BIGINT PK | Natural key from source |
| `product_name` | VARCHAR(500) | Product display name |
| `barcode` | VARCHAR(26) | Product barcode |
| `unity_price` | DECIMAL | Unit price (mirrors source naming) |
| `is_active` | BOOLEAN | Product status |
| `created_at` | TIMESTAMP | Dim creation time |
| `updated_at` | TIMESTAMP | Dim update time |

Required for product-level analytics queries. Denormalization avoids repeated source joins.

---

### `dim_customer`

| Column | Type | Purpose |
|--------|------|---------|
| `customer_id` | BIGINT PK | Natural key from source |
| `customer_name` | VARCHAR(500) | Customer display name |
| `customer_address` | VARCHAR(500) | Customer address |
| `is_active` | BOOLEAN | Customer status in source |
| `created_at` | TIMESTAMP | Dim creation time |
| `updated_at` | TIMESTAMP | Dim update time |

Required for customer-level analytics queries.

---

### `dim_date` (Optional)

| Column | Type | Purpose |
|--------|------|---------|
| `delivery_date` | DATE PK | Date value |
| `day_of_week` | VARCHAR(10) | Day name |
| `fiscal_quarter` | VARCHAR(5) | Fiscal quarter (Q1, Q2, Q3, Q4) |
| `created_at` | TIMESTAMP | Dim creation time |

Supports GROUP BY delivery_date without re-computing day-of-week attributes. Can be sparsely populated if not needed for MVP.

---

## Constraints & Data Integrity

**Primary Keys:** All dimensions use natural keys as PK. Fact table uses surrogate key (order_item_key).

**Foreign Keys (ON DELETE RESTRICT):**
- `fct_order_items.order_id` → `dim_order.order_id`
- `fct_order_items.product_id` → `dim_product.product_id`
- `fct_order_items.customer_id` → `dim_customer.customer_id`
- `fct_order_items.delivery_date` → `dim_date.delivery_date`

**Check Constraints:**
- `quantity_pending >= 0` — Prevents negative quantities
- `is_open IN (TRUE, FALSE)` — Ensures valid states

**NOT NULL Constraints:** order_id, product_id, customer_id, delivery_date, quantity_pending, is_open (all critical for aggregations).

---

## Schema Summary

| Component | Count |
|-----------|-------|
| Tables | 5 (1 fact, 4 dimensions) |
| Columns | ~35 |
| Primary Keys | 5 |
| Foreign Keys | 4 |
| Check Constraints | 2 |
| Explicit Indexes | 6 |
| Implicit Indexes | 5 |

---

**Design approved. Ready for implementation.**

**Created by:** Phase 1 Planning  
**Last Modified:** 2026-04-29