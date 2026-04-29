# Deel Analytics Platform - Functional Contract

**Date:** 2026-04-28  
**Status:** APPROVED  
**Version:** 1.0

---

## Definition: "Open Order"

An **open order** is any order in the `operations.orders` table where the `status` field is **not equal to 'COMPLETED'**. This includes orders with status values: `PENDING`, `PROCESSING`, and `REPROCESSING`. There are no exclusions for cancelled, returned, or failed items — the authoritative source of order state is the `status` field itself.

**Key Assumption:** The `status` field is single-valued and authoritative. No multi-valued or implicit order states are assumed.

---

## Definition: "Pending Items"

**Pending items** are the sum of quantities from `operations.order_items` for all items belonging to orders where `orders.status <> 'COMPLETED'`. The calculation uses the source column name `quanity` (noted misspelling; dimensional schema will normalize to `quantity` in Phase 2).

**Grain:** Aggregations can be per product, per customer, or both, depending on the API endpoint. No pre-aggregation is required — fact table grain supports all requested levels.

**Key Assumption:** Quantity is always positive; no negative adjustments or cancellations in the source data.

---

## Endpoint-to-SQL Mapping

### 1. GET /analytics/orders?status=open
**Purpose:** List open orders by delivery date and status  
**Contract:** Returns count of open orders grouped by delivery_date and status

```sql
SELECT
    delivery_date,
    status,
    COUNT(*) as order_count
FROM operations.orders
WHERE status <> 'COMPLETED'
GROUP BY delivery_date, status
ORDER BY delivery_date DESC;
```

---

### 2. GET /analytics/orders/top?limit=3
**Purpose:** Top 3 delivery dates by open order count  
**Contract:** Returns delivery_date and order_count sorted descending

```sql
SELECT
    delivery_date,
    COUNT(*) as order_count
FROM operations.orders
WHERE status <> 'COMPLETED'
GROUP BY delivery_date
ORDER BY order_count DESC
LIMIT 3;
```

---

### 3. GET /analytics/orders/product
**Purpose:** Pending items by product  
**Contract:** Returns product_id, product_name, and total quantity_pending across all open orders

```sql
SELECT
    oi.product_id,
    p.product_name,
    SUM(oi.quanity) as quantity_pending
FROM operations.order_items oi
JOIN operations.orders o ON oi.order_id = o.order_id
JOIN operations.products p ON oi.product_id = p.product_id
WHERE o.status <> 'COMPLETED'
GROUP BY oi.product_id, p.product_name
ORDER BY quantity_pending DESC;
```

---

### 4. GET /analytics/orders/customers?status=open&limit=3
**Purpose:** Top 3 customers by count of open orders  
**Contract:** Returns customer_id, customer_name, pending_order_count (distinct order count)

```sql
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT o.order_id) as pending_order_count
FROM operations.orders o
JOIN operations.customers c ON o.customer_id = c.customer_id
WHERE o.status <> 'COMPLETED'
GROUP BY c.customer_id, c.customer_name
ORDER BY pending_order_count DESC
LIMIT 3;
```

---

# Data Quality Notes

The source has customers, products, orders, and order_items tables. Here's what we found:

- `orders.status` — no NULLs (safe to filter on)
- `orders.customer_id` — can be NULL (orphaned orders; we'll handle gracefully)
- `orders.delivery_date` — can be NULL (fine, we allow it in the model)
- `order_items.quanity` — can be NULL (treat as zero in sums)
- Foreign keys are assumed valid

The source uses exactly 4 status values: `PENDING`, `PROCESSING`, `REPROCESSING`, `COMPLETED`.

---

## What This Means for the Schema

The fact table will be grain = order_item (one row per item). We'll have dimensions for order, product, customer, and date. This supports all four endpoints without needing pre-aggregation.

Known quirks to handle during load:
- Normalize `quanity` → `quantity` 
- Convert NULL quantities to 0
- Skip or mark orphaned orders (depends on final decision)
- Validate status values before they hit the consumer

---

**Contract is locked. Changes require phase review.**

---

**Created by:** Phase 1 Planning  
**Last Modified:** 2026-04-29
