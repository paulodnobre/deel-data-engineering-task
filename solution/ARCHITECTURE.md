# Deel Analytics Platform — Architecture

Comprehensive design documentation for the real-time analytics platform. This document describes the data flow, component responsibilities, scaling considerations, and high-availability strategies.

## Data Flow Pipeline

```
Flow:
1. Source PostgreSQL Database
   │
   └──→ Debezium CDC (pgoutput plugin)
       │ Capture INSERT/UPDATE/DELETE from: orders, order_items, products, customers
       │
       └──→ Kafka Topics (3 topics)
           ├─ order-changes
           ├─ product-changes
           └─ customer-changes
           │
           └──→ Python Pipeline Container
               ├─ Phase 1: Backfill (initial load from source DB)
               │  └─ Query public.orders, public.order_items, public.products, public.customers
               │  └─ Compute derived columns (is_open, quantity_pending)
               │  └─ Upsert into analytics schema (ON CONFLICT for idempotency)
               │
               └─ Phase 2: Consumer (streaming incremental updates)
                  └─ Poll Kafka for CDC events
                  └─ Parse Debezium JSON (before/after payloads, operation type)
                  └─ Route to correct upsert function (dim_order, dim_product, dim_customer, fct_order_items)
                  └─ Commit Kafka offset AFTER DB write (transactional safety)
               │
               └──→ Analytics PostgreSQL Schema
                   ├─ fct_order_items (fact table: 7M+ rows, grows daily)
                   │  Columns: order_item_id, order_id, product_id, customer_id,
                   │           delivery_date, quantity_pending, is_open, created_at, updated_at
                   │
                   ├─ dim_order (dimension: ~100k rows)
                   │  Columns: order_id, order_date, status, updated_at
                   │
                   ├─ dim_product (dimension: ~10k rows)
                   │  Columns: product_id, product_name, barcode, unity_price, is_active, updated_at
                   │
                   └─ dim_customer (dimension: ~50k rows)
                      Columns: customer_id, customer_name, customer_address, is_active, updated_at
               │
               └──→ FastAPI REST API (port 8000)
                   ├─ GET /analytics/orders — GROUP BY delivery_date, status COUNT
                   ├─ GET /analytics/orders/top — TOP N delivery_dates by order count
                   ├─ GET /analytics/orders/product — GROUP BY product_id SUM(quantity_pending)
                   ├─ GET /analytics/orders/customers — TOP N customers by pending order count
                   ├─ GET /health — DB connectivity check
                   └─ GET /docs, /redoc — OpenAPI documentation
               │
               └──→ Client Applications (browsers, dashboards, BI tools)
```

## Component Responsibilities

| Component | Purpose | Latency SLA | Failure Mode | Recovery |
|-----------|---------|-------------|-------------|----------|
| **PostgreSQL (Source)** | Single source of truth for operational data (orders, items, products, customers) | <10ms read | If down: backfill can't run; API queries fail; live changes not captured | Manual: wait for source DB restart; backfill re-runs on pipeline restart |
| **Debezium CDC** | Captures INSERT/UPDATE/DELETE from source DB; streams to Kafka via pgoutput plugin | ~1-2 sec lag | If down: new source changes not streamed; backfill still works; analytics become stale | Restart Debezium connector (via docker-compose restart); consumer resumes from last committed offset |
| **Kafka** | Event streaming; decouples source DB from analytics processing; provides replay capability | ~500ms-2s end-to-end | If down: new CDC events lost (until source re-streams); backfill still works; analytics frozen | Restart: `docker-compose restart zookeeper kafka`; Kafka replays from broker retention (7 days default) |
| **Zookeeper** | Kafka coordination; manages broker election and topic metadata | — | If down: Kafka unhealthy; no leader election | Restart: `docker-compose restart zookeeper` (first, before Kafka) |
| **Pipeline (Backfill)** | Initial population of analytics schema from source DB; idempotent (ON CONFLICT upsert) | <5 minutes (1M rows) | If fails: analytics schema empty; API returns 500 | Restart: `docker-compose restart pipeline` (backfill re-runs, skips already-inserted rows via upsert) |
| **Pipeline (Consumer)** | Continuous CDC streaming updates; manual offset management ensures no data loss | ~1-2 min lag (typical); <30s SLA | If down: analytics data stale; no new CDC events applied; backfill not affected | Restart: `docker-compose restart pipeline`; consumer resumes from last committed offset (Kafka broker stores this) |
| **Analytics PostgreSQL** | Analytical schema (star schema: fact + dimensions); optimized for queries, not writes | <100ms p95 query | If down: API returns 503; analytics unavailable | Data persists in named volume (pg-data); restart container to recover |
| **FastAPI** | REST API interface; queries analytics schema; returns JSON responses | <100ms p95 | If down: no API access; data intact in DB | Restart: `docker-compose restart api` |

## Database Schema Overview

### Star Schema Design

**Fact Table: `analytics.fct_order_items`**
- Grain: One row per order item (7M+ rows, grows 10k/day)
- Natural key: `order_item_id` (from source)
- Columns:
  - `order_item_id` (PK): Unique identifier
  - `order_id` (FK): Links to dim_order
  - `product_id` (FK): Links to dim_product
  - `customer_id` (FK): Links to dim_customer
  - `delivery_date` (DATE): Aggregation dimension
  - `quantity_pending` (INT, computed): Quantity if order open; 0 if closed
  - `is_open` (BOOLEAN, computed): 1 if order status != 'COMPLETED'; 0 otherwise
  - `created_at`, `updated_at` (TIMESTAMP): Audit columns

**Dimension Tables**

`analytics.dim_order` (100k rows):
- `order_id` (PK): From public.orders
- `order_date`, `status`, `updated_at`

`analytics.dim_product` (10k rows):
- `product_id` (PK): From public.products
- `product_name`, `barcode`, `unity_price`, `is_active`, `updated_at`

`analytics.dim_customer` (50k rows):
- `customer_id` (PK): From public.customers
- `customer_name`, `customer_address`, `is_active`, `updated_at`

## Kafka Topics and CDC Events

| Topic | Source Table | Events/sec | Retention | Format |
|-------|-------------|-----------|-----------|--------|
| `order-changes` | public.orders | 1-10 (typical) | 7 days | Debezium JSON with op, before, after, source |
| `product-changes` | public.products | 0-1 | 7 days | Debezium JSON |
| `customer-changes` | public.customers | 0-1 | 7 days | Debezium JSON |

**Debezium CDC Event Format:**
```json
{
  "op": "c|u|d",           // Operation: create, update, delete
  "before": { ... },       // Previous row state (null for INSERT)
  "after": { ... },        // New row state (null for DELETE)
  "source": {
    "table": "orders",
    "ts_ms": 1234567890000 // Timestamp in milliseconds
  }
}
```

## Scaling Considerations

### Horizontal Scaling (Multiple Pipeline Containers)
- **Barrier:** Kafka topic partitions (currently 3 topics × 1 partition each = 3 partitions total)
- **Action:** Increase partitions per topic: `kafka-topics --alter --topic order-changes --partitions 3`
- **Benefit:** Run 3+ consumers in different consumer groups; each consumes from different partitions
- **Deferred to Phase 5:** Requires testing to ensure order consistency within order_id (ordering guarantees)

### Vertical Scaling (Single Container, More Resources)
- **Batch Size:** Increase `BATCH_SIZE` in .env (default 1000 → 5000)
  - Impact: Fewer DB round trips; higher memory usage
  - Benefit: +3-5x throughput for backfill
- **Connection Pool:** Increase `DB_POOL_MAX_CONNECTIONS` (default 5 → 10)
  - Impact: More concurrent queries; higher DB memory
  - Benefit: Lower latency for high-volume API traffic

### Database Scaling
- **Indexes:** Add indexes on dim_* PK/FK columns if joins slow (currently fast <100ms)
  - Index: `analytics.dim_product(product_id)`, etc.
  - Benefit: Sub-100ms join queries
- **Materialized Views:** Cache expensive aggregations (e.g., top-N queries)
  - Syntax: `CREATE MATERIALIZED VIEW top_products_by_qty AS SELECT ... ORDER BY qty DESC LIMIT 100`
  - Refresh: `REFRESH MATERIALIZED VIEW top_products_by_qty` (via cron)

### API Scaling
- **Load Balancer:** Run multiple API containers behind Nginx or HAProxy
  - Containers are stateless (all state in DB); safe to run 3-5 replicas
  - Example: `docker-compose up --scale api=3`
  - Health check: `/health` endpoint detects DB connectivity

## High Availability & Disaster Recovery

### Backfill Idempotency
- **Mechanism:** `ON CONFLICT ... DO UPDATE` (natural key upsert)
- **Benefit:** Re-run backfill without duplication (safe after crashes, restarts)
- **Example:**
  ```sql
  INSERT INTO analytics.fct_order_items (order_item_id, ...) 
  VALUES (...) 
  ON CONFLICT (order_item_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
  ```

### Consumer Offset Management
- **Manual Commit:** Offset committed AFTER DB write (not before)
- **Benefit:** No data loss if consumer crashes mid-write
- **Auto-resume:** On restart, consumer resumes from last committed offset (broker-stored)
- **Failure Case:** If consumer crashes before offset commit, message re-delivered on restart

### Data Persistence
- **Named Volume:** PostgreSQL data stored in `pg-data` volume (survives container restarts)
- **Backup:** Volumes can be backed up via:
  ```bash
  docker run -v pg-data:/volume -v /backup:/backup alpine cp -r /volume /backup/pg-data
  ```

### Recovery Procedures

**If Analytics Schema Corrupted:**
```sql
DROP SCHEMA analytics CASCADE;
-- Re-apply schemas/analytics.sql DDL
-- Restart pipeline to re-run backfill
```

**If Kafka Offset Stuck:**
```bash
# Consumer will auto-resume from last committed offset on restart
docker-compose restart pipeline
# If still stuck, reset Kafka offsets manually (Phase 5)
```

**If Pipeline Container Crashes:**
```bash
docker-compose logs pipeline | grep -i error
docker-compose restart pipeline
```

## Known Limitations (Deferred to Phase 5)

- **Single Consumer:** No horizontal scaling without topic repartitioning (1 consumer per topic)
- **No Secrets Management:** .env file suitable for sandbox; production requires Vault/AWS Secrets Manager/GCP Secret Manager
- **No TLS:** Network traffic unencrypted; production requires reverse proxy (Nginx, Traefik) with HTTPS
- **No Monitoring:** No Prometheus metrics; logging via `docker logs` only; Phase 5 to add Grafana dashboard
- **No Multi-AZ Deployment:** Single Docker Compose instance; production requires Kubernetes/Docker Swarm for HA

## Latency SLAs

- **API Queries:** <100ms p95 (typical: 20-50ms)
- **Backfill:** <5 minutes for 1M rows (typical: 1-2 min depending on DB size)
- **Consumer Lag:** ~1-2 minutes typical; <30 seconds SLA if batch size optimal
- **Source-to-Analytics Latency:** ~1-3 minutes (1s CDC → 2min consumer processing)

## References

- Debezium Documentation: https://debezium.io/documentation/reference/
- Kafka Offset Management: https://kafka.apache.org/documentation/#consumerconfigs
- PostgreSQL Star Schema: https://en.wikipedia.org/wiki/Star_schema
- FastAPI Deployment: https://fastapi.tiangolo.com/deployment/

