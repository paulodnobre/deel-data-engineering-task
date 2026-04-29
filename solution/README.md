# Deel Analytics Platform — Solution

Real-time analytics API for order fulfillment tracking, powered by PostgreSQL, Kafka CDC, and FastAPI.

## ⚡ Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Ports 5432 (PostgreSQL), 9092 (Kafka), 8000 (API) available

### Setup (2 minutes)

```bash
# 1. Create environment file
cp .env.example .env

# 2. Start all services
docker-compose up -d

# 3. Wait for health checks (watch output until all "Up")
watch docker-compose ps
# Press Ctrl+C when postgres, kafka, api all show "(healthy)"

# 4. Verify API
curl http://localhost:8000/health | jq .
# Expected: {"status": "ok", "db": "connected"}

# 5. Query sample data
curl http://localhost:8000/analytics/orders?status=open | jq . | head -20
```

**First time?** Read [PLAYBOOK.md](PLAYBOOK.md) §1 for detailed setup + verification checklist.

---

## Architecture

### Data Flow

```
Source DB (PostgreSQL)
  ↓
Debezium CDC → Kafka Topics (3 topics)
  ↓
Pipeline Container
  ├─ Phase 1: Backfill (initial load, idempotent)
  └─ Phase 2: Consumer (streaming CDC updates)
  ↓
Analytics Schema (star schema: 1 fact + 4 dimensions)
  ↓
FastAPI REST API (port 8000)
  ↓
Client Applications (dashboards, BI tools)
```

**Full details:** See [ARCHITECTURE.md](ARCHITECTURE.md) (data flow, component SLAs, scaling strategies)

---

## API Endpoints

All endpoints return JSON; test via curl or browser at `http://localhost:8000/docs` (Swagger UI).

| Endpoint | Description | Example |
|----------|-------------|---------|
| `GET /analytics/orders` | Orders grouped by delivery_date & status | `?status=open` → returns order counts by date |
| `GET /analytics/orders/top` | Top N delivery dates by order count | `?limit=5` → top 5 dates |
| `GET /analytics/orders/product` | Pending quantities by product | → aggregated inventory status |
| `GET /analytics/orders/customers` | Top customers by pending orders | `?status=open&limit=3` → top 3 customers |
| `GET /health` | Database connectivity check | → `{"status": "ok", "db": "connected"}` |

**Examples & full query syntax:** See [PLAYBOOK.md](PLAYBOOK.md) §4

---

## Project Structure

```
.
├── README.md                    # This file
├── ARCHITECTURE.md              # Technical design & scaling
├── PLAYBOOK.md                  # Operational procedures
├── docker-compose.yml           # Service definitions
├── .env.example                 # Environment template
├── requirements.txt             # Python dependencies
├── Dockerfile.pipeline          # Pipeline container
├── Dockerfile.api               # API container
│
├── pipeline/
│   ├── main.py                 # Backfill + consumer startup
│   ├── backfill.py             # Initial data load (idempotent)
│   ├── consumer.py             # Kafka CDC streaming consumer
│   └── utils.py                # Helper functions
│
├── api/
│   ├── main.py                 # FastAPI application & endpoints
│   ├── config.py               # Configuration (env vars)
│   ├── db.py                   # Database pooling & queries
│   ├── models.py               # Pydantic request/response schemas
│   └── __init__.py
│
├── tests/
│   ├── conftest.py             # pytest fixtures & test DB setup
│   ├── test_endpoints.py       # Integration tests (43 tests)
│   ├── test_validation.py      # Validation unit tests (36 tests)
│   ├── test_error_handling.py  # Error scenario tests (26 tests)
│   └── __init__.py
│
└── schemas/
    ├── analytics.sql           # Analytics DDL (5 tables, 8 indexes, 4 FKs, 2 checks)
    └── contract-validation-queries.sql  # 21 audit queries
```

---

## Features

✅ **Real-time Analytics API**
- 4 REST endpoints for order/product/customer analytics
- Query parameters with automatic validation
- OpenAPI documentation (auto-generated at `/docs`)

✅ **Hybrid Data Pipeline**
- Idempotent backfill (initial load from source DB)
- Streaming consumer (Kafka CDC updates)
- Transactional safety (offset committed after DB write)

✅ **Production-Ready Design**
- Star schema (1 fact table + 4 dimensions)
- Connection pooling (psycopg2, tunable)
- Parameterized queries (SQL injection protection)
- Error handling (422 validation, 500 DB, 404 not found)
- Health checks (for monitoring)

✅ **Comprehensive Testing**
- 119 automated tests (36 validation + 43 integration + 26 error handling + 11 consumer + 3 backfill)
- pytest fixtures with test database
- All endpoints + error paths + pipeline components covered

✅ **Operational Documentation**
- Step-by-step setup procedures
- Debugging guide (5 common issues)
- Recovery procedures (3 scenarios)
- Performance tuning guide

---

## Requirements Met

| Requirement | Status | Evidence |
|---|---|---|
| REST API with 4+ endpoints | ✅ | `/analytics/orders`, `/top`, `/product`, `/customers`, `/health` |
| Query validation | ✅ | Pydantic models with ranges, defaults, coercion |
| Error handling | ✅ | 422 (validation), 500 (DB), 404 (not found) |
| Comprehensive testing | ✅ | 119 tests (36 validation, 43 integration, 26 error handling, 11 consumer, 3 backfill) |
| Database schema (star) | ✅ | 1 fact + 4 dimensions, 8 explicit indexes, 4 FKs, 2 checks |
| Data pipeline | ✅ | Backfill + streaming consumer |
| Docker containerization | ✅ | Dockerfile.pipeline + Dockerfile.api + docker-compose.yml |
| Documentation | ✅ | README, ARCHITECTURE.md, PLAYBOOK.md |
| Code comments | ✅ | WHY-focused comments in api/ and pipeline/ |

---

## Common Tasks

### I want to...

**Query the API interactively**
```bash
open http://localhost:8000/docs  # Swagger UI with test interface
```

**Run tests**
```bash
docker-compose exec api pytest -v
```

**Monitor consumer lag**
```bash
docker-compose logs -f pipeline | grep -i "processed\|lag"
```

**Check API health**
```bash
curl http://localhost:8000/health | jq .
```

**View API logs**
```bash
docker-compose logs api
```

**Restart the pipeline**
```bash
docker-compose restart pipeline
```

**Manually backfill analytics** (if schema corrupted)
```bash
docker-compose exec pipeline python -m pipeline.backfill
```

**Full setup troubleshooting** → See [PLAYBOOK.md](PLAYBOOK.md) §1-2

**Debugging errors** → See [PLAYBOOK.md](PLAYBOOK.md) §5

**Operational procedures** → See [PLAYBOOK.md](PLAYBOOK.md)

**Technical deep dive** → See [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Verification Checklist

After `docker-compose up -d`, verify:

- [ ] `docker-compose ps` shows all 7 services "Up" (postgres, zookeeper, kafka, kafka-connect, debezium-init, pipeline, api)
- [ ] `curl http://localhost:8000/health` returns 200 with `{"status": "ok"}`
- [ ] `curl http://localhost:8000/analytics/orders?status=open` returns JSON array
- [ ] `docker-compose logs pipeline | grep "Backfill complete"` appears within 2 minutes
- [ ] Visit `http://localhost:8000/docs` and test endpoints

**Stuck?** → See [PLAYBOOK.md](PLAYBOOK.md) §5 (debugging guide)

---

## Design Decisions

Why these choices?

| Decision | Rationale | Alternative |
|----------|-----------|-------------|
| **Hybrid (batch + streaming)** | Reuse existing CDC investment; backfill handles initial load; consumer handles low-latency updates | Batch-only (simpler, slower) or streaming-only (complex state management) |
| **Star schema** | Denormalized for analytics queries; supports GROUP BY/aggregations; easy to extend dimensions | OLTP schema (slower queries) or data lake (more infra) |
| **Idempotent backfill** | Safe to re-run after crashes; no duplicate data; simple retry logic | Stateful offset management (risky) |
| **Manual offset commit** | Transactional safety: offset only persisted after DB write | Auto-commit (data loss risk) |
| **FastAPI + Pydantic** | Type-safe, auto-validation, auto-docs, minimal boilerplate | Flask (manual validation) or Django (heavy) |
| **psycopg2 pooling** | Standard library, battle-tested, simple config | SQLAlchemy (ORM overhead) or async (complexity) |

---

## Scaling & HA

**Quick win (vertical):**
```bash
# Edit .env: increase batch size & connection pool
BATCH_SIZE=5000        # (was 1000) process faster
DB_POOL_MAX_CONNECTIONS=10  # (was 5) more concurrency
docker-compose restart pipeline
```

**Next steps (horizontal):**
- Add load balancer (Nginx/HAProxy) in front of API
- Repartition Kafka topics (for multi-consumer)
- Add Prometheus/Grafana monitoring

See [ARCHITECTURE.md](ARCHITECTURE.md) §"Scaling Considerations" for details.

---

## Development

### Run tests locally
```bash
docker-compose exec api pytest -v
# All 119 tests should pass
```

### View test coverage
```bash
docker-compose exec api pytest --cov=api --cov=pipeline
```

### Add a new endpoint
1. Define Pydantic model in `api/models.py`
2. Implement query in `api/db.py`
3. Add route in `api/main.py`
4. Add tests in `tests/test_endpoints.py`

---

## Support

**Questions about setup?** → [PLAYBOOK.md](PLAYBOOK.md) §1

**Debugging?** → [PLAYBOOK.md](PLAYBOOK.md) §5

**Architecture & scaling?** → [ARCHITECTURE.md](ARCHITECTURE.md)

---

## Stack

| Layer | Technology |
|-------|-----------|
| **API** | FastAPI 0.128.0, Pydantic v2, psycopg2 |
| **Database** | PostgreSQL 15 (source + analytics) |
| **Streaming** | Apache Kafka + Debezium CDC |
| **Pipeline** | Python 3.11, confluent-kafka |
| **Testing** | pytest, httpx |
| **Containers** | Docker & Docker Compose |
| **Monitoring** | `docker logs` (Phase 5: Prometheus/Grafana) |

---

## Known Limitations

- **Single consumer** (Kafka 1 partition per topic; multi-consumer deferred to Phase 5)
- **No secrets management** (`.env` only; prod: use Vault/AWS Secrets Manager)
- **No TLS** (sandboxed; prod: reverse proxy + HTTPS)
- **No monitoring** (logging only; Phase 5: add Grafana)
- **Single-instance** (no Kubernetes; Phase 5: HA orchestration)

---

## License

Challenge solution for Deel take-home assessment (2026).
