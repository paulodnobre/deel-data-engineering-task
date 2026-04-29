# Operational Playbook — Deel Analytics Platform

Step-by-step procedures for deploying, operating, monitoring, and recovering the analytics platform. All commands reference docker-compose; assume you're in the project root directory.

## Section 1: One-Time Setup

### Prerequisites
- Docker & Docker Compose installed
- PostgreSQL, Kafka, and Python not running locally on ports 5432, 9092, 8000

### Setup Steps

```bash
# 1. Navigate to project root
cd /path/to/deel-analytics

# 2. Create .env file from template
cp .env.example .env

# 3. (Optional) Edit .env if using custom DB password
# For sandbox, defaults are fine: DB_PASSWORD=changeme
nano .env

# 4. Start all services (postgres, zookeeper, kafka, pipeline, api)
docker-compose up -d

# 5. Wait 60-90 seconds for services to become healthy
# Watch status repeatedly; don't proceed until all are "Up" or "(healthy)"
watch docker-compose ps
# Press Ctrl+C to exit watch

# Expected output:
# postgres    "..." Up (healthy)
# zookeeper   "..." Up (healthy)  
# kafka       "..." Up (healthy)
# pipeline    "..." Up
# api         "..." Up (healthy)

# 6. Verify backfill completed
sleep 30
docker-compose logs pipeline | grep -i "backfill complete"
# Output: "Backfill complete; starting consumer..." indicates success

# 7. Test API connectivity
curl http://localhost:8000/health | jq .
# Expected response: {"status": "ok", "db": "connected"}

# 8. Query API to verify data population
curl http://localhost:8000/analytics/orders?status=open | jq . | head -20
# Should return array of orders grouped by delivery_date and status

# 9. (Optional) Visit interactive documentation
open http://localhost:8000/docs
# Test endpoints directly in Swagger UI
```

### Verification Checklist
- [ ] All 5 services show "Up" in `docker-compose ps`
- [ ] PostgreSQL shows "(healthy)"
- [ ] API shows "(healthy)"
- [ ] `curl http://localhost:8000/health` returns 200 with status="ok"
- [ ] `curl http://localhost:8000/analytics/orders` returns JSON array (not 500 error)

---

## Section 2: Running Backfill Manually

Use this if analytics schema becomes empty, corrupted, or needs reset.

```bash
# 1. (Optional) Verify current row count in fact table
docker-compose exec postgres psql -U postgres -d deel \
  -c "SELECT COUNT(*) as row_count FROM analytics.fct_order_items;"
# Output: row_count | <number>

# 2. Connect to pipeline container
docker-compose exec pipeline bash
# Inside container:

# 3. Run backfill manually (idempotent; safe to re-run)
python -m pipeline.backfill
# Output: "Backfill complete: X rows inserted/updated"

# 4. Exit container
exit

# 5. Verify results
docker-compose exec postgres psql -U postgres -d deel \
  -c "SELECT COUNT(*) as row_count FROM analytics.fct_order_items;"
# Should show same count (ON CONFLICT upsert prevents duplicates)

# 6. Query API to verify data
curl http://localhost:8000/analytics/orders?status=open | jq .
```

---

## Section 3: Monitoring Consumer Lag

Consumer lag indicates how far behind the pipeline is from live Kafka messages. Monitor via logs.

```bash
# 1. Watch consumer logs in real-time (follow mode)
docker-compose logs -f pipeline | grep -i "offset\|lag\|processed"
# Ctrl+C to exit

# Example output:
# "Processed INSERT to orders: partition=0, offset=1234"
# "Consumer lag: 50 messages behind"

# 2. One-time check of recent activity (last 20 lines)
docker-compose logs pipeline | tail -20

# 3. Check if consumer is stuck (no new log lines for >30 seconds)
docker-compose logs pipeline | tail -1 | cut -d' ' -f1-4  # Show timestamp

# 4. If lag is growing (> 1000 messages behind):

# 4a. Check for errors that might slow processing
docker-compose logs pipeline | grep -i "error\|exception"
# If errors found, see "Debugging" section below

# 4b. Check if consumer is still running
docker-compose ps pipeline
# Should show "Up"; if not, see recovery section

# 4c. Increase batch size to process faster
# Edit .env: BATCH_SIZE=5000 (was 1000)
nano .env
# Save and restart
docker-compose restart pipeline

# 5. Monitor lag after restart
docker-compose logs -f pipeline | grep "processed"
# Lag should decrease towards 0 over 2-5 minutes

# 6. Once lag reaches 0, consumer is caught up
docker-compose logs pipeline | tail -5
```

### Lag Targets
- **Healthy:** Lag 0-100 messages (< 1 second behind)
- **Acceptable:** Lag 100-1000 messages (< 1 minute behind)
- **Warning:** Lag 1000-10000 messages (> 1 minute, investigate)
- **Critical:** Lag growing indefinitely (consumer stuck, restart required)

---

## Section 4: Querying the API

Examples of common analytics queries via curl and browser.

```bash
# 1. Get all open orders, grouped by delivery date and status
curl -X GET "http://localhost:8000/analytics/orders?status=open" \
  -H "accept: application/json" | jq .

# Response example:
# {
#   "items": [
#     {"delivery_date": "2026-05-15", "status": "PENDING", "order_count": 42},
#     {"delivery_date": "2026-05-16", "status": "PENDING", "order_count": 38},
#     ...
#   ],
#   "total": 1234
# }

# 2. Get top 5 delivery dates by order count (default limit 3)
curl -X GET "http://localhost:8000/analytics/orders/top?limit=5" \
  -H "accept: application/json" | jq .

# Response example:
# {
#   "items": [
#     {"delivery_date": "2026-05-15", "order_count": 1000},
#     {"delivery_date": "2026-05-16", "order_count": 950},
#     ...
#   ]
# }

# 3. Get product quantities pending (all products with pending items)
curl -X GET "http://localhost:8000/analytics/orders/product" \
  -H "accept: application/json" | jq .

# Response example:
# {
#   "items": [
#     {"product_id": 101, "product_name": "Widget A", "quantity_pending": 500},
#     {"product_id": 102, "product_name": "Widget B", "quantity_pending": 350},
#     ...
#   ]
# }

# 4. Get top 3 customers for open orders (limit capped at 100)
curl -X GET "http://localhost:8000/analytics/orders/customers?status=open&limit=3" \
  -H "accept: application/json" | jq .

# Response example:
# {
#   "items": [
#     {"customer_id": 201, "customer_name": "Acme Corp", "pending_order_count": 42},
#     {"customer_id": 202, "customer_name": "Beta Ltd", "pending_order_count": 38},
#     ...
#   ]
# }

# 5. Health check (used by docker-compose and monitoring systems)
curl -X GET "http://localhost:8000/health" \
  -H "accept: application/json" | jq .
# Response: {"status": "ok", "db": "connected"}
# HTTP 200 = healthy, HTTP 503 = DB unavailable

# 6. Interactive API documentation
# Open browser and navigate to:
open http://localhost:8000/docs      # Swagger UI (test endpoints)
open http://localhost:8000/redoc     # ReDoc (read-only spec)
open http://localhost:8000/openapi.json  # Machine-readable spec
```

---

## Section 5: Debugging Common Issues

### Issue 1: API returns 500 on /analytics/orders

**Symptom:** Request to `/analytics/orders` returns HTTP 500

**Investigation:**
```bash
# Check API logs for error message
docker-compose logs api | tail -20 | grep -i "error\|exception"

# Check if table exists and has data
docker-compose exec postgres psql -U postgres -d deel \
  -c "SELECT COUNT(*) FROM analytics.fct_order_items;"

# If query fails: table missing or not created yet
# If count = 0: table exists but empty (backfill hasn't run)
# If count > 0: table has data; error is in API code
```

**Resolution:**
1. **If table missing:** Pipeline failed to run. Restart: `docker-compose restart pipeline`
2. **If table empty:** Wait 60+ seconds for backfill to complete on startup
3. **If table populated:** Check API logs for specific error; may be configuration issue

---

### Issue 2: Kafka connection timeout

**Symptom:** Pipeline logs show "KafkaError", "Connection refused", or "Broker unreachable"

**Investigation:**
```bash
# Check Kafka and Zookeeper status
docker-compose ps kafka zookeeper
# Should both show "Up (healthy)"

# Check Kafka logs for startup errors
docker-compose logs kafka | grep -i "error\|fatal\|exception"

# Check if Zookeeper is healthy (prerequisite for Kafka)
docker-compose logs zookeeper | tail -10 | grep -i "error"
```

**Resolution:**
1. **If Zookeeper unhealthy:** Restart Zookeeper first: `docker-compose restart zookeeper`
2. **Wait 30 seconds for Zookeeper to stabilize**
3. **Restart Kafka:** `docker-compose restart kafka`
4. **Wait 30 seconds for Kafka to connect to Zookeeper**
5. **Restart pipeline:** `docker-compose restart pipeline`
6. **Verify:** `docker-compose logs pipeline | grep "Consumer created"`

---

### Issue 3: Consumer lag growing indefinitely

**Symptom:** `docker-compose logs pipeline | grep offset` shows lag > 10000 and still increasing

**Investigation:**
```bash
# Check for errors slowing the consumer
docker-compose logs pipeline | grep -i "error\|exception" | tail -10

# Check if consumer is polling messages at all
docker-compose logs pipeline | grep -i "processed\|upsert" | tail -5
# If no recent activity, consumer may be stuck

# Check resource usage
docker stats pipeline
# If CPU near 100%, may be slow due to resource limits
```

**Resolution:**
1. **Increase batch size (process more messages per iteration):**
   ```bash
   # Edit .env: BATCH_SIZE=1000 → BATCH_SIZE=5000
   nano .env
   docker-compose restart pipeline
   # Monitor: docker-compose logs -f pipeline | grep processed
   ```

2. **Increase connection pool (allow more concurrent DB writes):**
   ```bash
   # Edit .env: DB_POOL_MAX_CONNECTIONS=5 → DB_POOL_MAX_CONNECTIONS=10
   nano .env
   docker-compose restart pipeline
   ```

3. **If errors found, resolve underlying issue (see Issue 5 below)**

---

### Issue 4: Port 8000 already in use

**Symptom:** `docker-compose up` fails with "bind: address already in use"

**Investigation:**
```bash
# Check what process is using port 8000
lsof -i :8000
# Output: PID, name, and details of process using port

# Check if Docker container is stuck in startup
docker-compose ps api
```

**Resolution:**
1. **Kill the interfering process:**
   ```bash
   kill -9 <PID>  # From lsof output
   docker-compose restart api
   ```

2. **OR change port mapping in docker-compose.yml:**
   ```yaml
   # Change line: "8000:8000" to "8001:8000"
   api:
     ports:
       - "8001:8000"
   ```
   Then restart: `docker-compose up -d`
   And access API at: `http://localhost:8001`

---

### Issue 5: Pipeline exits immediately

**Symptom:** `docker-compose ps pipeline` shows "Exited", not "Up"

**Investigation:**
```bash
# Check exit code (0 = success, 1+ = error)
docker-compose ps pipeline | grep -oE 'Exited \(.*\)'

# Check full error log
docker-compose logs pipeline

# Look for specific error messages
docker-compose logs pipeline | grep -i "error\|exception\|traceback" | head -20

# Check if environment variables are set correctly
docker-compose exec pipeline env | grep POSTGRES_
docker-compose exec pipeline env | grep KAFKA_
# If variables missing, see .env.example for required vars
```

**Resolution:**
1. **If config validation error:**
   - Check .env file: `cat .env | grep POSTGRES_`
   - Verify all required variables are set (see .env.example)
   - Restart: `docker-compose restart pipeline`

2. **If Kafka unreachable:**
   - Restart Kafka first: `docker-compose restart zookeeper kafka`
   - Wait 30 seconds
   - Restart pipeline: `docker-compose restart pipeline`

3. **If PostgreSQL unreachable:**
   - Restart PostgreSQL: `docker-compose restart postgres`
   - Wait 10 seconds
   - Restart pipeline: `docker-compose restart pipeline`

4. **If unknown error:**
   - Increase logging: Edit .env: `LOG_LEVEL=INFO` → `LOG_LEVEL=DEBUG`
   - Restart: `docker-compose restart pipeline`
   - Examine full logs: `docker-compose logs pipeline | head -100`

---

## Section 6: Recovery Procedures

### Scenario 1: Analytics Schema Corrupted or Stale

```bash
# 1. Verify current state
docker-compose exec postgres psql -U postgres -d deel \
  -c "SELECT COUNT(*) FROM analytics.fct_order_items;"

# 2. Drop and recreate schema (CAUTION: deletes all analytics data)
docker-compose exec postgres psql -U postgres -d deel << EOF
DROP SCHEMA analytics CASCADE;
\q
EOF

# 3. Reapply schema DDL
docker-compose exec postgres psql -U postgres -d deel < schemas/analytics.sql

# 4. Restart pipeline to re-run backfill (idempotent, safe)
docker-compose restart pipeline

# 5. Monitor backfill progress
docker-compose logs -f pipeline | grep -i "backfill\|insert"
# Wait until: "Backfill complete"

# 6. Verify data repopulated
docker-compose exec postgres psql -U postgres -d deel \
  -c "SELECT COUNT(*) FROM analytics.fct_order_items;"
# Should show row count > 0

# 7. Test API
curl http://localhost:8000/analytics/orders?status=open | jq . | head -10
```

### Scenario 2: Kafka Offset Corrupted (Consumer Stuck)

```bash
# 1. Restart consumer (will resume from last committed offset)
docker-compose restart pipeline

# 2. Monitor offset recovery
docker-compose logs -f pipeline | grep "offset\|processed"
# Watch for: "Processed INSERT/UPDATE/DELETE"
# Lag should decrease towards 0

# 3. If still stuck after 5 minutes, check errors
docker-compose logs pipeline | grep -i "error" | tail -10

# 4. Advanced: Reset Kafka offsets (if needed)
# NOTE: Requires Kafka CLI tools; documented in Phase 5
# For now: stopping consumer for 24 hours expires old offsets; Kafka auto-resets
```

### Scenario 3: Pipeline or API Container Crashes

```bash
# 1. Check exit status and logs
docker-compose ps pipeline
docker-compose logs pipeline | tail -30

# 2. Restart the service
docker-compose restart pipeline
# OR
docker-compose restart api

# 3. Verify it's running
docker-compose ps | grep pipeline

# 4. Tail logs to confirm success
docker-compose logs -f pipeline
# Watch for: "Consumer created", "Backfill complete", "Processed INSERT"
```

---

## Section 7: Performance Tuning

### Scenario: Backfill is too slow or Consumer lag is high

```bash
# 1. Identify bottleneck: CPU, memory, or I/O?
docker stats pipeline
# If CPU near 100%: increase BATCH_SIZE
# If memory near 100%: reduce BATCH_SIZE or allocate more RAM to container
# If I/O wait high: check disk, may need faster storage

# 2. Increase batch size for faster processing
# Edit .env
nano .env
# Change: BATCH_SIZE=1000 → BATCH_SIZE=5000

# 3. Increase connection pool to allow more concurrent DB writes
# Change: DB_POOL_MAX_CONNECTIONS=5 → DB_POOL_MAX_CONNECTIONS=10

# 4. Restart pipeline to apply changes
docker-compose restart pipeline

# 5. Monitor performance
docker-compose logs -f pipeline | grep "processed\|updated"
# Watch for throughput improvement (messages/sec increase)

# 6. If improved, keep new settings; if worse, revert
# (Higher BATCH_SIZE = higher memory use; monitor docker stats)
```

---

## Shutdown and Cleanup

### Graceful Shutdown
```bash
# 1. Stop all services gracefully (waits for in-flight operations)
docker-compose down

# 2. (Optional) Remove volumes (CAUTION: deletes data)
docker-compose down -v

# 3. Verify all containers stopped
docker ps
# Should show no deel-analytics containers
```

### Full Reset (WARNING: Deletes all data and containers)
```bash
# 1. Stop and remove everything
docker-compose down -v

# 2. Delete Docker images (if no longer needed)
docker-compose down --rmi all

# 3. Clean up locally (if desired)
rm -f .env
rm -rf pg-data/  # If using local volume binding

# 4. Restart from scratch
cp .env.example .env
docker-compose up -d
```

---

## References

- Docker Compose CLI: https://docs.docker.com/compose/reference/
- PostgreSQL CLI (psql): https://www.postgresql.org/docs/current/app-psql.html
- Kafka CLI: https://kafka.apache.org/quickstart
- curl HTTP client: https://curl.se/docs/

