## TaskCrawler
# Distributed Data Ingestion & Resilient Feature Engineering Engine

An enterprise-grade, asynchronous, and non-blocking data ingestion pipeline built to extract high-value structured metadata, clean content, and media graphs from web resources. This system decouples client requests from intense network operations, ensuring maximum throughput and anti-fragile execution under heavy load.

##  Architecture Overview

The system implements a decoupled microservices-inspired pattern using an API Gateway layer, atomic persistence, and asynchronous background processing loops.
##  Key Engineering Features

* **Strict Request Idempotency:** Intercepts incoming payloads and hashes target URLs against the database. Duplicate requests are immediately rejected/returned with a `200 OK` status, preventing redundant network IO and database bloat.
* **Non-Blocking IO Concurrency:** Built entirely on top of Python's `async/await` paradigm using FastAPI and `httpx.AsyncClient`. Server worker threads are never blocked while waiting for external network responses.
* **Anti-Fragile Resilience Layer:** Features custom error-handling intercepts with an **Exponential Backoff and Jitter** retry algorithm. Prevents target server hammering and thundering herd problems during transient network failures.
* **Structured Semantic Parsing:** Strips advertising scripts, menus, and cookies to isolate high-value fields (OpenGraph SEO tags, isolated inner/outer link graphs, sanitised text bodies) tailored for Vector Database ingestion (RAG).

##  Tech Stack

* **Language:** Python 3.11+
* **Framework:** FastAPI
* **Async Database Driver:** SQLAlchemy (Asyncio) + Asyncpg
* **Database:** PostgreSQL
* **Caching & Broker Abstraction:** Redis / In-Memory Background Loops
* **Containerization:** Docker & Docker Compose

##  Getting Started

### Prerequisites
* Docker and Docker Compose installed.
* Python 3.11+ (if running outside Docker for development).

### 1. Spin up the Infrastructure
Launch the database, caching broker, and backend containers in detached mode:
```bash
docker-compose up -d
```

### 2. Install Dependencies (Local Dev)
```bash
pip install -r requirements.txt
```

### 3. Run the Application Gateway
```bash
uvicorn main:app --reload
```
The API documentation will be available at `http://127.0.0`.

##  Validating System Behavior

### 1. Ingestion & Asynchronous Decoupling
Send a `POST` request to queue a complex heavy resource (e.g., a YouTube Video or Amazon Product):
```bash
curl -X 'POST' \
  'http://127.0.0' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "url": "https://youtube.com"
}'
```
**Expected Response (`202 Accepted`):** The gateway returns a `job_id` (UUID) instantly. It does not wait for the scraping task to complete.

### 2. Idempotency Validation
Fire the exact same `POST` request again. The system will bypass worker allocation and instantly return the pre-existing database record ID with a `200 OK` status, proving the ingestion shield works.

### 3. Network Failure Resilience Check
Submit a URL that returns infrastructure redirect responses (like `https://google.com` without `www` when redirects are unhandled) or a dead domain. 
Inspect the status via `GET /api/v1/jobs/{job_id}`. You will observe the `retry_count` incrementing incrementally over randomized intervals, followed by a graceful termination state (`FAILED`) logging exact system tracebacks into the database JSONB block for SRE audit profiling.
