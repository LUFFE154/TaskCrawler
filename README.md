# TaskCrawler
<img width="1896" height="947" alt="TaskCrawler" src="https://github.com/user-attachments/assets/cea832ce-8868-479f-a398-5a05d86caf15" />

## Asynchronous Web Scraping Job Processing Pipeline

TaskCrawler is an asynchronous web scraping processing system built with **FastAPI, PostgreSQL, Redis and Docker**.

The project demonstrates a scalable backend architecture where incoming scraping requests are decoupled from heavy network operations through a background worker and message queue.

Instead of keeping API requests waiting for scraping operations, the system creates jobs, stores their state, pushes tasks into Redis, and processes them asynchronously.

---

# Architecture Overview

The system follows an event-driven architecture:

```
                 Client
                   |
                   |
                   v
            FastAPI REST API
                   |
          +--------+--------+
          |                 |
          v                 v
    PostgreSQL          Redis Queue
    Job Storage        Message Broker
                            |
                            |
                            v
                    Async Background Worker
                            |
                            |
                            v
                    HTTP Web Scraper
                            |
                            |
                            v
                    PostgreSQL Update
```

---

# Features

## Asynchronous Job Processing

The API does not execute scraping operations during the HTTP request lifecycle.

When a URL is submitted:

1. A scraping job is created.
2. The job is stored in PostgreSQL.
3. A message is published to Redis.
4. A background worker processes the task.
5. The final result is persisted.

This allows the API to remain responsive even during slow network operations.

---

## Queue-Based Worker System

Redis is used as a lightweight message broker.

The worker continuously listens for new scraping tasks using blocking queue operations.

Benefits:

- Decoupled processing
- Better scalability
- Independent worker execution
- Non-blocking API requests

---

## Idempotent Job Creation

The system prevents duplicated scraping tasks.

When a URL is submitted, the API checks if the same resource already exists.

If a previous job exists:

- The existing record is returned.
- A new scraping operation is not created.

This avoids unnecessary network requests and duplicate processing.

---

## Resilient Error Handling

The scraping worker includes failure recovery mechanisms:

- Automatic retries
- Exponential backoff
- Random jitter delays
- Error persistence

Temporary network problems do not crash the worker.

Failed jobs are stored with error information for later inspection.

---

## Async Network Processing

The application uses asynchronous I/O throughout the stack:

- FastAPI async endpoints
- SQLAlchemy AsyncIO
- asyncpg PostgreSQL driver
- redis.asyncio
- httpx.AsyncClient

This allows efficient handling of multiple operations without blocking threads.

---

# Technology Stack

## Backend

- Python 3.11+
- FastAPI
- Pydantic
- SQLAlchemy AsyncIO
- Asyncpg
- HTTPX

## Infrastructure

- PostgreSQL 16
- Redis 7
- Docker
- Docker Compose

---

# Project Structure

```
TaskCrawler/
│
├── main.py              # FastAPI application and worker logic
├── database.py          # Async database configuration
├── models.py            # SQLAlchemy database models
├── config.py            # Environment configuration
│
├── docker-compose.yml   # Infrastructure setup
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables
│
└── README.md
```

---

# Requirements

Before running the project, install:

- Python 3.11+
- Docker
- Docker Compose
- PostegreSQL

---

# Running the Project

## 1. Clone repository

```bash
git clone https://github.com/LUFFE154/TaskCrawler

cd TaskCrawler
```

---

## 2. Start infrastructure

Start PostgreSQL and Redis containers:

```bash
docker-compose up -d
```

The project requires:

```
PostgreSQL
localhost:5432

Redis
localhost:6379
```

---

## 3. Install dependencies

Create a virtual environment:

```bash
python -m venv .venv
```

Activate:

Windows:

```bash
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Install packages:

```bash
pip install -r requirements.txt
```

---

## 4. Configure environment variables

Create a `.env` file:

```env
POSTGRES_ASYNC_URL=postgresql+asyncpg://app_db:app_db@127.0.0.1:5432/app_db

REDIS_URL=redis://127.0.0.1:6379/0
```

---

## 5. Start API

Run:

```bash
uvicorn main:app --reload
```

The API will be available at:

```
http://127.0.0.1:8000
```

Swagger documentation:

```
http://127.0.0.1:8000/docs
```

---

# API Usage

## Create Scraping Job

### Request

```
POST /api/v1/jobs
```

Example:

```bash
curl -X POST \
http://127.0.0.1:8000/api/v1/jobs \
-H "Content-Type: application/json" \
-d "{\"url\":\"https://example.com\"}"
```

---

### Response

```json
{
  "id": "5c5ff4a0-9e0e-41bc-80fb-9c98f6f9c095",
  "url": "https://example.com",
  "status": "PENDING",
  "retry_count": 0,
  "scraped_title": null,
  "error_log": null
}
```

The API immediately returns the job identifier while processing continues asynchronously.

---

# Check Job Status

## Request

```
GET /api/v1/jobs/{job_id}
```

Example:

```
GET /api/v1/jobs/5c5ff4a0-9e0e-41bc-80fb-9c98f6f9c095
```

---

## Job Lifecycle

A job follows this lifecycle:

```
PENDING
   |
   v
PROCESSING
   |
   +------------+
   |            |
   v            v
COMPLETED     FAILED
```

---

# Example Result

Successful processing:

```json
{
  "status": "COMPLETED",
  "retry_count": 0,
  "scraped_title": "Example Domain",
  "error_log": null
}
```

---

# Failure Handling Example

If a website cannot be reached:

```json
{
  "status": "FAILED",
  "retry_count": 3,
  "error_log": "Connection timeout"
}
```

The worker retries automatically before marking the job as failed.

---

# Engineering Decisions

## Why Redis?

Redis provides a simple and fast queue mechanism between the API and workers.

Advantages:

- Low latency
- Easy horizontal scaling
- Reliable message passing

---

## Why AsyncIO?

Web scraping is highly I/O-bound.

Using asynchronous programming allows the application to:

- Handle multiple network operations efficiently
- Avoid blocking execution
- Improve throughput

---

## Why PostgreSQL?

PostgreSQL stores:

- Job metadata
- Processing state
- Results
- Errors

It provides reliable persistence and transactional guarantees.

---

# Future Improvements

Planned improvements:

- Multiple worker processes
- Distributed worker scaling
- Better HTML extraction
- OpenGraph metadata extraction
- Link relationship mapping
- Authentication and authorization
- Rate limiting
- Monitoring with Prometheus/Grafana
- Docker production deployment
- Vector database integration for RAG pipelines

---

# License

This project is licensed under the MIT License.

---

# Author

Luiz Fernando Pio Ferreira

Backend Developer

Technologies:

Python • FastAPI • PostgreSQL • Redis • Docker • Async Systems
