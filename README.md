# Best Deal Store — Makeline Service

**FastAPI** service on **port 5002**: HTTP routes for admin/makeline workflows and a **RabbitMQ** consumer that processes order messages and updates **MongoDB** (order status / fulfilment).

## What this service does

This service implements the **fulfilment side** of the demo. A **background thread** consumes the same **orders** queue the Order service publishes to, then updates order documents in MongoDB (for example moving status from pending toward completed). That keeps heavy or slow work off the Order API’s request path. The **HTTP** surface exists so **Store Admin** can list or act on orders through paths under `/makeline` (proxied by admin nginx), giving operators a single place to watch the “makeline” without calling the Order service for pipeline state.

## HTTP API (base URL `http://<host>:5002`)

Admin nginx forwards `/makeline` to this service, so browsers call **`/makeline/...`** on the admin host; the paths below are what **FastAPI** exposes on the container itself.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness; `{ "status": "ok" }`. |
| GET | `/makeline/orders` | List all orders (newest first). |
| PUT | `/makeline/orders/{order_id}` | Body `{ "status": "<value>" }` required; updates Mongo. |

**Queue:** consumes durable queue **`orders`** on `RABBITMQ_URL` (same name the Order service publishes to).

## Stack

- Python 3.11, FastAPI, Pika, PyMongo

## Run locally

```bash
pip install -r requirements.txt
export MONGO_URL=... DB_NAME=bestdeal RABBITMQ_URL=amqp://...
uvicorn main:app --host 0.0.0.0 --port 5002
```

Health: `GET /health`

## Docker

```bash
docker build -t best-deal-makeline-service .
```

Cluster wiring and broker URL: **Best-Deal-Store-Final-Project**.
