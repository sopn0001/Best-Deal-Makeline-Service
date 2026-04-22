"""
Makeline Service
- HTTP API  : exposes /makeline/orders for the Store-Admin
- Background: consumes RabbitMQ 'orders' queue and updates order status in MongoDB
"""

import threading
import time
import json
import os

import pika
from bson import ObjectId
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

MONGO_URL    = os.getenv("MONGO_URL",    "mongodb://localhost:27017")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://localhost")
DB_NAME      = os.getenv("DB_NAME",      "bestdeal")
QUEUE        = "orders"

mongo = MongoClient(MONGO_URL)
db    = mongo[DB_NAME]

app = FastAPI(title="Makeline Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def to_doc(doc):
    doc["id"] = str(doc.pop("_id"))
    return doc


# ── HTTP API (consumed by Store-Admin) ────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/makeline/orders")
def list_orders():
    orders = db.orders.find().sort("created_at", -1)
    return [to_doc(o) for o in orders]


@app.put("/makeline/orders/{order_id}")
def update_order(order_id: str, body: dict):
    status = body.get("status")
    if not status:
        raise HTTPException(status_code=400, detail="status field required")
    db.orders.update_one({"_id": ObjectId(order_id)}, {"$set": {"status": status}})
    return {"updated": order_id}


# ── RabbitMQ consumer (background thread) ────────────────────────────────────

def process_order(ch, method, _props, body):
    order = json.loads(body)
    oid   = order.get("id")
    print(f"[Makeline] Processing order {oid}", flush=True)
    db.orders.update_one({"_id": ObjectId(oid)}, {"$set": {"status": "processing"}})
    time.sleep(5)
    db.orders.update_one({"_id": ObjectId(oid)}, {"$set": {"status": "completed"}})
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[Makeline] Order {oid} completed", flush=True)


def consume():
    attempt = 0
    while True:
        try:
            conn    = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = conn.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE, on_message_callback=process_order)
            print("[Makeline] RabbitMQ consumer started", flush=True)
            channel.start_consuming()
            return
        except Exception as exc:
            attempt += 1
            print(f"[Makeline] RabbitMQ attempt {attempt} failed: {exc}. Retry in 5s…", flush=True)
            time.sleep(5)


# Start the background consumer when the app loads
threading.Thread(target=consume, daemon=True).start()
