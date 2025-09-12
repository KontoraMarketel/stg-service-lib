# !/usr/bin/env python3
import os
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from minio_pool import MinioClientPool
from storage import download_from_minio

from process_data import process_data
import clickhouse_connect

from clickhouse_connect.driver.asyncclient import AsyncClient

# Configure logging
logging.basicConfig(level=logging.INFO)

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_TOPIC = os.getenv("KAFKA_INGEST_COMMISSIONS_TASKS_TOPIC")
PRODUCER_TOPIC = os.getenv("KAFKA_MART_COMMISSIONS_TOPIC")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# ClickHouse configuration
CLICKHOUSE_HOST=os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT=int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUES_USERNAME=os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD=os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB=os.getenv("CLICKHOUSE_DB")

# Concurrency control
CONCURRENT_TASKS = 100  # максимальное количество одновременных задач
semaphore = asyncio.Semaphore(CONCURRENT_TASKS)


# Handle a single message
async def handle_message(msg, minio_pool: MinioClientPool, db_conn):
    logging.info(f"Received message: {msg}")
    task_id = msg["task_id"]
    minio_key = msg["minio_key"]
    ts = msg["ts"]

    logging.info(f"Start processing task {task_id}")

    data = await download_from_minio(minio_pool, MINIO_BUCKET, minio_key)
    logging.info(f"Downloaded data from MinIO {data}")

    await process_data(db_conn, data, task_id, ts)

    logging.info(f"Task {task_id} completed successfully.")

    return {
        "task_id": task_id,
        "ts": ts,
    }

# Process and produce message
async def process_and_produce(msg_value, producer, minio_pool, db_conn):
    async with semaphore:
        try:
            next_msg = await handle_message(msg_value, minio_pool, db_conn)
            encoded_task_id = str(next_msg["task_id"]).encode("utf-8")
            await producer.send(
                PRODUCER_TOPIC,
                value=next_msg,
                key=encoded_task_id,
            )
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # TODO: write task to out of the box table

# Main function
async def main():

    consumer = AIOKafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="sales-ingestors",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    minio_pool = MinioClientPool(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        size=5,
    )
    await minio_pool.start()
    
    clickhoose_conn = await clickhouse_connect.get_async_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUES_USERNAME,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    await consumer.start()
    await producer.start()
    tasks = set()
    try:
        async for msg in consumer:
            task = asyncio.create_task(process_and_produce(msg.value, producer, minio_pool, clickhoose_conn))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
    finally:
        logging.info("Stopping consumer. Waiting for tasks to finish...")
        await consumer.stop()
        await producer.stop()
        await minio_pool.stop()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())



