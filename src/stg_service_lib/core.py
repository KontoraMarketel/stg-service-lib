import asyncio
import json
import logging

from typing import Any, Dict, Optional
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import clickhouse_connect

from clickhouse_connect.driver.asyncclient import AsyncClient

from .configuration import Config
from .custom_types import DataCallback, KafkaMessage
from .exceptions import ConfigurationError
from .minio_pool import MinioClientPool
from .process_data import process_data
from .storage import download_from_minio


logger = logging.getLogger(__name__)


class DataProcessor:
    """Основной класс для обработки данных из Kafka в ClickHouse через MinIO"""

    def __init__(
        self,
        config: Config,
        concurrent_tasks: int = 100,
        data_callback: Optional[DataCallback] = None,
    ):
        self.config = config
        self.concurrent_tasks = concurrent_tasks
        self.data_callback = data_callback

        self.semaphore = asyncio.Semaphore(concurrent_tasks)
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._minio_pool: Optional[MinioClientPool] = None
        self._db_conn: Optional[AsyncClient] = None
        self._running_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Инициализация всех подключений"""
        try:
            await self._initialize_connections()
            logger.info("All connections established successfully")
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            await self.stop()
            raise

    async def _initialize_connections(self) -> None:
        """Инициализация отдельных подключений"""
        self._consumer = AIOKafkaConsumer(
            self.config.kafka.consumer_topic,
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            group_id=self.config.kafka.group_id,
            auto_offset_reset=self.config.kafka.auto_offset_reset,
            enable_auto_commit=self.config.kafka.enable_auto_commit,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self._minio_pool = MinioClientPool(
            endpoint_url=self.config.minio.endpoint,
            access_key=self.config.minio.access_key,
            secret_key=self.config.minio.secret_key,
            size=5,
        )
        await self._minio_pool.start()

        self._db_conn = await clickhouse_connect.get_async_client(
            host=self.config.clickhouse.host,
            port=self.config.clickhouse.port,
            username=self.config.clickhouse.username,
            password=self.config.clickhouse.password,
            database=self.config.clickhouse.database,
        )

        await self._consumer.start()
        await self._producer.start()

    async def process_message(self, msg: KafkaMessage) -> Optional[Dict[str, Any]]:
        """Обработка одного сообщения (публичный метод для ручного использования)"""
        async with self.semaphore:
            return await self._handle_message(msg)

    async def _handle_message(self, msg: KafkaMessage) -> Optional[Dict[str, Any]]:
        """Внутренняя обработка сообщения"""
        logger.info(f"Received message: {msg}")

        task_id = msg["task_id"]
        minio_key = msg["minio_key"]
        ts = msg["ts"]

        logger.info(f"Start processing task {task_id}")

        try:
            # Загрузка данных из MinIO
            data = await download_from_minio(
                self._minio_pool, self.config.minio.bucket, minio_key
            )
            logger.info(
                f"Downloaded data from MinIO: {data[:100]}..."
            )  # Логируем первые 100 символов

            # Обработка данных
            await process_data(
                db_conn=self._db_conn,
                data=data,
                dwh_table=self.config.clickhouse.table,
                msg_payload=msg,
                on_data=self.data_callback,
            )

            logger.info(f"Task {task_id} completed successfully")

            return {
                "task_id": task_id,
                "ts": ts,
            }

        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            await self._handle_processing_error(task_id, e, msg)
            return None

    async def _handle_processing_error(
        self, task_id: str, error: Exception, msg: Dict[str, Any]
    ) -> None:
        """Обработка ошибок обработки"""
        # TODO: реализовать логику записи в dead letter queue или специальную таблицу
        logger.error(f"Task {task_id} failed: {error}")
        # Можно добавить метрики, алертинг и т.д.

    async def run_consumer_loop(self) -> None:
        """Запуск основного цикла потребления сообщений"""
        if not all([self._consumer, self._producer, self._minio_pool, self._db_conn]):
            raise ConfigurationError("Components not initialized. Call start() first")

        logger.info("Starting consumer loop")

        try:
            async for msg in self._consumer:
                task = asyncio.create_task(self._process_and_produce(msg.value))
                self._running_tasks.add(task)
                task.add_done_callback(self._running_tasks.discard)

        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise

    async def _process_and_produce(self, msg_value: Dict[str, Any]) -> None:
        """Обработка и отправка результата"""
        try:
            result = await self.process_message(msg_value)
            if result:
                encoded_task_id = str(result["task_id"]).encode("utf-8")
                await self._producer.send(
                    self.config.kafka.producer_topic,
                    value=result,
                    key=encoded_task_id,
                )

        except Exception as e:
            logger.error(f"Error in process_and_produce: {e}")

    async def stop(self) -> None:
        """Корректная остановка всех компонентов"""
        logger.info("Stopping data processor...")

        # Останавливаем consumer и producer
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()

        # Ждем завершения текущих задач
        if self._running_tasks:
            logger.info("Waiting for running tasks to complete...")
            await asyncio.gather(*self._running_tasks, return_exceptions=True)

        # Закрываем соединения
        if self._minio_pool:
            await self._minio_pool.stop()
        if self._db_conn:
            await self._db_conn.close()

        logger.info("Data processor stopped successfully")

    async def __aenter__(self) -> "DataProcessor":
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.stop()
