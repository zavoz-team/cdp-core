import asyncio
import logging
import time
from pathlib import Path

from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

from adapter.worker.handlers import ProcessingRetryError

logger = logging.getLogger(__name__)

HEARTBEAT_PATH = Path('/tmp/worker_heartbeat')


class KafkaWorker:
    def __init__(self, consumer: AIOKafkaConsumer, router) -> None:
        self._consumer = consumer
        self._router = router
        self._is_running = False

    def stop(self) -> None:
        self._is_running = False
        logger.info('worker stop requested')

    def _heartbeat(self) -> None:
        # Записываем текущий timestamp в файл
        HEARTBEAT_PATH.write_text(str(time.time()))

    async def run(self) -> None:
        self._is_running = True
        self._heartbeat()
        logger.info('worker loop started')

        try:
            async for msg in self._consumer:
                if not self._is_running:
                    break

                logger.info(
                    'kafka_event_received topic=%s partition=%d offset=%d',
                    msg.topic,
                    msg.partition,
                    msg.offset,
                )

                try:
                    await self._router.dispatch(msg)
                    # Commit only after stable outcome (processed, ignored, sent_to_dlq)
                    await self._consumer.commit()
                    self._heartbeat()
                except ProcessingRetryError as e:
                    # Specific error that requires retry - do NOT commit
                    logger.warning(f'processing retry required: {e}')
                    # Optional: sleep to avoid tight loop on persistent failures
                    await asyncio.sleep(1)
                except Exception as e:
                    # General error - do NOT commit, better to crash or log and retry
                    logger.error(
                        f'unexpected error during message dispatch: {e}', exc_info=True
                    )
                    await asyncio.sleep(1)
        finally:
            logger.info('worker loop stopped')
