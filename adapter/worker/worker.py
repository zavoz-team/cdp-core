import asyncio
import logging
from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


class KafkaWorker:
    def __init__(self, consumer: AIOKafkaConsumer, router) -> None:
        self._consumer = consumer
        self._router = router
        self._is_running = False

    def stop(self) -> None:
        self._is_running = False
        logger.info('worker stop requested')

    async def run(self) -> None:
        self._is_running = True
        logger.info('worker loop started')
        
        try:
            async for msg in self._consumer:
                if not self._is_running:
                    break
                
                try:
                    await self._router.dispatch(msg)
                    await self._consumer.commit()
                except Exception as e:
                    logger.error(f'failed to process message: {e}', exc_info=True)
                    # We do NOT commit offset on error to allow retry if stable outcome wasn't reached
                    # Stable outcomes (processed, DLQ) are handled inside router.dispatch
        finally:
            logger.info('worker loop stopped')
