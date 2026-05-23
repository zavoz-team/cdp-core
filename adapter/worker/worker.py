import asyncio
import time
from pathlib import Path

from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

from adapter.worker.handlers import ProcessingRetryError
from usecase.interface import Logger, Metrics, Tracer

HEARTBEAT_PATH = Path('/tmp/worker_heartbeat')


class KafkaWorker:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        router,
        logger: Logger,
        tracer: Tracer,
        metrics: Metrics,
    ) -> None:
        self._consumer = consumer
        self._router = router
        self._logger = logger
        self._tracer = tracer
        self._metrics = metrics
        self._is_running = False

    def stop(self) -> None:
        self._is_running = False
        self._logger.info('worker stop requested')

    def _heartbeat(self) -> None:
        # Записываем текущий timestamp в файл
        HEARTBEAT_PATH.write_text(str(time.time()))

    async def run(self) -> None:
        self._is_running = True
        self._heartbeat()
        self._logger.info('worker loop started')

        try:
            async for msg in self._consumer:
                if not self._is_running:
                    break

                self._metrics.increment(
                    'events_received_total',
                    attrs={'topic': msg.topic},
                )

                with self._tracer.start_span(
                    'worker.run.iteration',
                    attrs={
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                    },
                ) as span:
                    self._logger.info(
                        'kafka event received',
                        attrs={
                            'topic': msg.topic,
                            'partition': msg.partition,
                            'offset': msg.offset,
                        },
                    )

                    try:
                        await self._router.dispatch(msg)
                        # Commit only after stable outcome
                        await self._consumer.commit()
                        self._heartbeat()
                        span.set_attribute('outcome', 'committed')
                    except ProcessingRetryError as e:
                        # Требуется повторная обработка - НЕ коммитим
                        self._logger.warning(
                            'processing retry required',
                            attrs={'error': str(e)},
                        )
                        span.record_error(e)
                        span.set_attribute('outcome', 'retry')
                        await asyncio.sleep(1)
                    except Exception as e:
                        # Общая ошибка - НЕ коммитим
                        self._logger.error(
                            'unexpected error during message dispatch',
                            attrs={'error': str(e)},
                        )
                        span.record_error(e)
                        span.set_attribute('outcome', 'error')
                        await asyncio.sleep(1)
        finally:
            self._logger.info('worker loop stopped')
