import json
import logging
from aiokafka import AIOKafkaProducer
from usecase.process_event import EventProcessingService
from usecase.dto import ProcessEventResult, ProcessEventOutcome

logger = logging.getLogger(__name__)


class UnknownWorkerTopicError(Exception):
    pass


class WorkerMessageRouter:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        process_event_service: EventProcessingService,
        event_mapper,
        now_provider,
        events_v1_topic: str,
        events_dlq_topic: str
    ) -> None:
        self._producer = producer
        self._process_event_service = process_event_service
        self._event_mapper = event_mapper
        self._now_provider = now_provider
        self._events_v1_topic = events_v1_topic
        self._events_dlq_topic = events_dlq_topic

    async def dispatch(self, message) -> None:
        if message.topic == self._events_v1_topic:
            await self._handle_cdp_event(message)
            return

        raise UnknownWorkerTopicError(f"No handler for topic {message.topic}")

    async def send(self, topic_name: str, payload, key=None, headers=None) -> None:
        value = json.dumps(payload).encode('utf-8')
        await self._producer.send_and_wait(topic_name, value=value, key=key, headers=headers)

    async def _handle_cdp_event(self, message) -> None:
        try:
            raw_event = self._event_mapper.to_raw_event(message)
        except Exception as e:
            logger.error(f'failed to map event: {e}')
            await self._send_to_dlq(message, f"mapping_error: {e}")
            return

        result = await self._process_event_service.process_event(raw_event)
        
        if result.outcome == ProcessEventOutcome.SEND_TO_DLQ:
             await self._send_to_dlq(message, result.reason or "unknown_failure")

    async def _send_to_dlq(self, message, reason: str) -> None:
        payload = {
            "original_message": {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "value": message.value.decode('utf-8', errors='replace') if message.value else None,
            },
            "reason": reason,
            "metadata": {
                "timestamp": self._now_provider().isoformat()
            }
        }
        await self.send(self._events_dlq_topic, payload)
        logger.info(f"message sent to DLQ: {reason}")
