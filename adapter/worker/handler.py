from kafka.admin import KafkaAdminClient  # type: ignore[import-untyped]

from domain.config import Config
from usecase.interface import Logger


def ensure_topics_exist(config: Config, logger: Logger) -> None:
    """
    Checks if required Kafka topics exist.
    Raises RuntimeError if any topic is missing.
    """
    logger.info('checking kafka topics availability')

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=config.kafka.bootstrap_servers,
            client_id='cdp-core-worker-init',
        )
    except Exception as e:
        raise RuntimeError(f'failed to connect to kafka: {e}') from e

    try:
        existing_topics = admin_client.list_topics()
        required_topics = [config.kafka.events_v1_topic, config.kafka.events_dlq_topic]

        missing = [t for t in required_topics if t not in existing_topics]
        if missing:
            raise RuntimeError(
                f'required kafka topics are missing: {", ".join(missing)}. '
                'please ensure topics are created before starting the worker.'
            )

        logger.info('all required kafka topics exist')
    finally:
        admin_client.close()
