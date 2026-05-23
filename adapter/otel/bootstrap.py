from dataclasses import dataclass

from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from domain.config import Config

from .logging import OtelLogger
from .metrics import OtelMetrics
from .tracing import OtelTracer


@dataclass(frozen=True, slots=True)
class OtelRuntime:
    logger_provider: LoggerProvider | None = None
    meter_provider: MeterProvider | None = None
    tracer_provider: TracerProvider | None = None
    logger: OtelLogger | None = None
    metrics: OtelMetrics | None = None
    tracer: OtelTracer | None = None

    def shutdown(self) -> None:
        if self.meter_provider is not None:
            self.meter_provider.shutdown()
        if self.tracer_provider is not None:
            self.tracer_provider.shutdown()
        if self.logger_provider is not None:
            self.logger_provider.shutdown()


def setup_otel(
    config: Config,
    *,
    enable_logs: bool,
    enable_metrics: bool,
    enable_tracing: bool,
) -> OtelRuntime:
    if not any((enable_logs, enable_metrics, enable_tracing)):
        raise ValueError('empty otel runtime')

    if enable_metrics and config.otel.metric_export_interval <= 0:
        raise ValueError('invalid metric export interval')

    resource = Resource.create(
        {
            'service.name': config.otel.service_name,
            'deployment.environment': config.app.env,
        }
    )

    scope_name = config.app.name
    logger_provider = None
    meter_provider = None
    tracer_provider = None
    logger = None
    metrics_adapter = None
    tracer = None

    if enable_logs:
        logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=config.otel.logs_endpoint))
        )
        logger = OtelLogger(
            logger_provider.get_logger(scope_name), config.logging.level
        )

    if enable_metrics:
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=config.otel.metrics_endpoint),
            export_interval_millis=config.otel.metric_export_interval,
        )
        meter_provider = MeterProvider(
            metric_readers=[metric_reader],
            resource=resource,
            shutdown_on_exit=False,
        )
        metrics_adapter = OtelMetrics(meter_provider.get_meter(scope_name))

    if enable_tracing:
        tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=config.otel.traces_endpoint))
        )
        tracer = OtelTracer(tracer_provider.get_tracer(scope_name))

    return OtelRuntime(
        logger_provider=logger_provider,
        meter_provider=meter_provider,
        tracer_provider=tracer_provider,
        logger=logger,
        metrics=metrics_adapter,
        tracer=tracer,
    )
