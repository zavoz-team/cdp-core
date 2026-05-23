from contextlib import AbstractContextManager
from types import TracebackType

from opentelemetry.trace import Span as OtelApiSpan
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace import Tracer as OtelApiTracer

from usecase.interface import Attrs, AttrValue


class OtelSpan:
    def __init__(self, span: OtelApiSpan) -> None:
        self._span = span
        self._error_recorded = False

    @property
    def error_recorded(self) -> bool:
        return self._error_recorded

    def set_attribute(self, name: str, value: AttrValue) -> None:
        self._span.set_attribute(name, value)

    def record_error(self, error: Exception) -> None:
        self._span.record_exception(error)
        self._span.set_status(Status(StatusCode.ERROR, str(error)))
        self._error_recorded = True


class OtelSpanContext:
    def __init__(self, context_manager: AbstractContextManager[OtelApiSpan]) -> None:
        self._context_manager = context_manager
        self._span: OtelSpan | None = None

    def __enter__(self) -> OtelSpan:
        self._span = OtelSpan(self._context_manager.__enter__())
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if isinstance(exc, Exception) and self._span is not None:
            if not self._span.error_recorded:
                self._span.record_error(exc)
        return self._context_manager.__exit__(exc_type, exc, traceback)


class OtelTracer:
    def __init__(self, tracer: OtelApiTracer) -> None:
        self._tracer = tracer

    def start_span(
        self,
        name: str,
        attrs: Attrs | None = None,
    ) -> OtelSpanContext:
        context_manager = self._tracer.start_as_current_span(
            name,
            attributes=None if attrs is None else dict(attrs),
            record_exception=False,
            set_status_on_exception=True,
        )
        return OtelSpanContext(context_manager)
