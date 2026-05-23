from types import TracebackType

from usecase.interface import Attrs, AttrValue


class NoopMetrics:
    def increment(
        self,
        name: str,
        value: int = 1,
        attrs: Attrs | None = None,
    ) -> None:
        return None

    def record(
        self,
        name: str,
        value: float,
        attrs: Attrs | None = None,
    ) -> None:
        return None


class NoopSpan:
    def set_attribute(self, name: str, value: AttrValue) -> None:
        return None

    def record_error(self, error: Exception) -> None:
        return None


class NoopSpanContext:
    def __init__(self) -> None:
        self._span = NoopSpan()

    def __enter__(self) -> NoopSpan:
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return None


class NoopTracer:
    def start_span(
        self,
        name: str,
        attrs: Attrs | None = None,
    ) -> NoopSpanContext:
        return NoopSpanContext()
