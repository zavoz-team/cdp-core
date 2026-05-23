from threading import Lock

from opentelemetry.metrics import Counter, Histogram, Meter

from usecase.interface import Attrs


class OtelMetrics:
    def __init__(self, meter: Meter) -> None:
        self._meter = meter
        self._lock = Lock()
        self._counters: dict[str, Counter] = {}
        self._histograms: dict[str, Histogram] = {}

    def increment(
        self,
        name: str,
        value: int = 1,
        attrs: Attrs | None = None,
    ) -> None:
        self._counter(name).add(
            value,
            attributes=None if attrs is None else dict(attrs),
        )

    def record(
        self,
        name: str,
        value: float,
        attrs: Attrs | None = None,
    ) -> None:
        self._histogram(name).record(
            value,
            attributes=None if attrs is None else dict(attrs),
        )

    def _counter(self, name: str) -> Counter:
        counter = self._counters.get(name)
        if counter is not None:
            return counter

        with self._lock:
            counter = self._counters.get(name)
            if counter is None:
                counter = self._meter.create_counter(name)
                self._counters[name] = counter
        return counter

    def _histogram(self, name: str) -> Histogram:
        histogram = self._histograms.get(name)
        if histogram is not None:
            return histogram

        with self._lock:
            histogram = self._histograms.get(name)
            if histogram is None:
                histogram = self._meter.create_histogram(name)
                self._histograms[name] = histogram
        return histogram
