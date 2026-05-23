from collections.abc import Callable
from dataclasses import dataclass, field

from usecase.interface import Logger, Metrics, Tracer


@dataclass(slots=True)
class ObservabilityRuntime:
    logger: Logger
    metrics: Metrics
    tracer: Tracer
    _shutdown_callbacks: tuple[Callable[[], None], ...] = ()
    _is_shutdown: bool = field(default=False, init=False, repr=False)

    def shutdown(self) -> None:
        if self._is_shutdown:
            return

        try:
            for callback in self._shutdown_callbacks:
                callback()
        finally:
            self._is_shutdown = True
