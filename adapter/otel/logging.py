from opentelemetry._logs import Logger as OtelApiLogger
from opentelemetry._logs import SeverityNumber

from usecase.interface import Attrs

_LEVELS = {
    'DEBUG': 10,
    'INFO': 20,
    'WARN': 30,
    'WARNING': 30,
    'ERROR': 40,
    'FATAL': 50,
    'CRITICAL': 50,
}


class OtelLogger:
    def __init__(self, logger: OtelApiLogger, level: str) -> None:
        self._logger = logger
        self._threshold = _log_level_value(level)

    def debug(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit('DEBUG', SeverityNumber.DEBUG, _LEVELS['DEBUG'], message, attrs)

    def info(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit('INFO', SeverityNumber.INFO, _LEVELS['INFO'], message, attrs)

    def warning(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit('WARN', SeverityNumber.WARN, _LEVELS['WARNING'], message, attrs)

    def error(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit('ERROR', SeverityNumber.ERROR, _LEVELS['ERROR'], message, attrs)

    def _emit(
        self,
        severity_text: str,
        severity_number: SeverityNumber,
        level: int,
        message: str,
        attrs: Attrs | None,
    ) -> None:
        if level < self._threshold:
            return

        self._logger.emit(
            severity_text=severity_text,
            severity_number=severity_number,
            body=message,
            attributes=None if attrs is None else dict(attrs),
        )


def _log_level_value(level: str) -> int:
    normalized = level.strip().upper()
    if normalized not in _LEVELS:
        raise ValueError('invalid log level')
    return _LEVELS[normalized]
