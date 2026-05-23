import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import override

from usecase.interface import Attrs, AttrValue

_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'FATAL': logging.CRITICAL,
    'CRITICAL': logging.CRITICAL,
}


class BaseLogger:
    def __init__(self, logger: logging.Logger, handler: logging.Handler) -> None:
        self._logger = logger
        self._handler = handler

    def debug(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit(logging.DEBUG, message, attrs)

    def info(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit(logging.INFO, message, attrs)

    def warning(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit(logging.WARNING, message, attrs)

    def error(self, message: str, attrs: Attrs | None = None) -> None:
        self._emit(logging.ERROR, message, attrs)

    def shutdown(self) -> None:
        self._logger.removeHandler(self._handler)
        try:
            self._handler.close()
        except (OSError, ValueError):
            return

    def _emit(self, level: int, message: str, attrs: Attrs | None) -> None:
        extra = {'attrs': None if attrs is None else dict(attrs)}
        self._logger.log(level, message, extra=extra)


class StdoutLogger(BaseLogger):
    def __init__(self, name: str, level: str, output_format: str) -> None:
        handler = SafeStreamHandler(sys.stdout)
        super().__init__(_build_logger(name, level, output_format, handler), handler)


class FileLogger(BaseLogger):
    def __init__(self, path: str, name: str, level: str, output_format: str) -> None:
        file_path = Path(path)
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            handler = SafeFileHandler(file_path, mode='a', encoding='utf-8')
        except OSError as exc:
            raise ValueError('invalid logging.file_path') from exc

        try:
            logger = _build_logger(name, level, output_format, handler)
        except Exception:
            handler.close()
            raise

        super().__init__(logger, handler)


class SafeStreamHandler(logging.StreamHandler):
    @override
    def handleError(self, record: logging.LogRecord) -> None:
        return None


class SafeFileHandler(logging.FileHandler):
    @override
    def handleError(self, record: logging.LogRecord) -> None:
        return None


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        parts = [_timestamp(record.created), record.levelname, record.getMessage()]
        attrs = _record_attrs(record)
        if attrs is not None:
            for key in sorted(attrs):
                parts.append(f'{key}={json.dumps(attrs[key], separators=(",", ":"))}')
        return ' '.join(parts)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            'timestamp': _timestamp(record.created),
            'level': record.levelname,
            'message': record.getMessage(),
        }
        attrs = _record_attrs(record)
        if attrs is not None:
            payload['attrs'] = attrs
        return json.dumps(payload, separators=(',', ':'), sort_keys=True)


def _build_logger(
    name: str,
    level: str,
    output_format: str,
    handler: logging.Handler,
) -> logging.Logger:
    logger = logging.Logger(name)
    logger.setLevel(_log_level_value(level))
    logger.propagate = False
    handler.setLevel(_log_level_value(level))
    handler.setFormatter(_formatter(output_format))
    logger.addHandler(handler)
    return logger


def _formatter(output_format: str) -> logging.Formatter:
    normalized = output_format.strip().lower()
    if normalized not in {'text', 'json'}:
        raise ValueError('invalid log format')
    if normalized == 'json':
        return JsonFormatter()
    return TextFormatter()


def _record_attrs(record: logging.LogRecord) -> dict[str, AttrValue] | None:
    attrs = getattr(record, 'attrs', None)
    if not isinstance(attrs, dict):
        return None
    return attrs


def _timestamp(created: float) -> str:
    return (
        datetime.fromtimestamp(created, tz=UTC)
        .isoformat(timespec='milliseconds')
        .replace('+00:00', 'Z')
    )


def _log_level_value(level: str) -> int:
    normalized = level.strip().upper()
    if normalized not in _LEVELS:
        raise ValueError('invalid log level')
    return _LEVELS[normalized]
