import logging
import sys

import structlog
from opentelemetry import trace
from structlog.types import EventDict, Processor, WrappedLogger


def add_otel_context(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Добавляет trace_id и span_id активного спана OpenTelemetry."""
    span = trace.get_current_span()
    if span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = trace.format_trace_id(ctx.trace_id)
        event_dict["span_id"] = trace.format_span_id(ctx.span_id)
    else:
        # Оставляем пустые строки для стабильной схемы колонок ClickHouse
        event_dict["trace_id"] = ""
        event_dict["span_id"] = ""
    return event_dict


def rename_event_to_message(
    _: WrappedLogger, __: str, event_dict: EventDict
) -> EventDict:
    """Переименовывает стандартный ключ 'event' в
    'message' под схему ClickHouse/Grafana."""
    if "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict


def setup_logging(
    env: str = "local",
    log_level: str = "INFO",
    service_name: str = "unknown-service",
) -> None:
    is_prod = env.lower() in ("prod", "production")
    level = getattr(logging, log_level.upper(), logging.INFO)

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        add_otel_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if is_prod:
        # Добавляем имя сервиса в каждый лог для фильтрации в Grafana/ClickHouse
        def add_service_name(_: WrappedLogger, __: str, ed: EventDict) -> EventDict:
            ed["service_name"] = service_name
            return ed

        prod_processors = shared_processors + [
            add_service_name,
            structlog.processors.format_exc_info,  # Строковый traceback для ClickHouse
            rename_event_to_message,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ]
        renderer_chain = prod_processors
    else:
        renderer_chain = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            *(renderer_chain[len(shared_processors) :]),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lib_logger = logging.getLogger(logger_name)
        lib_logger.handlers.clear()
        lib_logger.propagate = True
