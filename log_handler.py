import logging
import os
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk.resources import Resource

SERVICE_NAME = "purelens-events-documents-producer"
_provider:LoggerProvider |None = None

def _get_otel_provider() -> LoggerProvider:
    global _provider
    if _provider is None:
        # Also reads OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES from env
        resource = Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", SERVICE_NAME)})
        _provider = LoggerProvider(resource=resource)
        # Endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT
        _provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
        set_logger_provider(_provider)
    return _provider

# set_logger_provider(LoggerProvider())
def get_logger(name: str = SERVICE_NAME) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        stream = logging.StreamHandler()
        stream.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        logger.addHandler(stream)

        logger.addHandler(LoggingHandler(level=logging.NOTSET, logger_provider=_get_otel_provider()))

    logger.propagate = False
    return logger

def shutdown_telemetry() -> None:
    if _provider is not None:
        _provider.shutdown()
logger = get_logger()
