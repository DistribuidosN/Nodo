from __future__ import annotations

from collections.abc import Iterable
from contextlib import nullcontext
from typing import Any

try:
    from opentelemetry import propagate, trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    propagate = None
    trace = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    OTEL_AVAILABLE = False

TRACEPARENT_KEY = "_traceparent"
TRACESTATE_KEY = "_tracestate"
INTERNAL_TRACE_KEYS = {TRACEPARENT_KEY, TRACESTATE_KEY}


class _Getter:
    def get(self, carrier: dict[str, str], key: str):
        value = carrier.get(key)
        return [value] if value is not None else []

    def keys(self, carrier: dict[str, str]):
        return carrier.keys()


class _Setter:
    def set(self, carrier: dict[str, str], key: str, value: str) -> None:
        carrier[key] = value


_GETTER = _Getter()
_SETTER = _Setter()
_TRACING_CONFIGURED = False


def configure_tracing(config) -> None:
    global _TRACING_CONFIGURED
    if _TRACING_CONFIGURED or not config.tracing_enabled or not OTEL_AVAILABLE:
        return

    resource = Resource.create(
        {
            "service.name": config.tracing_service_name or "distributed-image-worker",
            "service.instance.id": config.node_id,
        }
    )
    provider = TracerProvider(resource=resource)
    if config.tracing_otlp_endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter(endpoint=config.tracing_otlp_endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _TRACING_CONFIGURED = True


def shutdown_tracing() -> None:
    if not OTEL_AVAILABLE:
        return
    provider = trace.get_tracer_provider()
    shutdown = getattr(provider, "shutdown", None)
    if callable(shutdown):
        shutdown()


def get_tracer(name: str):
    if not OTEL_AVAILABLE:
        return None
    return trace.get_tracer(name)


def start_span(name: str, *, context=None, attributes: dict[str, Any] | None = None):
    if not OTEL_AVAILABLE:
        return nullcontext()
    tracer = get_tracer("worker")
    return tracer.start_as_current_span(name, context=context, attributes=attributes)


def extract_context_from_grpc_metadata(metadata: Iterable[tuple[str, str]] | Any) -> Any:
    if not OTEL_AVAILABLE:
        return None
    carrier: dict[str, str] = {}
    for item in metadata:
        key = getattr(item, "key", item[0]).lower()
        value = getattr(item, "value", item[1])
        carrier[key] = value
    return propagate.extract(carrier, getter=_GETTER)


def copy_internal_trace_metadata_from_grpc(target: dict[str, str], metadata: Iterable[tuple[str, str]] | Any) -> None:
    for item in metadata:
        key = getattr(item, "key", item[0]).lower()
        value = getattr(item, "value", item[1])
        if key == "traceparent":
            target[TRACEPARENT_KEY] = value
        elif key == "tracestate":
            target[TRACESTATE_KEY] = value


def inject_current_context(target: dict[str, str]) -> None:
    if not OTEL_AVAILABLE:
        return
    carrier: dict[str, str] = {}
    propagate.inject(carrier, setter=_SETTER)
    if "traceparent" in carrier:
        target[TRACEPARENT_KEY] = carrier["traceparent"]
    if "tracestate" in carrier:
        target[TRACESTATE_KEY] = carrier["tracestate"]


def extract_context_from_internal_metadata(metadata: dict[str, str] | None) -> Any:
    if not OTEL_AVAILABLE:
        return None
    if not metadata:
        return None
    carrier: dict[str, str] = {}
    if TRACEPARENT_KEY in metadata:
        carrier["traceparent"] = metadata[TRACEPARENT_KEY]
    if TRACESTATE_KEY in metadata:
        carrier["tracestate"] = metadata[TRACESTATE_KEY]
    if not carrier:
        return None
    return propagate.extract(carrier, getter=_GETTER)


def grpc_metadata_from_internal(metadata: dict[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not metadata:
        return ()
    pairs: list[tuple[str, str]] = []
    if traceparent := metadata.get(TRACEPARENT_KEY):
        pairs.append(("traceparent", traceparent))
    if tracestate := metadata.get(TRACESTATE_KEY):
        pairs.append(("tracestate", tracestate))
    return tuple(pairs)


def copy_internal_trace_metadata(source: dict[str, str] | None, target: dict[str, str]) -> None:
    if not source:
        return
    if traceparent := source.get(TRACEPARENT_KEY):
        target[TRACEPARENT_KEY] = traceparent
    if tracestate := source.get(TRACESTATE_KEY):
        target[TRACESTATE_KEY] = tracestate


def strip_internal_trace_metadata(metadata: dict[str, str] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {key: value for key, value in metadata.items() if key not in INTERNAL_TRACE_KEYS}


def current_trace_ids() -> dict[str, str]:
    if not OTEL_AVAILABLE:
        return {}
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if not span_context.is_valid:
        return {}
    return {
        "trace_id": format(span_context.trace_id, "032x"),
        "span_id": format(span_context.span_id, "016x"),
    }


def maybe_span(name: str, *, enabled: bool = True, context=None, attributes: dict[str, Any] | None = None):
    if not enabled:
        return nullcontext()
    return start_span(name, context=context, attributes=attributes)
