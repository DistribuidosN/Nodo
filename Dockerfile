FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WORKER_BIND_HOST=0.0.0.0 \
    WORKER_METRICS_HOST=0.0.0.0 \
    WORKER_HEALTH_HOST=0.0.0.0

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl tesseract-ocr tesseract-ocr-spa \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY coordinator ./coordinator
COPY proto ./proto
COPY worker ./worker
COPY examples ./examples
COPY scripts ./scripts

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data/out /app/data/state \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 50051 8081 9100

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD python scripts/healthcheck.py ready

CMD ["python", "-m", "worker"]
