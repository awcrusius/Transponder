# Runtime image for the ingestion service. feeds.yaml is not baked in; the
# Compose file bind-mounts it at /app/feeds.yaml (FEEDS_CONFIG) so it can be
# edited without a rebuild. Migrations are baked in and applied on start.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FEEDS_CONFIG=/app/feeds.yaml \
    MIGRATIONS_DIR=/app/migrations

WORKDIR /app
COPY pyproject.toml README.md ./
COPY transponder ./transponder
COPY migrations ./migrations
RUN pip install .

CMD ["transponder", "run"]
