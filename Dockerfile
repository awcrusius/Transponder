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
