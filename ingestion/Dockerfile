# Ingestion service: loads monthly Uber CSV files into the raw bucket.
ARG PYTHON_IMAGE=python:3.11-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY ingest.py .

# Run as an unprivileged user
RUN useradd --create-home --uid 10001 pipeline
USER pipeline

ENTRYPOINT ["python", "/app/ingest.py"]
