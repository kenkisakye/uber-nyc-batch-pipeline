# Scheduler: starts the monthly and quarterly cycles on a timetable.
ARG PYTHON_IMAGE=python:3.11-slim-bookworm
FROM ${PYTHON_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY pipeline.py scheduler.py ./

# Unprivileged user; /state holds the job store and the cycle lock
RUN useradd --create-home --uid 10001 pipeline \
 && mkdir /state && chown pipeline /state
USER pipeline

ENTRYPOINT ["python", "/app/scheduler.py"]
