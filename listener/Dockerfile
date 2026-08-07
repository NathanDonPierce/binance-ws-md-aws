# Slim Python base; confluent-kafka ships with librdkafka bundled in the wheel,
# so no OS-level librdkafka install needed.
FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash listener
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER listener

ENV PYTHONUNBUFFERED=1

WORKDIR /app/src
CMD ["python", "listener.py"]
