# confluent-kafka's Linux wheels bundle librdkafka, so the slim base needs no
# additional system libraries.
FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash reaper
WORKDIR /app

# Dependencies first, so the install layer caches independently of source edits.
COPY reaper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./src/
COPY reaper/src/ ./src/

USER reaper

# Without this, Python buffers stdout and log lines only appear when the
# process exits — which makes a running pod effectively undebuggable.
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

WORKDIR /app/src
CMD ["python", "reaper.py"]
