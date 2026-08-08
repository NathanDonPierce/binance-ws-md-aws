
FROM python:3.12-slim

RUN useradd --create-home --shell /bin/bash arbitrator
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

USER arbitrator

ENV PYTHONUNBUFFERED=1

EXPOSE 8080

WORKDIR /app/src
CMD ["python", "arbitrator.py"]