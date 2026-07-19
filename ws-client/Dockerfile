FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/websocket_market_data_listener.py .

RUN useradd --create-home appuser && \
    mkdir -p /app/logs && \
    chown -R appuser:appuser /app
USER appuser

ENV SYMBOL=btcusdt
ENV STREAM_TYPE=trade
ENV LOG_DIR=/app/logs
ENV PING_INTERVAL_SECONDS=20
ENV PING_TIMEOUT_SECONDS=10

CMD ["python3", "websocket_market_data_listener.py"]