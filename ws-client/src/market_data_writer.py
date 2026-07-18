import argparse
import asyncio
import logging
import os
import signal
import websockets

PING_INTERVAL = int(os.environ.get("PING_INTERVAL_SECONDS", "20"))
PING_TIMEOUT = int(os.environ.get("PING_TIMEOUT_SECONDS", "10"))

shutdown_event = asyncio.Event()


def parse_args():
    parser = argparse.ArgumentParser(description="Binance market data WebSocket writer")
    parser.add_argument(
        "--symbol",
        default=os.environ.get("SYMBOL", "btcusdt"),
        help="Trading pair symbol)",
    )
    parser.add_argument(
        "--stream-type",
        default=os.environ.get("STREAM_TYPE", "trade"),
        help="Binance stream type",
    )
    parser.add_argument(
        "--ws-url",
        default=os.environ.get("WS_URL", "wss://stream.binance.com:9443/ws"),
        help="Websocket URL",
    )
    parser.add_argument(
        "--log-dir",
        default=os.environ.get("LOG_DIR", "."),
        help="Directory for log files",
    )
    return parser.parse_args()


def create_logger(name, path, fmt, stdout=False):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    if stdout:
        logger.addHandler(logging.StreamHandler())
    logger.propagate = False
    return logger


def initialize_loggers(symbol, log_dir):
    ops_logger = create_logger(
        "ops",
        os.path.join(log_dir, "market_data_writer.log"),
        "%(asctime)s [ops] %(message)s",
        stdout=True,
    )
    event_logger = create_logger(
        "events",
        os.path.join(log_dir, f"{symbol}.log"),
        "%(message)s",
    )
    return ops_logger, event_logger


async def consume(ws_url, ops_logger, event_logger):
    backoff = 1
    while not shutdown_event.is_set():
        try:
            ops_logger.info(f"Connecting to {ws_url}")
            async with websockets.connect(
                ws_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
            ) as ws:
                ops_logger.info(f"Connected (keepalive: ping every {PING_INTERVAL}s, timeout {PING_TIMEOUT}s)")
                backoff = 1
                while not shutdown_event.is_set():
                    message = await ws.recv()
                    event_logger.info(message)
        except websockets.ConnectionClosed as e:
            ops_logger.warning(f"Connection closed ({e}); reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as e:
            ops_logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def handle_shutdown(ops_logger, *_):
    ops_logger.info("Shutdown signal received")
    shutdown_event.set()


async def main():
    args = parse_args()
    symbol = args.symbol.lower()
    ws_url = f"{args.ws_url}/{symbol}@{args.stream_type}"

    ops_logger, event_logger = initialize_loggers(symbol, args.log_dir)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: handle_shutdown(ops_logger))

    ops_logger.info(f"Starting market data writer for symbol={symbol}, stream={args.stream_type}")
    await consume(ws_url, ops_logger, event_logger)


if __name__ == "__main__":
    asyncio.run(main())
