import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/debug.log"),
        # logging.StreamHandler()
    ]
)

rootLogger = logging.getLogger()
