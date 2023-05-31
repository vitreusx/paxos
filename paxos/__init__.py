import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)-4s - %(name)-7s: %(message)s",
    datefmt="%H:%M:%S",
)
