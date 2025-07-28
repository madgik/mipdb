import logging
import sys

logging.basicConfig(
    stream=sys.stderr, level=logging.INFO, format="%(levelname)s: %(message)s"
)
LOGGER = logging.getLogger(__name__)
