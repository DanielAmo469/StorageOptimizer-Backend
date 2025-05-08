import os
import time

def safe_stat(path, retries=5, delay=0.2):
    """
    Retry os.stat with a short delay to handle Windows file system lag.
    """
    for attempt in range(retries):
        try:
            return os.stat(path)
        except FileNotFoundError:
            time.sleep(delay)
    raise FileNotFoundError(f"File not found after {retries} retries: {path}")
