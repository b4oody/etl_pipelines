import requests
import logging

logger = logging.getLogger(__name__)

def extract_posts(timeout=30, retries=3):
    """
    Витяг постів з API

    Args:
        timeout: timeout для запиту (сек)
        retries: кількість повторів при помилці

    Returns:
        list: Список постів
    """
    last_error = None

    for attempt in range(retries):
        try:
            logger.info(f"Extracting posts (attempt {attempt + 1}/{retries})...")

            response = requests.get(
                "https://jsonplaceholder.typicode.com/posts",
                timeout=timeout
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"[OK] Extracted {len(data)} posts")

            return data

        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning(f"[WARN] Attempt {attempt + 1} failed: {e}")

            if attempt < retries - 1:
                continue

    logger.error(f"[ERROR] Extract failed after {retries} attempts: {last_error}")
    raise last_error