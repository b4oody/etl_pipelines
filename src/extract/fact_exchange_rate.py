import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def extract_exchange_rates(rate_date=None, timeout=30, retries=3):
    """
    Витяг курсів валют з API

    Args:
        timeout: timeout для запиту (сек)
        retries: кількість повторів при помилці

    Returns:
        list: Список курсів валют
    """
    last_error = None
    params = {"json": ""}

    if rate_date is not None:
        if isinstance(rate_date, str):
            rate_date = datetime.strptime(rate_date, "%Y-%m-%d").date()
        params["date"] = rate_date.strftime("%Y%m%d")

    for attempt in range(retries):
        try:
            logger.info(f"Extracting exchange rates for {rate_date or 'today'} (attempt {attempt + 1}/{retries})...")

            response = requests.get(
                "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange",
                params=params,
                timeout=timeout,
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"[OK] Extracted {len(data)} exchange rates for {rate_date or 'today'}")

            return data

        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning(f"[WARN] Attempt {attempt + 1} failed: {e}")

            if attempt < retries - 1:
                continue

    logger.error(f"[ERROR] Extract failed after {retries} attempts: {last_error}")
    raise last_error
