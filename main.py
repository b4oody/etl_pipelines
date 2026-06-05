import sys

from src.pipelines.dim_currency import run_dim_currency_etl
from src.pipelines.fact_exchange_rate import run_fact_exchange_rate_etl
from src.pipelines.jsonplaceholder import run_jsonplaceholder_etl


def run_all():
    results = {}

    results["jsonplaceholder"] = run_jsonplaceholder_etl()
    results["dim_currency"] = run_dim_currency_etl()
    results["fact_exchange_rate"] = run_fact_exchange_rate_etl(start_date="2024-01-01", end_date="2024-12-31")

    return results


if __name__ == "__main__":
    results = run_all()
    print(results)

    has_failed_pipeline = any(result["status"] != "success" for result in results.values())
    sys.exit(1 if has_failed_pipeline else 0)
