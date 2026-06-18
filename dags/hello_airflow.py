"""
Найпростіший DAG для перевірки, що Airflow живий.
Нічого не пише в БД, не ходить у мережу — лише логує.
Запускати вручну (Trigger) з UI. schedule=None = за розкладом не стартує сам.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="hello_airflow",
    description="Перший тестовий DAG — просто логує hello",
    schedule=None,                       # тільки ручний запуск
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Kyiv"),
    catchup=False,
    tags=["demo", "smoke-test"],
)
def hello_airflow():

    @task
    def say_hello() -> str:
        msg = "Hello, Airflow! 👋"
        print(msg)                       # потрапить у логи задачі
        return msg

    @task
    def show_context(greeting: str, **context) -> None:
        # context містить службові поля запуску; беремо безпечно через .get()
        print(f"Отримав від попередньої задачі: {greeting}")
        print(f"logical_date цього запуску: {context.get('logical_date')}")
        print(f"run_id: {context.get('run_id')}")
        print(f"dag_id: {context.get('dag').dag_id if context.get('dag') else 'n/a'}")

    # граф: спершу say_hello, його результат → show_context
    show_context(say_hello())


hello_airflow()
