"""DataVow Airflow integration — lazy import to avoid loading datavow at scheduler parse time."""


def __getattr__(name: str):
    if name == "DataVowOperator":
        from datavow.airflow.operators.datavow_operator import DataVowOperator

        return DataVowOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
