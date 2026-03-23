"""DataVow Airflow Operator — runs data contract validation as an Airflow task."""

from __future__ import annotations

import logging
from typing import Any

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator

log = logging.getLogger(__name__)

_FAIL_ON_THRESHOLDS = {
    "strained": 95,
    "broken": 80,
    "shattered": 50,
}


class DataVowOperator(BaseOperator):
    """Run DataVow contract validation and push results to XCom.

    Parameters
    ----------
    contract_path : str
        Path to the YAML data contract file (Jinja-templated).
    data_path : str
        Path to the data file to validate (Jinja-templated).
    on_failure : str
        Action on validation failure: ``"fail"`` (raise), ``"warn"`` (log),
        or ``"skip"`` (skip task). Default ``"fail"``.
    fail_on : str
        Verdict threshold that triggers failure: ``"strained"`` (score < 95),
        ``"broken"`` (score < 80), or ``"shattered"`` (score < 50).
        Default ``"strained"``.
    report_format : str | None
        Report format: ``"html"``, ``"markdown"``/``"md"``, or ``None`` to skip.
    report_path : str | None
        Output path for the report (Jinja-templated).
    """

    template_fields = ("contract_path", "data_path", "report_path")

    def __init__(
        self,
        *,
        contract_path: str,
        data_path: str,
        on_failure: str = "fail",
        fail_on: str = "strained",
        report_format: str | None = None,
        report_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if on_failure not in ("fail", "warn", "skip"):
            raise ValueError(f"on_failure must be 'fail', 'warn', or 'skip', got {on_failure!r}")
        if fail_on not in _FAIL_ON_THRESHOLDS:
            raise ValueError(
                f"fail_on must be 'strained', 'broken', or 'shattered', got {fail_on!r}"
            )
        self.contract_path = contract_path
        self.data_path = data_path
        self.on_failure = on_failure
        self.fail_on = fail_on
        self.report_format = report_format
        self.report_path = report_path

    def execute(self, context: Any) -> dict[str, Any]:
        from datavow.contract import DataContract
        from datavow.reporter import write_report
        from datavow.validator import validate

        log.info("DataVow validating %s against %s", self.data_path, self.contract_path)

        result = validate(self.contract_path, self.data_path)

        # Generate report if requested
        actual_report_path: str | None = None
        if self.report_format and self.report_path:
            written = write_report(
                DataContract.from_yaml(self.contract_path),
                result,
                self.report_path,
                format=self.report_format,
            )
            actual_report_path = str(written)

        # Push XCom values
        ti = context["ti"]
        ti.xcom_push(key="vow_score", value=result.score)
        ti.xcom_push(key="vow_verdict", value=result.verdict.value)
        ti.xcom_push(key="violations_critical", value=result.critical_count)
        ti.xcom_push(key="violations_warning", value=result.warning_count)
        ti.xcom_push(key="violations_info", value=result.info_count)
        ti.xcom_push(key="contract_name", value=result.contract_name)
        ti.xcom_push(key="report_path", value=actual_report_path)

        log.info(
            "DataVow result: score=%d verdict=%s (critical=%d warning=%d info=%d)",
            result.score,
            result.verdict.value,
            result.critical_count,
            result.warning_count,
            result.info_count,
        )

        # Check threshold
        threshold = _FAIL_ON_THRESHOLDS[self.fail_on]
        if result.score < threshold:
            msg = (
                f"DataVow validation failed: score {result.score} < {threshold} "
                f"({self.fail_on}), verdict: {result.verdict.value}"
            )
            if self.on_failure == "fail":
                raise AirflowException(msg)
            if self.on_failure == "skip":
                raise AirflowSkipException(msg)
            # on_failure == "warn"
            log.warning(msg)

        return {
            "vow_score": result.score,
            "vow_verdict": result.verdict.value,
            "contract_name": result.contract_name,
        }
