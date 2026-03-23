"""Tests for the DataVow Airflow Operator.

Airflow is NOT installed in the test environment, so we mock the airflow
modules in sys.modules. We also mock datavow.validator, datavow.findings,
and datavow.reporter for isolation — but we do NOT mock the datavow package
itself, because Python needs the real package for import traversal to
datavow.airflow.operators.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Mock Airflow modules before importing the operator
# ---------------------------------------------------------------------------

_airflow_mod = ModuleType("airflow")
_airflow_models = ModuleType("airflow.models")
_airflow_exceptions = ModuleType("airflow.exceptions")


class _AirflowException(Exception):
    pass


class _AirflowSkipException(Exception):
    pass


class _FakeBaseOperator:
    def __init__(self, **kwargs: Any) -> None:
        self.task_id = kwargs.get("task_id", "test_task")


_airflow_models.BaseOperator = _FakeBaseOperator  # type: ignore[attr-defined]
_airflow_exceptions.AirflowException = _AirflowException  # type: ignore[attr-defined]
_airflow_exceptions.AirflowSkipException = _AirflowSkipException  # type: ignore[attr-defined]

sys.modules.setdefault("airflow", _airflow_mod)
sys.modules.setdefault("airflow.models", _airflow_models)
sys.modules.setdefault("airflow.exceptions", _airflow_exceptions)

# ---------------------------------------------------------------------------
# Mock datavow internals (but NOT the datavow package itself)
# ---------------------------------------------------------------------------


class FakeSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class FakeVerdict(str, Enum):
    KEPT = "Vow Kept"
    STRAINED = "Vow Strained"
    BROKEN = "Vow Broken"
    SHATTERED = "Vow Shattered"


@dataclass
class FakeFinding:
    rule: str
    category: str
    severity: FakeSeverity
    message: str
    passed: bool
    details: str = ""


@dataclass
class FakeValidationResult:
    contract_name: str
    source_path: str
    findings: list[FakeFinding] = field(default_factory=list)

    def add(self, finding: FakeFinding) -> None:
        self.findings.append(finding)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == FakeSeverity.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == FakeSeverity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == FakeSeverity.INFO)

    @property
    def score(self) -> int:
        raw = 100 - (20 * self.critical_count + 5 * self.warning_count + 1 * self.info_count)
        return max(0, raw)

    @property
    def verdict(self) -> FakeVerdict:
        s = self.score
        if s >= 95:
            return FakeVerdict.KEPT
        if s >= 80:
            return FakeVerdict.STRAINED
        if s >= 50:
            return FakeVerdict.BROKEN
        return FakeVerdict.SHATTERED


def _make_result(
    *,
    critical: int = 0,
    warning: int = 0,
    info: int = 0,
    name: str = "test_contract",
) -> FakeValidationResult:
    r = FakeValidationResult(contract_name=name, source_path="/data/test.csv")
    for i in range(critical):
        r.add(FakeFinding(f"crit_{i}", "schema", FakeSeverity.CRITICAL, "fail", passed=False))
    for i in range(warning):
        r.add(FakeFinding(f"warn_{i}", "quality", FakeSeverity.WARNING, "fail", passed=False))
    for i in range(info):
        r.add(FakeFinding(f"info_{i}", "quality", FakeSeverity.INFO, "fail", passed=False))
    return r


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_mock_validate = MagicMock()
_mock_write_report = MagicMock(return_value=Path("/tmp/report.html"))
_mock_from_yaml = MagicMock()

_mock_validator_mod = ModuleType("datavow.validator")
_mock_validator_mod.validate = _mock_validate  # type: ignore[attr-defined]

_mock_contract_mod = ModuleType("datavow.contract")
_mock_contract_mod.DataContract = MagicMock()  # type: ignore[attr-defined]
_mock_contract_mod.DataContract.from_yaml = _mock_from_yaml  # type: ignore[attr-defined]
_mock_contract_mod.Severity = FakeSeverity  # type: ignore[attr-defined]

_mock_findings_mod = ModuleType("datavow.findings")
_mock_findings_mod.ValidationResult = FakeValidationResult  # type: ignore[attr-defined]
_mock_findings_mod.Finding = FakeFinding  # type: ignore[attr-defined]
_mock_findings_mod.Verdict = FakeVerdict  # type: ignore[attr-defined]

_mock_reporter_mod = ModuleType("datavow.reporter")
_mock_reporter_mod.write_report = _mock_write_report  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def _patch_modules():
    """Patch datavow sub-modules used by the operator for every test."""
    _mock_validate.reset_mock()
    _mock_write_report.reset_mock()
    _mock_from_yaml.reset_mock()

    # Default: perfect score
    _mock_validate.return_value = _make_result()

    saved = {}
    mods = {
        "datavow.validator": _mock_validator_mod,
        "datavow.contract": _mock_contract_mod,
        "datavow.findings": _mock_findings_mod,
        "datavow.reporter": _mock_reporter_mod,
    }
    for k, v in mods.items():
        saved[k] = sys.modules.get(k)
        sys.modules[k] = v

    # Force reimport of operator module so it picks up mocks
    for key in list(sys.modules):
        if "datavow.airflow" in key:
            del sys.modules[key]

    yield

    for k, v in saved.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v

    for key in list(sys.modules):
        if "datavow.airflow" in key:
            del sys.modules[key]


def _get_operator_class():
    from datavow.airflow.operators.datavow_operator import DataVowOperator

    return DataVowOperator


def _make_context() -> dict[str, Any]:
    ti = MagicMock()
    return {"ti": ti}


def _xcom_dict(context: dict[str, Any]) -> dict[str, Any]:
    ti = context["ti"]
    return {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}


# ===================================================================
# TestParams — 7 tests
# ===================================================================


class TestParams:
    def test_defaults(self):
        Op = _get_operator_class()
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        assert op.on_failure == "fail"
        assert op.fail_on == "strained"
        assert op.report_format is None
        assert op.report_path is None

    def test_custom_params(self):
        Op = _get_operator_class()
        op = Op(
            contract_path="c.yaml",
            data_path="d.csv",
            on_failure="warn",
            fail_on="broken",
            report_format="html",
            report_path="/tmp/r.html",
            task_id="t",
        )
        assert op.on_failure == "warn"
        assert op.fail_on == "broken"
        assert op.report_format == "html"
        assert op.report_path == "/tmp/r.html"

    def test_invalid_on_failure(self):
        Op = _get_operator_class()
        with pytest.raises(ValueError, match="on_failure"):
            Op(contract_path="c.yaml", data_path="d.csv", on_failure="explode", task_id="t")

    def test_invalid_fail_on(self):
        Op = _get_operator_class()
        with pytest.raises(ValueError, match="fail_on"):
            Op(contract_path="c.yaml", data_path="d.csv", fail_on="destroyed", task_id="t")

    def test_template_fields(self):
        Op = _get_operator_class()
        assert "contract_path" in Op.template_fields
        assert "data_path" in Op.template_fields
        assert "report_path" in Op.template_fields

    def test_on_failure_fail_accepted(self):
        Op = _get_operator_class()
        op = Op(contract_path="c.yaml", data_path="d.csv", on_failure="fail", task_id="t")
        assert op.on_failure == "fail"

    def test_on_failure_skip_accepted(self):
        Op = _get_operator_class()
        op = Op(contract_path="c.yaml", data_path="d.csv", on_failure="skip", task_id="t")
        assert op.on_failure == "skip"


# ===================================================================
# TestVowKept — 2 tests
# ===================================================================


class TestVowKept:
    def test_perfect_score_succeeds(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result()  # score=100
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        ctx = _make_context()
        ret = op.execute(ctx)
        assert ret["vow_score"] == 100
        assert ret["vow_verdict"] == "Vow Kept"

    def test_xcom_pushed_on_success(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result()
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        ctx = _make_context()
        op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["vow_score"] == 100
        assert xcoms["vow_verdict"] == "Vow Kept"
        assert xcoms["violations_critical"] == 0
        assert xcoms["violations_warning"] == 0
        assert xcoms["violations_info"] == 0
        assert xcoms["contract_name"] == "test_contract"
        assert xcoms["report_path"] is None


# ===================================================================
# TestFail — 5 tests
# ===================================================================


class TestFail:
    def test_strained_raises(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(warning=2)  # score=90 < 95
        op = Op(contract_path="c.yaml", data_path="d.csv", fail_on="strained", task_id="t")
        with pytest.raises(_AirflowException, match="score 90"):
            op.execute(_make_context())

    def test_broken_raises(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=2)  # score=60 < 80
        op = Op(contract_path="c.yaml", data_path="d.csv", fail_on="broken", task_id="t")
        with pytest.raises(_AirflowException, match="score 60"):
            op.execute(_make_context())

    def test_shattered_raises(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=3)  # score=40 < 50
        op = Op(contract_path="c.yaml", data_path="d.csv", fail_on="shattered", task_id="t")
        with pytest.raises(_AirflowException, match="score 40"):
            op.execute(_make_context())

    def test_score_at_threshold_passes(self):
        """Score exactly at threshold should NOT trigger failure."""
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(warning=1)  # score=95, threshold=95
        op = Op(contract_path="c.yaml", data_path="d.csv", fail_on="strained", task_id="t")
        ret = op.execute(_make_context())
        assert ret["vow_score"] == 95

    def test_xcom_pushed_before_exception(self):
        """XCom values must be pushed even when the task fails."""
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1)  # score=80 < 95
        op = Op(contract_path="c.yaml", data_path="d.csv", fail_on="strained", task_id="t")
        ctx = _make_context()
        with pytest.raises(_AirflowException):
            op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["vow_score"] == 80
        assert xcoms["violations_critical"] == 1


# ===================================================================
# TestWarn — 2 tests
# ===================================================================


class TestWarn:
    def test_warn_does_not_raise(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1)  # score=80 < 95
        op = Op(
            contract_path="c.yaml", data_path="d.csv", on_failure="warn", task_id="t"
        )
        ret = op.execute(_make_context())
        assert ret["vow_score"] == 80

    def test_warn_logs_warning(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1)
        op = Op(
            contract_path="c.yaml", data_path="d.csv", on_failure="warn", task_id="t"
        )
        with patch("datavow.airflow.operators.datavow_operator.log") as mock_log:
            op.execute(_make_context())
            mock_log.warning.assert_called_once()
            assert "score 80" in mock_log.warning.call_args[0][0]


# ===================================================================
# TestSkip — 2 tests
# ===================================================================


class TestSkip:
    def test_skip_raises_skip_exception(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1)  # score=80 < 95
        op = Op(
            contract_path="c.yaml", data_path="d.csv", on_failure="skip", task_id="t"
        )
        with pytest.raises(_AirflowSkipException):
            op.execute(_make_context())

    def test_skip_xcom_pushed(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1)
        op = Op(
            contract_path="c.yaml", data_path="d.csv", on_failure="skip", task_id="t"
        )
        ctx = _make_context()
        with pytest.raises(_AirflowSkipException):
            op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["vow_score"] == 80


# ===================================================================
# TestCounting — 2 tests
# ===================================================================


class TestCounting:
    def test_mixed_violations(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=1, warning=2, info=3)
        # score = 100 - 20 - 10 - 3 = 67
        op = Op(
            contract_path="c.yaml",
            data_path="d.csv",
            on_failure="warn",
            fail_on="shattered",
            task_id="t",
        )
        ctx = _make_context()
        op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["vow_score"] == 67
        assert xcoms["violations_critical"] == 1
        assert xcoms["violations_warning"] == 2
        assert xcoms["violations_info"] == 3

    def test_zero_score_floor(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=10)  # would be -100, floored to 0
        op = Op(
            contract_path="c.yaml",
            data_path="d.csv",
            on_failure="warn",
            fail_on="shattered",
            task_id="t",
        )
        ctx = _make_context()
        op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["vow_score"] == 0
        assert xcoms["vow_verdict"] == "Vow Shattered"


# ===================================================================
# TestReport — 3 tests
# ===================================================================


class TestReport:
    def test_report_generated(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result()
        _mock_write_report.return_value = Path("/tmp/report.html")
        op = Op(
            contract_path="c.yaml",
            data_path="d.csv",
            report_format="html",
            report_path="/tmp/report.html",
            task_id="t",
        )
        ctx = _make_context()
        op.execute(ctx)
        _mock_write_report.assert_called_once()
        xcoms = _xcom_dict(ctx)
        assert xcoms["report_path"] == "/tmp/report.html"

    def test_no_report_without_format(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result()
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        ctx = _make_context()
        op.execute(ctx)
        _mock_write_report.assert_not_called()
        xcoms = _xcom_dict(ctx)
        assert xcoms["report_path"] is None

    def test_markdown_report(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result()
        _mock_write_report.return_value = Path("/tmp/report.md")
        op = Op(
            contract_path="c.yaml",
            data_path="d.csv",
            report_format="markdown",
            report_path="/tmp/report.md",
            task_id="t",
        )
        ctx = _make_context()
        op.execute(ctx)
        call_kwargs = _mock_write_report.call_args
        assert call_kwargs.kwargs.get("format") == "markdown" or call_kwargs[1].get(
            "format"
        ) == "markdown"


# ===================================================================
# TestEdge — 4 tests
# ===================================================================


class TestEdge:
    def test_contract_name_in_xcom(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(name="my_contract")
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        ctx = _make_context()
        op.execute(ctx)
        xcoms = _xcom_dict(ctx)
        assert xcoms["contract_name"] == "my_contract"

    def test_broken_fail_on_with_kept_score(self):
        """fail_on=broken should pass when score >= 80."""
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(warning=1)  # score=95
        op = Op(
            contract_path="c.yaml", data_path="d.csv", fail_on="broken", task_id="t"
        )
        ret = op.execute(_make_context())
        assert ret["vow_score"] == 95

    def test_shattered_fail_on_lets_broken_pass(self):
        """fail_on=shattered should pass even when score is in broken range."""
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(critical=2)  # score=60
        op = Op(
            contract_path="c.yaml", data_path="d.csv", fail_on="shattered", task_id="t"
        )
        ret = op.execute(_make_context())
        assert ret["vow_score"] == 60

    def test_return_value(self):
        Op = _get_operator_class()
        _mock_validate.return_value = _make_result(name="edge_contract")
        op = Op(contract_path="c.yaml", data_path="d.csv", task_id="t")
        ret = op.execute(_make_context())
        assert ret == {
            "vow_score": 100,
            "vow_verdict": "Vow Kept",
            "contract_name": "edge_contract",
        }
