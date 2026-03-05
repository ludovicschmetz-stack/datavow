"""Validation findings and scoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from datavow.contract import Severity


class Verdict(str, Enum):
    """Vow verdict based on score."""

    KEPT = "Vow Kept"
    STRAINED = "Vow Strained"
    BROKEN = "Vow Broken"
    SHATTERED = "Vow Shattered"

    @property
    def emoji(self) -> str:
        return {
            Verdict.KEPT: "✅",
            Verdict.STRAINED: "⚠️",
            Verdict.BROKEN: "🔧",
            Verdict.SHATTERED: "❌",
        }[self]

    @property
    def description(self) -> str:
        return {
            Verdict.KEPT: "fully compliant",
            Verdict.STRAINED: "action needed",
            Verdict.BROKEN: "blocking issues",
            Verdict.SHATTERED: "critical violations",
        }[self]


@dataclass
class Finding:
    """Single validation finding."""

    rule: str
    category: str
    severity: Severity
    message: str
    passed: bool
    details: str = ""

    @property
    def status_icon(self) -> str:
        return "✓" if self.passed else "✗"


@dataclass
class ValidationResult:
    """Aggregated validation result with scoring."""

    contract_name: str
    source_path: str
    findings: list[Finding] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.INFO)

    @property
    def passed_count(self) -> int:
        return sum(1 for f in self.findings if f.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed)

    @property
    def score(self) -> int:
        """Vow Score = 100 - (20×CRITICAL + 5×WARNING + 1×INFO), min 0."""
        raw = 100 - (20 * self.critical_count + 5 * self.warning_count + 1 * self.info_count)
        return max(0, raw)

    @property
    def verdict(self) -> Verdict:
        s = self.score
        if s >= 95:
            return Verdict.KEPT
        if s >= 80:
            return Verdict.STRAINED
        if s >= 50:
            return Verdict.BROKEN
        return Verdict.SHATTERED

    @property
    def has_critical_failures(self) -> bool:
        return self.critical_count > 0
