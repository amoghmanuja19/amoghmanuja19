"""Small, dependency-light data quality primitives for portfolio use."""
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def required_columns(actual: Sequence[str], required: Sequence[str]) -> CheckResult:
    missing = sorted(set(required) - set(actual))
    return CheckResult(
        "required_columns",
        not missing,
        "all required columns present" if not missing else f"missing: {missing}",
    )


def no_nulls(values: Iterable[object], column: str) -> CheckResult:
    null_count = sum(value is None for value in values)
    return CheckResult(
        f"not_null:{column}",
        null_count == 0,
        "no nulls" if null_count == 0 else f"null_count={null_count}",
    )


def unique(values: Iterable[object], column: str) -> CheckResult:
    values = list(values)
    duplicate_count = len(values) - len(set(values))
    return CheckResult(
        f"unique:{column}",
        duplicate_count == 0,
        "unique" if duplicate_count == 0 else f"duplicate_count={duplicate_count}",
    )


def accepted_values(values: Iterable[object], allowed: Sequence[object], column: str) -> CheckResult:
    allowed_set = set(allowed)
    invalid = [value for value in values if value not in allowed_set]
    return CheckResult(
        f"accepted_values:{column}",
        not invalid,
        "all values accepted" if not invalid else f"invalid_values={sorted(set(invalid))}",
    )


def quality_gate(results: Sequence[CheckResult]) -> None:
    failures = [result for result in results if not result.passed]
    if failures:
        summary = "; ".join(f"{r.name}: {r.details}" for r in failures)
        raise ValueError(f"Data quality gate failed: {summary}")


if __name__ == "__main__":
    checks = [
        required_columns(["order_id", "status"], ["order_id", "status"]),
        no_nulls(["A", "B", "C"], "order_id"),
        unique(["A", "B", "C"], "order_id"),
        accepted_values(["NEW", "SHIPPED"], ["NEW", "SHIPPED", "CANCELLED"], "status"),
    ]
    quality_gate(checks)
    print("All quality checks passed")
