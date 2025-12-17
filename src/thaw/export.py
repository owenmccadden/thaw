"""Export functionality for analysis results."""

import csv
from pathlib import Path

from thaw.models import AnalysisResult, InvocationReport


def export_to_csv(result: AnalysisResult, output_path: str | Path) -> None:
    """
    Export invocation data to a CSV file.

    Args:
        result: AnalysisResult containing invocations to export
        output_path: Path to write the CSV file
    """
    output_path = Path(output_path)

    fieldnames = [
        "timestamp",
        "request_id",
        "duration_ms",
        "billed_duration_ms",
        "memory_size_mb",
        "max_memory_used_mb",
        "init_duration_ms",
        "restore_duration_ms",
        "is_cold_start",
        "is_snapstart_restore",
    ]

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for report in result.invocations:
            writer.writerow({
                "timestamp": report.timestamp.isoformat(),
                "request_id": report.request_id,
                "duration_ms": report.duration_ms,
                "billed_duration_ms": report.billed_duration_ms,
                "memory_size_mb": report.memory_size_mb,
                "max_memory_used_mb": report.max_memory_used_mb,
                "init_duration_ms": report.init_duration_ms or "",
                "restore_duration_ms": report.restore_duration_ms or "",
                "is_cold_start": report.is_cold_start,
                "is_snapstart_restore": report.is_snapstart_restore,
            })


def export_reports_to_csv(reports: list[InvocationReport], output_path: str | Path) -> None:
    """
    Export raw invocation reports to a CSV file.

    Args:
        reports: List of InvocationReport objects
        output_path: Path to write the CSV file
    """
    output_path = Path(output_path)

    fieldnames = [
        "timestamp",
        "request_id",
        "duration_ms",
        "billed_duration_ms",
        "memory_size_mb",
        "max_memory_used_mb",
        "init_duration_ms",
        "restore_duration_ms",
        "is_cold_start",
        "is_snapstart_restore",
    ]

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for report in reports:
            writer.writerow({
                "timestamp": report.timestamp.isoformat(),
                "request_id": report.request_id,
                "duration_ms": report.duration_ms,
                "billed_duration_ms": report.billed_duration_ms,
                "memory_size_mb": report.memory_size_mb,
                "max_memory_used_mb": report.max_memory_used_mb,
                "init_duration_ms": report.init_duration_ms or "",
                "restore_duration_ms": report.restore_duration_ms or "",
                "is_cold_start": report.is_cold_start,
                "is_snapstart_restore": report.is_snapstart_restore,
            })
