"""Parser for Lambda REPORT lines from CloudWatch logs."""

import re
from datetime import datetime, timezone

from thaw.models import InvocationReport

# Regex pattern for parsing REPORT lines
# Example: REPORT RequestId: abc-123 Duration: 45.67 ms Billed Duration: 46 ms
#          Memory Size: 512 MB Max Memory Used: 128 MB Init Duration: 234.56 ms
REPORT_PATTERN = re.compile(
    r"REPORT\s+"
    r"RequestId:\s*(?P<request_id>\S+)\s+"
    r"Duration:\s*(?P<duration>[\d.]+)\s*ms\s+"
    r"Billed Duration:\s*(?P<billed>\d+)\s*ms\s+"
    r"Memory Size:\s*(?P<memory_size>\d+)\s*MB\s+"
    r"Max Memory Used:\s*(?P<memory_used>\d+)\s*MB"
    r"(?:\s+Init Duration:\s*(?P<init>[\d.]+)\s*ms)?"
    r"(?:\s+Restore Duration:\s*(?P<restore>[\d.]+)\s*ms)?"
)


def parse_report_line(message: str, timestamp_ms: int) -> InvocationReport | None:
    """
    Parse a CloudWatch log message containing a REPORT line.

    Args:
        message: The log message text
        timestamp_ms: The timestamp in milliseconds since epoch

    Returns:
        InvocationReport if the message contains a valid REPORT line, None otherwise
    """
    match = REPORT_PATTERN.search(message)
    if not match:
        return None

    groups = match.groupdict()

    return InvocationReport(
        request_id=groups["request_id"],
        timestamp=datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
        duration_ms=float(groups["duration"]),
        billed_duration_ms=int(groups["billed"]),
        memory_size_mb=int(groups["memory_size"]),
        max_memory_used_mb=int(groups["memory_used"]),
        init_duration_ms=float(groups["init"]) if groups["init"] else None,
        restore_duration_ms=float(groups["restore"]) if groups["restore"] else None,
    )


def parse_report_lines(
    events: list[dict],
) -> list[InvocationReport]:
    """
    Parse multiple CloudWatch log events into InvocationReports.

    Args:
        events: List of CloudWatch log events with 'message' and 'timestamp' keys

    Returns:
        List of parsed InvocationReport objects
    """
    reports = []
    for event in events:
        report = parse_report_line(event["message"], event["timestamp"])
        if report:
            reports.append(report)
    return reports
