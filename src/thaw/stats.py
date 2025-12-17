"""Statistical calculations for Lambda invocation data."""

import statistics
from datetime import datetime

from thaw.models import AnalysisResult, DistributionStats, InvocationReport


def calculate_percentile(sorted_values: list[float], percentile: float) -> float:
    """
    Calculate a percentile from a sorted list of values.

    Uses linear interpolation between closest ranks.
    """
    if not sorted_values:
        return 0.0

    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]

    # Calculate rank
    rank = (percentile / 100) * (n - 1)
    lower_idx = int(rank)
    upper_idx = lower_idx + 1
    fraction = rank - lower_idx

    if upper_idx >= n:
        return sorted_values[-1]

    return sorted_values[lower_idx] + fraction * (sorted_values[upper_idx] - sorted_values[lower_idx])


def calculate_distribution_stats(values: list[float]) -> DistributionStats | None:
    """
    Calculate distribution statistics for a list of values.

    Returns None if the list is empty.
    """
    if not values:
        return None

    sorted_values = sorted(values)
    n = len(sorted_values)

    # Calculate standard deviation (use population stdev for consistency)
    if n == 1:
        std_dev = 0.0
    else:
        std_dev = statistics.stdev(sorted_values)

    return DistributionStats(
        count=n,
        mean=statistics.mean(sorted_values),
        median=statistics.median(sorted_values),
        std_dev=std_dev,
        min=sorted_values[0],
        max=sorted_values[-1],
        p50=calculate_percentile(sorted_values, 50),
        p90=calculate_percentile(sorted_values, 90),
        p95=calculate_percentile(sorted_values, 95),
        p99=calculate_percentile(sorted_values, 99),
    )


def analyze_reports(
    function_name: str,
    reports: list[InvocationReport],
    start_time: datetime,
    end_time: datetime,
) -> AnalysisResult:
    """
    Analyze a list of invocation reports and compute statistics.

    Args:
        function_name: Name of the Lambda function
        reports: List of InvocationReport objects
        start_time: Start of the analysis time range
        end_time: End of the analysis time range

    Returns:
        AnalysisResult with computed statistics
    """
    # Extract values for each metric
    durations = [r.duration_ms for r in reports]
    billed_durations = [float(r.billed_duration_ms) for r in reports]
    memory_used = [float(r.max_memory_used_mb) for r in reports]

    # Cold start analysis
    cold_starts = [r for r in reports if r.is_cold_start]
    cold_start_durations = [r.init_duration_ms for r in cold_starts]

    # SnapStart analysis
    snapstart_restores = [r for r in reports if r.is_snapstart_restore]
    snapstart_durations = [r.restore_duration_ms for r in snapstart_restores]

    total_count = len(reports)

    return AnalysisResult(
        function_name=function_name,
        start_time=start_time,
        end_time=end_time,
        invocations=reports,
        duration_stats=calculate_distribution_stats(durations)
        or DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        billed_duration_stats=calculate_distribution_stats(billed_durations)
        or DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        memory_used_stats=calculate_distribution_stats(memory_used)
        or DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        cold_start_count=len(cold_starts),
        cold_start_rate=len(cold_starts) / total_count if total_count > 0 else 0.0,
        cold_start_duration_stats=calculate_distribution_stats(cold_start_durations),
        snapstart_restore_count=len(snapstart_restores),
        snapstart_restore_rate=len(snapstart_restores) / total_count if total_count > 0 else 0.0,
        snapstart_restore_duration_stats=calculate_distribution_stats(snapstart_durations),
    )
