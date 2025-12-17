"""Statistical calculations for Lambda invocation data."""

import math
import statistics
from datetime import datetime

from thaw.models import (
    AnalysisResult,
    Comparison,
    ComparisonResult,
    DistributionStats,
    FunctionSummary,
    InvocationReport,
    MultiFunctionComparison,
)


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


def calculate_cohens_d(
    mean1: float, std1: float, n1: int, mean2: float, std2: float, n2: int
) -> float:
    """
    Calculate Cohen's d effect size between two distributions.

    Uses pooled standard deviation for unequal sample sizes.
    Positive d means mean2 > mean1 (regression for duration metrics).
    Negative d means mean2 < mean1 (improvement for duration metrics).

    Args:
        mean1: Mean of first (before) distribution
        std1: Standard deviation of first distribution
        n1: Sample size of first distribution
        mean2: Mean of second (after) distribution
        std2: Standard deviation of second distribution
        n2: Sample size of second distribution

    Returns:
        Cohen's d effect size
    """
    if n1 == 0 or n2 == 0:
        return 0.0

    # Handle edge case where both std devs are 0
    if std1 == 0 and std2 == 0:
        if mean1 == mean2:
            return 0.0
        # Return a large effect size if means differ but no variance
        return float("inf") if mean2 > mean1 else float("-inf")

    # Pooled standard deviation (weighted by sample size)
    pooled_std = math.sqrt(((n1 - 1) * std1**2 + (n2 - 1) * std2**2) / (n1 + n2 - 2))

    if pooled_std == 0:
        return 0.0

    return (mean2 - mean1) / pooled_std


def calculate_overlap_percent(
    mean1: float, std1: float, mean2: float, std2: float
) -> float:
    """
    Calculate the overlap percentage between two normal distributions.

    Uses the overlapping coefficient (OVL) approximation for two normal distributions.

    Args:
        mean1: Mean of first distribution
        std1: Standard deviation of first distribution
        mean2: Mean of second distribution
        std2: Standard deviation of second distribution

    Returns:
        Overlap percentage (0-100)
    """
    # Handle edge cases
    if std1 == 0 and std2 == 0:
        return 100.0 if mean1 == mean2 else 0.0

    if std1 == 0 or std2 == 0:
        # One distribution is a point, overlap depends on if it's within the other
        return 50.0  # Approximate

    # For two normal distributions, use the formula based on the difference
    # OVL = 2 * Phi(-|mean1 - mean2| / (2 * sqrt((std1^2 + std2^2) / 2)))
    # Simplified approximation using Cohen's d
    combined_std = math.sqrt((std1**2 + std2**2) / 2)
    if combined_std == 0:
        return 100.0

    z = abs(mean1 - mean2) / combined_std

    # Approximate the overlap using the normal CDF
    # OVL ≈ 2 * Phi(-z/2) where Phi is the standard normal CDF
    # Using approximation: Phi(x) ≈ 1 / (1 + exp(-1.7 * x))
    phi = 1 / (1 + math.exp(1.7 * z / 2))
    overlap = 2 * phi * 100

    return min(100.0, max(0.0, overlap))


def compare_distributions(
    before_values: list[float], after_values: list[float]
) -> Comparison | None:
    """
    Compare two distributions and compute effect size metrics.

    Args:
        before_values: Values from the "before" period
        after_values: Values from the "after" period

    Returns:
        Comparison object with statistics and effect size, or None if insufficient data
    """
    before_stats = calculate_distribution_stats(before_values)
    after_stats = calculate_distribution_stats(after_values)

    if before_stats is None or after_stats is None:
        return None

    cohens_d = calculate_cohens_d(
        before_stats.mean,
        before_stats.std_dev,
        before_stats.count,
        after_stats.mean,
        after_stats.std_dev,
        after_stats.count,
    )

    overlap = calculate_overlap_percent(
        before_stats.mean, before_stats.std_dev, after_stats.mean, after_stats.std_dev
    )

    return Comparison(
        before=before_stats,
        after=after_stats,
        cohens_d=cohens_d,
        overlap_percent=overlap,
    )


def compare_reports(
    function_name: str,
    before_reports: list[InvocationReport],
    after_reports: list[InvocationReport],
    pivot_time: datetime,
    before_start: datetime,
    after_end: datetime,
) -> ComparisonResult:
    """
    Compare invocation reports from before and after a pivot time.

    Args:
        function_name: Name of the Lambda function
        before_reports: Reports from before the pivot
        after_reports: Reports from after the pivot
        pivot_time: The pivot timestamp
        before_start: Start of the before period
        after_end: End of the after period

    Returns:
        ComparisonResult with comparison metrics
    """
    # Extract durations
    before_durations = [r.duration_ms for r in before_reports]
    after_durations = [r.duration_ms for r in after_reports]

    before_billed = [float(r.billed_duration_ms) for r in before_reports]
    after_billed = [float(r.billed_duration_ms) for r in after_reports]

    before_memory = [float(r.max_memory_used_mb) for r in before_reports]
    after_memory = [float(r.max_memory_used_mb) for r in after_reports]

    # Calculate cold start rates
    before_cold_starts = sum(1 for r in before_reports if r.is_cold_start)
    after_cold_starts = sum(1 for r in after_reports if r.is_cold_start)

    before_cold_rate = before_cold_starts / len(before_reports) if before_reports else 0.0
    after_cold_rate = after_cold_starts / len(after_reports) if after_reports else 0.0

    # Compare distributions
    duration_comparison = compare_distributions(before_durations, after_durations)
    billed_comparison = compare_distributions(before_billed, after_billed)
    memory_comparison = compare_distributions(before_memory, after_memory)

    # Create default comparison if no data
    empty_stats = DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    empty_comparison = Comparison(empty_stats, empty_stats, 0.0, 100.0)

    return ComparisonResult(
        function_name=function_name,
        pivot_time=pivot_time,
        before_start=before_start,
        after_end=after_end,
        before_count=len(before_reports),
        after_count=len(after_reports),
        duration=duration_comparison or empty_comparison,
        billed_duration=billed_comparison or empty_comparison,
        memory_used=memory_comparison or empty_comparison,
        cold_start_rate_before=before_cold_rate,
        cold_start_rate_after=after_cold_rate,
    )


def summarize_function(
    function_name: str,
    reports: list[InvocationReport],
) -> FunctionSummary:
    """
    Create a summary of a function's performance.

    Args:
        function_name: Name of the Lambda function
        reports: List of InvocationReport objects

    Returns:
        FunctionSummary with computed statistics
    """
    durations = [r.duration_ms for r in reports]
    billed_durations = [float(r.billed_duration_ms) for r in reports]
    memory_used = [float(r.max_memory_used_mb) for r in reports]

    cold_starts = sum(1 for r in reports if r.is_cold_start)
    cold_start_rate = cold_starts / len(reports) if reports else 0.0

    empty_stats = DistributionStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    return FunctionSummary(
        function_name=function_name,
        invocation_count=len(reports),
        duration_stats=calculate_distribution_stats(durations) or empty_stats,
        billed_duration_stats=calculate_distribution_stats(billed_durations) or empty_stats,
        memory_used_stats=calculate_distribution_stats(memory_used) or empty_stats,
        cold_start_rate=cold_start_rate,
    )
