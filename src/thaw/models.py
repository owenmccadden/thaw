"""Data models for Lambda invocation reports and statistics."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class InvocationReport:
    """A single Lambda invocation report parsed from CloudWatch logs."""

    request_id: str
    timestamp: datetime
    duration_ms: float
    billed_duration_ms: int
    memory_size_mb: int
    max_memory_used_mb: int
    init_duration_ms: float | None = None  # Cold start
    restore_duration_ms: float | None = None  # SnapStart

    @property
    def is_cold_start(self) -> bool:
        """Return True if this invocation was a cold start."""
        return self.init_duration_ms is not None

    @property
    def is_snapstart_restore(self) -> bool:
        """Return True if this invocation was a SnapStart restore."""
        return self.restore_duration_ms is not None


@dataclass
class DistributionStats:
    """Statistical summary of a distribution of values."""

    count: int
    mean: float
    median: float
    std_dev: float
    min: float
    max: float
    p50: float
    p90: float
    p95: float
    p99: float


@dataclass
class AnalysisResult:
    """Complete analysis result for a Lambda function."""

    function_name: str
    start_time: datetime
    end_time: datetime
    invocations: list[InvocationReport]
    duration_stats: DistributionStats
    billed_duration_stats: DistributionStats
    memory_used_stats: DistributionStats
    cold_start_count: int
    cold_start_rate: float
    cold_start_duration_stats: DistributionStats | None
    snapstart_restore_count: int
    snapstart_restore_rate: float
    snapstart_restore_duration_stats: DistributionStats | None


@dataclass
class Comparison:
    """Comparison between two distributions."""

    before: DistributionStats
    after: DistributionStats
    cohens_d: float
    overlap_percent: float

    @property
    def effect_size_label(self) -> str:
        """Return a human-readable label for the effect size."""
        d = abs(self.cohens_d)
        if d < 0.2:
            return "negligible"
        elif d < 0.5:
            return "small"
        elif d < 0.8:
            return "medium"
        else:
            return "large"

    @property
    def direction(self) -> str:
        """Return whether the change was an improvement or regression."""
        if self.cohens_d < -0.2:
            return "improved"
        elif self.cohens_d > 0.2:
            return "regressed"
        else:
            return "unchanged"


@dataclass
class ComparisonResult:
    """Complete comparison result for a Lambda function."""

    function_name: str
    pivot_time: datetime
    before_start: datetime
    after_end: datetime
    before_count: int
    after_count: int
    duration: Comparison
    billed_duration: Comparison
    memory_used: Comparison
    cold_start_rate_before: float
    cold_start_rate_after: float


@dataclass
class FunctionSummary:
    """Summary statistics for a single function in multi-function comparison."""

    function_name: str
    invocation_count: int
    duration_stats: DistributionStats
    billed_duration_stats: DistributionStats
    memory_used_stats: DistributionStats
    cold_start_rate: float


@dataclass
class MultiFunctionComparison:
    """Comparison across multiple Lambda functions."""

    start_time: datetime
    end_time: datetime
    functions: list[FunctionSummary]
