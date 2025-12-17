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
