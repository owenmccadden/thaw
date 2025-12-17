"""Command-line interface for Thaw."""

import re
from datetime import datetime, timedelta, timezone

import click
from rich.console import Console
from rich.table import Table

from thaw import __version__
from thaw.cloudwatch import CloudWatchError, fetch_reports
from thaw.export import export_to_csv
from thaw.models import (
    AnalysisResult,
    Comparison,
    ComparisonResult,
    DistributionStats,
    FunctionSummary,
    MultiFunctionComparison,
)
from thaw.stats import analyze_reports, calculate_cohens_d, compare_reports, summarize_function

console = Console()


def parse_time_range(time_str: str) -> timedelta:
    """
    Parse a human-readable time range string.

    Supports formats like: 1h, 24h, 7d, 30d, 1w
    """
    match = re.match(r"^(\d+)([hdwm])$", time_str.lower())
    if not match:
        raise click.BadParameter(
            f"Invalid time range '{time_str}'. Use format like: 1h, 24h, 7d, 1w, 1m"
        )

    value = int(match.group(1))
    unit = match.group(2)

    if unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    elif unit == "w":
        return timedelta(weeks=value)
    elif unit == "m":
        return timedelta(days=value * 30)  # Approximate month

    raise click.BadParameter(f"Unknown time unit: {unit}")


def format_duration(ms: float) -> str:
    """Format a duration in milliseconds for display."""
    if ms < 1:
        return f"{ms:.3f}ms"
    elif ms < 10:
        return f"{ms:.2f}ms"
    elif ms < 1000:
        return f"{ms:.1f}ms"
    else:
        return f"{ms/1000:.2f}s"


def format_memory(mb: float) -> str:
    """Format memory in MB for display."""
    return f"{mb:.0f}MB"


def format_percentage(value: float) -> str:
    """Format a percentage for display."""
    return f"{value * 100:.1f}%"


def print_stats_table(title: str, stats: DistributionStats, formatter: callable) -> None:
    """Print a statistics table."""
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="dim")
    table.add_column("Value", justify="right")

    table.add_row("Count", str(stats.count))
    table.add_row("Mean", formatter(stats.mean))
    table.add_row("Median", formatter(stats.median))
    table.add_row("Std Dev", formatter(stats.std_dev))
    table.add_row("Min", formatter(stats.min))
    table.add_row("Max", formatter(stats.max))
    table.add_row("p50", formatter(stats.p50))
    table.add_row("p90", formatter(stats.p90))
    table.add_row("p95", formatter(stats.p95))
    table.add_row("p99", formatter(stats.p99))

    console.print(table)


def print_analysis_result(result: AnalysisResult) -> None:
    """Print a complete analysis result."""
    # Header
    console.print()
    console.print(f"[bold]Analysis: {result.function_name}[/bold]")
    console.print(
        f"Time range: {result.start_time.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.end_time.strftime('%Y-%m-%d %H:%M')} UTC"
    )
    console.print(f"Total invocations: {len(result.invocations)}")
    console.print()

    if len(result.invocations) == 0:
        console.print("[yellow]No invocations found in this time range.[/yellow]")
        return

    # Duration stats
    print_stats_table("Duration", result.duration_stats, format_duration)
    console.print()

    # Billed duration stats
    print_stats_table("Billed Duration", result.billed_duration_stats, format_duration)
    console.print()

    # Memory stats
    print_stats_table("Memory Used", result.memory_used_stats, format_memory)
    console.print()

    # Cold start summary
    summary_table = Table(title="Cold Starts & SnapStart", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", justify="right")

    summary_table.add_row("Cold Start Count", str(result.cold_start_count))
    summary_table.add_row("Cold Start Rate", format_percentage(result.cold_start_rate))

    if result.cold_start_duration_stats:
        summary_table.add_row(
            "Cold Start Duration (mean)", format_duration(result.cold_start_duration_stats.mean)
        )
        summary_table.add_row(
            "Cold Start Duration (p99)", format_duration(result.cold_start_duration_stats.p99)
        )

    summary_table.add_row("SnapStart Restore Count", str(result.snapstart_restore_count))
    summary_table.add_row("SnapStart Restore Rate", format_percentage(result.snapstart_restore_rate))

    if result.snapstart_restore_duration_stats:
        summary_table.add_row(
            "Restore Duration (mean)",
            format_duration(result.snapstart_restore_duration_stats.mean),
        )
        summary_table.add_row(
            "Restore Duration (p99)", format_duration(result.snapstart_restore_duration_stats.p99)
        )

    console.print(summary_table)


def format_cohens_d(d: float) -> str:
    """Format Cohen's d with color based on effect size."""
    if abs(d) < 0.2:
        return f"{d:+.2f} (negligible)"
    elif abs(d) < 0.5:
        return f"{d:+.2f} (small)"
    elif abs(d) < 0.8:
        return f"{d:+.2f} (medium)"
    else:
        return f"{d:+.2f} (large)"


def format_change(before: float, after: float, formatter: callable) -> str:
    """Format a before/after change with direction indicator."""
    if before == 0:
        return f"{formatter(after)}"
    change_pct = ((after - before) / before) * 100
    arrow = "+" if change_pct > 0 else ""
    return f"{formatter(before)} -> {formatter(after)} ({arrow}{change_pct:.1f}%)"


def print_comparison_table(
    title: str, comparison: Comparison, formatter: callable
) -> None:
    """Print a comparison table for a metric."""
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="dim")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Change", justify="right")

    # Determine color based on direction (for duration, lower is better)
    direction = comparison.direction
    if direction == "improved":
        change_style = "green"
    elif direction == "regressed":
        change_style = "red"
    else:
        change_style = "dim"

    before = comparison.before
    after = comparison.after

    def change_str(b: float, a: float) -> str:
        if b == 0:
            return "-"
        pct = ((a - b) / b) * 100
        return f"{pct:+.1f}%"

    table.add_row("Count", str(before.count), str(after.count), "-")
    table.add_row(
        "Mean",
        formatter(before.mean),
        formatter(after.mean),
        f"[{change_style}]{change_str(before.mean, after.mean)}[/{change_style}]",
    )
    table.add_row(
        "Median",
        formatter(before.median),
        formatter(after.median),
        f"[{change_style}]{change_str(before.median, after.median)}[/{change_style}]",
    )
    table.add_row(
        "p95",
        formatter(before.p95),
        formatter(after.p95),
        f"[{change_style}]{change_str(before.p95, after.p95)}[/{change_style}]",
    )
    table.add_row(
        "p99",
        formatter(before.p99),
        formatter(after.p99),
        f"[{change_style}]{change_str(before.p99, after.p99)}[/{change_style}]",
    )

    console.print(table)

    # Effect size summary
    effect_color = "green" if comparison.cohens_d < -0.2 else ("red" if comparison.cohens_d > 0.2 else "dim")
    console.print(
        f"  Cohen's d: [{effect_color}]{format_cohens_d(comparison.cohens_d)}[/{effect_color}]  "
        f"Overlap: {comparison.overlap_percent:.1f}%"
    )


def print_comparison_result(result: ComparisonResult) -> None:
    """Print a complete comparison result."""
    console.print()
    console.print(f"[bold]Comparison: {result.function_name}[/bold]")
    console.print(f"Pivot time: {result.pivot_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    console.print(
        f"Before: {result.before_start.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.pivot_time.strftime('%Y-%m-%d %H:%M')} ({result.before_count} invocations)"
    )
    console.print(
        f"After:  {result.pivot_time.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.after_end.strftime('%Y-%m-%d %H:%M')} ({result.after_count} invocations)"
    )
    console.print()

    if result.before_count == 0 or result.after_count == 0:
        console.print("[yellow]Insufficient data for comparison.[/yellow]")
        return

    # Duration comparison
    print_comparison_table("Duration", result.duration, format_duration)
    console.print()

    # Billed duration comparison
    print_comparison_table("Billed Duration", result.billed_duration, format_duration)
    console.print()

    # Memory comparison
    print_comparison_table("Memory Used", result.memory_used, format_memory)
    console.print()

    # Cold start rate comparison
    cold_table = Table(title="Cold Start Rate", show_header=True, header_style="bold cyan")
    cold_table.add_column("Period", style="dim")
    cold_table.add_column("Rate", justify="right")

    cold_table.add_row("Before", format_percentage(result.cold_start_rate_before))
    cold_table.add_row("After", format_percentage(result.cold_start_rate_after))

    change = result.cold_start_rate_after - result.cold_start_rate_before
    change_color = "green" if change < 0 else ("red" if change > 0 else "dim")
    cold_table.add_row("Change", f"[{change_color}]{change * 100:+.1f}pp[/{change_color}]")

    console.print(cold_table)


def print_multi_function_comparison(result: MultiFunctionComparison) -> None:
    """Print a multi-function comparison result."""
    console.print()
    console.print("[bold]Multi-Function Comparison[/bold]")
    console.print(
        f"Time range: {result.start_time.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.end_time.strftime('%Y-%m-%d %H:%M')} UTC"
    )
    console.print()

    if not result.functions:
        console.print("[yellow]No functions to compare.[/yellow]")
        return

    # Duration comparison table
    duration_table = Table(title="Duration Comparison", show_header=True, header_style="bold cyan")
    duration_table.add_column("Function", style="dim")
    duration_table.add_column("Count", justify="right")
    duration_table.add_column("Mean", justify="right")
    duration_table.add_column("p95", justify="right")
    duration_table.add_column("p99", justify="right")
    duration_table.add_column("Cohen's d", justify="right")

    # Sort by mean duration (fastest first)
    sorted_funcs = sorted(result.functions, key=lambda f: f.duration_stats.mean)
    baseline = sorted_funcs[0]  # Best performer is baseline

    for func in sorted_funcs:
        # Truncate long function names
        name = func.function_name
        if len(name) > 40:
            name = "..." + name[-37:]

        # Calculate Cohen's d vs baseline
        if func == baseline:
            cohens_d_str = "[dim]baseline[/dim]"
        else:
            d = calculate_cohens_d(
                baseline.duration_stats.mean,
                baseline.duration_stats.std_dev,
                baseline.duration_stats.count,
                func.duration_stats.mean,
                func.duration_stats.std_dev,
                func.duration_stats.count,
            )
            # Positive d means this function is slower (worse)
            if abs(d) < 0.2:
                cohens_d_str = f"[dim]{d:+.2f}[/dim]"
            elif d > 0:
                cohens_d_str = f"[red]{d:+.2f}[/red]"
            else:
                cohens_d_str = f"[green]{d:+.2f}[/green]"

        duration_table.add_row(
            name,
            str(func.invocation_count),
            format_duration(func.duration_stats.mean),
            format_duration(func.duration_stats.p95),
            format_duration(func.duration_stats.p99),
            cohens_d_str,
        )

    console.print(duration_table)
    console.print()

    # Memory comparison table
    memory_table = Table(title="Memory Comparison", show_header=True, header_style="bold cyan")
    memory_table.add_column("Function", style="dim")
    memory_table.add_column("Mean", justify="right")
    memory_table.add_column("Max", justify="right")

    for func in sorted_funcs:
        name = func.function_name
        if len(name) > 40:
            name = "..." + name[-37:]

        memory_table.add_row(
            name,
            format_memory(func.memory_used_stats.mean),
            format_memory(func.memory_used_stats.max),
        )

    console.print(memory_table)
    console.print()

    # Cold start comparison table
    cold_table = Table(title="Cold Start Comparison", show_header=True, header_style="bold cyan")
    cold_table.add_column("Function", style="dim")
    cold_table.add_column("Rate", justify="right")

    # Sort by cold start rate (lowest first)
    sorted_by_cold = sorted(result.functions, key=lambda f: f.cold_start_rate)

    for func in sorted_by_cold:
        name = func.function_name
        if len(name) > 40:
            name = "..." + name[-37:]

        rate_color = "green" if func.cold_start_rate < 0.05 else ("yellow" if func.cold_start_rate < 0.1 else "red")
        cold_table.add_row(
            name,
            f"[{rate_color}]{format_percentage(func.cold_start_rate)}[/{rate_color}]",
        )

    console.print(cold_table)


@click.group(invoke_without_command=True)
@click.version_option(version=__version__)
@click.pass_context
def main(ctx):
    """Thaw: An open source tool for optimizing AWS Lambda."""
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@main.command()
@click.argument("function_name")
@click.option(
    "--from",
    "from_time",
    default="24h",
    help="Start of time range (e.g., 24h, 7d, 1w). Default: 24h",
)
@click.option(
    "--to",
    "to_time",
    default="now",
    help="End of time range. Use 'now' or ISO format. Default: now",
)
@click.option(
    "--region",
    default=None,
    help="AWS region. Uses default region if not specified.",
)
@click.option(
    "--max-results",
    default=10000,
    help="Maximum number of invocations to fetch. Default: 10000",
)
@click.option(
    "--export",
    "export_format",
    type=click.Choice(["csv"]),
    default=None,
    help="Export format (csv).",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Output file path for export.",
)
def analyze(
    function_name: str,
    from_time: str,
    to_time: str,
    region: str | None,
    max_results: int,
    export_format: str | None,
    output: str | None,
):
    """
    Analyze performance metrics for a Lambda function.

    FUNCTION_NAME is the name or ARN of the Lambda function to analyze.

    Examples:

        thaw analyze my-function --from 24h

        thaw analyze my-function --from 7d --export csv -o data.csv

        thaw analyze arn:aws:lambda:us-east-1:123456789:function:my-func
    """
    # Parse time range
    end_time = datetime.now(timezone.utc)
    if to_time.lower() != "now":
        try:
            end_time = datetime.fromisoformat(to_time.replace("Z", "+00:00"))
        except ValueError:
            raise click.BadParameter(f"Invalid end time: {to_time}")

    try:
        delta = parse_time_range(from_time)
        start_time = end_time - delta
    except click.BadParameter:
        # Try parsing as ISO format
        try:
            start_time = datetime.fromisoformat(from_time.replace("Z", "+00:00"))
        except ValueError:
            raise click.BadParameter(f"Invalid start time: {from_time}")

    # Validate export options
    if export_format and not output:
        raise click.BadParameter("--output is required when using --export")

    # Fetch data
    console.print(f"[dim]Fetching logs for {function_name}...[/dim]")

    try:
        with console.status("[bold green]Fetching CloudWatch logs...") as status:
            reports = fetch_reports(
                function_name=function_name,
                start_time=start_time,
                end_time=end_time,
                region=region,
                max_results=max_results,
                progress_callback=lambda count, _: status.update(
                    f"[bold green]Fetched {count} invocations..."
                ),
            )
    except CloudWatchError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise SystemExit(1)

    console.print(f"[green]Fetched {len(reports)} invocations[/green]")

    # Analyze
    result = analyze_reports(function_name, reports, start_time, end_time)

    # Print results
    print_analysis_result(result)

    # Export if requested
    if export_format == "csv" and output:
        export_to_csv(result, output)
        console.print(f"\n[green]Exported to {output}[/green]")


@main.command()
@click.argument("function_names", nargs=-1, required=True)
@click.option(
    "--pivot",
    default=None,
    help="Pivot timestamp (ISO format) for before/after comparison (single function).",
)
@click.option(
    "--window",
    default="24h",
    help="Time window for pivot mode, or lookback for multi-function. Default: 24h",
)
@click.option(
    "--from",
    "from_time",
    default=None,
    help="Start of time range for multi-function comparison (e.g., 24h, 7d).",
)
@click.option(
    "--region",
    default=None,
    help="AWS region. Uses default region if not specified.",
)
@click.option(
    "--max-results",
    default=10000,
    help="Maximum number of invocations to fetch per function. Default: 10000",
)
def compare(
    function_names: tuple[str, ...],
    pivot: str | None,
    window: str,
    from_time: str | None,
    region: str | None,
    max_results: int,
):
    """
    Compare Lambda function performance.

    Two modes:

    1. PIVOT MODE (single function): Compare before/after a timestamp.

       thaw compare my-function --pivot "2024-01-15T10:00:00Z"

    2. MULTI-FUNCTION MODE: Compare multiple functions over the same period.

       thaw compare func1 func2 func3 --from 7d
    """
    # Determine mode
    if pivot:
        # Pivot mode: before/after comparison for single function
        if len(function_names) > 1:
            raise click.BadParameter(
                "Pivot mode only supports one function. Remove --pivot for multi-function comparison."
            )

        function_name = function_names[0]

        # Parse pivot time
        try:
            pivot_time = datetime.fromisoformat(pivot.replace("Z", "+00:00"))
            if pivot_time.tzinfo is None:
                pivot_time = pivot_time.replace(tzinfo=timezone.utc)
        except ValueError:
            raise click.BadParameter(f"Invalid pivot timestamp: {pivot}")

        # Parse window
        try:
            window_delta = parse_time_range(window)
        except click.BadParameter:
            raise click.BadParameter(f"Invalid window: {window}")

        start_time = pivot_time - window_delta
        end_time = pivot_time + window_delta

        # Check if end time is in the future
        now = datetime.now(timezone.utc)
        if end_time > now:
            end_time = now

        console.print(f"[dim]Fetching logs for {function_name}...[/dim]")

        try:
            with console.status("[bold green]Fetching CloudWatch logs...") as status:
                reports = fetch_reports(
                    function_name=function_name,
                    start_time=start_time,
                    end_time=end_time,
                    region=region,
                    max_results=max_results * 2,
                    progress_callback=lambda count, _: status.update(
                        f"[bold green]Fetched {count} invocations..."
                    ),
                )
        except CloudWatchError as e:
            console.print(f"[red]Error: {e}[/red]")
            raise SystemExit(1)

        console.print(f"[green]Fetched {len(reports)} invocations[/green]")

        # Split reports into before and after
        before_reports = [r for r in reports if r.timestamp < pivot_time]
        after_reports = [r for r in reports if r.timestamp >= pivot_time]

        console.print(f"[dim]Before pivot: {len(before_reports)} invocations[/dim]")
        console.print(f"[dim]After pivot: {len(after_reports)} invocations[/dim]")

        # Compare
        result = compare_reports(
            function_name=function_name,
            before_reports=before_reports,
            after_reports=after_reports,
            pivot_time=pivot_time,
            before_start=start_time,
            after_end=end_time,
        )

        print_comparison_result(result)

    else:
        # Multi-function mode
        if len(function_names) < 2:
            raise click.BadParameter(
                "Multi-function mode requires at least 2 functions. "
                "Use --pivot for single function before/after comparison."
            )

        # Parse time range
        end_time = datetime.now(timezone.utc)
        time_range = from_time or window

        try:
            delta = parse_time_range(time_range)
            start_time = end_time - delta
        except click.BadParameter:
            raise click.BadParameter(f"Invalid time range: {time_range}")

        # Fetch data for each function
        summaries: list[FunctionSummary] = []

        for func_name in function_names:
            console.print(f"[dim]Fetching logs for {func_name}...[/dim]")

            try:
                reports = fetch_reports(
                    function_name=func_name,
                    start_time=start_time,
                    end_time=end_time,
                    region=region,
                    max_results=max_results,
                )
                console.print(f"[green]  {len(reports)} invocations[/green]")

                summary = summarize_function(func_name, reports)
                summaries.append(summary)

            except CloudWatchError as e:
                console.print(f"[red]Error fetching {func_name}: {e}[/red]")
                continue

        if not summaries:
            console.print("[red]No data fetched for any function.[/red]")
            raise SystemExit(1)

        result = MultiFunctionComparison(
            start_time=start_time,
            end_time=end_time,
            functions=summaries,
        )

        print_multi_function_comparison(result)


if __name__ == "__main__":
    main()
