"""Command-line interface for Thaw."""

import re
from datetime import datetime, timedelta, timezone

import click
from rich.console import Console
from rich.table import Table

from thaw import __version__
from thaw.cloudwatch import CloudWatchError, fetch_reports
from thaw.export import export_to_csv
from thaw.models import AnalysisResult, DistributionStats
from thaw.stats import analyze_reports

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


if __name__ == "__main__":
    main()
