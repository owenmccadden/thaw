"""CloudWatch Logs client for fetching Lambda REPORT lines."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from thaw.models import InvocationReport
from thaw.parser import parse_report_lines


class CloudWatchError(Exception):
    """Error fetching data from CloudWatch."""

    pass


def get_log_group_name(function_name: str, region: str | None = None) -> str:
    """
    Get the CloudWatch log group name for a Lambda function.

    Calls Lambda GetFunctionConfiguration to get the actual log group,
    handling cases where:
    - The function uses a custom log group (LoggingConfig)
    - The log group name is truncated due to length limits

    Args:
        function_name: Lambda function name or ARN
        region: AWS region (uses default if not specified)

    Returns:
        The log group name for the function
    """
    client_kwargs = {}
    if region:
        client_kwargs["region_name"] = region

    lambda_client = boto3.client("lambda", **client_kwargs)

    try:
        response = lambda_client.get_function_configuration(FunctionName=function_name)

        # Check for custom log group in LoggingConfig (available since late 2023)
        logging_config = response.get("LoggingConfig", {})
        if log_group := logging_config.get("LogGroup"):
            return log_group

        # Fall back to default pattern using the actual function name from response
        # This handles ARN input and gives us the canonical function name
        actual_name = response.get("FunctionName", function_name)
        return f"/aws/lambda/{actual_name}"

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "ResourceNotFoundException":
            raise CloudWatchError(
                f"Lambda function '{function_name}' not found. "
                "Check the function name and region."
            ) from e
        elif error_code == "AccessDeniedException":
            raise CloudWatchError(
                f"Access denied to Lambda function '{function_name}'. "
                "Ensure you have lambda:GetFunctionConfiguration permission."
            ) from e
        else:
            # For other errors, fall back to default pattern
            # Extract function name from ARN if needed
            if function_name.startswith("arn:"):
                parts = function_name.split(":")
                function_name = parts[-1]
            return f"/aws/lambda/{function_name}"


def fetch_reports(
    function_name: str,
    start_time: datetime,
    end_time: datetime,
    region: str | None = None,
    max_results: int = 10000,
    progress_callback: Callable | None = None,
) -> list[InvocationReport]:
    """
    Fetch Lambda invocation reports from CloudWatch Logs.

    Uses FilterLogEvents API with pagination to fetch REPORT lines.

    Args:
        function_name: Lambda function name or ARN
        start_time: Start of time range (inclusive)
        end_time: End of time range (inclusive)
        region: AWS region (uses default if not specified)
        max_results: Maximum number of reports to fetch (default 10000)
        progress_callback: Optional callback(fetched_count, total_events) for progress updates

    Returns:
        List of InvocationReport objects sorted by timestamp

    Raises:
        CloudWatchError: If there's an error fetching logs
    """
    # Ensure times are in UTC
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    # Convert to milliseconds since epoch
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Create CloudWatch Logs client
    client_kwargs = {}
    if region:
        client_kwargs["region_name"] = region
    client = boto3.client("logs", **client_kwargs)

    log_group = get_log_group_name(function_name, region)
    all_events = []
    next_token = None
    request_count = 0

    try:
        while True:
            # Build request parameters
            params = {
                "logGroupName": log_group,
                "startTime": start_ms,
                "endTime": end_ms,
                "filterPattern": "REPORT RequestId",
            }
            if next_token:
                params["nextToken"] = next_token

            # Make API call
            response = client.filter_log_events(**params)
            request_count += 1

            events = response.get("events", [])
            all_events.extend(events)

            if progress_callback:
                progress_callback(len(all_events), None)

            # Check if we've hit our limit
            if len(all_events) >= max_results:
                all_events = all_events[:max_results]
                break

            # Check for more pages
            next_token = response.get("nextToken")
            if not next_token:
                break

            # Rate limiting: CloudWatch allows ~10 requests/second
            if request_count % 10 == 0:
                time.sleep(0.1)

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", str(e))

        if error_code == "ResourceNotFoundException":
            raise CloudWatchError(
                f"Log group '{log_group}' not found. "
                f"Ensure the function '{function_name}' exists and has been invoked."
            ) from e
        elif error_code == "AccessDeniedException":
            raise CloudWatchError(
                f"Access denied to log group '{log_group}'. "
                "Check your AWS credentials and IAM permissions."
            ) from e
        else:
            raise CloudWatchError(f"CloudWatch error ({error_code}): {error_message}") from e

    # Parse events into InvocationReports
    reports = parse_report_lines(all_events)

    # Sort by timestamp
    reports.sort(key=lambda r: r.timestamp)

    return reports
