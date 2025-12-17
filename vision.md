
Thaw: an open source tool for optimizing AWS Lambda.

Core features:
- Analyze distributions for a single function over a time range:
 - Duration
 - Init duration (cold starts)
 - Restore duration (SnapStart)
 - Billed duration
 - Memory used
- Compare distributions before/after a timestamp (Cohen's d, percentiles, overlap %)
- Compare distributions across multiple functions
- Cold start rate tracking (% of invocations with init duration)
- SnapStart restore rate tracking (% of invocations with restore duration)
- Generate histogram and box plot images
- Export raw data to CSV
- Benchmark: measure duration and cost at each memory configuration

Interface:
- TUI mode (default) — interactive function selection, live charts
- CLI mode — scriptable commands for CI/automation
- Export to PNG, CSV

Data source:
- CloudWatch Logs — parse REPORT lines
- Works with AWS creds, just need function name
