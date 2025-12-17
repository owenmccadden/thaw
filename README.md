<p align="center">
  <img src="thaw-logo.png" alt="Thaw" width="300">
</p>

<h1 align="center">Thaw</h1>

<p align="center">
  An open source tool for optimizing AWS Lambda performance and cold starts.
</p>

---

## Installation

```bash
uv pip install thaw
```

## Usage

```bash
# Analyze a Lambda function (last 24 hours)
thaw analyze my-function

# Analyze with custom time range
thaw analyze my-function --from 7d

# Export to CSV
thaw analyze my-function --from 24h --export csv -o data.csv
```
