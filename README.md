# gmetrics

CLI for querying Kubernetes metrics from GCP Cloud Monitoring API v3.

## Install

```bash
# From PyPI
uv tool install gmetrics

# One-shot (no install)
uvx gmetrics --help

# From source
uv sync
```

Requires `gcloud` CLI for authentication (`gcloud auth print-access-token`).

Set your GCP project:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
```

Or pass `--project` on each invocation.

## Commands

```
gmetrics pod <name>        # CPU + memory for a pod
gmetrics top cpu|memory    # rank pods by resource usage
gmetrics node <name>       # CPU + memory for a node
gmetrics query <metric>    # generic metric query
gmetrics metrics           # list/search metric descriptors
```

## Common flags

| Flag | Description |
|------|-------------|
| `--start` | Time window start (`15m`, `1h`, `2d`, `1w`, or RFC3339) |
| `--end` | Time window end (default: now) |
| `--period` | Alignment period (`60s`, `5m`) |
| `--namespace` | Kubernetes namespace |
| `--cluster` | Kubernetes cluster name |
| `--memory-type` | `non-evictable` (default), `evictable`, `any` — avoids double-counting |
| `--pod-pattern` | Substring filter on pod name (`top` only) |
| `--json` | Raw JSON output (global, before command) |
| `--project` | GCP project ID |

## Examples

```bash
# Pod CPU + memory (last 15 minutes)
gmetrics pod my-service

# Pod with longer window and cluster filter
gmetrics pod my-service --start 1h --cluster us-east1

# Top 10 pods by CPU
gmetrics top cpu --namespace production

# Top 20 pods by memory in a cluster
gmetrics top memory --cluster us-east1 --limit 20

# Node resources
gmetrics node gke-my-cluster-pool-abc --start 2h

# Generic query
gmetrics query "kubernetes.io/container/memory/used_bytes" \
    --filter 'resource.labels.pod_name="my-pod"' --aligner mean

# List kubernetes container metrics
gmetrics metrics --filter kubernetes.io/container

# JSON output
gmetrics --json pod my-service --start 1h
```

## Filter syntax

`--filter` uses the
[GCP Monitoring filter language](https://cloud.google.com/monitoring/api/v3/filters),
not PromQL:

| Operator | Use |
|----------|-----|
| `label = "exact"` | Exact match |
| `label = starts_with("prefix")` | Prefix match |
| `label = monitoring.regex.full_match("regex")` | Regex match |
| `AND` | Combine conditions |

`=~` and `!~` are **not** supported — gmetrics rejects these with a hint.

### Memory metric gotcha

`kubernetes.io/container/memory/used_bytes` returns **two series per pod**
(`memory_type = evictable` + `memory_type = non-evictable`). Summing both
double-counts resident memory.

`pod` / `node` / `top` default to `--memory-type non-evictable`. Override
with `--memory-type evictable` or `--memory-type any`.

For `query`, add the filter manually:

```bash
gmetrics query "kubernetes.io/container/memory/used_bytes" \
    --filter 'metric.labels.memory_type = "non-evictable"' \
    --aligner max
```

## JSON output schema

`--json` returns command-specific shapes:

**`pod` / `node`**

```json
{
  "cpu": [<series>, ...],
  "mem": [<series>, ...],
  "pod": "my-service"
}
```

**`top`**

```json
{
  "grouped": {"pod-name": [[ts, value], ...]},
  "metric_type": "kubernetes.io/container/cpu/core_usage_time"
}
```

**`query`** — returns a **list** of series directly (no wrapping object).

**`metrics`** — returns a list of metric descriptor objects.

### Series shape

Each series:

```json
{
  "resource": {
    "type": "k8s_container",
    "labels": {
      "pod_name": "...",
      "namespace_name": "...",
      "cluster_name": "...",
      "container_name": "...",
      "location": "...",
      "project_id": "..."
    }
  },
  "metric": {
    "type": "kubernetes.io/container/memory/used_bytes",
    "labels": {"memory_type": "non-evictable"}
  },
  "metricKind": "GAUGE",
  "valueType": "INT64",
  "points": [
    {
      "interval": {"startTime": "...", "endTime": "..."},
      "value": {"doubleValue": 123.4}
    }
  ]
}
```

Value is in `points[].value.doubleValue` **or** `int64Value` depending on
metric kind — check both.
