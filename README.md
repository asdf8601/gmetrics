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
gmetrics labels <metric>   # show available resource + metric labels
```

## Common flags

| Flag | Description |
|------|-------------|
| `--start` | Time window start (`15m`, `1h`, `2d`, `1w`, or RFC3339) |
| `--end` | Time window end (default: now) |
| `--period` | Alignment period (`60s`, `5m`) |
| `--namespace` | Kubernetes namespace |
| `--cluster` | Kubernetes cluster name |
| `--memory-type` | `any` (default, total — matches GCP Metrics Explorer), `non-evictable` (working set — matches `kubectl top`), `evictable` (page cache) |
| `--pod-pattern` | Substring filter on pod name (`top` only) |
| `--show` | Extra columns for `top` — CSV of `cluster`, `namespace`, `container`, `location`, `node`, `memory_type`, or full GCP field paths |
| `--order` | Sort direction for `top` — `desc` (default, highest first) or `asc` (lowest first) |
| `--lines` | Render `pod` / `node` as full line charts (plotext) instead of sparklines |
| `--json` | Raw JSON output (global, before command) |
| `--project` | GCP project ID |

## Examples

```bash
# Pod CPU + memory (last 15 minutes)
gmetrics pod my-service

# Pod with longer window and cluster filter
gmetrics pod my-service --start 1h --cluster us-east1

# Pod with full line chart (plotext) instead of sparklines
gmetrics pod my-service --start 1h --lines

# Top 10 pods by CPU
gmetrics top cpu --namespace production

# Top 20 pods by memory in a cluster
gmetrics top memory --cluster us-east1 --limit 20

# Top with extra columns (cluster, namespace)
gmetrics top memory --pod-pattern my-service --show cluster,namespace --limit 10

# Bottom: pods using the LEAST memory (over-provisioned candidates)
gmetrics top memory --order asc --limit 20

# Discover which labels a metric exposes (useful for --show / --group-by)
gmetrics labels "kubernetes.io/container/memory/used_bytes" \
    --filter 'resource.labels.pod_name = starts_with("my-service")'

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

### Memory metric: `--memory-type`

`kubernetes.io/container/memory/used_bytes` returns **two series per pod**:

- `non-evictable` — working set (RSS + kernel stacks). What `kubectl top`
  shows and what the OOM killer watches.
- `evictable` — page cache. Reclaimable under pressure.
- `any` — total (sum of both). What GCP Metrics Explorer shows by default.

`pod` / `node` / `top` default to `--memory-type any` so output matches the
GCP console. Use `--memory-type non-evictable` for working-set numbers
matching `kubectl top`.

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
  "rows": [{"pod": "...", "extras": {"cluster": "..."}, "values": [[ts, v]]}],
  "columns": ["cluster"],
  "metric_type": "kubernetes.io/container/cpu/core_usage_time"
}
```

`rows` and `columns` are always present; they carry the extra fields
requested via `--show`. `grouped` is kept for backward compatibility
(pod-name keyed).

**`query`** — returns a **list** of series directly (no wrapping object).

**`metrics`** — returns a list of metric descriptor objects.

**`labels`**

```json
{
  "metric_type": "kubernetes.io/container/memory/used_bytes",
  "series_sampled": 500,
  "resource": {"pod_name": [...], "cluster_name": [...]},
  "metric": {"memory_type": ["evictable", "non-evictable"]}
}
```

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
