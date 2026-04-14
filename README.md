# gcm

CLI for querying Kubernetes metrics from GCP Cloud Monitoring API v3.

## Install

```bash
# From GitHub
uv tool install git+https://github.com/asdf8601/gmetrics

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
gcm pod <name>        # CPU + memory for a pod
gcm top cpu|memory    # rank pods by resource usage
gcm node <name>       # CPU + memory for a node
gcm query <metric>    # generic metric query
gcm metrics           # list/search metric descriptors
```

## Common flags

| Flag | Description |
|------|-------------|
| `--start` | Time window start (`15m`, `1h`, `2d`, `1w`, or RFC3339) |
| `--end` | Time window end (default: now) |
| `--period` | Alignment period (`60s`, `5m`) |
| `--namespace` | Kubernetes namespace |
| `--cluster` | Kubernetes cluster name |
| `--json` | Raw JSON output (global, before command) |
| `--project` | GCP project ID |

## Examples

```bash
# Pod CPU + memory (last 15 minutes)
gcm pod my-service

# Pod with longer window and cluster filter
gcm pod my-service --start 1h --cluster us-east1

# Top 10 pods by CPU
gcm top cpu --namespace production

# Top 20 pods by memory in a cluster
gcm top memory --cluster us-east1 --limit 20

# Node resources
gcm node gke-my-cluster-pool-abc --start 2h

# Generic query
gcm query "kubernetes.io/container/memory/used_bytes" \
    --filter 'resource.labels.pod_name="my-pod"' --aligner mean

# List kubernetes container metrics
gcm metrics --filter kubernetes.io/container

# JSON output
gcm --json pod my-service --start 1h
```
