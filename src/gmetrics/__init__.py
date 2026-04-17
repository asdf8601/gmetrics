"""gmetrics - CLI for GCP Cloud Monitoring API v3.

Also importable as a library:

    import gmetrics
    gmetrics.set_token("my-token")
    series = gmetrics.metric_pod("my-project", "my-pod", start="1h")
"""

import functools
import json
import random
import re
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import click

__all__ = [
    "metric_pod",
    "metric_node",
    "metric_top",
    "metric_query",
    "metric_descriptors",
    "get_token",
    "set_token",
    "fetch_time_series",
    "ApiError",
]


# -- Constants ----------------------------------------------------------------

API = "https://monitoring.googleapis.com/v3/projects"

_CPU_METRIC = "kubernetes.io/container/cpu/core_usage_time"
_MEM_METRIC = "kubernetes.io/container/memory/used_bytes"

_NODE_CPU_METRIC = "kubernetes.io/node/cpu/core_usage_time"
_NODE_MEM_METRIC = "kubernetes.io/node/memory/used_bytes"

_DEFAULT_PERIOD = "60s"
_CPU_ALIGNER = "ALIGN_RATE"
_MEM_ALIGNER = "ALIGN_MEAN"
_DEFAULT_REDUCER = "REDUCE_SUM"

_ALIGNERS = {
    "rate": "ALIGN_RATE",
    "mean": "ALIGN_MEAN",
    "max": "ALIGN_MAX",
    "min": "ALIGN_MIN",
    "sum": "ALIGN_SUM",
    "count": "ALIGN_COUNT",
    "delta": "ALIGN_DELTA",
    "none": "ALIGN_NONE",
}

_REDUCERS = {
    "sum": "REDUCE_SUM",
    "mean": "REDUCE_MEAN",
    "max": "REDUCE_MAX",
    "min": "REDUCE_MIN",
    "count": "REDUCE_COUNT",
    "none": "REDUCE_NONE",
}


# -- Auth & HTTP --------------------------------------------------------------

_token = None


class ApiError(click.ClickException):
    """Raised on API failures with actionable hints."""


def get_token():
    """Get access token via gcloud (cached for process lifetime)."""
    global _token
    if _token:
        return _token
    try:
        r = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True,
            text=True,
            check=True,
        )
        _token = r.stdout.strip()
        return _token
    except FileNotFoundError:
        raise ApiError(
            "gcloud not found. Install: https://cloud.google.com/sdk/docs/install"
        )
    except subprocess.CalledProcessError:
        raise ApiError("Auth failed. Run: gcloud auth login")


def set_token(tok):
    """Set the auth token directly, bypassing gcloud CLI."""
    global _token
    _token = tok


_MAX_RETRIES = 3
_RETRY_BASE_SEC = 1.0


def _encode_params(params):
    """Encode params, handling repeated keys (list values) correctly."""
    parts = []
    for k, v in params.items():
        if isinstance(v, list):
            for item in v:
                parts.append((k, item))
        else:
            parts.append((k, v))
    return urlencode(parts)


def api_get(project, path, params=None):
    """GET from Cloud Monitoring API v3 with retry on 429."""
    url = f"{API}/{project}{path}"
    if params:
        url += "?" + _encode_params(params)
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        req = Request(url, headers={"Authorization": f"Bearer {get_token()}"})
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except HTTPError as e:
            body = e.read().decode(errors="replace")
            if e.code == 429 and attempt < _MAX_RETRIES:
                delay = _RETRY_BASE_SEC * (2**attempt) + random.uniform(0, 0.5)
                time.sleep(delay)
                last_exc = e
                continue
            msgs = {
                401: "Auth expired. Run: gcloud auth login",
                403: f"Permission denied for project '{project}'",
                404: "Not found",
                429: (f"Rate limited after {_MAX_RETRIES} retries. " "Try again later"),
            }
            raise ApiError(f"{msgs.get(e.code, f'HTTP {e.code}')}\n{body}")
    raise ApiError(f"Request failed after {_MAX_RETRIES} retries: {last_exc}")


def fetch_time_series(project, params, max_results=None):
    """Fetch time series with automatic pagination."""
    all_series = []
    while True:
        data = api_get(project, "/timeSeries", params)
        all_series.extend(data.get("timeSeries", []))
        if max_results and len(all_series) >= max_results:
            return all_series[:max_results]
        token = data.get("nextPageToken")
        if not token:
            break
        params = {**params, "pageToken": token}
    return all_series


# -- Helpers ------------------------------------------------------------------


def _now():
    """Current time as RFC3339."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(s):
    """Parse relative (15m, 1h, 2d, 1w) or RFC3339 to RFC3339 string."""
    m = re.match(r"^(\d+)([mhdw])$", s)
    if m:
        n, u = int(m.group(1)), m.group(2)
        delta = {
            "m": timedelta(minutes=n),
            "h": timedelta(hours=n),
            "d": timedelta(days=n),
            "w": timedelta(weeks=n),
        }[u]
        return (datetime.now(timezone.utc) - delta).strftime("%Y-%m-%dT%H:%M:%SZ")
    return s


def _parse_period(s):
    """Parse period string like '60s', '5m', '1h' to seconds-based API format."""
    m = re.match(r"^(\d+)([smh])$", s)
    if not m:
        raise ValueError(f"Bad period: {s} (use e.g. 60s, 5m, 1h)")
    n, u = int(m.group(1)), m.group(2)
    secs = {"s": n, "m": n * 60, "h": n * 3600}[u]
    return f"{secs}s"


def _resolve_aligner(s):
    """Resolve human name or ALIGN_ constant to API aligner string."""
    if s.startswith("ALIGN_"):
        return s
    low = s.lower()
    if low in _ALIGNERS:
        return _ALIGNERS[low]
    raise ValueError(f"Unknown aligner: {s} (use {', '.join(_ALIGNERS)})")


def _resolve_reducer(s):
    """Resolve REDUCE_ constant or short name to API reducer string."""
    if s.startswith("REDUCE_"):
        return s
    low = s.lower()
    if low in _REDUCERS:
        return _REDUCERS[low]
    raise ValueError(f"Unknown reducer: {s} (use {', '.join(_REDUCERS)})")


def _escape_re(s):
    """Escape regex special characters for monitoring filter."""
    return re.sub(r"([.+?^${}()|[\]\\])", r"\\\1", s)


def _pod_filter(pod_name, namespace=None, cluster=None, container=None):
    """Build monitoring filter for pod-level k8s_container metrics."""
    parts = ['resource.type = "k8s_container"']
    safe = _escape_re(pod_name)
    parts.append(
        f'resource.labels.pod_name = monitoring.regex.full_match(".*{safe}.*")'
    )
    if namespace:
        parts.append(f'resource.labels.namespace_name = "{namespace}"')
    if cluster:
        parts.append(f'resource.labels.cluster_name = "{cluster}"')
    if container:
        parts.append(f'resource.labels.container_name = "{container}"')
    return " AND ".join(parts)


def _node_filter(node_name, cluster=None):
    """Build monitoring filter for node-level k8s_node metrics."""
    parts = ['resource.type = "k8s_node"']
    safe = _escape_re(node_name)
    parts.append(
        f'resource.labels.node_name = monitoring.regex.full_match(".*{safe}.*")'
    )
    if cluster:
        parts.append(f'resource.labels.cluster_name = "{cluster}"')
    return " AND ".join(parts)


def _build_ts_params(
    metric_type, filt, start, end, period, aligner, reducer=None, group_by=None
):
    """Build timeSeries.list query parameters."""
    start_time = _parse_time(start)
    end_time = _parse_time(end) if end else _now()
    period_str = _parse_period(period) if period else _DEFAULT_PERIOD

    filter_parts = [f'metric.type = "{metric_type}"']
    if filt:
        filter_parts.append(filt)
    combined = " AND ".join(filter_parts)

    params = {
        "filter": combined,
        "interval.startTime": start_time,
        "interval.endTime": end_time,
        "aggregation.alignmentPeriod": period_str,
        "aggregation.perSeriesAligner": aligner,
    }
    if reducer and reducer != "REDUCE_NONE":
        params["aggregation.crossSeriesReducer"] = reducer
    if group_by:
        params["aggregation.groupByFields"] = group_by
    return params


def _extract_value(point):
    """Extract numeric value from a time series point."""
    v = point.get("value", {})
    if "doubleValue" in v:
        return v["doubleValue"]
    if "int64Value" in v:
        return int(v["int64Value"])
    if "distributionValue" in v:
        return v["distributionValue"].get("mean", 0)
    return 0


def _series_values(series):
    """Extract (timestamp, value) pairs from a time series, oldest first."""
    points = series.get("points", [])
    result = []
    for p in reversed(points):
        ts = p["interval"].get("endTime") or p["interval"].get("startTime")
        val = _extract_value(p)
        result.append((ts, val))
    return result


def _series_label(series, key):
    """Extract a resource or metric label from a time series."""
    rl = series.get("resource", {}).get("labels", {})
    ml = series.get("metric", {}).get("labels", {})
    return rl.get(key) or ml.get(key) or ""


def _summary(values):
    """Compute min/avg/max/last from a list of values."""
    if not values:
        return {"min": 0, "avg": 0, "max": 0, "last": 0}
    return {
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
        "last": values[-1],
    }


# -- Rendering ----------------------------------------------------------------

_SPARK = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"


def _sparkline(values):
    """Render a list of numbers as a Unicode sparkline."""
    if not values:
        return ""
    mn, mx = min(values), max(values)
    if mn == mx:
        return _SPARK[3] * len(values)
    rng = mx - mn
    return "".join(_SPARK[min(int((v - mn) / rng * 7), 7)] for v in values)


def _fmt_bytes(n):
    """Format bytes to human-readable (Ki, Mi, Gi)."""
    if n >= 1024**3:
        return f"{n / 1024**3:.1f}Gi"
    if n >= 1024**2:
        return f"{n / 1024**2:.1f}Mi"
    if n >= 1024:
        return f"{n / 1024:.1f}Ki"
    return f"{n:.0f}B"


def _fmt_cpu(cores):
    """Format CPU cores (e.g. 0.250 -> 250m, 1.5 -> 1.500)."""
    if cores < 1:
        return f"{cores * 1000:.0f}m"
    return f"{cores:.3f}"


def _fmt_val(value, metric_type):
    """Format a metric value based on the metric type."""
    low = metric_type.lower()
    if low.endswith("bytes") or "/byte_count" in low:
        return _fmt_bytes(value)
    if "cpu" in low and "usage_time" in low:
        return _fmt_cpu(value)
    return f"{value:.4g}"


def _combine_series(series_list):
    """Combine multiple time series by summing values at each timestamp."""
    by_ts = defaultdict(float)
    for s in series_list:
        for ts, v in _series_values(s):
            by_ts[ts] += v
    ordered = sorted(by_ts.items())
    return [v for _, v in ordered]


def render_pod(cpu_series, mem_series, name):
    """Render CPU + memory for a pod/node with sparklines and summary."""
    click.echo(f"\n  {name}\n")

    for label, series_list, fmt in [
        ("CPU", cpu_series, _fmt_cpu),
        ("Memory", mem_series, _fmt_bytes),
    ]:
        if not series_list:
            click.echo(f"  {label}: no data\n")
            continue

        values = _combine_series(series_list)
        if not values:
            click.echo(f"  {label}: no data points\n")
            continue

        stats = _summary(values)
        spark = _sparkline(values)

        click.echo(f"  {label}  {spark}")
        click.echo(
            f"    min {fmt(stats['min'])}  avg {fmt(stats['avg'])}  "
            f"max {fmt(stats['max'])}  last {fmt(stats['last'])}"
        )
        click.echo()


def render_top(grouped, metric_type, limit):
    """Render ranked table of pods by metric value."""
    fmt = _fmt_cpu if "cpu" in metric_type else _fmt_bytes

    ranked = []
    for pod_name, ts_values in grouped.items():
        values = [v for _, v in sorted(ts_values)]
        if not values:
            continue
        stats = _summary(values)
        spark = _sparkline(values)
        ranked.append((pod_name, stats, spark))

    ranked.sort(key=lambda x: -x[1]["last"])
    ranked = ranked[:limit]

    if not ranked:
        click.echo("No data found.")
        return

    max_name = max(len(r[0]) for r in ranked)
    max_name = max(max_name, 8)

    click.echo(
        f"  {'#':<3}  {'POD':<{max_name}}  {'LAST':>10}  {'AVG':>10}  "
        f"{'MAX':>10}  TREND"
    )
    click.echo("  " + "\u2500" * (max_name + 45))

    for i, (pod_name, stats, spark) in enumerate(ranked, 1):
        click.echo(
            f"  {i:<3}  {pod_name:<{max_name}}  {fmt(stats['last']):>10}  "
            f"{fmt(stats['avg']):>10}  {fmt(stats['max']):>10}  {spark}"
        )
    click.echo()


def render_query(series_list, metric_type):
    """Render raw time series output with sparklines."""
    if not series_list:
        click.echo("No time series found.")
        return

    for s in series_list:
        rl = s.get("resource", {}).get("labels", {})
        ml = s.get("metric", {}).get("labels", {})
        labels = {**rl, **ml}
        label_str = " ".join(f"{k}={v}" for k, v in labels.items())

        vals = _series_values(s)
        values = [v for _, v in vals]
        spark = _sparkline(values)
        stats = _summary(values)
        click.echo(f"\n  {label_str}")
        click.echo(f"    {spark}")
        click.echo(
            f"    min {_fmt_val(stats['min'], metric_type)}  "
            f"avg {_fmt_val(stats['avg'], metric_type)}  "
            f"max {_fmt_val(stats['max'], metric_type)}  "
            f"last {_fmt_val(stats['last'], metric_type)}  "
            f"({len(values)} points)"
        )
    click.echo()


def render_descriptors(descriptors):
    """Render metric descriptor list."""
    if not descriptors:
        click.echo("No metric descriptors found.")
        return

    click.echo(f"\n  {'METRIC TYPE':<65}  {'KIND':<12}  VALUE TYPE")
    click.echo("  " + "\u2500" * 90)
    for d in descriptors:
        click.echo(
            f"  {d.get('type', '?'):<65}  "
            f"{d.get('metricKind', '?'):<12}  "
            f"{d.get('valueType', '?')}"
        )
    click.echo(f"\n  {len(descriptors)} descriptors.")
    click.echo()


# -- Programmatic API ---------------------------------------------------------


def metric_pod(
    project,
    pod_name,
    *,
    start="15m",
    end=None,
    period="60s",
    namespace=None,
    cluster=None,
    container=None,
):
    """Fetch CPU + memory for a pod."""
    filt = _pod_filter(
        pod_name, namespace=namespace, cluster=cluster, container=container
    )

    cpu_params = _build_ts_params(
        _CPU_METRIC,
        filt,
        start,
        end,
        period,
        aligner=_CPU_ALIGNER,
        reducer=_DEFAULT_REDUCER,
        group_by=["resource.labels.pod_name"],
    )
    mem_params = _build_ts_params(
        _MEM_METRIC,
        filt,
        start,
        end,
        period,
        aligner=_MEM_ALIGNER,
        reducer=_DEFAULT_REDUCER,
        group_by=["resource.labels.pod_name"],
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        cpu_fut = pool.submit(fetch_time_series, project, cpu_params)
        mem_fut = pool.submit(fetch_time_series, project, mem_params)
        cpu_series = cpu_fut.result()
        mem_series = mem_fut.result()

    return {"cpu": cpu_series, "mem": mem_series, "pod": pod_name}


def metric_node(
    project, node_name, *, start="1h", end=None, period="60s", cluster=None
):
    """Fetch CPU + memory for a node."""
    filt = _node_filter(node_name, cluster=cluster)

    cpu_params = _build_ts_params(
        _NODE_CPU_METRIC,
        filt,
        start,
        end,
        period,
        aligner=_CPU_ALIGNER,
        reducer=_DEFAULT_REDUCER,
        group_by=["resource.labels.node_name"],
    )
    mem_params = _build_ts_params(
        _NODE_MEM_METRIC,
        filt,
        start,
        end,
        period,
        aligner=_MEM_ALIGNER,
        reducer=_DEFAULT_REDUCER,
        group_by=["resource.labels.node_name"],
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        cpu_fut = pool.submit(fetch_time_series, project, cpu_params)
        mem_fut = pool.submit(fetch_time_series, project, mem_params)
        cpu_series = cpu_fut.result()
        mem_series = mem_fut.result()

    return {"cpu": cpu_series, "mem": mem_series, "node": node_name}


def metric_top(
    project,
    metric,
    *,
    start="15m",
    end=None,
    period="60s",
    namespace=None,
    cluster=None,
    limit=10,
):
    """Rank pods by CPU or memory usage."""
    if metric == "cpu":
        metric_type, aligner = _CPU_METRIC, _CPU_ALIGNER
    elif metric == "memory":
        metric_type, aligner = _MEM_METRIC, _MEM_ALIGNER
    else:
        raise ValueError(f"Unknown metric: {metric} (use 'cpu' or 'memory')")

    parts = ['resource.type = "k8s_container"']
    if namespace:
        parts.append(f'resource.labels.namespace_name = "{namespace}"')
    if cluster:
        parts.append(f'resource.labels.cluster_name = "{cluster}"')
    filt = " AND ".join(parts)

    params = _build_ts_params(
        metric_type,
        filt,
        start,
        end,
        period,
        aligner=aligner,
        reducer=_DEFAULT_REDUCER,
        group_by=["resource.labels.pod_name"],
    )
    series_list = fetch_time_series(project, params)

    grouped = defaultdict(list)
    for s in series_list:
        pod = _series_label(s, "pod_name")
        grouped[pod].extend(_series_values(s))

    return {"grouped": dict(grouped), "metric_type": metric_type}


def metric_query(
    project,
    metric_type,
    *,
    start="15m",
    end=None,
    period="60s",
    filt=None,
    aligner="ALIGN_MEAN",
    reducer=None,
    group_by=None,
):
    """Generic metric query."""
    aligner = _resolve_aligner(aligner)
    if reducer:
        reducer = _resolve_reducer(reducer)
    params = _build_ts_params(
        metric_type,
        filt,
        start,
        end,
        period,
        aligner=aligner,
        reducer=reducer,
        group_by=group_by,
    )
    return fetch_time_series(project, params)


def metric_descriptors(project, *, filt=None):
    """List metric descriptors, optionally filtered by type prefix."""
    params = {}
    if filt:
        params["filter"] = f'metric.type = starts_with("{filt}")'
    descriptors = []
    while True:
        data = api_get(project, "/metricDescriptors", params)
        descriptors.extend(data.get("metricDescriptors", []))
        token = data.get("nextPageToken")
        if not token:
            break
        params = {**params, "pageToken": token}
    return descriptors


# -- CLI ----------------------------------------------------------------------


def _cli_validate(fn):
    """Decorator: convert library exceptions to Click exceptions."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ValueError as e:
            raise click.BadParameter(str(e))
        except ApiError:
            raise
        except Exception as e:
            raise click.ClickException(str(e))

    return wrapper


@click.group()
@click.option(
    "--project",
    envvar="GOOGLE_CLOUD_PROJECT",
    required=True,
    help="GCP project ID (or set GOOGLE_CLOUD_PROJECT)",
)
@click.option("--json", "as_json", is_flag=True, help="Raw JSON output")
@click.pass_context
def cli(ctx, project, as_json):
    """gmetrics - query Kubernetes metrics from GCP Cloud Monitoring."""
    ctx.ensure_object(dict)
    ctx.obj["project"] = project
    ctx.obj["json"] = as_json
    try:
        get_token()
    except ApiError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument("pod_name")
@click.option(
    "--start",
    default="15m",
    show_default=True,
    help="Start time (15m, 1h, 2d, 1w, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--period", default="60s", show_default=True, help="Alignment period (60s, 5m, 1h)"
)
@click.option("--namespace", default=None, help="Kubernetes namespace")
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.option("--container", default=None, help="Container name (default: all)")
@click.pass_context
@_cli_validate
def pod(ctx, pod_name, start, end, period, namespace, cluster, container):
    """CPU and memory for a Kubernetes pod.

    POD_NAME is a substring match (e.g. 'my-service' matches
    'my-service-abc-123').

    \b
    Examples:
      gmetrics pod my-service
      gmetrics pod my-service --start 1h --cluster us-east1
      gmetrics pod my-pod --namespace production --container main
    """
    result = metric_pod(
        ctx.obj["project"],
        pod_name,
        start=start,
        end=end,
        period=period,
        namespace=namespace,
        cluster=cluster,
        container=container,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        render_pod(result["cpu"], result["mem"], pod_name)


@cli.command()
@click.argument("metric", type=click.Choice(["cpu", "memory"]))
@click.option(
    "--start",
    default="15m",
    show_default=True,
    help="Start time (15m, 1h, 2d, 1w, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--period", default="60s", show_default=True, help="Alignment period (60s, 5m)"
)
@click.option("--namespace", default=None, help="Kubernetes namespace")
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.option(
    "--limit", default=10, show_default=True, type=int, help="Number of pods to show"
)
@click.pass_context
@_cli_validate
def top(ctx, metric, start, end, period, namespace, cluster, limit):
    """Rank pods by CPU or memory usage.

    \b
    Examples:
      gmetrics top cpu --namespace prod --start 15m
      gmetrics top memory --cluster us-east1 --limit 20
    """
    result = metric_top(
        ctx.obj["project"],
        metric,
        start=start,
        end=end,
        period=period,
        namespace=namespace,
        cluster=cluster,
        limit=limit,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        render_top(result["grouped"], result["metric_type"], limit)


@cli.command()
@click.argument("node_name")
@click.option(
    "--start",
    default="1h",
    show_default=True,
    help="Start time (15m, 1h, 2d, 1w, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--period", default="60s", show_default=True, help="Alignment period (60s, 5m, 1h)"
)
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.pass_context
@_cli_validate
def node(ctx, node_name, start, end, period, cluster):
    """CPU and memory for a Kubernetes node.

    NODE_NAME is a substring match.

    \b
    Examples:
      gmetrics node gke-my-cluster-pool-abc
      gmetrics node my-node --start 2h --cluster us-east1
    """
    result = metric_node(
        ctx.obj["project"],
        node_name,
        start=start,
        end=end,
        period=period,
        cluster=cluster,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        render_pod(result["cpu"], result["mem"], node_name)


@cli.command()
@click.argument("metric_type")
@click.option(
    "--start",
    default="15m",
    show_default=True,
    help="Start time (15m, 1h, 2d, 1w, or RFC3339)",
)
@click.option("--end", default=None, help="End time (default: now)")
@click.option(
    "--filter",
    "filt",
    default=None,
    help="Additional monitoring filter (AND-ed with metric.type)",
)
@click.option(
    "--aligner",
    default="ALIGN_MEAN",
    show_default=True,
    help="Per-series aligner (rate, mean, max, min, sum, delta)",
)
@click.option(
    "--reducer", default=None, help="Cross-series reducer (sum, mean, max, min, count)"
)
@click.option(
    "--period", default="60s", show_default=True, help="Alignment period (60s, 5m, 1h)"
)
@click.option("--group-by", default=None, help="Comma-separated fields to group by")
@click.pass_context
@_cli_validate
def query(ctx, metric_type, start, end, filt, aligner, reducer, period, group_by):
    """Query any metric type with full control over aggregation.

    \b
    Examples:
      gmetrics query "kubernetes.io/container/memory/used_bytes" \\
          --filter 'resource.labels.pod_name="my-pod"' --aligner mean
      gmetrics query "custom.googleapis.com/my/metric" --aligner rate --period 5m
    """
    group_fields = [g.strip() for g in group_by.split(",")] if group_by else None
    series = metric_query(
        ctx.obj["project"],
        metric_type,
        start=start,
        end=end,
        period=period,
        filt=filt,
        aligner=aligner,
        reducer=reducer,
        group_by=group_fields,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(series, indent=2))
    else:
        render_query(series, metric_type)


@cli.command()
@click.option(
    "--filter",
    "filt",
    default=None,
    help="Metric type prefix (e.g. kubernetes.io/container)",
)
@click.pass_context
@_cli_validate
def metrics(ctx, filt):
    """List or search metric descriptors.

    \b
    Examples:
      gmetrics metrics --filter kubernetes.io/container
      gmetrics metrics --filter custom.googleapis.com
    """
    descriptors = metric_descriptors(ctx.obj["project"], filt=filt)
    if ctx.obj["json"]:
        click.echo(json.dumps(descriptors, indent=2))
    else:
        render_descriptors(descriptors)


if __name__ == "__main__":
    cli()
