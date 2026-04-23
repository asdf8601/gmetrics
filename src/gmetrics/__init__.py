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
    "metric_labels",
    "metric_descriptors",
    "auto_period",
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


_NICE_PERIODS_SEC = (60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400)


def _window_seconds(start):
    """Return the window length in seconds for a relative start string.

    Returns None if `start` is not relative (e.g. RFC3339 absolute).
    """
    if not start:
        return None
    m = re.match(r"^(\d+)([mhdw])$", start)
    if not m:
        return None
    n, u = int(m.group(1)), m.group(2)
    unit_secs = {"m": 60, "h": 3600, "d": 86400, "w": 604800}[u]
    return n * unit_secs


def _pretty_period(period):
    """Render a seconds-based period like '600s' as '10m', '3600s' as '1h'."""
    if not period:
        return ""
    m = re.match(r"^(\d+)s$", period)
    if not m:
        return period
    secs = int(m.group(1))
    if secs % 86400 == 0 and secs >= 86400:
        return f"{secs // 86400}d"
    if secs % 3600 == 0 and secs >= 3600:
        return f"{secs // 3600}h"
    if secs % 60 == 0 and secs >= 60:
        return f"{secs // 60}m"
    return f"{secs}s"


def auto_period(start, target_bars=60):
    """Pick an alignment period so the window renders ~target_bars buckets.

    Snaps to human-friendly values (60s, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1d).
    Falls back to "60s" for absolute or unparseable start strings.
    """
    secs = _window_seconds(start)
    if not secs:
        return "60s"
    bucket = max(60, secs // target_bars)
    for v in _NICE_PERIODS_SEC:
        if bucket <= v:
            return f"{v}s"
    return f"{_NICE_PERIODS_SEC[-1]}s"


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


def _validate_user_filter(filt):
    """Reject common filter mistakes with actionable errors.

    GCP Monitoring filter language does NOT support `=~` or `!~`.
    Use `monitoring.regex.full_match("pat")` or `starts_with("prefix")`.
    """
    if not filt:
        return
    if re.search(r"=~|!~", filt):
        raise ValueError(
            "Invalid filter: GCP Monitoring does not support '=~' or '!~'. "
            'Use: label = monitoring.regex.full_match("regex") '
            'or: label = starts_with("prefix")'
        )


def _memory_type_filter(memory_type):
    """Build memory_type filter fragment, or return None for 'any'."""
    if not memory_type or memory_type == "any":
        return None
    return f'metric.labels.memory_type = "{memory_type}"'


# Short aliases for common k8s_container label fields.
# Users can pass either the short name ("cluster") or the full field
# ("resource.labels.cluster_name") to --show.
_FIELD_SHORTCUTS = {
    "cluster": "resource.labels.cluster_name",
    "namespace": "resource.labels.namespace_name",
    "container": "resource.labels.container_name",
    "location": "resource.labels.location",
    "node": "resource.labels.node_name",
    "pod": "resource.labels.pod_name",
    "memory_type": "metric.labels.memory_type",
    "mem_type": "metric.labels.memory_type",
}


def _resolve_show_fields(show_str):
    """Parse a CSV list of field shortcuts or full field paths.

    Returns list of (display, short_key, full_field) tuples. `display` is
    the user-facing column header (shortcut name if one was given, else
    the trailing label). `short_key` is the trailing label name
    (e.g. "cluster_name") used to read from series.resource.labels or
    series.metric.labels.
    """
    if not show_str:
        return []
    out = []
    for raw in show_str.split(","):
        name = raw.strip()
        if not name:
            continue
        if name in _FIELD_SHORTCUTS:
            display = name
            full = _FIELD_SHORTCUTS[name]
        else:
            full = name
            if "." not in full:
                # Assume resource.labels.<name> for bare label names.
                full = f"resource.labels.{full}"
            display = full.rsplit(".", 1)[-1]
        short = full.rsplit(".", 1)[-1]
        out.append((display, short, full))
    return out


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
    """Compute min/avg/max/last from a list of values. Ignores None (gaps)."""
    vals = [v for v in values if v is not None]
    if not vals:
        return {"min": 0, "avg": 0, "max": 0, "last": 0}
    last_val = next((v for v in reversed(values) if v is not None), vals[-1])
    return {
        "min": min(vals),
        "avg": sum(vals) / len(vals),
        "max": max(vals),
        "last": last_val,
    }


# -- Rendering ----------------------------------------------------------------

_SPARK = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"


def _sparkline(values):
    """Render numbers as Unicode sparkline. None entries render as gap (space)."""
    if not values:
        return ""
    real = [v for v in values if v is not None]
    if not real:
        return " " * len(values)
    mn, mx = min(real), max(real)
    if mn == mx:
        return "".join(" " if v is None else _SPARK[3] for v in values)
    rng = mx - mn
    out = []
    for v in values:
        if v is None:
            out.append(" ")
        else:
            out.append(_SPARK[min(int((v - mn) / rng * 7), 7)])
    return "".join(out)


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


def _ts_to_epoch(ts_str):
    """Parse RFC3339 timestamp to epoch seconds (UTC)."""
    s = ts_str.replace("Z", "+00:00")
    return int(datetime.fromisoformat(s).timestamp())


def _period_secs(period):
    """Return period in seconds from API string like '300s'."""
    if not period:
        return 60
    m = re.match(r"^(\d+)s$", period)
    if m:
        return int(m.group(1))
    m = re.match(r"^(\d+)([smh])$", period)
    if m:
        n, u = int(m.group(1)), m.group(2)
        return {"s": n, "m": n * 60, "h": n * 3600}[u]
    return 60


def _align_combined(series_list, start, end, period):
    """Sum series per bucket across [start, end] grid. None for missing buckets."""
    if not series_list:
        return []
    period_sec = _period_secs(period)
    start_epoch = _ts_to_epoch(_parse_time(start))
    end_epoch = _ts_to_epoch(_parse_time(end) if end else _now())

    by_bucket = defaultdict(float)
    has = set()
    for s in series_list:
        for ts, v in _series_values(s):
            bucket = (_ts_to_epoch(ts) // period_sec) * period_sec
            by_bucket[bucket] += v
            has.add(bucket)

    result = []
    b = (start_epoch // period_sec) * period_sec
    last_bucket = (end_epoch // period_sec) * period_sec
    while b <= last_bucket:
        result.append(by_bucket[b] if b in has else None)
        b += period_sec
    return result


def _align_combined_ts(series_list, start, end, period):
    """Like `_align_combined` but returns [(datetime, value-or-None), ...]."""
    if not series_list:
        return []
    period_sec = _period_secs(period)
    start_epoch = _ts_to_epoch(_parse_time(start))
    end_epoch = _ts_to_epoch(_parse_time(end) if end else _now())

    by_bucket = defaultdict(float)
    has = set()
    for s in series_list:
        for ts, v in _series_values(s):
            bucket = (_ts_to_epoch(ts) // period_sec) * period_sec
            by_bucket[bucket] += v
            has.add(bucket)

    result = []
    b = (start_epoch // period_sec) * period_sec
    last_bucket = (end_epoch // period_sec) * period_sec
    while b <= last_bucket:
        dt = datetime.fromtimestamp(b, tz=timezone.utc)
        result.append((dt, by_bucket[b] if b in has else None))
        b += period_sec
    return result


def _align_pairs(ts_value_pairs, start, end, period):
    """Align single-series (ts, value) pairs to [start, end] grid. None for gaps."""
    if not ts_value_pairs:
        return []
    period_sec = _period_secs(period)
    start_epoch = _ts_to_epoch(_parse_time(start))
    end_epoch = _ts_to_epoch(_parse_time(end) if end else _now())

    by_bucket = {}
    for ts, v in ts_value_pairs:
        bucket = (_ts_to_epoch(ts) // period_sec) * period_sec
        by_bucket[bucket] = v

    result = []
    b = (start_epoch // period_sec) * period_sec
    last_bucket = (end_epoch // period_sec) * period_sec
    while b <= last_bucket:
        result.append(by_bucket.get(b))
        b += period_sec
    return result


def render_pod(cpu_series, mem_series, name, period=None, start=None, end=None, memory_type=None):
    """Render CPU + memory for a pod/node with sparklines and summary.

    When `start` is provided, values are aligned to the [start, end] bucket
    grid so missing buckets render as gaps in the sparkline.
    """
    pretty = _pretty_period(period)
    header = f"\n  {name}"
    meta = []
    if pretty:
        meta.append(f"1 bar = {pretty}")
    if memory_type:
        meta.append(f"memory_type={memory_type}")
    if meta:
        header += "  (" + ", ".join(meta) + ")"
    click.echo(header + "\n")

    sections = [
        ("CPU", cpu_series, _fmt_cpu),
        ("Memory", mem_series, _fmt_bytes),
    ]
    label_w = max(len(lbl) for lbl, _, _ in sections)
    stats_indent = " " * (2 + label_w + 2)

    for label, series_list, fmt in sections:
        if not series_list:
            click.echo(f"  {label:<{label_w}}: no data\n")
            continue

        if start:
            values = _align_combined(series_list, start, end, period)
        else:
            values = _combine_series(series_list)
        if not values or all(v is None for v in values):
            click.echo(f"  {label:<{label_w}}: no data points\n")
            continue

        stats = _summary(values)
        spark = _sparkline(values)

        click.echo(f"  {label:<{label_w}}  {spark}")
        click.echo(
            f"{stats_indent}min {fmt(stats['min'])}  avg {fmt(stats['avg'])}  "
            f"max {fmt(stats['max'])}  last {fmt(stats['last'])}"
        )
        click.echo()


def _pick_mem_unit(max_bytes):
    """Return (scale, unit) to render memory in MiB or GiB based on magnitude."""
    if max_bytes >= 1024**3:
        return 1 / (1024**3), "GiB"
    return 1 / (1024**2), "MiB"


def render_pod_lines(cpu_series, mem_series, name, period=None, start=None, end=None, memory_type=None):
    """Render CPU + memory as line charts via plotext.

    Two stacked subplots: CPU in millicores (top), Memory in MiB/GiB (bottom).
    Requires `start` for timestamp alignment; unaligned mode falls back to
    `render_pod`.
    """
    if not start:
        return render_pod(cpu_series, mem_series, name, period=period, memory_type=memory_type)

    import plotext as plt

    pretty = _pretty_period(period)
    meta = []
    if pretty:
        meta.append(f"1 point = {pretty}")
    if memory_type:
        meta.append(f"memory_type={memory_type}")
    subtitle = "  (" + ", ".join(meta) + ")" if meta else ""
    click.echo(f"\n  {name}{subtitle}\n")

    cpu_pairs = _align_combined_ts(cpu_series, start, end, period) if cpu_series else []
    mem_pairs = _align_combined_ts(mem_series, start, end, period) if mem_series else []

    cpu_x = [dt for dt, v in cpu_pairs if v is not None]
    cpu_y = [v * 1000.0 for _, v in cpu_pairs if v is not None]

    mem_max = max((v for _, v in mem_pairs if v is not None), default=0)
    mem_scale, mem_unit = _pick_mem_unit(mem_max)
    mem_x = [dt for dt, v in mem_pairs if v is not None]
    mem_y = [v * mem_scale for _, v in mem_pairs if v is not None]

    if not cpu_y and not mem_y:
        click.echo("  no data points\n")
        return

    plt.clf()
    plt.theme("clear")
    plt.subplots(2, 1)

    plt.subplot(1, 1)
    plt.date_form("H:M")
    plt.title("CPU (mCPU)")
    if cpu_y:
        plt.plot(plt.datetimes_to_string(cpu_x), cpu_y, marker="braille")
        plt.ylim(0, max(cpu_y) * 1.1 if max(cpu_y) > 0 else 1)

    plt.subplot(2, 1)
    plt.date_form("H:M")
    plt.title(f"Memory ({mem_unit})")
    if mem_y:
        plt.plot(plt.datetimes_to_string(mem_x), mem_y, marker="braille")
        plt.ylim(0, max(mem_y) * 1.1 if max(mem_y) > 0 else 1)

    plt.show()
    click.echo()


def render_top(grouped, metric_type, limit, rows=None, columns=None, period=None, order="desc", start=None, end=None):
    """Render ranked table of pods by metric value.

    When `rows` (list of {pod, extras, values}) and `columns` are provided,
    additional columns are rendered between POD and LAST. Otherwise falls
    back to the legacy grouped-by-pod view.

    order: "desc" (highest last first) or "asc" (lowest first).
    """
    fmt = _fmt_cpu if "cpu" in metric_type else _fmt_bytes
    columns = columns or []

    def _values_for(ts_pairs):
        sorted_pairs = sorted(ts_pairs)
        if start:
            return _align_pairs(sorted_pairs, start, end, period)
        return [v for _, v in sorted_pairs]

    ranked = []
    if rows and columns:
        for r in rows:
            values = _values_for(r["values"])
            if not values or all(v is None for v in values):
                continue
            stats = _summary(values)
            spark = _sparkline(values)
            ranked.append((r["pod"], r["extras"], stats, spark))
    else:
        for pod_name, ts_values in grouped.items():
            values = _values_for(ts_values)
            if not values or all(v is None for v in values):
                continue
            stats = _summary(values)
            spark = _sparkline(values)
            ranked.append((pod_name, {}, stats, spark))

    ranked.sort(key=lambda x: x[2]["last"], reverse=(order != "asc"))
    ranked = ranked[:limit]

    if not ranked:
        click.echo("No data found.")
        return

    pretty = _pretty_period(period)
    if pretty:
        click.echo(f"  (1 TREND bar = {pretty})")

    max_name = max(len(r[0]) for r in ranked)
    max_name = max(max_name, 8)

    col_widths = {}
    for col in columns:
        width = max(len(col), *(len(str(r[1].get(col, ""))) for r in ranked))
        col_widths[col] = max(width, 8)

    header = f"  {'#':<3}  {'POD':<{max_name}}"
    for col in columns:
        header += f"  {col.upper():<{col_widths[col]}}"
    header += f"  {'LAST':>10}  {'AVG':>10}  {'MAX':>10}  TREND"
    click.echo(header)

    extra_width = sum(col_widths[c] + 2 for c in columns)
    click.echo("  " + "\u2500" * (max_name + 45 + extra_width))

    for i, (pod_name, extras, stats, spark) in enumerate(ranked, 1):
        line = f"  {i:<3}  {pod_name:<{max_name}}"
        for col in columns:
            val = str(extras.get(col, ""))
            line += f"  {val:<{col_widths[col]}}"
        line += (
            f"  {fmt(stats['last']):>10}  {fmt(stats['avg']):>10}  "
            f"{fmt(stats['max']):>10}  {spark}"
        )
        click.echo(line)
    click.echo()


_QUERY_LABEL_ORDER = (
    "pod_name",
    "node_name",
    "container_name",
    "namespace_name",
    "cluster_name",
    "location",
    "memory_type",
)
_QUERY_LABEL_ABBR = {
    "pod_name": "pod",
    "node_name": "node",
    "container_name": "container",
    "namespace_name": "ns",
    "cluster_name": "cluster",
    "location": "loc",
    "memory_type": "mem_type",
}
_QUERY_LABEL_HIDE = {"project_id"}


def _format_query_labels(rl, ml):
    """Format series labels in stable order with short keys."""
    combined = {**rl, **ml}
    seen = set()
    parts = []
    for key in _QUERY_LABEL_ORDER:
        if key in combined:
            parts.append(f"{_QUERY_LABEL_ABBR.get(key, key)}={combined[key]}")
            seen.add(key)
    for k, v in combined.items():
        if k in seen or k in _QUERY_LABEL_HIDE:
            continue
        parts.append(f"{k}={v}")
    return "  ".join(parts)


def render_query(series_list, metric_type, period=None, start=None, end=None):
    """Render raw time series output with sparklines."""
    if not series_list:
        click.echo("No time series found.")
        return

    pretty = _pretty_period(period)
    if pretty:
        click.echo(f"\n  (1 bar = {pretty})")

    for s in series_list:
        rl = s.get("resource", {}).get("labels", {})
        ml = s.get("metric", {}).get("labels", {})
        label_str = _format_query_labels(rl, ml)

        vals = _series_values(s)
        if start:
            values = _align_pairs(vals, start, end, period)
        else:
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
            f"({sum(1 for v in values if v is not None)} points)"
        )
    click.echo()


def render_descriptors(descriptors):
    """Render metric descriptor list."""
    if not descriptors:
        click.echo("No metric descriptors found.")
        return

    click.echo(f"\n  {'KIND':<12}  {'VALUE TYPE':<14}  METRIC TYPE")
    click.echo("  " + "\u2500" * 90)
    for d in descriptors:
        click.echo(
            f"  {d.get('metricKind', '?'):<12}  "
            f"{d.get('valueType', '?'):<14}  "
            f"{d.get('type', '?')}"
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
    memory_type="any",
):
    """Fetch CPU + memory for a pod.

    memory_type: "any" (default, total — matches GCP Metrics Explorer),
        "non-evictable" (working set, matches `kubectl top`), or "evictable"
        (page cache only).
    """
    filt = _pod_filter(
        pod_name, namespace=namespace, cluster=cluster, container=container
    )
    mem_extra = _memory_type_filter(memory_type)
    mem_filt = f"{filt} AND {mem_extra}" if mem_extra else filt

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
        mem_filt,
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
    project,
    node_name,
    *,
    start="1h",
    end=None,
    period="60s",
    cluster=None,
    memory_type="any",
):
    """Fetch CPU + memory for a node.

    memory_type: "any" (default, total), "non-evictable" (working set),
        or "evictable" (page cache).
    """
    filt = _node_filter(node_name, cluster=cluster)
    mem_extra = _memory_type_filter(memory_type)
    mem_filt = f"{filt} AND {mem_extra}" if mem_extra else filt

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
        mem_filt,
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
    pod_pattern=None,
    memory_type="any",
    show_fields=None,
):
    """Rank pods by CPU or memory usage.

    pod_pattern: optional substring to filter pod names (e.g. "my-service").
    memory_type: "non-evictable" (default), "evictable", or "any" — only
        applied when metric == "memory" (avoids double-counting).
    show_fields: optional list of (short_key, full_field) tuples to include
        as extra columns and in the group_by. Typically built from
        _resolve_show_fields() but callers may pass their own list.
    """
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
    if pod_pattern:
        safe = _escape_re(pod_pattern)
        parts.append(
            f'resource.labels.pod_name = monitoring.regex.full_match(".*{safe}.*")'
        )
    if metric == "memory":
        mem_extra = _memory_type_filter(memory_type)
        if mem_extra:
            parts.append(mem_extra)
    filt = " AND ".join(parts)

    show_fields = list(show_fields or [])
    group_by = ["resource.labels.pod_name"] + [full for _, _, full in show_fields]

    params = _build_ts_params(
        metric_type,
        filt,
        start,
        end,
        period,
        aligner=aligner,
        reducer=_DEFAULT_REDUCER,
        group_by=group_by,
    )
    series_list = fetch_time_series(project, params)

    rows = []
    grouped = defaultdict(list)
    for s in series_list:
        pod = _series_label(s, "pod_name")
        extras = {
            display: _series_label(s, short) for display, short, _ in show_fields
        }
        vals = _series_values(s)
        grouped[pod].extend(vals)
        rows.append({"pod": pod, "extras": extras, "values": vals})

    return {
        "grouped": dict(grouped),
        "rows": rows,
        "metric_type": metric_type,
        "columns": [display for display, _, _ in show_fields],
    }


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
    _validate_user_filter(filt)
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


def metric_labels(project, metric_type, *, filt=None, start="5m"):
    """Discover available resource + metric labels for a metric type.

    Returns {"resource": {key: sorted_values}, "metric": {key: sorted_values}}.
    Probes a short time window without aggregation so every label is
    preserved in the response.
    """
    _validate_user_filter(filt)
    filter_parts = [f'metric.type = "{metric_type}"']
    if filt:
        filter_parts.append(filt)
    params = {
        "filter": " AND ".join(filter_parts),
        "interval.startTime": _parse_time(start),
        "interval.endTime": _now(),
        "aggregation.alignmentPeriod": _parse_period("5m"),
        "aggregation.perSeriesAligner": "ALIGN_NONE",
    }
    series_list = fetch_time_series(project, params, max_results=500)

    resource_labels = defaultdict(set)
    metric_labels_out = defaultdict(set)
    for s in series_list:
        for k, v in s.get("resource", {}).get("labels", {}).items():
            resource_labels[k].add(v)
        for k, v in s.get("metric", {}).get("labels", {}).items():
            metric_labels_out[k].add(v)

    return {
        "metric_type": metric_type,
        "series_sampled": len(series_list),
        "resource": {k: sorted(v) for k, v in resource_labels.items()},
        "metric": {k: sorted(v) for k, v in metric_labels_out.items()},
    }


def render_labels(info, max_samples=5):
    """Render label discovery output."""
    click.echo(f"\n  metric: {info['metric_type']}")
    click.echo(f"  series sampled: {info['series_sampled']}\n")

    for section, title in [("resource", "resource.labels"), ("metric", "metric.labels")]:
        labels = info.get(section, {})
        if not labels:
            click.echo(f"  {title}: (none)\n")
            continue
        click.echo(f"  {title}:")
        key_w = max(len(k) for k in labels) if labels else 0
        for key in sorted(labels):
            values = labels[key]
            n = len(values)
            samples = values[:max_samples]
            suffix = ", ..." if n > max_samples else ""
            click.echo(
                f"    {key:<{key_w}}  ({n} values)  e.g. "
                f"{', '.join(repr(v) for v in samples)}{suffix}"
            )
        click.echo()


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
    "--period",
    default=None,
    help="Alignment period (60s, 5m, 1h). Default: auto-scaled from --start to render ~60 sparkline buckets.",
)
@click.option("--namespace", default=None, help="Kubernetes namespace")
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.option("--container", default=None, help="Container name (default: all)")
@click.option(
    "--memory-type",
    type=click.Choice(["any", "non-evictable", "evictable"]),
    default="any",
    show_default=True,
    help=(
        "Memory series filter. "
        "'any'=total (RSS+cache, matches GCP Metrics Explorer). "
        "'non-evictable'=working set (matches `kubectl top`, used for OOM). "
        "'evictable'=page cache only."
    ),
)
@click.option(
    "--lines",
    is_flag=True,
    default=False,
    help="Render as full line chart (plotext) instead of sparkline.",
)
@click.pass_context
@_cli_validate
def pod(ctx, pod_name, start, end, period, namespace, cluster, container, memory_type, lines):
    """CPU and memory for a Kubernetes pod.

    POD_NAME is a substring match (e.g. 'my-service' matches
    'my-service-abc-123').

    \b
    Examples:
      gmetrics pod my-service
      gmetrics pod my-service --start 1h --cluster us-east1
      gmetrics pod my-pod --namespace production --container main
    """
    period = period or auto_period(start)
    result = metric_pod(
        ctx.obj["project"],
        pod_name,
        start=start,
        end=end,
        period=period,
        namespace=namespace,
        cluster=cluster,
        container=container,
        memory_type=memory_type,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        renderer = render_pod_lines if lines else render_pod
        renderer(
            result["cpu"],
            result["mem"],
            pod_name,
            period=period,
            start=start,
            end=end,
            memory_type=memory_type,
        )


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
    "--period",
    default=None,
    help="Alignment period (60s, 5m, 1h). Default: auto-scaled from --start.",
)
@click.option("--namespace", default=None, help="Kubernetes namespace")
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.option(
    "--limit", default=10, show_default=True, type=int, help="Number of pods to show"
)
@click.option(
    "--pod-pattern",
    default=None,
    help="Substring filter on pod name (e.g. 'my-service')",
)
@click.option(
    "--memory-type",
    type=click.Choice(["any", "non-evictable", "evictable"]),
    default="any",
    show_default=True,
    help=(
        "Memory series filter (memory metric only). "
        "'any'=total (matches GCP Metrics Explorer). "
        "'non-evictable'=working set (matches `kubectl top`). "
        "'evictable'=page cache."
    ),
)
@click.option(
    "--show",
    "show",
    default=None,
    help=(
        "Extra columns to include (comma-separated). "
        "Shortcuts: cluster, namespace, container, location, node, memory_type. "
        "Or full GCP field paths, e.g. 'resource.labels.cluster_name'."
    ),
)
@click.option(
    "--order",
    type=click.Choice(["desc", "asc"]),
    default="desc",
    show_default=True,
    help="Sort direction by LAST value. 'asc' = lowest usage first.",
)
@click.pass_context
@_cli_validate
def top(ctx, metric, start, end, period, namespace, cluster, limit, pod_pattern, memory_type, show, order):
    """Rank pods by CPU or memory usage.

    \b
    Examples:
      gmetrics top cpu --namespace prod --start 15m
      gmetrics top memory --cluster us-east1 --limit 20
      gmetrics top memory --pod-pattern my-service --limit 20
      gmetrics top memory --pod-pattern my-service --show cluster,namespace
      gmetrics top cpu --order asc --limit 20   # lowest CPU pods
    """
    period = period or auto_period(start)
    show_fields = _resolve_show_fields(show)
    result = metric_top(
        ctx.obj["project"],
        metric,
        start=start,
        end=end,
        period=period,
        namespace=namespace,
        cluster=cluster,
        limit=limit,
        pod_pattern=pod_pattern,
        memory_type=memory_type,
        show_fields=show_fields,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        render_top(
            result["grouped"],
            result["metric_type"],
            limit,
            rows=result.get("rows"),
            columns=result.get("columns"),
            period=period,
            order=order,
            start=start,
            end=end,
        )


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
    "--period",
    default=None,
    help="Alignment period (60s, 5m, 1h). Default: auto-scaled from --start.",
)
@click.option("--cluster", default=None, help="Kubernetes cluster name")
@click.option(
    "--memory-type",
    type=click.Choice(["any", "non-evictable", "evictable"]),
    default="any",
    show_default=True,
    help=(
        "Memory series filter. "
        "'any'=total (matches GCP Metrics Explorer). "
        "'non-evictable'=working set (matches `kubectl top`). "
        "'evictable'=page cache."
    ),
)
@click.option(
    "--lines",
    is_flag=True,
    default=False,
    help="Render as full line chart (plotext) instead of sparkline.",
)
@click.pass_context
@_cli_validate
def node(ctx, node_name, start, end, period, cluster, memory_type, lines):
    """CPU and memory for a Kubernetes node.

    NODE_NAME is a substring match.

    \b
    Examples:
      gmetrics node gke-my-cluster-pool-abc
      gmetrics node my-node --start 2h --cluster us-east1
    """
    period = period or auto_period(start)
    result = metric_node(
        ctx.obj["project"],
        node_name,
        start=start,
        end=end,
        period=period,
        cluster=cluster,
        memory_type=memory_type,
    )
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
    else:
        renderer = render_pod_lines if lines else render_pod
        renderer(
            result["cpu"],
            result["mem"],
            node_name,
            period=period,
            start=start,
            end=end,
            memory_type=memory_type,
        )


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
    help=(
        'Additional monitoring filter (AND-ed). Examples: '
        'resource.labels.pod_name = "my-pod", '
        'resource.labels.pod_name = starts_with("prefix"), '
        'metric.labels.memory_type = "non-evictable". '
        "NOTE: '=~' is NOT supported — use starts_with() or "
        "monitoring.regex.full_match()."
    ),
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
    "--period",
    default=None,
    help="Alignment period (60s, 5m, 1h). Default: auto-scaled from --start.",
)
@click.option("--group-by", default=None, help="Comma-separated fields to group by")
@click.pass_context
@_cli_validate
def query(ctx, metric_type, start, end, filt, aligner, reducer, period, group_by):
    """Query any metric type with full control over aggregation.

    \b
    Examples:
      gmetrics query "kubernetes.io/container/memory/used_bytes" \\
          --filter 'resource.labels.pod_name = "my-pod"' --aligner mean
      gmetrics query "kubernetes.io/container/memory/used_bytes" \\
          --filter 'resource.labels.pod_name = starts_with("my-service") \\
                    AND metric.labels.memory_type = "non-evictable"' \\
          --aligner max --group-by resource.labels.pod_name
      gmetrics query "custom.googleapis.com/my/metric" --aligner rate --period 5m
    """
    period = period or auto_period(start)
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
        render_query(series, metric_type, period=period, start=start, end=end)


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


@cli.command()
@click.argument("metric_type")
@click.option(
    "--filter",
    "filt",
    default=None,
    help="Additional monitoring filter (same syntax as `query`)",
)
@click.option(
    "--start",
    default="5m",
    show_default=True,
    help="Probe window (short is fine; small label set usually stable)",
)
@click.pass_context
@_cli_validate
def labels(ctx, metric_type, filt, start):
    """Show resource and metric labels available for a metric type.

    Useful for finding which fields to pass to `top --show` or to
    `query --group-by`.

    \b
    Examples:
      gmetrics labels "kubernetes.io/container/memory/used_bytes"
      gmetrics labels "kubernetes.io/container/memory/used_bytes" \\
          --filter 'resource.labels.pod_name = starts_with("my-service")'
    """
    info = metric_labels(ctx.obj["project"], metric_type, filt=filt, start=start)
    if ctx.obj["json"]:
        click.echo(json.dumps(info, indent=2))
    else:
        render_labels(info)


if __name__ == "__main__":
    cli()
