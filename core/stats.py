#!/usr/bin/env python3
"""stats.py -- every statistic monbench reports is computed here.

Backends supply raw inputs only: core/ingest.py times the write phases and
records counts and progress checkpoints, and core/egress.py times each query
run; the adapters only call their backend. Nothing in a backend computes a rate, a
percentile or any other derived number, so every backend is summarized by
exactly the same code and the figures are comparable.

One summary shape is used for every measured series, ingest and egress
alike (see summarize()):

    {"n", "min", "p50", "p90", "p95", "p99", "max", "mean", "stddev"}

A percentile is reported only when the series has enough samples for it to
be more than the maximum (see min_samples()); otherwise it is null.

Raw ingest output (one JSON file per server host, written by core/ingest.py;
its docstring lists every field): counts, phase timers, per-record generation
and write() histograms, and checkpoints. A checkpoint row is
[wall_s, generate_s, write_s, samples]: seconds since the loop began, and the
generation and store time so far, at the end of every batch (after its commit
returned), beginning with an all-zero row. The three time columns give three
views of the same batches:

    store     write_s     what the backend needed (capacity)
    wall      wall_s      what was delivered, generation and throttling included
    generate  generate_s  what the harness spent producing the records

A batch is the unit of work: `batch` node records committed or sent
together. Each interval between two checkpoints is therefore one batch (a
"window"), and its duration is that batch's latency. A final batch smaller
than `batch` is left out of the distributions (it is still part of the totals).
Nothing here assumes a data set size, a batch size or a host count: the number
of windows is records / batch, and a percentile is reported only when there
are enough of them (see min_samples()).
"""
import math

PERCENTILES = (50, 90, 95, 99)


def min_samples(p):
    """Fewest samples for which percentile *p* is more than the maximum.

    The tail above the p-th percentile must be able to hold at least one
    sample: n >= 1 / (1 - p/100). That is 2 for p50, 10 for p90, 20 for
    p95 and 100 for p99.
    """
    return math.ceil(round(100.0 / (100.0 - p), 9))


def percentile(ordered, p):
    """Percentile *p* of an ascending list, by linear interpolation
    between the closest ranks (the same method as numpy's default)."""
    n = len(ordered)
    if n == 0:
        return None
    if n == 1:
        return ordered[0]
    k = (n - 1) * p / 100.0
    lo = math.floor(k)
    hi = min(lo + 1, n - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def _r(x):
    return None if x is None else round(x, 6)


def summarize(samples):
    """Summarize a series of numbers; see the module docstring.

    None and non-finite values are ignored. An empty series gives n = 0
    and null everywhere else.
    """
    values = sorted(float(x) for x in samples
                    if x is not None and math.isfinite(x))
    n = len(values)
    out = {"n": n, "min": None, "p50": None, "p90": None, "p95": None,
           "p99": None, "max": None, "mean": None, "stddev": None}
    if n == 0:
        return out
    mean = math.fsum(values) / n
    out["min"], out["max"], out["mean"] = _r(values[0]), _r(values[-1]), _r(mean)
    if n > 1:
        out["stddev"] = _r(math.sqrt(math.fsum((v - mean) ** 2 for v in values)
                                     / (n - 1)))
    for p in PERCENTILES:
        if n >= min_samples(p):
            out[f"p{p}"] = _r(percentile(values, p))
    return out


def rate(count, seconds):
    """count / seconds, or None when there is no elapsed time to divide by."""
    if count is None or not seconds or seconds <= 0:
        return None
    return count / seconds


# ----------------------------------------------------------------- ingest


WALL, GENERATE, STORE = 0, 1, 2          # checkpoint time columns
SAMPLES = 3


def windows(checkpoints, batch=0, col=STORE):
    """Intervals between consecutive checkpoints as (seconds, samples), the
    seconds taken from time column *col* (WALL, GENERATE or STORE).

    Intervals with no elapsed time or no samples are dropped. When *batch*
    (the run's batch size) is given, so is any interval with fewer
    samples than that: a final partial batch would only add noise to a
    distribution of equally sized windows.
    """
    out = []
    prev = None
    for row in checkpoints:
        if prev is not None:
            dt, dn = row[col] - prev[col], row[SAMPLES] - prev[SAMPLES]
            if dt > 0 and dn > 0 and (not batch or dn >= batch):
                out.append((dt, dn))
        prev = row
    return out


def hist_summary(hist, maximum=None):
    """Summary of a log2 histogram of durations (see core/ingest.py).

    Bucket b holds durations of [2^(b-1), 2^b) ns; a percentile is reported
    as the geometric middle of its bucket, so it is accurate to within a
    factor of about 1.4. *maximum* is the exact longest duration. Percentiles
    follow the same minimum-sample rule as summarize().
    """
    n = sum(hist or [])
    out = {"n": n, "p50": None, "p90": None, "p95": None, "p99": None,
           "max": maximum, "resolution": "log2 buckets"}
    if n == 0:
        return out
    for p in (50, 90, 95, 99):
        if n < min_samples(p):
            continue
        need, seen = n * p / 100.0, 0
        for b, count in enumerate(hist):
            seen += count
            if seen >= need:
                out[f"p{p}"] = 0.0 if b == 0 else round(2.0 ** (b - 0.5) * 1e-9, 12)
                break
    return out


def _series(raw, col, batch):
    wins = windows(raw.get("checkpoints") or [], batch, col)
    return summarize(dt for dt, _ in wins), summarize(dn / dt for dt, dn in wins)


def ingest_host(raw):
    """Statistics for one ingest run, from its raw JSON (see module docstring)."""
    records = raw.get("records")
    write_s = raw.get("write_s")
    sync_s = raw.get("sync_s") or 0.0
    loop_s = raw.get("loop_s")
    generate_s = raw.get("generate_s")
    throttle_s = raw.get("throttle_s") or 0.0
    batch = int(raw.get("batch") or 0)
    durable_s = write_s + sync_s if write_s is not None else None
    other_s = None
    if None not in (loop_s, generate_s, write_s):
        other_s = loop_s - generate_s - write_s - throttle_s
    latency, throughput = _series(raw, STORE, batch)
    wall_latency, wall_throughput = _series(raw, WALL, batch)
    generate_latency, generate_throughput = _series(raw, GENERATE, batch)
    return {
        "records": records,
        "list_entries": raw.get("list_entries", 0),
        "errors": raw.get("errors", 0),
        "batch": raw.get("batch"),
        "rate": raw.get("rate"),
        "open_s": raw.get("open_s"),
        "loop_s": loop_s,
        "generate_s": generate_s,
        "write_s": write_s,
        "write_call_s": raw.get("write_call_s"),
        "commit_s": raw.get("commit_s"),
        "throttle_s": throttle_s,
        "other_s": _r(other_s),
        "sync_s": sync_s,
        "close_s": raw.get("close_s"),
        "timer_cost_ns": raw.get("timer_cost_ns"),
        "records_per_s": _r(rate(records, write_s)),
        "durable_records_per_s": _r(rate(records, durable_s)),
        "delivered_records_per_s": _r(rate(records, loop_s)),
        "generate_records_per_s": _r(rate(records, generate_s)),
        "window_records": batch or records,
        "latency_s": latency,
        "throughput_per_s": throughput,
        "wall_latency_s": wall_latency,
        "wall_throughput_per_s": wall_throughput,
        "generate_latency_s": generate_latency,
        "record_latency_s": {
            "generate": hist_summary(raw.get("generate_hist"), raw.get("generate_max_s")),
            "write": hist_summary(raw.get("write_hist"), raw.get("write_call_max_s")),
        },
    }


def ingest_summary(raws):
    """Statistics across all ingest runs of one write phase.

    Two aggregate views, both over all hosts:
      store   the backend's capacity: total records over the slowest host's
              store time (write_s, or write_s + sync_s for durable). A host's
              rate does not depend on when it started, so no clock is shared.
      wall    what was delivered: total records over the interval from the
              earliest loop start to the latest loop end, using the hosts'
              wall clocks (so it assumes they are synchronized). If a host
              did not report its start and end it falls back to the slowest
              host's own loop time.

    The pooled latency/throughput series hold every host's windows
    together: they describe the rate each server achieved over a window,
    not a sum across servers.
    """
    hosts = [ingest_host(r) for r in raws]
    total = sum(h["records"] for h in hosts)
    starts = [r.get("start_epoch_s") for r in raws]
    ends = [r.get("end_epoch_s") for r in raws]
    sync = [h["sync_s"] for h in hosts]
    store_s = max(h["write_s"] for h in hosts)
    durable_store_s = max(h["write_s"] + y for h, y in zip(hosts, sync))
    if all(isinstance(x, (int, float)) for x in starts + ends):
        basis = "wall-clock"
        t0 = min(starts)
        wall_s = max(ends) - t0
        durable_wall_s = max(e + y for e, y in zip(ends, sync)) - t0
    else:
        basis = "slowest-host"
        wall_s = max(h["loop_s"] or h["write_s"] for h in hosts)
        durable_wall_s = max((h["loop_s"] or h["write_s"]) + y
                             for h, y in zip(hosts, sync))
    host_rates = [h["records_per_s"] for h in hosts if h["records_per_s"]]

    def pooled(col):
        wins = [w for r in raws
                for w in windows(r.get("checkpoints") or [], int(r.get("batch") or 0), col)]
        return (summarize(dt for dt, _ in wins),
                summarize(dn / dt for dt, dn in wins))

    latency, throughput = pooled(STORE)
    wall_latency, wall_throughput = pooled(WALL)
    generate_latency, _ = pooled(GENERATE)
    return {
        "hosts": len(hosts),
        "total_records": total,
        "total_list_entries": sum(h["list_entries"] for h in hosts),
        "errors": sum(h["errors"] for h in hosts),
        "wall_basis": basis,
        "store_s": _r(store_s),
        "durable_store_s": _r(durable_store_s),
        "store_records_per_s": _r(rate(total, store_s)),
        "durable_store_records_per_s": _r(rate(total, durable_store_s)),
        "wall_s": _r(wall_s),
        "durable_wall_s": _r(durable_wall_s),
        "wall_records_per_s": _r(rate(total, wall_s)),
        "durable_wall_records_per_s": _r(rate(total, durable_wall_s)),
        "host_records_per_s_min": _r(min(host_rates)) if host_rates else None,
        "host_records_per_s_max": _r(max(host_rates)) if host_rates else None,
        "host_imbalance": (_r(max(host_rates) / min(host_rates))
                           if host_rates else None),
        "latency_s": latency,
        "throughput_per_s": throughput,
        "wall_latency_s": wall_latency,
        "wall_throughput_per_s": wall_throughput,
        "generate_latency_s": generate_latency,
    }, hosts


# ----------------------------------------------------------------- egress


def egress(times, rows, first_batch_times=()):
    """Statistics for one query's timed runs.

    *times* are the per-run durations in seconds, *rows* the row count every
    run returned, *first_batch_times* the time until the first batch of each
    run had been fetched.
    """
    return {
        "latency_s": summarize(times),
        "throughput_per_s": summarize(rate(rows, t) for t in times),
        "first_batch_s": summarize(first_batch_times),
    }
