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
see its docstring):

    records         node records written
    list_entries    list-attribute entries written (0 without list attributes)
    errors          samples that failed to write
    batch, rate     the run's own settings, echoed
    start_epoch_s   wall-clock time at which the write phase began
    open_s          one-time setup: connect, open the container, schemas
    write_s         first record -> last commit returned (generation included)
    sync_s          making the data durable and visible (0 if no such step)
    close_s         teardown; reported, not part of any rate
    durability      what sync_s guarantees for this backend
    checkpoints     [[seconds since write start, samples done so far], ...]
                    beginning with [0, 0], one entry at the end of every batch
                    (after its commit returned) and a last one when the write
                    finished

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


def windows(checkpoints, batch=0):
    """Intervals between consecutive checkpoints as (seconds, records).

    Intervals with no elapsed time or no records are dropped. When *batch*
    (the run's batch size) is given, so is any interval with fewer
    records than that: a final partial batch would only add noise to a
    distribution of equally sized windows.
    """
    out = []
    prev = None
    for t, n in checkpoints:
        if prev is not None:
            dt, dn = t - prev[0], n - prev[1]
            if dt > 0 and dn > 0 and (not batch or dn >= batch):
                out.append((dt, dn))
        prev = (t, n)
    return out


def ingest_host(raw):
    """Statistics for one ingest run, from its raw JSON (see module docstring)."""
    records = raw.get("records")
    write_s = raw.get("write_s")
    sync_s = raw.get("sync_s") or 0.0
    batch = int(raw.get("batch") or 0)
    wins = windows(raw.get("checkpoints") or [], batch)
    durable_s = write_s + sync_s if write_s is not None else None
    return {
        "records": records,
        "list_entries": raw.get("list_entries", 0),
        "errors": raw.get("errors", 0),
        "batch": raw.get("batch"),
        "rate": raw.get("rate"),
        "open_s": raw.get("open_s"),
        "write_s": write_s,
        "sync_s": sync_s,
        "close_s": raw.get("close_s"),
        "records_per_s": _r(rate(records, write_s)),
        "durable_records_per_s": _r(rate(records, durable_s)),
        "window_records": batch or records,
        "latency_s": summarize(dt for dt, _ in wins),
        "throughput_per_s": summarize(dn / dt for dt, dn in wins),
    }


def ingest_summary(raws):
    """Statistics across all ingest runs of one write phase.

    Wall time is measured from the earliest write start to the latest write
    end using the hosts' wall clocks (so it assumes they are synchronized);
    if any host did not report a start time it falls back to the slowest
    host's own elapsed time.

    The pooled latency/throughput series hold every host's windows
    together: they describe the rate each server achieved over a window,
    not a sum across servers.
    """
    hosts = [ingest_host(r) for r in raws]
    total = sum(h["records"] for h in hosts)
    starts = [r.get("start_epoch_s") for r in raws]
    sync = [h["sync_s"] for h in hosts]
    if all(isinstance(s, (int, float)) for s in starts):
        basis = "wall-clock"
        t0 = min(starts)
        write_wall = max(s + h["write_s"] for s, h in zip(starts, hosts)) - t0
        durable_wall = max(s + h["write_s"] + y
                           for s, h, y in zip(starts, hosts, sync)) - t0
    else:
        basis = "slowest-host"
        write_wall = max(h["write_s"] for h in hosts)
        durable_wall = max(h["write_s"] + y for h, y in zip(hosts, sync))
    host_rates = [h["records_per_s"] for h in hosts if h["records_per_s"]]
    wins = [w for r in raws
            for w in windows(r.get("checkpoints") or [],
                             int(r.get("batch") or 0))]
    return {
        "hosts": len(hosts),
        "total_records": total,
        "total_list_entries": sum(h["list_entries"] for h in hosts),
        "errors": sum(h["errors"] for h in hosts),
        "wall_basis": basis,
        "write_wall_s": _r(write_wall),
        "durable_wall_s": _r(durable_wall),
        "records_per_s": _r(rate(total, write_wall)),
        "durable_records_per_s": _r(rate(total, durable_wall)),
        "host_records_per_s_min": _r(min(host_rates)) if host_rates else None,
        "host_records_per_s_max": _r(max(host_rates)) if host_rates else None,
        "host_imbalance": (_r(max(host_rates) / min(host_rates))
                           if host_rates else None),
        "latency_s": summarize(dt for dt, _ in wins),
        "throughput_per_s": summarize(dn / dt for dt, dn in wins),
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
