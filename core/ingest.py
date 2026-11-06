#!/usr/bin/env python3
"""ingest.py -- write one partition of the benchmark data set through an
ingest adapter, time it, and save the raw result.

Runs on each server host (through 'bin/node.sh write'), and on the query node
for 'write-summary', which prints the per-host results of a finished write.

    ingest.py --backend NAME [--adapter NAME] --writer W --output FILE
              --node-index I [dataset options] [adapter options]
    ingest.py write-summary --writer W

The engine is backend-neutral and the counterpart of core/egress.py. It
loads the adapter from backends/<NAME>/writer/<NAME>_write.py (see
core/ingest_adapter.py), generates every record of the partition, hands them
to the adapter in batches, and measures:

    open_s        adapter.open()
    generate_s    generating the records (Dataset.expected*), summed over records
    write_s       the store: write_call_s + commit_s
      write_call_s  the adapter.write() calls, summed over records
      commit_s      the adapter.commit() calls, summed over batches
    sync_s        adapter.sync()
    close_s       adapter.close()
    loop_s        wall time of the whole loop, first record -> last commit
    throttle_s    time slept to hold --rate

Generation and the store are timed record by record, so generation is not
part of write_s, and nothing is held in memory beyond the current record.
Whatever the loop spends outside those intervals (bookkeeping, garbage
collection, preemption) is loop_s - generate_s - write_s - throttle_s.

Only the engine reads the clock and writes FILE, so every backend is
measured and recorded the same way. The raw file holds counts, timers and
checkpoints; core/stats.py derives every rate and percentile from it:

    writer, backend, adapter, durability, node_index
    records         node records written successfully
    list_entries    list-attribute entries written
    errors          samples whose write() raised
    batch, rate     the settings, echoed
    start_epoch_s, end_epoch_s
                    wall-clock time at the start and the end of the loop
    open_s, loop_s, generate_s, write_s, write_call_s, commit_s,
    throttle_s, sync_s, close_s      (see above)
    timer_cost_ns   cost of one clock read here; every timed interval
                    includes about one such read
    generate_max_s, write_call_max_s
                    the longest single generation / write() call
    generate_hist, write_hist
                    histograms of the per-record generation / write() call
                    durations: bucket b counts durations of [2^(b-1), 2^b)
                    nanoseconds (bucket 0 is under 1 ns), trailing empty
                    buckets omitted
    checkpoint_fields, checkpoints
                    ["wall_s", "generate_s", "write_s", "samples"], and one
                    row per batch (after its commit returned) plus a last one
                    when the write finished: seconds since the loop began
                    (wall), the generation and store time so far, and the
                    samples done; beginning with an all-zero row
    plus whatever adapter.info() returns (target, storage, ...)

'--adapter null' stores nothing: it measures the cost of the harness itself
(generation and the loop), the floor under every other backend's figures.
"""
import argparse
import glob
import importlib.util
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import stats                                              # noqa: E402
from dataset import Dataset, read_meta                    # noqa: E402
from ingest_adapter import BackendError, IngestAdapter    # noqa: E402

MAX_ERRORS_SHOWN = 10


class NullIngest(IngestAdapter):
    """Stores nothing."""
    name = "null"
    durability = "none"

    def open(self):
        pass

    def write(self, comp, seq, values, lists):
        pass

    def commit(self):
        pass


class Context:
    def __init__(self, args, dataset):
        self.args = args
        self.dataset = dataset
        self.env = os.environ


# ----------------------------------------------------------------- loading


def load_adapter_class(backend, name):
    """The adapter class NAME of BACKEND's ingest plugin ('null' is built in)."""
    if name == "null":
        return NullIngest
    pattern = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                           "backends", backend, "writer", "*_write.py")
    paths = glob.glob(pattern)
    if len(paths) != 1:
        sys.exit(f"ingest.py: backend '{backend}' needs exactly one "
                 f"writer/*_write.py ingest adapter; found {len(paths)}")
    spec = importlib.util.spec_from_file_location(f"ingest_{backend}", paths[0])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    name = name or module.DEFAULT_ADAPTER
    if name not in module.ADAPTERS:
        sys.exit(f"ingest.py: backend '{backend}' has no ingest adapter "
                 f"'{name}'; available: {', '.join(sorted(module.ADAPTERS))}")
    return module.ADAPTERS[name]


def build_parser(adapter_class=None):
    p = argparse.ArgumentParser(description=__doc__, allow_abbrev=False,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--backend", required=True, help="backend plugin name")
    p.add_argument("--adapter", default="", help="ingest adapter (default: the plugin's)")
    p.add_argument("--writer", required=True, help="writer name recorded in the result")
    p.add_argument("--output", required=True, help="raw result file to write")
    p.add_argument("--node-index", type=int, required=True)
    p.add_argument("--components", type=int, default=16)
    p.add_argument("--samples", type=int, default=5000)
    p.add_argument("--t0", type=int, default=1700000000)
    p.add_argument("--interval", type=int, default=1)
    p.add_argument("--job-len", type=int, default=500)
    p.add_argument("--batch", type=int, default=500,
                   help="node records per commit, 0 = one commit at the end")
    p.add_argument("--rate", type=float, default=0.0,
                   help="records per second, 0 = unthrottled")
    if adapter_class is not None:
        adapter_class.add_arguments(p)
    return p


def parse_args(argv):
    first = build_parser()
    first.add_help = False
    known, _ = first.parse_known_args(argv)
    cls = load_adapter_class(known.backend, known.adapter)
    return build_parser(cls).parse_args(argv), cls


# ----------------------------------------------------------------- engine


CHECKPOINT_FIELDS = ["wall_s", "generate_s", "write_s", "samples"]
HIST_BUCKETS = 64
SLOW_CLOCK_NS = 1000


def timer_cost_ns(clock, reads=20000):
    t = clock()
    for _ in range(reads):
        clock()
    return (clock() - t) / reads * 1e9


def trim(hist):
    while hist and hist[-1] == 0:
        hist.pop()
    return hist


def run(a, cls):
    if "DATASET" not in os.environ:
        sys.exit("ingest.py: DATASET is not set; it names the data set definition")
    ds = Dataset(Dataset.load_definition(os.environ["DATASET"]),
                 partitions=1, component_ids=a.components, samples=a.samples,
                 t0=a.t0, interval=a.interval, job_len=a.job_len)
    adapter = cls(Context(a, ds))
    clock = time.perf_counter

    cost_ns = timer_cost_ns(clock)
    if cost_ns > SLOW_CLOCK_NS:
        print(f"ingest.py: warning: a clock read costs {cost_ns:.0f} ns here; "
              f"the per-record timings include that error", file=sys.stderr)

    t = clock()
    adapter.open()
    open_s = clock() - t

    has_lists = bool(ds.list_attrs)
    expected, expected_lists = ds.expected, ds.expected_lists
    write, commit = adapter.write, adapter.commit
    batch, rate = a.batch, a.rate
    base = (a.node_index + 1) * 1000
    records = errors = list_entries = in_batch = done = 0
    generate_s = write_call_s = commit_s = throttle_s = 0.0
    generate_max = write_max = 0.0
    generate_hist = [0] * HIST_BUCKETS
    write_hist = [0] * HIST_BUCKETS
    last = HIST_BUCKETS - 1
    checkpoints = [[0.0, 0.0, 0.0, 0]]

    start_epoch = time.time()
    t_loop = clock()
    for seq in range(a.samples):
        for c in range(a.components):
            comp = base + c
            t0 = clock()
            values = expected(comp, seq)
            lists = expected_lists(comp, seq) if has_lists else ()
            t1 = clock()
            try:
                write(comp, seq, values, lists)
                records += 1
                list_entries += len(lists)
            except Exception as e:
                errors += 1
                if errors <= MAX_ERRORS_SHOWN:
                    print(f"ingest.py: record {done} ({comp}, {seq}): {e}",
                          file=sys.stderr)
            t2 = clock()
            g, w = t1 - t0, t2 - t1
            generate_s += g
            write_call_s += w
            if g > generate_max:
                generate_max = g
            if w > write_max:
                write_max = w
            generate_hist[min(int(g * 1e9).bit_length(), last)] += 1
            write_hist[min(int(w * 1e9).bit_length(), last)] += 1
            done += 1
            in_batch += 1
            if batch and in_batch >= batch:
                t3 = clock()
                commit()
                t4 = clock()
                commit_s += t4 - t3
                checkpoints.append([round(t4 - t_loop, 6), round(generate_s, 6),
                                    round(write_call_s + commit_s, 6), done])
                in_batch = 0
            if rate > 0:
                wait = t_loop + done / rate - clock()
                if wait > 0:
                    t5 = clock()
                    time.sleep(wait)
                    throttle_s += clock() - t5
    t3 = clock()
    commit()
    t4 = clock()
    commit_s += t4 - t3
    loop_s = t4 - t_loop
    end_epoch = time.time()
    write_s = write_call_s + commit_s
    if checkpoints[-1][3] != done:
        checkpoints.append([round(loop_s, 6), round(generate_s, 6),
                            round(write_s, 6), done])

    t = clock()
    adapter.sync()
    sync_s = clock() - t

    t = clock()
    adapter.close()
    close_s = clock() - t

    raw = {
        "writer": a.writer, "backend": a.backend, "adapter": cls.name,
        "durability": cls.durability, "node_index": a.node_index,
        "records": records, "list_entries": list_entries, "errors": errors,
        "batch": batch, "rate": rate,
        "start_epoch_s": round(start_epoch, 6), "end_epoch_s": round(end_epoch, 6),
        "open_s": round(open_s, 6), "loop_s": round(loop_s, 6),
        "generate_s": round(generate_s, 6), "write_s": round(write_s, 6),
        "write_call_s": round(write_call_s, 6), "commit_s": round(commit_s, 6),
        "throttle_s": round(throttle_s, 6),
        "sync_s": round(sync_s, 6), "close_s": round(close_s, 6),
        "timer_cost_ns": round(cost_ns, 1),
        "generate_max_s": round(generate_max, 9), "write_call_max_s": round(write_max, 9),
        "generate_hist": trim(generate_hist), "write_hist": trim(write_hist),
        "checkpoint_fields": CHECKPOINT_FIELDS, "checkpoints": checkpoints,
    }
    raw.update(adapter.info())
    return raw


def save(raw, path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(raw, f)
        f.write("\n")
    os.replace(tmp, path)


def engine_main(argv):
    a, cls = parse_args(argv)
    try:
        raw = run(a, cls)
    except BackendError as e:
        print(f"ingest.py: backend '{a.backend}': {e}", file=sys.stderr)
        return 2
    save(raw, a.output)
    print(f"ingest.py: {raw['records']} records, generate {raw['generate_s']:.2f}s, "
          f"write {raw['write_s']:.2f}s, sync {raw['sync_s']:.2f}s, "
          f"{raw['errors']} errors -> {a.output}")
    return 1 if raw["errors"] else 0


# ----------------------------------------------------------------- results
# These run on the query node, after the write phase.


def _num(value):
    """A rate for a table cell; '-' when not available."""
    return "-" if value is None else f"{value:,.0f}"


def load_results(output_dir, writer, backend):
    """Read the raw write results of WRITER and compute their statistics.

    Each server host left one raw file (see the module docstring); every
    derived number -- rates, window percentiles, wall time -- is computed
    here, by core/stats.py, the same way for every backend.
    """
    pattern = os.path.join(output_dir, "results", f"ingest-{writer}-*-raw.json")
    found = []                      # (host, raw dict) or (host, error text)
    for f in sorted(glob.glob(pattern)):
        host = os.path.basename(f).replace(f"ingest-{writer}-", "").replace("-raw.json", "")
        try:
            with open(f) as fh:
                raw = json.load(fh)
        except Exception:
            found.append((host, f"could not read {f}"))
            continue
        if not all(isinstance(raw.get(k), (int, float)) for k in ("records", "write_s", "loop_s")):
            found.append((host, f"{f} has no records/write_s/loop_s"))
            continue
        found.append((host, raw))
    raws = [r for _, r in found if isinstance(r, dict)]
    summary, per_host = stats.ingest_summary(raws) if raws else ({}, [])
    per_host = iter(per_host)
    hosts = []
    for host, raw in found:
        if isinstance(raw, str):
            hosts.append({"host": host, "error": raw})
            continue
        hosts.append({"host": host, "backend": backend,
                      "writer": raw.get("writer", "-"),
                      "storage": raw.get("backend", "-"),
                      "durability": raw.get("durability", ""),
                      **next(per_host)})
    return {"hosts": hosts, "summary": summary}


def _secs(value):
    return "-" if value is None else f"{value:.2f}"


def print_summary(ingest):
    """Print the ingest results as two tables: throughput, and where the time went."""
    hosts = ingest.get("hosts", [])
    if not hosts:
        print("  (no ingest result files found)")
        return
    ok = [h for h in hosts if "error" not in h]
    hw = max(max(len(h.get("host", "")) for h in hosts), len("all hosts"))
    print(f"  {'host':{hw}}  {'writer':>6}  {'records':>10}  {'write_s':>8}  {'sync_s':>7}"
          f"  {'records/s':>10}  {'durable/s':>10}  {'win p50/s':>10}  {'win p95/s':>10}")
    for h in hosts:
        host = h.get("host", "?")
        if "error" in h:
            print(f"  {host:{hw}}  {h['error']}")
            continue
        thr = h["throughput_per_s"]
        print(f"  {host:{hw}}  {h.get('writer','-'):>6}  {h['records']:>10d}"
              f"  {h['write_s']:>8.2f}  {h['sync_s']:>7.2f}"
              f"  {_num(h['records_per_s']):>10}  {_num(h['durable_records_per_s']):>10}"
              f"  {_num(thr['p50']):>10}  {_num(thr['p95']):>10}")
    sm = ingest.get("summary", {})
    if sm:
        thr = sm["throughput_per_s"]
        print(f"  {'all hosts':{hw}}  {'':>6}  {sm['total_records']:>10d}"
              f"  {sm['store_s']:>8.2f}  {sm['durable_store_s'] - sm['store_s']:>7.2f}"
              f"  {_num(sm['store_records_per_s']):>10}"
              f"  {_num(sm['durable_store_records_per_s']):>10}"
              f"  {_num(thr['p50']):>10}  {_num(thr['p95']):>10}")
    print("\n  where the time went (seconds):")
    print(f"  {'host':{hw}}  {'loop_s':>8}  {'generate_s':>10}  {'write_call':>10}"
          f"  {'commit_s':>8}  {'throttle':>8}  {'other':>7}  {'delivered/s':>11}  {'generate/s':>10}")
    for h in ok:
        print(f"  {h['host']:{hw}}  {_secs(h['loop_s']):>8}  {_secs(h['generate_s']):>10}"
              f"  {_secs(h['write_call_s']):>10}  {_secs(h['commit_s']):>8}"
              f"  {_secs(h['throttle_s']):>8}  {_secs(h['other_s']):>7}"
              f"  {_num(h['delivered_records_per_s']):>11}  {_num(h['generate_records_per_s']):>10}")
    if sm:
        print(f"  {'all hosts':{hw}}  {_secs(sm['wall_s']):>8}  {'':>10}  {'':>10}  {'':>8}"
              f"  {'':>8}  {'':>7}  {_num(sm['wall_records_per_s']):>11}")
    every = next((h["window_records"] for h in ok), None)
    if sm:
        thr = sm["throughput_per_s"]
        imbalance = sm["host_imbalance"]
        imbalance = "-" if imbalance is None else f"{imbalance:.2f}x"
        print(f"  windows: {thr['n']} of {every} records each, pooled over the hosts; "
              f"a percentile shows '-' below {stats.min_samples(95)} windows (p95) "
              f"or {stats.min_samples(99)} (p99)")
        print(f"  records/s and win/s are store rates (generation excluded); "
              f"delivered/s is records over loop_s; all hosts: store_s is the slowest "
              f"host, wall_s is the earliest start to the latest end ({sm['wall_basis']} "
              f"basis); fastest/slowest host {imbalance}")
    slow = [h["host"] for h in ok if (h.get("timer_cost_ns") or 0) > 1000]
    if slow:
        print(f"  warning: slow clock on {', '.join(slow)}: per-record timings include "
              f"the cost of the clock reads (timer_cost_ns)")
    durabilities = sorted({h["durability"] for h in ok if h.get("durability")})
    if durabilities:
        print(f"  durable/s counts sync_s; sync guarantees: {', '.join(durabilities)}")


def summary_main(argv):
    p = argparse.ArgumentParser(prog="ingest.py write-summary")
    p.add_argument("--writer", required=True)
    a = p.parse_args(argv)
    output_dir = os.environ["OUTPUT_DIR"]
    backend = os.environ.get("WRITE_BACKEND") or os.environ.get("BACKEND", "-")
    meta = read_meta(a.writer)
    ds = Dataset.from_meta(meta) if meta else Dataset.from_env()
    source = (f"written by {meta['backend']} ({meta['writer']} writer) at {meta['written_at']}"
              if meta else "no metadata file; using the current settings")
    print(f"Data set '{ds.name}': {ds.size()} records in {ds.partitions} partitions; {source}")
    print("\nIngest performance:")
    print_summary(load_results(output_dir, a.writer, backend))
    return 0


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "write-summary":
        return summary_main(sys.argv[2:])
    return engine_main(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
