"""
gh_explore.py — Phase 2, Step 1: explore one hour of GH Archive data.

Downloads one hourly file (if not already downloaded), reads it, and writes
a plain-text summary plus one sample event per event type. Builds nothing.

Usage:
    python gh_explore.py 2026-09-27-15      # a recent hour
    python gh_explore.py 2014-06-01-15      # an old hour, to compare formats

Hour format: YYYY-MM-DD-H  (hour is 0-23, no leading zero, UTC)

Outputs (created next to this script):
    data/<hour>.json.gz              the raw downloaded file
    notes/<hour>_summary.txt         counts, fields, payload shapes
    notes/<hour>_samples.json        one full example event per type
"""

import gzip
import json
import shutil
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

BASE_URL = "https://data.gharchive.org/{hour}.json.gz"
USER_AGENT = "Mozilla/5.0 (gh-archive-learning-project)"
HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
NOTES_DIR = HERE / "notes"


def download(hour: str) -> Path:
    """Download the hourly file once; reuse it on later runs."""
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"{hour}.json.gz"
    if path.exists():
        print(f"Using existing file: {path}")
        return path

    url = BASE_URL.format(hour=hour)
    print(f"Downloading {url} ...")

    # Some servers reject Python's default "Python-urllib" identity,
    # so identify as a normal client.
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=120) as response, open(path, "wb") as out:
            shutil.copyfileobj(response, out)
    except urllib.error.HTTPError as e:
        if path.exists():
            path.unlink()  # don't leave a half-written file behind
        if e.code in (403, 404):
            print(
                f"\nServer said {e.code} for {hour}. This usually means that hour's file\n"
                "doesn't exist (a gap in the archive, or it hasn't been published yet).\n"
                "Try opening the URL above in your browser, or pick a different hour."
            )
            sys.exit(1)
        raise
    except Exception:
        if path.exists():
            path.unlink()
        raise

    print(f"Saved {path} ({path.stat().st_size / 1_000_000:.1f} MB)")
    return path


def describe(value) -> str:
    """Short label for a value's type, e.g. 'str', 'dict', 'list[dict]'."""
    if isinstance(value, list):
        inner = type(value[0]).__name__ if value else "empty"
        return f"list[{inner}]"
    return type(value).__name__


def analyze(path: Path):
    type_counts = Counter()
    top_level_keys = Counter()            # which top-level fields appear, how often
    payload_keys = defaultdict(Counter)   # event type -> payload field -> count
    payload_kinds = defaultdict(dict)     # event type -> payload field -> value type
    samples = {}                          # event type -> first full event seen
    bad_lines = 0
    total = 0

    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue

            total += 1
            etype = event.get("type", "<no type>")
            type_counts[etype] += 1
            top_level_keys.update(event.keys())

            payload = event.get("payload") or {}
            if isinstance(payload, dict):
                payload_keys[etype].update(payload.keys())
                for k, v in payload.items():
                    payload_kinds[etype].setdefault(k, describe(v))

            samples.setdefault(etype, event)

    return {
        "total": total,
        "bad_lines": bad_lines,
        "type_counts": type_counts,
        "top_level_keys": top_level_keys,
        "payload_keys": payload_keys,
        "payload_kinds": payload_kinds,
        "samples": samples,
    }


def write_report(hour: str, r: dict):
    NOTES_DIR.mkdir(exist_ok=True)
    total = r["total"] or 1
    out = []

    out.append(f"GH Archive exploration — hour {hour}")
    out.append("=" * 60)
    out.append(f"Total events: {r['total']:,}")
    out.append(f"Unreadable lines: {r['bad_lines']:,}")
    out.append("")

    out.append("1) EVENT TYPES (most common first)")
    for etype, n in r["type_counts"].most_common():
        out.append(f"   {etype:<32} {n:>9,}  {n / total:6.1%}")
    out.append("")

    out.append("2) TOP-LEVEL FIELDS (share of events that have them)")
    out.append("   100% = on every event -> dimension candidate")
    for key, n in r["top_level_keys"].most_common():
        out.append(f"   {key:<32} {n / total:6.1%}")
    out.append("")

    out.append("3) PAYLOAD FIELDS BY EVENT TYPE")
    for etype, _ in r["type_counts"].most_common():
        n_type = r["type_counts"][etype]
        out.append(f"   [{etype}]  ({n_type:,} events)")
        for key, n in r["payload_keys"][etype].most_common():
            kind = r["payload_kinds"][etype].get(key, "?")
            out.append(f"      {key:<28} {kind:<14} {n / n_type:6.1%}")
        out.append("")

    summary_path = NOTES_DIR / f"{hour}_summary.txt"
    summary_path.write_text("\n".join(out), encoding="utf-8")

    samples_path = NOTES_DIR / f"{hour}_samples.json"
    samples_path.write_text(
        json.dumps(r["samples"], indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("\n".join(out[:4 + len(r["type_counts"]) + 2]))
    print(f"Full summary: {summary_path}")
    print(f"Sample events: {samples_path}")


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    hour = sys.argv[1]
    path = download(hour)
    results = analyze(path)
    write_report(hour, results)


if __name__ == "__main__":
    main()
