"""Downloads the Uber pickups dataset from Kaggle and copies the six 2014
monthly files into data/, the pipeline's landing folder.

Run once on your own machine, from the repository root:
    python3 -m venv .venv && source .venv/bin/activate
    pip install kagglehub
    python tools/fetch_data.py

Exit code 0 means all six files are present with the expected row total.
"""

import hashlib
import shutil
import sys
from pathlib import Path

import kagglehub

# For reproducibility, pin the version once you know it, for example
# "fivethirtyeight/uber-pickups-in-new-york-city/versions/2".
# The download path printed below shows which version was fetched.
HANDLE = "fivethirtyeight/uber-pickups-in-new-york-city"

MONTHS = ["apr", "may", "jun", "jul", "aug", "sep"]
EXPECTED_TOTAL = 4_534_327
TARGET = Path(__file__).resolve().parent.parent / "data"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_rows(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f) - 1  # minus the header line


def main() -> int:
    source = Path(kagglehub.dataset_download(HANDLE))
    print(f"Downloaded to: {source}\n")
    TARGET.mkdir(exist_ok=True)

    total = 0
    for month in MONTHS:
        name = f"uber-raw-data-{month}14.csv"
        matches = list(source.rglob(name))
        if not matches:
            print(f"ERROR: {name} not found in {source}")
            return 1
        dest = TARGET / name
        shutil.copy2(matches[0], dest)
        rows = count_rows(dest)
        total += rows
        print(f"{name}  {rows:>10,} rows  sha256 {sha256(dest)}")

    print(f"\nTotal: {total:,} rows (expected {EXPECTED_TOTAL:,})")
    return 0 if total == EXPECTED_TOTAL else 1


if __name__ == "__main__":
    sys.exit(main())
