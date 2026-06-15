"""Local helper to populate the landing zone from your own machine.

WHY THIS EXISTS (the workaround)
--------------------------------
Databricks **Free Edition** runs notebooks on **serverless compute that has NO
outbound internet access**. A `requests.get(...)` from inside the notebook fails with
`Temporary failure in name resolution`, so the NYC TLC files cannot be downloaded
from within Databricks.

The fix is to fetch the files where there *is* internet (your laptop) and then place
them in the Unity Catalog Volume `ifood.bronze.landing`, which is the project's raw
"landing zone" in the lake. From there the Spark notebooks read them normally — no
table creation or catalog registration is needed; a Volume is UC-managed storage and
`spark.read.parquet("/Volumes/ifood/bronze/landing/...")` just works.

WHY NOT COMMIT THE PARQUET TO GIT
---------------------------------
The yellow files are ~50 MB each (~250 MB for 5 months). GitHub warns above 50 MB and
blocks above 100 MB per file, Databricks Git folders aren't meant for large binaries,
and raw data belongs in the lake (a Volume), not in the code repo. So we keep the repo
clean (`data/` is git-ignored) and push the files to the Volume instead.

USAGE (Windows / any OS)
------------------------
    # 1) download only -> data/landing/{yellow,green}/*.parquet
    python src/download_landing_local.py

    # 2) download + upload to the UC Volume in one go (needs the Databricks CLI)
    python src/download_landing_local.py --upload

    # custom local output dir
    python src/download_landing_local.py --out C:\\tmp\\landing

UPLOAD WITHOUT THIS SCRIPT
--------------------------
You can also upload manually after a plain download:
  * UI : Catalog Explorer -> ifood -> bronze -> Volumes -> landing -> Upload
  * CLI: databricks fs cp -r data/landing dbfs:/Volumes/ifood/bronze/landing --overwrite
The CLI needs a one-time `databricks configure` (workspace host + a Databricks PAT).

Scope mirrors src/00_config.py (yellow + green, Jan-May 2023).
"""
import argparse
import os
import subprocess
import sys
import urllib.request

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR = 2023
MONTHS = range(1, 6)  # Jan..May
SERVICES = {"yellow": "yellow_tripdata", "green": "green_tripdata"}

# Destination inside Databricks. Must match 00_config (catalog.bronze.landing).
VOLUME_URI = "dbfs:/Volumes/ifood/bronze/landing"


def download(out_dir: str) -> None:
    for service, prefix in SERVICES.items():
        service_dir = os.path.join(out_dir, service)
        os.makedirs(service_dir, exist_ok=True)
        for month in MONTHS:
            file_name = f"{prefix}_{YEAR}-{month:02d}.parquet"
            dest = os.path.join(service_dir, file_name)
            if os.path.exists(dest) and os.path.getsize(dest) > 0:
                print(f"skip (exists): {file_name}")
                continue
            print(f"downloading: {file_name} ...")
            urllib.request.urlretrieve(f"{BASE_URL}/{file_name}", dest)
    print(f"Download done -> {os.path.abspath(out_dir)}")


def upload(out_dir: str) -> None:
    """Push the local landing folder to the UC Volume via the Databricks CLI.

    Requires the Databricks CLI installed and authenticated (`databricks configure`
    with the workspace host and a Databricks PAT). The recursive copy preserves the
    yellow/ and green/ subfolders.
    """
    cmd = ["databricks", "fs", "cp", "-r", out_dir, VOLUME_URI, "--overwrite"]
    print("uploading to Volume:", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        sys.exit("Databricks CLI not found. Install it, run `databricks configure`, "
                 "then re-run with --upload (or upload via the Catalog Explorer UI).")
    print(f"Upload done -> {VOLUME_URI}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC files locally and "
                                                 "optionally upload them to the UC Volume.")
    parser.add_argument("--out", default=os.path.join("data", "landing"),
                        help="local output directory (default: data/landing)")
    parser.add_argument("--upload", action="store_true",
                        help="also upload to the UC Volume via the Databricks CLI")
    args = parser.parse_args()

    download(args.out)
    if args.upload:
        upload(args.out)


if __name__ == "__main__":
    main()
