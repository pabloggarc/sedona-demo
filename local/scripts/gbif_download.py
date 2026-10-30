# /// script
# requires-python = ">=3.12"
# dependencies = ["requests==2.34.2"]
# ///
"""
Download the GBIF occurrences of one region as Parquet, through the GBIF download API.

The GBIF snapshot on S3 is not partitioned by location, so getting a single region out of it
means scanning the whole thing. The download API does the filtering on GBIF's side instead. It
takes a while to prepare each download, so the key of the pending one is kept and the script
picks it up again if it is stopped. Every download gets a DOI, saved in download.json.
"""

import json
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import requests

API_URL = "https://api.gbif.org/v1/occurrence/download"
GBIF_USER = os.environ["GBIF_USER"]
GBIF_PWD = os.environ["GBIF_PWD"]
GBIF_EMAIL = os.environ["GBIF_EMAIL"]
GADM_GID = os.environ["GADM_GID"]

POLL_INTERVAL_S = 60


def request_download() -> str:
    """
    Ask GBIF for the occurrences in the region, leaving out records without coordinates and
    records with geospatial issues flagged by GBIF.

    Returns:
        str: Key of the new download.
    """
    predicate = {
        "type": "and",
        "predicates": [
            {"type": "equals", "key": "GADM_GID", "value": GADM_GID},
            {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
            {"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"},
        ],
    }
    body = {
        "creator": GBIF_USER,
        "notificationAddresses": [GBIF_EMAIL],
        "sendNotification": False,
        "format": "SIMPLE_PARQUET",
        "predicate": predicate,
    }
    response = requests.post(
        f"{API_URL}/request", json=body, auth=(GBIF_USER, GBIF_PWD), timeout=60
    )
    response.raise_for_status()
    return response.text.strip()


def wait_for(key: str) -> dict[str, Any]:
    """
    Poll a download until GBIF has prepared it.

    Args:
        key (str): Key of the download.

    Returns:
        dict[str, Any]: Metadata of the download, with its DOI and download link.

    Raises:
        RuntimeError: If the download failed on GBIF's side.
    """
    while True:
        response = requests.get(f"{API_URL}/{key}", timeout=60)
        response.raise_for_status()
        meta: dict[str, Any] = response.json()
        if meta["status"] == "SUCCEEDED":
            return meta
        if meta["status"] in ("FAILED", "KILLED", "CANCELLED"):
            raise RuntimeError(f"download {key} ended as {meta['status']}")
        print(f"download {key} is {meta['status'].lower()}", flush=True)
        time.sleep(POLL_INTERVAL_S)


def extract(url: str, out_dir: Path) -> None:
    """
    Download the zip with the occurrences and extract it into ``out_dir/occurrence.parquet``.

    The zip is extracted into a temporary folder that is only moved into place at the end, so
    a run that is stopped halfway never leaves an incomplete dataset behind.

    Args:
        url (str): Link to the zip.
        out_dir (Path): Directory to extract it into.
    """
    zip_path = out_dir / "download.zip"
    tmp_dir = out_dir / "tmp"

    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with zip_path.open("wb") as f:
            shutil.copyfileobj(response.raw, f)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(tmp_dir)

    # GBIF ships some empty part files that Parquet readers refuse to open
    for part in (tmp_dir / "occurrence.parquet").iterdir():
        if part.stat().st_size == 0:
            part.unlink()

    (tmp_dir / "occurrence.parquet").rename(out_dir / "occurrence.parquet")
    tmp_dir.rmdir()
    zip_path.unlink()


def main() -> None:
    """
    Request the download, or resume the pending one, wait for it and extract it.
    """
    out_dir = Path(sys.argv[1])
    key_file = out_dir / "download_key"

    if (out_dir / "occurrence.parquet").exists():
        print("occurrences already downloaded", flush=True)
        return

    if key_file.exists():
        key = key_file.read_text()
        print(f"resuming download {key}", flush=True)
    else:
        key = request_download()
        key_file.write_text(key)
        print(f"requested download {key}", flush=True)

    try:
        meta = wait_for(key)
    except RuntimeError:
        key_file.unlink()
        raise

    (out_dir / "download.json").write_text(json.dumps(meta, indent=2))
    print(f"{meta['totalRecords']:,} occurrences, doi {meta['doi']}", flush=True)

    extract(meta["downloadLink"], out_dir)
    key_file.unlink()


if __name__ == "__main__":
    main()
