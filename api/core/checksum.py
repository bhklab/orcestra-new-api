import json
import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone

def calculate_checksums(path: Path, chunk_size: int = 1024 * 1024) -> dict:
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()

    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            sha256.update(chunk)
            md5.update(chunk)

    return {
        "sha256": sha256.hexdigest(),
        "md5": md5.hexdigest(),
        "size_bytes": path.stat().st_size,
    }