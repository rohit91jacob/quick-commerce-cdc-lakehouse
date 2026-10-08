"""Download Maven artifacts into a directory, verifying each against Maven Central's SHA-1."""

import hashlib
import sys
import urllib.request
from pathlib import Path

REPO = "https://repo1.maven.org/maven2"


def fetch(coordinate: str, target: Path) -> None:
    group, artifact, version = coordinate.split(":")
    path = f"{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"
    with urllib.request.urlopen(f"{REPO}/{path}", timeout=300) as response:  # noqa: S310 - fixed https host
        data = response.read()
    with urllib.request.urlopen(f"{REPO}/{path}.sha1", timeout=60) as response:  # noqa: S310
        expected = response.read().decode().split()[0].strip()
    actual = hashlib.sha1(data).hexdigest()  # noqa: S324 - integrity check against Maven's published digest
    if actual != expected:
        raise SystemExit(f"checksum mismatch for {coordinate}: {actual} != {expected}")
    (target / f"{artifact}-{version}.jar").write_bytes(data)
    print(f"{coordinate}: {len(data):,} bytes, sha1 ok")


if __name__ == "__main__":
    out = Path(sys.argv[1])
    for coordinate in sys.argv[2:]:
        fetch(coordinate, out)
