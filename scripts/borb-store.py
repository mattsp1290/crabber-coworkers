#!/usr/bin/env python3
"""Create and inspect complete SQLite snapshots without displaying stored content."""

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time


class StoreError(Exception):
    pass


@contextlib.contextmanager
def open_store(path, *, offline=False):
    path = path.resolve()
    before = None
    if offline:
        # immutable bypasses WAL and locking. It is safe only when the caller has
        # stopped every writer and checkpointed the database into its main file.
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                raise StoreError("offline store must have no SQLite sidecars")
        before = path.stat()
    uri = path.as_uri() + ("?mode=ro&immutable=1" if offline else "?mode=ro")
    try:
        with contextlib.closing(sqlite3.connect(uri, uri=True)) as db:
            yield db
    except sqlite3.OperationalError as error:
        if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_CANTOPEN:
            raise StoreError(
                "store could not be opened read-only; --offline requires every writer "
                "stopped and a checkpointed store without SQLite sidecars"
            ) from None
        raise
    if offline:
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise StoreError("offline store changed during the operation")
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                raise StoreError("offline store changed during the operation")


def inspect_store(path, *, offline=False):
    with open_store(path, offline=offline) as db:
        db.execute("PRAGMA trusted_schema=OFF")
        db.execute("PRAGMA query_only=ON")
        if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise StoreError("store integrity check failed")
        names = [row[0] for row in db.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        counts = [db.execute('SELECT COUNT(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
                  for name in names]
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return {"integrity": "ok", "tables": len(names), "table_rows": counts,
            "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def snapshot(source, destination, *, offline=False):
    if not source.is_file():
        raise StoreError("source store does not exist")
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(destination) + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            raise FileExistsError
    fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        deadline = time.monotonic() + 30

        def progress(_status, _remaining, _total):
            if time.monotonic() >= deadline:
                raise StoreError("store snapshot timed out")

        with open_store(source, offline=offline) as src:
            with contextlib.closing(sqlite3.connect(destination)) as dst:
                src.backup(dst, pages=128, progress=progress, sleep=0.05)
                if dst.execute("PRAGMA journal_mode=DELETE").fetchone() != ("delete",):
                    raise StoreError("snapshot could not be made standalone")
        # Some SQLite builds leave an unused shared-memory file after leaving
        # WAL mode. Every connection to this new destination is now closed.
        for suffix in ("-wal", "-shm", "-journal"):
            Path(str(destination) + suffix).unlink(missing_ok=True)
        result = inspect_store(destination)
        with destination.open("rb") as output:
            os.fsync(output.fileno())
        return result
    except BaseException:
        for suffix in ("", "-wal", "-shm", "-journal"):
            Path(str(destination) + suffix).unlink(missing_ok=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    copy = commands.add_parser("snapshot", help="copy every SQLite table, including committed WAL data")
    copy.add_argument("source", type=Path)
    copy.add_argument("destination", type=Path)
    verify = commands.add_parser("verify", help="report integrity, table row counts and file checksum")
    verify.add_argument("path", type=Path)
    for command in (copy, verify):
        command.add_argument(
            "--offline", action="store_true",
            help="read a checkpointed main file without locks; requires every writer stopped "
                 "and no -wal, -shm or -journal files (never use on a live store)",
        )
    args = parser.parse_args()
    try:
        result = (snapshot(args.source, args.destination, offline=args.offline)
                  if args.command == "snapshot" else inspect_store(args.path, offline=args.offline))
        print(json.dumps(result, sort_keys=True))
        return 0
    except FileExistsError:
        print("destination already exists; refusing to overwrite", file=sys.stderr)
    except StoreError as error:
        print(str(error), file=sys.stderr)
    except (OSError, sqlite3.Error):
        print("store operation failed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
