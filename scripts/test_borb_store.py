import contextlib
import importlib.util
import http.server
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import urllib.error
import unittest


def load_script(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), Path(__file__).with_name(name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


store = load_script("borb-store")
runtime = load_script("verify-borb-runtime")


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.sqlite"
        self.destination = self.root / "snapshot.sqlite"

    def test_snapshot_preserves_wal_rows_and_unrelated_tables(self):
        with contextlib.closing(sqlite3.connect(self.source)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE conversations (id TEXT, prompt TEXT)")
            db.execute("CREATE TABLE metadata (value TEXT)")
            db.execute("INSERT INTO conversations VALUES ('id', 'private prompt')")
            db.execute("INSERT INTO metadata VALUES ('private metadata')")
            db.commit()
            self.assertTrue(Path(str(self.source) + "-wal").exists())
            result = store.snapshot(self.source, self.destination)
        self.assertEqual(result["integrity"], "ok")
        self.assertEqual(result["table_rows"], [1, 1])
        with contextlib.closing(sqlite3.connect(self.destination)) as copied:
            self.assertEqual(copied.execute("PRAGMA journal_mode").fetchone(), ("delete",))
            self.assertEqual(copied.execute("SELECT prompt FROM conversations").fetchone(), ("private prompt",))
            self.assertEqual(copied.execute("SELECT value FROM metadata").fetchone(), ("private metadata",))
        self.assertEqual(self.destination.stat().st_mode & 0o777, 0o600)
        self.assertEqual(store.inspect_store(self.destination), result)

    def test_existing_destination_is_never_overwritten(self):
        self.source.write_bytes(b"source")
        self.destination.write_bytes(b"original")
        with self.assertRaises(FileExistsError):
            store.snapshot(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), b"original")

    def test_offline_snapshot_reads_standalone_wal_mode_main_file(self):
        live = self.root / "live.sqlite"
        with contextlib.closing(sqlite3.connect(live)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE history (value TEXT)")
            db.execute("INSERT INTO history VALUES ('private history')")
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copyfile(live, self.source)
        original = self.source.read_bytes()
        self.assertEqual(original[18:20], b"\x02\x02")
        result = store.snapshot(self.source, self.destination, offline=True)
        self.assertEqual(result["table_rows"], [1])
        self.assertEqual(store.inspect_store(self.source, offline=True)["table_rows"], [1])
        self.assertEqual(self.source.read_bytes(), original)
        self.assertFalse(any(self.root.glob("source.sqlite-*")))
        self.assertFalse(any(self.root.glob("snapshot.sqlite-*")))
        with contextlib.closing(sqlite3.connect(self.destination)) as db:
            self.assertEqual(db.execute("SELECT value FROM history").fetchone(), ("private history",))
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone(), ("delete",))

    def test_offline_mode_rejects_every_sqlite_sidecar(self):
        with contextlib.closing(sqlite3.connect(self.source)) as db:
            db.execute("CREATE TABLE history (value TEXT)")
        for suffix in ("-wal", "-shm", "-journal"):
            with self.subTest(suffix=suffix):
                sidecar = Path(str(self.source) + suffix)
                sidecar.write_bytes(b"private WAL data")
                try:
                    with self.assertRaisesRegex(store.StoreError, "no SQLite sidecars"):
                        store.inspect_store(self.source, offline=True)
                    with self.assertRaisesRegex(store.StoreError, "no SQLite sidecars"):
                        store.snapshot(self.source, self.destination, offline=True)
                    self.assertFalse(self.destination.exists())
                    self.assertEqual(sidecar.read_bytes(), b"private WAL data")
                finally:
                    sidecar.unlink()

    def test_offline_mode_never_ignores_committed_wal_rows(self):
        with contextlib.closing(sqlite3.connect(self.source)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE history (value TEXT)")
            db.execute("INSERT INTO history VALUES ('committed history')")
            db.commit()
            with self.assertRaisesRegex(store.StoreError, "no SQLite sidecars"):
                store.snapshot(self.source, self.destination, offline=True)
            self.assertFalse(self.destination.exists())
            self.assertEqual(store.snapshot(self.source, self.destination)["table_rows"], [1])

    def test_cli_offline_verifies_standalone_wal_mode_store(self):
        live = self.root / "live.sqlite"
        with contextlib.closing(sqlite3.connect(live)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE history (value TEXT)")
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copyfile(live, self.source)
        result = subprocess.run(
            [sys.executable, str(Path(__file__).with_name("borb-store.py")),
             "verify", "--offline", str(self.source)], capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["table_rows"], [0])

    def test_existing_destination_sidecars_are_preserved(self):
        self.source.write_bytes(b"source")
        wal = Path(str(self.destination) + "-wal")
        wal.write_bytes(b"original")
        with self.assertRaises(FileExistsError):
            store.snapshot(self.source, self.destination)
        self.assertEqual(wal.read_bytes(), b"original")
        self.assertFalse(self.destination.exists())

    def test_failed_snapshot_leaves_no_destination(self):
        self.source.write_bytes(b"invalid SQLite file with private prompt")
        with self.assertRaises(sqlite3.DatabaseError):
            store.snapshot(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_cli_redacts_database_errors_and_content(self):
        secret = "private prompt and answer"
        self.source.write_text(secret)
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("borb-store.py")), "verify", str(self.source)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr.strip(), "store operation failed")
        self.assertNotIn(secret, result.stderr)

    def test_cli_reports_only_statistics(self):
        with contextlib.closing(sqlite3.connect(self.source)) as db, db:
            db.execute('CREATE TABLE "private prompt" (answer TEXT)')
            db.execute('INSERT INTO "private prompt" VALUES ("private answer")')
        result = subprocess.run([sys.executable, str(Path(__file__).with_name("borb-store.py")), "snapshot", str(self.source), str(self.destination)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["table_rows"], [1])
        self.assertNotIn("private", result.stdout + result.stderr)


class RuntimeEvidenceTests(unittest.TestCase):
    def test_worker_identity_requires_executable_and_exact_arguments(self):
        binary = b"/usr/local/bin/crabber-coworkers"
        argv = binary + b"\0worker\0--coworker-id\0alice\0"
        self.assertTrue(runtime.check_process(binary + b"\n", argv, "alice"))
        self.assertFalse(runtime.check_process(b"/bin/sleep\n", argv, "alice"))
        self.assertFalse(runtime.check_process(binary, argv, "bob"))
        self.assertFalse(runtime.check_process(binary, argv + b"--extra\0", "alice"))

    def test_api_refuses_redirects_instead_of_forwarding_credentials(self):
        requests = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(302)
                self.send_header("Location", "/unexpected")
                self.end_headers()

            def log_message(self, *_args):
                pass

        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                with self.assertRaises(urllib.error.HTTPError) as error:
                    runtime.fetch_borbs("http://127.0.0.1:" + str(server.server_port), "test-token")
                error.exception.close()
            finally:
                server.shutdown()
                thread.join()
        self.assertEqual(requests, ["/v1/borbs?limit=100"])

    def test_selects_exact_coworker_and_rejects_duplicate_workers(self):
        row = {"id": "4xkg29ezhmktk16cz833j4r339", "spec": {"name": "coworkers-alice"}}
        self.assertEqual(runtime.select_borbs([row], "coworkers", "alice"), [("alice", row)])
        with self.assertRaises(runtime.VerificationError):
            runtime.select_borbs([row], "coworkers", "ali")
        with self.assertRaises(runtime.VerificationError):
            runtime.select_borbs([row, row], "coworkers", None)


if __name__ == "__main__":
    unittest.main()
