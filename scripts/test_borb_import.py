import contextlib
import importlib.util
import io
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('borb_import', Path(__file__).with_name('import-borb-store.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / 'source.sqlite'
        with contextlib.closing(sqlite3.connect(self.source)) as db, db:
            db.execute('CREATE TABLE messages (content BLOB)')
            db.execute('INSERT INTO messages VALUES (?)', (b'x' * 180_000,))
        self.destination = self.root / 'remote'
        self.calls = []
        self.tamper = False

    def execute(self, _url, _token, _id, script, stdin=None):
        self.calls.append(script)
        script = script.replace('/home/borb/workspace/state', str(self.destination)).replace(' status=none', '')
        if stdin is not None and self.tamper:
            stdin = 'dGFtcGVyZWQ='
        result = subprocess.run(['/bin/sh', '-c', script], input=stdin, text=True, capture_output=True)
        module.require(result.returncode == 0)
        return result.stdout.strip()

    def run_import(self):
        with patch.dict(os.environ, {'BORB_ID':'4xkg29ezhmktk16cz833j4r339', 'BORB_URL':'http://localhost', 'BORB_TOKEN':'injected-token'}), \
                patch.object(sys, 'argv', ['import-borb-store.py', '--source', str(self.source), '--coworker-id', 'scout']), \
                patch.object(module, 'execute', self.execute), contextlib.redirect_stdout(io.StringIO()):
            module.main()

    def test_multiple_chunks_preserve_database_and_refuse_overwrite(self):
        self.run_import()
        target = self.destination / 'scout.sqlite'
        self.assertEqual(target.read_bytes(), self.source.read_bytes())
        self.assertEqual(target.stat().st_mode & 0o777, 0o600)
        with contextlib.closing(sqlite3.connect(target)) as db:
            self.assertEqual(db.execute('SELECT length(content) FROM messages').fetchone(), (180_000,))
        self.assertGreater(sum('base64 -d' in call for call in self.calls), 1)
        with self.assertRaises(RuntimeError):
            self.run_import()
        self.assertEqual(target.read_bytes(), self.source.read_bytes())

    def test_bad_checksum_never_publishes(self):
        self.tamper = True
        with self.assertRaises(RuntimeError):
            self.run_import()
        self.assertFalse((self.destination / 'scout.sqlite').exists())

    def test_dangling_sidecar_is_refused(self):
        self.destination.mkdir()
        (self.destination / 'scout.sqlite-wal').symlink_to(self.root / 'absent')
        with self.assertRaises(RuntimeError):
            self.run_import()
        self.assertFalse((self.destination / 'scout.sqlite').exists())

    def test_redirects_are_refused(self):
        self.assertIsNone(module.NoRedirect().redirect_request(None, None, 302, '', {}, 'http://elsewhere'))


if __name__ == '__main__':
    unittest.main()
