#!/usr/bin/env python3
"""Import an offline SQLite snapshot into an empty worker through borb exec."""
import argparse
import contextlib
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import sys
import urllib.request
import urllib.parse
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def require(condition):
    if not condition:
        raise RuntimeError('store import refused')


def execute(url, token, borb_id, script, stdin=None):
    data = json.dumps({
        'argv': ['/bin/sh', '-c', script], 'stdin': stdin,
        'timeout_seconds': 30, 'wake': False,
    }).encode()
    request = urllib.request.Request(
        url.rstrip('/') + '/v1/borbs/' + borb_id + '/exec', data=data,
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(request, timeout=40) as response:
        payload = response.read(1_048_577)
    require(len(payload) <= 1_048_576)
    result = json.loads(payload)
    require(result['exit_code'] == 0 and not result['stdout_truncated'] and not result['stderr_truncated'])
    return result['stdout'].strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--coworker-id', required=True)
    args = parser.parse_args()
    require(re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', args.coworker_id) and len(args.coworker_id) <= 64)
    borb_id = os.environ['BORB_ID']
    require(re.fullmatch(r'[0-9abcdefghjkmnpqrstvwxyz]{26}', borb_id))
    url, token = os.environ['BORB_URL'], os.environ['BORB_TOKEN']
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme in ('http', 'https') and parsed.hostname and not parsed.username
            and not parsed.password and not parsed.query and not parsed.fragment)
    require(token and args.source.is_file() and args.source.stat().st_size <= 128 * 1024 * 1024)
    require(not Path(str(args.source) + '-wal').exists())
    with contextlib.closing(sqlite3.connect(args.source.resolve().as_uri() + '?mode=ro', uri=True)) as source:
        require(source.execute('PRAGMA integrity_check').fetchall() == [('ok',)])
    digest = hashlib.sha256(args.source.read_bytes()).hexdigest()
    target = '/home/borb/workspace/state/' + args.coworker_id + '.sqlite'
    stage = target + '.import-' + uuid.uuid4().hex
    qtarget, qstage = shlex.quote(target), shlex.quote(stage)
    absent = "; ".join("test ! -e " + shlex.quote(target + suffix) + "; test ! -L " + shlex.quote(target + suffix) for suffix in ("", "-wal", "-shm", "-journal"))
    execute(url, token, borb_id,
            f'set -eu; umask 077; {absent}; mkdir -p /home/borb/workspace/state; set -C; : > {qstage}')
    offset = 0
    with args.source.open('rb') as source:
        while chunk := source.read(48 * 1024):
            execute(url, token, borb_id,
                    f'set -eu; base64 -d | dd of={qstage} bs=49152 seek={offset // (48 * 1024)} conv=notrunc status=none',
                    base64.b64encode(chunk).decode())
            offset += len(chunk)
    actual = execute(url, token, borb_id, f'sha256sum {qstage}').split()[0]
    require(actual == digest)
    execute(url, token, borb_id,
            f'set -eu; {absent}; chmod 0600 {qstage}; ln {qstage} {qtarget}; rm {qstage}')
    require(execute(url, token, borb_id, f'sha256sum {qtarget}').split()[0] == digest)
    print(json.dumps({'imported': True, 'sha256': digest, 'bytes': offset}))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('store import failed; destination may require inspection', file=sys.stderr)
        sys.exit(1)
