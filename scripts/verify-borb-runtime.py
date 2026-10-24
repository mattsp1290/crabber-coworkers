#!/usr/bin/env python3
"""Read-only evidence that selected borbs run the coworker worker as PID 1."""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.parse
import urllib.request


class VerificationError(Exception):
    pass


def valid_id(value):
    return isinstance(value, str) and len(value) <= 64 and re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value) is not None


def select_borbs(rows, prefix, coworker_id):
    selected = []
    for row in rows:
        name = row.get("spec", {}).get("name", "")
        if not isinstance(name, str) or not name.startswith(prefix + "-"):
            continue
        suffix = name[len(prefix) + 1:]
        if coworker_id is not None and suffix != coworker_id:
            continue
        if not valid_id(suffix) or not re.fullmatch(r"[0-9abcdefghjkmnpqrstvwxyz]{26}", row.get("id", "")):
            raise VerificationError("invalid worker identity in borb response")
        selected.append((suffix, row))
    if not selected:
        raise VerificationError("no matching worker borbs")
    if len({coworker for coworker, _row in selected}) != len(selected):
        raise VerificationError("multiple borbs matched a coworker")
    return selected


def check_process(executable, cmdline, coworker_id):
    argv = cmdline.rstrip(b"\0").split(b"\0")
    return (executable.strip() == b"/usr/local/bin/crabber-coworkers"
            and len(argv) == 4
            and argv[0] == b"/usr/local/bin/crabber-coworkers"
            and argv[1:] == [b"worker", b"--coworker-id", coworker_id.encode()])


def ssh_kubectl(host, namespace, *args):
    command = shlex.join(["kubectl", "--kubeconfig=/etc/rancher/k3s/k3s.yaml", "-n", namespace, *args])
    result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, command],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
    if result.returncode != 0:
        raise VerificationError("remote runtime inspection failed")
    return result.stdout


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch_borbs(url, token):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    rows = []
    cursor = None
    seen = set()
    for _page in range(100):
        query = {"limit": "100"}
        if cursor:
            query["cursor"] = cursor
        request = urllib.request.Request(url.rstrip("/") + "/v1/borbs?" + urllib.parse.urlencode(query),
                                         headers={"Authorization": "Bearer " + token})
        with opener.open(request, timeout=30) as response:
            payload = response.read(8 * 1024 * 1024 + 1)
        if len(payload) > 8 * 1024 * 1024:
            raise VerificationError("borb response limit exceeded")
        page = json.loads(payload)
        rows.extend(page["items"])
        cursor = page.get("next_cursor")
        if not cursor:
            return rows
        if not isinstance(cursor, str) or cursor in seen:
            raise VerificationError("invalid borb pagination")
        seen.add(cursor)
    raise VerificationError("borb pagination limit exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coworker-id")
    parser.add_argument("--name-prefix", default="coworkers")
    parser.add_argument("--ssh-host", default=os.environ.get("BORB_SSH_HOST"))
    parser.add_argument("--namespace", default="borb-sandboxes")
    args = parser.parse_args()
    try:
        url, token = os.environ.get("BORB_URL", ""), os.environ.get("BORB_TOKEN", "")
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or not token):
            raise VerificationError("set BORB_URL and BORB_TOKEN in the environment")
        if (not valid_id(args.name_prefix) or len(args.name_prefix) > 24
                or (args.coworker_id is not None and not valid_id(args.coworker_id))):
            raise VerificationError("invalid coworker selection")
        if (not args.ssh_host or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@:-]*", args.ssh_host)
                or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.namespace)):
            raise VerificationError("set --ssh-host or BORB_SSH_HOST and a valid namespace")
        selected = select_borbs(fetch_borbs(url, token), args.name_prefix, args.coworker_id)
        pods = json.loads(ssh_kubectl(args.ssh_host, args.namespace, "get", "pods", "-o", "json"))["items"]
        evidence = []
        for coworker_id, row in selected:
            matching = [pod for pod in pods if pod.get("metadata", {}).get("labels", {}).get("borb.dev/id") == row["id"]
                        and not pod.get("metadata", {}).get("deletionTimestamp")]
            verified = False
            executable = None
            node = None
            phase = row.get("phase")
            if len(matching) == 1:
                pod = matching[0]
                node = pod.get("spec", {}).get("nodeName")
                if node is not None and not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", node):
                    raise VerificationError("invalid runtime node identity")
                name = pod["metadata"]["name"]
                if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name):
                    raise VerificationError("invalid runtime pod identity")
                if phase == "running" and pod.get("status", {}).get("phase") == "Running":
                    exe = ssh_kubectl(args.ssh_host, args.namespace, "exec", name, "-c", "borb", "--", "readlink", "/proc/1/exe")
                    argv = ssh_kubectl(args.ssh_host, args.namespace, "exec", name, "-c", "borb", "--", "cat", "/proc/1/cmdline")
                    verified = check_process(exe, argv, coworker_id)
                    if verified:
                        executable = "/usr/local/bin/crabber-coworkers"
            evidence.append({"borb_id": row["id"], "coworker_id": coworker_id, "node": node,
                             "phase": "running" if phase == "running" else "not-running",
                             "executable": executable, "verified": verified})
        print(json.dumps(evidence, sort_keys=True))
        return 0 if all(item["verified"] for item in evidence) else 1
    except VerificationError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("runtime verification failed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
