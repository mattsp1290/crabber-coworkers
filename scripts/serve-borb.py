#!/usr/bin/env python3
"""Keep the Slack host connected to borb with renewable, memory-only tokens."""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import tempfile
import time
from urllib.parse import urlsplit


class SupervisorError(Exception):
    pass


def diagnostic(message):
    print(message, file=sys.stderr, flush=True)


def token_expiry(token, now):
    try:
        parts = token.split(".")
        if len(parts) != 3 or not all(parts) or len(token) > 65536:
            raise ValueError
        payload = base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4),
                                   altchars=b"-_", validate=True)
        claims = json.loads(payload)
        if not isinstance(claims, dict):
            raise ValueError
        expiry = claims["exp"]
        audience = claims.get("aud")
        if (type(expiry) is not int or expiry <= now + 60
                or not (audience == "borb" or isinstance(audience, list) and "borb" in audience)
                or claims.get("sub") != "system:serviceaccount:borb-system:crabber-coworkers"):
            raise ValueError
        return expiry
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise SupervisorError("Token response rejected.") from None


def child_environment(environ, token):
    result = dict(environ)
    result.pop("OPENCODE_GO_API_KEY", None)
    result["BORB_TOKEN"] = token
    return result


def ssh_environment(environ):
    return {key: value for key, value in environ.items()
            if key not in {"OPENCODE_GO_API_KEY", "BORB_TOKEN"}
            and not key.startswith("SLACK_")}


def validate_options(args):
    if not 1 <= args.local_port <= 65535:
        raise SupervisorError("Invalid local port.")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+@[A-Za-z0-9.-]+", args.control_ssh):
        raise SupervisorError("Invalid control SSH destination.")
    if not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]+", args.service_address):
        raise SupervisorError("Invalid service address.")
    if not 1 <= int(args.service_address.rsplit(":", 1)[1]) <= 65535:
        raise SupervisorError("Invalid service address.")
    try:
        with Path(args.config).open() as source:
            url = urlsplit(json.load(source)["borb"]["url"])
        if (url.scheme != "http" or url.hostname != "127.0.0.1"
                or url.port != args.local_port or url.path not in {"", "/"}
                or url.username is not None or url.password is not None
                or url.query or url.fragment):
            raise ValueError
    except (OSError, ValueError, TypeError, KeyError):
        raise SupervisorError("Config must use the configured loopback forward URL.") from None
    args.config = str(Path(args.config).resolve())
    args.binary = str(Path(args.binary).resolve())
    if not os.path.isfile(args.binary) or not os.access(args.binary, os.X_OK):
        raise SupervisorError("Binary must be executable.")


def ssh_command():
    return ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3"]


def obtain_token(args, environ):
    command = ssh_command() + ["--", args.control_ssh,
        "kubectl", "--kubeconfig=/etc/rancher/k3s/k3s.yaml", "-n", "borb-system",
        "create", "token", "crabber-coworkers", "--audience=borb", "--duration=24h"]
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, timeout=30, check=True,
                                env=ssh_environment(environ))
        token = result.stdout.decode("ascii").strip()
        return token, token_expiry(token, time.time())
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise SupervisorError("Token request failed.") from None


def stop_process(process, first_signal, timeout):
    if process is None or process.poll() is not None:
        return
    try:
        process.send_signal(first_signal)
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    except ProcessLookupError:
        pass


class Supervisor:
    def __init__(self, args, environ):
        self.args = args
        self.environ = dict(environ)
        self.stopped = threading.Event()
        self.child = None
        self.tunnel = None
        self.control_directory = None

    def request_stop(self, _signal=None, _frame=None):
        self.stopped.set()

    def drain(self):
        stop_process(self.child, signal.SIGINT, 25)
        self.child = None

    def renew_child(self, expiry):
        if self.child is None or self.child.poll() is not None:
            self.child = None
            return
        self.child.send_signal(signal.SIGUSR1)
        deadline = time.monotonic() + max(0, expiry - time.time() - 30)
        while not self.stopped.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                self.child.wait(timeout=min(0.5, remaining))
                self.child = None
                return
            except subprocess.TimeoutExpired:
                pass
        self.drain()

    def close(self):
        try:
            self.drain()
        finally:
            stop_process(self.tunnel, signal.SIGTERM, 5)
            self.tunnel = None
            if self.control_directory is not None:
                self.control_directory.cleanup()
                self.control_directory = None

    def start_tunnel(self):
        args = self.args
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
                reservation.bind(("127.0.0.1", args.local_port))
        except OSError:
            raise SupervisorError("Tunnel port unavailable.") from None
        self.control_directory = tempfile.TemporaryDirectory(prefix="borb-ssh-")
        control_path = str(Path(self.control_directory.name) / "control")
        command = ssh_command() + ["-o", "ControlMaster=yes", "-S", control_path,
            "-o", "ExitOnForwardFailure=yes", "-N",
            "-L", f"127.0.0.1:{args.local_port}:{args.service_address}", "--", args.control_ssh]
        try:
            self.tunnel = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=ssh_environment(self.environ), start_new_session=True)
        except OSError:
            self.control_directory.cleanup()
            self.control_directory = None
            raise
        deadline = time.monotonic() + 15
        while not self.stopped.is_set() and time.monotonic() < deadline:
            if self.tunnel.poll() is not None:
                break
            try:
                ready = subprocess.run(ssh_command() + ["-S", control_path, "-O", "check",
                    "--", args.control_ssh], stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    env=ssh_environment(self.environ), timeout=1, check=False)
                if ready.returncode == 0:
                    with socket.create_connection(("127.0.0.1", args.local_port), timeout=0.5):
                        if self.tunnel.poll() is None:
                            return
            except (OSError, subprocess.SubprocessError):
                pass
            self.stopped.wait(0.2)
        stop_process(self.tunnel, signal.SIGTERM, 5)
        self.tunnel = None
        self.control_directory.cleanup()
        self.control_directory = None
        raise SupervisorError("Tunnel unavailable.")

    def start_child(self, token):
        self.child = subprocess.Popen([self.args.binary, "serve", "--config", self.args.config],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=child_environment(self.environ, token), start_new_session=True)

    def run(self):
        token = None
        expiry = 0
        refresh_at = 0
        delay = 1
        try:
            while not self.stopped.is_set():
                try:
                    if token is None or time.time() >= refresh_at:
                        renewed, renewed_expiry = obtain_token(self.args, self.environ)
                        if self.stopped.is_set():
                            break
                        self.renew_child(expiry)
                        token, expiry = renewed, renewed_expiry
                        refresh_at = time.time() + (expiry - time.time()) / 2
                    if self.tunnel is None:
                        self.start_tunnel()
                    if self.stopped.is_set():
                        break
                    if self.child is None:
                        self.start_child(token)
                    started = time.monotonic()
                    while not self.stopped.wait(0.5):
                        if self.child.poll() is not None or self.tunnel.poll() is not None:
                            raise SupervisorError("Service restarting.")
                        if time.time() >= refresh_at:
                            break
                        if time.monotonic() - started >= 60:
                            delay = 1
                except (SupervisorError, OSError, subprocess.SubprocessError):
                    diagnostic("Supervisor retrying after a transient failure.")
                    if (self.tunnel is not None and self.tunnel.poll() is not None
                            or self.child is not None and self.child.poll() is not None
                            or time.time() >= expiry - 30):
                        self.close()
                    retry_delay = delay
                    if self.child is not None:
                        retry_delay = min(delay, max(0.5, expiry - time.time() - 60))
                    self.stopped.wait(retry_delay)
                    delay = min(delay * 2, 60)
        finally:
            self.close()


class SafeParser(argparse.ArgumentParser):
    def error(self, _message):
        self.exit(2, "Invalid supervisor arguments. Use --help.\n")


def main():
    parser = SafeParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--control-ssh", default="infra-admin@100.82.43.93")
    parser.add_argument("--local-port", type=int, default=18080)
    parser.add_argument("--service-address", default="10.43.85.21:8080")
    args = parser.parse_args()
    try:
        validate_options(args)
        supervisor = Supervisor(args, os.environ)
        signal.signal(signal.SIGINT, supervisor.request_stop)
        signal.signal(signal.SIGTERM, supervisor.request_stop)
        supervisor.run()
        return 0
    except Exception:
        diagnostic("Supervisor could not run.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
