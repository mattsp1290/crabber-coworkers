import argparse
import base64
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location("serve_borb", Path(__file__).with_name("serve-borb.py"))
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


def jwt(expiry=2000, **overrides):
    claims = {"exp": expiry, "aud": ["borb"],
              "sub": "system:serviceaccount:borb-system:crabber-coworkers"}
    claims.update(overrides)
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return "header." + body + ".signature"


def options(**overrides):
    values = dict(config="private.json", binary=sys.executable, local_port=18080,
                  service_address="10.43.85.21:8080", control_ssh="infra-admin@100.82.43.93")
    values.update(overrides)
    return argparse.Namespace(**values)


class FakeProcess:
    def __init__(self):
        self.returncode = None
        self.signals = []
        self.waits = []

    def poll(self):
        return self.returncode

    def send_signal(self, value):
        self.signals.append(value)

    def wait(self, timeout):
        self.waits.append(timeout)
        self.returncode = 0


class SupervisorTests(unittest.TestCase):
    def test_token_expiry_and_untrusted_claims(self):
        self.assertEqual(supervisor.token_expiry(jwt(), 1000), 2000)
        malformed = ["secret", "a.!!!.b", "a.W10.b", "a.bnVsbA.b", "a.e30.b",
                     jwt(999), jwt(1060), jwt(True), jwt("2000"),
                     jwt(aud=["other"]), jwt(sub="another-subject")]
        for token in malformed:
            with self.subTest(token_kind=malformed.index(token)):
                with self.assertRaisesRegex(supervisor.SupervisorError, "Token response rejected"):
                    supervisor.token_expiry(token, 1000)

    def test_environment_keeps_slack_only_in_service_and_excludes_provider_key(self):
        environ = {"PATH": "/bin", "SLACK_APP_TOKEN_A": "private-slack",
                   "SLACK_BOT_TOKEN_A": "private-bot", "OPENCODE_GO_API_KEY": "private-key",
                   "BORB_TOKEN": "old-token"}
        child = supervisor.child_environment(environ, "new-token")
        self.assertEqual(child["SLACK_APP_TOKEN_A"], "private-slack")
        self.assertEqual(child["BORB_TOKEN"], "new-token")
        self.assertNotIn("OPENCODE_GO_API_KEY", child)
        self.assertEqual(supervisor.ssh_environment(environ), {"PATH": "/bin"})
        self.assertEqual(environ["BORB_TOKEN"], "old-token")

    def test_config_must_match_forward(self):
        with tempfile.TemporaryDirectory() as root:
            config = Path(root) / "config.json"
            for url in ["http://127.0.0.1:18080", "http://127.0.0.1:18080/"]:
                config.write_text(json.dumps({"borb": {"url": url}}))
                supervisor.validate_options(options(config=str(config)))
            for url in ["http://localhost:18080", "https://127.0.0.1:18080",
                        "http://127.0.0.1:8080", "http://secret@127.0.0.1:18080",
                        "http://127.0.0.1:18080/path", "http://127.0.0.1:18080?secret"]:
                config.write_text(json.dumps({"borb": {"url": url}}))
                with self.assertRaises(supervisor.SupervisorError):
                    supervisor.validate_options(options(config=str(config)))

    def test_token_command_captures_output_and_suppresses_errors(self):
        token = jwt(9999999999)
        with patch.object(supervisor.subprocess, "run", return_value=Mock(stdout=token.encode())) as run:
            self.assertEqual(supervisor.obtain_token(options(), {"SLACK_APP_TOKEN_A": "secret"})[0], token)
        command = run.call_args.args[0]
        self.assertIn("--audience=borb", command)
        self.assertIn("--duration=24h", command)
        self.assertIn("crabber-coworkers", command)
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertEqual(run.call_args.kwargs["stdout"], subprocess.PIPE)
        self.assertEqual(run.call_args.kwargs["env"], {})

    def test_token_failure_does_not_disclose_remote_error(self):
        captured = io.StringIO()
        failure = subprocess.CalledProcessError(1, ["secret-command"], stderr=b"secret-error")
        with patch.object(supervisor.subprocess, "run", side_effect=failure):
            with contextlib.redirect_stderr(captured):
                with self.assertRaises(supervisor.SupervisorError) as error:
                    supervisor.obtain_token(options(), {})
        self.assertEqual(str(error.exception), "Token request failed.")
        self.assertEqual(captured.getvalue(), "")

    def test_child_drain_is_bounded_and_forces_kill(self):
        process = Mock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("secret", 25), 0]
        supervisor.stop_process(process, signal.SIGINT, 25)
        process.send_signal.assert_called_once_with(signal.SIGINT)
        process.kill.assert_called_once_with()
        self.assertEqual([call.kwargs["timeout"] for call in process.wait.call_args_list], [25, 5])

    def test_renewal_drains_child_and_reuses_tunnel_then_shutdown(self):
        service = supervisor.Supervisor(options(), {"SLACK_APP_TOKEN_A": "secret"})
        clock = [1000.0]
        children = []
        tunnel = FakeProcess()

        def start_child(_token):
            child = FakeProcess()
            children.append(child)
            service.child = child
            if len(children) == 2:
                service.request_stop(signal.SIGTERM)

        def wait(delay):
            clock[0] += delay
            return service.stopped.is_set()

        def start_tunnel():
            service.tunnel = tunnel

        with patch.object(supervisor, "obtain_token", side_effect=[("first", 1301), ("renewed", 5000)]) as obtain:
            with patch.object(service, "start_tunnel", side_effect=start_tunnel) as forward:
                with patch.object(service, "start_child", side_effect=start_child) as child:
                    with patch.object(service.stopped, "wait", side_effect=wait):
                        with patch.object(supervisor.time, "time", side_effect=lambda: clock[0]):
                            service.run()
        self.assertEqual(obtain.call_count, 2)
        self.assertEqual(forward.call_count, 1)
        self.assertEqual([call.args[0] for call in child.call_args_list], ["first", "renewed"])
        self.assertEqual([process.signals for process in children], [[signal.SIGUSR1], [signal.SIGINT]])
        self.assertEqual(tunnel.signals, [signal.SIGTERM])
        self.assertIsNone(service.child)
        self.assertIsNone(service.tunnel)

    def test_transient_failures_back_off_and_stop_without_sensitive_diagnostics(self):
        service = supervisor.Supervisor(options(), {})
        waits = []

        def wait(delay):
            waits.append(delay)
            if len(waits) == 9:
                service.request_stop()
            return service.stopped.is_set()

        captured = io.StringIO()
        with patch.object(supervisor, "obtain_token", side_effect=OSError("private-credential")):
            with patch.object(service.stopped, "wait", side_effect=wait):
                with contextlib.redirect_stderr(captured):
                    service.run()
        self.assertEqual(waits, [1, 2, 4, 8, 16, 32, 60, 60, 60])
        self.assertEqual(set(captured.getvalue().splitlines()),
                         {"Supervisor retrying after a transient failure."})

    def test_child_and_tunnel_exit_restart_both_processes(self):
        for failed_process in ("child", "tunnel"):
            with self.subTest(failed_process=failed_process):
                service = supervisor.Supervisor(options(), {})
                children = []
                tunnels = []

                def start_tunnel():
                    service.tunnel = FakeProcess()
                    tunnels.append(service.tunnel)

                def start_child(_token):
                    service.child = FakeProcess()
                    children.append(service.child)
                    if len(children) == 2:
                        service.request_stop()

                def wait(delay):
                    if delay == 0.5 and len(children) == 1:
                        getattr(service, failed_process).returncode = 1
                    return service.stopped.is_set()

                with patch.object(supervisor, "obtain_token", return_value=("token", 2000)) as obtain:
                    with patch.object(supervisor.time, "time", return_value=1000):
                        with patch.object(service, "start_tunnel", side_effect=start_tunnel):
                            with patch.object(service, "start_child", side_effect=start_child):
                                with patch.object(service.stopped, "wait", side_effect=wait):
                                    with contextlib.redirect_stderr(io.StringIO()):
                                        service.run()
                self.assertEqual(obtain.call_count, 1)
                self.assertEqual(len(children), 2)
                self.assertEqual(len(tunnels), 2)
                self.assertIsNone(service.child)
                self.assertIsNone(service.tunnel)

    def test_unready_tunnel_is_stopped_without_starting_child(self):
        service = supervisor.Supervisor(options(local_port=0), {})
        tunnel = FakeProcess()
        with patch.object(supervisor.subprocess, "Popen", return_value=tunnel):
            with patch.object(supervisor.time, "monotonic", side_effect=[0, 16]):
                with self.assertRaises(supervisor.SupervisorError):
                    service.start_tunnel()
        self.assertEqual(tunnel.signals, [signal.SIGTERM])
        self.assertIsNone(service.tunnel)
        self.assertIsNone(service.child)

    def test_renewal_allows_ordinary_queued_turns_to_drain(self):
        service = supervisor.Supervisor(options(), {})
        service.child = FakeProcess()
        process = service.child
        clock = [1000.0]

        def wait(timeout):
            clock[0] += timeout
            if clock[0] < 1180:
                raise subprocess.TimeoutExpired("service", timeout)
            process.returncode = 0

        with patch.object(process, "wait", side_effect=wait):
            with patch.object(supervisor.time, "time", side_effect=lambda: clock[0]):
                with patch.object(supervisor.time, "monotonic", side_effect=lambda: clock[0]):
                    service.renew_child(2000)
        self.assertEqual(process.signals, [signal.SIGUSR1])
        self.assertGreaterEqual(clock[0], 1180)
        self.assertIsNone(service.child)

    def test_explicit_stop_interrupts_renewal_with_bounded_shutdown(self):
        service = supervisor.Supervisor(options(), {})
        process = FakeProcess()
        service.child = process

        def wait(timeout):
            if timeout == 0.5:
                service.request_stop()
                raise subprocess.TimeoutExpired("service", timeout)
            self.assertEqual(timeout, 25)
            process.returncode = 0

        with patch.object(process, "wait", side_effect=wait):
            with patch.object(supervisor.time, "time", return_value=1000):
                service.renew_child(2000)
        self.assertEqual(process.signals, [signal.SIGUSR1, signal.SIGINT])
        self.assertIsNone(service.child)

    def test_occupied_port_never_starts_ssh_or_service(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as foreign:
            foreign.bind(("127.0.0.1", 0))
            foreign.listen()
            service = supervisor.Supervisor(options(local_port=foreign.getsockname()[1]), {})
            with patch.object(supervisor.subprocess, "Popen") as spawn:
                with self.assertRaisesRegex(supervisor.SupervisorError, "Tunnel port unavailable"):
                    service.start_tunnel()
            spawn.assert_not_called()
        self.assertIsNone(service.child)
        self.assertIsNone(service.tunnel)

    def test_foreign_connection_is_not_readiness_without_ssh_control(self):
        service = supervisor.Supervisor(options(local_port=0), {})
        tunnel = FakeProcess()
        clock = [0.0]

        def wait(delay):
            clock[0] += delay
            return False

        with patch.object(supervisor.subprocess, "Popen", return_value=tunnel):
            with patch.object(supervisor.subprocess, "run", return_value=Mock(returncode=1)) as check:
                with patch.object(supervisor.socket, "create_connection") as connect:
                    with patch.object(supervisor.time, "monotonic", side_effect=lambda: clock[0]):
                        with patch.object(service.stopped, "wait", side_effect=wait):
                            with self.assertRaises(supervisor.SupervisorError):
                                service.start_tunnel()
        check.assert_called()
        connect.assert_not_called()
        self.assertEqual(tunnel.signals, [signal.SIGTERM])
        self.assertIsNone(service.control_directory)
        self.assertIsNone(service.child)

    def test_tcp_ready_after_ssh_exit_does_not_start_service(self):
        service = supervisor.Supervisor(options(local_port=0), {})
        tunnel = FakeProcess()

        def connect(*_args, **_kwargs):
            tunnel.returncode = 1
            return contextlib.nullcontext()

        with patch.object(supervisor.subprocess, "Popen", return_value=tunnel):
            with patch.object(supervisor.subprocess, "run", return_value=Mock(returncode=0)):
                with patch.object(supervisor.socket, "create_connection", side_effect=connect):
                    with patch.object(service.stopped, "wait", return_value=False):
                        with self.assertRaises(supervisor.SupervisorError):
                            service.start_tunnel()
        self.assertIsNone(service.tunnel)
        self.assertIsNone(service.child)
        self.assertIsNone(service.control_directory)

    def test_spawn_arguments_never_include_credentials_or_inherit_output(self):
        service = supervisor.Supervisor(options(), {"OPENCODE_GO_API_KEY": "provider-secret"})
        with patch.object(supervisor.subprocess, "Popen", return_value=FakeProcess()) as spawn:
            service.start_child("borb-secret")
        self.assertEqual(spawn.call_args.args[0], [sys.executable, "serve", "--config", "private.json"])
        self.assertEqual(spawn.call_args.kwargs["env"], {"BORB_TOKEN": "borb-secret"})
        self.assertEqual(spawn.call_args.kwargs["stdout"], subprocess.DEVNULL)
        self.assertEqual(spawn.call_args.kwargs["stderr"], subprocess.DEVNULL)


if __name__ == "__main__":
    unittest.main()
