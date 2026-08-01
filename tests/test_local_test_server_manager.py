from __future__ import annotations

import argparse
import io
from unittest.mock import MagicMock, patch

from rich.console import Console

import local_test_server_manager as mod
from local_test_server_manager import (
    Instance,
    ProcessInfo,
    WindowsBackend,
    build_arg_parser,
    build_instances,
    cmd_kill,
    cmd_list,
    is_test_server_cmdline,
    kill_instance,
    parse_ports_json,
    parse_processes_json,
    render_instances_table,
    resolve_selector,
)


def test_is_test_server_cmdline_matches_npx_serve():
    assert is_test_server_cmdline(r'"C:\Program Files\nodejs\npx.cmd" serve .')


def test_is_test_server_cmdline_matches_python_http_server():
    assert is_test_server_cmdline("python.exe -m http.server 8000")


def test_is_test_server_cmdline_matches_vite_preview():
    assert is_test_server_cmdline(r'"C:\proj\node_modules\.bin\vite.cmd" preview')


def test_is_test_server_cmdline_rejects_unrelated_process():
    assert not is_test_server_cmdline(r'"C:\Windows\explorer.exe"')


def test_is_test_server_cmdline_rejects_empty():
    assert not is_test_server_cmdline("")


def test_build_instances_groups_npx_wrapper_and_node_child():
    processes = [
        ProcessInfo(pid=100, ppid=1, name="cmd.exe", cmdline="cmd.exe"),
        ProcessInfo(pid=200, ppid=100, name="npx.cmd", cmdline="npx serve ."),
        ProcessInfo(
            pid=300,
            ppid=200,
            name="node.exe",
            cmdline=r"node C:\...\serve\build\main.js",
        ),
    ]
    port_map = {300: [3000]}

    instances = build_instances(processes, port_map)

    assert len(instances) == 1
    instance = instances[0]
    assert instance.root_pid == 200
    assert instance.pids == (200, 300)
    assert instance.ports == (3000,)
    assert instance.cmdline == "npx serve ."


def test_build_instances_excludes_unmatched_processes():
    processes = [
        ProcessInfo(pid=1, ppid=None, name="explorer.exe", cmdline="explorer.exe"),
    ]
    assert build_instances(processes, {}) == []


def test_build_instances_does_not_cross_unrelated_trees():
    processes = [
        ProcessInfo(pid=10, ppid=None, name="cmd.exe", cmdline="cmd.exe"),
        ProcessInfo(pid=20, ppid=10, name="python.exe", cmdline="python -m http.server 8000"),
        ProcessInfo(pid=30, ppid=None, name="cmd.exe", cmdline="cmd.exe"),
        ProcessInfo(pid=40, ppid=30, name="python.exe", cmdline="python -m http.server 9000"),
    ]
    port_map = {20: [8000], 40: [9000]}

    instances = build_instances(processes, port_map)

    assert sorted(i.root_pid for i in instances) == [20, 40]


def test_build_instances_handles_self_referential_ppid():
    processes = [
        ProcessInfo(pid=0, ppid=0, name="System Idle Process", cmdline=""),
    ]
    assert build_instances(processes, {}) == []


def test_parse_processes_json_handles_array():
    raw = '[{"ProcessId":1,"ParentProcessId":0,"Name":"a.exe","CommandLine":"a.exe"}]'
    assert parse_processes_json(raw) == [
        ProcessInfo(pid=1, ppid=0, name="a.exe", cmdline="a.exe")
    ]


def test_parse_processes_json_handles_single_object():
    raw = '{"ProcessId":1,"ParentProcessId":0,"Name":"a.exe","CommandLine":"a.exe"}'
    assert parse_processes_json(raw) == [
        ProcessInfo(pid=1, ppid=0, name="a.exe", cmdline="a.exe")
    ]


def test_parse_processes_json_handles_null_commandline():
    raw = '{"ProcessId":4,"ParentProcessId":0,"Name":"System","CommandLine":null}'
    assert parse_processes_json(raw) == [
        ProcessInfo(pid=4, ppid=0, name="System", cmdline="")
    ]


def test_parse_processes_json_handles_empty_input():
    assert parse_processes_json("") == []


def test_parse_ports_json_groups_multiple_ports_per_pid():
    raw = '[{"LocalPort":3000,"OwningProcess":200},{"LocalPort":3001,"OwningProcess":200}]'
    assert parse_ports_json(raw) == {200: [3000, 3001]}


def test_parse_ports_json_handles_single_object():
    raw = '{"LocalPort":3000,"OwningProcess":200}'
    assert parse_ports_json(raw) == {200: [3000]}


def test_parse_ports_json_handles_empty_input():
    assert parse_ports_json("") == {}


def test_graceful_kill_invokes_taskkill_without_force():
    backend = WindowsBackend()
    with patch("local_test_server_manager.subprocess.run") as mock_run:
        backend.graceful_kill(1234)
    assert mock_run.call_args.args[0] == ["taskkill", "/PID", "1234", "/T"]


def test_force_kill_invokes_taskkill_with_force():
    backend = WindowsBackend()
    with patch("local_test_server_manager.subprocess.run") as mock_run:
        backend.force_kill(1234)
    assert mock_run.call_args.args[0] == ["taskkill", "/PID", "1234", "/T", "/F"]


def test_is_alive_true_when_pid_present_in_tasklist_output():
    backend = WindowsBackend()
    fake_result = MagicMock(stdout="python.exe                    1234 Console  1     12,345 K")
    with patch("local_test_server_manager.subprocess.run", return_value=fake_result):
        assert backend.is_alive(1234) is True


def test_is_alive_false_when_pid_absent_from_tasklist_output():
    backend = WindowsBackend()
    fake_result = MagicMock(
        stdout="INFO: No tasks are running which match the specified criteria."
    )
    with patch("local_test_server_manager.subprocess.run", return_value=fake_result):
        assert backend.is_alive(1234) is False


def test_list_processes_parses_powershell_output():
    backend = WindowsBackend()
    fake_result = MagicMock(
        stdout='[{"ProcessId":1,"ParentProcessId":0,"Name":"a.exe","CommandLine":"a.exe"}]'
    )
    with patch(
        "local_test_server_manager.subprocess.run", return_value=fake_result
    ) as mock_run:
        result = backend.list_processes()
    assert result == [ProcessInfo(pid=1, ppid=0, name="a.exe", cmdline="a.exe")]
    assert mock_run.call_args.args[0][0] == "powershell"


def test_list_listening_ports_parses_powershell_output():
    backend = WindowsBackend()
    fake_result = MagicMock(stdout='[{"LocalPort":3000,"OwningProcess":200}]')
    with patch("local_test_server_manager.subprocess.run", return_value=fake_result):
        result = backend.list_listening_ports()
    assert result == {200: [3000]}


class FakeBackend:
    def __init__(self, alive_after_graceful: bool = False):
        self.alive_after_graceful = alive_after_graceful
        self.graceful_calls: list[int] = []
        self.force_calls: list[int] = []
        self._forced: set[int] = set()

    def list_processes(self):
        return []

    def list_listening_ports(self):
        return {}

    def graceful_kill(self, pid: int) -> None:
        self.graceful_calls.append(pid)

    def force_kill(self, pid: int) -> None:
        self.force_calls.append(pid)
        self._forced.add(pid)

    def is_alive(self, pid: int) -> bool:
        if pid in self._forced:
            return False
        return self.alive_after_graceful


def test_kill_instance_stops_after_graceful_when_process_exits():
    backend = FakeBackend(alive_after_graceful=False)
    instance = Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve .")

    result = kill_instance(backend, instance, wait_seconds=0)

    assert result is True
    assert backend.graceful_calls == [200]
    assert backend.force_calls == []


def test_kill_instance_escalates_to_force_when_still_alive():
    backend = FakeBackend(alive_after_graceful=True)
    instance = Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve .")

    result = kill_instance(backend, instance, wait_seconds=0)

    assert result is True
    assert backend.graceful_calls == [200]
    assert backend.force_calls == [200]


def _sample_instances() -> list[Instance]:
    return [
        Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve ."),
        Instance(id=2, root_pid=400, pids=(400,), ports=(8000,), cmdline="python -m http.server 8000"),
    ]


def test_resolve_selector_by_id():
    instances = _sample_instances()
    assert resolve_selector(instances, "1") == [instances[0]]


def test_resolve_selector_by_port():
    instances = _sample_instances()
    assert resolve_selector(instances, "8000") == [instances[1]]


def test_resolve_selector_all():
    instances = _sample_instances()
    assert resolve_selector(instances, "all") == instances


def test_resolve_selector_unknown_returns_empty():
    instances = _sample_instances()
    assert resolve_selector(instances, "999") == []


def test_build_arg_parser_defaults_command_to_none():
    parser = build_arg_parser()
    args = parser.parse_args([])
    assert args.command is None


def test_build_arg_parser_parses_kill_with_yes_flag():
    parser = build_arg_parser()
    args = parser.parse_args(["kill", "1", "-y"])
    assert args.command == "kill"
    assert args.selector == "1"
    assert args.yes is True


def test_build_arg_parser_kill_defaults_yes_to_false():
    parser = build_arg_parser()
    args = parser.parse_args(["kill", "8000"])
    assert args.yes is False


def test_render_instances_table_includes_port_and_command():
    instances = [Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve .")]
    table = render_instances_table(instances)
    console = Console(file=io.StringIO(), width=100)
    console.print(table)
    output = console.file.getvalue()
    assert "3000" in output
    assert "npx serve ." in output


def test_cmd_list_reports_when_nothing_detected():
    empty_backend = FakeBackend()
    console = Console(file=io.StringIO(), width=100)

    exit_code = cmd_list(argparse.Namespace(), empty_backend, console)

    assert exit_code == 0
    assert "No local test servers" in console.file.getvalue()


def test_cmd_kill_aborts_without_confirmation(monkeypatch):
    instance = Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve .")
    backend = FakeBackend()
    monkeypatch.setattr("local_test_server_manager.discover_instances", lambda b: [instance])
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    console = Console(file=io.StringIO(), width=100)

    exit_code = cmd_kill(argparse.Namespace(selector="1", yes=False), backend, console)

    assert exit_code == 1
    assert backend.graceful_calls == []


def test_cmd_kill_with_yes_skips_confirmation_and_kills():
    instance = Instance(id=1, root_pid=200, pids=(200,), ports=(3000,), cmdline="npx serve .")
    backend = FakeBackend(alive_after_graceful=False)
    console = Console(file=io.StringIO(), width=100)

    original = mod.discover_instances
    mod.discover_instances = lambda b: [instance]
    try:
        exit_code = cmd_kill(argparse.Namespace(selector="1", yes=True), backend, console)
    finally:
        mod.discover_instances = original

    assert exit_code == 0
    assert backend.graceful_calls == [200]


def test_cmd_kill_reports_error_for_unknown_selector():
    backend = FakeBackend()
    console = Console(file=io.StringIO(), width=100)

    original = mod.discover_instances
    mod.discover_instances = lambda b: []
    try:
        exit_code = cmd_kill(argparse.Namespace(selector="999", yes=True), backend, console)
    finally:
        mod.discover_instances = original

    assert exit_code == 1
    assert "No matching" in console.file.getvalue()
