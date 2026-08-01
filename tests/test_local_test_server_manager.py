from __future__ import annotations

from unittest.mock import MagicMock, patch

from local_test_server_manager import (
    ProcessInfo,
    WindowsBackend,
    build_instances,
    is_test_server_cmdline,
    parse_ports_json,
    parse_processes_json,
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
