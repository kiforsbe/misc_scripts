from __future__ import annotations

import json
import platform
import re
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class ProcessInfo:
    pid: int
    ppid: int | None
    name: str
    cmdline: str


@dataclass(frozen=True)
class Instance:
    id: int
    root_pid: int
    pids: tuple[int, ...]
    ports: tuple[int, ...]
    cmdline: str


TEST_SERVER_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\bnpx(\.cmd)?\b.*\b(serve|http-server|live-server|browser-sync|lite-server|five-server)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bnode(\.exe)?\b.*\b(http-server|live-server|browser-sync|five-server|lite-server)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bpython3?(\.exe)?\b.*-m\s+(http\.server|SimpleHTTPServer)\b", re.IGNORECASE),
    re.compile(r"\bpy(\.exe)?\b.*-m\s+(http\.server|SimpleHTTPServer)\b", re.IGNORECASE),
    re.compile(r"\bphp(\.exe)?\b.*-S\s", re.IGNORECASE),
    re.compile(r"\bvite(\.cmd)?\b.*\b(dev|preview)\b", re.IGNORECASE),
    re.compile(r"\bwebpack(-dev-server)?(\.cmd)?\b.*\bserve\b", re.IGNORECASE),
    re.compile(r"\bhttp-server(\.cmd)?\b", re.IGNORECASE),
]


def is_test_server_cmdline(cmdline: str) -> bool:
    if not cmdline:
        return False
    return any(pattern.search(cmdline) for pattern in TEST_SERVER_PATTERNS)


def build_instances(
    processes: list[ProcessInfo], port_map: dict[int, list[int]]
) -> list[Instance]:
    by_pid = {p.pid: p for p in processes}
    children: dict[int, list[int]] = defaultdict(list)
    for p in processes:
        if p.ppid is not None and p.ppid != p.pid:
            children[p.ppid].append(p.pid)

    matched = {p.pid for p in processes if is_test_server_cmdline(p.cmdline)}

    def is_root(pid: int) -> bool:
        parent_pid = by_pid[pid].ppid
        return parent_pid is None or parent_pid not in matched

    roots = sorted(pid for pid in matched if is_root(pid))

    instances: list[Instance] = []
    for idx, root_pid in enumerate(roots, start=1):
        subtree: list[int] = []
        seen: set[int] = set()
        stack = [root_pid]
        while stack:
            pid = stack.pop()
            if pid in seen:
                continue
            seen.add(pid)
            subtree.append(pid)
            stack.extend(children.get(pid, []))
        ports = tuple(sorted({port for pid in subtree for port in port_map.get(pid, [])}))
        instances.append(
            Instance(
                id=idx,
                root_pid=root_pid,
                pids=tuple(subtree),
                ports=ports,
                cmdline=by_pid[root_pid].cmdline,
            )
        )
    return instances


def parse_processes_json(raw: str) -> list[ProcessInfo]:
    if not raw or not raw.strip():
        return []
    data = json.loads(raw)
    if isinstance(data, dict):
        data = [data]
    processes: list[ProcessInfo] = []
    for item in data:
        pid = item.get("ProcessId")
        if pid is None:
            continue
        ppid = item.get("ParentProcessId")
        processes.append(
            ProcessInfo(
                pid=int(pid),
                ppid=int(ppid) if ppid is not None else None,
                name=item.get("Name") or "",
                cmdline=item.get("CommandLine") or "",
            )
        )
    return processes


def parse_ports_json(raw: str) -> dict[int, list[int]]:
    if not raw or not raw.strip():
        return {}
    data = json.loads(raw)
    if isinstance(data, dict):
        data = [data]
    port_map: dict[int, list[int]] = {}
    for item in data:
        pid = item.get("OwningProcess")
        port = item.get("LocalPort")
        if pid is None or port is None:
            continue
        port_map.setdefault(int(pid), []).append(int(port))
    return port_map


class Backend(Protocol):
    def list_processes(self) -> list[ProcessInfo]: ...
    def list_listening_ports(self) -> dict[int, list[int]]: ...
    def graceful_kill(self, pid: int) -> None: ...
    def force_kill(self, pid: int) -> None: ...
    def is_alive(self, pid: int) -> bool: ...


_PS_LIST_PROCESSES = (
    "@(Get-CimInstance Win32_Process | "
    "Select-Object ProcessId,ParentProcessId,Name,CommandLine) | ConvertTo-Json -Compress"
)
_PS_LIST_PORTS = (
    "@(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | "
    "Select-Object LocalPort,OwningProcess) | ConvertTo-Json -Compress"
)


def _run_powershell(script: str) -> str:
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout


class WindowsBackend:
    def list_processes(self) -> list[ProcessInfo]:
        return parse_processes_json(_run_powershell(_PS_LIST_PROCESSES))

    def list_listening_ports(self) -> dict[int, list[int]]:
        return parse_ports_json(_run_powershell(_PS_LIST_PORTS))

    def graceful_kill(self, pid: int) -> None:
        subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True, check=False)

    def force_kill(self, pid: int) -> None:
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, check=False
        )

    def is_alive(self, pid: int) -> bool:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return str(pid) in result.stdout


def get_backend() -> Backend:
    if platform.system() != "Windows":
        raise SystemExit("local_test_server_manager currently only supports Windows.")
    return WindowsBackend()


def kill_instance(backend: Backend, instance: Instance, wait_seconds: float = 2.0) -> bool:
    backend.graceful_kill(instance.root_pid)
    time.sleep(wait_seconds)
    if backend.is_alive(instance.root_pid):
        backend.force_kill(instance.root_pid)
        time.sleep(0.5)
    return not backend.is_alive(instance.root_pid)


def resolve_selector(instances: list[Instance], selector: str) -> list[Instance]:
    if selector.lower() == "all":
        return list(instances)
    try:
        value = int(selector)
    except ValueError:
        return []
    by_id = {inst.id: inst for inst in instances}
    if value in by_id:
        return [by_id[value]]
    for inst in instances:
        if value in inst.ports:
            return [inst]
    return []
