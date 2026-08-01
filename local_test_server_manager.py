from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass


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
