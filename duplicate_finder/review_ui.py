"""Interactive DOS-styled review UI for duplicate-file groups.

Key Tree-widget facts this module relies on (verified against the installed
textual version during planning, not assumed): `Space` is Tree's own
expand/collapse binding, so per-file toggling is wired through the
`on_tree_node_selected` message handler (fires on Enter or click) instead of
a competing keybinding. Labels are built as `rich.text.Text` objects, not raw
strings, so literal `[x]`/`[ ]` prefixes render as-is instead of being parsed
as Rich console markup.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Header, Input, Label, Select, Switch, Tree

from .similarity_engine import DuplicateGroup, FileRecord

RescanFn = Callable[["ScanParams"], list[DuplicateGroup]]


def _human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024


def _unique_target(target: Path) -> Path:
    if not target.exists():
        return target
    stem, suffix = target.stem, target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem} ({counter}){suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


@dataclass
class ScanParams:
    recursive: bool
    name_threshold: float
    size_tolerance_percent: float | None
    min_group_size: int
    include_keywords: list[str]
    exclude_keywords: list[str]
    output_dir: Path


@dataclass
class _GroupState:
    group: DuplicateGroup
    keep: set[int] = field(default_factory=set)
    touched: bool = False

    def __post_init__(self) -> None:
        self.keep = set(range(len(self.group.files)))

    @property
    def resolved(self) -> bool:
        return self.touched and len(self.keep) < len(self.group.files)

    def status_text(self) -> str:
        if not self.touched:
            return "unreviewed"
        if len(self.keep) == len(self.group.files):
            return "keep all"
        if not self.keep:
            return "discard all"
        return "resolved"

    def discarded_files(self) -> list[FileRecord]:
        return [f for i, f in enumerate(self.group.files) if i not in self.keep]


class CommitConfirmScreen(ModalScreen[bool]):
    def __init__(self, resolved_states: list[_GroupState], output_dir: Path):
        super().__init__()
        self._resolved_states = resolved_states
        self._output_dir = output_dir

    def compose(self) -> ComposeResult:
        total_files = sum(len(s.discarded_files()) for s in self._resolved_states)
        total_bytes = sum(f.size for s in self._resolved_states for f in s.discarded_files())
        with Vertical(id="commit-dialog"):
            yield Label(
                f"{total_files} file(s) across {len(self._resolved_states)} group(s) "
                f"will be moved to {self._output_dir}, totalling {_human_size(total_bytes)}."
            )
            yield Button("Confirm", id="confirm", variant="primary")
            yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "confirm")


class SettingsScreen(ModalScreen[ScanParams | None]):
    SIZE_TOLERANCE_PRESETS = ["Disabled", "5%", "10%", "15%", "20%", "25%", "Custom..."]
    MIN_GROUP_SIZE_PRESETS = ["2", "3", "4", "5", "6"]

    def __init__(self, params: ScanParams):
        super().__init__()
        self._params = params

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="settings-dialog"):
            yield Label("Rescan settings (rescanning discards unsaved keep/discard choices)")
            yield Label("Recursive")
            yield Switch(value=self._params.recursive, id="recursive")
            yield Label("Name similarity threshold (0-100)")
            yield Input(value=str(self._params.name_threshold), id="threshold")
            yield Label("Size tolerance")
            yield Select(
                [(preset, preset) for preset in self.SIZE_TOLERANCE_PRESETS],
                value=self._initial_tolerance_option(),
                id="tolerance-select",
            )
            yield Input(
                value=self._initial_tolerance_custom_value(),
                placeholder="custom %",
                id="tolerance-custom",
            )
            yield Label("Minimum group size")
            yield Select(
                [(preset, preset) for preset in self._min_group_size_options()],
                value=str(self._params.min_group_size),
                id="min-group-size",
            )
            yield Label("Include keywords (comma-separated)")
            yield Input(value=", ".join(self._params.include_keywords), id="include-keywords")
            yield Label("Exclude keywords (comma-separated)")
            yield Input(value=", ".join(self._params.exclude_keywords), id="exclude-keywords")
            yield Label("Output directory")
            yield Input(value=str(self._params.output_dir), id="output-dir")
            with Vertical():
                yield Button("Apply && Rescan", id="apply", variant="primary")
                yield Button("Cancel", id="cancel")

    def _initial_tolerance_option(self) -> str:
        percent = self._params.size_tolerance_percent
        if percent is None:
            return "Disabled"
        preset = f"{percent:g}%"
        return preset if preset in self.SIZE_TOLERANCE_PRESETS else "Custom..."

    def _initial_tolerance_custom_value(self) -> str:
        percent = self._params.size_tolerance_percent
        if percent is None:
            return ""
        preset = f"{percent:g}%"
        return "" if preset in self.SIZE_TOLERANCE_PRESETS else str(percent)

    def _min_group_size_options(self) -> list[str]:
        # --min-group-size accepts any int >= 2 on the CLI, but the dialog
        # only presets 2-6. If the scan was started outside that range,
        # constructing Select with a value missing from its option list
        # raises InvalidSelectValueError and tears down the whole app, so
        # the current value is unioned in as an extra option instead of
        # being silently clamped or dropped.
        current = str(self._params.min_group_size)
        if current in self.MIN_GROUP_SIZE_PRESETS:
            return self.MIN_GROUP_SIZE_PRESETS
        return [*self.MIN_GROUP_SIZE_PRESETS, current]

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return
        try:
            params = self._collect_params()
        except ValueError as exc:
            self.notify(str(exc), severity="error")
            return
        self.dismiss(params)

    def _keywords_from(self, widget_id: str) -> list[str]:
        raw = self.query_one(f"#{widget_id}", Input).value
        return [keyword.strip() for keyword in raw.split(",") if keyword.strip()]

    def _parse_float_field(self, widget_id: str, field_label: str) -> float:
        raw = self.query_one(f"#{widget_id}", Input).value
        try:
            return float(raw)
        except ValueError:
            raise ValueError(f"{field_label} must be a number (got {raw!r})") from None

    def _collect_params(self) -> ScanParams:
        tolerance_choice = self.query_one("#tolerance-select", Select).value
        if tolerance_choice == "Disabled":
            tolerance = None
        elif tolerance_choice == "Custom...":
            tolerance = self._parse_float_field("tolerance-custom", "Custom size tolerance")
        else:
            tolerance = float(str(tolerance_choice).rstrip("%"))

        return ScanParams(
            recursive=self.query_one("#recursive", Switch).value,
            name_threshold=self._parse_float_field("threshold", "Name similarity threshold"),
            size_tolerance_percent=tolerance,
            min_group_size=int(str(self.query_one("#min-group-size", Select).value)),
            include_keywords=self._keywords_from("include-keywords"),
            exclude_keywords=self._keywords_from("exclude-keywords"),
            output_dir=Path(self.query_one("#output-dir", Input).value),
        )


class DuplicateReviewApp(App):
    CSS_PATH = "review_ui.tcss"
    BINDINGS = [
        ("f2", "open_settings", "Settings"),
        ("ctrl+s", "commit", "Commit"),
        ("k", "keep_all_in_group", "Keep all in group"),
        ("d", "discard_all_in_group", "Discard all in group"),
        ("q", "quit", "Quit"),
    ]

    def __init__(
        self,
        root: Path,
        groups: list[DuplicateGroup],
        params: ScanParams,
        rescan: RescanFn,
    ):
        super().__init__()
        self._root = root
        self.params = params
        self._rescan = rescan
        self._states: dict[str, _GroupState] = {}
        self._load_groups(groups)
        self.moved_count = 0
        self.move_errors: list[str] = []

    def _load_groups(self, groups: list[DuplicateGroup]) -> None:
        self._states = {f"group-{i}": _GroupState(group=group) for i, group in enumerate(groups)}

    def compose(self) -> ComposeResult:
        yield Header()
        yield Tree("Duplicate groups", id="group-tree")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = "Enter: toggle keep/discard"
        self._rebuild_tree()

    def _rebuild_tree(self) -> None:
        tree = self.query_one("#group-tree", Tree)
        tree.clear()
        tree.root.expand()
        for key, state in self._states.items():
            group_node = tree.root.add(self._group_label(state), data={"kind": "group", "key": key}, expand=True)
            for index in range(len(state.group.files)):
                group_node.add_leaf(
                    self._file_label(state, index),
                    data={"kind": "file", "key": key, "index": index},
                )

    def _group_label(self, state: _GroupState) -> Text:
        return Text(f"{state.group.label} ({len(state.group.files)} files) [{state.status_text()}]")

    def _file_label(self, state: _GroupState, index: int) -> Text:
        file = state.group.files[index]
        mark = "x" if index in state.keep else " "
        try:
            relative = file.path.resolve().relative_to(self._root.resolve())
        except ValueError:
            relative = file.path
        return Text(f"[{mark}] {file.name}  {_human_size(file.size)}  {relative}")

    def _current_group_key(self) -> str | None:
        tree = self.query_one("#group-tree", Tree)
        node = tree.cursor_node
        if node is None or node.data is None:
            return None
        return node.data.get("key")

    def _refresh_group(self, key: str) -> None:
        tree = self.query_one("#group-tree", Tree)
        state = self._states[key]
        for node in tree.root.children:
            if node.data and node.data.get("key") == key:
                node.set_label(self._group_label(state))
                for index, child in enumerate(node.children):
                    child.set_label(self._file_label(state, index))
                break

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if node.data is None or node.data.get("kind") != "file":
            return
        key, index = node.data["key"], node.data["index"]
        state = self._states[key]
        if index in state.keep:
            state.keep.discard(index)
        else:
            state.keep.add(index)
        state.touched = True
        self._refresh_group(key)

    def action_keep_all_in_group(self) -> None:
        key = self._current_group_key()
        if key is None:
            return
        state = self._states[key]
        state.keep = set(range(len(state.group.files)))
        state.touched = True
        self._refresh_group(key)

    def action_discard_all_in_group(self) -> None:
        key = self._current_group_key()
        if key is None:
            return
        state = self._states[key]
        state.keep = set()
        state.touched = True
        self._refresh_group(key)

    def action_open_settings(self) -> None:
        self.push_screen(SettingsScreen(self.params), self._handle_settings_result)

    def _handle_settings_result(self, new_params: ScanParams | None) -> None:
        if new_params is None:
            return
        try:
            new_groups = self._rescan(new_params)
        except (OSError, ValueError) as exc:
            self.notify(f"Rescan failed: {exc}", severity="error")
            return
        self.params = new_params
        self._load_groups(new_groups)
        self._rebuild_tree()
        if not new_groups:
            self.notify("Rescan found no duplicate groups.")

    def action_commit(self) -> None:
        resolved = [state for state in self._states.values() if state.resolved]
        if not resolved:
            self.bell()
            self.notify("No reviewed groups to commit.")
            return
        self.push_screen(CommitConfirmScreen(resolved, self.params.output_dir), self._handle_commit_result)

    def _handle_commit_result(self, confirmed: bool | None) -> None:
        if not confirmed:
            return
        resolved = [state for state in self._states.values() if state.resolved]
        for state in resolved:
            for file in state.discarded_files():
                try:
                    relative = file.path.resolve().relative_to(self._root.resolve())
                except ValueError:
                    relative = Path(file.name)
                target = _unique_target(self.params.output_dir / relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(file.path), str(target))
                    self.moved_count += 1
                except OSError as exc:
                    self.move_errors.append(f"{file.path}: {exc}")
        self.exit()


def run_review(
    root: Path,
    groups: list[DuplicateGroup],
    params: ScanParams,
    rescan: RescanFn,
) -> DuplicateReviewApp:
    app = DuplicateReviewApp(root=root, groups=groups, params=params, rescan=rescan)
    app.run()
    return app
