from __future__ import annotations

from pathlib import Path

import pytest
from textual.widgets import Input, Select, Tree

from duplicate_finder.review_ui import DuplicateReviewApp, ScanParams
from duplicate_finder.similarity_engine import DuplicateGroup, FileRecord


def _write(root: Path, relative_path: str, size_bytes: int = 10) -> Path:
    file_path = root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"x" * size_bytes)
    return file_path


def _make_params(tmp_path: Path) -> ScanParams:
    return ScanParams(
        recursive=True,
        name_threshold=85.0,
        size_tolerance_percent=None,
        min_group_size=2,
        include_keywords=[],
        exclude_keywords=[],
        output_dir=tmp_path / "_duplicates",
    )


def _no_op_rescan(params: ScanParams) -> list[DuplicateGroup]:
    return []


def _make_two_file_group(tmp_path: Path) -> tuple[DuplicateGroup, Path, Path]:
    file_a = _write(tmp_path, "movie.mp4")
    file_b = _write(tmp_path, "movie_copy.mp4")
    group = DuplicateGroup(
        label="movie",
        files=[
            FileRecord(path=file_a, name="movie.mp4", size=10, category="video"),
            FileRecord(path=file_b, name="movie_copy.mp4", size=10, category="video"),
        ],
    )
    return group, file_a, file_b


@pytest.mark.asyncio
async def test_tree_populates_with_group_and_file_nodes(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        tree = app.query_one("#group-tree", Tree)
        group_nodes = list(tree.root.children)
        assert len(group_nodes) == 1
        assert "movie" in group_nodes[0].label.plain
        assert len(list(group_nodes[0].children)) == 2


@pytest.mark.asyncio
async def test_toggling_a_file_marks_group_resolved(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down", "down")  # root -> group -> first file leaf
        await pilot.press("enter")
        await pilot.pause()

        state = next(iter(app._states.values()))
        assert state.resolved is True
        assert state.keep == {1}


@pytest.mark.asyncio
async def test_bulk_discard_all_marks_every_file_discarded(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down")  # root -> group node
        await pilot.press("d")
        await pilot.pause()

        state = next(iter(app._states.values()))
        assert state.keep == set()
        assert state.resolved is True


@pytest.mark.asyncio
async def test_bulk_keep_all_after_discard_resets_group_to_unresolved(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down")  # group node
        await pilot.press("d")
        await pilot.press("k")
        await pilot.pause()

        state = next(iter(app._states.values()))
        assert state.keep == {0, 1}
        assert state.resolved is False


@pytest.mark.asyncio
async def test_commit_moves_discarded_files_to_output_dir(tmp_path):
    group, file_a, file_b = _make_two_file_group(tmp_path)
    params = _make_params(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=params, rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down", "down")  # first file leaf: movie.mp4
        await pilot.press("enter")  # discard movie.mp4, keep movie_copy.mp4
        await pilot.pause()

        await pilot.press("ctrl+s")
        await pilot.pause()
        await pilot.click("#confirm")
        await pilot.pause()

    assert not file_a.exists()
    assert (params.output_dir / "movie.mp4").exists()
    assert file_b.exists()
    assert app.moved_count == 1
    assert app.move_errors == []


@pytest.mark.asyncio
async def test_commit_does_nothing_for_unresolved_groups(tmp_path):
    group, file_a, file_b = _make_two_file_group(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=_no_op_rescan)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("ctrl+s")
        await pilot.pause()

    # No confirmation dialog to click through — the app should still be running
    # (commit is a no-op when nothing is resolved), so no files moved.
    assert file_a.exists()
    assert file_b.exists()


@pytest.mark.asyncio
async def test_commit_records_error_and_continues_when_a_move_fails(tmp_path, monkeypatch):
    group, file_a, file_b = _make_two_file_group(tmp_path)
    params = _make_params(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=params, rescan=_no_op_rescan)

    import shutil as shutil_module

    real_move = shutil_module.move

    def flaky_move(src, dst):
        if Path(src) == file_a:
            raise OSError("simulated permission error")
        return real_move(src, dst)

    monkeypatch.setattr("duplicate_finder.review_ui.shutil.move", flaky_move)

    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("down")  # root -> group node
        await pilot.press("d")  # discard both files in the group
        await pilot.pause()

        await pilot.press("ctrl+s")
        await pilot.pause()
        await pilot.click("#confirm")
        await pilot.pause()

    # file_a's move was made to fail; file_b's should still go through.
    assert file_a.exists()
    assert not file_b.exists()
    assert app.moved_count == 1
    assert len(app.move_errors) == 1
    assert "movie.mp4" in app.move_errors[0]


@pytest.mark.asyncio
async def test_settings_dialog_prefills_and_apply_triggers_rescan(tmp_path):
    group, file_a, file_b = _make_two_file_group(tmp_path)
    calls: list[ScanParams] = []

    def rescan(params: ScanParams) -> list[DuplicateGroup]:
        calls.append(params)
        return [group]

    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=rescan)

    # A taller virtual terminal than the 80x24 default: the settings dialog
    # stacks enough labels/inputs that its Apply/Cancel buttons render below
    # row 24 otherwise, which is out of Pilot's clickable region.
    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("f2")
        await pilot.pause()

        # app.query_one() only searches the app's default screen, not a
        # pushed modal screen, so widgets inside SettingsScreen must be
        # queried via the current screen instead.
        threshold_input = app.screen.query_one("#threshold", Input)
        assert threshold_input.value == "85.0"

        threshold_input.value = "50"
        await pilot.click("#apply")
        await pilot.pause()

        assert calls[-1].name_threshold == 50.0
        assert app.params.name_threshold == 50.0


@pytest.mark.asyncio
async def test_settings_cancel_leaves_params_unchanged(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    original_params = _make_params(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=original_params, rescan=_no_op_rescan)

    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("f2")
        await pilot.pause()
        await pilot.click("#cancel")
        await pilot.pause()

        assert app.params is original_params


@pytest.mark.asyncio
async def test_rescan_discards_uncommitted_decisions(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)

    def rescan(params: ScanParams) -> list[DuplicateGroup]:
        return [group]

    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=rescan)

    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("down", "down")
        await pilot.press("enter")
        await pilot.pause()
        assert next(iter(app._states.values())).resolved is True

        await pilot.press("f2")
        await pilot.pause()
        await pilot.click("#apply")
        await pilot.pause()

        state_after = next(iter(app._states.values()))
        assert state_after.resolved is False
        assert state_after.keep == {0, 1}


@pytest.mark.asyncio
async def test_settings_dialog_apply_reachable_at_80x24(tmp_path):
    """Regression test for the VerticalScroll/max-height fix in review_ui.tcss:
    at the common 80x24 terminal size the settings dialog's fields overflow
    the viewport, so Apply/Cancel must be reachable by scrolling the dialog
    rather than sitting off-screen and unclickable. If the dialog's outer
    container ever regresses to a plain, unbounded Vertical, scrolling it
    becomes a no-op and the click below fails with a Pilot OutOfBounds error.
    """
    group, _, _ = _make_two_file_group(tmp_path)
    calls: list[ScanParams] = []

    def rescan(params: ScanParams) -> list[DuplicateGroup]:
        calls.append(params)
        return [group]

    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=_make_params(tmp_path), rescan=rescan)

    async with app.run_test(size=(80, 24)) as pilot:
        await pilot.pause()
        await pilot.press("f2")
        await pilot.pause()

        # Simulate a user scrolling the dialog down to reach Apply, the same
        # way mouse-wheel/PageDown/End scrolling would.
        dialog = app.screen.query_one("#settings-dialog")
        dialog.scroll_end(animate=False)
        await pilot.pause()

        await pilot.click("#apply")
        await pilot.pause()

        assert calls, "Apply should have triggered a rescan"
        assert app.params.name_threshold == 85.0


@pytest.mark.asyncio
async def test_settings_apply_with_invalid_threshold_keeps_dialog_open(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)
    calls: list[ScanParams] = []

    def rescan(params: ScanParams) -> list[DuplicateGroup]:
        calls.append(params)
        return [group]

    original_params = _make_params(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=original_params, rescan=rescan)

    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("f2")
        await pilot.pause()

        threshold_input = app.screen.query_one("#threshold", Input)
        threshold_input.value = "not-a-number"

        await pilot.click("#apply")
        await pilot.pause()

        # Apply must not crash the app or dismiss the dialog on bad input --
        # the settings screen should still be open with the field queryable.
        assert app.screen.query_one("#threshold", Input) is threshold_input
        assert calls == []
        assert app.params is original_params


@pytest.mark.asyncio
async def test_settings_dialog_with_out_of_preset_min_group_size_does_not_crash(tmp_path):
    """Regression test: --min-group-size accepts any int >= 2, but the F2
    dialog's Select only lists presets 2-6. Textual's Select raises
    InvalidSelectValueError if constructed with a value outside its option
    list, which previously tore down the whole app on mount whenever the
    scan was started with e.g. --min-group-size 10.
    """
    group, _, _ = _make_two_file_group(tmp_path)
    params = _make_params(tmp_path)
    params.min_group_size = 10
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=params, rescan=_no_op_rescan)

    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("f2")
        await pilot.pause()

        min_group_select = app.screen.query_one("#min-group-size", Select)
        assert min_group_select.value == "10"


@pytest.mark.asyncio
async def test_rescan_failure_notifies_and_preserves_existing_state(tmp_path):
    group, _, _ = _make_two_file_group(tmp_path)

    def raising_rescan(params: ScanParams) -> list[DuplicateGroup]:
        raise OSError("simulated scan failure")

    original_params = _make_params(tmp_path)
    app = DuplicateReviewApp(root=tmp_path, groups=[group], params=original_params, rescan=raising_rescan)

    async with app.run_test(size=(80, 50)) as pilot:
        await pilot.pause()
        await pilot.press("down", "down")  # root -> group -> first file leaf
        await pilot.press("enter")
        await pilot.pause()

        state_before = next(iter(app._states.values()))
        assert state_before.resolved is True

        await pilot.press("f2")
        await pilot.pause()
        await pilot.click("#apply")
        await pilot.pause()

        # The app must still be running with the prior review session intact
        # rather than crashing out on the OSError from the failed rescan.
        assert app.params is original_params
        state_after = next(iter(app._states.values()))
        assert state_after is state_before
        assert state_after.resolved is True
        tree = app.query_one("#group-tree", Tree)
        assert len(list(tree.root.children)) == 1
