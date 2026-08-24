from __future__ import annotations

from pathlib import Path

import pytest
from textual.widgets import Tree

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
