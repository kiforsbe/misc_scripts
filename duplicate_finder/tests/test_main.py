from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from duplicate_finder.main import build_arg_parser, main

# duplicate_finder/__init__.py does `from .main import main`, which rebinds the
# `main` attribute on the `duplicate_finder` package to the function itself
# (shadowing the submodule). `importlib.import_module` reads straight from
# sys.modules, so it reliably returns the actual `duplicate_finder.main`
# module object to monkeypatch attributes on, regardless of that shadowing.
main_module = importlib.import_module("duplicate_finder.main")


def _write(root: Path, relative_path: str, size_bytes: int = 10) -> Path:
    file_path = root / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"x" * size_bytes)
    return file_path


def test_build_arg_parser_defaults():
    parser = build_arg_parser()
    args = parser.parse_args(["some/folder"])

    assert args.recursive is True
    assert args.name_threshold == 100.0
    assert args.size_tolerance_percent is None
    assert args.min_group_size == 2
    assert args.include_keyword == []
    assert args.exclude_keyword == []
    assert args.output_dir is None
    assert args.dry_run is False


def test_main_errors_on_missing_root(tmp_path):
    missing = tmp_path / "does-not-exist"

    with pytest.raises(SystemExit) as exc_info:
        main([str(missing)])

    assert exc_info.value.code != 0


def test_main_errors_on_min_group_size_below_two(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        main([str(tmp_path), "--min-group-size", "1"])

    assert exc_info.value.code != 0


def test_main_reports_no_duplicates_found(tmp_path, capsys):
    _write(tmp_path, "solo.mp4")

    exit_code = main([str(tmp_path)])

    assert exit_code == 0
    assert "No likely duplicates found" in capsys.readouterr().out


def test_main_dry_run_prints_groups_without_launching_ui(tmp_path, capsys):
    _write(tmp_path, "Movie.mp4")
    _write(tmp_path, "Movie (Alt).mp4")

    exit_code = main([str(tmp_path), "--dry-run"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Movie.mp4" in output
    assert "Movie (Alt).mp4" in output


def test_main_applies_cli_exclude_keyword_flag_in_dry_run(tmp_path, capsys):
    _write(tmp_path, "movie.mp4")
    _write(tmp_path, "movie_sample.mp4")

    exit_code = main([str(tmp_path), "--dry-run", "--exclude-keyword", "sample"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "No likely duplicates found" in output


def test_main_summary_uses_post_rescan_output_dir(tmp_path, monkeypatch, capsys):
    """If the user changes the output dir via the F2 settings dialog mid-session,
    the final summary must name the folder files actually moved to (app.params
    .output_dir, which run_review's app tracks live), not the pre-launch local
    variable computed before the app ran.
    """
    _write(tmp_path, "Movie.mp4")
    _write(tmp_path, "Movie (Alt).mp4")

    default_output_dir = tmp_path / "_duplicates"
    changed_output_dir = tmp_path / "custom_output"

    class FakeApp:
        moved_count = 2
        move_errors: list[str] = []
        params = SimpleNamespace(output_dir=changed_output_dir)

    def fake_run_review(root, groups, params, rescan):
        return FakeApp()

    monkeypatch.setattr(main_module, "run_review", fake_run_review)

    exit_code = main([str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert str(changed_output_dir) in output
    assert str(default_output_dir) not in output
