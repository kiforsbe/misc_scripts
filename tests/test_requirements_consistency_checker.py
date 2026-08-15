from pathlib import Path

from requirements_consistency_checker import run_checks


def test_orphaned_tool_requirements_reported_as_failure(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("# no reference to faketool\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert not result.ok
    assert any("faketool" in msg and "orphan" in msg.lower() for msg in result.hard_failures)


def test_stale_root_reference_reported_as_failure(tmp_path):
    (tmp_path / "requirements.txt").write_text("-r ghosttool/requirements.txt\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert not result.ok
    assert any("ghosttool" in msg for msg in result.hard_failures)


def test_undeclared_third_party_import_reported_as_warning(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("# faketool has no deps yet\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert result.ok
    assert any("requests" in msg for msg in result.warnings)


def test_unused_declaration_reported_as_warning(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("print('hello')\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert result.ok
    assert any("requests" in msg and "unused" in msg.lower() for msg in result.warnings)


def test_consistent_tool_reports_nothing(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert result.ok
    assert not result.warnings
