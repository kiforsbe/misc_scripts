from pathlib import Path

from requirements_consistency_checker import (
    check_installed_versions,
    check_version_consistency,
    fix_undeclared_imports,
    fix_unused_declarations,
    fix_version_inconsistencies,
    run_checks,
    run_fix_command,
    update_root_requirements,
)


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


def test_orphaned_root_script_requirements_reported_as_failure(tmp_path):
    (tmp_path / "fakescript.requirements.txt").write_text("requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("# no reference to fakescript\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert not result.ok
    assert any("fakescript.requirements.txt" in msg and "orphan" in msg.lower() for msg in result.hard_failures)


def test_referenced_root_script_requirements_reports_nothing(tmp_path):
    (tmp_path / "fakescript.requirements.txt").write_text("# no third-party packages\n", encoding="utf-8")
    (tmp_path / "fakescript.py").write_text("print('hello')\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r fakescript.requirements.txt\n", encoding="utf-8")

    result = run_checks(tmp_path)

    assert result.ok
    assert not result.hard_failures


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


def test_update_adds_missing_and_removes_stale_references(tmp_path):
    tool_dir = tmp_path / "toolA"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tmp_path / "newscript.requirements.txt").write_text("tqdm\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r toolA/requirements.txt\n-r ghosttool/requirements.txt\n", encoding="utf-8"
    )

    changes = update_root_requirements(tmp_path)

    assert any("newscript.requirements.txt" in c and c.startswith("+") for c in changes)
    assert any("ghosttool/requirements.txt" in c and c.startswith("-") for c in changes)

    content = (tmp_path / "requirements.txt").read_text(encoding="utf-8")
    assert "-r newscript.requirements.txt" in content
    assert "ghosttool" not in content

    result = run_checks(tmp_path)
    assert result.ok


def test_update_reports_no_changes_when_already_consistent(tmp_path):
    tool_dir = tmp_path / "toolA"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r toolA/requirements.txt\n", encoding="utf-8")

    changes = update_root_requirements(tmp_path)

    assert changes == []


def test_fix_appends_recognized_undeclared_import(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("# faketool has no deps yet\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    changes = fix_undeclared_imports(tmp_path)

    assert any("requests" in c and "faketool" in c for c in changes)
    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "requests" in content

    result = run_checks(tmp_path)
    assert not any("requests" in msg for msg in result.warnings)


def test_fix_does_not_touch_unrecognized_import(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("# faketool has no deps yet\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import SomeWeirdModule\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    changes = fix_undeclared_imports(tmp_path)

    assert changes == []
    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "SomeWeirdModule" not in content

    result = run_checks(tmp_path)
    assert any("SomeWeirdModule" in msg and "unrecognized" in msg.lower() for msg in result.warnings)


def test_fix_does_not_remove_unused_declaration(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("print('hello')\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    fix_undeclared_imports(tmp_path)

    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "requests" in content

    result = run_checks(tmp_path)
    assert any("requests" in msg and "unused" in msg.lower() for msg in result.warnings)


def test_update_dry_run_reports_changes_without_writing(tmp_path):
    tool_dir = tmp_path / "toolA"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\n", encoding="utf-8")
    original_root_content = "-r ghosttool/requirements.txt\n"
    (tmp_path / "requirements.txt").write_text(original_root_content, encoding="utf-8")

    changes = update_root_requirements(tmp_path, dry_run=True)

    assert any("toolA/requirements.txt" in c and c.startswith("+") for c in changes)
    assert any("ghosttool/requirements.txt" in c and c.startswith("-") for c in changes)
    assert (tmp_path / "requirements.txt").read_text(encoding="utf-8") == original_root_content


def test_fix_dry_run_reports_changes_without_writing(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    original_requirements_content = "# faketool has no deps yet\n"
    (tool_dir / "requirements.txt").write_text(original_requirements_content, encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    changes = fix_undeclared_imports(tmp_path, dry_run=True)

    assert any("requests" in c and "faketool" in c for c in changes)
    assert (tool_dir / "requirements.txt").read_text(encoding="utf-8") == original_requirements_content

    result = run_checks(tmp_path)
    assert any("requests" in msg for msg in result.warnings)


def _make_unused_declaration_scenario(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("requests\ntqdm\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")
    return tool_dir


def test_fix_unused_declarations_removes_unused_package(tmp_path):
    tool_dir = _make_unused_declaration_scenario(tmp_path)

    changes = fix_unused_declarations(tmp_path)

    assert any("tqdm" in c and "faketool" in c and c.startswith("-") for c in changes)
    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "tqdm" not in content
    assert "requests" in content

    result = run_checks(tmp_path)
    assert not any("tqdm" in msg for msg in result.warnings)


def test_fix_unused_declarations_dry_run_does_not_write(tmp_path):
    tool_dir = _make_unused_declaration_scenario(tmp_path)
    original_content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")

    changes = fix_unused_declarations(tmp_path, dry_run=True)

    assert any("tqdm" in c for c in changes)
    assert (tool_dir / "requirements.txt").read_text(encoding="utf-8") == original_content


def test_run_fix_command_default_level_does_not_touch_unused_declarations(tmp_path):
    tool_dir = _make_unused_declaration_scenario(tmp_path)

    run_fix_command(tmp_path, use_color=False)

    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "tqdm" in content


def test_run_fix_command_level_all_removes_unused_declarations(tmp_path):
    tool_dir = _make_unused_declaration_scenario(tmp_path)

    run_fix_command(tmp_path, use_color=False, types={"root", "undeclared", "unused"})

    content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "tqdm" not in content


def test_run_fix_command_types_override_restricts_to_named_types(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("tqdm\n", encoding="utf-8")
    (tool_dir / "faketool.py").write_text("import requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r faketool/requirements.txt\n-r ghosttool/requirements.txt\n", encoding="utf-8"
    )

    run_fix_command(tmp_path, use_color=False, types={"unused"})

    root_content = (tmp_path / "requirements.txt").read_text(encoding="utf-8")
    assert "ghosttool" in root_content  # 'root' type not selected, stale ref untouched

    tool_content = (tool_dir / "requirements.txt").read_text(encoding="utf-8")
    assert "requests" not in tool_content  # 'undeclared' type not selected
    assert "tqdm" not in tool_content  # 'unused' type selected, so this was removed


def _make_shared_module_scenario(tmp_path, declare_requests: bool):
    common_dir = tmp_path / "common"
    common_dir.mkdir()
    (common_dir / "presentation.py").write_text("import requests\n", encoding="utf-8")
    (common_dir / "unrelated.py").write_text("import somelib\n", encoding="utf-8")

    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    requirements_content = "requests\n" if declare_requests else "# faketool has no deps yet\n"
    (tool_dir / "requirements.txt").write_text(requirements_content, encoding="utf-8")
    (tool_dir / "faketool.py").write_text("from common.presentation import X\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")
    return tool_dir


def test_recursive_check_flags_undeclared_import_pulled_in_via_shared_module(tmp_path):
    _make_shared_module_scenario(tmp_path, declare_requests=False)

    result = run_checks(tmp_path)

    assert any(
        "requests" in msg and "common/presentation.py" in msg and "faketool/requirements.txt" in msg
        for msg in result.warnings
    )


def test_recursive_check_does_not_follow_shared_submodule_the_tool_never_imports(tmp_path):
    _make_shared_module_scenario(tmp_path, declare_requests=False)

    result = run_checks(tmp_path)

    assert not any("somelib" in msg for msg in result.warnings)


def test_no_recursive_check_ignores_imports_inside_shared_modules(tmp_path):
    _make_shared_module_scenario(tmp_path, declare_requests=False)

    result = run_checks(tmp_path, recursive=False)

    assert not any("requests" in msg for msg in result.warnings)


def test_recursive_check_treats_shared_module_import_as_satisfying_declaration(tmp_path):
    _make_shared_module_scenario(tmp_path, declare_requests=True)

    result = run_checks(tmp_path)

    assert not any("requests" in msg and "unused" in msg.lower() for msg in result.warnings)


def test_no_recursive_check_reports_declaration_unused_when_only_used_via_shared_module(tmp_path):
    _make_shared_module_scenario(tmp_path, declare_requests=True)

    result = run_checks(tmp_path, recursive=False)

    assert any("requests" in msg and "unused" in msg.lower() for msg in result.warnings)


def test_version_consistency_flags_conflicting_pins_across_files(tmp_path):
    tool_a = tmp_path / "toolA"
    tool_a.mkdir()
    (tool_a / "requirements.txt").write_text("requests>=2.28\n", encoding="utf-8")
    tool_b = tmp_path / "toolB"
    tool_b.mkdir()
    (tool_b / "requirements.txt").write_text("requests==2.20.0\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r toolA/requirements.txt\n-r toolB/requirements.txt\n", encoding="utf-8"
    )

    failures = check_version_consistency(tmp_path)

    assert any(
        "requests" in f and ">=2.28" in f and "==2.20.0" in f and "toolA/requirements.txt" in f
        and "toolB/requirements.txt" in f
        for f in failures
    )

    result = run_checks(tmp_path)
    assert not result.ok
    assert any("requests" in f for f in result.hard_failures)


def test_version_consistency_ignores_matching_pins(tmp_path):
    tool_a = tmp_path / "toolA"
    tool_a.mkdir()
    (tool_a / "requirements.txt").write_text("requests>=2.28\n", encoding="utf-8")
    tool_b = tmp_path / "toolB"
    tool_b.mkdir()
    (tool_b / "requirements.txt").write_text("requests>=2.28\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r toolA/requirements.txt\n-r toolB/requirements.txt\n", encoding="utf-8"
    )

    assert check_version_consistency(tmp_path) == []


def test_version_consistency_ignores_unpinned_declarations(tmp_path):
    tool_a = tmp_path / "toolA"
    tool_a.mkdir()
    (tool_a / "requirements.txt").write_text("requests>=2.28\n", encoding="utf-8")
    tool_b = tmp_path / "toolB"
    tool_b.mkdir()
    (tool_b / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r toolA/requirements.txt\n-r toolB/requirements.txt\n", encoding="utf-8"
    )

    assert check_version_consistency(tmp_path) == []


def test_installed_version_check_flags_unsatisfied_pin_for_installed_package(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    # pytest is guaranteed installed (it's running this test); pin an
    # impossible version so the mismatch is unambiguous.
    (tool_dir / "requirements.txt").write_text("pytest<1.0\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    warnings = check_installed_versions(tmp_path)

    assert any("pytest" in w and "faketool/requirements.txt" in w for w in warnings)


def test_installed_version_check_is_silent_when_pin_is_satisfied(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text("pytest>=1.0\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    assert check_installed_versions(tmp_path) == []


def test_installed_version_check_is_silent_for_package_not_installed(tmp_path):
    tool_dir = tmp_path / "faketool"
    tool_dir.mkdir()
    (tool_dir / "requirements.txt").write_text(
        "totally-fake-package-that-does-not-exist-xyz>=1.0\n", encoding="utf-8"
    )
    (tmp_path / "requirements.txt").write_text("-r faketool/requirements.txt\n", encoding="utf-8")

    assert check_installed_versions(tmp_path) == []


def _make_version_conflict_scenario(tmp_path):
    tool_a = tmp_path / "toolA"
    tool_a.mkdir()
    (tool_a / "requirements.txt").write_text("mutagen>=1.45.0\n", encoding="utf-8")
    tool_b = tmp_path / "toolB"
    tool_b.mkdir()
    (tool_b / "requirements.txt").write_text("mutagen>=1.45.1\n", encoding="utf-8")
    (tmp_path / "requirements.txt").write_text(
        "-r toolA/requirements.txt\n-r toolB/requirements.txt\n", encoding="utf-8"
    )
    return tool_a, tool_b


def test_fix_version_inconsistencies_heuristic_picks_highest_version(tmp_path):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)

    changes = fix_version_inconsistencies(tmp_path)

    assert any("mutagen>=1.45.1" in c and "toolA/requirements.txt" in c and "1.45.0" in c for c in changes)
    assert (tool_a / "requirements.txt").read_text(encoding="utf-8") == "mutagen>=1.45.1\n"
    assert (tool_b / "requirements.txt").read_text(encoding="utf-8") == "mutagen>=1.45.1\n"

    result = run_checks(tmp_path)
    assert result.ok
    assert not any("mutagen" in f for f in result.hard_failures)


def test_fix_version_inconsistencies_dry_run_does_not_write(tmp_path):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)
    original_a = (tool_a / "requirements.txt").read_text(encoding="utf-8")

    changes = fix_version_inconsistencies(tmp_path, dry_run=True)

    assert any("mutagen>=1.45.1" in c for c in changes)
    assert (tool_a / "requirements.txt").read_text(encoding="utf-8") == original_a
    assert not check_version_consistency(tmp_path) == []


def test_run_fix_command_default_level_resolves_version_conflicts(tmp_path):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)

    exit_code = run_fix_command(tmp_path, use_color=False)

    assert exit_code == 0
    assert (tool_a / "requirements.txt").read_text(encoding="utf-8") == "mutagen>=1.45.1\n"


def test_fix_version_inconsistencies_interactive_uses_prompted_choice(tmp_path, monkeypatch):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "1")  # picks the first listed spec

    changes = fix_version_inconsistencies(tmp_path, interactive=True)

    assert len(changes) == 1
    winning_content = (tool_a / "requirements.txt").read_text(encoding="utf-8")
    assert winning_content == (tool_b / "requirements.txt").read_text(encoding="utf-8")
    assert winning_content in ("mutagen>=1.45.0\n", "mutagen>=1.45.1\n")


def test_fix_version_inconsistencies_interactive_skip_leaves_conflict_unresolved(tmp_path, monkeypatch):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)
    original_a = (tool_a / "requirements.txt").read_text(encoding="utf-8")
    original_b = (tool_b / "requirements.txt").read_text(encoding="utf-8")
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "s")

    changes = fix_version_inconsistencies(tmp_path, interactive=True)

    assert changes == []
    assert (tool_a / "requirements.txt").read_text(encoding="utf-8") == original_a
    assert (tool_b / "requirements.txt").read_text(encoding="utf-8") == original_b
    assert not check_version_consistency(tmp_path) == []


def test_fix_version_inconsistencies_interactive_falls_back_to_heuristic_when_not_a_tty(tmp_path, monkeypatch):
    tool_a, tool_b = _make_version_conflict_scenario(tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    def _unexpected_input(prompt):
        raise AssertionError("input() should never be called when stdin is not a tty")

    monkeypatch.setattr("builtins.input", _unexpected_input)

    changes = fix_version_inconsistencies(tmp_path, interactive=True)

    assert any("mutagen>=1.45.1" in c for c in changes)
    assert (tool_a / "requirements.txt").read_text(encoding="utf-8") == "mutagen>=1.45.1\n"
