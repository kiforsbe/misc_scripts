"""Checks that per-tool, per-script, and root requirements.txt files stay in sync.

Usage:
  python requirements_consistency_checker.py check [--repo-root PATH] [--color|--no-color]
  python requirements_consistency_checker.py update [--repo-root PATH] [--color|--no-color] [--dry-run]
  python requirements_consistency_checker.py fix [--repo-root PATH] [--color|--no-color] [--dry-run]
      [--level {error,warning,all}] [--types root undeclared unused ...]

`check` (the default when no subcommand is given) exits non-zero if any
tool's requirements.txt or root-level script's <script>.requirements.txt is
orphaned (not referenced by the root requirements.txt) or if the root file
references a requirements file that no longer exists. Undeclared-import and
unused-declaration findings are best-effort static analysis and are only
ever reported as warnings. `check` never writes anything, so it has no
--dry-run option.

`update` rewrites the root requirements.txt to add '-r' lines for orphaned
requirements files and remove '-r' lines that point at files that no longer
exist. It never modifies per-tool or per-script requirements.txt content.

`fix` resolves issues found by `check`, gated by --level (default: warning):
  - error:   only the root requirements.txt '-r' sync (same as `update`).
  - warning: also appends the missing package for each "recognized"
             undeclared import (a known import-name-to-package mapping, or
             a lowercase module name that looks like a real package name).
  - all:     also removes "Unused declaration" findings.
--types overrides --level with an exact set from {root, undeclared, unused}
(e.g. '--types unused' to fix only unused declarations). `fix` never
guesses at ambiguous "Unrecognized import" findings at any level/type --
those always require manual review. Removing an "Unused declaration" is
the riskiest fix, since it may really be an indirect or subprocess
dependency that static analysis can't see; it only happens at --level all
or with 'unused' explicitly named in --types. After applying fixes, `fix`
re-checks and reports any remaining warnings that still need manual review.

`update` and `fix` both accept --dry-run: it prints exactly what would be
changed without writing any file, and (for `fix`) skips the post-fix
remaining-warnings re-check, since nothing was actually written.
"""
import argparse
import ast
import re
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from common.presentation import Colors

# Import-name -> PyPI-package-name mismatches known ahead of time.
IMPORT_TO_PACKAGE = {
    "PIL": "pillow",
    "bs4": "beautifulsoup4",
    "cv2": "opencv-python",
    "yaml": "PyYAML",
    "eyed3": "eyeD3",
    "magic": "python-magic-bin",
    "libarchive": "libarchive-c",
    "docx": "python-docx",
    "ffmpeg": "ffmpeg-python",
}

STDLIB_MODULES = set(getattr(sys, "stdlib_module_names", ())) | {"__future__"}

# Modules that are typing-only stubs or otherwise never resolve to a pip install.
IGNORED_MODULES = {"_typeshed"}

# Repo-internal modules imported across a folder boundary (via a sys.path bootstrap)
# rather than being a sibling file/package of the importing tool. Not PyPI packages.
KNOWN_LOCAL_MODULES = set()

# Issue types the 'fix' command knows how to resolve, from safest to riskiest.
#   root       - root requirements.txt '-r' line sync (orphaned/stale references)
#   undeclared - append the package for a 'recognized' undeclared import
#   unused     - remove an 'Unused declaration' (never guessed, always risky)
FIX_TYPES = ("root", "undeclared", "unused")

# --level shorthand: which FIX_TYPES each tier enables by default.
FIX_LEVEL_TYPES = {
    "error": {"root"},
    "warning": {"root", "undeclared"},
    "all": {"root", "undeclared", "unused"},
}


@dataclass
class CheckResult:
    hard_failures: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.hard_failures


def find_tool_requirements_files(repo_root: Path) -> list[Path]:
    """Every requirements.txt strictly below repo_root (excludes the root aggregator)."""
    return sorted(
        p for p in repo_root.rglob("requirements.txt")
        if p.parent != repo_root and ".git" not in p.parts
    )


def find_root_script_requirements_files(repo_root: Path) -> list[Path]:
    """Every root-level standalone script's own <script>.requirements.txt."""
    return sorted(repo_root.glob("*.requirements.txt"))


def find_tool_dirs(repo_root: Path) -> list[Path]:
    return sorted({p.parent for p in find_tool_requirements_files(repo_root)})


def parse_referenced_paths(root_requirements_path: Path) -> list[str]:
    if not root_requirements_path.exists():
        return []
    referenced = []
    for line in root_requirements_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("-r "):
            referenced.append(stripped[3:].strip())
    return referenced


def check_orphaned_and_stale(repo_root: Path) -> tuple[list[str], list[str]]:
    root_requirements_path = repo_root / "requirements.txt"
    referenced = parse_referenced_paths(root_requirements_path)
    referenced_abs = {(repo_root / r).resolve() for r in referenced}

    all_requirements = find_tool_requirements_files(repo_root) + find_root_script_requirements_files(repo_root)
    all_requirements_abs = {p.resolve() for p in all_requirements}

    orphaned = []
    for req in all_requirements:
        if req.resolve() not in referenced_abs:
            rel = req.relative_to(repo_root)
            orphaned.append(
                f"Orphaned requirements file: '{rel}' exists but is not referenced "
                f"by a '-r' line in the root requirements.txt"
            )

    stale = []
    for ref in referenced:
        if (repo_root / ref).resolve() not in all_requirements_abs:
            stale.append(
                f"Stale root reference: requirements.txt references '-r {ref}' "
                f"but that file does not exist"
            )

    return orphaned, stale


def _is_root_script_reference(ref: str) -> bool:
    """True for a bare '<script>.requirements.txt' reference (no path separator)."""
    return "/" not in ref and "\\" not in ref


def update_root_requirements(repo_root: Path, dry_run: bool = False) -> list[str]:
    """Rewrites the root requirements.txt: adds '-r' lines for orphaned
    requirements files and removes '-r' lines whose target no longer
    exists. Never touches per-tool/script requirements.txt content.

    With dry_run=True, computes the same changes but never writes the file.

    Returns a list of human-readable change descriptions ('+ -r ...' for
    additions, '- -r ...' for removals); empty if nothing changed.
    """
    root_requirements_path = repo_root / "requirements.txt"
    referenced = parse_referenced_paths(root_requirements_path)

    all_requirements = find_tool_requirements_files(repo_root) + find_root_script_requirements_files(repo_root)
    all_requirements_abs = {p.resolve() for p in all_requirements}
    all_refs = {p.relative_to(repo_root).as_posix() for p in all_requirements}

    stale_refs = [r for r in referenced if (repo_root / r).resolve() not in all_requirements_abs]
    missing_refs = sorted(all_refs - set(referenced))

    if not stale_refs and not missing_refs:
        return []

    lines = root_requirements_path.read_text(encoding="utf-8").splitlines() if root_requirements_path.exists() else []
    stale_set = set(stale_refs)
    lines = [
        line for line in lines
        if not (line.strip().startswith("-r ") and line.strip()[3:].strip() in stale_set)
    ]

    missing_tool_refs = sorted(r for r in missing_refs if not _is_root_script_reference(r))
    missing_script_refs = sorted(r for r in missing_refs if _is_root_script_reference(r))

    def insert_missing(lines: list[str], missing: list[str], is_script: bool, header: str) -> list[str]:
        if not missing:
            return lines
        last_idx = -1
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("-r ") and _is_root_script_reference(stripped[3:].strip()) == is_script:
                last_idx = i
        new_lines = [f"-r {ref}" for ref in missing]
        if last_idx >= 0:
            return lines[:last_idx + 1] + new_lines + lines[last_idx + 1:]
        return lines + ["", header] + new_lines

    lines = insert_missing(
        lines, missing_tool_refs, is_script=False,
        header="# Tool folders each own their own requirements.txt.",
    )
    lines = insert_missing(
        lines, missing_script_refs, is_script=True,
        header="# Root-level standalone scripts each own their own <script>.requirements.txt.",
    )

    if not dry_run:
        root_requirements_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    changes = [f"+ -r {ref}" for ref in missing_refs]
    changes += [f"- -r {ref}" for ref in stale_refs]
    return changes


def extract_top_level_imports(py_file: Path) -> set[str]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError, OSError):
        return set()

    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                modules.add(node.module.split(".")[0])
    return modules


def local_sibling_modules(tool_dir: Path) -> set[str]:
    """Module/package names that resolve to something local inside this tool's own folder tree."""
    names = {p.stem for p in tool_dir.rglob("*.py")}
    names |= {p.parent.name for p in tool_dir.rglob("__init__.py")}
    names.add(tool_dir.name)
    return names


def _line_declared_package(line: str) -> str | None:
    """The package name on one requirements.txt line, or None for
    blank/comment/'-r' lines."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or stripped.startswith("-r "):
        return None
    name = re.split(r"[<>=!~\[;]", stripped, maxsplit=1)[0].strip()
    return name or None


def declared_packages(requirements_path: Path) -> set[str]:
    if not requirements_path.exists():
        return set()
    packages = set()
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        name = _line_declared_package(line)
        if name:
            packages.add(name)
    return packages


def normalize_for_compare(name: str) -> str:
    return name.lower().replace("_", "-")


@dataclass
class UndeclaredImport:
    py_file: Path
    module: str
    package_name: str
    # True when package_name is a confident guess (a known IMPORT_TO_PACKAGE
    # mismatch, or the module name itself looks like a real PyPI package name).
    # False means it's ambiguous and needs a human to verify it manually.
    recognized: bool


def find_undeclared_imports(tool_dir: Path) -> list[UndeclaredImport]:
    requirements_path = tool_dir / "requirements.txt"
    declared_normalized = {normalize_for_compare(p) for p in declared_packages(requirements_path)}
    locals_ = local_sibling_modules(tool_dir) | {"common", "metadatacommon"} | KNOWN_LOCAL_MODULES

    found = []
    for py_file in sorted(tool_dir.rglob("*.py")):
        for module in sorted(extract_top_level_imports(py_file)):
            if module in STDLIB_MODULES or module in locals_ or module in IGNORED_MODULES:
                continue
            package_name = IMPORT_TO_PACKAGE.get(module, module)
            if normalize_for_compare(package_name) in declared_normalized:
                continue
            recognized = module in IMPORT_TO_PACKAGE or module.islower()
            found.append(UndeclaredImport(py_file, module, package_name, recognized))
    return found


def check_undeclared_imports(tool_dir: Path) -> list[str]:
    warnings = []
    for item in find_undeclared_imports(tool_dir):
        rel = item.py_file.relative_to(tool_dir)
        if item.recognized:
            warnings.append(
                f"Undeclared import: '{tool_dir.name}/{rel}' imports '{item.module}' "
                f"(package '{item.package_name}') not listed in '{tool_dir.name}/requirements.txt'"
            )
        else:
            warnings.append(
                f"Unrecognized import: '{tool_dir.name}/{rel}' imports '{item.module}', "
                f"verify manually whether it needs a requirements.txt entry"
            )
    return warnings


def find_unused_declarations(tool_dir: Path) -> list[str]:
    """Package names declared in this tool's requirements.txt with no
    matching import found anywhere under the tool_dir tree."""
    requirements_path = tool_dir / "requirements.txt"
    declared = declared_packages(requirements_path)
    if not declared:
        return []

    imported_modules: set[str] = set()
    for py_file in tool_dir.rglob("*.py"):
        imported_modules |= extract_top_level_imports(py_file)
    imported_normalized = {normalize_for_compare(IMPORT_TO_PACKAGE.get(m, m)) for m in imported_modules}

    return sorted(
        package for package in declared
        if normalize_for_compare(package) not in imported_normalized
    )


def check_unused_declarations(tool_dir: Path) -> list[str]:
    return [
        f"Unused declaration: '{tool_dir.name}/requirements.txt' lists '{package}' but no "
        f"import was found under '{tool_dir.name}/' (may be an indirect or subprocess dependency)"
        for package in find_unused_declarations(tool_dir)
    ]


def _append_declared_package(requirements_path: Path, package_name: str, dry_run: bool = False) -> None:
    if dry_run:
        return
    text = requirements_path.read_text(encoding="utf-8") if requirements_path.exists() else ""
    if text and not text.endswith("\n"):
        text += "\n"
    text += f"{package_name}\n"
    requirements_path.write_text(text, encoding="utf-8")


def _remove_declared_package(requirements_path: Path, package_name: str, dry_run: bool = False) -> None:
    if dry_run:
        return
    if not requirements_path.exists():
        return
    target = normalize_for_compare(package_name)
    lines = requirements_path.read_text(encoding="utf-8").splitlines()
    kept = [
        line for line in lines
        if normalize_for_compare(_line_declared_package(line) or "") != target
    ]
    text = "\n".join(kept)
    if text:
        text += "\n"
    requirements_path.write_text(text, encoding="utf-8")


def fix_undeclared_imports(repo_root: Path, dry_run: bool = False) -> list[str]:
    """Appends the missing package to a tool's requirements.txt for every
    'recognized' undeclared import (see UndeclaredImport.recognized).
    Ambiguous ('Unrecognized import') findings and unused-declaration
    findings are never auto-fixed: guessing a package name that's actually
    wrong, or deleting a declaration that's really an indirect/subprocess
    dependency, would silently corrupt requirements.txt. Those stay
    warnings for a human to resolve.

    With dry_run=True, computes the same changes but never writes any file.

    Returns human-readable change descriptions ('+ package -> tool/requirements.txt');
    empty if nothing changed.
    """
    changes = []
    for tool_dir in find_tool_dirs(repo_root):
        requirements_path = tool_dir / "requirements.txt"
        seen = set()
        for item in find_undeclared_imports(tool_dir):
            if not item.recognized or item.package_name in seen:
                continue
            seen.add(item.package_name)
            _append_declared_package(requirements_path, item.package_name, dry_run=dry_run)
            rel = requirements_path.relative_to(repo_root).as_posix()
            changes.append(f"+ {item.package_name} -> {rel}")
    return changes


def fix_unused_declarations(repo_root: Path, dry_run: bool = False) -> list[str]:
    """Removes each 'Unused declaration' package from its tool's requirements.txt.

    Riskier than fix_undeclared_imports: an apparently-unused declaration may
    really be an indirect or subprocess dependency that static analysis can't
    see, so this only runs when explicitly selected (--level all or
    --types unused).

    With dry_run=True, computes the same changes but never writes any file.

    Returns human-readable change descriptions ('- package -> tool/requirements.txt');
    empty if nothing changed.
    """
    changes = []
    for tool_dir in find_tool_dirs(repo_root):
        requirements_path = tool_dir / "requirements.txt"
        for package in find_unused_declarations(tool_dir):
            _remove_declared_package(requirements_path, package, dry_run=dry_run)
            rel = requirements_path.relative_to(repo_root).as_posix()
            changes.append(f"- {package} -> {rel}")
    return changes


def run_checks(repo_root: Path) -> CheckResult:
    result = CheckResult()

    orphaned, stale = check_orphaned_and_stale(repo_root)
    result.hard_failures.extend(orphaned)
    result.hard_failures.extend(stale)

    for tool_dir in find_tool_dirs(repo_root):
        result.warnings.extend(check_undeclared_imports(tool_dir))
        result.warnings.extend(check_unused_declarations(tool_dir))

    return result


_QUOTED_TOKEN_RE = re.compile(r"'[^']*'")


def _colorize_finding(msg: str, label_color: str, token_color: str, use_color: bool) -> str:
    """Color-code a single finding line: the leading 'Label:' in ``label_color``,
    every single-quoted token (paths, module/package names) in ``token_color``,
    and the connective prose in between left uncolored so the highlights stand out."""
    if not use_color:
        return msg

    label, sep, rest = msg.partition(": ")
    if not sep:
        return Colors.wrap(msg, label_color, use_color)

    colored_label = Colors.wrap(f"{label}:", Colors.BOLD + label_color, use_color)
    colored_rest = _QUOTED_TOKEN_RE.sub(
        lambda m: Colors.wrap(m.group(0), token_color, use_color), rest
    )
    return f"{colored_label} {colored_rest}"


def run_check_command(repo_root: Path, use_color: bool) -> int:
    result = run_checks(repo_root)

    if result.hard_failures:
        print(Colors.wrap("FAILURES:", Colors.BOLD + Colors.BRIGHT_RED, use_color))
        for msg in result.hard_failures:
            bullet = Colors.wrap("  - ", Colors.DIM, use_color)
            print(f"{bullet}{_colorize_finding(msg, Colors.RED, Colors.BRIGHT_CYAN, use_color)}")
    if result.warnings:
        print(Colors.wrap("WARNINGS (best-effort, verify manually):", Colors.BOLD + Colors.BRIGHT_YELLOW, use_color))
        for msg in result.warnings:
            bullet = Colors.wrap("  - ", Colors.DIM, use_color)
            print(f"{bullet}{_colorize_finding(msg, Colors.YELLOW, Colors.BRIGHT_CYAN, use_color)}")
    if not result.hard_failures and not result.warnings:
        print(Colors.wrap("All requirements.txt files are consistent.", Colors.BRIGHT_GREEN, use_color))

    return 1 if result.hard_failures else 0


def run_update_command(repo_root: Path, use_color: bool, dry_run: bool = False) -> int:
    changes = update_root_requirements(repo_root, dry_run=dry_run)

    if not changes:
        print(Colors.wrap("requirements.txt is already up to date.", Colors.BRIGHT_GREEN, use_color))
        return 0

    print("Would update requirements.txt:" if dry_run else "Updated requirements.txt:")
    for change in changes:
        print(f"  {_colorize_root_change(change, use_color)}")
    summary = f"Dry run. {len(changes)} change(s) would be written." if dry_run \
        else f"Done. {len(changes)} change(s) written."
    print(Colors.wrap(summary, Colors.CYAN, use_color))
    return 0


_FIX_CHANGE_RE = re.compile(r"^.\s+(\S+)\s+->\s+(\S+)$")


def _colorize_fix_change(change: str, use_color: bool) -> str:
    """Render a '+ package -> path' (would add) or '- package -> path'
    (would remove) fix line as an explicit 'add ... to' / 'remove ... from'
    sentence. A bare colored sign plus arrow reads ambiguously either way
    (does the arrow mean the package is going INTO that file, or being
    taken OUT of it?), so spell out the verb instead of relying on it."""
    sign = change[0]
    match = _FIX_CHANGE_RE.match(change)
    package, path = match.group(1), match.group(2)

    if sign == "+":
        verb, verb_color, preposition = "add   ", Colors.BRIGHT_GREEN, "to"
    else:
        verb, verb_color, preposition = "remove", Colors.BRIGHT_RED, "from"

    colored_verb = Colors.wrap(verb, Colors.BOLD + verb_color, use_color)
    colored_package = Colors.wrap(package, Colors.BRIGHT_CYAN, use_color)
    colored_path = Colors.wrap(path, Colors.BRIGHT_CYAN, use_color)
    return f"{colored_verb} {colored_package} {preposition} {colored_path}"


def _colorize_root_change(change: str, use_color: bool) -> str:
    """Render a '+ -r path' / '- -r path' root-sync change as an explicit
    'add'/'remove' line, for the same reason as _colorize_fix_change."""
    sign, rest = change[0], change[1:].strip()  # rest is '-r path'

    if sign == "+":
        verb, verb_color = "add   ", Colors.BRIGHT_GREEN
    else:
        verb, verb_color = "remove", Colors.BRIGHT_RED

    colored_verb = Colors.wrap(verb, Colors.BOLD + verb_color, use_color)
    colored_rest = re.sub(
        r"(-r )(\S+)",
        lambda m: f"{m.group(1)}{Colors.wrap(m.group(2), Colors.BRIGHT_CYAN, use_color)}",
        rest,
    )
    return f"{colored_verb} {colored_rest}"


def run_fix_command(
    repo_root: Path, use_color: bool, dry_run: bool = False, types: set = None,
) -> int:
    types = types if types is not None else FIX_LEVEL_TYPES["warning"]

    root_changes = update_root_requirements(repo_root, dry_run=dry_run) if "root" in types else []
    import_changes = fix_undeclared_imports(repo_root, dry_run=dry_run) if "undeclared" in types else []
    unused_changes = fix_unused_declarations(repo_root, dry_run=dry_run) if "unused" in types else []

    total = len(root_changes) + len(import_changes) + len(unused_changes)
    if total:
        print("Would apply fixes:" if dry_run else "Applied fixes:")
        if root_changes:
            print(Colors.wrap("  Root requirements.txt sync:", Colors.BOLD, use_color))
            for change in root_changes:
                print(f"    {_colorize_root_change(change, use_color)}")
        if import_changes:
            print(Colors.wrap("  Add missing packages for recognized imports:", Colors.BOLD, use_color))
            for change in import_changes:
                print(f"    {_colorize_fix_change(change, use_color)}")
        if unused_changes:
            print(Colors.wrap(
                "  Remove unused declarations (verify these aren't indirect/subprocess dependencies):",
                Colors.BOLD + Colors.BRIGHT_YELLOW, use_color,
            ))
            for change in unused_changes:
                print(f"    {_colorize_fix_change(change, use_color)}")
        summary = f"Dry run. {total} change(s) would be written." if dry_run \
            else f"Done. {total} change(s) written."
        print(Colors.wrap(summary, Colors.CYAN, use_color))
    else:
        print(Colors.wrap("Nothing to fix.", Colors.BRIGHT_GREEN, use_color))

    if dry_run:
        # Nothing was actually written, so re-running checks now would just show
        # the same findings again (including the ones listed above as fixable),
        # which would be misleading. Point the user at the real run instead.
        print(Colors.wrap(
            "Run without --dry-run to apply these fixes, then 'check' to see what still needs manual review.",
            Colors.DIM, use_color,
        ))
        return 0

    result = run_checks(repo_root)
    if result.warnings:
        print(Colors.wrap(
            "Remaining warnings (could not be auto-fixed, review manually):",
            Colors.BOLD + Colors.BRIGHT_YELLOW, use_color,
        ))
        for msg in result.warnings:
            bullet = Colors.wrap("  - ", Colors.DIM, use_color)
            print(f"{bullet}{_colorize_finding(msg, Colors.YELLOW, Colors.BRIGHT_CYAN, use_color)}")

    return 1 if result.hard_failures else 0


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo-root", default=".", help="Repository root to check (default: current directory)")
    color_group = parser.add_mutually_exclusive_group()
    color_group.add_argument("--color", action="store_true", dest="use_color", help="Force ANSI color output")
    color_group.add_argument("--no-color", action="store_false", dest="use_color", help="Disable ANSI color output")
    parser.set_defaults(use_color=None)


def _add_write_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would change without writing any file.",
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Check and maintain requirements.txt consistency across tool folders and root-level scripts."
    )
    _add_common_args(parser)
    subparsers = parser.add_subparsers(dest="command")

    check_parser = subparsers.add_parser("check", help="Report inconsistencies (default).")
    _add_common_args(check_parser)

    update_parser = subparsers.add_parser(
        "update", help="Add missing '-r' lines and remove stale ones in the root requirements.txt."
    )
    _add_common_args(update_parser)
    _add_write_args(update_parser)

    fix_parser = subparsers.add_parser(
        "fix", help="Everything 'update' does, plus append packages for recognized undeclared imports."
    )
    _add_common_args(fix_parser)
    _add_write_args(fix_parser)
    fix_parser.add_argument(
        "--level", choices=("error", "warning", "all"), default="warning",
        help=(
            "How much to fix (default: warning). 'error' only syncs the root "
            "requirements.txt (orphaned/stale '-r' lines). 'warning' also appends "
            "packages for recognized undeclared imports. 'all' additionally removes "
            "'Unused declaration' findings -- riskier, since an apparently-unused "
            "declaration may really be an indirect or subprocess dependency. "
            "Overridden by --types if given."
        ),
    )
    fix_parser.add_argument(
        "--types", nargs="+", choices=FIX_TYPES, default=None,
        help=(
            "Exact set of issue types to fix, overriding --level: "
            "'root' (orphaned/stale root references), 'undeclared' (recognized "
            "undeclared imports), 'unused' (unused declarations, risky). "
            "E.g. '--types unused' fixes only unused declarations."
        ),
    )

    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    command = args.command or "check"
    use_color = Colors.should_use(args.use_color)
    dry_run = getattr(args, "dry_run", False)

    if command == "update":
        return run_update_command(repo_root, use_color, dry_run=dry_run)
    if command == "fix":
        types = set(args.types) if getattr(args, "types", None) else FIX_LEVEL_TYPES[args.level]
        return run_fix_command(repo_root, use_color, dry_run=dry_run, types=types)
    return run_check_command(repo_root, use_color)


if __name__ == "__main__":
    sys.exit(main())
