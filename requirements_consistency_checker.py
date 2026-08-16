"""Checks that per-tool and root requirements.txt files stay in sync.

Usage: python requirements_consistency_checker.py [--repo-root PATH]
Exits non-zero if any tool's requirements.txt is orphaned (not referenced by
the root requirements.txt) or if the root file references a requirements.txt
that no longer exists. Undeclared-import and unused-declaration findings are
best-effort static analysis and are only ever reported as warnings.
"""
import argparse
import ast
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

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

    tool_requirements = find_tool_requirements_files(repo_root)
    tool_requirements_abs = {p.resolve() for p in tool_requirements}

    orphaned = []
    for tool_req in tool_requirements:
        if tool_req.resolve() not in referenced_abs:
            rel = tool_req.relative_to(repo_root)
            orphaned.append(
                f"Orphaned tool requirements: {rel} exists but is not referenced "
                f"by a '-r' line in the root requirements.txt"
            )

    stale = []
    for ref in referenced:
        if (repo_root / ref).resolve() not in tool_requirements_abs:
            stale.append(
                f"Stale root reference: requirements.txt references '-r {ref}' "
                f"but that file does not exist"
            )

    return orphaned, stale


def extract_top_level_imports(py_file: Path) -> set[str]:
    try:
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


def declared_packages(requirements_path: Path) -> set[str]:
    if not requirements_path.exists():
        return set()
    packages = set()
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("-r "):
            continue
        name = re.split(r"[<>=!~\[;]", stripped, maxsplit=1)[0].strip()
        if name:
            packages.add(name)
    return packages


def normalize_for_compare(name: str) -> str:
    return name.lower().replace("_", "-")


def check_undeclared_imports(tool_dir: Path) -> list[str]:
    requirements_path = tool_dir / "requirements.txt"
    declared_normalized = {normalize_for_compare(p) for p in declared_packages(requirements_path)}
    locals_ = local_sibling_modules(tool_dir) | {"common", "metadatacommon"} | KNOWN_LOCAL_MODULES

    warnings = []
    for py_file in sorted(tool_dir.rglob("*.py")):
        for module in sorted(extract_top_level_imports(py_file)):
            if module in STDLIB_MODULES or module in locals_ or module in IGNORED_MODULES:
                continue
            package_name = IMPORT_TO_PACKAGE.get(module, module)
            if normalize_for_compare(package_name) in declared_normalized:
                continue
            rel = py_file.relative_to(tool_dir)
            if module in IMPORT_TO_PACKAGE or module.islower():
                warnings.append(
                    f"Undeclared import: {tool_dir.name}/{rel} imports '{module}' "
                    f"(package '{package_name}') not listed in {tool_dir.name}/requirements.txt"
                )
            else:
                warnings.append(
                    f"Unrecognized import: {tool_dir.name}/{rel} imports '{module}', "
                    f"verify manually whether it needs a requirements.txt entry"
                )
    return warnings


def check_unused_declarations(tool_dir: Path) -> list[str]:
    requirements_path = tool_dir / "requirements.txt"
    declared = declared_packages(requirements_path)
    if not declared:
        return []

    imported_modules: set[str] = set()
    for py_file in tool_dir.rglob("*.py"):
        imported_modules |= extract_top_level_imports(py_file)
    imported_normalized = {normalize_for_compare(IMPORT_TO_PACKAGE.get(m, m)) for m in imported_modules}

    warnings = []
    for package in sorted(declared):
        if normalize_for_compare(package) not in imported_normalized:
            warnings.append(
                f"Unused declaration: {tool_dir.name}/requirements.txt lists '{package}' but no "
                f"import was found under {tool_dir.name}/ (may be an indirect or subprocess dependency)"
            )
    return warnings


def run_checks(repo_root: Path) -> CheckResult:
    result = CheckResult()

    orphaned, stale = check_orphaned_and_stale(repo_root)
    result.hard_failures.extend(orphaned)
    result.hard_failures.extend(stale)

    for tool_dir in find_tool_dirs(repo_root):
        result.warnings.extend(check_undeclared_imports(tool_dir))
        result.warnings.extend(check_unused_declarations(tool_dir))

    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Check requirements.txt consistency across tool folders.")
    parser.add_argument("--repo-root", default=".", help="Repository root to check (default: current directory)")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    result = run_checks(repo_root)

    if result.hard_failures:
        print("FAILURES:")
        for msg in result.hard_failures:
            print(f"  - {msg}")
    if result.warnings:
        print("WARNINGS (best-effort, verify manually):")
        for msg in result.warnings:
            print(f"  - {msg}")
    if not result.hard_failures and not result.warnings:
        print("All requirements.txt files are consistent.")

    return 1 if result.hard_failures else 0


if __name__ == "__main__":
    sys.exit(main())
