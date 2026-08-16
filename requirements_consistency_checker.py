"""Checks that per-tool, per-script, and root requirements.txt files stay in sync.

Usage:
  python requirements_consistency_checker.py check [--repo-root PATH] [--color|--no-color] [--no-recursive]
  python requirements_consistency_checker.py update [--repo-root PATH] [--color|--no-color] [--dry-run]
  python requirements_consistency_checker.py fix [--repo-root PATH] [--color|--no-color] [--dry-run]
      [--level {error,warning,all}] [--types root undeclared version unused ...]
      [--interactive-versions] [--no-recursive]

`check` (the default when no subcommand is given) exits non-zero if any
tool's requirements.txt or root-level script's <script>.requirements.txt is
orphaned (not referenced by the root requirements.txt), if the root file
references a requirements file that no longer exists, or if the same
package is pinned to different version constraints in different
requirements files (a hard failure, since the root requirements.txt '-r'
references pull every file into one aggregate install, so conflicting pins
for one package can make that install unsatisfiable). Undeclared-import,
unused-declaration, and installed-version findings are best-effort and are
only ever reported as warnings. `check` never writes anything, so it has
no --dry-run option.

`check` also compares every version-pinned declaration against what's
actually installed in the environment running the checker (needs the
'packaging' library; if it's missing, one note is reported instead of
silently skipping). Only a package that IS installed but at a version
that doesn't satisfy its declared pin is reported -- a package simply not
being installed here is expected and not flagged, since this repo's tools
each keep their own separate requirements.txt rather than sharing one
environment.

Undeclared-import and unused-declaration checks are recursive by default:
for a tool that imports a repo-local shared library directory (see
SHARED_LIBRARY_DIRS, e.g. `from common.presentation import Table`), the
scan also follows into that shared directory's own imports (transitively,
if it in turn imports another shared directory). This means a package a
tool only needs because of what `common`/`metadatacommon` actually does
is correctly attributed to that tool, rather than being wrongly flagged
as "undeclared" (missed because it's only imported inside the shared
code) or "unused" (missed because the checker only looked at the tool's
own folder). Pass --no-recursive to check only each tool's own folder,
matching the old non-recursive behavior.

`update` rewrites the root requirements.txt to add '-r' lines for orphaned
requirements files and remove '-r' lines that point at files that no longer
exist. It never modifies per-tool or per-script requirements.txt content.

`fix` resolves issues found by `check`, gated by --level (default: warning):
  - error:   only the root requirements.txt '-r' sync (same as `update`).
  - warning: also appends the missing package for each "recognized"
             undeclared import (a known import-name-to-package mapping, or
             a lowercase module name that looks like a real package name),
             and resolves "Version mismatch" findings.
  - all:     also removes "Unused declaration" findings.
--types overrides --level with an exact set from {root, undeclared, version,
unused} (e.g. '--types version' to fix only version-pin conflicts). `fix`
never guesses at ambiguous "Unrecognized import" findings at any level/type
-- those always require manual review. Removing an "Unused declaration" is
the riskiest fix, since it may really be an indirect or subprocess
dependency that static analysis can't see; it only happens at --level all
or with 'unused' explicitly named in --types. After applying fixes, `fix`
re-checks and reports any remaining hard failures and warnings that still
need manual review.

A "Version mismatch" is resolved by picking one spec to win and rewriting
every other file's declaration of that package to match it. By default
this is heuristic and unattended: the conflicting spec with the highest
version number literally present in its text wins (e.g. '>=1.45.1' beats
'>=1.45.0'), which is a guess -- it assumes the higher-looking pin is the
more recently-intentional one. Pass --interactive-versions to prompt for
each conflict instead and pick by hand (numbered choice, a custom spec, or
's' to skip); it automatically falls back to the heuristic when not
running in a real terminal, and is a no-op under --dry-run.

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

# Top-level directories at the repo root that hold shared code imported across
# folder boundaries (e.g. `from common.presentation import Table`) rather than
# being a tool's own sibling files. Recursive checking (the default) follows a
# tool's imports of these directories into their own source, so a package a
# tool only needs because of what its `common`/`metadatacommon` calls actually
# do isn't wrongly flagged as undeclared or unused.
SHARED_LIBRARY_DIRS = {"common", "metadatacommon"}

# Issue types the 'fix' command knows how to resolve, from safest to riskiest.
#   root       - root requirements.txt '-r' line sync (orphaned/stale references)
#   undeclared - append the package for a 'recognized' undeclared import
#   version    - resolve a version-mismatch hard failure (heuristic by default,
#                or interactive with --interactive-versions)
#   unused     - remove an 'Unused declaration' (never guessed, always risky)
FIX_TYPES = ("root", "undeclared", "version", "unused")

# --level shorthand: which FIX_TYPES each tier enables by default.
FIX_LEVEL_TYPES = {
    "error": {"root"},
    "warning": {"root", "undeclared", "version"},
    "all": {"root", "undeclared", "version", "unused"},
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


def _collect_version_conflicts(repo_root: Path) -> dict[str, dict[str, list[Path]]]:
    """normalized package name -> {version_spec: [requirements.txt paths]},
    for every package pinned to more than one distinct spec across the repo.
    Declarations with no version pin never conflict with anything. Shared by
    check_version_consistency and fix_version_inconsistencies so both agree
    on exactly what counts as a conflict."""
    all_requirements = find_tool_requirements_files(repo_root) + find_root_script_requirements_files(repo_root)

    seen: dict[str, dict[str, list[Path]]] = {}
    for req_path in all_requirements:
        for name, spec in declared_requirements(req_path).items():
            if not spec:
                continue
            key = normalize_for_compare(name)
            seen.setdefault(key, {}).setdefault(spec, []).append(req_path)

    return {key: spec_map for key, spec_map in seen.items() if len(spec_map) > 1}


def check_version_consistency(repo_root: Path) -> list[str]:
    """Hard failure when the same package is pinned to different version
    constraints across requirements files in the repo. The root
    requirements.txt '-r' references every tool/script file into one
    aggregate install, so conflicting pins for the same package can make
    `pip install -r requirements.txt` unsatisfiable (or resolve to an
    arbitrary winner) -- this is a structural inconsistency, not a
    best-effort heuristic, so it's a hard failure like orphaned/stale.
    Declarations with no version pin never conflict with anything."""
    conflicts = _collect_version_conflicts(repo_root)

    failures = []
    for key, spec_map in sorted(conflicts.items()):
        spec_descriptions = []
        for spec, paths in sorted(spec_map.items()):
            quoted_files = ", ".join(f"'{p.relative_to(repo_root).as_posix()}'" for p in paths)
            spec_descriptions.append(f"'{spec}' in {quoted_files}")
        failures.append(
            f"Version mismatch: '{key}' is pinned inconsistently across the repo: "
            + "; ".join(spec_descriptions)
        )
    return failures


def check_installed_versions(repo_root: Path) -> list[str]:
    """Best-effort comparison of every declared, version-pinned package
    against what's actually installed in the environment running this
    checker. Only flags packages that ARE installed but whose installed
    version doesn't satisfy the declared pin -- a package simply not being
    installed here is expected and not reported, since this repo's tools
    each have their own separate requirements.txt and aren't meant to
    share one environment. Always a warning: never affects the exit code,
    since it depends on the local machine's state, not just the repo.

    Requires the third-party 'packaging' library to parse version specs;
    if it isn't installed, returns a single note explaining the check was
    skipped rather than silently doing nothing.
    """
    try:
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version
    except ImportError:
        return [
            "Installed-version check skipped: the 'packaging' package is not installed "
            "in the environment running this checker (pip install packaging)"
        ]

    from importlib import metadata

    all_requirements = find_tool_requirements_files(repo_root) + find_root_script_requirements_files(repo_root)

    seen: set[tuple[str, str]] = set()
    result = []
    for req_path in all_requirements:
        rel = req_path.relative_to(repo_root).as_posix()
        for name, spec in declared_requirements(req_path).items():
            if not spec:
                continue
            key = (normalize_for_compare(name), spec)
            if key in seen:
                continue
            seen.add(key)

            try:
                installed_version = metadata.version(name)
            except metadata.PackageNotFoundError:
                continue

            try:
                satisfied = Version(installed_version) in SpecifierSet(spec)
            except Exception:
                continue

            if not satisfied:
                result.append(
                    f"Version mismatch with installed: '{name}{spec}' (declared in '{rel}') "
                    f"but the environment running this checker has '{name}' installed at "
                    f"'{installed_version}'"
                )
    return result


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


def _parse_import_refs(py_file: Path) -> set[str]:
    """Full dotted module references from this file's absolute imports
    ('import a.b', 'from a.b import X'; relative imports are skipped, since
    they can only resolve within the importing file's own package)."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError, OSError):
        return set()

    refs = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                refs.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                refs.add(node.module)
    return refs


def extract_top_level_imports(py_file: Path) -> set[str]:
    return {ref.split(".")[0] for ref in _parse_import_refs(py_file)}


def local_sibling_modules(tool_dir: Path) -> set[str]:
    """Module/package names that resolve to something local inside this tool's own folder tree."""
    names = {p.stem for p in tool_dir.rglob("*.py")}
    names |= {p.parent.name for p in tool_dir.rglob("__init__.py")}
    names.add(tool_dir.name)
    return names


def _resolve_module_file(repo_root: Path, dotted: str) -> Path | None:
    """Best-effort resolution of an absolute dotted import (e.g.
    'common.presentation') to its source file under repo_root."""
    base = repo_root.joinpath(*dotted.split("."))
    candidate = base.with_suffix(".py")
    if candidate.is_file():
        return candidate
    candidate = base / "__init__.py"
    if candidate.is_file():
        return candidate
    return None


def _resolve_shared_dependency_files(repo_root: Path, tool_dir: Path) -> set[Path]:
    """Repo-local shared-library files (see SHARED_LIBRARY_DIRS) that
    tool_dir transitively imports, resolved at the specific submodule level
    -- not the whole shared directory -- so a tool that only imports e.g.
    'common.presentation' isn't blamed for unrelated modules elsewhere
    under common/. Follows into further shared-dir submodules referenced
    by an already-resolved file (e.g. common.foo importing metadatacommon.bar)."""
    tool_refs: set[str] = set()
    for py_file in tool_dir.rglob("*.py"):
        tool_refs |= _parse_import_refs(py_file)

    resolved_files: set[Path] = set()
    visited_refs: set[str] = set()
    pending = [ref for ref in tool_refs if ref.split(".")[0] in SHARED_LIBRARY_DIRS]
    while pending:
        ref = pending.pop()
        if ref in visited_refs:
            continue
        visited_refs.add(ref)
        module_file = _resolve_module_file(repo_root, ref)
        if module_file is None:
            continue
        resolved_files.add(module_file)
        pending.extend(
            r for r in _parse_import_refs(module_file)
            if r.split(".")[0] in SHARED_LIBRARY_DIRS and r not in visited_refs
        )
    return resolved_files


_REQUIREMENT_SPLIT_RE = re.compile(r"[<>=!~\[;]")


def _split_requirement_line(line: str) -> tuple[str, str] | None:
    """(package_name, version_spec) for one requirements.txt line, or None
    for blank/comment/'-r' lines. version_spec is the raw trailing
    constraint/extras/marker text verbatim (e.g. '>=2.28,<3'), '' if the
    line has no pin at all."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or stripped.startswith("-r "):
        return None
    match = _REQUIREMENT_SPLIT_RE.search(stripped)
    if match is None:
        return (stripped, "") if stripped else None
    name = stripped[:match.start()].strip()
    return (name, stripped[match.start():].strip()) if name else None


def _line_declared_package(line: str) -> str | None:
    """The package name on one requirements.txt line, or None for
    blank/comment/'-r' lines."""
    parsed = _split_requirement_line(line)
    return parsed[0] if parsed else None


def declared_requirements(requirements_path: Path) -> dict[str, str]:
    """{package_name: version_spec} for every declared line in this
    requirements.txt (version_spec is '' when the line has no pin)."""
    if not requirements_path.exists():
        return {}
    result: dict[str, str] = {}
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        parsed = _split_requirement_line(line)
        if parsed:
            name, spec = parsed
            result[name] = spec
    return result


def declared_packages(requirements_path: Path) -> set[str]:
    return set(declared_requirements(requirements_path))


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


def find_undeclared_imports(repo_root: Path, tool_dir: Path, recursive: bool = True) -> list[UndeclaredImport]:
    requirements_path = tool_dir / "requirements.txt"
    declared_normalized = {normalize_for_compare(p) for p in declared_packages(requirements_path)}
    locals_ = local_sibling_modules(tool_dir) | SHARED_LIBRARY_DIRS | KNOWN_LOCAL_MODULES

    py_files = set(tool_dir.rglob("*.py"))
    if recursive:
        py_files |= _resolve_shared_dependency_files(repo_root, tool_dir)

    found = []
    for py_file in sorted(py_files):
        for module in sorted(extract_top_level_imports(py_file)):
            if module in STDLIB_MODULES or module in locals_ or module in IGNORED_MODULES:
                continue
            package_name = IMPORT_TO_PACKAGE.get(module, module)
            if normalize_for_compare(package_name) in declared_normalized:
                continue
            recognized = module in IMPORT_TO_PACKAGE or module.islower()
            found.append(UndeclaredImport(py_file, module, package_name, recognized))
    return found


def check_undeclared_imports(repo_root: Path, tool_dir: Path, recursive: bool = True) -> list[str]:
    warnings = []
    for item in find_undeclared_imports(repo_root, tool_dir, recursive=recursive):
        rel = item.py_file.relative_to(repo_root).as_posix()
        if item.recognized:
            warnings.append(
                f"Undeclared import: '{rel}' imports '{item.module}' "
                f"(package '{item.package_name}') not listed in '{tool_dir.name}/requirements.txt'"
            )
        else:
            warnings.append(
                f"Unrecognized import: '{rel}' imports '{item.module}', "
                f"verify manually whether '{tool_dir.name}/requirements.txt' needs an entry"
            )
    return warnings


def find_unused_declarations(repo_root: Path, tool_dir: Path, recursive: bool = True) -> list[str]:
    """Package names declared in this tool's requirements.txt with no
    matching import found anywhere under the tool_dir tree (and, if
    recursive, anywhere in the specific shared-library submodules it
    transitively imports -- see SHARED_LIBRARY_DIRS)."""
    requirements_path = tool_dir / "requirements.txt"
    declared = declared_packages(requirements_path)
    if not declared:
        return []

    py_files = set(tool_dir.rglob("*.py"))
    if recursive:
        py_files |= _resolve_shared_dependency_files(repo_root, tool_dir)

    imported_modules: set[str] = set()
    for py_file in py_files:
        imported_modules |= extract_top_level_imports(py_file)
    imported_normalized = {normalize_for_compare(IMPORT_TO_PACKAGE.get(m, m)) for m in imported_modules}

    return sorted(
        package for package in declared
        if normalize_for_compare(package) not in imported_normalized
    )


def check_unused_declarations(repo_root: Path, tool_dir: Path, recursive: bool = True) -> list[str]:
    scope = f"'{tool_dir.name}/' or the shared library code it depends on" if recursive else f"'{tool_dir.name}/'"
    return [
        f"Unused declaration: '{tool_dir.name}/requirements.txt' lists '{package}' but no "
        f"import was found under {scope} (may be an indirect or subprocess dependency)"
        for package in find_unused_declarations(repo_root, tool_dir, recursive=recursive)
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


def _replace_declared_spec(requirements_path: Path, package_name: str, new_spec: str, dry_run: bool = False) -> None:
    """Rewrites this file's declaration line for package_name to carry
    new_spec instead of whatever spec it previously had. Keeps the line's
    own original package-name spelling (e.g. 'PyYAML' stays 'PyYAML') --
    only the trailing version constraint changes."""
    if dry_run:
        return
    if not requirements_path.exists():
        return
    target = normalize_for_compare(package_name)
    lines = requirements_path.read_text(encoding="utf-8").splitlines()
    updated = []
    for line in lines:
        parsed = _split_requirement_line(line)
        if parsed and normalize_for_compare(parsed[0]) == target:
            updated.append(f"{parsed[0]}{new_spec}")
        else:
            updated.append(line)
    text = "\n".join(updated)
    if text:
        text += "\n"
    requirements_path.write_text(text, encoding="utf-8")


_VERSION_TOKEN_RE = re.compile(r"\d+(?:\.\d+)*(?:[a-zA-Z0-9.+-]*)?")


def _choose_version_winner_heuristic(spec_map: dict[str, list[Path]]) -> str:
    """Best-effort automatic pick among conflicting version pins: the spec
    with the highest version number literally present in its text wins
    (so '>=1.45.1' beats '>=1.45.0', '==2.20.0' ranks by 2.20.0, and so
    on). This is a guess -- it assumes the higher-looking pin is the more
    recently-intentional one, which won't always be true (a tool may cap
    an older version on purpose; that case needs --interactive-versions
    or manual review instead). Deterministic tie-break: most files
    declaring that spec, then alphabetically, so re-running never flips
    the answer.

    Ranks with packaging.version.Version when available; falls back to a
    naive dotted-integer-tuple comparison if 'packaging' isn't installed,
    so this still works even when that optional dependency is missing."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        Version, InvalidVersion = None, ()

    def parse_token(token: str):
        if Version is not None:
            try:
                return Version(token)
            except InvalidVersion:
                return None
        try:
            return tuple(int(part) for part in token.split("."))
        except ValueError:
            return None

    def rank(spec: str):
        versions = [v for v in (parse_token(t) for t in _VERSION_TOKEN_RE.findall(spec)) if v is not None]
        best = max(versions) if versions else None
        return (best is not None, best, len(spec_map[spec]), spec)

    return max(spec_map, key=rank)


def _prompt_version_winner(key: str, spec_map: dict[str, list[Path]], repo_root: Path) -> str | None:
    """Interactive per-package prompt for --interactive-versions: lists
    each conflicting spec with the files that declare it, and lets the
    user pick one by number, type a custom spec, or skip ('s') to leave
    this package for manual review. Only called when sys.stdin.isatty(),
    so it's never reached in a non-interactive run."""
    specs = sorted(spec_map)
    print(f"\nPackage '{key}' has conflicting version pins:")
    for i, spec in enumerate(specs, 1):
        files = ", ".join(p.relative_to(repo_root).as_posix() for p in spec_map[spec])
        print(f"  {i}) {key}{spec}  ({files})")

    try:
        from packaging.specifiers import SpecifierSet
    except ImportError:
        SpecifierSet = None

    while True:
        choice = input(f"Choose 1-{len(specs)}, enter a custom spec, or 's' to skip: ").strip()
        if choice.lower() == "s":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(specs):
            return specs[int(choice) - 1]
        if choice:
            if SpecifierSet is not None:
                try:
                    SpecifierSet(choice)
                except Exception:
                    print(f"  '{choice}' doesn't look like a valid version spec, try again.")
                    continue
            return choice


def fix_version_inconsistencies(repo_root: Path, dry_run: bool = False, interactive: bool = False) -> list[str]:
    """Resolves each 'Version mismatch' finding by picking one spec to win
    and rewriting every other file's declaration of that package to match.

    Default (interactive=False): picks the winner heuristically via
    _choose_version_winner_heuristic -- no prompting, safe to run
    unattended. Pass interactive=True to prompt per conflict instead (see
    _prompt_version_winner) and let a human pick; automatically falls
    back to the heuristic when stdin isn't a real terminal (sys.stdin.isatty()
    is False), so it never hangs a non-interactive run, and a user who
    explicitly skips ('s') a package leaves it unresolved for manual review.

    With dry_run=True, always previews the heuristic choice and never
    prompts (nothing would be written either way) or writes any file.

    Returns human-readable change descriptions; empty if nothing changed.
    """
    conflicts = _collect_version_conflicts(repo_root)
    changes = []

    can_prompt = interactive and not dry_run and sys.stdin.isatty()
    for key, spec_map in sorted(conflicts.items()):
        winner = _prompt_version_winner(key, spec_map, repo_root) if can_prompt \
            else _choose_version_winner_heuristic(spec_map)
        if winner is None:
            continue

        for spec, paths in spec_map.items():
            if spec == winner:
                continue
            for path in paths:
                _replace_declared_spec(path, key, winner, dry_run=dry_run)
                rel = path.relative_to(repo_root).as_posix()
                changes.append(f"update {key}{winner} in {rel} (was {key}{spec})")
    return changes


def fix_undeclared_imports(repo_root: Path, dry_run: bool = False, recursive: bool = True) -> list[str]:
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
        for item in find_undeclared_imports(repo_root, tool_dir, recursive=recursive):
            if not item.recognized or item.package_name in seen:
                continue
            seen.add(item.package_name)
            _append_declared_package(requirements_path, item.package_name, dry_run=dry_run)
            rel = requirements_path.relative_to(repo_root).as_posix()
            changes.append(f"+ {item.package_name} -> {rel}")
    return changes


def fix_unused_declarations(repo_root: Path, dry_run: bool = False, recursive: bool = True) -> list[str]:
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
        for package in find_unused_declarations(repo_root, tool_dir, recursive=recursive):
            _remove_declared_package(requirements_path, package, dry_run=dry_run)
            rel = requirements_path.relative_to(repo_root).as_posix()
            changes.append(f"- {package} -> {rel}")
    return changes


def run_checks(repo_root: Path, recursive: bool = True) -> CheckResult:
    result = CheckResult()

    orphaned, stale = check_orphaned_and_stale(repo_root)
    result.hard_failures.extend(orphaned)
    result.hard_failures.extend(stale)
    result.hard_failures.extend(check_version_consistency(repo_root))

    for tool_dir in find_tool_dirs(repo_root):
        result.warnings.extend(check_undeclared_imports(repo_root, tool_dir, recursive=recursive))
        result.warnings.extend(check_unused_declarations(repo_root, tool_dir, recursive=recursive))

    result.warnings.extend(check_installed_versions(repo_root))

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


def run_check_command(repo_root: Path, use_color: bool, recursive: bool = True) -> int:
    result = run_checks(repo_root, recursive=recursive)

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


_VERSION_CHANGE_RE = re.compile(r"^update (\S+) in (\S+) \(was (\S+)\)$")


def _colorize_version_change(change: str, use_color: bool) -> str:
    """Render an 'update pkg>=x in path (was pkg>=y)' version-fix line with
    the winning declaration and file highlighted, and the superseded spec
    dimmed -- distinct from add/remove since this rewrites a line in place
    rather than adding or removing one."""
    match = _VERSION_CHANGE_RE.match(change)
    if not match:
        return change
    new_decl, path, old_decl = match.groups()

    verb = Colors.wrap("update", Colors.BOLD + Colors.BRIGHT_YELLOW, use_color)
    colored_new = Colors.wrap(new_decl, Colors.BRIGHT_CYAN, use_color)
    colored_path = Colors.wrap(path, Colors.BRIGHT_CYAN, use_color)
    colored_old = Colors.wrap(old_decl, Colors.DIM, use_color)
    return f"{verb} {colored_new} in {colored_path} (was {colored_old})"


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
    repo_root: Path, use_color: bool, dry_run: bool = False, types: set = None, recursive: bool = True,
    interactive_versions: bool = False,
) -> int:
    types = types if types is not None else FIX_LEVEL_TYPES["warning"]

    root_changes = update_root_requirements(repo_root, dry_run=dry_run) if "root" in types else []
    import_changes = fix_undeclared_imports(repo_root, dry_run=dry_run, recursive=recursive) \
        if "undeclared" in types else []
    version_changes = fix_version_inconsistencies(repo_root, dry_run=dry_run, interactive=interactive_versions) \
        if "version" in types else []
    unused_changes = fix_unused_declarations(repo_root, dry_run=dry_run, recursive=recursive) \
        if "unused" in types else []

    total = len(root_changes) + len(import_changes) + len(version_changes) + len(unused_changes)
    if total:
        print("Would apply fixes:" if dry_run else "Applied fixes:")
        if root_changes:
            print(Colors.wrap("  Root requirements.txt sync:", Colors.BOLD, use_color))
            for change in root_changes:
                print(f"    {_colorize_root_change(change, use_color)}")
        if version_changes:
            print(Colors.wrap("  Resolve version-pin conflicts:", Colors.BOLD, use_color))
            for change in version_changes:
                print(f"    {_colorize_version_change(change, use_color)}")
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

    result = run_checks(repo_root, recursive=recursive)
    if result.hard_failures:
        print(Colors.wrap(
            "Remaining failures (could not be auto-fixed, review manually):",
            Colors.BOLD + Colors.BRIGHT_RED, use_color,
        ))
        for msg in result.hard_failures:
            bullet = Colors.wrap("  - ", Colors.DIM, use_color)
            print(f"{bullet}{_colorize_finding(msg, Colors.RED, Colors.BRIGHT_CYAN, use_color)}")
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


def _add_recursive_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-recursive", action="store_false", dest="recursive", default=True,
        help=(
            "Only scan each tool's own folder for imports, without following "
            "into shared library directories it depends on (see "
            "SHARED_LIBRARY_DIRS, e.g. 'common', 'metadatacommon'). Recursive "
            "scanning is the default, since a tool's requirements.txt has to "
            "cover what the shared code it calls into actually imports, not "
            "just its own top-level source files."
        ),
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Check and maintain requirements.txt consistency across tool folders and root-level scripts."
    )
    _add_common_args(parser)
    subparsers = parser.add_subparsers(dest="command")

    check_parser = subparsers.add_parser("check", help="Report inconsistencies (default).")
    _add_common_args(check_parser)
    _add_recursive_args(check_parser)

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
    _add_recursive_args(fix_parser)
    fix_parser.add_argument(
        "--level", choices=("error", "warning", "all"), default="warning",
        help=(
            "How much to fix (default: warning). 'error' only syncs the root "
            "requirements.txt (orphaned/stale '-r' lines). 'warning' also appends "
            "packages for recognized undeclared imports and resolves version-pin "
            "conflicts (heuristically, unless --interactive-versions is given). "
            "'all' additionally removes 'Unused declaration' findings -- riskier, "
            "since an apparently-unused declaration may really be an indirect or "
            "subprocess dependency. Overridden by --types if given."
        ),
    )
    fix_parser.add_argument(
        "--types", nargs="+", choices=FIX_TYPES, default=None,
        help=(
            "Exact set of issue types to fix, overriding --level: "
            "'root' (orphaned/stale root references), 'undeclared' (recognized "
            "undeclared imports), 'version' (version-pin conflicts), "
            "'unused' (unused declarations, risky). "
            "E.g. '--types version' fixes only version-pin conflicts."
        ),
    )
    fix_parser.add_argument(
        "--interactive-versions", action="store_true",
        help=(
            "Prompt for each version-pin conflict instead of resolving it "
            "heuristically: shows every conflicting spec and the files that "
            "declare it, and lets you pick one, type a custom spec, or skip "
            "('s') to leave it for manual review. Falls back to the heuristic "
            "automatically when not running in a real terminal, and is a no-op "
            "with --dry-run (nothing would be written either way)."
        ),
    )

    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    command = args.command or "check"
    use_color = Colors.should_use(args.use_color)
    dry_run = getattr(args, "dry_run", False)
    recursive = getattr(args, "recursive", True)

    if command == "update":
        return run_update_command(repo_root, use_color, dry_run=dry_run)
    if command == "fix":
        types = set(args.types) if getattr(args, "types", None) else FIX_LEVEL_TYPES[args.level]
        return run_fix_command(
            repo_root, use_color, dry_run=dry_run, types=types, recursive=recursive,
            interactive_versions=args.interactive_versions,
        )
    return run_check_command(repo_root, use_color, recursive=recursive)


if __name__ == "__main__":
    sys.exit(main())
