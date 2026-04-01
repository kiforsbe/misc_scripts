from __future__ import annotations

import io
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path

from smartls import (
    DirectoryScanner,
    Entry,
    FilterFactory,
    OutputRenderer,
    ScanResult,
    SmartLSArgumentParser,
    collect_matches,
    compute_visible_tree,
    export_webapp_report,
    normalize_cli_argv,
    parse_numeric_expr,
    parse_size_expr,
    parse_time_expr,
)


def make_entry(path: str, entry_type: str, depth: int, **overrides: object) -> Entry:
    path_obj = Path(path)
    defaults = {
        "path": path_obj,
        "name": path_obj.name,
        "entry_type": entry_type,
        "depth": depth,
        "parent": path_obj.parent if str(path_obj.parent) != "." else None,
        "size_bytes": 0,
        "raw_size_bytes": 0,
        "created_ts": None,
        "modified_ts": None,
        "accessed_ts": None,
        "permissions_octal": None,
        "permissions_text": None,
        "owner": None,
        "group": None,
        "is_symlink": False,
        "symlink_target": None,
        "mime_type": None,
        "hash_md5": None,
        "hash_sha256": None,
        "direct_files": 0,
        "direct_dirs": 0,
        "direct_children": 0,
        "recursive_files": 0,
        "deepest_nesting": 0,
        "is_empty": False,
        "is_sparse": False,
        "children": [],
    }
    defaults.update(overrides)
    return Entry(**defaults)


class NumericExpressionTests(unittest.TestCase):
    def test_parse_numeric_expr_supports_all_requested_forms(self) -> None:
        self.assertTrue(parse_numeric_expr("=3")(3))
        self.assertTrue(parse_numeric_expr("!=3")(2))
        self.assertTrue(parse_numeric_expr(">3")(4))
        self.assertTrue(parse_numeric_expr(">=3")(3))
        self.assertTrue(parse_numeric_expr("<3")(2))
        self.assertTrue(parse_numeric_expr("<=3")(3))
        self.assertTrue(parse_numeric_expr("1..5")(4))
        self.assertTrue(parse_numeric_expr("0,2,4")(2))
        self.assertTrue(parse_numeric_expr("%2")(8))
        self.assertTrue(parse_numeric_expr("~100±10")(95))

    def test_size_and_time_expressions_normalize_units(self) -> None:
        self.assertTrue(parse_size_expr(">=1KB")(2048))
        self.assertTrue(parse_size_expr("1KB..5KB")(4096))
        self.assertTrue(parse_time_expr("<7d")(2 * 86400))
        self.assertTrue(parse_time_expr("2d..14d")(7 * 86400))


class FilterEngineTests(unittest.TestCase):
    def test_sort_value_with_leading_dash_is_normalized(self) -> None:
        self.assertEqual(normalize_cli_argv(["--type", "f", "--sort", "-size"]), ["--type", "f", "--sort=-size"])

    def test_or_and_not_combinators(self) -> None:
        groups = FilterFactory.from_argv(["--type", "f", "--not", "--ext", "py", "--or", "--type", "d", "--files", "=0"])
        file_entry = make_entry("demo/readme.md", "f", 1)
        py_entry = make_entry("demo/test.py", "f", 1)
        dir_entry = make_entry("demo/empty", "d", 1, direct_files=0)
        self.assertTrue(any(all(f.apply(file_entry) for f in group) for group in groups))
        self.assertFalse(any(all(f.apply(py_entry) for f in group) for group in groups))
        self.assertTrue(any(all(f.apply(dir_entry) for f in group) for group in groups))

    def test_zero_file_filter_uses_recursive_file_count(self) -> None:
        groups = FilterFactory.from_argv(["--type", "d", "--files", "=0"])
        empty_dir = make_entry("demo/empty", "d", 1, direct_files=0, recursive_files=0)
        nested_dir = make_entry("demo/nested", "d", 1, direct_files=0, recursive_files=1)
        self.assertTrue(any(all(f.apply(empty_dir) for f in group) for group in groups))
        self.assertFalse(any(all(f.apply(nested_dir) for f in group) for group in groups))

    def test_files_filter_uses_recursive_counts_for_nonzero_expressions(self) -> None:
        groups = FilterFactory.from_argv(["--type", "d", "--files", "<5"])
        direct_only_dir = make_entry("demo/direct", "d", 1, direct_files=4, recursive_files=6)
        recursive_small_dir = make_entry("demo/recursive-small", "d", 1, direct_files=0, recursive_files=4)
        self.assertFalse(any(all(f.apply(direct_only_dir) for f in group) for group in groups))
        self.assertTrue(any(all(f.apply(recursive_small_dir) for f in group) for group in groups))

    def test_sparse_filter_uses_recursive_counts(self) -> None:
        groups = FilterFactory.from_argv(["--type", "d", "--sparse"])
        nested_dir = make_entry("demo/nested", "d", 1, direct_files=0, recursive_files=4)
        sparse_dir = make_entry("demo/sparse", "d", 1, direct_files=0, recursive_files=3)
        self.assertFalse(any(all(f.apply(nested_dir) for f in group) for group in groups))
        self.assertTrue(any(all(f.apply(sparse_dir) for f in group) for group in groups))

    def test_collect_matches_applies_sort_and_limit(self) -> None:
        root = Path("root")
        entries = {
            root: make_entry("root", "d", 0),
            root / "b.txt": make_entry("root/b.txt", "f", 1, size_bytes=20),
            root / "a.txt": make_entry("root/a.txt", "f", 1, size_bytes=10),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(sort="name", limit=1)
        matched = collect_matches(scan_result, [], args)
        self.assertEqual([entry.name for entry in matched], ["a.txt"])


class ConsoleColumnOutputTests(unittest.TestCase):
    def test_parser_normalizes_column_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            args = SmartLSArgumentParser().parse([temp_dir, "--columns", "size,recursive-files,relative-path"])
        self.assertEqual(args.columns, ["size", "recursive_files", "relative_path"])
        self.assertEqual(args.column_widths, {})

    def test_parser_accepts_column_width_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            args = SmartLSArgumentParser().parse([temp_dir, "--columns", "type:12,relative-path:80,size:6"])
        self.assertEqual(args.columns, ["type", "relative_path", "size"])
        self.assertEqual(args.column_widths, {"type": 12, "relative_path": 80, "size": 6})

    def test_parser_accepts_name_width_override_in_first_position(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            args = SmartLSArgumentParser().parse([temp_dir, "--columns", "name:12,type:10,size:6"])
        self.assertEqual(args.columns, ["type", "size"])
        self.assertEqual(args.column_widths, {"name": 12, "type": 10, "size": 6})

    def test_parser_rejects_name_column_outside_first_position(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(SystemExit):
                SmartLSArgumentParser().parse([temp_dir, "--columns", "type:10,name:12,size:6"])

    def test_flat_table_output_uses_headers_and_right_aligns_numeric_columns(self) -> None:
        root = Path("root")
        alpha = root / "alpha.txt"
        beta = root / "sub" / "very-long-folder-name" / "another-long-segment" / "beta.txt"
        entries = {
            root: make_entry("root", "d", 0),
            alpha: make_entry("root/alpha.txt", "f", 1, size_bytes=5, modified_ts=0),
            beta: make_entry("root/sub/very-long-folder-name/another-long-segment/beta.txt", "f", 2, size_bytes=12, modified_ts=0),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(
            json=False,
            csv=False,
            flat=True,
            columns=["type", "size", "relative_path"],
            column_widths={},
            export_html=None,
            stats=False,
            show_errors=False,
            group_by=None,
            relative_paths=True,
            human_sizes=False,
            icons=False,
            use_color=False,
            sort="name",
            short=False,
            long=False,
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            OutputRenderer(scan_result, args).render([entries[alpha], entries[beta]])

        lines = stdout.getvalue().splitlines()
        self.assertTrue(lines[0].startswith("| Name"))
        self.assertIn("| Type", lines[0])
        self.assertIn("| Size", lines[0])
        self.assertIn("| Relative path", lines[0])
        self.assertTrue(lines[1].startswith("| :"))
        self.assertIn("---:", lines[1])
        self.assertEqual(len(lines[0]), len(lines[1]))
        size_header_index = lines[0].index("Size")
        self.assertGreater(lines[2].index("5"), size_header_index)
        self.assertGreater(lines[3].index("12"), size_header_index)
        self.assertIn("...", lines[3])
        self.assertNotIn("very-long-folder-name/another-long-segment/beta.txt", lines[3])

    def test_tree_table_output_preserves_hierarchy_in_name_column(self) -> None:
        root = Path("root")
        nested = root / "nested"
        deep = nested / "deep.txt"
        entries = {
            root: make_entry("root", "d", 0, children=[nested]),
            nested: make_entry("root/nested", "d", 1, parent=root, children=[deep]),
            deep: make_entry("root/nested/deep.txt", "f", 2, parent=nested, size_bytes=7),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(
            json=False,
            csv=False,
            flat=False,
            columns=["type", "size"],
            column_widths={},
            export_html=None,
            stats=False,
            show_errors=False,
            group_by=None,
            relative_paths=True,
            human_sizes=False,
            icons=False,
            use_color=False,
            sort="name",
            short=False,
            long=False,
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            OutputRenderer(scan_result, args).render([entries[deep]])

        lines = stdout.getvalue().splitlines()
        self.assertTrue(any(line.startswith("| root") for line in lines[2:]))
        self.assertTrue(any(line.startswith("|   nested") for line in lines[2:]))
        self.assertTrue(any(line.startswith("|     deep.txt") for line in lines[2:]))

    def test_grouped_flat_table_output_has_no_blank_lines_between_sections(self) -> None:
        root = Path("root")
        alpha = root / "alpha.txt"
        beta = root / "beta.md"
        entries = {
            root: make_entry("root", "d", 0),
            alpha: make_entry("root/alpha.txt", "f", 1, size_bytes=5),
            beta: make_entry("root/beta.md", "f", 1, size_bytes=7),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(
            json=False,
            csv=False,
            flat=True,
            columns=["type", "size"],
            column_widths={},
            export_html=None,
            stats=False,
            show_errors=False,
            group_by="ext",
            relative_paths=True,
            human_sizes=False,
            icons=False,
            use_color=False,
            sort="name",
            short=False,
            long=False,
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            OutputRenderer(scan_result, args).render([entries[alpha], entries[beta]])

        lines = stdout.getvalue().splitlines()
        self.assertNotIn("", lines)
        self.assertIn(".md", lines[0])
        self.assertIn(".txt", lines[4])

    def test_column_width_override_changes_rendered_width(self) -> None:
        root = Path("root")
        sample = root / "very-long-folder-name" / "sample.txt"
        entries = {
            root: make_entry("root", "d", 0),
            sample: make_entry("root/very-long-folder-name/sample.txt", "f", 1, size_bytes=12),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(
            json=False,
            csv=False,
            flat=True,
            columns=["type", "relative_path"],
            column_widths={"relative_path": 12},
            export_html=None,
            stats=False,
            show_errors=False,
            group_by=None,
            relative_paths=True,
            human_sizes=False,
            icons=False,
            use_color=False,
            sort="name",
            short=False,
            long=False,
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            OutputRenderer(scan_result, args).render([entries[sample]])

        lines = stdout.getvalue().splitlines()
        self.assertIn("| Relative ... |", lines[0])
        self.assertIn("very-long...", lines[2])
        self.assertNotIn("very-long-folder-name/sample.txt", lines[2])

    def test_name_column_width_override_changes_rendered_width(self) -> None:
        root = Path("root")
        sample = root / "very-long-file-name-that-should-truncate.txt"
        entries = {
            root: make_entry("root", "d", 0),
            sample: make_entry("root/very-long-file-name-that-should-truncate.txt", "f", 1, size_bytes=12),
        }
        scan_result = ScanResult(root=root, entries=entries, errors=[])
        args = Namespace(
            json=False,
            csv=False,
            flat=True,
            columns=["type"],
            column_widths={"name": 12},
            export_html=None,
            stats=False,
            show_errors=False,
            group_by=None,
            relative_paths=True,
            human_sizes=False,
            icons=False,
            use_color=False,
            sort="name",
            short=False,
            long=False,
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            OutputRenderer(scan_result, args).render([entries[sample]])

        lines = stdout.getvalue().splitlines()
        self.assertIn("| Name         |", lines[0])
        self.assertIn("very-long...", lines[2])
        self.assertNotIn("very-long-file-name-that-should-truncate.txt", lines[2])


class DirectoryScannerTests(unittest.TestCase):
    def test_scanner_aggregates_direct_and_recursive_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "empty").mkdir()
            sparse = root / "sparse"
            sparse.mkdir()
            (sparse / "one.txt").write_text("one", encoding="utf-8")
            nested = root / "nested"
            nested.mkdir()
            inner = nested / "inner"
            inner.mkdir()
            (inner / "deep.txt").write_text("deep", encoding="utf-8")

            scan_result = DirectoryScanner(root=root, max_depth=None, hash_mode=None).scan()
            root_entry = scan_result.entries[root]
            nested_entry = scan_result.entries[nested]
            inner_entry = scan_result.entries[inner]
            empty_entry = scan_result.entries[root / "empty"]

            self.assertEqual(root_entry.direct_dirs, 3)
            self.assertEqual(root_entry.direct_files, 0)
            self.assertEqual(root_entry.recursive_files, 2)
            self.assertEqual(nested_entry.recursive_files, 1)
            self.assertEqual(nested_entry.deepest_nesting, 1)
            self.assertEqual(inner_entry.direct_files, 1)
            self.assertTrue(empty_entry.is_empty)

    def test_tree_visibility_preserves_ancestors(self) -> None:
        root = Path("root")
        nested = root / "nested"
        inner = nested / "inner"
        deep = inner / "deep.txt"
        entries = {
            root: make_entry("root", "d", 0, children=[nested]),
            nested: make_entry("root/nested", "d", 1, parent=root, children=[inner]),
            inner: make_entry("root/nested/inner", "d", 2, parent=nested, children=[deep]),
            deep: make_entry("root/nested/inner/deep.txt", "f", 3, parent=inner),
        }
        visible = compute_visible_tree(entries, [entries[deep]])
        self.assertEqual(visible, {root, nested, inner, deep})

    def test_hashing_is_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sample = root / "file.txt"
            sample.write_text("abc", encoding="utf-8")
            scan_result = DirectoryScanner(root=root, max_depth=None, hash_mode="both").scan()
            file_entry = scan_result.entries[sample]
            self.assertIsNotNone(file_entry.hash_md5)
            self.assertIsNotNone(file_entry.hash_sha256)

    def test_export_webapp_report_writes_html_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            sample = root / "file.txt"
            sample.write_text("abc", encoding="utf-8")
            scan_result = DirectoryScanner(root=root, max_depth=None, hash_mode=None).scan()
            output_path = root / "report.html"
            args = Namespace(
                relative_paths=True,
                human_sizes=True,
                sort="name",
                group_by=None,
                export_html=output_path,
            )

            written_path = export_webapp_report(scan_result, list(scan_result.entries.values()), args)
            self.assertEqual(written_path, output_path.resolve())
            content = written_path.read_text(encoding="utf-8")
            self.assertIn("Filesystem Report", content)
            self.assertIn("file.txt", content)
            self.assertIn("smartls web report", content)


if __name__ == "__main__":
    unittest.main()