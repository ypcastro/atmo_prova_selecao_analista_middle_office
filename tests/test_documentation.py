from __future__ import annotations

import ast
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src" / "app"
DOC_REQUIRED_MODULES = [
    PROJECT_ROOT / "src" / "app" / "api" / "main.py",
    PROJECT_ROOT / "src" / "app" / "jobs" / "extract_job.py",
    PROJECT_ROOT / "src" / "app" / "core" / "pipeline_io.py",
    PROJECT_ROOT / "src" / "app" / "dashboard" / "streamlit_app.py",
    PROJECT_ROOT / "src" / "app" / "core" / "config.py",
    PROJECT_ROOT / "src" / "app" / "core" / "logging_setup.py",
    PROJECT_ROOT / "src" / "app" / "jobs" / "scheduler.py",
    PROJECT_ROOT / "src" / "app" / "ana" / "catalog.py",
]
DOCSTRING_COVERAGE_THRESHOLD = 0.70


def _load_ast(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))


def _module_docstring(path: Path) -> str | None:
    return ast.get_docstring(_load_ast(path))


def _iter_public_functions(tree: ast.Module):
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if not node.name.startswith("_"):
                yield node
            continue
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not child.name.startswith("_"):
                        yield child


def test_required_modules_have_module_docstrings() -> None:
    missing = []
    for path in DOC_REQUIRED_MODULES:
        if not _module_docstring(path):
            missing.append(path.relative_to(PROJECT_ROOT).as_posix())
    assert not missing, f"Missing module docstrings: {missing}"


def test_required_modules_have_public_function_docstrings() -> None:
    missing = []
    for path in DOC_REQUIRED_MODULES:
        tree = _load_ast(path)
        for fn in _iter_public_functions(tree):
            if ast.get_docstring(fn):
                continue
            rel_path = path.relative_to(PROJECT_ROOT).as_posix()
            missing.append(f"{rel_path}:{fn.name}")
    assert not missing, f"Missing public function docstrings: {missing}"


def test_project_docstring_coverage_threshold() -> None:
    total = 0
    documented = 0
    for path in SRC_ROOT.rglob("*.py"):
        tree = _load_ast(path)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            total += 1
            if ast.get_docstring(node):
                documented += 1

    coverage = (documented / total) if total else 1.0
    assert coverage >= DOCSTRING_COVERAGE_THRESHOLD, (
        f"Docstring coverage {coverage:.1%} below threshold "
        f"{DOCSTRING_COVERAGE_THRESHOLD:.1%} ({documented}/{total})."
    )


def test_markdown_has_no_local_absolute_links() -> None:
    md_files = sorted(PROJECT_ROOT.rglob("*.md"))
    patterns = {
        "windows_link_target": re.compile(r"\]\(/?[A-Za-z]:[/\\]"),
        "windows_path": re.compile(r"[A-Za-z]:\\"),
        "file_uri": re.compile(r"file://", re.IGNORECASE),
    }

    violations: list[str] = []
    for path in md_files:
        rel = path.relative_to(PROJECT_ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for name, pattern in patterns.items():
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                violations.append(f"{rel}:{line}:{name}")

    assert not violations, f"Found local absolute links in markdown files: {violations}"
