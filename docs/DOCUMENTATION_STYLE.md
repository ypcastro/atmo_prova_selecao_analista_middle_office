# Documentation Style Guide

This project uses **Google-style docstrings** for Python code documentation.

## Scope

1. Docstrings are required for:
- Public functions.
- Public classes.
- Public module entrypoints (module-level docstring).
- Private functions that are operationally critical (I/O, scheduling, persistence, API behavior).
2. Inline comments should be used only when code intent is not obvious.

## Language Rules

1. Code docstrings: English (technical and concise).
2. Operational guides (`README`, `RUNBOOK`, `DECISIONS`): Portuguese.
3. Keep terms and field names consistent with code (`record_id`, `watermark`, `checkpoint`).

## Function Template

```python
def function_name(arg1: str, arg2: int) -> dict[str, object]:
    """Short imperative summary.

    Args:
        arg1: What this argument controls.
        arg2: Any relevant constraints or range.

    Returns:
        dict[str, object]: What is returned and key semantics.

    Raises:
        ValueError: When input is invalid.

    Side Effects:
        Writes files and updates metadata state.
    """
```

Notes:

1. `Raises` and `Side Effects` are mandatory when applicable.
2. Keep summary line under ~100 characters when possible.
3. Avoid repeating type information already present in function signature.

## Module Template

```python
"""Single-line module responsibility summary."""
```

## FastAPI Endpoint Documentation

1. Every route should define `summary` and `description` in the decorator.
2. Endpoint function docstring should include:
- What it returns.
- Key filters/inputs (`Args` when applicable).
- Error conditions (`Raises` for HTTP exceptions).

## PowerShell Script Help

Use comment-based help at the top of each script:

1. `.SYNOPSIS`
2. `.DESCRIPTION`
3. `.PARAMETER` for each user-facing parameter
4. `.EXAMPLE` (at least one practical invocation)

## Quality Gate

Validation command:

```powershell
$env:PYTHONPATH='src'
python -m pytest -q tests/test_documentation.py
```
