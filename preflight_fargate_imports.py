#!/usr/bin/env python3
"""Fail the image build if a fargate Action cannot import what it needs.

Why this exists
---------------
On 2026-10-08 at 11:00 UTC a scheduled Action died in 594 ms with
`ModuleNotFoundError: No module named 'numpy'`. The Action had been committed,
published in a wheel, baked into this image and scheduled, and nothing along
that path ever asked whether the image could import it. The answer was no, and
had been no since the Action was written.

What it checks
--------------
Every module under the installed `plus_engine.actions` that declares
`runtime = 'fargate'` is this image's responsibility -- fargate Actions run
HERE, not in Lambda. For each one we collect every third-party package it
imports, at module level AND inside functions, and import them for real.

Function-level imports matter more than top-level ones here. The convention in
plus_engine is to defer heavy imports so `crawl.py` can register an Action in a
plain environment. That convention hides exactly this bug: the module imports
fine, registration succeeds, the wheel publishes, the image builds, and the
Action explodes the first time it actually runs. `pdf/extract_images.py`
(`fitz`) and `image/resize.py` (`PIL`) use the same pattern.

An import guarded by `try: ... except ImportError:` is treated as optional and
only warned about -- `wikidrama/daily_scan.py` degrades to "review skipped"
without `anthropic`, which is a choice, not a break. Everything else is fatal.

Limits, stated plainly: this proves the packages import. It does not prove the
Action has enough memory, enough disk, or the right credentials.
"""
import ast
import importlib
import importlib.util
import pathlib
import sys

FIRST_PARTY = {'plus_engine', 'chalicelib', 'feaas', 'plus_core', 'src'}


def _is_fargate(tree):
    """A module-level or class-level `runtime = 'fargate'`."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for t in node.targets:
            if (isinstance(t, ast.Name) and t.id == 'runtime'
                    and isinstance(node.value, ast.Constant)
                    and node.value.value == 'fargate'):
                return True
    return False


def _optional_import_lines(tree):
    """Line numbers of imports inside a `try` that handles ImportError, which
    the Action has explicitly chosen to survive without."""
    lines = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        handled = any(
            (isinstance(h.type, ast.Name) and h.type.id in ('ImportError', 'ModuleNotFoundError'))
            or (isinstance(h.type, ast.Tuple) and any(
                isinstance(e, ast.Name) and e.id in ('ImportError', 'ModuleNotFoundError')
                for e in h.type.elts))
            for h in node.handlers)
        if not handled:
            continue
        for stmt in node.body:
            for sub in ast.walk(stmt):
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    lines.add(sub.lineno)
    return lines


def _module_file(dotted, pkg_root):
    """-> Path of a first-party module's source, or None if it is not a file we
    can read (a namespace package, a C extension, or simply not installed)."""
    rel = pathlib.Path(*dotted.split('.'))
    for cand in (pkg_root / rel.with_suffix('.py'), pkg_root / rel / '__init__.py'):
        if cand.is_file():
            return cand
    return None


def _collect(path, pkg_root, dotted, seen, out, required_ctx=True):
    """Walk `path`'s imports and recurse through FIRST-PARTY ones, accumulating
    third-party package roots into `out` as {root: required_bool}.

    The recursion is the point. `wikidrama/daily_scan.py` imports no numpy; it
    defers `from plus_engine.actions.wikidrama import _detector`, and numpy is
    in _detector. A check that stopped at the fargate module would have passed
    the very image that could not run it.
    """
    if path in seen:
        return
    seen.add(path)
    try:
        tree = ast.parse(path.read_text(encoding='utf-8'))
    except (SyntaxError, UnicodeDecodeError):
        return
    optional = _optional_import_lines(tree)
    pkg_parts = dotted.split('.')[:-1]

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = pkg_parts[:len(pkg_parts) - node.level + 1]
                names = ['.'.join(base + ([node.module] if node.module else []))]
            else:
                names = [node.module or '']
        else:
            continue
        required = required_ctx and node.lineno not in optional
        for name in names:
            if not name:
                continue
            root = name.split('.')[0]
            if root in sys.stdlib_module_names:
                continue
            if root in FIRST_PARTY:
                # `chalicelib.actions.x` and `plus_engine.actions.x` are the same
                # module under two names; the wheel renames the top package.
                for alias in {name, name.replace('chalicelib.actions', 'plus_engine.actions')}:
                    sub = _module_file(alias, pkg_root)
                    if sub:
                        _collect(sub, pkg_root, alias, seen, out, required)
                        break
                # An ImportFrom may name the symbol, not the module:
                # `from plus_engine.actions.wikidrama import _detector`.
                if isinstance(node, ast.ImportFrom) and not node.level:
                    for a in node.names:
                        sub = _module_file(f'{name}.{a.name}', pkg_root)
                        if sub:
                            _collect(sub, pkg_root, f'{name}.{a.name}', seen, out, required)
                continue
            out[root] = out.get(root, False) or required


def main():
    spec = importlib.util.find_spec('plus_engine.actions')
    if spec is None or not spec.submodule_search_locations:
        print('PREFLIGHT FAIL: plus_engine.actions is not installed', file=sys.stderr)
        return 1
    root = pathlib.Path(list(spec.submodule_search_locations)[0])

    fargate, failures, warnings = [], [], []
    for path in sorted(root.rglob('*.py')):
        try:
            tree = ast.parse(path.read_text(encoding='utf-8'))
        except (SyntaxError, UnicodeDecodeError) as e:
            warnings.append(f'{path.relative_to(root)}: unparseable ({e})')
            continue
        if not _is_fargate(tree):
            continue
        dotted = 'plus_engine.actions.' + '.'.join(
            path.relative_to(root).with_suffix('').parts)
        fargate.append(dotted)

        # The module itself must import.
        try:
            importlib.import_module(dotted)
        except Exception as e:
            failures.append(f'{dotted}: module import failed: {type(e).__name__}: {e}')
            continue

        # ...and so must everything it reaches for, whenever it reaches for
        # it, including through its own first-party helper modules.
        reached = {}
        _collect(path, root.parent.parent, dotted, set(), reached)
        for pkg, required in sorted(reached.items()):
            try:
                importlib.import_module(pkg)
            except Exception as e:
                msg = f'{dotted}: cannot import {pkg!r}: {type(e).__name__}: {e}'
                (failures if required else warnings).append(msg)

    print(f'preflight: checked {len(fargate)} fargate actions')
    for w in warnings:
        print(f'  WARN  {w}')
    for f in failures:
        print(f'  FAIL  {f}', file=sys.stderr)
    if not fargate:
        print('PREFLIGHT FAIL: found no fargate actions, so this check proved '
              'nothing -- the scan or the install layout changed', file=sys.stderr)
        return 1
    if failures:
        print(f'\nPREFLIGHT FAIL: {len(failures)} fargate action(s) cannot run in '
              f'this image. Add the missing package to pyproject.toml.', file=sys.stderr)
        return 1
    print('preflight: all fargate action imports resolve')
    return 0


if __name__ == '__main__':
    sys.exit(main())
