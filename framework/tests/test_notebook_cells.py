"""Every Databricks notebook in framework/notebooks/ must parse CELL BY CELL. Databricks compiles each `# COMMAND ----------`
cell on its own, so a cell that only parses in the context of its neighbours (e.g. an indented block whose `if`
line ended up in the previous cell, or inside a comment) passes a whole-file lint and still fails at run time.
Magic lines (`%pip`, `# MAGIC`) are treated as comments, as Databricks strips them before compiling Python.
"""
import ast
import glob
import os
import re

import pytest

_FRAMEWORK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = sorted(glob.glob(os.path.join(_FRAMEWORK_ROOT, "notebooks", "*.py")))


def _cells(path):
    return open(path).read().split("# COMMAND ----------\n")


@pytest.mark.parametrize("path", NOTEBOOKS, ids=os.path.basename)
def test_every_notebook_cell_parses_on_its_own(path):
    assert NOTEBOOKS
    for i, cell in enumerate(_cells(path)):
        code = "\n".join(("#" + line) if line.lstrip().startswith("%") else line for line in cell.split("\n"))
        try:
            ast.parse(code)
        except SyntaxError as exc:
            pytest.fail(f"{os.path.basename(path)} cell {i}: {exc.msg} at line {exc.lineno}")


# The notebooks locate their roots from their own workspace path, which the bundle syncs to
# <bundle files>/framework/notebooks/<name>. These are the exact lines that do it; evaluated against this
# repo's tree (the same layout the bundle syncs), FRAMEWORK_ROOT must hold pipeline_lib and FILES_ROOT the
# _pipelines/ tree.
_ROOT_LINE = re.compile(r"^(_?FRAMEWORK_ROOT|FILES_ROOT) = (.+?)(\s+#.*)?$", re.M)
_WORKSPACE_EXPR = '"/Workspace" + _nb_path'


def test_notebooks_found():
    assert sorted(os.path.basename(p) for p in NOTEBOOKS) == [
        "build_wheel.py", "checkpoint_clear.py", "deploy_views.py", "es_diagnostics.py",
        "log_table_create.py", "log_table_prune.py", "run_index_pipeline.py"]


@pytest.mark.parametrize("path", NOTEBOOKS, ids=os.path.basename)
def test_notebook_roots_resolve_to_the_synced_layout(path):
    source = open(path).read()
    roots = {}
    for name, expr, _ in _ROOT_LINE.findall(source):
        if _WORKSPACE_EXPR in expr:
            expr = expr.replace(_WORKSPACE_EXPR, "_ws_path")
        roots[name] = eval(expr, {"os": os, "_ws_path": path[:-len(".py")]}, dict(roots))
    framework = roots.get("FRAMEWORK_ROOT", roots.get("_FRAMEWORK_ROOT"))
    if "from pipeline_lib" in source:
        assert framework is not None, "imports pipeline_lib but defines no FRAMEWORK_ROOT"
        assert os.path.isfile(os.path.join(framework, "pipeline_lib", "__init__.py"))
        assert re.search(r"sys\.path\.insert\(0, _?FRAMEWORK_ROOT\)", source)
    if '"_pipelines"' in source:
        assert "FILES_ROOT" in roots, "reads _pipelines/ but defines no FILES_ROOT"
        assert os.path.isdir(os.path.join(roots["FILES_ROOT"], "_pipelines", "pipeline_configs"))
    if not ("from pipeline_lib" in source or '"_pipelines"' in source):
        assert not roots  # e.g. build_wheel: needs neither root


def test_every_runtime_knob_is_a_runner_widget_that_is_read():
    # Each run-time job parameter reaches the runner as a widget of the same name; a knob in the registry that the
    # notebook never declares or reads would be generated into every job and silently ignored at run time.
    from pipeline_lib.config import RUNTIME_KNOB_NAMES
    source = open(os.path.join(_FRAMEWORK_ROOT, "notebooks", "run_index_pipeline.py")).read()
    for name in RUNTIME_KNOB_NAMES:
        assert f'dbutils.widgets.text("{name}",' in source, f"{name} is not declared as a widget"
        assert f'dbutils.widgets.get("{name}")' in source, f"{name} is never read"
