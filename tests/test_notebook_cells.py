"""Every Databricks notebook in notebooks/ must parse CELL BY CELL. Databricks compiles each `# COMMAND ----------`
cell on its own, so a cell that only parses in the context of its neighbours (e.g. an indented block whose `if`
line ended up in the previous cell, or inside a comment) passes a whole-file lint and still fails at run time.
Magic lines (`%pip`, `# MAGIC`) are treated as comments, as Databricks strips them before compiling Python.
"""
import ast
import glob
import os

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = sorted(glob.glob(os.path.join(_REPO_ROOT, "notebooks", "*.py")))


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
