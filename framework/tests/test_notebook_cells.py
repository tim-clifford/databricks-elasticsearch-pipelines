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
        "build_wheel.py", "checkpoint_clear.py", "cluster_log_prune.py", "deploy_views.py", "es_diagnostics.py",
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


def _shipped_job_notebook_params():
    """(job file, notebook file, parameter name) for every base_parameter and job-level parameter of every task
    in every shipped job: the hand-authored framework/resources jobs and the generated _pipelines/resources jobs."""
    import yaml
    repo_root = os.path.dirname(_FRAMEWORK_ROOT)
    out = []
    for job_file in sorted(glob.glob(os.path.join(_FRAMEWORK_ROOT, "resources", "*.job.yml"))
                           + glob.glob(os.path.join(repo_root, "_pipelines", "resources", "*.job.yml"))):
        with open(job_file) as fh:
            jobs = yaml.safe_load(fh)["resources"]["jobs"]
        for job in jobs.values():
            job_params = [p["name"] for p in job.get("parameters", [])]
            for task in job["tasks"]:
                nb = task.get("notebook_task")
                if not nb:
                    continue
                notebook = os.path.normpath(os.path.join(os.path.dirname(job_file), nb["notebook_path"]))
                for name in list(nb.get("base_parameters", {})) + job_params:
                    out.append((os.path.relpath(job_file, repo_root), notebook, name))
    return out


@pytest.mark.parametrize("job_file,notebook,name", _shipped_job_notebook_params(),
                         ids=lambda v: os.path.basename(v) if isinstance(v, str) and os.sep in v else v)
def test_every_job_parameter_is_a_notebook_widget_that_is_read(job_file, notebook, name):
    # A job passes each parameter to its notebook as a widget of the same name. A name that drifts on either
    # side silently falls back to the widget's "" default (for view_schema_per_user that means "off": the jobs
    # would read the shared view schema while deploy_views writes the per-user one), so pin every pairing.
    source = open(notebook).read()
    assert f'dbutils.widgets.text("{name}",' in source, f"{job_file}: {name} is not a widget in {notebook}"
    assert f'dbutils.widgets.get("{name}")' in source, f"{job_file}: {name} is never read in {notebook}"


def test_job_notebook_params_found():
    params = _shipped_job_notebook_params()
    names = {(os.path.basename(nb), n) for _, nb, n in params}
    for expected in [("run_index_pipeline.py", "view_schema_per_user"), ("run_index_pipeline.py", "view_schema_user_name"),
                     ("deploy_views.py", "view_schema_per_user"), ("deploy_views.py", "views"),
                     ("cluster_log_prune.py", "dry_run"), ("cluster_log_prune.py", "cluster_log_retention_days")]:
        assert expected in names
    assert len(params) > 100   # 5 generated feed jobs (one task each, plus a 2-task group) x ~37 params + fixed jobs
