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


def test_runner_passes_each_write_knob_from_its_own_widget():
    # write_config_overrides receives every one of its parameters, and each one from the widget OF THE SAME
    # NAME: a swapped (retry_backoff_initial=RETRY_BACKOFF_MAX) or dropped argument would otherwise pass every
    # other test while silently ignoring a per-run override.
    import inspect
    from pipeline_lib.config import write_config_overrides
    path = os.path.join(_FRAMEWORK_ROOT, "notebooks", "run_index_pipeline.py")
    tree = _file_ast(path)
    widget_of = {}   # NAME -> widget it was read from, for `NAME = dbutils.widgets.get("w")...`
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            for call in ast.walk(node.value):
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "get"
                        and ast.unparse(call.func.value) == "dbutils.widgets" and call.args
                        and isinstance(call.args[0], ast.Constant)):
                    widget_of[node.targets[0].id] = call.args[0].value
    calls = _calls(path, "write_config_overrides")
    assert len(calls) == 1
    call = calls[0]
    params = list(inspect.signature(write_config_overrides).parameters)
    passed = dict(zip(params, call.args))
    passed.update({kw.arg: kw.value for kw in call.keywords})
    assert set(passed) == set(params), f"not passed: {sorted(set(params) - set(passed))}"
    for name, value in passed.items():
        assert isinstance(value, ast.Name), f"{name} is not passed a plain variable"
        assert widget_of.get(value.id) == name, f"{name} is passed {value.id}, read from {widget_of.get(value.id)!r}"


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
    calls = _widget_calls(notebook)
    assert ("text", name) in calls, f"{job_file}: {name} is not a widget in {notebook}"
    assert ("get", name) in calls, f"{job_file}: {name} is never read in {notebook}"


def _widget_calls(path):
    """{(method, name)} for every real `dbutils.widgets.<method>("<name>", ...)` call in a notebook, parsed cell
    by cell, so a call that only appears in a comment or a string doesn't count."""
    calls = set()
    for cell in _cells(path):
        code = "\n".join(("#" + line) if line.lstrip().startswith("%") else line for line in cell.split("\n"))
        for node in ast.walk(ast.parse(code)):
            f = getattr(node, "func", None)
            if (isinstance(node, ast.Call) and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Attribute)
                    and f.value.attr == "widgets" and isinstance(f.value.value, ast.Name)
                    and f.value.value.id == "dbutils" and node.args and isinstance(node.args[0], ast.Constant)):
                calls.add((f.attr, node.args[0].value))
    return calls


def test_widget_calls_ignore_comments_and_strings(tmp_path):
    nb = tmp_path / "nb.py"
    nb.write_text('# dbutils.widgets.text("a", "")\nx = \'dbutils.widgets.get("a")\'\n'
                  'dbutils.widgets.text("b", "")\nB = dbutils.widgets.get("b")\n')
    assert _widget_calls(str(nb)) == {("text", "b"), ("get", "b")}


def test_job_notebook_params_found():
    params = _shipped_job_notebook_params()
    names = {(os.path.basename(nb), n) for _, nb, n in params}
    for expected in [("run_index_pipeline.py", "view_schema_per_user"), ("run_index_pipeline.py", "view_schema_user_name"),
                     ("deploy_views.py", "view_schema_per_user"), ("deploy_views.py", "views"),
                     ("cluster_log_prune.py", "dry_run"), ("cluster_log_prune.py", "cluster_log_retention_days")]:
        assert expected in names
    # Every shipped job file contributes (a renamed glob or a skipped task type would drop one), and the total
    # is pinned so a job or task that stops being collected fails here. Update the count when a job, task or
    # parameter is added or removed.
    repo_root = os.path.dirname(_FRAMEWORK_ROOT)
    job_files = {os.path.relpath(f, repo_root) for f in
                 glob.glob(os.path.join(_FRAMEWORK_ROOT, "resources", "*.job.yml"))
                 + glob.glob(os.path.join(repo_root, "_pipelines", "resources", "*.job.yml"))}
    assert len(job_files) == 12 and {j for j, _, _ in params} == job_files
    assert len(params) == 238



def _file_ast(path):
    """The whole notebook as one module (cell separators are comments; magic lines commented out), so line
    numbers are file-wide and calls in different cells can be ordered."""
    src = open(path).read()
    return ast.parse("\n".join(("#" + line) if line.lstrip().startswith("%") else line for line in src.split("\n")))


def _calls(path, func_name):
    """Every call to `func_name(...)` in a notebook, as ast.Call nodes with file-wide line numbers."""
    return [n for n in ast.walk(_file_ast(path))
            if isinstance(n, ast.Call) and getattr(n.func, "id", None) == func_name]


def _attr_calls(path, attr):
    """Every `<x>.<attr>(...)` call in a notebook, with file-wide line numbers."""
    return [n for n in ast.walk(_file_ast(path))
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == attr]


def _passes(call, name):
    """Does `call` pass the variable `name` (positionally or as any keyword)?"""
    args = list(call.args) + [k.value for k in call.keywords]
    return any(isinstance(a, ast.Name) and a.id == name for a in args)


def test_deploy_views_threads_the_per_user_suffix():
    # deploy_views must create views where the jobs read them: every view_substitutions / shared_view_conflict
    # call gets the suffix, and the per-user pre-flight and schema guard both run.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "deploy_views.py")
    for fn in ("view_substitutions", "shared_view_conflict", "per_user_preflight_problem"):
        calls = _calls(nb, fn)
        assert calls and all(_passes(c, "VIEW_SCHEMA_USER") for c in calls), fn
    (vsu,) = _calls(nb, "view_schema_user")
    assert [a.id for a in vsu.args] == ["VIEW_SCHEMA_PER_USER", "VIEW_SCHEMA_USER_NAME"]
    # Order: the pre-flight before anything deploys; the schema guard before the view's CREATE.
    (pre,) = _calls(nb, "per_user_preflight_problem")
    (ensure,) = _calls(nb, "ensure_per_user_schema")
    creates = [c for c in _attr_calls(nb, "sql") if c.args and getattr(c.args[0], "id", None) == "rendered"]
    loop = min(c.lineno for c in _calls(nb, "shared_view_conflict"))
    assert creates and pre.lineno < loop and ensure.lineno < creates[0].lineno


def test_runner_applies_the_per_user_suffix_exactly_once():
    # The runner moves the view once (apply_view_schema_user) and never passes the suffix to
    # view_substitutions again, which would make it <schema>_<user>_<user>.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "run_index_pipeline.py")
    applies = _calls(nb, "apply_view_schema_user")
    assert len(applies) == 1 and _passes(applies[0], "VIEW_SCHEMA_USER")
    for c in _calls(nb, "view_substitutions"):
        assert not _passes(c, "VIEW_SCHEMA_USER") and not any(k.arg == "view_schema_user" for k in c.keywords)



def test_cluster_log_prune_passes_its_parameters_through():
    # dry_run=true must reach run_prune (a literal False there would delete on a dry run), and root/cutoff
    # must be passed in the right positions.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "cluster_log_prune.py")
    (call,) = _calls(nb, "run_prune")
    names = [getattr(a, "id", None) for a in call.args[:4]]
    assert names == ["ROOT", "RETENTION_DAYS", "CUTOFF_MS", "DRY_RUN"]
    assert [ast.unparse(a) for a in call.args[4:6]] == ["dbutils.fs.ls", "dbutils.fs.rm"]


def test_cluster_log_prune_derives_dry_run_from_its_widget():
    # How DRY_RUN is computed, not just that it reaches run_prune: an inverted or constant value would
    # delete files on `--params dry_run=true`.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "cluster_log_prune.py")
    assigns = {t.id: ast.unparse(n.value) for n in ast.walk(_file_ast(nb)) if isinstance(n, ast.Assign)
               for t in n.targets if isinstance(t, ast.Name) and t.id in ("DRY_RUN", "DRY_RUN_RAW")}
    assert assigns == {"DRY_RUN_RAW": "dbutils.widgets.get('dry_run')",
                       "DRY_RUN": "require_es_flag(DRY_RUN_RAW, 'dry_run') == 'true'"}
    assert len([n for n in ast.walk(_file_ast(nb)) if isinstance(n, ast.Assign)
                and any(getattr(t, "id", None) == "DRY_RUN" for t in n.targets)]) == 1


def test_deploy_views_preflight_sees_every_config():
    # The collision check must see every loaded config (a per-user schema can land on a schema a config
    # outside the subset names); only the unresolved-config check is narrowed to the subset.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "deploy_views.py")
    (pre,) = _calls(nb, "per_user_preflight_problem")
    assert [ast.unparse(a) for a in pre.args[:2]] == [
        "[entry for entries in configs_by_view.values() for entry in entries]",
        "selected_config_labels(sql_files, configs_by_view)"]
    tree = _file_ast(nb)
    rebinds = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
               and any(getattr(t, "id", None) == "configs_by_view" for t in n.targets)]
    assert len(rebinds) == 1 and rebinds[0].lineno < pre.lineno
    # The subset must already be applied (sql_files = _selected) when the pre-flight reads sql_files.
    (subset,) = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and getattr(n.value, "id", None) == "_selected"
                 and any(getattr(t, "id", None) == "sql_files" for t in n.targets)]
    assert subset.lineno < pre.lineno


def test_deploy_views_guards_each_per_user_schema_once_as_the_run_identity():
    # ensure_per_user_schema checks ownership against _ME (the current_user() result), and runs once per
    # schema: inside `if _schema not in _schemas_created`, which then records the schema.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "deploy_views.py")
    tree = _file_ast(nb)
    (call,) = _calls(nb, "ensure_per_user_schema")
    assert [ast.unparse(a) for a in call.args[:2]] == ["_schema", "_ME"]
    (me,) = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
             and any(getattr(t, "id", None) == "_ME" for t in n.targets)]
    assert "current_user()" in ast.unparse(me.value)
    (guard,) = [n for n in ast.walk(tree) if isinstance(n, ast.If)
                and ast.unparse(n.test) == "_schema not in _schemas_created"]
    body = [ast.unparse(n) for n in guard.body]
    assert guard.body[0].lineno <= call.lineno <= guard.body[-1].end_lineno   # the call is inside the guard
    assert body[-1] == "_schemas_created.add(_schema)"


def test_deploy_views_deploys_only_the_selected_files():
    # The subset is applied by reassigning sql_files before the deploy loop iterates it.
    nb = os.path.join(_FRAMEWORK_ROOT, "notebooks", "deploy_views.py")
    tree = _file_ast(nb)
    (sel,) = _calls(nb, "selected_view_files")
    assigns = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
               and any(getattr(t, "id", None) == "sql_files" for t in n.targets)
               and getattr(n.value, "id", None) == "_selected"]
    loops = [n for n in ast.walk(tree) if isinstance(n, ast.For) and getattr(n.iter, "id", None) == "sql_files"]
    assert len(assigns) == 1 and loops and sel.lineno < assigns[0].lineno < min(l.lineno for l in loops)
