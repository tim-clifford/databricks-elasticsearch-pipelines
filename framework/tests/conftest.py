"""Put the framework root on sys.path so the tests import pipeline_lib however pytest is invoked (from the
repo root as `python -m pytest framework/tests`, or from inside framework/), and keep the shell's
BUNDLE_VAR_ overrides out of the tests."""
import os
import shutil
import sys

import pytest

_FRAMEWORK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _FRAMEWORK_ROOT not in sys.path:
    sys.path.insert(0, _FRAMEWORK_ROOT)


@pytest.fixture(autouse=True)
def _no_ambient_bundle_var_overrides(monkeypatch):
    """gen_jobs rejects BUNDLE_VAR_ overrides of these variables, so one exported in the developer's or CI
    shell would fail unrelated generator tests; only the tests that set them on purpose see them."""
    for var in ("es_host_configs_json", "default_es_host_config"):
        monkeypatch.delenv(f"BUNDLE_VAR_{var}", raising=False)


@pytest.fixture(autouse=True)
def _no_local_variable_overrides(tmp_path_factory, monkeypatch):
    """gen_jobs also reads the git-ignored .databricks/bundle/<target>/variable-overrides.json next to
    databricks.yml. Point it at a copy of the repo's databricks.yml in an empty folder, so a developer's
    local overrides can't change test results (tests that build their own tree repoint it again)."""
    scripts = os.path.join(_FRAMEWORK_ROOT, "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    import gen_jobs

    copy = tmp_path_factory.mktemp("bundle") / "databricks.yml"
    shutil.copyfile(os.path.join(os.path.dirname(_FRAMEWORK_ROOT), "databricks.yml"), copy)
    monkeypatch.setattr(gen_jobs, "_DATABRICKS_YML", str(copy))
