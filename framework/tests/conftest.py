"""Put the framework root on sys.path so the tests import pipeline_lib however pytest is invoked (from the
repo root as `python -m pytest framework/tests`, or from inside framework/)."""
import os
import sys

_FRAMEWORK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _FRAMEWORK_ROOT not in sys.path:
    sys.path.insert(0, _FRAMEWORK_ROOT)
