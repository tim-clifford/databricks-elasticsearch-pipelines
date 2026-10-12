"""Offline unit tests for pipeline_lib.checkpoint. No Spark, no cluster: plain pytest with a fake `ls`.

The classifier decides seed-vs-resume for streaming_start=new. A false "empty" on an EXISTING checkpoint
is a silent-data-loss bug (the no-op seed drains over an un-exported backlog), so the contract these
tests pin is: existence is decided ONLY from SUCCESSFUL listings, never from a listing exception, and an
existing checkpoint can never be classified EMPTY. The `unreadable-but-present` cases are the regressions
that a fuzzy exception-string implementation got wrong.
"""
import pytest

from pipeline_lib.checkpoint import EMPTY, HAS_OFFSET, UNKNOWN, checkpoint_offsets_state

CP = "/Volumes/cat/schema/vol/checkpoints/user/my_config"
OFFSETS = CP + "/offsets"
BASE_PARENT = "/Volumes/cat/schema/vol/checkpoints/user"  # parent of CP


class _Entry:
    # Mimics a dbutils.fs.ls FileInfo: only `.name` is read, and a directory entry may carry a "/".
    def __init__(self, name):
        self.name = name


def make_ls(tree):
    """Build an ls(path) from {path: [child names]}. A path absent from `tree` RAISES (like dbutils)."""
    def ls(path):
        key = path.rstrip("/")
        if key not in tree:
            raise FileNotFoundError(f"No such file or directory: {path}")
        return [_Entry(n) for n in tree[key]]
    return ls


def raising_ls(exc):
    """An ls that always raises `exc` - to prove existence is never inferred from an exception string."""
    def ls(path):
        raise exc
    return ls


# --------------------------------------------------------------- offsets dir lists (fast path)

@pytest.mark.parametrize("names", [["0"], ["0", "1", "2"], ["3", "metadata"], ["10", ".tmp"]])
def test_has_offset_when_offsets_dir_holds_a_digit_named_file(names):
    ls = make_ls({OFFSETS: names})
    assert checkpoint_offsets_state(CP, ls) == HAS_OFFSET


@pytest.mark.parametrize("names", [[], ["metadata"], [".tmp", "_spark_metadata"], ["commits"]])
def test_empty_when_offsets_dir_lists_without_a_digit_named_file(names):
    ls = make_ls({OFFSETS: names})
    assert checkpoint_offsets_state(CP, ls) == EMPTY


def test_trailing_slash_on_entry_names_is_ignored():
    # A subdir entry may come back as "0/"; it must still count as the committed batch offset "0".
    ls = make_ls({OFFSETS: ["0/"]})
    assert checkpoint_offsets_state(CP, ls) == HAS_OFFSET


# --------------------------------------------------------------- offsets dir does NOT list: walk up

def test_empty_when_offsets_absent_and_base_lists_without_offsets_child():
    # Genuine first run: the checkpoint dir exists (e.g. Spark just created it) but has no offsets child.
    ls = make_ls({CP: ["commits", "metadata"]})  # OFFSETS not in tree -> raises; CP lists, no "offsets"
    assert checkpoint_offsets_state(CP, ls) == EMPTY


def test_unknown_when_offsets_unreadable_but_base_shows_offsets_present():
    # THE data-loss regression: offsets dir exists (base shows it) but listing it failed transiently.
    # Must be UNKNOWN (safe fallback), never EMPTY - seeding here would drain over a real backlog.
    tree = {CP: ["offsets", "commits"]}  # OFFSETS itself absent from tree -> ls(OFFSETS) raises
    assert checkpoint_offsets_state(CP, make_ls(tree)) == UNKNOWN


def test_empty_when_base_absent_and_grandparent_lists_without_config_child():
    # First run where the per-config checkpoint dir does not exist yet: neither OFFSETS nor CP list, but
    # the parent lists and shows no my_config child => positively absent => EMPTY (seed engages).
    ls = make_ls({BASE_PARENT: ["other_config"]})
    assert checkpoint_offsets_state(CP, ls) == EMPTY


def test_unknown_when_base_unreadable_but_grandparent_shows_config_present():
    # CP exists (parent shows it) but neither CP nor OFFSETS could be listed (transient). CP might hold a
    # backlog, so UNKNOWN, not EMPTY.
    ls = make_ls({BASE_PARENT: ["my_config", "other_config"]})
    assert checkpoint_offsets_state(CP, ls) == UNKNOWN


def test_unknown_when_nothing_lists_at_all():
    # Every listing fails (all transient/permission). Existence is never inferred from an exception, so
    # the classifier fails closed to UNKNOWN rather than guessing EMPTY.
    assert checkpoint_offsets_state(CP, raising_ls(RuntimeError("transient"))) == UNKNOWN


def test_not_found_exception_text_never_yields_empty():
    # Even when the raised exception literally says "No such file or directory", a bare exception (no
    # successful ancestor listing) must NOT be read as a first run - this is the whole point of the fix.
    assert checkpoint_offsets_state(CP, raising_ls(FileNotFoundError("No such file or directory"))) == UNKNOWN


def test_climb_reaches_a_listable_ancestor_several_levels_up():
    # OFFSETS, CP, and BASE_PARENT all fail to list; the volume-level ancestor lists and shows the
    # 'checkpoints' subtree present -> UNKNOWN (something exists below, could not resolve the leaf).
    vol = "/Volumes/cat/schema/vol"
    assert checkpoint_offsets_state(CP, make_ls({vol: ["checkpoints"]})) == UNKNOWN
    # ...and absent at that level -> EMPTY (the whole checkpoints subtree is gone).
    assert checkpoint_offsets_state(CP, make_ls({vol: ["artifacts"]})) == EMPTY


def test_max_climb_is_bounded():
    # With max_climb=1 only the immediate base is tried; if it does not list, fail closed to UNKNOWN
    # rather than climbing further.
    ls = make_ls({BASE_PARENT: ["other_config"]})  # would be EMPTY if allowed to climb to the parent
    assert checkpoint_offsets_state(CP, ls, max_climb=1) == UNKNOWN
