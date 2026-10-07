"""Every Python file in the repo must parse on the OLDEST Python the code runs on: DBR 15.3+, whose Python is 3.11.
Python 3.12 (PEP 701) allows a string inside an f-string's {expression} to reuse the f-string's own quote
(f"{', '.join(f"'{t}'" for t in ts)}"); 3.11 rejects the whole module with a SyntaxError. The test suite itself
runs on a newer Python, where such code parses fine, so this test looks for the construct in the token stream.
"""
import glob
import io
import os
import tokenize

import pytest

# The f-string tokens this test reads exist from Python 3.12. On an older Python the construct cannot parse at all,
# so importing the code under test already fails there.
pytestmark = pytest.mark.skipif(not hasattr(tokenize, "FSTRING_START"), reason="needs Python 3.12+ tokenize")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = sorted(p for pattern in ("pipeline_lib/*.py", "notebooks/*.py", "scripts/*.py", "tests/*.py")
               for p in glob.glob(os.path.join(_REPO_ROOT, pattern)))


def _quote(token_string):
    """The quote of a string or f-string start token: ' " ''' or \"\"\" (after any r/b/u/f prefix)."""
    body = token_string.lstrip("rRbBuUfF")
    return body[:3] if body[:3] in ("'''", '"""') else body[:1]


def quote_reuse(source):
    """(line, quote) for every string inside an f-string expression that reuses an enclosing f-string's quote."""
    found = []
    open_quotes = []  # the quote of each f-string we are inside, innermost last
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.FSTRING_START:
            q = _quote(tok.string)
            if q in open_quotes:
                found.append((tok.start[0], q))
            open_quotes.append(q)
        elif tok.type == tokenize.FSTRING_END:
            open_quotes.pop()
        elif tok.type == tokenize.STRING and open_quotes and _quote(tok.string) in open_quotes:
            found.append((tok.start[0], _quote(tok.string)))
    return found


def test_quote_reuse_detector_catches_the_312_only_form_and_allows_the_311_form():
    assert quote_reuse('x = f"{", ".join(ts)}"\n') == [(1, '"')]
    assert quote_reuse('x = f"{f"{t}" for t in ts}"\n') == [(1, '"')]
    assert quote_reuse("x = f\"{', '.join(f\"'{t}'\" for t in ts)}\"\n") == [(1, '"')]
    assert quote_reuse("x = f\"{', '.join(ts)}\"\n") == []
    assert quote_reuse("x = f\"{d['k']}\" + f'{d[\"k\"]}'\n") == []


@pytest.mark.parametrize("path", FILES, ids=lambda p: os.path.relpath(p, _REPO_ROOT))
def test_no_python_312_only_fstring_quote_reuse(path):
    assert FILES
    assert quote_reuse(open(path).read()) == [], f"{path}: f-string reuses its own quote (Python 3.12+ only)"
