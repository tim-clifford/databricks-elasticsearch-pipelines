"""The docs must not point anywhere that doesn't exist.

Two kinds of pointer are checked:
- every relative Markdown link in README.md and docs/*.md: the target file exists, and a `#fragment` names a
  heading or `<a id="...">` in that file (GitHub's anchor rules);
- every `"<Section>" in docs/<file>.md` pointer in code, config and generator messages: that doc has a
  heading with that text. Python adjacent string literals are joined first, so a message split across
  lines is read as one string.
"""
import glob
import os
import re

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = [os.path.join(_REPO_ROOT, "README.md")] + sorted(glob.glob(os.path.join(_REPO_ROOT, "docs", "*.md")))
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_SECTION_POINTER = re.compile(r'"([^"\n]+)" in (docs/[\w-]+\.md)')


def _outside_fences(path):
    """(line number, line) for every line of a Markdown file outside ``` code fences."""
    fence = False
    for number, line in enumerate(open(path, encoding="utf-8"), 1):
        if line.lstrip().startswith("```"):
            fence = not fence
            continue
        if not fence:
            yield number, line


def _slug(heading):
    heading = re.sub(r"<[^>]+>", "", heading).strip().lower()
    heading = re.sub(r"[`*~]", "", heading)
    return re.sub(r"[^\w\- ]", "", heading).replace(" ", "-")


def headings(path):
    """The heading texts of a Markdown file, with inline code marks removed."""
    return [re.sub(r"`", "", m.group(2)).strip() for _, line in _outside_fences(path)
            if (m := re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line))]


def anchors(path):
    """Every anchor GitHub generates for a Markdown file: heading slugs (numbered when repeated) and <a id>."""
    found, seen = set(), {}
    for _, line in _outside_fences(path):
        found.update(re.findall(r'<a id="([^"]+)"', line))
        m = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", line)
        if m:
            slug = _slug(m.group(2))
            count = seen.get(slug, 0)
            seen[slug] = count + 1
            found.add(slug if count == 0 else f"{slug}-{count}")
    return found


def broken_links(path):
    """(line, target, problem) for every relative link in `path` that doesn't resolve."""
    broken = []
    for number, line in _outside_fences(path):
        for target in _LINK.findall(re.sub(r"`[^`]*`", "", line)):
            if re.match(r"^[a-z]+:", target):
                continue  # http:, https:, mailto:
            file_part, _, fragment = target.partition("#")
            dest = os.path.normpath(os.path.join(os.path.dirname(path), file_part)) if file_part else path
            if not os.path.exists(dest):
                broken.append((number, target, "no such file"))
            elif fragment and (not dest.endswith(".md") or fragment not in anchors(dest)):
                broken.append((number, target, "no such anchor"))
    return broken


def _joined_python_strings(source):
    """`source` with adjacent string literals ("a" "b", across lines, optionally f-prefixed) joined and \\" unescaped."""
    return re.sub(r'"\s*\n\s*f?"', "", source).replace('\\"', '"')


def section_pointers():
    """(file, section, doc) for every `"<Section>" in docs/<file>.md` pointer outside the docs themselves."""
    files = (glob.glob(os.path.join(_REPO_ROOT, "framework", "**", "*.py"), recursive=True)
             + glob.glob(os.path.join(_REPO_ROOT, "_pipelines", "**", "*.yml"), recursive=True)
             + [os.path.join(_REPO_ROOT, "databricks.yml")])
    found = []
    for path in files:
        if os.path.basename(path) == os.path.basename(__file__):
            continue
        text = open(path, encoding="utf-8").read()
        if path.endswith(".py"):
            text = _joined_python_strings(text)
        else:
            text = re.sub(r"\n\s*#\s*", " ", text)  # a YAML comment wrapped over several lines
        for section, doc in _SECTION_POINTER.findall(text):
            found.append((os.path.relpath(path, _REPO_ROOT), section, doc))
    return found


@pytest.mark.parametrize("path", DOCS, ids=lambda p: os.path.relpath(p, _REPO_ROOT))
def test_every_relative_link_resolves(path):
    assert broken_links(path) == []


def test_code_points_only_at_sections_that_exist():
    pointers = section_pointers()
    assert pointers, "expected the generator messages and config comments to point at doc sections"
    missing = [(f, section, doc) for f, section, doc in pointers
               if not os.path.exists(os.path.join(_REPO_ROOT, doc))
               or section not in headings(os.path.join(_REPO_ROOT, doc))]
    assert missing == []


def test_the_checks_catch_a_broken_link_and_a_stale_pointer(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text("# Title\n\n## A section\n\n[ok](#a-section) [bad](#gone) [missing](nope.md)\n"
                   "```\n[ignored](#inside-a-fence)\n```\n")
    assert [(t, p) for _, t, p in broken_links(str(doc))] == [("#gone", "no such anchor"),
                                                                ("nope.md", "no such file")]
    assert headings(str(doc)) == ["Title", "A section"]
    split = 'raise ValueError(\n    f"see \\"Elasticsearch host "\n    f"configs\\" in docs/setup.md")'
    assert _SECTION_POINTER.findall(_joined_python_strings(split)) == [("Elasticsearch host configs",
                                                                        "docs/setup.md")]
