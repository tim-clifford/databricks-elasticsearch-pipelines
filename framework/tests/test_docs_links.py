"""The docs must not point anywhere that doesn't exist.

Checked:
- every relative Markdown link in README.md and docs/*.md: the target file exists, and a `#fragment` names a
  heading or `<a id="...">` in that file;
- every pointer from code and config to the docs: a `docs/<file>.md` mention names a file that exists, and a
  `"<Section>" in docs/<file>.md` (or `the README "<Section>"`) pointer names a heading in it. Python strings
  are read with ast (so adjacent literals are joined exactly) and comments with tokenize (consecutive comment
  lines joined); YAML comments are joined across wrapped lines.

The anchor rules here cover the Markdown the docs use. Forms they don't model are rejected outright, so a link
can't escape the check: reference-style links, and headings with underscore emphasis or HTML entities.
"""
import ast
import glob
import io
import os
import re
import tokenize

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = [os.path.join(_REPO_ROOT, "README.md")] + sorted(glob.glob(os.path.join(_REPO_ROOT, "docs", "*.md")))
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_REFERENCE_DEFINITION = re.compile(r"^\s*\[[^\]]+\]:\s")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_DOC_MENTION = re.compile(r"\bdocs/[\w-]+\.md\b")
_SECTION_POINTER = re.compile(r'"([^"\n]+)" in (docs/[\w-]+\.md)|the README "([^"\n]+)"')


def _outside_fences(path):
    """(line number, line) for every line of a Markdown file outside ``` or ~~~ code fences."""
    fence = None  # the opening fence string while inside one
    for number, line in enumerate(open(path, encoding="utf-8"), 1):
        m = re.match(r"^\s*(`{3,}|~{3,})", line)
        if fence is None and m:
            fence = m.group(1)
            continue
        if fence is not None:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
            continue
        yield number, line


def _slug(heading):
    heading = re.sub(r"<[^>]+>", "", heading).strip().lower()
    heading = heading.replace("`", "").replace("*", "").replace("~", "")
    return re.sub(r"[^\w\- ]", "", heading).replace(" ", "-")


def heading_lines(path):
    """(line number, heading text) for every heading, with inline code marks removed."""
    return [(number, m.group(2).replace("`", "").strip()) for number, line in _outside_fences(path)
            if (m := _HEADING.match(line))]


def headings(path):
    return [text for _, text in heading_lines(path)]


def anchors(path):
    """Every anchor GitHub generates for a Markdown file: heading slugs (numbered when repeated) and <a id>."""
    found, seen = set(), {}
    for _, line in _outside_fences(path):
        found.update(re.findall(r'<a id="([^"]+)"', line))
        m = _HEADING.match(line)
        if m:
            slug = _slug(m.group(2))
            count = seen.get(slug, 0)
            seen[slug] = count + 1
            found.add(slug if count == 0 else f"{slug}-{count}")
    return found


def broken_links(path):
    """(line, target, problem) for every link in `path` that doesn't resolve or can't be checked."""
    broken = []
    for number, line in _outside_fences(path):
        if _REFERENCE_DEFINITION.match(line):
            broken.append((number, line.strip(), "reference-style link (use an inline link)"))
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


def unsupported_headings(path):
    """(line, heading) for headings whose GitHub anchor these rules don't model."""
    return [(number, text) for number, text in heading_lines(path)
            if re.search(r"&\w+;|&#\d+;", text) or re.search(r"(^|\W)_[^_\s][^_]*_(\W|$)", text)]


def python_texts(source):
    """Every string literal (adjacent literals joined, as Python joins them) and every comment block."""
    texts = []
    tree = ast.parse(source)
    inside_fstring = {id(v) for node in ast.walk(tree) if isinstance(node, ast.JoinedStr) for v in node.values}
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in inside_fstring:
            texts.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            texts.append("".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values))
    block, last_line = [], None
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            if last_line is not None and tok.start[0] != last_line + 1:
                texts.append(" ".join(block))
                block = []
            block.append(tok.string.lstrip("#").strip())
            last_line = tok.start[0]
    if block:
        texts.append(" ".join(block))
    return texts


def yaml_texts(source):
    """The file with each run of wrapped `#` comment lines joined into one line."""
    return [re.sub(r"\n\s*#\s*", " ", source)]


def _pointer_files():
    files = (glob.glob(os.path.join(_REPO_ROOT, "framework", "**", "*.py"), recursive=True)
             + glob.glob(os.path.join(_REPO_ROOT, "_pipelines", "**", "*.yml"), recursive=True)
             + [os.path.join(_REPO_ROOT, "databricks.yml")])
    return [p for p in files if os.path.abspath(p) != os.path.abspath(__file__)]


def doc_pointers():
    """(file, doc, section or None) for every pointer from code or config into the docs."""
    found = []
    for path in _pointer_files():
        source = open(path, encoding="utf-8").read()
        texts = python_texts(source) if path.endswith(".py") else yaml_texts(source)
        rel = os.path.relpath(path, _REPO_ROOT)
        for text in texts:
            for doc in _DOC_MENTION.findall(text):
                found.append((rel, doc, None))
            for section, doc, readme_section in _SECTION_POINTER.findall(text):
                found.append((rel, doc, section) if doc else (rel, "README.md", readme_section))
    return found


@pytest.mark.parametrize("path", DOCS, ids=lambda p: os.path.relpath(p, _REPO_ROOT))
def test_every_relative_link_resolves(path):
    assert broken_links(path) == []


@pytest.mark.parametrize("path", DOCS, ids=lambda p: os.path.relpath(p, _REPO_ROOT))
def test_headings_use_only_forms_the_anchor_rules_model(path):
    assert unsupported_headings(path) == []


def test_code_points_only_at_docs_and_sections_that_exist():
    pointers = doc_pointers()
    assert any(section for _, _, section in pointers), "expected code and config to point at doc sections"
    missing = [(f, doc, section) for f, doc, section in pointers
               if not os.path.exists(os.path.join(_REPO_ROOT, doc))
               or (section is not None and section not in headings(os.path.join(_REPO_ROOT, doc)))]
    assert missing == []


def test_the_checks_catch_what_they_claim_to(tmp_path):
    doc = tmp_path / "doc.md"
    doc.write_text("# Title\n\n## A section\n\n[ok](#a-section) [bad](#gone) [missing](nope.md)\n"
                   "```\n[ignored](#inside-a-fence)\n```\n~~~~\n```\n[ignored](#still-fenced)\n~~~~\n"
                   "[ref]: https://example.com\n## _emphasis_ &amp; more\n")
    assert [(t, p) for _, t, p in broken_links(str(doc))] == [
        ("#gone", "no such anchor"), ("nope.md", "no such file"),
        ("[ref]: https://example.com", "reference-style link (use an inline link)")]
    assert headings(str(doc)) == ["Title", "A section", "_emphasis_ &amp; more"]
    assert [h for _, h in unsupported_headings(str(doc))] == ["_emphasis_ &amp; more"]

    source = ('def f():\n    """A docstring line ending in a quote: "x"\n    "not a pointer" in docs/a.md"""\n'
              '    raise ValueError(\n        f"see \\"Elasticsearch host "\n        f"configs\\" in docs/setup.md")\n'
              '# wrapped comment: see "Job\n# groups" in docs/feed-configuration.md\n')
    texts = python_texts(source)
    pointers = [m for t in texts for m in _SECTION_POINTER.findall(t)]
    assert ("Elasticsearch host configs", "docs/setup.md", "") in pointers
    assert ("Job groups", "docs/feed-configuration.md", "") in pointers
    assert ("not a pointer", "docs/a.md", "") in pointers  # a docstring is a real string: still checked
    assert sum(1 for t in texts for _ in _DOC_MENTION.finditer(t)) == 3
