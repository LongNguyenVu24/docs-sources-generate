"""Shared helpers: file walking, comment stripping, property files, redaction."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

DEFAULT_EXCLUDES = {
    ".git", ".idea", ".vscode", "target", "build", "dist", "out", "node_modules",
    "logs", "vendor", ".mvn", "__pycache__", ".gradle", "bin",
}


# --------------------------------------------------------------------------- files
def walk(root: Path, exts: Iterable[str], excludes: Iterable[str] = DEFAULT_EXCLUDES) -> Iterator[Path]:
    exts = tuple(exts)
    excl = set(excludes)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in excl and not d.startswith(".")]
        for fn in filenames:
            if fn.endswith(exts):
                yield Path(dirpath) / fn


def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


def rel(p: Path, root: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except ValueError:
        return p.as_posix()


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def strip_c_comments(src: str) -> str:
    """Blank out // and /* */ comments (Java, Go, JS...) keeping offsets and newlines intact."""
    out = list(src)
    i, n = 0, len(src)
    in_str: Optional[str] = None
    while i < n:
        c = src[i]
        if in_str:
            if c == "\\":
                i += 2
                continue
            if c == in_str:
                in_str = None
            i += 1
            continue
        if c in "\"'`":
            in_str = c
            i += 1
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            j = src.find("\n", i)
            j = n if j == -1 else j
            for k in range(i, j):
                out[k] = " "
            i = j
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            j = src.find("*/", i + 2)
            j = n if j == -1 else j + 2
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        i += 1
    return "".join(out)


def kebab(name: str) -> str:
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", name)
    return s.lower().strip("-")


# --------------------------------------------------------------------------- properties
def _flatten(obj, prefix: str, out: Dict[str, str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            _flatten(v, key, out)
    elif isinstance(obj, list):
        if all(not isinstance(x, (dict, list)) for x in obj):
            out[prefix] = ",".join("" if x is None else str(x) for x in obj)
        else:
            for i, v in enumerate(obj):
                _flatten(v, f"{prefix}[{i}]", out)
    else:
        out[prefix] = "" if obj is None else str(obj)


def _naive_yaml(text: str, out: Dict[str, str]) -> None:
    """Fallback indentation-based flattener used when a YAML doc cannot be parsed."""
    stack: List[tuple] = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith(("#", "-")) or raw.strip() == "---":
            continue
        indent = len(raw) - len(raw.lstrip())
        m = re.match(r"\s*([^:#]+?)\s*:\s*(.*)$", raw)
        if not m:
            continue
        key, val = m.group(1).strip().strip("'\""), m.group(2).split(" #")[0].strip()
        while stack and stack[-1][0] >= indent:
            stack.pop()
        full = ".".join([s[1] for s in stack] + [key])
        if val:
            out[full] = val.strip("'\"")
        else:
            stack.append((indent, key))


def load_yaml_props(p: Path) -> Dict[str, str]:
    text = read_text(p)
    out: Dict[str, str] = {}
    docs = re.split(r"^---\s*$", text, flags=re.M)
    for doc in docs:
        if not doc.strip():
            continue
        parsed = None
        if yaml is not None:
            try:
                parsed = yaml.safe_load(doc)
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            _flatten(parsed, "", out)
        else:
            _naive_yaml(doc, out)
    return out


def load_properties(p: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in read_text(p).splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        m = re.match(r"([^=:\s]+)\s*[=:]\s*(.*)", line)
        if m:
            out[m.group(1)] = m.group(2).strip()
    return out


PLACEHOLDER = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def resolve_placeholders(value: str, props: Dict[str, str], depth: int = 0) -> str:
    if depth > 5 or "${" not in value:
        return value

    def sub(m):
        key, default = m.group(1).strip(), m.group(2)
        if key in props:
            return resolve_placeholders(props[key], props, depth + 1)
        return default if default is not None else m.group(0)

    return PLACEHOLDER.sub(sub, value)


# --------------------------------------------------------------------------- redaction
SECRET_KEY = re.compile(r"(pass(word)?|secret|token|credential|private[-_]?key|api[-_]?key|access[-_]?key|jaas|sasl)", re.I)
URL_USERINFO = re.compile(r"(\w+://)[^/@\s:]+:[^/@\s]+@")


def redact(key: str, value: str) -> str:
    if not value:
        return value
    if value.startswith("ENC(") or SECRET_KEY.search(key):
        return "***"
    value = URL_USERINFO.sub(r"\1***:***@", value)
    value = re.sub(r"(password|pwd)=([^&;\s]+)", r"\1=***", value, flags=re.I)
    return value


ENDPOINT_KEY = re.compile(r"(^|[.\-_])(url|uri|urls|host|hosts|address|addresses|endpoint|endpoints|link|base[-_]?url|server|servers|bootstrap[-_]servers|defaultzone)$", re.I)


def config_endpoints(props: Dict[str, str]) -> Dict[str, str]:
    """Pick config entries that point at other systems, redacted."""
    out = {}
    for k, v in props.items():
        if not v or not ENDPOINT_KEY.search(k.split(".")[-1]) and not ENDPOINT_KEY.search(k):
            continue
        if k.startswith(("logging.", "management.", "info.")):
            continue
        rv = redact(k, resolve_placeholders(v, props))
        if rv == "***":
            continue
        out[k] = rv
    return dict(sorted(out.items()))
