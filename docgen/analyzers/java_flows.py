"""Lightweight Java call graph -> function flows with control structure.

Parses classes, injected fields, methods and method bodies (string-aware brace matching, no
compiler needed). From every entry point (REST handler, Rabbit/Kafka listener, @Scheduled job) it
walks controller -> service -> helper classes and records, in source order:

* calls between the service's own classes, and private-method self calls
* boundary calls (Feign/REST, gRPC, repository/DB, Redis, Kafka, RabbitMQ, MinIO, raw HTTP)
* control structure: if/else-if/else and switch (alt), for/while/do/forEach (loop), catch (opt)
* throws (`throw new X`, `orElseThrow(() -> new X)`) and the handler's return type

Blocks that end up containing nothing are pruned, so diagrams only show logic that matters.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from ..model import Evidence, Flow, Service, Step
from ..util import line_of, resolve_placeholders

KEYWORDS = {"if", "for", "while", "switch", "catch", "synchronized", "return", "new", "throw", "else", "try",
            "do", "super", "this", "case", "assert", "finally", "yield"}
MODIFIERS = {"public", "private", "protected", "static", "final", "abstract", "synchronized", "default", "native",
             "transient", "volatile", "strictfp"}
DEFAULT_SKIP_PACKAGES = {"dto", "dtos", "entity", "entities", "model", "models", "enums", "enum", "constants",
                         "constant", "exception", "exceptions", "utils", "util", "records", "config",
                         "configuration", "aop", "filter", "filters", "request", "response", "requests",
                         "responses", "jpa"}
BOUNDARY_TYPES = [
    (re.compile(r"^(\w+)Grpc\.\w+Stub$"), "grpc", None),
    (re.compile(r"^(?:Reactive)?KafkaTemplate$"), "kafka", "Kafka"),
    (re.compile(r"^(?:Rabbit|Amqp)Template$"), "rabbitmq", "RabbitMQ"),
    (re.compile(r"^(?:String|Reactive)?Redis(?:Template|Operations)$|^Jedis\w*$|^RedissonClient$"), "cache", "Redis"),
    (re.compile(r"^(?:RestTemplate|OkHttpClient|WebClient|HttpClient|CloseableHttpClient)$"), "http-dynamic", "HTTP (dynamic URL)"),
    (re.compile(r"^(?:Minio(?:Admin)?Client|S3Client|AmazonS3)$"), "storage", "MinIO / S3"),
    (re.compile(r"^(?:NamedParameter)?JdbcTemplate$|^EntityManager$|^DataSource$"), "db", None),
    (re.compile(r"^SimpMessagingTemplate$"), "other", "WebSocket clients"),
    (re.compile(r"^JDA$"), "other", "Discord"),
    (re.compile(r"^(?:JavaMailSender|MailSender)$"), "other", "SMTP"),
]
REPO_BASE = re.compile(r"\b(?:Jpa|Crud|PagingAndSorting|ListCrud|Mongo|Reactive\w*|R2dbc|Elasticsearch)?Repository\s*<")
FIELD_RE = re.compile(r"(?:^|\s)((?:[\w.]+)(?:<[^;=]*?>)?(?:\[\])?)\s+(\w+)\s*(?:=[^;]*)?$", re.S)
LOCAL_RE = re.compile(r"(?<![\w.])([A-Z][\w.]*(?:<[^;=()]*?>)?)\s+(\w+)\s*(?:=|:)")
CALL_RE = re.compile(r"(?<![\w.$])(?:this\s*\.\s*)?(\w+)\s*\.\s*(?:with\w+\s*\((?:[^()]|\([^()]*\))*\)\s*\.\s*)*(\w+)\s*\(")
SELF_RE = re.compile(r"(?<![\w.$])(\w+)\s*\(")
THROW_RE = re.compile(r"\bthrow\s+new\s+([\w.]+)|orElseThrow\s*\(\s*(?:\(\s*\)\s*->\s*)?new\s+([\w.]+)|orElseThrow\s*\(\s*([\w.]+)::new")
ACCESSOR_BODY = re.compile(r"^\s*(return\s+(this\.)?\w+\s*;|this\.\w+\s*=\s*\w+\s*;)?\s*$")
STRUCT = {"alt", "else", "loop", "opt", "end", "throw", "return", "self"}


# ============================================================================ text helpers
def blank_strings(code: str) -> str:
    """Replace string/char literal contents with spaces so braces inside strings don't confuse matching."""
    out = list(code)
    i, n = 0, len(code)
    while i < n:
        if code.startswith('"""', i):
            j = code.find('"""', i + 3)
            j = n if j == -1 else j
            for k in range(i + 3, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j + 3
            continue
        c = code[i]
        if c in "\"'":
            j = i + 1
            while j < n and code[j] != c and code[j] != "\n":
                j += 2 if code[j] == "\\" else 1
            for k in range(i + 1, min(j, n)):
                out[k] = " "
            i = j + 1
            continue
        i += 1
    return "".join(out)


def match_close(s: str, i: int, o: str = "{", c: str = "}") -> int:
    """Index of the bracket closing the one at s[i] (or len-1)."""
    depth, n = 0, len(s)
    while i < n:
        ch = s[i]
        if ch == o:
            depth += 1
        elif ch == c:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return n - 1


def match_open_back(s: str, i: int) -> int:
    """s[i] == ')': index of its '('."""
    depth = 0
    while i >= 0:
        if s[i] == ")":
            depth += 1
        elif s[i] == "(":
            depth -= 1
            if depth == 0:
                return i
        i -= 1
    return -1


def strip_annotations(s: str) -> str:
    out, i = [], 0
    for m in re.finditer(r"@[\w.]+", s):
        if m.start() < i:
            continue
        out.append(s[i:m.start()])
        j = m.end()
        k = j
        while k < len(s) and s[k].isspace():
            k += 1
        if k < len(s) and s[k] == "(":
            j = match_close(s, k, "(", ")") + 1
        i = j
    out.append(s[i:])
    return " ".join("".join(out).split())


def split_top(s: str, sep: str = ",") -> List[str]:
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch in "<([{":
            depth += 1
        elif ch in ">)]}":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def simple_type(t: str) -> str:
    t = re.sub(r"<.*>", "", t).replace("[]", "").replace("...", "").strip()
    parts = t.split(".")
    if len(parts) >= 2 and parts[-2].endswith("Grpc"):
        return ".".join(parts[-2:])
    return parts[-1]


def parse_header(seg: str) -> Optional[Tuple[str, str, str]]:
    """'... public Foo<Bar> name(params) throws X' -> (name, params, before-name text)."""
    t = re.sub(r"\bthrows\s+[\w.,\s<>]+$", "", seg.rstrip()).rstrip()
    if not t.endswith(")"):
        return None
    o = match_open_back(t, len(t) - 1)
    if o < 0:
        return None
    nm = re.search(r"(\w+)\s*$", t[:o])
    if not nm or nm.group(1) in KEYWORDS:
        return None
    before = t[:nm.start()]
    if re.search(r"(=|->|\bnew\s+\w+\s*$|\breturn\b)", strip_annotations(before)[-40:]):
        return None
    return nm.group(1), t[o + 1:-1], before


def return_type(before: str) -> str:
    words = strip_annotations(before)
    words = re.sub(r"^\s*<[^>]*>\s*", "", " ".join(w for w in words.split() if w not in MODIFIERS))
    words = re.sub(r"^\s*<[^>]*>\s*", "", words)
    return words.strip()


def clean(raw: str, n: int = 70) -> str:
    t = " ".join(raw.split())
    return t if len(t) <= n else t[:n - 1] + "…"


# ============================================================================ model
@dataclass
class Branch:
    label: str
    head: Tuple[int, int]      # condition span (calls here run before/at branch start)
    body: Tuple[int, int]


@dataclass
class Block:
    kind: str                  # alt | loop | opt
    start: int
    end: int
    branches: List[Branch]


@dataclass
class JMethod:
    name: str
    cls: str
    annotations: str
    body: str                  # strings blanked
    raw_body: str              # strings kept
    line: int
    file: str
    returns: str = ""
    locals: Dict[str, str] = field(default_factory=dict)
    _blocks: Optional[List[Block]] = None
    _events: Optional[list] = None


@dataclass
class JClass:
    name: str
    pkg_role: str
    file: str
    kind: str
    header: str
    fields: Dict[str, str] = field(default_factory=dict)
    methods: Dict[str, List[JMethod]] = field(default_factory=dict)
    implements: List[str] = field(default_factory=list)
    extends: str = ""
    pkg: str = ""
    imports: Dict[str, str] = field(default_factory=dict)   # simple name -> fully qualified

    @property
    def fqn(self) -> str:
        return f"{self.pkg}.{self.name}" if self.pkg else self.name


# ============================================================================ parsing
def parse_class(code: str, rpath: str, base_pkg_len: int, pkg: str) -> Optional[JClass]:
    blank = blank_strings(code)
    m = re.search(r"\b(class|interface|enum|record)\s+(\w+)([^{]*)\{", blank)
    if not m:
        return None
    segs = pkg.split(".")
    role = segs[base_pkg_len] if len(segs) > base_pkg_len else ""
    cls = JClass(m.group(2), role, rpath, m.group(1), blank[max(0, m.start() - 600):m.end()], pkg=pkg)
    for im_ in re.finditer(r"^\s*import\s+(?!static)([\w.]+)\.(\w+)\s*;", code, re.M):
        cls.imports[im_.group(2)] = f"{im_.group(1)}.{im_.group(2)}"
    tail = m.group(3)
    em = re.search(r"\bextends\s+([\w.<>, ]+?)(?:\bimplements\b|$)", tail)
    if em:
        cls.extends = em.group(1).strip()
    im = re.search(r"\bimplements\s+([\w.<>, ]+)", tail)
    if im:
        cls.implements = [simple_type(x) for x in split_top(im.group(1)) if x.strip()]
    if cls.kind == "interface" and cls.extends:
        cls.implements += [simple_type(x) for x in split_top(cls.extends)]
    body_open = m.end() - 1
    body_close = match_close(blank, body_open)
    i, seg_start, pd = body_open + 1, body_open + 1, 0
    while i < body_close:
        c = blank[i]
        if c == "(":
            pd += 1
        elif c == ")":
            pd -= 1
        elif c == ";" and pd == 0:
            seg = strip_annotations(blank[seg_start:i])
            fm = FIELD_RE.search(seg)
            if fm and "(" not in fm.group(1) and fm.group(1) not in ("return", "import", "package"):
                cls.fields[fm.group(2)] = simple_type(fm.group(1))
            seg_start = i + 1
        elif c == "{":
            end = match_close(blank, i)
            if pd > 0:              # braces inside annotation arguments, e.g. examples = {...}
                i = end + 1
                continue
            seg = blank[seg_start:i]
            if not re.search(r"\b(class|interface|enum|record)\s+\w+", strip_annotations(seg)):
                h = parse_header(seg)
                if h:
                    name, params, before = h
                    meth = JMethod(name, cls.name, before, blank[i + 1:end], code[i + 1:end],
                                   line_of(code, i), rpath, return_type(before))
                    for p in split_top(params):
                        p = strip_annotations(p).replace("final ", "").strip()
                        pm = re.match(r"(.+?)\s+(\w+)$", p)
                        if pm:
                            meth.locals[pm.group(2)] = simple_type(pm.group(1))
                    for lm in LOCAL_RE.finditer(meth.body):
                        meth.locals.setdefault(lm.group(2), simple_type(lm.group(1)))
                    cls.methods.setdefault(name, []).append(meth)
            i = end + 1
            seg_start = i
            continue
        i += 1
    return cls


def _statement_end(s: str, i: int) -> int:
    """End index (inclusive) of the statement starting at/after i: a {block} or up to ';'."""
    n = len(s)
    while i < n and s[i].isspace():
        i += 1
    if i < n and s[i] == "{":
        return match_close(s, i)
    depth = 0
    while i < n:
        ch = s[i]
        if ch in "({":
            depth += 1
        elif ch in ")}":
            depth -= 1
        elif ch == ";" and depth <= 0:
            return i
        i += 1
    return n - 1


def _body_span(s: str, i: int) -> Tuple[int, int]:
    j = i
    while j < len(s) and s[j].isspace():
        j += 1
    end = _statement_end(s, j)
    if j < len(s) and s[j] == "{":
        return j + 1, end
    return j, end + 1


def parse_blocks(m: JMethod) -> List[Block]:
    if m._blocks is not None:
        return m._blocks
    s, raw = m.body, m.raw_body
    blocks: List[Block] = []
    consumed = set()
    for km in re.finditer(r"\b(if|for|while|catch|switch)\s*\(|\bdo\s*\{|\.forEach\s*\(", s):
        kw = km.group(1) or ("do" if km.group(0).startswith("do") else "forEach")
        if km.start() in consumed:
            continue
        if kw == "do":
            o = s.index("{", km.start())
            e = match_close(s, o)
            w = re.match(r"\s*while\s*\(", s[e + 1:])
            lab = "do-while"
            end = e
            if w:
                po = e + 1 + w.end() - 1
                pc = match_close(s, po, "(", ")")
                lab = "while " + clean(raw[po + 1:pc], 50)
                end = pc
                consumed.add(e + 1 + w.start() + w.group(0).index("while"))
            blocks.append(Block("loop", km.start(), end, [Branch(lab, (km.start(), km.start()), (o + 1, e))]))
            continue
        po = km.end() - 1
        pc = match_close(s, po, "(", ")")
        cond = clean(raw[po + 1:pc])
        if kw == "forEach":
            blocks.append(Block("loop", km.start(), pc, [Branch("forEach", (km.start(), km.start()), (po + 1, pc))]))
            continue
        if kw in ("for", "while"):
            b0, b1 = _body_span(s, pc + 1)
            blocks.append(Block("loop", km.start(), b1, [Branch(f"{kw} {cond}", (po + 1, pc), (b0, b1))]))
            continue
        if kw == "catch":
            b0, b1 = _body_span(s, pc + 1)
            exc = cond.rsplit(" ", 1)[0] if " " in cond else cond
            blocks.append(Block("opt", km.start(), b1, [Branch(f"on {exc}", (po, po), (b0, b1))]))
            continue
        if kw == "switch":
            o = s.find("{", pc)
            e = match_close(s, o)
            cases = []
            depth = 0
            for cm in re.finditer(r"[{}]|\b(case\s+[^:>]+?|default)\s*(?:->|:)", s[o + 1:e]):
                tok = cm.group(0)
                if tok == "{":
                    depth += 1
                elif tok == "}":
                    depth -= 1
                elif depth == 0:
                    cases.append((o + 1 + cm.start(), o + 1 + cm.end(), clean(raw[o + 1 + cm.start():o + 1 + cm.end()].rstrip(":->").strip(), 40)))
            branches = []
            for k, (cs, ce, lab) in enumerate(cases):
                nxt = cases[k + 1][0] if k + 1 < len(cases) else e
                branches.append(Branch(f"{cond}: {lab}" if k == 0 else lab, (cs, cs), (ce, nxt)))
            if branches:
                blocks.append(Block("alt", km.start(), e, branches))
            continue
        # if / else if / else chain
        branches = []
        b0, b1 = _body_span(s, pc + 1)
        branches.append(Branch(f"if {cond}", (po + 1, pc), (b0, b1)))
        end = b1
        while True:
            after = end + 1 if s[end:end + 1] == "}" else end
            em = re.match(r"\s*else\b", s[after:])
            if not em:
                break
            epos = after + em.end()
            im = re.match(r"\s*if\s*\(", s[epos:])
            if im:
                consumed.add(epos + im.group(0).index("if"))
                ipos = epos + im.end() - 1
                ic = match_close(s, ipos, "(", ")")
                c0, c1 = _body_span(s, ic + 1)
                branches.append(Branch(f"else if {clean(raw[ipos + 1:ic])}", (ipos + 1, ic), (c0, c1)))
                end = c1
            else:
                c0, c1 = _body_span(s, epos)
                branches.append(Branch("else", (epos, epos), (c0, c1)))
                end = c1
                break
        blocks.append(Block("alt", km.start(), end, branches))
    blocks.sort(key=lambda b: (b.start, -b.end))
    m._blocks = blocks
    return blocks


# ============================================================================ call graph
class CallGraph:
    def __init__(self, sources, svc: Service, props: Dict[str, str], settings: dict):
        self.svc = svc
        self.props = props
        cfg = settings.get("features") or {}
        self.max_depth = int(cfg.get("max_depth", 6))
        self.max_steps = int(cfg.get("max_steps", 80))
        self.show_self = bool(cfg.get("self_calls", True))
        self.show_logic = bool(cfg.get("control_flow", True))
        self.skip_packages = set(cfg.get("skip_packages") or DEFAULT_SKIP_PACKAGES)
        pkgs = [p.split(".") for _, _, _, p in sources if p]
        base = 0
        if pkgs:
            for seg in zip(*pkgs):
                if len(set(seg)) == 1:
                    base += 1
                else:
                    break
        self.all: List[JClass] = []
        self.by_simple: Dict[str, List[JClass]] = {}
        for f, rpath, code, pkg in sources:
            jc = parse_class(code, rpath, base, pkg)
            if jc:
                self.all.append(jc)
                self.by_simple.setdefault(jc.name, []).append(jc)
        self.impls: Dict[str, List[JClass]] = {}
        for c in self.all:
            if c.kind == "class":
                for i in c.implements + ([simple_type(c.extends)] if c.extends else []):
                    self.impls.setdefault(i, []).append(c)
        self.feign = set()
        for d in svc.dependencies:
            for op in d.operations:
                if d.kind == "http" and "." in op.caller:
                    self.feign.add(op.caller.split(".")[0])

    # ------------------------------------------------------------------ resolution
    def lookup(self, name: str, ctx: Optional[JClass] = None, file: str = "") -> Optional[JClass]:
        """Resolve a simple class name the way javac would: imports, then same package, then unique name."""
        cands = self.by_simple.get(name)
        if not cands:
            return None
        if len(cands) == 1:
            return cands[0]
        if file:
            hit = next((c for c in cands if c.file == file), None)
            if hit:
                return hit
        if ctx:
            fq = ctx.imports.get(name)
            if fq:
                hit = next((c for c in cands if c.fqn == fq), None)
                if hit:
                    return hit
            hit = next((c for c in cands if c.pkg == ctx.pkg), None)
            if hit:
                return hit
        return cands[0]

    def _impl(self, name: str, ctx: Optional[JClass] = None) -> Optional[JClass]:
        c = self.lookup(name, ctx)
        if c and c.kind == "interface" and name not in self.feign and not self._is_repo(c):
            cands = [x for x in self.impls.get(name, [])
                     if x.imports.get(name, c.fqn) == c.fqn or x.pkg == c.pkg]
            pick = next((x for x in cands if x.name == name + "Impl"), cands[0] if cands else None)
            return pick or c
        return c

    @staticmethod
    def _is_repo(c: JClass) -> bool:
        return c.kind == "interface" and (bool(REPO_BASE.search(c.header)) or "@Repository" in c.header)

    def boundary(self, type_name: str, ctx: Optional[JClass] = None) -> Optional[Tuple[str, str]]:
        for rx, kind, label in BOUNDARY_TYPES:
            m = rx.match(type_name)
            if m:
                if kind == "grpc":
                    return f"grpc:{m.group(1)}", "grpc"
                if kind == "db":
                    return "db", "db"
                return f"{kind}:{label}", kind
        c = self.lookup(type_name, ctx)
        if c:
            if type_name in self.feign:
                return f"feign:{type_name}", "http"
            if self._is_repo(c):
                if "redis" in c.file.lower() or "@RedisHash" in c.header:
                    return "cache:Redis", "cache"
                return "db", "db"
        return None

    def find_method(self, c: JClass, name: str) -> Tuple[Optional[JClass], Optional[JMethod]]:
        """Look up a method on the class, then up its extends chain."""
        seen = 0
        while c and seen < 4:
            ms = c.methods.get(name)
            if ms:
                return c, max(ms, key=lambda x: len(x.body))
            c = self.lookup(simple_type(c.extends), c) if c.extends else None
            seen += 1
        return None, None

    def _events(self, c: JClass, m: JMethod) -> list:
        """(offset, kind, payload) for calls, self calls and throws in the method body."""
        if m._events is not None:
            return m._events
        ev = []
        for x in CALL_RE.finditer(m.body):
            recv, name = x.group(1), x.group(2)
            if recv in ("log", "logger", "LOG", "LOGGER", "System", "super"):
                continue
            t = m.locals.get(recv) or c.fields.get(recv)
            if t is None and recv[:1].isupper() and recv in self.by_simple:
                t = recv
            if t:
                ev.append((x.start(), "call", (t, name)))
        for x in SELF_RE.finditer(m.body):
            name = x.group(1)
            if name in KEYWORDS:
                continue
            if m.body[max(0, x.start() - 1):x.start()] == "." or re.search(r"\bnew\s+$", m.body[max(0, x.start() - 5):x.start()]):
                continue
            owner, sub = self.find_method(c, name)
            if sub is not None:
                ev.append((x.start(), "self", name))
        for x in THROW_RE.finditer(m.body):
            exc = simple_type(x.group(1) or x.group(2) or x.group(3))
            ev.append((x.start(), "throw", ("if absent: " if "orElseThrow" in x.group(0) else "") + exc))
        ev.sort(key=lambda e: e[0])
        m._events = ev
        return ev

    def _accessor(self, c: Optional[JClass], m: Optional[JMethod], name: str) -> bool:
        if not re.match(r"(get|set|is|has)[A-Z]", name):
            return False
        return m is None or bool(ACCESSOR_BODY.match(m.body))

    # ------------------------------------------------------------------ traversal
    def trace(self, cls_name: str, meth_name: str, file: str = "") -> Tuple[List[Step], bool, str]:
        steps: List[Step] = []
        expanded = set()
        state = {"trunc": False}

        def real() -> int:
            return sum(1 for s in steps if s.kind not in STRUCT or s.kind in ("self", "throw"))

        def emit_call(c: JClass, m: JMethod, off: int, kind: str, payload, depth: int, stack: set):
            if real() >= self.max_steps:
                state["trunc"] = True
                return
            if kind == "throw":
                steps.append(Step(c.name, c.name, f"throws {payload}", "throw", depth))
                return
            if kind == "self":
                owner, sub = self.find_method(c, payload)
                key = (c.name, payload)
                if not sub or key in stack:
                    return
                if self._accessor(owner, sub, payload):
                    return
                if self.show_self:
                    steps.append(Step(c.name, c.name, f"{payload}()", "self", depth))
                visit(owner, sub, depth + 1, stack | {key})
                return
            t, name = payload
            b = self.boundary(t, c)
            if b:
                dst, bkind = b
                if bkind not in ("http", "db") and re.match(r"(set|get|is|add|with|register|afterPropertiesSet)[A-Z]?", name):
                    return
                if bkind == "http" or (bkind in ("db", "cache") and t in self.by_simple):
                    label = f"{t}.{name}"
                elif bkind in ("kafka", "rabbitmq"):
                    label = self._msg_label(m, off, name)
                else:
                    label = name
                steps.append(Step(c.name, dst, label, bkind, depth))
                return
            target = self._impl(t, c)
            if not target:
                return
            if target.name == c.name:   # self-injected proxy (e.g. `self.foo()` for @Transactional/@Async)
                emit_call(c, m, off, "self", name, depth, stack)
                return
            if target.pkg_role.lower() in self.skip_packages or target.kind in ("enum", "record"):
                return
            owner, sub = self.find_method(target, name)
            if self._accessor(owner, sub, name):
                return
            steps.append(Step(c.name, target.name, name, "call", depth))
            key = (target.name, name)
            if sub and depth < self.max_depth and key not in stack and key not in expanded:
                expanded.add(key)
                visit(owner or target, sub, depth + 1, stack | {key})

        def emit_region(c: JClass, m: JMethod, lo: int, hi: int, depth: int, stack: set):
            blocks = [b for b in parse_blocks(m) if lo <= b.start and b.end <= hi] if self.show_logic else []
            top, last_end = [], -1
            for b in blocks:  # keep outermost blocks only
                if b.start > last_end:
                    top.append(b)
                    last_end = b.end
            items = [(b.start, "block", b) for b in top]
            for off, kind, payload in self._events(c, m):
                if lo <= off < hi and not any(b.start <= off <= b.end for b in top):
                    items.append((off, kind, payload))
            items.sort(key=lambda x: x[0])
            for off, kind, payload in items:
                if state["trunc"]:
                    return
                if kind != "block":
                    emit_call(c, m, off, kind, payload, depth, stack)
                    continue
                b: Block = payload
                first = b.branches[0]
                # condition / loop-header calls run before the block
                emit_region(c, m, first.head[0], first.head[1], depth, stack)
                mark, before = len(steps), real()
                for k, br in enumerate(b.branches):
                    if k == 0:
                        steps.append(Step(c.name, c.name, br.label, b.kind, depth))
                    else:
                        steps.append(Step(c.name, c.name, br.label, "else", depth))
                        emit_region(c, m, br.head[0], br.head[1], depth, stack)
                    emit_region(c, m, br.body[0], br.body[1], depth, stack)
                steps.append(Step(c.name, c.name, "", "end", depth))
                if real() == before:
                    del steps[mark:]          # nothing interesting happened inside
                else:
                    # drop empty else-branches at the tail ("else" immediately followed by end)
                    while len(steps) >= 2 and steps[-2].kind == "else" and steps[-1].kind == "end" and len(steps) - 2 > mark:
                        del steps[-2]

        def visit(c: JClass, m: JMethod, depth: int, stack: set):
            emit_region(c, m, 0, len(m.body), depth, stack)

        c = self.lookup(cls_name, file=file)
        owner, m = self.find_method(c, meth_name) if c else (None, None)
        ret = ""
        if owner and m:
            ret = m.returns
            visit(owner, m, 1, {(owner.name, m.name)})
        return steps, state["trunc"], ret

    def _msg_label(self, m: JMethod, off: int, name: str) -> str:
        seg = m.raw_body[off:off + 400]
        call = re.search(r"\b" + re.escape(name) + r"\s*\(", seg)
        args = seg[call.end() - 1:] if call else ""
        lit = re.match(r"\(\s*\"([^\"]+)\"", args)
        if lit:
            return f"{name}({lit.group(1)})"
        var = re.match(r"\(\s*(\w+)", args)
        if var:
            vm = re.search(r"\b" + re.escape(var.group(1)) + r"\s*=\s*\"([^\"]+)\"", m.raw_body)
            if vm:
                return f"{name}({vm.group(1)})"
        return name

    # ------------------------------------------------------------------ entry points
    def _flow(self, kind, feature, name, cls, meth, evidence) -> Flow:
        steps, trunc, ret = self.trace(cls, meth, evidence.file if evidence else "")
        if ret and ret != "void" and kind == "endpoint":
            steps.append(Step(cls, "__entry__", ret, "return", 0))
        return Flow(kind, feature, name, f"{cls}.{meth}", steps, trunc, evidence)

    def flows(self, raw_cache: Dict) -> List[Flow]:
        out: List[Flow] = []
        base = self.svc.context_path.rstrip("/") if self.svc.context_path else ""
        for e in self.svc.endpoints:
            if e.handler:
                out.append(self._flow("endpoint", e.component, f"{e.method} {base}{e.path}", e.component, e.handler, e.evidence))
        for c in self.all:
            for ms in c.methods.values():
                for m in ms:
                    ann = m.annotations
                    raw = raw_cache.get((c.name, m.name, m.line), ann)
                    if "@RabbitListener" in ann or "@KafkaListener" in ann:
                        hits = list(re.finditer(r"@(Rabbit|Kafka)Listener\s*\(((?:[^()]|\([^()]*\))*)\)", raw))
                        what = ""
                        broker = "RabbitMQ" if "@RabbitListener" in ann else "Kafka"
                        if hits:
                            names = re.findall(r"\"([^\"]+)\"", hits[-1].group(2))
                            what = ", ".join(resolve_placeholders(n, self.props) for n in names[:2] if "Factory" not in n)
                        out.append(self._flow("listener", "Messaging consumers", f"{broker} {what}".strip(),
                                              c.name, m.name, Evidence(m.file, m.line)))
                    elif "@Scheduled" in ann:
                        hits = list(re.finditer(r"@Scheduled\s*\(((?:[^()]|\([^()]*\))*)\)", raw))
                        sched = ", ".join(f"{k}={resolve_placeholders(v, self.props)}" for k, v in re.findall(
                            r"(cron|fixedRate|fixedDelay|fixedDelayString|fixedRateString|initialDelay)\s*=\s*\"?([^\",)]+)",
                            hits[-1].group(1) if hits else ""))
                        out.append(self._flow("job", "Scheduled jobs", f"{m.name} ({sched})" if sched else m.name,
                                              c.name, m.name, Evidence(m.file, m.line)))
        return out


def build_flows(sources, svc: Service, props: Dict[str, str], settings: dict) -> List[Flow]:
    cg = CallGraph(sources, svc, props, settings)
    raw_cache = {}
    by_file = {rpath: code for _, rpath, code, _ in sources}
    for c in cg.all:
        lines = by_file.get(c.file, "").splitlines()
        for ms in c.methods.values():
            for m in ms:
                if "@" in m.annotations:
                    start = max(0, m.line - 25)
                    raw_cache[(c.name, m.name, m.line)] = "\n".join(lines[start:m.line])
    return cg.flows(raw_cache)
