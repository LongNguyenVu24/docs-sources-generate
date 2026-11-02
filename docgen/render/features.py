"""Feature (per controller / consumer group / jobs) and function (per entry point) diagrams."""
from __future__ import annotations

import re
from collections import OrderedDict
from typing import Dict, List, Tuple

from ..model import Flow, Service, Step
from ..workspace import Workspace
from .mermaid import esc, mid

KIND_ORDER = {"endpoint": 0, "listener": 1, "job": 2}
STRUCT = {"alt", "else", "loop", "opt", "end", "throw", "return", "self"}


def is_boundary(s: Step) -> bool:
    return s.kind != "call" and s.kind not in STRUCT


def real_steps(f: Flow) -> int:
    return sum(1 for s in f.steps if s.kind not in STRUCT or s.kind == "self")


def throws(f: Flow) -> List[str]:
    out = OrderedDict()
    for s in f.steps:
        if s.kind == "throw":
            out[s.label.replace("throws ", "")] = 1
    return list(out)
EXT_STYLE = {
    "http": "ext", "grpc": "ext", "http-dynamic": "ext", "other": "ext",
    "db": "data", "cache": "data", "storage": "data", "kafka": "msg", "rabbitmq": "msg",
}
CLASSDEFS = """    classDef entry fill:#1f6feb,stroke:#0b3d91,color:#fff
    classDef cls fill:#dbeafe,stroke:#1f6feb,color:#0b1f44
    classDef ext fill:#fef3c7,stroke:#b45309,color:#451a03
    classDef msg fill:#ede9fe,stroke:#6d28d9,color:#2e1065
    classDef data fill:#dcfce7,stroke:#15803d,color:#052e16"""


def seq_esc(s: str) -> str:
    return s.replace(";", ",").replace("#", "#35;").replace("\n", " ")


class Resolver:
    """Turns boundary keys from the call graph into readable participants/messages."""

    def __init__(self, svc: Service):
        self.svc = svc
        self.ops: Dict[str, Tuple[str, str]] = {}
        for d in svc.dependencies:
            if d.kind == "http":
                for op in d.operations:
                    self.ops[op.caller] = (d.resolved or d.target, op.name)
        self.db = next((d.target for d in svc.dependencies if d.kind == "database"), "Database")

    def participant(self, step: Step) -> str:
        dst = step.dst
        if dst.startswith("feign:"):
            hit = self.ops.get(step.label)
            return f"{hit[0]} (REST)" if hit else dst[6:]
        if dst.startswith("grpc:"):
            return f"{dst[5:]} (gRPC)"
        if dst == "db":
            return self.db
        if ":" in dst:
            return dst.split(":", 1)[1]
        return dst

    def message(self, step: Step) -> str:
        if step.dst.startswith("feign:"):
            hit = self.ops.get(step.label)
            return hit[1] if hit else step.label.split(".", 1)[-1]
        if step.kind == "call":
            return f"{step.label}()"
        return step.label


def reaches(flow: Flow, r: Resolver) -> List[str]:
    out = OrderedDict()
    for s in flow.steps:
        if is_boundary(s):
            out[r.participant(s)] = 1
    return list(out)


def sequence(flow: Flow, svc: Service, r: Resolver) -> str:
    L = ["sequenceDiagram", "    autonumber"]
    ids: "OrderedDict[str, str]" = OrderedDict()
    internal, external = [], []
    entry_cls = flow.entry.split(".")[0]

    def pid(name: str, is_ext: bool) -> str:
        if name not in ids:
            ids[name] = f"p{len(ids)}"
            (external if is_ext else internal).append(name)
        return ids[name]

    trig = {"endpoint": ("Client", "actor"), "listener": (flow.name.split(" ")[0] + " broker", "participant"),
            "job": ("Scheduler", "participant")}[flow.kind]
    pid(entry_cls, False)
    msgs = []
    indent = 1
    for s in flow.steps:
        pad = "    " * indent
        if s.kind in ("alt", "loop", "opt"):
            msgs.append(f"{pad}{s.kind} {seq_esc(s.label)}")
            indent += 1
            continue
        if s.kind == "else":
            lbl = re.sub(r"^else\s*", "", s.label)
            msgs.append(f"{'    ' * max(1, indent - 1)}else {seq_esc(lbl)}".rstrip())
            continue
        if s.kind == "end":
            indent = max(1, indent - 1)
            msgs.append(f"{'    ' * indent}end")
            continue
        if s.kind == "throw":
            msgs.append(f"{pad}Note over {pid(s.src, False)}: {seq_esc(s.label)}")
            continue
        if s.kind == "return":
            msgs.append(f"{pad}{pid(s.src, False)}-->>T: {seq_esc(s.label)}")
            continue
        if s.kind == "self":
            a = pid(s.src, False)
            msgs.append(f"{pad}{a}->>{a}: {seq_esc(s.label)}")
            continue
        is_ext = s.kind != "call"
        a = pid(s.src, False)
        b = pid(r.participant(s) if is_ext else s.dst, is_ext)
        arrow = "-)" if s.kind in ("kafka", "rabbitmq") else "->>"
        msgs.append(f"{pad}{a}{arrow}{b}: {seq_esc(r.message(s))}")
    L.append(f"    {trig[1]} T as {seq_esc(trig[0])}")
    L.append(f"    box rgb(219,234,254) {seq_esc(svc.id)}")
    for n in internal:
        L.append(f"        participant {ids[n]} as {seq_esc(n)}")
    L.append("    end")
    if external:
        L.append("    box rgb(254,243,199) Outside the service")
        for n in external:
            L.append(f"        participant {ids[n]} as {seq_esc(n)}")
        L.append("    end")
    first = flow.name if flow.kind != "job" else flow.name
    L.append(f"    T->>{ids[entry_cls]}: {seq_esc(first)}")
    L += msgs
    if not real_steps(flow):
        L.append(f"    Note over {ids[entry_cls]}: handled locally (no calls to other components)")
    if flow.truncated:
        L.append(f"    Note over {ids[entry_cls]}: truncated (max steps reached)")
    return "\n".join(L)


def feature_flowchart(feature: str, flows: List[Flow], svc: Service, r: Resolver) -> str:
    L = ["flowchart LR"]
    nodes: "OrderedDict[str, Tuple[str, str]]" = OrderedDict()
    edges: "OrderedDict[Tuple[str, str], set]" = OrderedDict()
    kind = flows[0].kind
    entry_label = {"endpoint": f"REST API<br/>{len(flows)} endpoints", "listener": f"Message brokers<br/>{len(flows)} listeners",
                   "job": f"Scheduler<br/>{len(flows)} jobs"}[kind]
    nodes["__entry__"] = (entry_label, "entry")
    for f in flows:
        ec = f.entry.split(".")[0]
        nodes.setdefault(ec, (ec, "cls"))
        edges.setdefault(("__entry__", ec), set())
        for s in f.steps:
            if s.kind in STRUCT:
                continue
            if s.kind == "call":
                nodes.setdefault(s.dst, (s.dst, "cls"))
                nodes.setdefault(s.src, (s.src, "cls"))
                edges.setdefault((s.src, s.dst), set())
            else:
                p = r.participant(s)
                key = "ext_" + p
                nodes.setdefault(key, (p, EXT_STYLE.get(s.kind, "ext")))
                nodes.setdefault(s.src, (s.src, "cls"))
                edges.setdefault((s.src, key), set()).add({"http": "REST", "grpc": "gRPC", "kafka": "publish",
                                                            "rabbitmq": "publish"}.get(s.kind, ""))
    for k, (label, style) in nodes.items():
        i = "n_" + mid(k)
        lbl = esc(label)
        if style == "data":
            L.append(f'    {i}[("{lbl}")]:::{style}')
        elif style == "msg":
            L.append(f'    {i}[/"{lbl}"/]:::{style}')
        elif style == "ext":
            L.append(f'    {i}{{{{"{lbl}"}}}}:::{style}')
        elif style == "entry":
            L.append(f'    {i}(["{lbl}"]):::{style}')
        else:
            L.append(f'    {i}["{lbl}"]:::{style}')
    for (a, b), labels in edges.items():
        lbl = "/".join(sorted(x for x in labels if x))
        L.append(f'    n_{mid(a)} -->|"{lbl}"| n_{mid(b)}' if lbl else f"    n_{mid(a)} --> n_{mid(b)}")
    L.append(CLASSDEFS)
    return "\n".join(L)


def feature_title(feature: str, flows: List[Flow]) -> str:
    if flows[0].kind != "endpoint":
        return feature
    return re.sub(r"(Controller|Resource|Api|Endpoint)(V\d+)?$", r" \2", feature).strip() or feature


def render_features(svc: Service, ws: Workspace) -> str:
    r = Resolver(svc)
    groups: "OrderedDict[str, List[Flow]]" = OrderedDict()
    for f in sorted(svc.flows, key=lambda f: (KIND_ORDER[f.kind], f.feature, f.name)):
        groups.setdefault(f.feature, []).append(f)
    o = [f"# {svc.id} — features & functions\n",
         f"[← index](../index.md) · [service overview](../services/{svc.id}.md)\n",
         "Each **feature** is a controller, the set of message listeners, or the set of scheduled jobs. Each "
         "**function** is one entry point traced through the service's own classes until it leaves the service "
         "(REST/gRPC call, database, cache, broker, storage). Sequence diagrams show branches (`alt`), loops "
         "(`loop`), exception handlers (`opt`), private helper calls, thrown exceptions (notes) and the returned "
         "type; blocks with nothing inside are omitted. Traces come from static analysis, so calls through "
         "reflection or lambdas passed elsewhere may be missing.\n",
         "## Feature summary\n",
         "| Feature | Type | Functions | Reaches |", "|---|---|---|---|"]
    for feat, flows in groups.items():
        ext = OrderedDict()
        for f in flows:
            for x in reaches(f, r):
                ext[x] = 1
        anchor = re.sub(r"[^a-z0-9 -]", "", ("feature: " + feature_title(feat, flows)).lower()).replace(" ", "-")
        o.append(f"| [{feature_title(feat, flows)}](#{anchor}) | {flows[0].kind} | {len(flows)} | "
                 f"{', '.join(ext) or '—'} |")
    o.append("")
    for feat, flows in groups.items():
        title = feature_title(feat, flows)
        o.append(f"\n## Feature: {title}\n")
        if flows[0].kind == "endpoint":
            c = next((c for c in svc.components if c.kind == "controller" and c.name == feat), None)
            o.append(f"`{feat}`" + (f" · {c.evidence.file}" if c and c.evidence else "") + "\n")
        o.append("```mermaid\n" + feature_flowchart(feat, flows, svc, r) + "\n```\n")
        o.append("| Function | Handler | Steps | Reaches | Throws |\n|---|---|---|---|---|")
        for f in flows:
            o.append(f"| `{f.name.replace('|', '/')}` | `{f.entry}` | {real_steps(f)}{'+' if f.truncated else ''} | "
                     f"{', '.join(reaches(f, r)) or '—'} | {', '.join(throws(f)) or '—'} |")
        o.append("")
        for f in flows:
            o.append(f"\n### {f.name.replace('|', '/')}\n")
            o.append(f"`{f.entry}`" + (f" · `{f.evidence}`" if f.evidence else "") + "\n")
            o.append(f"<details><summary>Sequence diagram ({real_steps(f)} steps)</summary>\n")
            o.append("```mermaid\n" + sequence(f, svc, r) + "\n```\n")
            o.append("</details>\n")
    return "\n".join(o)
