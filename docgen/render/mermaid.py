"""Mermaid diagram builders."""
from __future__ import annotations

import re
from collections import Counter
from typing import Dict, Iterable, List, Optional, Set

from ..model import CACHE, DATABASE, EXTERNAL, GRPC, HTTP, KAFKA, RABBITMQ, REGISTRY, STORAGE, Service
from ..workspace import Edge, Node, Workspace

CLASSDEFS = """    classDef target fill:#1f6feb,stroke:#0b3d91,color:#fff,stroke-width:2px
    classDef callee fill:#dbeafe,stroke:#1f6feb,color:#0b1f44
    classDef peer fill:#f1f5f9,stroke:#64748b,color:#1e293b
    classDef stub fill:#fff,stroke:#94a3b8,color:#475569,stroke-dasharray:4 3
    classDef ext fill:#fef3c7,stroke:#b45309,color:#451a03
    classDef msg fill:#ede9fe,stroke:#6d28d9,color:#2e1065
    classDef data fill:#dcfce7,stroke:#15803d,color:#052e16"""


def mid(s: str) -> str:
    return re.sub(r"\W", "_", s)


def esc(s: str) -> str:
    return s.replace('"', "#quot;").replace("|", "/")


def _shape(n: Node, label: Optional[str] = None) -> str:
    lbl = esc(label or n.label)
    i = mid(n.id)
    if n.type in (DATABASE, CACHE, STORAGE):
        return f'{i}[("{lbl}")]'
    if n.type in (KAFKA, RABBITMQ):
        return f'{i}[/"{lbl}"/]'
    if n.type in ("external", "grpc-ext", EXTERNAL):
        return f'{i}{{{{"{lbl}"}}}}'
    if n.type == REGISTRY:
        return f'{i}(["{lbl}"])'
    return f'{i}["{lbl}"]'


def _cls(n: Node) -> str:
    if n.type == "service":
        if n.tier == 0:
            return "target"
        if n.tier is not None:
            return "callee"
        return "peer"
    if n.type == "stub":
        return "stub"
    if n.type in (KAFKA, RABBITMQ):
        return "msg"
    if n.type in (DATABASE, CACHE, STORAGE, REGISTRY):
        return "data"
    return "ext"


def _svc_label(ws: Workspace, n: Node) -> str:
    s = ws.services.get(n.service or "")
    if not s:
        return n.label
    bits = [s.framework or s.language]
    if s.ports.get("http"):
        bits.append(f":{s.ports['http']}")
    sub = " · ".join(bits)
    cp = f"<br/>{s.context_path}" if s.context_path and s.context_path.strip("/") != s.id else ""
    return f"<b>{s.id}</b><br/><small>{sub}</small>{cp}"


def _arrow(e: Edge) -> str:
    return "-.->" if e.inferred else "-->"


# ---------------------------------------------------------------------------- system level
DATA_TYPES = (DATABASE, CACHE, STORAGE, REGISTRY)
EXT_TYPES = ("external", "grpc-ext", EXTERNAL, "stub")


def index_edges(ws: Workspace) -> List[Edge]:
    """Edges worth showing on the system-level diagrams.

    Kept: anything touching a target; service<->service links inside scope; in-scope services'
    data stores; peers on topics/exchanges the targets use. Callees' own external calls live on
    their service pages.
    """
    nodes = ws.nodes
    tier = lambda nid: nodes[nid].tier
    target_msg = {e.dst for e in ws.edges.values() if tier(e.src) == 0 and nodes[e.dst].type in (KAFKA, RABBITMQ)} | \
                 {e.src for e in ws.edges.values() if tier(e.dst) == 0 and nodes[e.src].type in (KAFKA, RABBITMQ)}
    keep = []
    for e in ws.edges.values():
        sn, dn = nodes[e.src], nodes[e.dst]
        if sn.tier == 0 or dn.tier == 0:
            keep.append(e)
        elif sn.type == dn.type == "service" and sn.tier is not None and dn.tier is not None:
            keep.append(e)
        elif sn.tier is not None and dn.type in DATA_TYPES:
            keep.append(e)
        elif e.src in target_msg or e.dst in target_msg:
            keep.append(e)
    return keep


def architecture(ws: Workspace) -> str:
    """Container-style view grouped by tier; messaging collapsed to brokers."""
    def keep(e: Edge) -> bool:
        sn, dn = ws.nodes[e.src], ws.nodes[e.dst]
        if any(n.type == "service" and n.tier is None for n in (sn, dn)):
            return False
        if dn.type == REGISTRY:
            return False  # shown once as a shared node, not one arrow per service
        if dn.type in DATA_TYPES or dn.type in (KAFKA, RABBITMQ):
            return sn.tier == 0
        if sn.type in (KAFKA, RABBITMQ):
            return dn.tier == 0
        return True

    edges = [e for e in index_edges(ws) if keep(e)]
    registries = [n for n in ws.nodes.values() if n.type == REGISTRY]
    used = {e.src for e in edges} | {e.dst for e in edges} | {f"svc:{s}" for s in ws.scope}
    nodes = [ws.nodes[n] for n in used]
    lines = ["flowchart TB", '    client(["API clients<br/>(portal / API gateway / other services)"])']
    tiers: Dict[int, List[Node]] = {}
    for n in nodes:
        if n.type == "service":
            tiers.setdefault(n.tier, []).append(n)
    titles = {0: "Target services", 1: "Called services"}
    for t in sorted(tiers):
        lines.append(f'    subgraph T{t}["{titles.get(t, f"Called services (depth {t})")}"]')
        lines.append("        direction LR")
        for n in sorted(tiers[t], key=lambda x: x.id):
            lines.append(f"        {_shape(n, _svc_label(ws, n))}:::{_cls(n)}")
        lines.append("    end")
    kafka = sum(1 for n in nodes if n.type == KAFKA)
    rabbit = sum(1 for n in nodes if n.type == RABBITMQ)
    infra = []
    if kafka:
        infra.append(f'broker_kafka[/"Kafka<br/>{kafka} topics"/]:::msg')
    if rabbit:
        infra.append(f'broker_rabbit[/"RabbitMQ<br/>{rabbit} exchanges / queues"/]:::msg')
    infra += [f"{_shape(n)}:::data" for n in sorted(nodes, key=lambda x: x.id) if n.type in DATA_TYPES and n.type != REGISTRY]
    infra += [f'{mid(n.id)}(["{esc(n.label)}<br/><small>service discovery for all services</small>"]):::data' for n in registries]
    if infra:
        lines.append('    subgraph INFRA["Messaging & data"]')
        lines.append("        direction LR")
        lines += [f"        {x}" for x in infra]
        lines.append("    end")
    ext = sorted((n for n in nodes if n.type in EXT_TYPES), key=lambda x: (x.type, x.id))
    if ext:
        lines.append('    subgraph EXT["External / not in workspace"]')
        lines += [f"        {_shape(n)}:::{_cls(n)}" for n in ext]
        lines.append("    end")
    for n in sorted(tiers.get(0, []), key=lambda x: x.id):
        lines.append(f"    client --> {mid(n.id)}")
    agg: Dict[tuple, Set[str]] = {}
    for e in edges:
        sn, dn = ws.nodes[e.src], ws.nodes[e.dst]
        src = {KAFKA: "broker_kafka", RABBITMQ: "broker_rabbit"}.get(sn.type, mid(e.src))
        dst = {KAFKA: "broker_kafka", RABBITMQ: "broker_rabbit"}.get(dn.type, mid(e.dst))
        kind = {HTTP: "REST", GRPC: "gRPC", KAFKA: e.label, RABBITMQ: e.label}.get(e.kind, "")
        agg.setdefault((src, dst), set()).add(kind)
    for (s_, d_), kinds in sorted(agg.items()):
        lbl = "/".join(sorted(k for k in kinds if k))
        lines.append(f'    {s_} -->|"{esc(lbl)}"| {d_}' if lbl else f"    {s_} --> {d_}")
    lines.append(CLASSDEFS)
    return "\n".join(lines)


def relationships(ws: Workspace, focus: Optional[str] = None) -> str:
    """Detailed relationship graph. With `focus`, only edges touching that service node."""
    lines = ["flowchart LR"]
    edges = list(ws.edges.values()) if focus else index_edges(ws)
    if focus:
        fid = f"svc:{focus}"
        msg_nodes = {e.dst for e in edges if e.src == fid and ws.nodes[e.dst].type in (KAFKA, RABBITMQ)} | \
                    {e.src for e in edges if e.dst == fid and ws.nodes[e.src].type in (KAFKA, RABBITMQ)}
        edges = [e for e in edges if fid in (e.src, e.dst) or
                 (e.src in msg_nodes or e.dst in msg_nodes)]
    used = {e.src for e in edges} | {e.dst for e in edges}
    if focus:
        used.add(f"svc:{focus}")
    groups: Dict[str, List[Node]] = {}
    for nid in used:
        n = ws.nodes[nid]
        g = {KAFKA: "Kafka topics", RABBITMQ: "RabbitMQ exchanges / queues",
             DATABASE: "Data stores", CACHE: "Data stores", STORAGE: "Data stores", REGISTRY: "Data stores",
             "grpc-ext": "External gRPC services", "external": "External HTTP", EXTERNAL: "External HTTP"}.get(n.type)
        groups.setdefault(g or "", []).append(n)
    for n in sorted(groups.pop("", []), key=lambda x: (x.tier if x.tier is not None else 99, x.id)):
        label = _svc_label(ws, n) if n.type == "service" else n.label + (" ⚠ not found" if n.type == "stub" else "")
        lines.append(f"    {_shape(n, label)}:::{_cls(n)}")
    for gi, (g, ns) in enumerate(sorted(groups.items())):
        lines.append(f'    subgraph G{gi}["{g}"]')
        for n in sorted(ns, key=lambda x: x.id):
            lines.append(f"        {_shape(n)}:::{_cls(n)}")
        lines.append("    end")
    for e in sorted(edges, key=lambda e: (e.src, e.dst)):
        lines.append(f'    {mid(e.src)} {_arrow(e)}|"{esc(e.label)}"| {mid(e.dst)}')
    lines.append(CLASSDEFS)
    return "\n".join(lines)


# ---------------------------------------------------------------------------- service level
def service_components(s: Service) -> str:
    """Internal hexagonal-style view: inbound adapters -> core -> outbound adapters -> targets."""
    L = ["flowchart LR"]
    ctrls = [c for c in s.components if c.kind == "controller"]
    consumers = [c for c in s.components if c.kind == "consumer"]
    jobs = [c for c in s.components if c.kind == "job"]
    grpc_srv = [c for c in s.components if c.kind == "grpc-server"]
    L.append('    subgraph IN["Inbound"]')
    L.append("        direction TB")
    if ctrls:
        names = "<br/>".join(sorted({c.name for c in ctrls})[:14]) + ("<br/>…" if len(ctrls) > 14 else "")
        L.append(f'        rest["REST API · {len(s.endpoints)} endpoints<br/><small>{esc(names)}</small>"]')
    if grpc_srv:
        L.append(f'        grpcs["gRPC server<br/>{esc(", ".join(sorted({c.detail for c in grpc_srv})))}"]')
    kc = [d for d in s.dependencies if d.kind == KAFKA and d.direction == "consume"]
    rc = [d for d in s.dependencies if d.kind == RABBITMQ and d.direction == "consume"]
    if kc:
        L.append(f'        kin[/"Kafka consumers · {len(kc)} topics"/]')
    if rc:
        L.append(f'        rin[/"RabbitMQ listeners · {len(rc)} queues"/]')
    if jobs:
        L.append(f'        jobs(["Scheduled jobs · {len(jobs)}"])')
    if not (ctrls or grpc_srv or kc or rc or jobs):
        L.append('        none["(no inbound adapters detected)"]')
    L.append("    end")
    core_layers = [k for k in s.layers if k in ("service", "services", "helper", "handler", "usecase", "domain",
                                                   "controller", "controllers", "internal", "pkg", "core", "app")]
    L.append(f'    subgraph CORE["{esc(s.id)} core"]')
    L.append(f'        core["Business logic<br/><small>{esc(", ".join(f"{k} ({s.layers[k]})" for k in core_layers) or "—")}</small>"]')
    L.append("    end")
    L.append('    subgraph OUT["Outbound"]')
    L.append("        direction TB")
    outs = []
    for d in s.dependencies:
        if d.kind == HTTP:
            outs.append(("http_" + mid(d.target), f"{d.label}", f"{d.resolved or d.target}", "REST"))
        elif d.kind == GRPC:
            outs.append(("grpc_" + mid(d.target), f"{d.label} stub", d.target, "gRPC"))
    for oid, lbl, tgt, k in outs:
        L.append(f'        {oid}["{esc(lbl)}"]')
    kp = [d for d in s.dependencies if d.kind == KAFKA and d.direction == "publish"]
    rp = [d for d in s.dependencies if d.kind == RABBITMQ and d.direction == "publish"]
    if kp:
        L.append(f'        kout[/"Kafka producer · {len(kp)} topics"/]')
    if rp:
        L.append(f'        rout[/"RabbitMQ publisher · {len(rp)} exchanges"/]')
    stores = [d for d in s.dependencies if d.kind in (DATABASE, CACHE, STORAGE)]
    for d in stores:
        L.append(f'        st_{mid(d.target)}[("{esc(d.target)}<br/><small>{esc(d.label)}</small>")]')
    L.append("    end")
    for i in ("rest", "grpcs", "kin", "rin", "jobs"):
        if f"        {i}" in "\n".join(L):
            L.append(f"    {i} --> core")
    for oid, *_ in outs:
        L.append(f"    core --> {oid}")
    if kp:
        L.append("    core --> kout")
    if rp:
        L.append("    core --> rout")
    for d in stores:
        L.append(f"    core --> st_{mid(d.target)}")
    for oid, lbl, tgt, k in outs:
        L.append(f'    {oid} -->|"{k}"| t_{mid(tgt)}(["{esc(tgt)}"])')
    return "\n".join(L)
