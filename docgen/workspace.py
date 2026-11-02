"""Workspace discovery, cross-service resolution, scope selection and graph building."""
from __future__ import annotations

import re
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .analyzers import ANALYZERS
from .analyzers.base import Analyzer
from .model import (CACHE, DATABASE, EXTERNAL, GRPC, HTTP, KAFKA, RABBITMQ, REGISTRY, STORAGE,
                    Dependency, Service)


@dataclass
class Project:
    id: str
    root: Path
    analyzer: Analyzer
    names: List[str] = field(default_factory=list)


@dataclass
class Node:
    id: str
    label: str
    type: str          # service | stub | external | kafka | rabbitmq | database | cache | registry | storage | grpc-ext
    tier: Optional[int] = None   # 0 = target, 1 = direct callee, ... None = not in scope
    service: Optional[str] = None


@dataclass
class Edge:
    src: str
    dst: str
    kind: str
    label: str
    count: int = 1
    inferred: bool = False


def discover(source_root: Path, settings: dict) -> Dict[str, Project]:
    projects: Dict[str, Project] = {}
    excludes = set(settings["excludes"])
    ignore = set(settings.get("ignore_projects", []))
    for child in sorted(p for p in source_root.iterdir() if p.is_dir()):
        if child.name in excludes or child.name.startswith(".") or child.name in ignore:
            continue
        candidates = [child] + sorted(p for p in child.iterdir() if p.is_dir() and p.name not in excludes
                                      and not p.name.startswith(".") and p.name not in ("proto", "src"))
        for cand in candidates:
            a = next((a for a in ANALYZERS if a.detect(cand)), None)
            if a:
                projects[child.name] = Project(child.name, cand, a)
                break
    for p in projects.values():
        try:
            p.names = [p.id] + p.analyzer.quick_names(p.root)
        except Exception as e:  # never let one project break the run
            print(f"  ! quick_names failed for {p.id}: {e}", file=sys.stderr)
            p.names = [p.id]
    return projects


def _norm_path(p: str) -> str:
    p = p.split("?")[0]
    p = re.sub(r"\{[^}]*\}|:\w+|\*\w*", "{}", p)
    return "/" + "/".join(s for s in p.split("/") if s).lower()


class Workspace:
    def __init__(self, settings: dict):
        self.settings = settings
        self.root = Path(settings["source_root"])
        self.projects = discover(self.root, settings)
        self.services: Dict[str, Service] = {}
        self.index: Dict[str, str] = {}
        # a name claimed by several projects (e.g. a shared artifactId) is ambiguous -> not indexed
        owners = defaultdict(set)
        for p in self.projects.values():
            for n in p.names:
                owners[n.lower().strip("/")].add(p.id)
        for p in self.projects.values():
            self.index[p.id.lower()] = p.id
        for n, ids in owners.items():
            if len(ids) == 1:
                self.index.setdefault(n, next(iter(ids)))
        for alias, sid in (settings.get("aliases") or {}).items():
            self.index[alias.lower()] = sid
        self.nodes: Dict[str, Node] = {}
        self.edges: Dict[Tuple[str, str, str], Edge] = {}
        self.scope: Dict[str, int] = {}

    # ------------------------------------------------------------------ analysis
    def analyze_all(self) -> None:
        for p in self.projects.values():
            try:
                self.services[p.id] = p.analyzer.analyze(p.id, p.root, self.settings)
            except Exception as e:
                print(f"  ! analysis failed for {p.id}: {e}", file=sys.stderr)
        grpc_providers = {}
        for s in self.services.values():
            for ps in s.provides_grpc:
                grpc_providers[ps.lower()] = s.id
        grpc_alias = {k.lower(): v for k, v in (self.settings.get("grpc_aliases") or {}).items()}
        for s in self.services.values():
            for d in s.dependencies:
                if d.kind == HTTP:
                    d.resolved = self.index.get(d.target.lower())
                    if d.resolved == s.id:
                        d.resolved = None
                elif d.kind == GRPC:
                    proto = next((x.split(": ", 1)[1] for x in d.details if x.startswith("proto service: ")), "")
                    d.resolved = grpc_providers.get(proto.lower()) or self.index.get(grpc_alias.get(d.target.lower(), "").lower())
                if d.resolved and d.kind == HTTP:
                    self._match_ops(d)

    def _match_ops(self, d: Dependency) -> None:
        callee = self.services.get(d.resolved)
        if not callee or not callee.endpoints:
            return
        table = {}
        for e in callee.endpoints:
            for m in e.method.split("|"):
                table[(m, _norm_path((callee.context_path or "") + e.path))] = f"{e.component}.{e.handler}" if e.handler else e.component
                table[(m, _norm_path(e.path))] = table[(m, _norm_path((callee.context_path or "") + e.path))]
        for op in d.operations:
            parts = op.name.split(" ", 1)
            if len(parts) != 2:
                continue
            meth, path = parts
            np = _norm_path(path)
            op.matched = table.get((meth, np)) or table.get(("ANY", np))
            if not op.matched:
                # tolerate gateway prefixes such as /k8s-agent/... by suffix matching
                for (m, p), h in table.items():
                    if m in (meth, "ANY") and len(p) > 3 and np.endswith(p):
                        op.matched = h
                        break

    # ------------------------------------------------------------------ scope
    def select_scope(self, targets: List[str], depth: int, follow: List[str]) -> None:
        unknown = [t for t in targets if t not in self.services and t.lower() not in self.index]
        if unknown:
            raise SystemExit(f"Unknown target(s): {unknown}. Known projects: {sorted(self.services)}")
        targets = [t if t in self.services else self.index[t.lower()] for t in targets]
        consumers = defaultdict(set)
        for s in self.services.values():
            for d in s.dependencies:
                if d.kind in (KAFKA, RABBITMQ) and d.direction == "consume":
                    consumers[(d.kind, d.target)].add(s.id)
        q = deque((t, 0) for t in targets)
        self.scope = {t: 0 for t in targets}
        while q:
            sid, lvl = q.popleft()
            if depth >= 0 and lvl >= depth:
                continue
            nxt = set()
            for d in self.services[sid].dependencies:
                if d.kind in (HTTP, GRPC) and d.kind in follow and d.resolved:
                    nxt.add(d.resolved)
                if d.kind in (KAFKA, RABBITMQ) and "messaging" in follow and d.direction == "publish":
                    nxt |= consumers.get((d.kind, d.target), set())
            for n in nxt:
                if n not in self.scope and n != sid:
                    self.scope[n] = lvl + 1
                    q.append((n, lvl + 1))

    # ------------------------------------------------------------------ graph
    def _node(self, nid, label, typ, **kw) -> Node:
        if nid not in self.nodes:
            self.nodes[nid] = Node(nid, label, typ, **kw)
        return self.nodes[nid]

    def _edge(self, src, dst, kind, label, inferred=False) -> None:
        k = (src, dst, kind)
        if k in self.edges:
            self.edges[k].count += 1
        else:
            self.edges[k] = Edge(src, dst, kind, label, inferred=inferred)

    def build_graph(self) -> None:
        for sid, tier in self.scope.items():
            s = self.services[sid]
            self._node(f"svc:{sid}", sid, "service", tier=tier, service=sid)
        for sid in self.scope:
            s = self.services[sid]
            me = f"svc:{sid}"
            for d in s.dependencies:
                if d.kind in (HTTP, GRPC) and d.resolved:
                    n = self._node(f"svc:{d.resolved}", d.resolved, "service", service=d.resolved)
                    if n.tier is None and d.resolved in self.scope:
                        n.tier = self.scope[d.resolved]
                    lbl = "REST" if d.kind == HTTP else "gRPC"
                    self._edge(me, n.id, d.kind, f"{lbl} ({len(d.operations)} ops)" if d.operations else lbl, d.inferred)
                elif d.kind == HTTP:
                    if d.address:
                        host = re.sub(r"^https?://", "", d.address).split("/")[0] or d.target
                        n = self._node(f"ext:{d.target.lower()}", f"{d.target} ({host})", "external")
                    else:
                        n = self._node(f"stub:{d.target.lower()}", d.target, "stub")
                    self._edge(me, n.id, d.kind, f"REST ({len(d.operations)} ops)" if d.operations else "REST", d.inferred)
                elif d.kind == GRPC:
                    proto = next((x.split(": ", 1)[1] for x in d.details if x.startswith("proto service: ")), d.target)
                    n = self._node(f"grpc:{proto.lower()}", f"{proto} gRPC", "grpc-ext")
                    self._edge(me, n.id, d.kind, f"gRPC ({len(d.operations)} rpcs)" if d.operations else "gRPC")
                elif d.kind in (KAFKA, RABBITMQ):
                    n = self._node(f"{d.kind}:{d.target}", d.target, d.kind)
                    if d.direction == "consume":
                        self._edge(n.id, me, d.kind, "consume", d.inferred)
                    else:
                        self._edge(me, n.id, d.kind, "publish", d.inferred)
                elif d.kind in (DATABASE, CACHE, REGISTRY, STORAGE, EXTERNAL):
                    n = self._node(f"{d.kind}:{d.target.lower()}", d.target, d.kind)
                    self._edge(me, n.id, d.kind, d.label or d.kind)
        # wire messaging peers that are outside scope as stubs so the producer/consumer is visible
        for s in self.services.values():
            if s.id in self.scope:
                continue
            for d in s.dependencies:
                nid = f"{d.kind}:{d.target}"
                if d.kind in (KAFKA, RABBITMQ) and nid in self.nodes:
                    peer = self._node(f"svc:{s.id}", s.id, "service", service=s.id)
                    if d.direction == "consume":
                        self._edge(nid, peer.id, d.kind, "consume", d.inferred)
                    else:
                        self._edge(peer.id, nid, d.kind, "publish", d.inferred)
