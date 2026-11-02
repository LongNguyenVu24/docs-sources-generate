"""Command-line entry point.

    python -m docgen                         # uses ./docgen.yaml
    python -m docgen -c other.yaml
    python -m docgen --root D:/src --target obs-service --target opensearch-service --depth 1
    python -m docgen --list                  # show detected projects and exit
"""
from __future__ import annotations

import argparse
import importlib
import json
import shutil
import sys
import time
from dataclasses import asdict
from pathlib import Path

from .util import DEFAULT_EXCLUDES, yaml

DEFAULTS = {
    "title": "Service documentation",
    "source_root": ".",
    "output_dir": "output",
    "targets": [],
    "depth": 1,
    "follow": ["http", "grpc", "messaging"],
    "excludes": sorted(DEFAULT_EXCLUDES),
    "ignore_projects": [],
    "aliases": {},
    "grpc_aliases": {},
    "plugins": [],
    "formats": ["markdown", "json", "html"],
    "features": {"enabled": True, "max_depth": 6, "max_steps": 80},
}


def load_settings(args) -> dict:
    s = dict(DEFAULTS)
    cfg_path = Path(args.config)
    if cfg_path.exists():
        if yaml is None:
            sys.exit("PyYAML is required to read docgen.yaml: pip install pyyaml")
        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        s.update({k: v for k, v in data.items() if v is not None})
        base = cfg_path.resolve().parent
    elif args.config != "docgen.yaml":
        sys.exit(f"Config file not found: {cfg_path}")
    else:
        base = Path.cwd()
    if args.root:
        s["source_root"] = args.root
    if args.target:
        s["targets"] = args.target
    if args.depth is not None:
        s["depth"] = args.depth
    if args.out:
        s["output_dir"] = args.out
    if args.all:
        s["targets"] = ["*"]
    root = Path(s["source_root"]).expanduser()
    s["source_root"] = str(root if root.is_absolute() else (base / root).resolve())
    out = Path(s["output_dir"]).expanduser()
    s["output_dir"] = str(out if out.is_absolute() else (base / out).resolve())
    s["excludes"] = sorted(set(s["excludes"]) | DEFAULT_EXCLUDES)
    return s


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="docgen", description="Generate architecture docs from source code.")
    ap.add_argument("-c", "--config", default="docgen.yaml")
    ap.add_argument("--root", help="workspace folder containing one sub-folder per service")
    ap.add_argument("-t", "--target", action="append", help="service to document (repeatable)")
    ap.add_argument("-d", "--depth", type=int, help="how many hops of callees to include (-1 = unlimited)")
    ap.add_argument("-o", "--out", help="output folder")
    ap.add_argument("--all", action="store_true", help="document every project in the workspace")
    ap.add_argument("--list", action="store_true", help="list detected projects and exit")
    args = ap.parse_args(argv)
    s = load_settings(args)

    for mod in s.get("plugins") or []:
        importlib.import_module(mod)  # plugin modules call docgen.analyzers.register(...)

    from .workspace import Workspace
    from .render import markdown, html, features

    t0 = time.time()
    root = Path(s["source_root"])
    if not root.is_dir():
        sys.exit(f"source_root does not exist: {root}")
    print(f"Scanning {root} ...")
    ws = Workspace(s)
    if args.list:
        for p in ws.projects.values():
            print(f"  {p.id:<28} {p.analyzer.name:<12} names={p.names}")
        return 0
    ws.analyze_all()
    targets = list(ws.services) if s["targets"] in (["*"], "*") else s["targets"]
    if not targets:
        sys.exit("No targets. Set `targets:` in docgen.yaml or pass --target / --all.")
    ws.select_scope(targets, int(s["depth"]), s["follow"])
    ws.build_graph()
    print(f"  analyzed {len(ws.services)} projects, {len(ws.scope)} in scope: "
          + ", ".join(f"{k}(d{v})" for k, v in sorted(ws.scope.items(), key=lambda kv: kv[1])))

    out = Path(s["output_dir"])
    for sub in ("services", "features"):
        (out / sub).mkdir(parents=True, exist_ok=True)
    docs = {"index.md": markdown.render_index(ws, s)}
    for sid in ws.scope:
        docs[f"services/{sid}.md"] = markdown.render_service(ws.services[sid], ws)
        if ws.services[sid].flows:
            docs[f"features/{sid}.md"] = features.render_features(ws.services[sid], ws)
    stale = list((out / "services").glob("*.md")) + list((out / "features").glob("*.md"))
    for old in stale:  # remove pages of services no longer in scope
        if f"{old.parent.name}/{old.name}" not in docs:
            try:
                old.unlink()
            except OSError:
                print(f"  ! could not remove stale {old}", file=sys.stderr)
    if "markdown" in s["formats"]:
        for name, text in docs.items():
            (out / name).write_text(text, encoding="utf-8")
    if "json" in s["formats"]:
        graph = {
            "settings": {k: s[k] for k in ("source_root", "targets", "depth", "follow")},
            "scope": ws.scope,
            "services": {sid: ws.services[sid].to_dict() for sid in ws.scope},
            "nodes": [asdict(n) for n in ws.nodes.values()],
            "edges": [asdict(e) for e in ws.edges.values()],
        }
        (out / "graph.json").write_text(json.dumps(graph, indent=2, ensure_ascii=False), encoding="utf-8")
    if "html" in s["formats"]:
        (out / "index.html").write_text(html.render_html(s["title"], docs), encoding="utf-8")
    nflows = sum(len(ws.services[sid].flows) for sid in ws.scope)
    print(f"  traced {nflows} functions")
    print(f"  wrote {len(docs)} documents to {out} in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
