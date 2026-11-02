"""Analyzer plugin contract.

To support a new stack (Node, .NET, Python...), subclass Analyzer, implement
`detect` and `analyze`, and register it in docgen/analyzers/__init__.py.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..model import Service


class Analyzer:
    name = "base"
    language = "unknown"

    def detect(self, root: Path) -> bool:
        """Return True when `root` is a project this analyzer understands."""
        raise NotImplementedError

    def quick_names(self, root: Path) -> list:
        """Cheap list of names the project is known by (app name, module name...)."""
        return []

    def analyze(self, service_id: str, root: Path, settings: dict) -> Service:
        raise NotImplementedError

    # helpers shared by analyzers -------------------------------------------------
    @staticmethod
    def read_readme(root: Path) -> str:
        for name in ("README.md", "Readme.md", "readme.md", "README.MD"):
            p = root / name
            if p.exists():
                text = p.read_text(encoding="utf-8", errors="ignore")
                lines = [l.strip() for l in text.splitlines()]
                body = [l for l in lines if l and not l.startswith(("#", "```", "|", "![", "<"))]
                return " ".join(body)[:600]
        return ""

    @staticmethod
    def existing_docs(root: Path) -> list:
        docs = []
        for sub in ("docs", "doc", "arc", "architecture"):
            d = root / sub
            if d.is_dir():
                docs += sorted(f"{sub}/{p.name}" for p in d.iterdir() if p.suffix.lower() in (".md", ".adoc", ".txt"))
        for p in sorted(root.glob("*.md")):
            if p.name.lower() != "readme.md":
                docs.append(p.name)
        return docs

    @staticmethod
    def deployment(root: Path) -> list:
        found = []
        for name, label in (("Dockerfile", "Dockerfile"), ("Jenkinsfile", "Jenkins pipeline"),
                            ("charts", "Helm chart (charts/)"), ("helm-charts", "Helm chart (helm-charts/)"),
                            ("deployment", "Deployment manifests (deployment/)"), ("docker-compose.yml", "docker-compose")):
            if (root / name).exists():
                found.append(label)
        return found
