"""Analyzer registry. Order matters: the first analyzer whose detect() is True wins."""
from .base import Analyzer
from .golang import GoAnalyzer
from .java_spring import SpringJavaAnalyzer

ANALYZERS = [SpringJavaAnalyzer(), GoAnalyzer()]


def register(analyzer: Analyzer, first: bool = False) -> None:
    """Add a custom analyzer (e.g. from a plugin module listed in docgen.yaml)."""
    if first:
        ANALYZERS.insert(0, analyzer)
    else:
        ANALYZERS.append(analyzer)
