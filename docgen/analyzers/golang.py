"""Go analyzer: gin / echo / chi / net/http routes, outbound HTTP, gRPC, Kafka, AMQP, Redis, SQL."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

from ..model import (CACHE, DATABASE, GRPC, HTTP, KAFKA, RABBITMQ, REGISTRY, EXTERNAL,
                     Component, Dependency, Endpoint, Evidence, Operation, Service)
from ..util import config_endpoints, line_of, load_yaml_props, read_text, rel, strip_c_comments, walk
from .base import Analyzer

LIB_MAP = {
    "github.com/gin-gonic/gin": "Gin",
    "github.com/labstack/echo": "Echo",
    "github.com/go-chi/chi": "chi",
    "github.com/gorilla/mux": "gorilla/mux",
    "github.com/gofiber/fiber": "Fiber",
    "google.golang.org/grpc": "gRPC",
    "github.com/redis/go-redis": "Redis (go-redis)",
    "github.com/go-redis/redis": "Redis (go-redis)",
    "github.com/segmentio/kafka-go": "Kafka (kafka-go)",
    "github.com/IBM/sarama": "Kafka (sarama)",
    "github.com/Shopify/sarama": "Kafka (sarama)",
    "github.com/confluentinc/confluent-kafka-go": "Kafka (confluent)",
    "github.com/rabbitmq/amqp091-go": "RabbitMQ (amqp091)",
    "github.com/streadway/amqp": "RabbitMQ (streadway)",
    "gorm.io/gorm": "GORM",
    "github.com/go-sql-driver/mysql": "MySQL driver",
    "github.com/lib/pq": "PostgreSQL driver",
    "github.com/jackc/pgx": "PostgreSQL (pgx)",
    "go.mongodb.org/mongo-driver": "MongoDB driver",
    "github.com/spf13/viper": "Viper config",
    "github.com/sirupsen/logrus": "logrus",
    "go.uber.org/zap": "zap",
    "k8s.io/client-go": "Kubernetes client-go",
    "helm.sh/helm/v3": "Helm SDK",
    "github.com/hudl/fargo": "Eureka (fargo)",
    "github.com/ArthurHlt/go-eureka-client": "Eureka client",
    "github.com/opensearch-project/opensearch-go": "OpenSearch client",
    "github.com/minio/minio-go": "MinIO SDK",
    "github.com/prometheus/client_golang": "Prometheus client",
}

ROUTE_RE = re.compile(r"\b(\w+)\.(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS|Any|Handle|HandleFunc|Get|Post|Put|Delete|Patch)\(\s*(\"[^\"]*\"|\w+)")
GROUP_RE = re.compile(r"\b(\w+)\s*:?=\s*(\w+)\.(?:Group|Route)\(\s*\"([^\"]*)\"")
URL_LIT = re.compile(r"\"(https?://[^\"\s]+)\"")


class GoAnalyzer(Analyzer):
    name = "go"
    language = "go"

    def detect(self, root: Path) -> bool:
        return (root / "go.mod").exists()

    def _gomod(self, root: Path) -> dict:
        text = read_text(root / "go.mod")
        mod = re.search(r"^module\s+(\S+)", text, re.M)
        ver = re.search(r"^go\s+(\S+)", text, re.M)
        reqs = re.findall(r"^\s*([\w./\-]+)\s+v[\w.\-+]+(\s*//\s*indirect)?", text, re.M)
        return {"module": mod.group(1) if mod else "", "go": ver.group(1) if ver else "",
                "direct": [r for r, ind in reqs if not ind], "all": [r for r, _ in reqs]}

    def _config(self, root: Path) -> Dict[str, str]:
        props: Dict[str, str] = {}
        for pat in ("config.yaml", "config.yml", "config/*.yaml", "config/*.yml", "configs/*.yaml", "configs/*.yml"):
            for p in sorted(root.glob(pat)):
                props.update(load_yaml_props(p))
        return props

    def quick_names(self, root: Path) -> list:
        g = self._gomod(root)
        names = []
        if g["module"]:
            names.append(g["module"].rstrip("/").split("/")[-1])
        cfg = self._config(root)
        for k in ("eureka.app", "app.name", "service.name", "server.name"):
            if cfg.get(k):
                names.append(cfg[k])
        return names

    def analyze(self, service_id: str, root: Path, settings: dict) -> Service:
        ws_root = Path(settings["source_root"])
        g = self._gomod(root)
        cfg = self._config(root)
        svc = Service(id=service_id, path=rel(root, ws_root), language="go")
        svc.names = list(dict.fromkeys([service_id] + self.quick_names(root)))
        svc.display_name = service_id
        svc.readme_summary = self.read_readme(root)
        svc.existing_docs = self.existing_docs(root)
        svc.deployment = self.deployment(root)
        svc.tech.append(f"Go {g['go']}" if g["go"] else "Go")
        for r in g["all"]:
            for lib, label in LIB_MAP.items():
                if r.startswith(lib) and label not in svc.tech:
                    svc.tech.append(label)
        fw = [t for t in ("Gin", "Echo", "chi", "gorilla/mux", "Fiber") if t in svc.tech]
        svc.framework = fw[0] if fw else "net/http"
        for k in ("server.port", "port", "http.port"):
            if cfg.get(k):
                svc.ports["http"] = cfg[k]
                break
        for k, v in cfg.items():
            if k.endswith(".port") and k != "server.port" and k.count(".") == 1:
                svc.ports[k.split(".")[0]] = v
        svc.config_endpoints = config_endpoints(cfg)

        files = [f for f in walk(root, (".go",), settings["excludes"]) if not f.name.endswith("_test.go")]
        svc.file_count = len(files)
        layers: Dict[str, int] = {}
        imports_all = set()
        proxies: List[str] = []
        url_lits: Dict[str, Evidence] = {}
        grpc_dials = []
        for f in files:
            rpath = rel(f, ws_root)
            code = strip_c_comments(read_text(f))
            d = rel(f.parent, root) or "(root)"
            layers[d.split("/")[0] if d != "." else "(root)"] = layers.get(d.split("/")[0] if d != "." else "(root)", 0) + 1
            for imp in re.findall(r"\"([\w./\-]+)\"", "".join(re.findall(r"import\s*(?:\([^)]*\)|\"[^\"]+\")", code))):
                imports_all.add(imp)
            groups = {m.group(1): (m.group(2), m.group(3)) for m in GROUP_RE.finditer(code)}

            def prefix(var: str, seen=None) -> str:
                seen = seen or set()
                if var in groups and var not in seen:
                    seen.add(var)
                    parent, p = groups[var]
                    return prefix(parent, seen).rstrip("/") + "/" + p.strip("/")
                return ""

            for m in ROUTE_RE.finditer(code):
                var, meth, path = m.groups()
                if not path.startswith('"'):
                    path = "{" + path + "}"
                else:
                    path = path.strip('"')
                if var in ("http", "client", "c", "ctx", "req", "resp") and meth in ("Get", "Post"):
                    continue  # outbound http.Get etc.
                if meth in ("Get", "Post", "Put", "Delete", "Patch") and not path.startswith("/"):
                    continue
                full = (prefix(var).rstrip("/") + "/" + path.lstrip("/")) if prefix(var) else path
                method = {"Any": "ANY", "Handle": "ANY", "HandleFunc": "ANY"}.get(meth, meth.upper())
                svc.endpoints.append(Endpoint(method, full, "", rpath, Evidence(rpath, line_of(code, m.start()))))
            if re.search(r"httputil\.NewSingleHostReverseProxy|httputil\.ReverseProxy|http\.NewRequest(WithContext)?\(\s*\w+\.Request\.Method", code):
                proxies.append(rpath)
            for m in URL_LIT.finditer(code):
                url_lits.setdefault(m.group(1), Evidence(rpath, line_of(code, m.start())))
            for m in re.finditer(r"grpc\.(?:Dial|NewClient|DialContext)\(\s*(?:\w+\s*,\s*)?(\"[^\"]+\"|[\w.]+)", code):
                grpc_dials.append((m.group(1).strip('"'), Evidence(rpath, line_of(code, m.start()))))
            for m in re.finditer(r"\.Register(\w+)Server\(", code):
                svc.provides_grpc.setdefault(m.group(1), [])
        svc.layers = dict(sorted(layers.items(), key=lambda kv: -kv[1]))
        for p in sorted(set(e.component for e in svc.endpoints)):
            n = sum(1 for e in svc.endpoints if e.component == p)
            svc.components.append(Component("controller", p.split("/")[-1], f"{n} routes", Evidence(p, 1)))

        if proxies:
            svc.notes.append("Acts as a reverse proxy: forwards requests to targets resolved at runtime "
                             f"(see {', '.join(sorted(set(proxies)))}).")
            svc.dependencies.append(Dependency(HTTP, "dynamic upstream (runtime-resolved)", label="reverse proxy",
                                               inferred=True, evidence=[Evidence(p, 1) for p in sorted(set(proxies))]))
        for url, ev in url_lits.items():
            host = re.sub(r"^https?://", "", url).split("/")[0]
            hostname = host.split(":")[0]
            if hostname in ("localhost", "127.0.0.1", "0.0.0.0", "example.com") or "{" in host or "%" in host:
                continue
            svc.dependencies.append(Dependency(EXTERNAL, host, label="HTTP URL literal", address=url, evidence=[ev]))
        for target, ev in grpc_dials:
            svc.dependencies.append(Dependency(GRPC, target, label="grpc.Dial", evidence=[ev]))

        def uses(prefix: str) -> bool:
            return any(i.startswith(prefix) for i in imports_all) or any(r.startswith(prefix) for r in g["direct"])

        if uses("github.com/redis/go-redis") or uses("github.com/go-redis/redis"):
            svc.dependencies.append(Dependency(CACHE, "Redis", label="go-redis"))
        if "eureka" in cfg.get("eureka.defaultZone", "").lower() or any("eureka" in i.lower() for i in imports_all):
            svc.dependencies.append(Dependency(REGISTRY, "Eureka", label="service discovery",
                                               address=cfg.get("eureka.defaultZone", "")))
        if uses("github.com/go-sql-driver/mysql"):
            svc.dependencies.append(Dependency(DATABASE, "MySQL", label="database/sql"))
        if uses("github.com/lib/pq") or uses("github.com/jackc/pgx"):
            svc.dependencies.append(Dependency(DATABASE, "PostgreSQL", label="database/sql"))
        if uses("go.mongodb.org/mongo-driver"):
            svc.dependencies.append(Dependency(DATABASE, "MongoDB", label="mongo-driver"))
        if uses("k8s.io/client-go"):
            svc.dependencies.append(Dependency(EXTERNAL, "Kubernetes API", label="client-go"))
        if uses("helm.sh/helm"):
            svc.dependencies.append(Dependency(EXTERNAL, "Kubernetes API", label="Helm SDK"))
        if uses("github.com/opensearch-project/opensearch-go"):
            svc.dependencies.append(Dependency(EXTERNAL, "OpenSearch cluster", label="opensearch-go"))
        if any(uses(p) for p in ("github.com/segmentio/kafka-go", "github.com/IBM/sarama", "github.com/Shopify/sarama",
                                  "github.com/confluentinc/confluent-kafka-go")):
            svc.notes.append("Kafka library imported — topic names are not statically extracted for Go yet.")
        if uses("github.com/rabbitmq/amqp091-go") or uses("github.com/streadway/amqp"):
            svc.notes.append("AMQP library imported — exchange/queue names are not statically extracted for Go yet.")
        # dedupe
        seen, deps = set(), []
        for d in svc.dependencies:
            k = (d.kind, d.target, d.direction)
            if k not in seen:
                seen.add(k)
                deps.append(d)
        svc.dependencies = deps
        return svc
