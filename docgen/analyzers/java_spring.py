"""Spring Boot (Java/Kotlin-on-Maven/Gradle) analyzer.

Detects: REST controllers, OpenFeign clients (static & dynamic), gRPC clients/servers,
Kafka listeners/producers, RabbitMQ listeners/publishers, JPA entities, Redis, Eureka,
scheduled jobs, raw HTTP clients and configured endpoints.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..model import (CACHE, DATABASE, GRPC, HTTP, KAFKA, RABBITMQ, REGISTRY, STORAGE, EXTERNAL,
                     Component, Dependency, Endpoint, Evidence, Operation, Service)
from ..util import (config_endpoints, kebab, line_of, load_properties, load_yaml_props, read_text,
                    redact, rel, resolve_placeholders, strip_c_comments, walk)
from .base import Analyzer

TECH_MAP = {
    "spring-boot-starter-web": "Spring Web MVC",
    "spring-boot-starter-webflux": "Spring WebFlux",
    "spring-boot-starter-data-jpa": "Spring Data JPA / Hibernate",
    "spring-boot-starter-data-mongodb": "Spring Data MongoDB",
    "spring-boot-starter-data-redis": "Redis (Spring Data Redis)",
    "spring-boot-starter-amqp": "RabbitMQ (Spring AMQP)",
    "spring-kafka": "Kafka (Spring Kafka)",
    "spring-cloud-starter-netflix-eureka-client": "Eureka client",
    "spring-cloud-starter-netflix-eureka-server": "Eureka server",
    "spring-cloud-starter-openfeign": "OpenFeign",
    "spring-cloud-starter-gateway": "Spring Cloud Gateway",
    "grpc-client-spring-boot-starter": "gRPC client",
    "grpc-server-spring-boot-starter": "gRPC server",
    "grpc-spring-boot-starter": "gRPC",
    "minio": "MinIO SDK",
    "minio-admin": "MinIO Admin SDK",
    "mysql-connector-j": "MySQL",
    "mysql-connector-java": "MySQL",
    "postgresql": "PostgreSQL",
    "mongodb-driver-sync": "MongoDB",
    "spring-boot-starter-actuator": "Actuator",
    "micrometer-registry-prometheus": "Prometheus metrics",
    "springdoc-openapi-starter-webmvc-ui": "OpenAPI (springdoc)",
    "spring-boot-starter-websocket": "WebSocket",
    "jasypt-spring-boot": "Jasypt (encrypted config)",
    "spring-boot-starter-thymeleaf": "Thymeleaf",
    "thymeleaf-spring6": "Thymeleaf",
    "JDA": "Discord (JDA)",
    "caffeine": "Caffeine cache",
    "opensearch-java": "OpenSearch client",
    "opensearch-rest-high-level-client": "OpenSearch client",
    "elasticsearch-java": "Elasticsearch client",
    "kubernetes-client": "Kubernetes client",
    "aws-java-sdk-s3": "AWS S3 SDK",
    "spring-boot-starter-security": "Spring Security",
    "jjwt": "JWT (jjwt)",
    "spring-boot-starter-quartz": "Quartz scheduler",
    "logstash-logback-encoder": "Logstash JSON logging",
}

MAPPING_RE = re.compile(
    r"@(Get|Post|Put|Delete|Patch|Request)Mapping\b(?:\s*\((?P<args>(?:[^()]|\([^()]*\))*)\))?")
HANDLER_RE = re.compile(
    r"(?:\s*@[\w.]+(?:\s*\((?:[^()]|\([^()]*\))*\))?)*\s*"
    r"(?:public|protected|private)?\s*(?:static\s+)?(?:default\s+)?"
    r"(?:<[^>]+>\s*)?[\w.<>\[\]?,\s]+?\s+(\w+)\s*\(")
STR_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')
CLASS_RE = re.compile(r"\b(class|interface|enum|record)\s+(\w+)")
PKG_RE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.M)


def _ann_args(text: str, ann: str) -> Optional[Tuple[str, int]]:
    m = re.search(r"@" + ann + r"\b\s*(\((?:[^()]|\([^()]*\))*\))?", text)
    if not m:
        return None
    return (m.group(1) or "()")[1:-1], m.start()


def _attr(args: str, *names: str) -> List[str]:
    for n in names:
        m = re.search(r"\b" + n + r"\s*=\s*(\{[^}]*\}|\"(?:[^\"\\]|\\.)*\"|[\w.]+)", args)
        if m:
            return STR_RE.findall(m.group(1)) or [m.group(1)]
    return []


def _mapping_path_method(kind: str, args: str) -> Tuple[List[str], str]:
    args = args or ""
    paths = _attr(args, "value", "path")
    if not paths:
        # positional: first string literal(s) not belonging to another attribute
        head = args.split("=")[0] if "=" in args and not args.strip().startswith(('"', "{")) else args
        paths = STR_RE.findall(head) if head.strip().startswith(('"', "{")) else []
    if not paths:
        paths = [""]
    if kind == "Request":
        m = re.findall(r"RequestMethod\.(\w+)", args)
        method = "|".join(m) if m else "ANY"
    else:
        method = kind.upper()
    return paths, method


def _join(*parts: str) -> str:
    p = "/".join(x.strip("/") for x in parts if x and x.strip("/"))
    return "/" + p if p else "/"


class SpringJavaAnalyzer(Analyzer):
    name = "java-spring"
    language = "java"

    def detect(self, root: Path) -> bool:
        return any((root / f).exists() for f in ("pom.xml", "build.gradle", "build.gradle.kts"))

    # ----------------------------------------------------------------- config
    def _props(self, root: Path) -> Dict[str, str]:
        props: Dict[str, str] = {}
        res_dirs = [root / "src/main/resources"]
        for d in res_dirs:
            if not d.is_dir():
                continue
            files = sorted(d.glob("*.properties")) + sorted(d.glob("bootstrap*.y*ml")) + sorted(d.glob("application*.y*ml"))
            # base application.* last-but-profile so profile files don't override defaults
            files.sort(key=lambda p: (0 if p.suffix == ".properties" else 1, "-" in p.stem, p.name))
            for f in files:
                props.update(load_properties(f) if f.suffix == ".properties" else load_yaml_props(f))
        return props

    def quick_names(self, root: Path) -> list:
        props = self._props(root)
        names = []
        for k in ("spring.application.name", "server.servlet.context-path", "server.servlet.contextPath",
                  "spring.webflux.base-path"):
            v = props.get(k)
            if v:
                names.append(resolve_placeholders(v, props).strip("/"))
        pom = self._pom(root)
        if pom.get("artifactId"):
            names.append(pom["artifactId"])
        return [n for n in names if n and "${" not in n]

    def _pom(self, root: Path) -> dict:
        p = root / "pom.xml"
        info = {"deps": []}
        if not p.exists():
            g = root / "build.gradle"
            if g.exists():
                text = read_text(g)
                info["deps"] = re.findall(r"['\"][\w.\-]+:([\w.\-]+)(?::[\w.\-]+)?['\"]", text)
            return info
        try:
            tree = ET.parse(p)
        except ET.ParseError:
            return info
        r = tree.getroot()
        ns = {"m": r.tag.split("}")[0].strip("{")} if r.tag.startswith("{") else {}
        q = (lambda t: "m:" + t) if ns else (lambda t: t)

        def txt(el, path):
            x = el.find(path, ns)
            return x.text.strip() if x is not None and x.text else ""

        info["artifactId"] = txt(r, q("artifactId"))
        info["name"] = txt(r, q("name"))
        info["description"] = txt(r, q("description"))
        info["version"] = txt(r, q("version"))
        parent = r.find(q("parent"), ns)
        if parent is not None and "spring-boot" in txt(parent, q("artifactId")):
            info["spring_boot"] = txt(parent, q("version"))
        props = r.find(q("properties"), ns)
        if props is not None:
            for child in props:
                tag = child.tag.split("}")[-1]
                if tag in ("java.version", "maven.compiler.source"):
                    info["java"] = (child.text or "").strip()
        deps = r.find(q("dependencies"), ns)
        if deps is not None:
            for d in deps.findall(q("dependency"), ns):
                info["deps"].append(txt(d, q("artifactId")))
        return info

    # ----------------------------------------------------------------- main
    def analyze(self, service_id: str, root: Path, settings: dict) -> Service:
        ws_root = Path(settings["source_root"])
        props = self._props(root)
        pom = self._pom(root)
        svc = Service(id=service_id, path=rel(root, ws_root), language="java", framework="Spring Boot")
        svc.names = list(dict.fromkeys([service_id] + self.quick_names(root)))
        svc.display_name = resolve_placeholders(props.get("spring.application.name", service_id), props)
        svc.version = pom.get("version", "")
        svc.readme_summary = self.read_readme(root) or pom.get("description", "")
        svc.existing_docs = self.existing_docs(root)
        svc.deployment = self.deployment(root)
        if pom.get("spring_boot"):
            svc.framework = f"Spring Boot {pom['spring_boot']}"
        if pom.get("java"):
            svc.tech.append(f"Java {pom['java']}")
        for d in pom["deps"]:
            if d in TECH_MAP and TECH_MAP[d] not in svc.tech:
                svc.tech.append(TECH_MAP[d])

        port = props.get("server.port")
        if port:
            svc.ports["http"] = resolve_placeholders(port, props)
        mport = props.get("management.server.port")
        if mport:
            svc.ports["actuator"] = resolve_placeholders(mport, props)
        gport = props.get("grpc.server.port")
        if gport:
            svc.ports["grpc"] = resolve_placeholders(gport, props)
        svc.context_path = resolve_placeholders(
            props.get("server.servlet.context-path", props.get("server.servlet.contextPath", "")), props)
        svc.config_endpoints = config_endpoints(props)

        java_files = list(walk(root / "src/main", (".java", ".kt"), settings["excludes"])) if (root / "src/main").is_dir() \
            else list(walk(root, (".java", ".kt"), settings["excludes"]))
        svc.file_count = len(java_files)

        packages = []
        sources = []
        for f in java_files:
            raw = read_text(f)
            code = strip_c_comments(raw)
            pm = PKG_RE.search(code)
            pkg = pm.group(1) if pm else ""
            packages.append(pkg)
            sources.append((f, rel(f, ws_root), code, pkg))
        svc.layers = self._layers(packages)

        http_deps: Dict[str, Dependency] = {}
        self._dynamic = []
        grpc_deps: Dict[str, Dependency] = {}
        kafka_consumed, kafka_produced = {}, {}
        rabbit_listened: Dict[str, Evidence] = {}
        raw_http_files = []

        for f, rpath, code, pkg in sources:
            cls_m = CLASS_RE.search(code)
            cls = cls_m.group(2) if cls_m else f.stem
            if re.search(r"@(Rest)?Controller\b", code):
                self._controller(svc, code, cls, rpath)
            if re.search(r"\binterface\s+\w+", code) and MAPPING_RE.search(code) and not re.search(r"@(Rest)?Controller\b", code):
                self._feign(svc, code, cls, rpath, props, http_deps)
            if "@GrpcClient" in code:
                self._grpc_clients(code, rpath, props, grpc_deps)
            if "@GrpcService" in code:
                for m in re.finditer(r"extends\s+(\w+)Grpc\.\w+ImplBase", code):
                    rpcs = sorted(set(re.findall(r"public\s+void\s+(\w+)\s*\(", code)))
                    svc.provides_grpc.setdefault(m.group(1), []).extend(rpcs)
                    svc.components.append(Component("grpc-server", cls, m.group(1), Evidence(rpath, line_of(code, m.start()))))
            if "@KafkaListener" in code:
                for m in re.finditer(r"@KafkaListener\s*\(((?:[^()]|\([^()]*\))*)\)", code):
                    for t in _attr(m.group(1), "topics", "topicPattern") or STR_RE.findall(m.group(1))[:1]:
                        t = resolve_placeholders(t, props)
                        kafka_consumed.setdefault(t, Evidence(rpath, line_of(code, m.start())))
                    svc.components.append(Component("consumer", cls, "Kafka listener", Evidence(rpath, line_of(code, m.start()))))
            if "KafkaTemplate" in code:
                for m in re.finditer(r"\.send\(\s*\"([^\"]+)\"", code):
                    kafka_produced.setdefault(m.group(1), Evidence(rpath, line_of(code, m.start())))
                for m in re.finditer(r"\bString\s+\w*[tT]opic\w*\s*=\s*\"([^\"]+)\"", code):
                    kafka_produced.setdefault(m.group(1), Evidence(rpath, line_of(code, m.start())))
            if "@RabbitListener" in code:
                for m in re.finditer(r"@RabbitListener\s*\(((?:[^()]|\([^()]*\))*)\)", code):
                    for q in _attr(m.group(1), "queues", "queuesToDeclare"):
                        rabbit_listened.setdefault(q, Evidence(rpath, line_of(code, m.start())))
                svc.components.append(Component("consumer", cls, "RabbitMQ listener"))
            if re.search(r"\.convertAndSend\(|rabbitTemplate\.send\(", code):
                svc.components.append(Component("producer", cls, "RabbitMQ publisher"))
            for m in re.finditer(r"@Scheduled\s*\(((?:[^()]|\([^()]*\))*)\)", code):
                hm = HANDLER_RE.match(code, m.end())
                sched = ", ".join(f"{k}={v}" for k, v in re.findall(r"(cron|fixedRate|fixedDelay|fixedDelayString|fixedRateString)\s*=\s*\"?([^\",)]+)", m.group(1)))
                svc.components.append(Component("job", f"{cls}.{hm.group(1) if hm else '?'}", sched, Evidence(rpath, line_of(code, m.start()))))
            if "@Entity" in code:
                t = _ann_args(code, "Table")
                name = (_attr(t[0], "name") or [cls])[0] if t else cls
                svc.entities.append(f"{name} ({cls})" if name != cls else cls)
            if "@RedisHash" in code:
                a = _ann_args(code, "RedisHash")
                svc.entities.append(f"redis:{(STR_RE.findall(a[0]) or [cls])[0]} ({cls})")
            if re.search(r"\bnew\s+Request\.Builder\(|restTemplate\.(exchange|getForObject|postForObject|getForEntity|postForEntity)|WebClient\.", code):
                raw_http_files.append(rpath)

        for cls, rpath, key in self._dynamic:
            used = any(rp != rpath and re.search(r"\b" + cls + r"\b", code) for _, rp, code, _ in sources)
            if not used and key in http_deps:
                del http_deps[key]
                svc.notes.append(f"Ignored unused client interface `{cls}` ({rpath}) — no references found.")
        svc.dependencies.extend(http_deps.values())
        svc.dependencies.extend(grpc_deps.values())
        self._kafka(svc, props, kafka_consumed, kafka_produced)
        self._rabbit(svc, props, rabbit_listened)
        self._infra(svc, props, pom)
        if raw_http_files:
            svc.notes.append(
                f"Raw HTTP clients (RestTemplate/OkHttp/WebClient) used in {len(raw_http_files)} file(s) — targets are "
                f"usually dynamic (config/DB driven) and listed under configured endpoints: "
                + ", ".join(sorted(set(raw_http_files))[:8]))
        svc.proto_catalog = self._protos(root, settings)
        svc.entities.sort()
        if (settings.get("features") or {}).get("enabled", True):
            from .java_flows import build_flows
            try:
                svc.flows = build_flows(sources, svc, props, settings)
            except Exception as e:  # flows are best-effort; never fail the run
                svc.notes.append(f"Function flow extraction failed: {e}")
        return svc

    # ----------------------------------------------------------------- pieces
    @staticmethod
    def _layers(packages: List[str]) -> Dict[str, int]:
        pk = [p for p in packages if p]
        if not pk:
            return {}
        parts = [p.split(".") for p in pk]
        common = []
        for seg in zip(*parts):
            if len(set(seg)) == 1:
                common.append(seg[0])
            else:
                break
        c = Counter()
        for p in parts:
            rest = p[len(common):]
            c[rest[0] if rest else "(root)"] += 1
        return dict(c.most_common())

    def _controller(self, svc: Service, code: str, cls: str, rpath: str) -> None:
        cm = CLASS_RE.search(code)
        head = code[:cm.start()] if cm else ""
        base = ""
        hm = MAPPING_RE.search(head)
        if hm:
            base = _mapping_path_method(hm.group(1), hm.group("args"))[0][0]
        body_start = cm.end() if cm else 0
        n = 0
        for m in MAPPING_RE.finditer(code, body_start):
            paths, method = _mapping_path_method(m.group(1), m.group("args"))
            h = HANDLER_RE.match(code, m.end())
            for p in paths:
                svc.endpoints.append(Endpoint(method, _join(base, p), h.group(1) if h else "", cls,
                                              Evidence(rpath, line_of(code, m.start()))))
                n += 1
        svc.components.append(Component("controller", cls, f"{_join(base)} ({n} endpoints)", Evidence(rpath, 1)))

    def _feign(self, svc, code, cls, rpath, props, deps: Dict[str, Dependency]) -> None:
        fa = _ann_args(code, "FeignClient")
        url = ""
        if fa:
            args = fa[0]
            names = _attr(args, "name", "value") or STR_RE.findall(args)[:1]
            target = names[0] if names else cls
            url = (_attr(args, "url") or [""])[0]
            label = f"Feign: {cls}"
            inferred = False
        else:
            # Interface with Spring mappings but no @FeignClient -> dynamically built Feign client
            if not re.search(r"(Feign|Client|Api)\b", cls) and "feign" not in rpath.lower():
                return
            target = kebab(re.sub(r"(FeignClient|Client|Feign|Api)$", "", cls))
            label = f"Feign (dynamic URL): {cls}"
            inferred = True
            self._dynamic.append((cls, rpath, target.lower()))
        resolved_url = resolve_placeholders(url, props) if url else ""
        cm = CLASS_RE.search(code)
        head = code[:cm.start()] if cm else ""
        base = ""
        hm = MAPPING_RE.search(head)
        if hm:
            base = _mapping_path_method(hm.group(1), hm.group("args"))[0][0]
        key = target.lower()
        dep = deps.get(key)
        if not dep:
            dep = Dependency(HTTP, target, label=label, inferred=inferred,
                             address=redact("url", resolved_url))
            deps[key] = dep
        else:
            dep.label += f", {cls}"
        dep.evidence.append(Evidence(rpath, line_of(code, fa[1]) if fa else 1))
        for m in MAPPING_RE.finditer(code, cm.end() if cm else 0):
            paths, method = _mapping_path_method(m.group(1), m.group("args"))
            h = HANDLER_RE.match(code, m.end())
            for p in paths:
                dep.operations.append(Operation(f"{method} {_join(base, p)}", f"{cls}.{h.group(1) if h else '?'}",
                                                evidence=Evidence(rpath, line_of(code, m.start()))))

    def _grpc_clients(self, code, rpath, props, deps: Dict[str, Dependency]) -> None:
        for m in re.finditer(r"@GrpcClient\s*\(\s*\"([^\"]+)\"[^)]*\)\s*(?:[\w<>]+\s+)*?([\w.]*?)(\w+)Grpc\.\w+Stub\s+(\w+)\s*;", code):
            client, proto_svc, var = m.group(1), m.group(3), m.group(4)
            dep = deps.get(client)
            if not dep:
                addr = props.get(f"grpc.client.{client}.address", "")
                dep = Dependency(GRPC, client, label=f"gRPC: {proto_svc}", address=redact("address", addr))
                dep.details.append(f"proto service: {proto_svc}")
                deps[client] = dep
            dep.evidence.append(Evidence(rpath, line_of(code, m.start())))
            seen = {o.name for o in dep.operations}
            for c in re.finditer(r"\b" + re.escape(var) + r"\s*(?:\.\s*with\w+\([^)]*\)\s*)*\.\s*(\w+)\s*\(", code):
                op = c.group(1)
                if op.startswith("with") or op in seen:
                    continue
                seen.add(op)
                dep.operations.append(Operation(op, proto_svc, evidence=Evidence(rpath, line_of(code, c.start()))))
            # wrapper pattern: stub() returning the stub with deadline
            if re.search(r"\b\w+Stub\s+stub\s*\(", code):
                for c in re.finditer(r"\bstub\(\)\s*\.\s*(\w+)\s*\(", code):
                    if c.group(1) not in seen:
                        seen.add(c.group(1))
                        dep.operations.append(Operation(c.group(1), proto_svc, evidence=Evidence(rpath, line_of(code, c.start()))))

    def _kafka(self, svc, props, consumed, produced) -> None:
        for k, v in props.items():
            if "topic" in k.lower() and v and not re.search(r"(partition|replica|count|size|enabled)", k, re.I):
                t = resolve_placeholders(v, props)
                if t not in consumed and t not in produced:
                    produced[t] = None
        for t, ev in consumed.items():
            svc.dependencies.append(Dependency(KAFKA, t, "consume", label="@KafkaListener",
                                               evidence=[ev] if ev else []))
        for t, ev in produced.items():
            if t in consumed:
                svc.dependencies.append(Dependency(KAFKA, t, "publish", label="KafkaTemplate", evidence=[ev] if ev else []))
                continue
            svc.dependencies.append(Dependency(KAFKA, t, "publish", label="KafkaTemplate" if ev else "config (inferred)",
                                               evidence=[ev] if ev else [], inferred=ev is None))
        if consumed or produced:
            bs = props.get("spring.kafka.bootstrap-servers", "")
            for d in svc.dependencies:
                if d.kind == KAFKA:
                    d.address = redact("bootstrap", bs)

    def _rabbit(self, svc, props, listened: Dict[str, Evidence]) -> None:
        groups: Dict[str, Dict[str, str]] = defaultdict(dict)
        for k, v in props.items():
            m = re.match(r"(?:spring\.)?rabbitmq\.(exchange|exchanges|routing-key|routing_key|routingkey|routing-keys|queue|queues)\.(.+)$", k, re.I)
            if m:
                kind = m.group(1).lower().rstrip("s").replace("_", "-").replace("routingkey", "routing-key")
                groups[m.group(2)][kind] = resolve_placeholders(v, props)
        listened_names = {}
        for q, ev in listened.items():
            listened_names[resolve_placeholders(q, props)] = (q, ev)
        used_queues = set()
        for gname, g in sorted(groups.items()):
            q = g.get("queue")
            ex, rk = g.get("exchange"), g.get("routing-key")
            target = f"{ex} / {rk}" if ex and rk else (ex or q or gname)
            if q and q in listened_names:
                used_queues.add(q)
                ev = listened_names[q][1]
                d = Dependency(RABBITMQ, target, "consume", label="@RabbitListener", evidence=[ev])
                d.details.append(f"queue: {q}")
            elif ex:
                d = Dependency(RABBITMQ, target, "publish", label="RabbitTemplate (from config)", inferred=True)
            else:
                continue
            d.details.append(f"config group: {gname}")
            svc.dependencies.append(d)
        for qname, (raw, ev) in listened_names.items():
            if qname not in used_queues:
                svc.dependencies.append(Dependency(RABBITMQ, qname, "consume", label="@RabbitListener", evidence=[ev],
                                                   details=[f"queue: {qname}"]))

    def _infra(self, svc, props, pom) -> None:
        deps = set(pom["deps"])
        urls = " ".join(v for k, v in props.items() if "datasource" in k and k.endswith("url"))
        db = None
        if {"mysql-connector-j", "mysql-connector-java"} & deps or "mysql" in urls:
            db = "MySQL"
        elif "postgresql" in deps or "postgresql" in urls:
            db = "PostgreSQL"
        elif "spring-boot-starter-data-mongodb" in deps:
            db = "MongoDB"
        if db:
            schema = ""
            m = re.search(r"jdbc:\w+://[^/]+/(\w+)", urls)
            if m:
                schema = m.group(1)
            d = Dependency(DATABASE, db, label="JPA" if "spring-boot-starter-data-jpa" in deps else "JDBC")
            if schema:
                d.details.append(f"schema: {schema}")
            if "ENC(" in urls:
                d.details.append("connection string encrypted (Jasypt)")
            if any(k.startswith("spring.datasource.read") for k in props):
                d.details.append("read/write split datasource")
            svc.dependencies.append(d)
        if "spring-boot-starter-data-redis" in deps:
            host = props.get("spring.data.redis.host", props.get("spring.redis.host", ""))
            svc.dependencies.append(Dependency(CACHE, "Redis", label="Spring Data Redis", address=redact("host", host)))
        if any("eureka-client" in d for d in deps):
            svc.dependencies.append(Dependency(REGISTRY, "Eureka", label="service discovery",
                                               address=redact("x", resolve_placeholders(props.get("eureka.client.serviceUrl.defaultZone", props.get("eureka.client.service-url.defaultZone", "")), props))))
        if "minio" in deps:
            svc.dependencies.append(Dependency(STORAGE, "MinIO / S3", label="MinIO SDK"))
        if "opensearch-java" in deps or "opensearch-rest-high-level-client" in deps:
            svc.dependencies.append(Dependency(EXTERNAL, "OpenSearch cluster", label="OpenSearch client"))

    @staticmethod
    def _protos(root: Path, settings) -> Dict[str, List[str]]:
        out: Dict[str, List[str]] = {}
        for p in walk(root, (".proto",), settings["excludes"]):
            text = strip_c_comments(read_text(p))
            for m in re.finditer(r"\bservice\s+(\w+)\s*\{", text):
                depth, i = 1, m.end()
                while i < len(text) and depth:
                    depth += {"{": 1, "}": -1}.get(text[i], 0)
                    i += 1
                body = text[m.end():i]
                out.setdefault(m.group(1), []).extend(re.findall(r"\brpc\s+(\w+)\s*\(", body))
        return out
