"""Language-agnostic data model produced by analyzers and consumed by renderers."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


# Dependency kinds
HTTP = "http"            # synchronous REST call to another service (Feign, http client...)
GRPC = "grpc"            # gRPC client call
KAFKA = "kafka"          # Kafka topic (direction: publish | consume)
RABBITMQ = "rabbitmq"    # RabbitMQ exchange/queue (direction: publish | consume)
DATABASE = "database"    # SQL / NoSQL store
CACHE = "cache"          # Redis etc.
REGISTRY = "registry"    # Eureka / Consul ...
STORAGE = "storage"      # S3 / MinIO ...
EXTERNAL = "external"    # Anything else outside the workspace


@dataclass
class Evidence:
    file: str
    line: int

    def __str__(self) -> str:
        return f"{self.file}:{self.line}"


@dataclass
class Endpoint:
    method: str
    path: str
    handler: str = ""
    component: str = ""          # controller / router file
    evidence: Optional[Evidence] = None


@dataclass
class Operation:
    """One concrete call inside a dependency (e.g. a Feign method or a gRPC rpc)."""
    name: str                    # e.g. "GET /region/{code}" or "createBucket"
    caller: str = ""             # client interface / class in the calling service
    matched: Optional[str] = None  # handler on the callee side when it could be matched
    evidence: Optional[Evidence] = None


@dataclass
class Dependency:
    kind: str
    target: str                  # raw target name as found in source (eureka name, topic, ...)
    direction: str = "out"       # out | publish | consume
    resolved: Optional[str] = None   # id of a workspace service when resolved
    label: str = ""              # human label (e.g. "Feign: InfraFeignClient")
    address: str = ""            # configured address/url (redacted)
    details: List[str] = field(default_factory=list)
    operations: List[Operation] = field(default_factory=list)
    evidence: List[Evidence] = field(default_factory=list)
    inferred: bool = False       # True when derived from config only (weaker signal)


@dataclass
class Component:
    """A notable building block inside a service (controller, client, consumer, job...)."""
    kind: str                    # controller | client | consumer | producer | job | repository | grpc-server
    name: str
    detail: str = ""
    evidence: Optional[Evidence] = None


@dataclass
class Step:
    """One arrow in a function flow (sequence diagram)."""
    src: str                     # class name, or "__entry__" for the trigger
    dst: str                     # class name, or boundary key like "feign:ObsAgentFeignClient", "db", "grpc:Provisions"
    label: str                   # method / operation
    kind: str = "call"           # call | http | grpc | db | cache | kafka | rabbitmq | storage | http-dynamic | other
    depth: int = 0


@dataclass
class Flow:
    """A function-level flow from one entry point (endpoint, listener, job) to the boundaries it reaches."""
    kind: str                    # endpoint | listener | job
    feature: str                 # grouping key: controller / "Messaging consumers" / "Scheduled jobs"
    name: str                    # "GET /v1/obs/bucket" | "RabbitMQ obs-svc.Q.order-request" | "cron=0 */5 * * * *"
    entry: str                   # "Class.method"
    steps: List[Step] = field(default_factory=list)
    truncated: bool = False
    evidence: Optional[Evidence] = None


@dataclass
class Service:
    id: str                      # workspace folder name (stable id)
    path: str
    language: str = "unknown"
    framework: str = ""
    names: List[str] = field(default_factory=list)   # aliases used to resolve references
    display_name: str = ""
    version: str = ""
    readme_summary: str = ""
    existing_docs: List[str] = field(default_factory=list)
    tech: List[str] = field(default_factory=list)
    ports: Dict[str, str] = field(default_factory=dict)
    context_path: str = ""
    layers: Dict[str, int] = field(default_factory=dict)
    components: List[Component] = field(default_factory=list)
    endpoints: List[Endpoint] = field(default_factory=list)
    dependencies: List[Dependency] = field(default_factory=list)
    provides_grpc: Dict[str, List[str]] = field(default_factory=dict)  # proto service -> rpcs implemented
    proto_catalog: Dict[str, List[str]] = field(default_factory=dict)  # proto service -> rpcs declared
    entities: List[str] = field(default_factory=list)
    config_endpoints: Dict[str, str] = field(default_factory=dict)
    deployment: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    flows: List[Flow] = field(default_factory=list)
    file_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)
