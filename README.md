# docs-sources-generate (docgen)

Scans a workspace of service repositories and generates architecture documentation:

- **Architecture**: a container view of the target services, the services they call, brokers, data stores and external platforms, plus a component view for each service.
- **Relationship diagram** (Mermaid): REST, gRPC, Kafka and RabbitMQ links, each traced back to `file:line`.
- **Feature & function diagrams** (Java services): one flowchart per feature (a controller, the message listeners, or the scheduled jobs) and one sequence diagram per function (each endpoint, listener and job). Each one is traced through controller → service → helper classes until it leaves the service via a REST/gRPC call, database, Redis, Kafka/RabbitMQ or MinIO.
- **Description**: a written summary of each service, its tech stack, API endpoints, outbound calls (matched to the callee's handler), messaging, scheduled jobs, data model and configured endpoints (secrets removed).

Static analysis only. Nothing is compiled or executed, and no network access is needed.

## Quick start

```bash
pip install pyyaml            # only dependency
python -m docgen              # reads ./docgen.yaml
```

Open `output/index.html` in a browser for a rendered view with diagrams and service navigation. It loads marked and mermaid from a CDN. You can also read `output/index.md` directly in VS Code, GitLab or GitHub, which all render Mermaid.

### CLI

```bash
python -m docgen --list                                   # what projects were detected
python -m docgen -t obs-service -t opensearch-service -d 1
python -m docgen -t sqldb-service -d 2 -o output-sqldb    # different scope, different folder
python -m docgen --all                                    # whole workspace
python -m docgen --root D:/other/workspace -c other.yaml
```

| Option | Meaning |
|---|---|
| `-t/--target` | Service to document. Repeatable. Overrides `targets:` |
| `-d/--depth` | Callee hops to include: `1` = direct callees, `-1` = unlimited |
| `-o/--out` | Output folder |
| `--root` | Workspace folder (one sub-folder per service) |

## Output

```
output/
  index.md            overview: services table, architecture, relationship diagram, interactions
  services/<id>.md    one page per service in scope
  features/<id>.md    feature flowcharts + per-function sequence diagrams
  graph.json          machine-readable nodes, edges, services (for other tools / CI diffing)
  index.html          self-contained viewer
```

## What is detected

| Stack | Detected |
|---|---|
| Spring Boot (Maven/Gradle) | `@RestController` endpoints; `@FeignClient` (Eureka name or URL) plus dynamically built Feign interfaces, with unused ones ignored; `@GrpcClient` stubs and the RPCs actually invoked; `@GrpcService` servers; `@KafkaListener` and KafkaTemplate topics; `@RabbitListener` queues and exchanges/routing keys from config; JPA `@Entity`/`@Table`; `@RedisHash`; `@Scheduled` jobs; MySQL/Postgres/Mongo, Redis, Eureka, MinIO; tech stack from `pom.xml`; ports and context path from `application.yml`/`.properties` |
| Go | gin/echo/chi/net/http routes (including groups), reverse proxies, `grpc.Dial`, HTTP URL literals, go-redis, SQL drivers, Mongo, client-go/Helm, Eureka; tech from `go.mod`; `config.yaml` |

**Cross-service resolution.** References are matched to workspace folders by folder name, `spring.application.name`, context path, Maven `artifactId` and Go module name. gRPC clients are linked to the workspace project that implements the proto service. Messaging links services through shared topics and `exchange / routing-key` pairs.

**Scope.** Every project is analyzed. Only the targets and the services they reach within `depth` hops get pages: via REST, gRPC, or by consuming a topic/exchange the target publishes to (`follow:` in the config).

**Redaction.** `ENC(...)` values and any key containing password/secret/token/key/jaas/sasl are dropped. URL credentials are masked.

## Feature & function diagrams

Entry points are `@*Mapping` handlers, `@RabbitListener`/`@KafkaListener` methods and `@Scheduled` methods. From each one the tracer walks the service's own classes and records, in source order:

| In code | In the sequence diagram |
|---|---|
| call to another class of the service | arrow `A ->> B: method()` |
| call to a private/own method | self-arrow, followed by what that method does |
| `if / else if / else`, `switch` | `alt … else … end` |
| `for`, `while`, `do`, `.forEach(...)` | `loop … end` |
| `catch (X e)` | `opt on X … end` |
| `throw new X`, `orElseThrow(() -> new X)` | note `throws X` / `throws if absent: X` |
| Feign client call | arrow to the target service labelled with the real `METHOD /path` |
| gRPC stub, repository, Kafka/Rabbit/Redis template, MinIO, RestTemplate/OkHttp | arrow to that system (gRPC service, MySQL, Kafka…) |
| handler return type | dashed return arrow to the client |

**How names are resolved.** Interfaces map to their implementation (`FooService` → `FooServiceImpl`). Class names are resolved through each file's imports, so two classes with the same name in different packages don't get mixed up. Self-injected proxies (`self.foo()`) count as own calls.

**What's left out.** Calls into DTO, entity, utils, config and exception packages, plain getters and setters, and blocks with nothing inside are omitted. This keeps diagrams about behaviour rather than boilerplate.

**Limits.** This is static analysis, so calls made through reflection, or through lambdas handed to other code, can be missed. Very long flows are capped at `features.max_steps` and marked "truncated" (`+` in the steps column).

```yaml
features:
  enabled: true
  max_depth: 6
  max_steps: 80
  control_flow: true   # alt / loop / opt / throws
  self_calls: true     # private helper methods as self-calls
  # skip_packages: [dto, entity, model, enums, constants, exception, utils, config]
```

In `index.html`, the sequence diagrams sit in collapsed sections and render when opened, so large services stay fast. Go services don't get function diagrams yet.

## Tuning for your codebase

- Unresolved names appear under **Unresolved references** in `index.md`. Map them in `docgen.yaml`:
  ```yaml
  aliases:
    k8s-agent: k8s_helm_sdk
  grpc_aliases:
    grpc-provisions-service: provisions-service
  ```
- Skip folders that aren't services with `ignore_projects:`.
- Dotted arrows mean *inferred from config only*. For example, a RabbitMQ exchange in `rabbitmq.properties` that has no listener.

## Adding a language or framework

1. Create `docgen/analyzers/<name>.py` with a subclass of `Analyzer`:
   ```python
   from docgen.analyzers.base import Analyzer
   from docgen.model import Service, Dependency, Endpoint, HTTP

   class NodeAnalyzer(Analyzer):
       name = "node"
       def detect(self, root):  return (root / "package.json").exists()
       def quick_names(self, root): return [...]      # names other services use to call it
       def analyze(self, service_id, root, settings) -> Service:
           svc = Service(id=service_id, path=..., language="typescript", framework="NestJS")
           svc.endpoints.append(Endpoint("GET", "/users", "UsersController.list"))
           svc.dependencies.append(Dependency(HTTP, "user-service", operations=[...]))
           return svc
   ```
2. Register it, either in `docgen/analyzers/__init__.py` (`ANALYZERS`) or from your own module listed under `plugins:` in `docgen.yaml` that calls `docgen.analyzers.register(NodeAnalyzer())`.

The resolver, scope, diagrams and docs work unchanged for any analyzer that fills the `Service` model (`docgen/model.py`).

## Layout

```
docgen/
  cli.py            entry point, config loading, output writing
  model.py          Service / Dependency / Endpoint / Operation data model
  workspace.py      discovery, name resolution, endpoint matching, scope BFS, graph
  util.py           comment stripping, YAML/properties flattening, placeholders, redaction
  analyzers/        java_spring.py, java_flows.py (call graph), golang.py, base.py (plugin contract)
  render/           markdown.py, mermaid.py, features.py, html.py
docgen.yaml         run configuration
```

## Ideas for next steps

- Optional LLM pass that turns the extracted facts into richer prose descriptions (input: `graph.json`).
- Cross-service sequence diagrams that stitch a function's REST call into the callee's own function flow.
- Run in CI on every merge and diff `graph.json` to catch new or removed dependencies.
- Kafka/AMQP topic extraction for Go; Node/.NET/Python analyzers.
