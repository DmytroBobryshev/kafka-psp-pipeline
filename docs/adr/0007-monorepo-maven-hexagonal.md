# ADR-0007: Monorepo, Maven multi-module, package-by-hexagon

- **Status:** Accepted
- **Date:** 2026-08-08
- **Affects:** M1 (implements this), every service, CI

## Context

Eight services, a React UI, and shared event schemas raise two structural questions: how many
repositories, and how code is organised **inside** each service.

`libs/common-events` decides the first. It carries the Avro schemas every service depends on
(ADR-0002); in a polyrepo, one schema change means publishing a snapshot, bumping versions in N
repos, and merging N pull requests in dependency order — an entire evening, for a project that
gets a few evenings a week.

The second is decided by the plan's own test: `domain/` must compile with zero framework
dependencies. Package-by-layer (`controller/`, `service/`, `repository/`) cannot express that,
because its layers run horizontally across the whole service.

## Decision

**One repository.** Layout as in PLAN.md: `docs/`, `infra/`, `libs/`, `services/`, `ui/`.

**Maven multi-module**, one parent POM at the root:
- Parent declares `<dependencyManagement>` (Spring Boot BOM, Confluent BOM, Avro, MapStruct,
  Lombok, `lombok-mapstruct-binding`, Testcontainers BOM) and all plugin versions. Child POMs
  declare dependencies **without versions**.
- Annotation-processor order is fixed once in the parent: Lombok →
  `lombok-mapstruct-binding` → MapStruct. Getting this wrong produces MapStruct mappers that
  silently ignore Lombok-generated accessors — the single most common setup failure.
- All modules share the parent's version. There is no independent module versioning; the repo
  is released as a unit.
- `libs/common-events` and `libs/common-web` are plain jars. `common-events` MUST NOT depend on
  Spring; `common-web` MAY.
- The React UI is **not** a Maven module. It is built by pnpm and wired to the backend only
  through generated OpenAPI types (M17). Frontend-maven-plugin is explicitly avoided.

**Package-by-hexagon inside each service**, rooted at `com.example.psp.<service>`:

```
domain/          entities, value objects, domain services, ports (interfaces). Pure Java.
application/     use cases orchestrating ports. Spring annotations allowed; no adapters.
adapters/in/web        controllers, request/response DTOs, MapStruct dto <-> domain
adapters/in/kafka      listeners, MapStruct avro <-> domain
adapters/out/persistence  JPA/Mongo entities, repositories, MapStruct entity <-> domain
adapters/out/kafka     producers
adapters/out/http      external calls only (psp-connector, webhook-notifier — see ADR-0004)
config/          Spring configuration, bean wiring, profiles
```

- Dependencies point inward only: `adapters → application → domain`. Adapters never reference
  each other.
- **MapStruct at every hexagon boundary**, one mapper per boundary, `componentModel = "spring"`,
  `unmappedTargetPolicy = ERROR`. Domain types never leak into a wire contract and Avro types
  never reach `domain/`.
- Java records for DTOs and events; Lombok `@Getter/@Setter/@Builder` for JPA entities and
  never `@Data` on an entity.

**Enforcement.** An ArchUnit test in every service asserts: `domain..` has no import of
`org.springframework..`, `jakarta.persistence..`, `org.apache.kafka..`, or generated Avro
packages; and the inward-only dependency rule. A build that violates the hexagon fails, rather
than a reviewer noticing.

## Consequences

**Positive**
- A breaking event-schema change plus all N consumer updates is **one atomic commit**, which
  makes M9's compatibility exercises tractable.
- One CI pipeline, one dependency version set, no version-skew debugging.
- The hexagon makes the interesting logic (idempotency, saga state, EOS boundaries) testable
  without Kafka or Spring — plain JUnit against `domain/` and `application/`.

**Negative / accepted costs**
- CI builds everything on every commit. Mitigation: `mvn -pl <module> -am` locally; in CI,
  path filters once build time hurts.
- A monorepo makes it *easy* to break ADR-0005's "no shared entities" rule by dropping a
  convenient class into `libs/`. ArchUnit plus a stated policy are the only guards.
- More files and more mappers per feature than package-by-layer — the deliberate cost of the
  boundary. And one shared release version, so a UI-only change bumps the backend too.

## Alternatives considered

**Polyrepo, one repo per service.** Correct at team scale with independent release cadences.
Rejected: the coordination cost of shared event schemas dominates at solo scale, and
independent deployability is preserved anyway because deployment is per-container, not
per-repo.

**Gradle multi-project.** Faster builds, better incrementality, nicer Kotlin DSL. Rejected
because the plan's stated stack is Maven and because Maven's rigidity is an asset when the
annotation-processor ordering is this fragile.

**Package-by-feature (vertical slices) without a hexagon.** Excellent for CRUD services and
lighter than ports/adapters. Rejected: the explicit port/adapter split is what makes the
Kafka adapter swappable and the domain framework-free, which is the stated architectural goal.

**Package-by-layer.** Rejected — cannot express the framework-free-domain constraint.
