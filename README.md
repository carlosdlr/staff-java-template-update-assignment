# Template publish fan-out worker

This Java 25 component processes a publish against a paged set of engagement references. Records make the event, engagement, page, and limits contracts immutable; compact constructors validate inputs. Virtual threads handle the blocking downstream call without tying up platform threads, while a per-invocation in-flight bound and a **shared** capacity lease provide backpressure. A stable idempotency key is used for each publish/engagement pair, and the page cursor advances only after all items on that page complete so a crash resumes safely. Transient downstream failures are retried with capped exponential backoff and full jitter; explicitly non-retryable failures go directly to the dead-letter interface. Capacity exhaustion defers the publish for queue retry and is never dead-lettered as an engagement failure. A page is checkpointed only after all its items succeed or are durably dead-lettered.

The queue/checkpoint adapter must serialize claims per publish event, extend message visibility while the slow call is active, and acknowledge the event only after the final checkpoint. On `CapacityUnavailableException`, it should release/requeue with delay; the page cursor is not advanced, so replay uses the same idempotency keys. Run a small number of worker instances with a distributed `CapacityGate` backed by the downstream team’s agreed quota (for example, a DynamoDB lease/semaphore); an in-process semaphore alone is not safe across replicas. The downstream call is synchronous and may take a minute, so each acquired permit is held for the whole call. Set page size and per-invocation concurrency from the agreed quota, not from available CPU. Queue delivery is at-least-once; the downstream operation must honor the supplied idempotency key, or the adapter must persist a request ledger before acknowledging completion.

`downstream.refreshPending` means “make this engagement aware of the published target / enqueue its region-local status refresh”; it does not apply template content. Its call can take about a minute. The pending indicator must therefore use the fast index/head path described in `design.md`, not wait for this worker to drain. A durable cursor store and retry/dead-letter adapters are infrastructure responsibilities represented as interfaces here.

## AI use

AI helped draft the first design and worker. Review against the brief identified the need to target Java 25, account for the one-minute downstream capacity in the throughput math, and add jittered retries. The version graph, withdrawal policy, regional data handling, and audit evidence still need review by product and domain owners. AI should not decide whether a template change is professionally material, whether it is safe to apply, or what an auditor’s conclusion should be.

## Run

Requires JDK 25 and Maven. `mvn package` compiles the component. The core has no runtime dependencies. Validation note: source compilation succeeded with JDK 23, but the configured Java 25 release target still needs verification on JDK 25.

## Database and simulations

PostgreSQL is the selected regional database for the engagement-reference projection, publish checkpoints, and idempotency ledger. Its relational constraints and transactions fit the version/market lookups and safe replay path; keyset pagination uses `(region, product_id, market, engagement_id)`. Keep engagement data and database instances within each firm’s residency region. Store immutable summary evidence in regional object storage rather than growing the hot index tables. Start with managed RDS for PostgreSQL; consider Aurora PostgreSQL only after measured availability or throughput needs justify it.

Run the database-backed worker simulations with Docker available to Testcontainers:

```sh
mvn verify
```

`PostgresFanoutSimulationTest` launches an isolated PostgreSQL 17 container and checks paged scans, idempotent redelivery, transient retries, permanent-failure DLQ handling, concurrency bounds, and capacity-pressure deferral. These are behavior simulations, not production load benchmarks; the hosting service still supplies the durable queue and globally coordinated permit implementation.
