# Template publish fan-out

This Java 25 project implements the assignment's narrow component: a bounded, resumable worker for
processing a template publication across engagement references. It does not apply template content.
The architecture and tradeoffs are in [design.md](design.md).

## Implementation tradeoffs

- Virtual threads handle blocking downstream calls, while per-invocation concurrency and a shared
  capacity lease bound load. Production must provide a distributed gate; a local semaphore cannot
  enforce a quota across replicas.
- A page cursor advances only after every item succeeds or is durably dead-lettered. Stable
  idempotency keys make page replay safe; the queue adapter must serialize claims per event and
  acknowledge only after the final checkpoint.
- Transient failures use capped exponential backoff with jitter. Capacity is released before
  sleeping. Permanent failures go to the DLQ; capacity exhaustion defers the event without advancing
  its cursor.
- The product-template database, regional index, queue, and downstream integration are existing
  systems or hosting-service responsibilities. The implementation focuses on worker contracts
  rather than building a full application.

## Run and test

Requires JDK 25, Maven, and Docker. Run `mvn verify` to execute unit tests and PostgreSQL 17
Testcontainers simulations for pagination, replay, retries, dead-letter handling, concurrency, and
capacity-pressure deferral.

## AI use

AI assisted with drafting and review. Generated code and design suggestions were checked against
the assignment and validated with tests. AI should not determine whether a template change is
professionally material, make engagement decisions, or invent audit conclusions.
