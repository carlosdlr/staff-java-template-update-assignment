# Template publish fan-out worker

This Java 25 component processes a publish against a paged set of engagement references. Records make the event, engagement, page, and limits contracts immutable; compact constructors validate inputs. Virtual threads handle the blocking downstream call without tying up platform threads, while a strict in-flight bound and a **shared** capacity lease provide backpressure. A stable idempotency key is used for each publish/engagement pair, and the page cursor is checkpointed after every item on that page completes so a crash resumes safely. Failures are isolated per engagement; transient failures are retried with bounded exponential backoff and exhausted failures are sent to a dead-letter interface.

Serialize processing for a given publish event, and run a small number of worker instances with a distributed `CapacityGate` backed by the downstream team’s agreed quota (for example, a DynamoDB lease/semaphore); an in-process semaphore alone is not safe across replicas. The downstream call is synchronous and may take a minute, so each acquired permit is held for the whole call. Set page size and concurrency from the agreed quota, not from available CPU. Queue delivery is at-least-once; the downstream operation must honor the supplied idempotency key, or the adapter must persist a request ledger before acknowledging completion.

`downstream.refreshPending` means “make this engagement aware of the published target / enqueue its region-local status refresh”; it does not load an engagement or apply template content. That boundary keeps the expensive engagement session path out of publish processing. A durable cursor store and retry/dead-letter adapters are infrastructure responsibilities represented as interfaces here.

## Run

Requires JDK 25 and Maven. `mvn package` compiles the component. The core has no runtime dependencies. Validation note: source compilation succeeded with JDK 23, but the configured Java 25 release target still needs verification on JDK 25.
