CREATE TABLE engagement_reference (
    region text NOT NULL, product_id text NOT NULL, market text NOT NULL,
    firm_id text NOT NULL, engagement_id text NOT NULL, applied_version text NOT NULL,
    PRIMARY KEY (region, product_id, market, engagement_id)
);
CREATE INDEX engagement_reference_scan ON engagement_reference
    (region, product_id, market, engagement_id) INCLUDE (firm_id, applied_version);
CREATE TABLE publish_checkpoint (event_id text PRIMARY KEY, cursor text, completed boolean NOT NULL DEFAULT false);
CREATE TABLE refresh_ledger (idempotency_key text PRIMARY KEY, event_id text NOT NULL, engagement_id text NOT NULL);
CREATE TABLE dead_letter (idempotency_key text PRIMARY KEY, event_id text NOT NULL, engagement_id text NOT NULL, reason text NOT NULL);
