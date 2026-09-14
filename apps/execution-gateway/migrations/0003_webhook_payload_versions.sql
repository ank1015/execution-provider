ALTER TABLE users
    ADD COLUMN webhook_payload_version integer NOT NULL DEFAULT 1
    CHECK (webhook_payload_version IN (1, 2));
