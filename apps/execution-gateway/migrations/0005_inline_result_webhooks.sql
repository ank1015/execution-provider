ALTER TABLE users DROP CONSTRAINT users_webhook_payload_version_check;
ALTER TABLE users ADD CONSTRAINT users_webhook_payload_version_check
    CHECK (webhook_payload_version IN (1, 2, 3));
