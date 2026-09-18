ALTER TABLE jobs
    ADD COLUMN client_context json,
    ADD CONSTRAINT jobs_client_context_object
        CHECK (client_context IS NULL OR json_typeof(client_context) = 'object');
