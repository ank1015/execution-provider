CREATE TABLE users (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    callback_url text NOT NULL,
    webhook_secret_encrypted bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE user_api_keys (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id),
    name text,
    key_hash text NOT NULL UNIQUE,
    key_prefix text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);
CREATE TABLE machines (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id),
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    installation_id uuid,
    credential_hash text UNIQUE,
    credential_version integer NOT NULL DEFAULT 0 CHECK (credential_version >= 0),
    last_runtime_info json,
    last_binary_info json,
    last_seen_at timestamptz,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, id)
);
CREATE TABLE machine_registration_tokens (
    id uuid PRIMARY KEY,
    machine_id uuid NOT NULL REFERENCES machines(id),
    token_hash text NOT NULL UNIQUE,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (expires_at > created_at)
);
CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id),
    machine_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','dispatching','waiting_response','succeeded','failed','unknown')),
    runtime_generation_id uuid,
    response json,
    error json,
    created_at timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key),
    UNIQUE (user_id, id),
    FOREIGN KEY (user_id, machine_id) REFERENCES machines(user_id, id),
    CHECK ((status IN ('succeeded','failed','unknown')) = (finished_at IS NOT NULL))
);
CREATE TABLE job_requests (
    job_id uuid PRIMARY KEY REFERENCES jobs(id),
    request json NOT NULL,
    expires_at timestamptz
);
CREATE TABLE webhook_deliveries (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL UNIQUE,
    user_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('job.succeeded','job.failed','job.unknown')),
    callback_url text NOT NULL,
    payload json NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','delivering','retry_wait','delivered','failed')),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_expires_at timestamptz,
    retry_from_attempt integer NOT NULL DEFAULT 1 CHECK (retry_from_attempt > 0),
    retry_started_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz,
    FOREIGN KEY (user_id, job_id) REFERENCES jobs(user_id, id),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((status = 'delivering') = (lease_token IS NOT NULL))
);
CREATE TABLE webhook_delivery_attempts (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES webhook_deliveries(id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    http_status integer CHECK (http_status BETWEEN 100 AND 599),
    error json,
    UNIQUE (delivery_id, attempt_number)
);
CREATE INDEX users_created ON users(created_at DESC, id DESC);
CREATE INDEX keys_user_created ON user_api_keys(user_id, created_at DESC, id DESC);
CREATE INDEX machines_user_created ON machines(user_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX registration_machine ON machine_registration_tokens(machine_id);
CREATE INDEX jobs_user_created ON jobs(user_id, created_at DESC, id DESC);
CREATE INDEX jobs_machine_created ON jobs(user_id, machine_id, created_at DESC, id DESC);
CREATE INDEX jobs_pending ON jobs(status, created_at, id) WHERE status IN ('queued','dispatching','waiting_response');
CREATE INDEX requests_expiry ON job_requests(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX deliveries_user_created ON webhook_deliveries(user_id, created_at DESC, id DESC);
CREATE INDEX deliveries_runnable ON webhook_deliveries(next_attempt_at, id) WHERE status IN ('pending','retry_wait');
CREATE INDEX deliveries_leases ON webhook_deliveries(lease_expires_at) WHERE status = 'delivering';
