CREATE SCHEMA IF NOT EXISTS msgflux;

CREATE TABLE IF NOT EXISTS msgflux.messages (
    id            BIGSERIAL    PRIMARY KEY,
    message_id    UUID         NOT NULL,
    consumer_id   TEXT         NOT NULL,
    payload       BYTEA        NOT NULL,
    headers       JSONB        NOT NULL DEFAULT '{}',
    message_type  TEXT         NOT NULL,
    state         SMALLINT     NOT NULL DEFAULT 0,
    retry_count   INT          NOT NULL DEFAULT 0,
    error_details TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ,
    UNIQUE (message_id, consumer_id)
);

CREATE INDEX IF NOT EXISTS ix_msgflux_unprocessed
    ON msgflux.messages (consumer_id, state, message_type, created_at) WHERE state IN (0, 1, 3);
CREATE INDEX IF NOT EXISTS ix_msgflux_purge
    ON msgflux.messages (created_at) WHERE state = 2;
