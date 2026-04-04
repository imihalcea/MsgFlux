CREATE TABLE IF NOT EXISTS msgflux_messages (
    message_id    TEXT         PRIMARY KEY,
    payload       BYTEA        NOT NULL,
    headers       JSONB        NOT NULL DEFAULT '{}',
    message_type  TEXT         NOT NULL,
    state         SMALLINT     NOT NULL DEFAULT 0,
    retry_count   INT          NOT NULL DEFAULT 0,
    error_details TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_msgflux_unprocessed
    ON msgflux_messages (state, message_type) WHERE state IN (0, 2, 3);
CREATE INDEX IF NOT EXISTS ix_msgflux_purge
    ON msgflux_messages (created_at) WHERE state = 2;
