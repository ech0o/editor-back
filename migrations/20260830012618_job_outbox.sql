-- Add migration script here
CREATE TABLE job_outbox (
                            id UUID PRIMARY KEY,
                            job_id UUID NOT NULL REFERENCES jobs(id),
                            event_type TEXT NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            published_at TIMESTAMPTZ
);

CREATE INDEX idx_job_outbox_unpublished
    ON job_outbox (created_at)
    WHERE published_at IS NULL;