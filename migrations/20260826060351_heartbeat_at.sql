-- Add migration script here
ALTER TABLE jobs
    ADD COLUMN heartbeat_at TIMESTAMPTZ;