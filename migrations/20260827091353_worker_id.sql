-- Add migration script here
ALTER TABLE jobs
    ADD COLUMN worker_id TEXT;