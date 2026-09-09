-- Record which API key served each realtime fetch and the classified error kind,
-- so key rotation and failure modes can be inspected without reading logs.

ALTER TABLE rt_fetches
    ADD COLUMN error_kind text,
    ADD COLUMN api_key text;
