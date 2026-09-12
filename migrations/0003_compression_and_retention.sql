-- Compression and retention for the realtime hypertables.
--
-- Measured on the Pi after 23 h of ingest: stop_time_updates alone grew by
-- ~123 M rows / 46 GB per day, 26 GB of which was index. Every poll re-writes
-- the full trip-update snapshot with a new header timestamp, so the unique
-- index never dedupes anything for these feeds; compression is what makes the
-- history affordable. On a 1 h real sample the settings below compressed
-- stop_time_updates ~8x on the heap and drop the per-chunk indexes entirely
-- (compressed chunks only keep segment/order metadata), so a day of
-- stop_time_updates goes from ~46 GB to ~2.5 GB.
--
-- The unique indexes stay as they are: TimescaleDB only warns that their
-- columns are not all segmentby/orderby columns, ON CONFLICT inserts keep
-- working, and every column in stop_time_updates_uniq is needed for identity
-- (BC Transit uses positional entity_ids, so coalesce(trip_id, entity_id)
-- must stay; a feed may supply only stop_sequence or only stop_id). Dropping
-- stop_id or leading with time changed the index size by under 3 %.

-- Smaller chunks for the biggest table: a chunk is compressed once its whole
-- range is older than compress_after, so 6 h chunks cap the uncompressed
-- window at ~30 h instead of ~48 h, and each compression job sorts ~30 M rows
-- instead of ~125 M. Applies to chunks created from now on.
SELECT set_chunk_time_interval('stop_time_updates', INTERVAL '6 hours');

ALTER TABLE stop_time_updates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'feed_id',
    timescaledb.compress_orderby   = 'time DESC'
);
ALTER TABLE trip_updates SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'feed_id',
    timescaledb.compress_orderby   = 'time DESC'
);
ALTER TABLE vehicle_positions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'feed_id',
    timescaledb.compress_orderby   = 'time DESC'
);

-- Compress chunks whose range ended more than a day ago. The job runs hourly
-- (default would be every 12 h) so a chunk is not left uncompressed for long
-- after it becomes eligible.
SELECT add_compression_policy('stop_time_updates', compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');
SELECT add_compression_policy('trip_updates',      compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');
SELECT add_compression_policy('vehicle_positions', compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');

-- Retention. Budget is a 235 GB NVMe shared with the OS; steady state with
-- these windows is roughly: uncompressed window ~65 GB, stop_time_updates
-- 14 d x ~2.5 GB = ~35 GB, trip_updates 90 d x ~0.1 GB = ~10 GB,
-- vehicle_positions 180 d x ~0.1 GB = ~13 GB, so ~125 GB plus static versions.
-- stop_time_updates is the shortest because it is 90 % of the volume and its
-- per-stop predictions are mostly derivable from trip_updates + static
-- schedule; vehicle_positions is kept longest because it is the smallest and
-- the hardest to reconstruct.
SELECT add_retention_policy('stop_time_updates', drop_after => INTERVAL '14 days');
SELECT add_retention_policy('trip_updates',      drop_after => INTERVAL '90 days');
SELECT add_retention_policy('vehicle_positions', drop_after => INTERVAL '180 days');
