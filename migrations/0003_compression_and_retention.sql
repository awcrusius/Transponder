-- Compression and retention for the realtime hypertables.
--
-- Why: agencies republish their whole trip-update snapshot on every poll, so
-- stop_time_updates grows by tens of millions of rows a day per mid-size
-- agency and its per-chunk indexes are larger than the data. Compression
-- drops those indexes and packs the columns; measured on a full day of real
-- data these settings give ~15x on stop_time_updates, ~13x on trip_updates
-- and ~9x on vehicle_positions. Retention then bounds the total.
--
-- The unique indexes stay as they are. TimescaleDB only warns that their
-- columns are not all segmentby/orderby columns, ON CONFLICT inserts keep
-- working (inserts only ever target the current, uncompressed chunk), and
-- every column is needed for identity: some feeds number entities by
-- position, so coalesce(trip_id, entity_id) must stay, and a feed may supply
-- only stop_sequence or only stop_id. Reordering or trimming the index changed
-- its size by under 3 %, not worth an index rebuild on a large table.
--
-- Safe to apply on a populated database: everything here is metadata, no data
-- is rewritten, and the policies do the work in the background afterwards.

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

-- Compress chunks whose time range closed more than a day ago. The job runs
-- hourly (the default would be every 12 h) so an eligible chunk does not sit
-- uncompressed for long. Compressed chunks remain fully queryable.
SELECT add_compression_policy('stop_time_updates', compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');
SELECT add_compression_policy('trip_updates',      compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');
SELECT add_compression_policy('vehicle_positions', compress_after => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');

-- Retention. Windows are chosen so a deployment ingesting a couple of mid-size
-- agencies settles around 50 GB: stop_time_updates is the shortest because it
-- is ~90 % of the volume and its per-stop predictions are largely derivable
-- from trip_updates + the static schedule; vehicle_positions is kept longest
-- because it is the smallest and the hardest to reconstruct. Change a window
-- later with remove_retention_policy() + add_retention_policy(); see README.
SELECT add_retention_policy('stop_time_updates', drop_after => INTERVAL '14 days');
SELECT add_retention_policy('trip_updates',      drop_after => INTERVAL '90 days');
SELECT add_retention_policy('vehicle_positions', drop_after => INTERVAL '180 days');
