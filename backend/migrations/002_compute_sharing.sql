-- Compute sharing: identical frozen points + frequency plan share one computation.
-- spectrum_computations(fingerprint,status,result JSON,peak_frequency,max_power,global_p)
-- spectrum_runs.computation_id links each run to its shared computation; runs keep their own snapshots.
-- Partial unique index keeps at most one live (queued/running/completed) computation per fingerprint;
-- cancelled/failed computations leave the index so identical inputs can be recomputed later.
CREATE TABLE IF NOT EXISTS spectrum_computations (
 id SERIAL PRIMARY KEY,
 fingerprint VARCHAR(64) NOT NULL,
 status VARCHAR(24) NOT NULL DEFAULT 'queued',
 result JSON NULL,
 error TEXT NULL,
 peak_frequency DOUBLE PRECISION NULL,
 max_power DOUBLE PRECISION NULL,
 global_p DOUBLE PRECISION NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_computations_live ON spectrum_computations (fingerprint) WHERE status IN ('queued','running','completed');
ALTER TABLE spectrum_runs ADD COLUMN IF NOT EXISTS computation_id INTEGER REFERENCES spectrum_computations(id);
