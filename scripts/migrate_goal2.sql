-- Goal 2: Partition spec evolution migration
-- Run once against any existing database that was created before Goal 2.
-- Safe to run inside a transaction.

BEGIN;

-- 1. Add version column to tables
ALTER TABLE tables ADD COLUMN IF NOT EXISTS partition_spec_version INT NOT NULL DEFAULT 1;

-- 2. Convert partition_spec TEXT -> JSONB, wrapping plain strings into a single identity field
ALTER TABLE tables ALTER COLUMN partition_spec TYPE JSONB
    USING CASE
        WHEN partition_spec IS NULL OR partition_spec = ''
            THEN NULL
        ELSE jsonb_build_object(
                'fields', jsonb_build_array(
                    jsonb_build_object(
                        'field_id',        1,
                        'source_column',   partition_spec,
                        'transform',       'identity',
                        'transform_param', NULL
                    )
                )
             )
    END;

-- 3. Create partition_specs history table if not exists
CREATE TABLE IF NOT EXISTS partition_specs (
    partition_spec_id BIGSERIAL PRIMARY KEY,
    table_id          BIGINT NOT NULL REFERENCES tables(table_id),
    spec_version      INT NOT NULL,
    spec_json         JSONB NOT NULL,
    changed_at        TIMESTAMPTZ DEFAULT now(),
    change_summary    TEXT,
    UNIQUE (table_id, spec_version)
);

CREATE INDEX IF NOT EXISTS idx_partition_specs_table
    ON partition_specs(table_id, spec_version DESC);

-- 4. Backfill spec history for existing tables
INSERT INTO partition_specs (table_id, spec_version, spec_json, change_summary)
SELECT table_id, 1, partition_spec, 'migrated from initial text spec'
  FROM tables
 WHERE partition_spec IS NOT NULL
   AND is_deleted = false
ON CONFLICT (table_id, spec_version) DO NOTHING;

COMMIT;
