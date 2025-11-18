-- Migration 015: Add FluxNode count columns to address_summary
-- These columns track the number of active FluxNodes (Cumulus, Nimbus, Stratus)
-- that each address is currently operating, fetched from the Flux daemon's FluxNode list.
--
-- NOTE: This migration only creates the database structure. The actual node counts
-- are populated by the FluxNode sync service (TypeScript) which periodically queries
-- the Flux daemon via RPC and updates these columns.

BEGIN;

-- Add columns for FluxNode counts
ALTER TABLE address_summary
  ADD COLUMN IF NOT EXISTS cumulus_count SMALLINT DEFAULT 0 NOT NULL,
  ADD COLUMN IF NOT EXISTS nimbus_count SMALLINT DEFAULT 0 NOT NULL,
  ADD COLUMN IF NOT EXISTS stratus_count SMALLINT DEFAULT 0 NOT NULL;

-- Add column to track last sync time for FluxNode data
ALTER TABLE address_summary
  ADD COLUMN IF NOT EXISTS fluxnode_last_sync TIMESTAMP;

-- Add comments
COMMENT ON COLUMN address_summary.cumulus_count IS 'Number of active Cumulus nodes (1,000 FLUX collateral) operated by this address';
COMMENT ON COLUMN address_summary.nimbus_count IS 'Number of active Nimbus nodes (12,500 FLUX collateral) operated by this address';
COMMENT ON COLUMN address_summary.stratus_count IS 'Number of active Stratus nodes (40,000 FLUX collateral) operated by this address';
COMMENT ON COLUMN address_summary.fluxnode_last_sync IS 'Timestamp of last FluxNode count synchronization from daemon';

-- Create indexes for efficient querying by node counts
CREATE INDEX IF NOT EXISTS idx_address_summary_cumulus_count
  ON address_summary (cumulus_count) WHERE cumulus_count > 0;

CREATE INDEX IF NOT EXISTS idx_address_summary_nimbus_count
  ON address_summary (nimbus_count) WHERE nimbus_count > 0;

CREATE INDEX IF NOT EXISTS idx_address_summary_stratus_count
  ON address_summary (stratus_count) WHERE stratus_count > 0;

-- Create a composite index for total node operators
CREATE INDEX IF NOT EXISTS idx_address_summary_total_nodes
  ON address_summary ((cumulus_count + nimbus_count + stratus_count))
  WHERE (cumulus_count + nimbus_count + stratus_count) > 0;

COMMIT;
