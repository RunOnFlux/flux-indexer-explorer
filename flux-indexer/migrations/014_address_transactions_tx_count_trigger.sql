-- Migration 014: Create trigger to maintain tx_count in address_summary
-- This trigger ensures that when new rows are added to address_transactions,
-- the tx_count field in address_summary is automatically updated
--
-- IMPORTANT: Uses statement-level trigger to handle bulk inserts and ON CONFLICT updates

-- Create function to recalculate tx_count for affected addresses
CREATE OR REPLACE FUNCTION update_address_summary_tx_count()
RETURNS TRIGGER AS $$
BEGIN
  -- For INSERT: recalculate tx_count for all affected addresses
  -- This handles both new inserts AND ON CONFLICT DO UPDATE cases
  IF (TG_OP = 'INSERT') THEN
    -- Batch update all addresses that had rows inserted
    UPDATE address_summary a_sum
    SET tx_count = (
      SELECT COUNT(*)
      FROM address_transactions at
      WHERE at.address = a_sum.address
    )
    WHERE a_sum.address IN (SELECT DISTINCT address FROM new_table);

    RETURN NULL;
  END IF;

  -- For DELETE: recalculate tx_count for all affected addresses
  IF (TG_OP = 'DELETE') THEN
    UPDATE address_summary a_sum
    SET tx_count = (
      SELECT COUNT(*)
      FROM address_transactions at
      WHERE at.address = a_sum.address
    )
    WHERE a_sum.address IN (SELECT DISTINCT address FROM old_table);

    RETURN NULL;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create statement-level triggers on address_transactions
-- Need separate triggers for INSERT and DELETE because transition tables don't work with multiple events

DROP TRIGGER IF EXISTS update_tx_count_on_insert ON address_transactions;
CREATE TRIGGER update_tx_count_on_insert
  AFTER INSERT ON address_transactions
  REFERENCING NEW TABLE AS new_table
  FOR EACH STATEMENT
  EXECUTE FUNCTION update_address_summary_tx_count();

DROP TRIGGER IF EXISTS update_tx_count_on_delete ON address_transactions;
CREATE TRIGGER update_tx_count_on_delete
  AFTER DELETE ON address_transactions
  REFERENCING OLD TABLE AS old_table
  FOR EACH STATEMENT
  EXECUTE FUNCTION update_address_summary_tx_count();

-- Drop old trigger if it exists
DROP TRIGGER IF EXISTS update_tx_count_on_address_transactions ON address_transactions;

COMMENT ON FUNCTION update_address_summary_tx_count() IS
  'Maintains tx_count in address_summary when address_transactions rows are added/removed - works with ON CONFLICT updates';
COMMENT ON TRIGGER update_tx_count_on_insert ON address_transactions IS
  'Statement-level trigger that recalculates tx_count for addresses with new transactions';
COMMENT ON TRIGGER update_tx_count_on_delete ON address_transactions IS
  'Statement-level trigger that recalculates tx_count for addresses with deleted transactions';
