/**
 * Database Maintenance Utilities
 *
 * Provides helpful database maintenance functions:
 * - Ensuring performance indexes exist
 * - Rebuilding address summaries
 * - Running VACUUM ANALYZE
 *
 * Note: Fast sync mode has been removed for simplicity and reliability.
 * For fast deployment, use database bootstraps instead of syncing from scratch.
 */

import { DatabaseConnection } from './connection';
import { logger } from '../utils/logger';

export class DatabaseOptimizer {
  constructor(private db: DatabaseConnection) {}

  /**
   * Ensure critical performance indexes exist
   * Call this on startup to verify all required indexes are present
   */
  async ensurePerformanceIndexes(): Promise<void> {
    logger.info('Ensuring critical performance indexes exist...');

    const performanceIndexes = [
      {
        name: 'idx_blocks_hash',
        table: 'blocks',
        definition: 'CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash)',
      },
      {
        name: 'idx_blocks_timestamp',
        table: 'blocks',
        definition: 'CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp DESC)',
      },
      {
        name: 'idx_tx_txid',
        table: 'transactions',
        definition: 'CREATE INDEX IF NOT EXISTS idx_tx_txid ON transactions(txid)',
      },
      {
        name: 'idx_tx_timestamp',
        table: 'transactions',
        definition: 'CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions(timestamp DESC)',
      },
      // PHASE 3: address_transactions now uses address_id instead of address
      {
        name: 'idx_address_tx_address_id',
        table: 'address_transactions',
        definition: 'CREATE INDEX IF NOT EXISTS idx_address_tx_address_id ON address_transactions(address_id, block_height DESC, txid DESC)',
      },
      // PHASE 3: Use utxos_unspent table instead of utxos VIEW
      {
        name: 'idx_utxo_unspent_address_id',
        table: 'utxos_unspent',
        definition: 'CREATE INDEX IF NOT EXISTS idx_utxo_unspent_address_id ON utxos_unspent(address_id)',
      },
      // PHASE 3: Use utxos_spent table for spent_txid index
      {
        name: 'idx_utxo_spent_spent_txid',
        table: 'utxos_spent',
        definition: 'CREATE INDEX IF NOT EXISTS idx_utxo_spent_spent_txid ON utxos_spent(spent_txid)',
      },
      {
        name: 'idx_address_summary_balance',
        table: 'address_summary',
        definition: 'CREATE INDEX IF NOT EXISTS idx_address_summary_balance ON address_summary(balance DESC)',
      },
      {
        name: 'idx_tx_lookup_block_height',
        table: 'tx_lookup',
        definition: 'CREATE INDEX IF NOT EXISTS idx_tx_lookup_block_height ON tx_lookup(block_height)',
      },
    ];

    for (const index of performanceIndexes) {
      try {
        // Check if index already exists
        const checkResult = await this.db.query(`
          SELECT 1 FROM pg_indexes
          WHERE schemaname = 'public'
            AND tablename = $1
            AND indexname = $2
        `, [index.table, index.name]);

        if (checkResult.rows.length === 0) {
          logger.info(`Creating index: ${index.name}...`);
          await this.db.query(index.definition);
          logger.info(`Created index: ${index.name}`);
        }
      } catch (error) {
        logger.warn(`Failed to create index ${index.name}`, { error });
      }
    }

    logger.info('Performance indexes verified');
  }

  /**
   * Rebuild address_summary table from UTXO data
   * Useful for recovery or data consistency verification
   * PHASE 3: Uses utxos VIEW (which JOINs addresses) and address_transactions with addresses JOIN
   */
  async rebuildAddressSummary(): Promise<void> {
    logger.info('Rebuilding address_summary table from UTXO data...');

    await this.db.query('TRUNCATE address_summary');

    // PHASE 3: address_transactions uses address_id, need to JOIN addresses table
    await this.db.query(`
      INSERT INTO address_summary (
        address,
        balance,
        tx_count,
        received_total,
        sent_total,
        unspent_count,
        first_seen,
        last_activity,
        updated_at
      )
      SELECT
        u.address,
        SUM(CASE WHEN u.spent = false THEN u.value ELSE 0 END) AS balance,
        COALESCE(at.tx_count, 0) AS tx_count,
        SUM(u.value) AS received_total,
        SUM(CASE WHEN u.spent THEN u.value ELSE 0 END) AS sent_total,
        SUM(CASE WHEN u.spent = false THEN 1 ELSE 0 END) AS unspent_count,
        MIN(u.block_height) AS first_seen,
        MAX(GREATEST(u.block_height, COALESCE(u.spent_block_height, 0))) AS last_activity,
        NOW() AS updated_at
      FROM utxos u
      LEFT JOIN (
        SELECT addr.address, COUNT(DISTINCT atx.txid) AS tx_count
        FROM address_transactions atx
        JOIN addresses addr ON atx.address_id = addr.id
        GROUP BY addr.address
      ) at ON at.address = u.address
      GROUP BY u.address, at.tx_count
    `);

    logger.info('address_summary rebuild complete');
  }

  /**
   * Run VACUUM ANALYZE on main tables
   * Updates statistics for query optimizer and reclaims space
   */
  async vacuumAnalyze(): Promise<void> {
    const tables = ['blocks', 'transactions', 'utxos', 'address_summary', 'address_transactions', 'tx_lookup'];

    logger.info('Running VACUUM ANALYZE...');

    for (const table of tables) {
      try {
        logger.debug(`VACUUM ANALYZE ${table}`);
        await this.db.query(`VACUUM ANALYZE ${table}`);
      } catch (error) {
        logger.warn(`Failed to VACUUM ANALYZE ${table}`, { error });
      }
    }

    logger.info('VACUUM ANALYZE complete');
  }

  /**
   * Get database statistics
   */
  async getStats(): Promise<{
    tableCount: number;
    totalSize: string;
    indexCount: number;
  }> {
    const tableResult = await this.db.query(`
      SELECT COUNT(*) as count FROM pg_tables WHERE schemaname = 'public'
    `);

    const sizeResult = await this.db.query(`
      SELECT pg_size_pretty(pg_database_size(current_database())) as size
    `);

    const indexResult = await this.db.query(`
      SELECT COUNT(*) as count FROM pg_indexes WHERE schemaname = 'public'
    `);

    return {
      tableCount: parseInt(tableResult.rows[0]?.count || '0'),
      totalSize: sizeResult.rows[0]?.size || 'unknown',
      indexCount: parseInt(indexResult.rows[0]?.count || '0'),
    };
  }

  /**
   * Alias for ensurePerformanceIndexes for backward compatibility
   */
  async ensureIndexes(): Promise<void> {
    await this.ensurePerformanceIndexes();
  }
}
