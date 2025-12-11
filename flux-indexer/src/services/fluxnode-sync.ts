/**
 * FluxNode Synchronization Service
 *
 * Periodically queries the Flux daemon's FluxNode list and updates address_summary
 * with accurate FluxNode counts per address (Cumulus, Nimbus, Stratus).
 */

import { Pool } from 'pg';
import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { logger } from '../utils/logger';

export interface FluxNodeCounts {
  address: string;
  cumulusCount: number;
  nimbusCount: number;
  stratusCount: number;
}

export class FluxNodeSyncService {
  private db: Pool;
  private rpc: FluxRPCClient;
  private syncInterval: NodeJS.Timeout | null = null;
  private isSyncing = false;

  // Sync every 10 minutes (FluxNode list changes slowly)
  private readonly SYNC_INTERVAL_MS = 10 * 60 * 1000;

  constructor(db: Pool, rpc: FluxRPCClient) {
    this.db = db;
    this.rpc = rpc;
  }

  /**
   * Start periodic FluxNode synchronization
   */
  start(): void {
    if (this.syncInterval) {
      logger.warn('FluxNode sync service already running');
      return;
    }

    logger.info('Starting FluxNode sync service');

    // Run immediately on start (use debug level since daemon may not be ready)
    this.syncFluxNodes().catch((error) => {
      logger.debug('Initial FluxNode sync failed (daemon may not be ready)', { error: error.message });
    });

    // Then run periodically
    this.syncInterval = setInterval(
      () => {
        this.syncFluxNodes().catch((error) => {
          logger.error('Periodic FluxNode sync failed', { error: error.message });
        });
      },
      this.SYNC_INTERVAL_MS
    );

    logger.info(`FluxNode sync service started (interval: ${this.SYNC_INTERVAL_MS / 1000}s)`);
  }

  /**
   * Stop periodic synchronization
   */
  stop(): void {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
      logger.info('FluxNode sync service stopped');
    }
  }

  /**
   * Main synchronization logic
   */
  async syncFluxNodes(): Promise<void> {
    if (this.isSyncing) {
      logger.debug('FluxNode sync already in progress, skipping');
      return;
    }

    this.isSyncing = true;
    const startTime = Date.now();

    try {
      logger.info('Starting FluxNode synchronization');

      // Step 1: Fetch FluxNode list from daemon
      const nodeList = await this.rpc.getFluxNodeList();

      if (!Array.isArray(nodeList)) {
        throw new Error('FluxNode list is not an array');
      }

      logger.info(`Fetched ${nodeList.length} FluxNodes from daemon`);

      // If no nodes were fetched, skip database update to preserve existing data
      // This can happen if the daemon is not ready yet or listfluxnodes returns empty
      if (nodeList.length === 0) {
        logger.warn('No FluxNodes fetched from daemon, skipping database update to preserve existing data');
        return;
      }

      // Step 2: Parse and count nodes per address
      const nodeCounts = this.countNodesPerAddress(nodeList);

      logger.info(`Counted nodes for ${nodeCounts.size} unique addresses`);

      // Step 3: Update database
      await this.updateDatabase(nodeCounts);

      const duration = Date.now() - startTime;
      logger.info(`FluxNode synchronization completed in ${duration}ms`);
    } catch (error: any) {
      logger.error('FluxNode synchronization failed', {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    } finally {
      this.isSyncing = false;
    }
  }

  /**
   * Count FluxNodes per address by tier
   *
   * Parses the response from `flux-cli listfluxnodes` which returns an array of objects with:
   * - payment_address: The address receiving block rewards
   * - tier: "CUMULUS", "NIMBUS", or "STRATUS"
   */
  private countNodesPerAddress(nodeList: any[]): Map<string, FluxNodeCounts> {
    const counts = new Map<string, FluxNodeCounts>();

    for (const node of nodeList) {
      // Extract the payment address (reward recipient)
      const address = node.payment_address;

      if (!address || typeof address !== 'string') {
        logger.debug('FluxNode entry missing payment_address field', { node });
        continue;
      }

      // Extract the tier
      const tier = (node.tier || '').toUpperCase();

      if (!tier) {
        logger.debug('FluxNode entry missing tier field', { node, address });
        continue;
      }

      // Initialize counts for this address if not exists
      if (!counts.has(address)) {
        counts.set(address, {
          address,
          cumulusCount: 0,
          nimbusCount: 0,
          stratusCount: 0,
        });
      }

      const addressCounts = counts.get(address)!;

      // Increment the appropriate tier count
      if (tier === 'CUMULUS') {
        addressCounts.cumulusCount++;
      } else if (tier === 'NIMBUS') {
        addressCounts.nimbusCount++;
      } else if (tier === 'STRATUS') {
        addressCounts.stratusCount++;
      } else {
        logger.debug('Unknown FluxNode tier', { tier, address });
      }
    }

    return counts;
  }

  /**
   * Update address_summary with new FluxNode counts
   */
  private async updateDatabase(nodeCounts: Map<string, FluxNodeCounts>): Promise<void> {
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      // Step 1: Reset all counts to 0 first (nodes that are no longer active)
      await client.query(`
        UPDATE address_summary
        SET
          cumulus_count = 0,
          nimbus_count = 0,
          stratus_count = 0,
          fluxnode_last_sync = NOW()
        WHERE cumulus_count > 0 OR nimbus_count > 0 OR stratus_count > 0
      `);

      logger.info('Reset existing FluxNode counts to 0');

      // Step 2: Update counts for active nodes
      if (nodeCounts.size > 0) {
        // Prepare batch update using CASE statements for efficiency
        const addresses = Array.from(nodeCounts.keys());
        const addressPlaceholders = addresses.map((_, i) => `$${i + 1}`).join(', ');

        // Build CASE statements for each tier
        const cumulusCases = addresses
          .map((addr, i) => {
            const count = nodeCounts.get(addr)!.cumulusCount;
            return `WHEN address = $${i + 1} THEN ${count}`;
          })
          .join(' ');

        const nimbusCases = addresses
          .map((addr, i) => {
            const count = nodeCounts.get(addr)!.nimbusCount;
            return `WHEN address = $${i + 1} THEN ${count}`;
          })
          .join(' ');

        const stratusCases = addresses
          .map((addr, i) => {
            const count = nodeCounts.get(addr)!.stratusCount;
            return `WHEN address = $${i + 1} THEN ${count}`;
          })
          .join(' ');

        const updateQuery = `
          UPDATE address_summary
          SET
            cumulus_count = CASE ${cumulusCases} ELSE 0 END,
            nimbus_count = CASE ${nimbusCases} ELSE 0 END,
            stratus_count = CASE ${stratusCases} ELSE 0 END,
            fluxnode_last_sync = NOW()
          WHERE address IN (${addressPlaceholders})
        `;

        await client.query(updateQuery, addresses);

        logger.info(`Updated FluxNode counts for ${addresses.length} addresses`);
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Manually trigger a sync (useful for testing/debugging)
   */
  async triggerSync(): Promise<void> {
    await this.syncFluxNodes();
  }
}
