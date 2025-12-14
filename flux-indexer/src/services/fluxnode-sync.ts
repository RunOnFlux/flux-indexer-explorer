/**
 * ClickHouse FluxNode Synchronization Service
 *
 * Periodically queries the Flux daemon's FluxNode list and updates live_fluxnodes
 * table with accurate FluxNode counts per address (Cumulus, Nimbus, Stratus).
 *
 * Uses a separate table (live_fluxnodes) instead of address_summary because
 * SummingMergeTree deletes rows where all summing columns are zero.
 */

import { ClickHouseConnection } from '../database/connection';
import { FluxRPCClient } from '../rpc/flux-rpc-client';
import { logger } from '../utils/logger';

// Collateral amounts in satoshis
const CUMULUS_COLLATERAL = 1000_00000000;   // 1,000 FLUX
const NIMBUS_COLLATERAL = 12500_00000000;   // 12,500 FLUX
const STRATUS_COLLATERAL = 40000_00000000;  // 40,000 FLUX

export interface FluxNodeCounts {
  address: string;
  cumulusCount: number;
  nimbusCount: number;
  stratusCount: number;
  totalCollateral: bigint;
}

export class ClickHouseFluxNodeSyncService {
  private ch: ClickHouseConnection;
  private rpc: FluxRPCClient;
  private syncInterval: NodeJS.Timeout | null = null;
  private daemonCheckInterval: NodeJS.Timeout | null = null;
  private isSyncing = false;
  private daemonReady = false;

  // Sync every 10 minutes (FluxNode list changes slowly)
  private readonly SYNC_INTERVAL_MS = 10 * 60 * 1000;
  // Check if daemon is ready every 30 seconds
  private readonly DAEMON_CHECK_INTERVAL_MS = 30 * 1000;

  constructor(ch: ClickHouseConnection, rpc: FluxRPCClient) {
    this.ch = ch;
    this.rpc = rpc;
  }

  /**
   * Start periodic FluxNode synchronization
   * Waits for daemon to be ready before starting the sync schedule
   */
  start(): void {
    if (this.syncInterval || this.daemonCheckInterval) {
      logger.warn('FluxNode sync service already running');
      return;
    }

    logger.info('Starting ClickHouse FluxNode sync service');

    // Start checking if daemon is ready
    this.checkDaemonAndStartSync();
  }

  /**
   * Check if daemon is ready, then start sync schedule
   */
  private async checkDaemonAndStartSync(): Promise<void> {
    // Try to sync immediately
    try {
      const nodeList = await this.rpc.getFluxNodeList();
      if (Array.isArray(nodeList) && nodeList.length > 0) {
        this.daemonReady = true;
        logger.info('Daemon is ready, starting FluxNode sync');

        // Do initial sync
        await this.syncFluxNodes();

        // Start periodic sync
        this.startPeriodicSync();
        return;
      }
    } catch (error: any) {
      logger.debug('Daemon not ready yet', { error: error.message });
    }

    // Daemon not ready, check again later
    logger.info('Waiting for daemon to be ready before starting FluxNode sync...');
    this.daemonCheckInterval = setInterval(async () => {
      try {
        const nodeList = await this.rpc.getFluxNodeList();
        if (Array.isArray(nodeList) && nodeList.length > 0) {
          this.daemonReady = true;
          logger.info('Daemon is now ready, starting FluxNode sync');

          // Stop checking
          if (this.daemonCheckInterval) {
            clearInterval(this.daemonCheckInterval);
            this.daemonCheckInterval = null;
          }

          // Do initial sync
          await this.syncFluxNodes();

          // Start periodic sync
          this.startPeriodicSync();
        }
      } catch (error: any) {
        logger.debug('Still waiting for daemon...', { error: error.message });
      }
    }, this.DAEMON_CHECK_INTERVAL_MS);
  }

  /**
   * Start the periodic sync interval
   */
  private startPeriodicSync(): void {
    if (this.syncInterval) return;

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
    if (this.daemonCheckInterval) {
      clearInterval(this.daemonCheckInterval);
      this.daemonCheckInterval = null;
    }
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
    logger.info('FluxNode sync service stopped');
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
      if (nodeList.length === 0) {
        logger.warn('No FluxNodes fetched from daemon, skipping database update to preserve existing data');
        return;
      }

      // Step 2: Aggregate counts by payment address
      const addressCounts = new Map<string, FluxNodeCounts>();

      for (const node of nodeList) {
        // FluxNode objects have 'payment_address' and 'tier' fields
        const address = node.payment_address || node.payee;
        const tier = node.tier?.toUpperCase() || '';

        if (!address) continue;

        if (!addressCounts.has(address)) {
          addressCounts.set(address, {
            address,
            cumulusCount: 0,
            nimbusCount: 0,
            stratusCount: 0,
            totalCollateral: BigInt(0),
          });
        }

        const counts = addressCounts.get(address)!;
        if (tier === 'CUMULUS') {
          counts.cumulusCount++;
          counts.totalCollateral += BigInt(CUMULUS_COLLATERAL);
        } else if (tier === 'NIMBUS') {
          counts.nimbusCount++;
          counts.totalCollateral += BigInt(NIMBUS_COLLATERAL);
        } else if (tier === 'STRATUS') {
          counts.stratusCount++;
          counts.totalCollateral += BigInt(STRATUS_COLLATERAL);
        }
      }

      logger.info(`Aggregated FluxNode counts for ${addressCounts.size} addresses`);

      // Step 3: Truncate old data and insert fresh data
      // This ensures addresses that no longer run FluxNodes are removed
      const updates = Array.from(addressCounts.values());

      if (updates.length > 0) {
        // Truncate old data first (removes stale records from users who stopped running nodes)
        await this.ch.command('TRUNCATE TABLE live_fluxnodes');

        // Format date as 'YYYY-MM-DD HH:mm:ss' for ClickHouse DateTime
        const now = new Date();
        const lastSync = now.toISOString().slice(0, 19).replace('T', ' ');

        const rows = updates.map(u => ({
          address: u.address,
          cumulus_count: u.cumulusCount,
          nimbus_count: u.nimbusCount,
          stratus_count: u.stratusCount,
          total_collateral: u.totalCollateral.toString(),
          last_sync: lastSync,
        }));

        await this.ch.insert('live_fluxnodes', rows);
        logger.info(`Updated FluxNode counts for ${updates.length} addresses in live_fluxnodes`);
      }

      const elapsed = Date.now() - startTime;
      logger.info(`FluxNode synchronization complete in ${elapsed}ms`, {
        totalNodes: nodeList.length,
        addressesUpdated: addressCounts.size,
      });
    } catch (error: any) {
      logger.error('FluxNode synchronization failed', { error: error.message });
      throw error;
    } finally {
      this.isSyncing = false;
    }
  }
}
