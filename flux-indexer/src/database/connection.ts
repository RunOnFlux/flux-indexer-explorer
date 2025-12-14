/**
 * ClickHouse Connection Module
 *
 * Provides a wrapper around the ClickHouse client with connection pooling,
 * query helpers, and bulk insert capabilities optimized for blockchain indexing.
 */

import { createClient, ClickHouseClient } from '@clickhouse/client';
import { logger } from '../utils/logger';

export interface ClickHouseConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  requestTimeout?: number;
  maxOpenConnections?: number;
}

export class ClickHouseConnection {
  private client: ClickHouseClient;
  private config: ClickHouseConfig;
  private connected: boolean = false;

  constructor(config: ClickHouseConfig) {
    this.config = config;
    this.client = createClient({
      url: `http://${config.host}:${config.port}`,
      username: config.username,
      password: config.password,
      database: config.database,
      clickhouse_settings: {
        // ========== INSERT PERFORMANCE ==========
        // Async inserts for high throughput during sync
        async_insert: 1 as any,
        wait_for_async_insert: 0 as any,
        // Buffer settings for async inserts - longer timeout to batch more inserts
        // This reduces part creation rate by buffering inserts for up to 5 seconds
        async_insert_busy_timeout_ms: 5000 as any,
        async_insert_busy_timeout_max_ms: 10000 as any,
        async_insert_max_data_size: 104857600 as any,  // 100MB buffer before flush

        // ========== QUERY PERFORMANCE ==========
        // Use all available cores for queries
        max_threads: 0 as any,  // 0 = use all cores
        // Prefer projections when available
        optimize_use_projections: 1 as any,
        force_optimize_projection: 0 as any,
        // Use PREWHERE optimization
        optimize_move_to_prewhere: 1 as any,
        optimize_move_to_prewhere_if_final: 1 as any,

        // ========== MEMORY OPTIMIZATION ==========
        // Limit memory per query
        max_memory_usage: 10737418240 as any,  // 10GB max per query
        // Allow spill to disk if needed
        max_bytes_before_external_group_by: 5368709120 as any,  // 5GB
        max_bytes_before_external_sort: 5368709120 as any,  // 5GB

        // ========== BULK INSERT OPTIMIZATION ==========
        // Optimize for bulk inserts
        max_insert_block_size: 1048576 as any,
        min_insert_block_size_rows: 1048449 as any,
        min_insert_block_size_bytes: 268402944 as any,  // 256MB

        // ========== MISC ==========
        // Allow nullable types in inserts
        input_format_null_as_default: 1 as any,
        // Skip unavailable shards (for future clustering)
        skip_unavailable_shards: 1 as any,
        // Enable lightweight deletes for reorg handling
        allow_experimental_lightweight_delete: 1 as any,
        // Allow nondeterministic functions in mutations
        allow_nondeterministic_mutations: 1 as any,
      },
      max_open_connections: config.maxOpenConnections ?? 25,
      request_timeout: config.requestTimeout ?? 300000,
      keep_alive: {
        enabled: true,
      },
      compression: {
        request: true,
        response: true,
      },
    });
  }

  /**
   * Test connection and log success/failure
   */
  async connect(): Promise<void> {
    try {
      const result = await this.client.query({
        query: 'SELECT 1 AS ok',
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ ok: number }>();
      if (rows[0]?.ok === 1) {
        this.connected = true;
        logger.info('ClickHouse connection established', {
          host: this.config.host,
          port: this.config.port,
          database: this.config.database,
        });
      }
    } catch (error: any) {
      logger.error('ClickHouse connection failed', {
        error: error.message,
        host: this.config.host,
        port: this.config.port,
      });
      throw error;
    }
  }

  /**
   * Connect with retry logic - waits for ClickHouse to become available.
   * Used during startup when ClickHouse may still be bootstrapping.
   * @param maxRetries Maximum number of retry attempts (default: 60)
   * @param retryIntervalMs Interval between retries in ms (default: 10000 = 10s)
   */
  async connectWithRetry(maxRetries: number = 60, retryIntervalMs: number = 10000): Promise<void> {
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        await this.connect();
        return; // Success!
      } catch (error: any) {
        lastError = error;
        const waitMinutes = Math.ceil((maxRetries - attempt) * retryIntervalMs / 60000);
        logger.warn(`ClickHouse not ready (attempt ${attempt}/${maxRetries})`, {
          error: error.message,
          nextRetryIn: `${retryIntervalMs / 1000}s`,
          maxWaitRemaining: `~${waitMinutes} minutes`,
        });

        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, retryIntervalMs));
        }
      }
    }

    logger.error('ClickHouse connection failed after all retries', {
      attempts: maxRetries,
      totalWaitTime: `${(maxRetries * retryIntervalMs) / 60000} minutes`,
    });
    throw lastError;
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    await this.client.close();
    this.connected = false;
    logger.info('ClickHouse connection closed');
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Execute a SELECT query and return typed results
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<T[]> {
    const result = await this.client.query({
      query: sql,
      query_params: params,
      format: 'JSONEachRow',
    });
    return result.json<T>();
  }

  /**
   * Execute a single-row query (first result or null)
   */
  async queryOne<T = Record<string, unknown>>(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<T | null> {
    const results = await this.query<T>(sql, params);
    return results.length > 0 ? results[0] : null;
  }

  /**
   * Execute a count query and return the count
   */
  async queryCount(sql: string, params?: Record<string, unknown>): Promise<number> {
    const result = await this.queryOne<{ count: string | number }>(sql, params);
    if (!result) return 0;
    return typeof result.count === 'string' ? parseInt(result.count, 10) : result.count;
  }

  /**
   * Bulk insert rows into a table using JSONEachRow format (async buffered)
   */
  async insert<T extends Record<string, unknown>>(
    table: string,
    values: T[]
  ): Promise<void> {
    if (values.length === 0) return;

    await this.client.insert({
      table,
      values,
      format: 'JSONEachRow',
    });
  }

  /**
   * Synchronous bulk insert - bypasses async buffer for immediate visibility.
   * Use for data that must be queryable immediately after insert.
   * Slightly slower than async insert but guarantees read-after-write consistency.
   */
  async syncInsert<T extends Record<string, unknown>>(
    table: string,
    values: T[]
  ): Promise<void> {
    if (values.length === 0) return;

    await this.client.insert({
      table,
      values,
      format: 'JSONEachRow',
      clickhouse_settings: {
        async_insert: 0,
        wait_for_async_insert: 1,
      } as any,
    });
  }

  /**
   * Execute a command (DDL, ALTER, etc.) - no results returned
   */
  async command(sql: string): Promise<void> {
    await this.client.command({ query: sql });
  }

  /**
   * Execute multiple commands in sequence
   */
  async commandMulti(statements: string[]): Promise<void> {
    for (const sql of statements) {
      const trimmed = sql.trim();
      if (trimmed && !trimmed.startsWith('--')) {
        await this.command(trimmed);
      }
    }
  }

  /**
   * Execute schema file (split by semicolons, skip comments)
   * Handles multi-line statements and inline comments properly
   */
  async executeSchema(schemaSQL: string): Promise<void> {
    // Remove full-line comments and empty lines, but preserve inline content
    const lines = schemaSQL.split('\n');
    const cleanedLines: string[] = [];

    for (const line of lines) {
      const trimmed = line.trim();
      // Skip full-line comments
      if (trimmed.startsWith('--')) continue;
      // Remove inline comments (but keep the part before --)
      const commentIndex = line.indexOf('--');
      if (commentIndex > 0) {
        cleanedLines.push(line.substring(0, commentIndex));
      } else {
        cleanedLines.push(line);
      }
    }

    const cleanedSQL = cleanedLines.join('\n');

    // Split by semicolons
    const statements = cleanedSQL
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    let successCount = 0;
    let failCount = 0;

    for (const statement of statements) {
      // Skip if it's just whitespace or empty after cleanup
      if (!statement || statement.length < 5) continue;

      try {
        await this.command(statement);
        successCount++;
      } catch (error: any) {
        failCount++;
        // Only warn for non-critical errors (like "table already exists" or "projection already exists")
        const errMsg = error.message || '';
        const isExpectedError =
          errMsg.includes('already exists') ||
          errMsg.includes('Could not find table') ||
          errMsg.includes('Unknown table');

        if (isExpectedError) {
          logger.debug('Schema statement skipped (already exists or not found)', {
            statement: statement.substring(0, 80) + '...',
          });
        } else {
          logger.error('Schema statement FAILED', {
            error: error.message,
            statement: statement.substring(0, 200),
          });
        }
      }
    }

    logger.info('Schema execution complete', { successCount, failCount, totalStatements: statements.length });
  }

  /**
   * Check if a table exists
   */
  async tableExists(tableName: string): Promise<boolean> {
    const result = await this.queryOne<{ count: number }>(`
      SELECT count() as count
      FROM system.tables
      WHERE database = {database:String} AND name = {table:String}
    `, {
      database: this.config.database,
      table: tableName,
    });
    return (result?.count ?? 0) > 0;
  }

  /**
   * Get table row count
   */
  async getTableRowCount(tableName: string): Promise<number> {
    return this.queryCount(`SELECT count() as count FROM ${tableName}`);
  }

  /**
   * Optimize a table (force merge of parts)
   */
  async optimizeTable(tableName: string, final: boolean = false): Promise<void> {
    const finalClause = final ? ' FINAL' : '';
    await this.command(`OPTIMIZE TABLE ${tableName}${finalClause}`);
  }

  /**
   * Wait for all mutations to complete
   */
  async waitForMutations(timeoutMs: number = 300000): Promise<void> {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
      const result = await this.queryOne<{ count: number }>(`
        SELECT count() as count
        FROM system.mutations
        WHERE is_done = 0 AND database = {database:String}
      `, { database: this.config.database });

      if (!result || result.count === 0) {
        return;
      }

      logger.debug('Waiting for mutations to complete', { pending: result.count });
      await new Promise((r) => setTimeout(r, 1000));
    }
    throw new Error('Timeout waiting for mutations to complete');
  }

  /**
   * Get the raw ClickHouse client for advanced operations
   */
  getClient(): ClickHouseClient {
    return this.client;
  }
}

// Singleton instance
let clickhouseInstance: ClickHouseConnection | null = null;

/**
 * Get or create the ClickHouse connection singleton
 */
export function getClickHouse(config?: ClickHouseConfig): ClickHouseConnection {
  if (!clickhouseInstance) {
    if (!config) {
      throw new Error('ClickHouse config required for initial connection');
    }
    clickhouseInstance = new ClickHouseConnection(config);
  }
  return clickhouseInstance;
}

/**
 * Close the singleton connection
 */
export async function closeClickHouse(): Promise<void> {
  if (clickhouseInstance) {
    await clickhouseInstance.close();
    clickhouseInstance = null;
  }
}
