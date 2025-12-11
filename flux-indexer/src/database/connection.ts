/**
 * Database Connection Manager
 *
 * Handles PostgreSQL connection pooling and query execution
 */

import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { DatabaseError } from '../types';
import { logger } from '../utils/logger';

export class DatabaseConnection {
  private pool: Pool;
  private isConnected = false;

  constructor(config: {
    host: string;
    port: number;
    database: string;
    user: string;
    password: string;
    max?: number;
    idleTimeoutMillis?: number;
    connectionTimeoutMillis?: number;
  }) {
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: config.max || 20,
      idleTimeoutMillis: config.idleTimeoutMillis || 30000,
      connectionTimeoutMillis: config.connectionTimeoutMillis || 10000,
      // Explicitly set client encoding to UTF-8 to avoid encoding issues
      application_name: 'fluxindexer',
    });

    // Handle pool errors
    this.pool.on('error', (err) => {
      logger.error('Unexpected database pool error', { error: err });
    });
  }

  /**
   * Connect to database
   */
  async connect(): Promise<void> {
    try {
      const client = await this.pool.connect();
      client.release();
      this.isConnected = true;
      logger.info('Database connected successfully');
    } catch (error: any) {
      this.isConnected = false;
      throw new DatabaseError('Failed to connect to database', { error: error.message });
    }
  }

  /**
   * Execute query
   */
  async query<T extends QueryResultRow = any>(text: string, params?: any[]): Promise<QueryResult<T>> {
    try {
      return await this.pool.query<T>(text, params);
    } catch (error: any) {
      logger.error('Database query error', { query: text, params, error: error.message });
      throw new DatabaseError(`Query failed: ${error.message}`, { query: text, params });
    }
  }

  /**
   * Get a client for transactions
   */
  async getClient(): Promise<PoolClient> {
    try {
      return await this.pool.connect();
    } catch (error: any) {
      throw new DatabaseError('Failed to get database client', { error: error.message });
    }
  }

  /**
   * Execute queries in a transaction
   */
  async transaction<T>(
    callback: (client: PoolClient) => Promise<T>
  ): Promise<T> {
    const client = await this.getClient();

    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error: any) {
      await client.query('ROLLBACK');
      logger.error('Transaction rolled back', { error: error.message });
      throw new DatabaseError(`Transaction failed: ${error.message}`);
    } finally {
      client.release();
    }
  }

  /**
   * Close all connections
   */
  async close(): Promise<void> {
    await this.pool.end();
    this.isConnected = false;
    logger.info('Database connections closed');
  }

  /**
   * Check if connected
   */
  isConnectionActive(): boolean {
    return this.isConnected;
  }

  /**
   * Get pool statistics
   */
  getPoolStats() {
    return {
      total: this.pool.totalCount,
      idle: this.pool.idleCount,
      waiting: this.pool.waitingCount,
    };
  }

  /**
   * Get the underlying pool instance (for advanced use cases)
   */
  getPool(): Pool {
    return this.pool;
  }
}
