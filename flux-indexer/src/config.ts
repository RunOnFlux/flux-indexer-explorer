/**
 * Configuration
 *
 * Loads and validates configuration from environment variables
 */

import dotenv from 'dotenv';
import { IndexerConfig } from './types';

// Load environment variables
dotenv.config();

function getEnv(key: string, defaultValue?: string): string {
  const value = process.env[key];
  if (value === undefined) {
    if (defaultValue !== undefined) {
      return defaultValue;
    }
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

function getEnvNumber(key: string, defaultValue: number): number {
  const value = process.env[key];
  if (value === undefined) {
    return defaultValue;
  }
  const num = parseInt(value);
  if (isNaN(num)) {
    throw new Error(`Invalid number for environment variable ${key}: ${value}`);
  }
  return num;
}

function getEnvBoolean(key: string, defaultValue: boolean): boolean {
  const value = process.env[key];
  if (value === undefined) {
    return defaultValue;
  }
  return value.toLowerCase() === 'true';
}

export const config: IndexerConfig = {
  rpc: {
    url: getEnv('FLUX_RPC_URL', 'http://127.0.0.1:16124'),
    username: process.env.FLUX_RPC_USER,
    password: process.env.FLUX_RPC_PASSWORD,
    timeout: getEnvNumber('FLUX_RPC_TIMEOUT', 30000),
  },
  clickhouse: {
    host: getEnv('CH_HOST', '127.0.0.1'),
    // CH_HTTP_PORT for indexer app (HTTP protocol), CH_PORT is for clickhouse-client (native protocol)
    port: getEnvNumber('CH_HTTP_PORT', getEnvNumber('CH_PORT', 8123)),
    database: getEnv('CH_DATABASE', 'fluxindexer'),
    username: getEnv('CH_USER', 'default'),
    password: getEnv('CH_PASSWORD', ''),
    requestTimeout: getEnvNumber('CH_REQUEST_TIMEOUT', 300000), // 5 min for large queries
    maxOpenConnections: getEnvNumber('CH_MAX_CONNECTIONS', 10),
  },
  indexer: {
    batchSize: getEnvNumber('INDEXER_BATCH_SIZE', 100),
    pollingInterval: getEnvNumber('INDEXER_POLLING_INTERVAL', 5000),
    startHeight: process.env.INDEXER_START_HEIGHT
      ? parseInt(process.env.INDEXER_START_HEIGHT)
      : undefined,
    enableReorgHandling: getEnvBoolean('INDEXER_ENABLE_REORG', true),
    maxReorgDepth: getEnvNumber('INDEXER_MAX_REORG_DEPTH', 100),
  },
  api: {
    port: getEnvNumber('API_PORT', 3002),
    host: getEnv('API_HOST', '0.0.0.0'),
    corsEnabled: getEnvBoolean('API_CORS_ENABLED', true),
  },
};
