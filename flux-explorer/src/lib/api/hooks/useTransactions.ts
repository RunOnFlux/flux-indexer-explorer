/**
 * React Query Hooks for Transaction Operations
 *
 * Provides hooks for fetching and managing transaction data with
 * automatic caching, refetching, and error handling.
 */

import { useQuery, useQueries, UseQueryOptions, UseQueryResult } from "@tanstack/react-query";
import { FluxAPI } from "../client";
import type { Transaction } from "@/types/flux-api";

/**
 * Query keys for transaction operations
 */
export const transactionKeys = {
  all: ["transactions"] as const,
  details: () => [...transactionKeys.all, "detail"] as const,
  detail: (txid: string) => [...transactionKeys.details(), txid] as const,
  raw: (txid: string) => [...transactionKeys.all, "raw", txid] as const,
};

/**
 * Hook to fetch a transaction by ID
 *
 * @param txid - Transaction ID
 * @param options - React Query options
 * @returns Query result with transaction data
 *
 * @example
 * ```tsx
 * const { data: transaction, isLoading, error } = useTransaction(txid);
 *
 * if (isLoading) return <div>Loading...</div>;
 * if (error) return <div>Error: {error.message}</div>;
 * if (transaction) return <div>Value: {transaction.valueOut}</div>;
 * ```
 */
export function useTransaction(
  txid: string,
  options?: Omit<UseQueryOptions<Transaction, Error>, "queryKey" | "queryFn">
): UseQueryResult<Transaction, Error> {
  return useQuery<Transaction, Error>({
    queryKey: transactionKeys.detail(txid),
    queryFn: () => FluxAPI.getTransaction(txid),
    enabled: !!txid && txid.length === 64, // Valid txid is 64 hex characters
    staleTime: 5 * 60 * 1000, // 5 minutes - confirmed transactions don't change
    ...options,
  });
}

/**
 * Hook to fetch raw transaction data
 *
 * @param txid - Transaction ID
 * @param options - React Query options
 * @returns Query result with raw transaction hex
 *
 * @example
 * ```tsx
 * const { data } = useRawTransaction(txid);
 * if (data) {
 *   console.log('Raw TX:', data.rawtx);
 * }
 * ```
 */
export function useRawTransaction(
  txid: string,
  options?: Omit<UseQueryOptions<{ rawtx: string }, Error>, "queryKey" | "queryFn">
): UseQueryResult<{ rawtx: string }, Error> {
  return useQuery<{ rawtx: string }, Error>({
    queryKey: transactionKeys.raw(txid),
    queryFn: () => FluxAPI.getRawTransaction(txid),
    enabled: !!txid && txid.length === 64,
    staleTime: 5 * 60 * 1000,
    ...options,
  });
}

/**
 * Hook to fetch multiple transactions by IDs
 *
 * Useful when you need to fetch multiple transactions at once.
 * Each transaction is cached individually.
 *
 * @param txids - Array of transaction IDs
 * @returns Array of query results
 *
 * @example
 * ```tsx
 * const transactions = useTransactions(['txid1', 'txid2', 'txid3']);
 * const allLoaded = transactions.every(q => !q.isLoading);
 * const anyError = transactions.some(q => q.error);
 * ```
 */
export function useTransactions(txids: string[]): UseQueryResult<Transaction, Error>[] {
  return useQueries({
    queries: txids.map((txid) => ({
      queryKey: transactionKeys.detail(txid),
      queryFn: () => FluxAPI.getTransaction(txid),
      enabled: !!txid && txid.length === 64,
      staleTime: 5 * 60 * 1000,
    })),
  });
}

/**
 * Hook to fetch multiple transactions in a single batch request
 *
 * More efficient than useTransactions when fetching many transactions,
 * especially when all transactions are from the same block.
 *
 * @param txids - Array of transaction IDs
 * @param blockHeight - Optional block height (optimization hint)
 * @returns Query result with array of transactions
 *
 * @example
 * ```tsx
 * const { data: transactions, isLoading } = useTransactionsBatch(txids, block.height);
 * ```
 */
export function useTransactionsBatch(
  txids: string[],
  blockHeight?: number
): UseQueryResult<Transaction[], Error> {
  // Create a stable key from txids (sort a COPY to avoid mutating the input array)
  const txidsKey = [...txids].sort().join(',');

  return useQuery<Transaction[], Error>({
    queryKey: [...transactionKeys.all, "batch", txidsKey, blockHeight],
    queryFn: () => FluxAPI.getTransactionsBatch(txids, blockHeight),
    enabled: txids.length > 0 && txids.every(txid => txid.length === 64),
    staleTime: 5 * 60 * 1000, // 5 minutes - confirmed transactions don't change
  });
}
