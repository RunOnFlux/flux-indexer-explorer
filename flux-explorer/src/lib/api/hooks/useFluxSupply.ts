/**
 * React Query hooks for Flux supply statistics
 */

import { useQuery } from "@tanstack/react-query";

interface SupplyStats {
  blockHeight: number;
  transparentSupply: string;
  shieldedPool: string;
  circulatingSupply: string;
  totalSupply: string;
  maxSupply: string;
  lastUpdate: string;
  timestamp: string;
}

/**
 * Hook to fetch Flux supply statistics from the indexer API
 */
export function useFluxSupply() {
  return useQuery({
    queryKey: ["flux-supply"],
    queryFn: async (): Promise<{
      circulatingSupply: number;
      totalSupply: number;
      maxSupply: number;
    }> => {
      const response = await fetch("/api/supply");
      if (!response.ok) {
        throw new Error("Failed to fetch supply stats");
      }
      const data: SupplyStats = await response.json();
      return {
        circulatingSupply: parseFloat(data.circulatingSupply),
        totalSupply: parseFloat(data.totalSupply),
        maxSupply: parseFloat(data.maxSupply),
      };
    },
    staleTime: 15 * 1000, // 15 seconds
    refetchInterval: 15 * 1000, // Refetch every 15 seconds to catch blocks faster than 30s average
  });
}
