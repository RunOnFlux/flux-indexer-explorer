import { NextRequest, NextResponse } from "next/server";
import { FluxAPI, FluxAPIError } from "@/lib/api/client";

export const dynamic = "force-dynamic";
export const revalidate = 0;

/**
 * Request Coalescing for Block Lookup Endpoint
 *
 * Popular blocks (latest, genesis, round numbers) get heavy concurrent traffic.
 *
 * Problem: Popular block lookups can cause request storms:
 * - Latest block during active browsing
 * - Genesis block (block 0) from curious users
 * - Round numbers (1000000, 2000000) as milestones
 *
 * Solution: Share Promises across concurrent requests within a 2-second window.
 * - Cache key includes identifier (hash/height) + mode (regular/raw/summary)
 * - Result: 20 users viewing same block = 1 database query
 */
interface InflightRequest<T> {
  promise: Promise<T>;
  timestamp: number;
}

const inflightRequests = new Map<string, InflightRequest<unknown>>();
const COALESCE_WINDOW_MS = 2000; // 2 second window for popular blocks

// Periodic cleanup of stale inflight requests (prevents memory leaks)
setInterval(() => {
  const now = Date.now();
  const keysToDelete: string[] = [];

  inflightRequests.forEach((request, key) => {
    if (now - request.timestamp > COALESCE_WINDOW_MS) {
      keysToDelete.push(key);
    }
  });

  keysToDelete.forEach(key => inflightRequests.delete(key));
}, COALESCE_WINDOW_MS);

type RouteContext = {
  params: {
    hashOrHeight: string;
  };
};

function getBooleanParam(value: string | null): boolean {
  if (!value) return false;
  const normalized = value.toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

export async function GET(request: NextRequest, { params }: RouteContext) {
  const identifier = params.hashOrHeight;
  const searchParams = request.nextUrl.searchParams;
  const wantsRaw = getBooleanParam(searchParams.get("raw"));
  const wantsSummary = getBooleanParam(searchParams.get("summary"));

  // Determine mode for cache key
  const mode = wantsRaw ? "raw" : wantsSummary ? "summary" : "regular";
  const cacheKey = `block:${identifier}:${mode}`;

  // Check if there's already an inflight request for this block+mode
  const existingRequest = inflightRequests.get(cacheKey);
  if (existingRequest) {
    console.log(`[Block] Coalescing request for ${cacheKey} (sharing existing fetch)`);
    try {
      const data = await existingRequest.promise;
      return NextResponse.json(data, {
        headers: {
          "X-Coalesced": "true",
        },
      });
    } catch (error) {
      const statusCode =
        error instanceof FluxAPIError
          ? error.statusCode ?? 500
          : 500;
      const message =
        error instanceof FluxAPIError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Unknown error";

      return NextResponse.json(
        { error: message },
        { status: statusCode }
      );
    }
  }

  // No inflight request, create a new one
  console.log(`[Block] Starting new fetch for ${cacheKey}`);

  let fetchPromise: Promise<unknown>;

  try {
    if (wantsRaw) {
      fetchPromise = FluxAPI.getRawBlock(identifier);
    } else if (wantsSummary) {
      const height = Number(identifier);
      if (!Number.isFinite(height)) {
        return NextResponse.json(
          { error: "summary mode requires a numeric block height" },
          { status: 400 }
        );
      }
      fetchPromise = FluxAPI.getBlockIndex(height);
    } else {
      fetchPromise = FluxAPI.getBlock(identifier);
    }

    // Store the promise so concurrent requests can share it
    inflightRequests.set(cacheKey, {
      promise: fetchPromise,
      timestamp: Date.now(),
    });

    const data = await fetchPromise;
    return NextResponse.json(data);
  } catch (error) {
    const statusCode =
      error instanceof FluxAPIError
        ? error.statusCode ?? 500
        : 500;
    const message =
      error instanceof FluxAPIError
        ? error.message
        : error instanceof Error
          ? error.message
          : "Unknown error";

    return NextResponse.json(
      { error: message },
      { status: statusCode }
    );
  }
}
