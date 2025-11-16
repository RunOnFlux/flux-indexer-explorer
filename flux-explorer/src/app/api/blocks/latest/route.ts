import { NextRequest, NextResponse } from "next/server";
import { FluxAPI, FluxAPIError } from "@/lib/api/client";

export const dynamic = "force-dynamic";
export const revalidate = 0;

/**
 * Request Coalescing for Latest Blocks Endpoint
 *
 * CRITICAL: This endpoint is polled every 1-2 seconds by ALL homepage users.
 *
 * Problem: 50 concurrent users = 50 requests/sec to the indexer database.
 * Without protection, this is the #1 DDoS vector for the entire application.
 *
 * Solution: Share Promises across concurrent requests within a 1-second window.
 * - First request triggers the database query
 * - Concurrent requests (within 1s) share the same Promise
 * - Result: 50 concurrent requests = 1 database query
 */
interface InflightRequest<T> {
  promise: Promise<T>;
  timestamp: number;
}

const inflightRequests = new Map<string, InflightRequest<unknown>>();
const COALESCE_WINDOW_MS = 1000; // 1 second window (matches polling interval)

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

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const limitParam = searchParams.get("limit");

  let limit = 10;
  if (limitParam) {
    const parsed = Number(limitParam);
    if (Number.isFinite(parsed)) {
      limit = Math.max(1, Math.min(Math.floor(parsed), 50));
    }
  }

  // Request coalescing: cache key based on limit parameter
  const cacheKey = `latest-blocks:${limit}`;

  // Check if there's already an inflight request for this limit
  const existingRequest = inflightRequests.get(cacheKey);
  if (existingRequest) {
    console.log(`[Latest Blocks] Coalescing request for ${cacheKey} (sharing existing fetch)`);
    try {
      const blocks = await existingRequest.promise;
      return NextResponse.json(blocks, {
        headers: {
          "X-Coalesced": "true",
        },
      });
    } catch (error) {
      // Error will be re-thrown and handled below
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
  console.log(`[Latest Blocks] Starting new fetch for ${cacheKey}`);
  const fetchPromise = FluxAPI.getLatestBlocks(limit);

  // Store the promise so concurrent requests can share it
  inflightRequests.set(cacheKey, {
    promise: fetchPromise,
    timestamp: Date.now(),
  });

  try {
    const blocks = await fetchPromise;
    return NextResponse.json(blocks);
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
