/**
 * API Route for Flux Supply Statistics
 *
 * This server-side API route proxies requests to the FluxIndexer API
 * to get accurate supply statistics from the blockchain
 */

import { NextResponse } from "next/server";

export async function GET() {
  try {
    const indexerUrl = process.env.SERVER_API_URL || process.env.NEXT_PUBLIC_SERVER_API_URL || "http://localhost:3002";
    const response = await fetch(`${indexerUrl}/api/v1/supply`, {
      headers: {
        "Accept": "application/json",
      },
      // Cache for 15 seconds to catch blocks faster than the 30s average
      next: { revalidate: 15 },
    });

    if (!response.ok) {
      throw new Error(`FluxIndexer API returned ${response.status}`);
    }

    const data = await response.json();

    return NextResponse.json(data);
  } catch (error) {
    console.error("Failed to fetch supply from FluxIndexer:", error);
    return NextResponse.json(
      { error: "Failed to fetch supply data" },
      { status: 500 }
    );
  }
}
