/**
 * Block Range API - Fetch blocks in a height range
 *
 * GET /api/blocks/range?from=2000000&to=2010000
 *
 * Query Parameters:
 *   - from: starting block height (required)
 *   - to: ending block height (required)
 *   - fields: comma-separated list of fields (optional)
 *
 * Max range: 10,000 blocks per request
 *
 * Example responses:
 *   /api/blocks/range?from=2000000&to=2000010
 *   /api/blocks/range?from=2000000&to=2010000&fields=height,time,difficulty
 */

import { NextRequest, NextResponse } from 'next/server';

const INDEXER_API_URL = process.env.SERVER_API_URL || 'http://127.0.0.1:42067';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const from = searchParams.get('from');
    const to = searchParams.get('to');
    const fields = searchParams.get('fields');

    // Validate required parameters
    if (!from || !to) {
      return NextResponse.json(
        {
          error: 'Missing required parameters: from and to (block heights)',
          example: '/api/blocks/range?from=2000000&to=2010000',
          availableFields: [
            'height', 'hash', 'time', 'timestamp', 'difficulty', 'size',
            'txCount', 'tx_count', 'producer', 'producer_reward',
            'chainwork', 'bits', 'nonce', 'version', 'merkle_root', 'prev_hash'
          ]
        },
        { status: 400 }
      );
    }

    // Build query string for indexer
    let queryString = `from=${from}&to=${to}`;
    if (fields) {
      queryString += `&fields=${fields}`;
    }

    // Fetch from indexer
    const response = await fetch(
      `${INDEXER_API_URL}/api/v1/blocks/range?${queryString}`,
      {
        headers: {
          'Accept': 'application/json',
        },
        // Don't cache - data changes
        cache: 'no-store',
      }
    );

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
      return NextResponse.json(errorData, { status: response.status });
    }

    const data = await response.json();

    // Return with CORS headers
    return NextResponse.json(data, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      },
    });

  } catch (error) {
    console.error('[Blocks Range API] Error:', error);
    return NextResponse.json(
      {
        error: 'Failed to fetch blocks range',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// Handle CORS preflight
export async function OPTIONS() {
  return new NextResponse(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}
