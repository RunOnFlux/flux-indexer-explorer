/**
 * Theoretical Flux supply calculators ported from the legacy insight-api
 * SupplyHelper. Shared by the /api/v1 supply stats endpoint and the Insight
 * compatibility layer.
 */

/**
 * Calculate mainchain-only supply (without parallel asset distributions)
 * Based on SupplyHelper.getCirculatingSupplyByHeight from insight-api
 * Does NOT include: exchange fund, snapshot amounts, chain funds, or parallel asset mining
 */
export function calculateMainchainSupply(height: number): bigint {
  const PON_HEIGHT = 2020000;
  const FIRST_HALVING = 657850;
  const HALVING_INTERVAL = 655350;

  // Time-locked fund releases (12 releases)
  const FUND_RELEASES = [
    { height: 836274, amount: 7500000 },
    { height: 836994, amount: 2500000 },
    { height: 837714, amount: 22000000 },
    { height: 859314, amount: 22000000 },
    { height: 880914, amount: 22000000 },
    { height: 902514, amount: 22000000 },
    { height: 924114, amount: 22000000 },
    { height: 945714, amount: 22000000 },
    { height: 967314, amount: 22000000 },
    { height: 988914, amount: 22000000 },
    { height: 1010514, amount: 22000000 },
    { height: 1032114, amount: 22000000 },
  ];

  let subsidy = 150;
  const miningHeight = Math.min(height, PON_HEIGHT - 1);
  const halvings = Math.min(2, Math.floor((miningHeight - 2500) / HALVING_INTERVAL));

  // Initial supply: slow start + premine + dev fund
  let coins = (FIRST_HALVING - 5000) * 150 + 375000 + 13020000;

  // Calculate traditional mining rewards through halvings
  for (let i = 1; i <= halvings; i++) {
    subsidy = subsidy / 2;

    if (i === halvings) {
      const nBlocksMain = miningHeight - FIRST_HALVING - ((i - 1) * HALVING_INTERVAL);
      coins += nBlocksMain * subsidy;
    } else {
      coins += HALVING_INTERVAL * subsidy;
    }
  }

  // Add time-locked fund releases
  for (const release of FUND_RELEASES) {
    if (height >= release.height) {
      coins += release.amount;
    }
  }

  // Add PON rewards after PON_HEIGHT (mainchain only)
  if (height >= PON_HEIGHT) {
    coins += (height - PON_HEIGHT + 1) * 14;
  }

  return BigInt(Math.floor(coins * 100000000));
}

/**
 * Calculate circulating supply including all parallel asset chains
 * Based on insight-api getCirculatingSupplyAllChains implementation
 */
export function calculateCirculatingSupplyAllChains(height: number): bigint {
  const PON_HEIGHT = 2020000;
  const ASSET_MINING_START = 825000;
  const FIRST_HALVING = 657850;
  const HALVING_INTERVAL = 655350;
  const EXCHANGE_FUND_HEIGHT = 835554;
  const EXCHANGE_FUND_AMOUNT = 10000000;
  const CHAIN_FUND_AMOUNT = 1000000; // dev + exchange fund allocated per chain launch
  const SNAPSHOT_AMOUNT = 12313785.94991485; // user snapshot per chain

  // Parallel asset chain launch heights
  const CHAINS = [
    { name: 'KDA', launchHeight: 825000 },
    { name: 'BSC', launchHeight: 883000 },
    { name: 'ETH', launchHeight: 883000 },
    { name: 'SOL', launchHeight: 969500 },
    { name: 'TRX', launchHeight: 969500 },
    { name: 'AVAX', launchHeight: 1170000 },
    { name: 'ERGO', launchHeight: 1210000 },
    { name: 'ALGO', launchHeight: 1330000 },
    { name: 'MATIC', launchHeight: 1414000 },
    { name: 'BASE', launchHeight: 1738000 },
  ];

  let subsidy = 150;
  const miningHeight = Math.min(height, PON_HEIGHT - 1);
  const halvings = Math.min(2, Math.floor((miningHeight - 2500) / HALVING_INTERVAL));

  // Initial supply: slow start + premine + dev fund
  let coins = (FIRST_HALVING - 5000) * 150 + 375000 + 13020000;

  // Add exchange fund if height reached
  if (height >= EXCHANGE_FUND_HEIGHT) {
    coins += EXCHANGE_FUND_AMOUNT;
  }

  // Add snapshot amounts and chain funds for launched chains
  for (const chain of CHAINS) {
    if (height > chain.launchHeight) {
      coins += CHAIN_FUND_AMOUNT + SNAPSHOT_AMOUNT;
    }
  }

  // Calculate traditional mining rewards through halvings
  for (let i = 1; i <= halvings; i++) {
    subsidy = subsidy / 2;

    if (i === halvings) {
      // Current/last halving period - partial blocks
      const nBlocksMain = miningHeight - FIRST_HALVING - ((i - 1) * HALVING_INTERVAL);
      coins += nBlocksMain * subsidy;

      // Add parallel asset mining rewards (1/10 of main chain subsidy)
      if (miningHeight > ASSET_MINING_START) {
        const activeChains = CHAINS.filter(chain => miningHeight > chain.launchHeight).length;
        coins += nBlocksMain * subsidy * activeChains / 10;
      }
    } else {
      // Completed halving period - full interval
      coins += HALVING_INTERVAL * subsidy;

      // Add parallel asset mining for the completed period
      if (miningHeight > ASSET_MINING_START) {
        const nBlocksAsset = HALVING_INTERVAL - (ASSET_MINING_START - FIRST_HALVING);
        const activeChains = CHAINS.filter(chain => miningHeight > chain.launchHeight).length;
        coins += nBlocksAsset * subsidy * activeChains / 10;
      }
    }
  }

  // Add PON (Proof of Node) rewards after PON_HEIGHT - × 2 for parallel assets
  if (height >= PON_HEIGHT) {
    coins += (height - PON_HEIGHT + 1) * 14 * 2;
  }

  // Convert to zatoshis (1 FLUX = 100,000,000 zatoshis)
  // Use Math.floor to handle decimal from SNAPSHOT_AMOUNT
  return BigInt(Math.floor(coins * 100000000));
}
