import { calculateCirculatingSupplyAllChains, calculateMainchainSupply } from './supply-helper';

describe('supply helper', () => {
  // Pinned against the legacy insight-api at explorer.runonflux.io, where at
  // height 2676212 /statistics/main-chain-circulating-locked returned
  // 426165694.5 and /circulating-supply returned 411610498.4991484 FLUX.
  test('matches the legacy insight-api values at a post-PON height', () => {
    expect(calculateMainchainSupply(2676212)).toBe(42616569450000000n);
    expect(calculateCirculatingSupplyAllChains(2676212)).toBe(41161049849914840n);
  });

  test('computes pre-PON supplies', () => {
    expect(calculateMainchainSupply(1500000)).toBe(39747875000000000n);
    expect(calculateCirculatingSupplyAllChains(1500000)).toBe(33656082354923356n);
  });

  test('keeps the parallel-asset locked delta positive', () => {
    const locked = calculateMainchainSupply(2676212) - calculateCirculatingSupplyAllChains(2676212);
    expect(locked).toBe(1455519600085160n);
  });
});
