# Flux Blockchain Transaction Specification
## Byte-Level Documentation

Document Version: 2.1
Date: 2025-12-20
Last Updated: 2025-12-20 - Corrected Sapling output encCiphertext size and clarified JoinSplit sizes using on-chain parsing
Author: Compiled from parser implementation, RPC traces, and raw block parsing
Network: Flux (Zelcash fork of Zcash Sapling)
Status: Verified against production parser code and mainnet samples

---

## Table of Contents
1. Overview
2. Byte Size Cheat Sheet
3. Data Types and Encoding
4. Block Structure
5. Transaction Overview
6. Common Transparent Structures
7. Transaction Versions
8. Version 1: Legacy Transactions
9. Version 2: Sprout Shielded Transactions
10. FluxNode Transactions (Versions 3, 5, 6)
11. Version 4: Sapling Transactions
12. JoinSplit Description (Sprout and Sapling v4)
13. Fee Calculation for Shielded Transactions
14. Address Formats
15. Empirical Verification Samples
16. Common Pitfalls

---

## Overview

Flux extends Zcash Sapling with FluxNode transaction types and custom JoinSplit ciphertext sizes. This document specifies the exact serialized byte layout for blocks and transactions.

Key parameters:
- Genesis block: 0
- Sapling activation height: 250000
- Equihash(144,5) activation: 125111
- ZelHash activation: 372500
- Maximum block size: 2,097,152 bytes (2 MB)
- Consensus: PoW + Proof-of-Node (PoN)
- Shielded pools: Sprout (deprecated) and Sapling (active)

---

## Byte Size Cheat Sheet

Item | Size (bytes) | Notes
---- | ------------ | -----
Sprout JoinSplit ciphertext | 601 | v2 (standard Zcash)
Sapling v4 JoinSplit ciphertext | 549 | Flux-specific
JoinSplit total (v2) | 1802 | 600 + 2*601
JoinSplit total (v4) | 1698 | 600 + 2*549
Sapling SpendDesc | 384 | 32+32+32+32+192+64
Sapling OutputDesc | 948 | 32+32+32+192+580+80
Sapling output encCiphertext | 580 | matches Zcash Sapling
Sapling output outCiphertext | 80 | standard
bindingSig | 64 | present only if nShieldedSpend > 0 OR nShieldedOutput > 0
joinSplitPubKey | 32 | present only if nJoinSplit > 0
joinSplitSig | 64 | present only if nJoinSplit > 0

---

## Data Types and Encoding

All integer fields are little-endian unless stated otherwise.

VarInt (Bitcoin format):
- 0x00..0xfc: 1 byte value
- 0xfd: 2 byte uint16_LE follows
- 0xfe: 4 byte uint32_LE follows
- 0xff: 8 byte uint64_LE follows

VarBytes / VarString:
- VarInt length, followed by raw bytes of that length.

Arrays:
- VarInt count, followed by each item in sequence.

---

## Block Structure

### Common Header Fields (108 bytes base)

Offset | Size | Field | Type | Description
------ | ---- | ----- | ---- | -----------
0 | 4 | nVersion | uint32_LE | Block version (PoW usually 4, PoN >= 100)
4 | 32 | hashPrevBlock | char[32] | Previous block hash
36 | 32 | hashMerkleRoot | char[32] | Merkle root of transactions
68 | 32 | hashReserved/hashFinalSaplingRoot | char[32] | Pre-Sapling: zeros, Post-Sapling: Sapling tree root
100 | 4 | nTime | uint32_LE | Unix timestamp
104 | 4 | nBits | uint32_LE | Difficulty target

### PoW Block Header Extension (version < 100)

Offset | Size | Field | Type | Description
------ | ---- | ----- | ---- | -----------
108 | 32 | nNonce | char[32] | Equihash nonce
140 | VarInt | solution_size | VarInt | Equihash solution length in bytes
140+ | solution_size | nSolution | bytes | Equihash solution

Equihash variants by height:
- 0 to 125110: Equihash(200,9)
- 125111 to 372499: Equihash(144,5)
- 372500+: ZelHash(125,4)

### PoN Block Header Extension (version >= 100)

Offset | Size | Field | Type | Description
------ | ---- | ----- | ---- | -----------
108 | 32 | nodesCollateralHash | char[32] | FluxNode collateral tx hash
140 | 4 | nodesCollateralIdx | uint32_LE | FluxNode collateral output index
144 | VarInt | sig_size | VarInt | Block signature length
144+ | sig_size | blockSignature | bytes | FluxNode block signature

### Transaction Count

After the header extension:
- tx_count (VarInt)
- tx_count transactions in sequence

---

## Transaction Overview

Each transaction begins with a 4-byte version field:

- version_raw = uint32_LE
- is_overwintered = (version_raw & 0x80000000) != 0
- version_number = version_raw & 0x7fffffff

If is_overwintered:
- versionGroupId = uint32_LE
- Sapling v4 must use versionGroupId = 0x892f2085

FluxNode transactions (versions 3, 5, 6) use a custom payload:
- Immediately after the 4-byte version, read nType (uint8)
- If nType is 2 or 4, parse as FluxNode (no vin/vout)

Txid is double SHA256 of the serialized transaction bytes; the displayed hex is byte-reversed.

---

## Common Transparent Structures

### TxIn (input)

Offset | Size | Field | Type | Description
------ | ---- | ----- | ---- | -----------
0 | 32 | prevout_hash | char[32] | Previous tx hash
32 | 4 | prevout_index | uint32_LE | Output index
36 | VarInt | scriptSig_size | VarInt | ScriptSig length
36+ | scriptSig_size | scriptSig | bytes | ScriptSig bytes
... | 4 | nSequence | uint32_LE | Sequence

### TxOut (output)

Offset | Size | Field | Type | Description
------ | ---- | ----- | ---- | -----------
0 | 8 | nValue | int64_LE | Amount in zatoshis (1 FLUX = 100,000,000)
8 | VarInt | scriptPubKey_size | VarInt | ScriptPubKey length
8+ | scriptPubKey_size | scriptPubKey | bytes | ScriptPubKey bytes

---

## Transaction Versions

Version | Type | Overwintered | VersionGroupId | Notes
------- | ---- | ------------ | -------------- | -----
1 | Legacy | No | N/A | Transparent only
2 | Sprout shielded | No | N/A | JoinSplits
3 | FluxNode legacy | No | N/A | nType 2 or 4
4 | Sapling | Yes | 0x892f2085 | Sapling spends/outputs + JoinSplits
5 | FluxNode update/start | No | N/A | nType 2 or 4
6 | FluxNode confirm/start | No | N/A | nType 2 or 4, adds nFluxTxVersion for start

---

## Version 1: Legacy Transactions

Structure:
- nVersion (uint32_LE)
- vin_count (VarInt)
- vin[]
- vout_count (VarInt)
- vout[]
- nLockTime (uint32_LE)

---

## Version 2: Sprout Shielded Transactions

Structure:
- nVersion (uint32_LE) = 2
- vin_count, vin[]
- vout_count, vout[]
- nLockTime
- nJoinSplit (VarInt)
- vJoinSplit[] (each 1802 bytes; see JoinSplit section)
- joinSplitPubKey (32) if nJoinSplit > 0
- joinSplitSig (64) if nJoinSplit > 0

---

## FluxNode Transactions (Versions 3, 5, 6)

FluxNode transactions do not use the standard vin/vout layout. After the 4-byte version, read:

- nType (uint8)
  - 2 = START
  - 4 = CONFIRM

### START (nType = 2)

Version 3 or 5:
- nVersion (uint32_LE)
- nType (uint8)
- collateralOutpoint (36 bytes: 32 hash + 4 index)
- collateralPubKey (VarBytes)
- pubKey (VarBytes)
- sigTime (uint32_LE)
- sig (VarBytes)

Version 6 adds nFluxTxVersion (uint32_LE) after nType:
- nFluxTxVersion flags:
  - 0x01 normal
  - 0x02 p2sh
  - 0x0100 delegates feature

Based on nFluxTxVersion:
- If p2sh: read p2sh_pubkey (VarBytes) + p2sh_redeem (VarBytes)
- Else: read collateralPubKey (VarBytes) + pubKey (VarBytes)

Then:
- sigTime (uint32_LE)
- sig (VarBytes)
- If delegates feature flag set:
  - delegate_flag (uint8)
  - If delegate_flag == 1:
    - delegate_count (VarInt)
    - delegate_key[delegate_count] (VarBytes each)

### CONFIRM (nType = 4)

Structure:
- nVersion (uint32_LE)
- nType (uint8)
- collateralOutpoint (36 bytes)
- sigTime (uint32_LE)
- benchmarkTier (uint8)
- benchmarkSigTime (uint32_LE)
- nUpdateType (uint8)
- ip (VarString)
- sig (VarBytes)
- benchmarkSig (VarBytes)

---

## Version 4: Sapling Transactions

All Sapling transactions are overwintered and must use versionGroupId 0x892f2085.

Structure:
- nVersion (uint32_LE with overwintered bit set)
- versionGroupId (uint32_LE) = 0x892f2085
- vin_count, vin[]
- vout_count, vout[]
- nLockTime (uint32_LE)
- nExpiryHeight (uint32_LE)
- valueBalance (int64_LE)
- nShieldedSpend (VarInt)
- vShieldedSpend[] (384 bytes each)
- nShieldedOutput (VarInt)
- vShieldedOutput[] (948 bytes each)
- nJoinSplit (VarInt)
- vJoinSplit[] (each 1698 bytes; see JoinSplit section)
- joinSplitPubKey (32) if nJoinSplit > 0
- joinSplitSig (64) if nJoinSplit > 0
- bindingSig (64) if nShieldedSpend > 0 OR nShieldedOutput > 0

### Sapling SpendDesc (384 bytes)

Field | Size | Notes
----- | ---- | -----
cv | 32 | value commitment
anchor | 32 | merkle root
nullifier | 32 | nullifier
rk | 32 | randomized key
zkproof | 192 | Groth16 proof
spendAuthSig | 64 | spend authorization signature

### Sapling OutputDesc (948 bytes)

Field | Size | Notes
----- | ---- | -----
cv | 32 | value commitment
cmu | 32 | commitment
ephemeralKey | 32 | ephemeral key
zkproof | 192 | Groth16 proof
encCiphertext | 580 | encrypted note ciphertext
outCiphertext | 80 | outgoing ciphertext

---

## JoinSplit Description (Sprout and Sapling v4)

JoinSplit base fields (600 bytes):
- vpub_old (8)
- vpub_new (8)
- anchor (32)
- nullifiers (64)
- commitments (64)
- ephemeralKey (32)
- randomSeed (32)
- macs (64)
- proof (296)

Total size = 600 + 2*ciphertext_size

Ciphertext sizes:
- Sprout (v2): 601 bytes each, JoinSplit total 1802
- Sapling v4 (Flux): 549 bytes each, JoinSplit total 1698

---

## Fee Calculation for Shielded Transactions

fee = inputTotal - outputTotal - shieldedPoolChange

shieldedPoolChange:
- Sapling valueBalance (v4): positive means value leaves the shielded pool
- JoinSplits (v2 or v4): sum(vpub_old - vpub_new)

If both Sapling and JoinSplit fields are present, include both components.

---

## Address Formats

- t1... : P2PKH transparent
- t3... : P2SH transparent
- zc... : Sprout shielded (deprecated)
- zs... : Sapling shielded

---

## Empirical Verification Samples

Mainnet samples used to validate sizes (raw block hex parsing):

- Sapling output encCiphertext = 580 bytes:
  - Block 695525, tx 0f4e4a7bfc9c2b4dc7998c848f278af40cda93f9bcae76faa811f772fc0eb645
- Sapling v4 JoinSplit ciphertext = 549 bytes:
  - Block 278091, tx 4b9cf0de76b66379842cc9e6da9645d9f346e443d4674dedb4e1be517bc5efe5
- Sprout JoinSplit ciphertext = 601 bytes:
  - Block 200056, tx 3ba4f8e81369ff295c0d2cf252ae0d07396bd02012fb8d44139af7828a5ae536

---

## Common Pitfalls

- Do not infer Sapling tx fields from height; use versionGroupId = 0x892f2085.
- bindingSig appears only when nShieldedSpend > 0 or nShieldedOutput > 0.
- joinSplitPubKey/joinSplitSig appear only when nJoinSplit > 0.
- Sapling OutputDesc encCiphertext is 580 bytes (not 549).
- Flux v4 JoinSplit ciphertext is 549 bytes (Sprout is 601).
