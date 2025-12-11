/**
 * Script Utilities for Flux Blockchain
 *
 * Provides base58check decoding and script reconstruction for standard script types.
 * This allows us to skip storing script_pubkey for 95% of UTXOs (P2PKH, P2SH),
 * saving ~12GB of storage.
 */

import * as crypto from 'crypto';

// Base58 alphabet (Bitcoin/ZCash standard)
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

// Pre-compute alphabet index map for faster decoding
const BASE58_MAP: Record<string, number> = {};
for (let i = 0; i < BASE58_ALPHABET.length; i++) {
  BASE58_MAP[BASE58_ALPHABET[i]] = i;
}

// Flux mainnet address version bytes (2-byte prefix)
const FLUX_MAINNET_P2PKH = 0x1cb8; // t1... addresses
const FLUX_MAINNET_P2SH = 0x1cbd;  // t3... addresses

// Flux testnet address version bytes
const FLUX_TESTNET_P2PKH = 0x1d25; // tm... addresses
const FLUX_TESTNET_P2SH = 0x1cba;  // t2... addresses

/**
 * Standard script types that can be reconstructed from address
 */
export const RECONSTRUCTABLE_SCRIPT_TYPES = new Set([
  'pubkeyhash',
  'scripthash',
]);

/**
 * Check if a script type is standard and can be reconstructed from address
 */
export function isReconstructableScriptType(scriptType: string): boolean {
  return RECONSTRUCTABLE_SCRIPT_TYPES.has(scriptType);
}

/**
 * Decode a base58-encoded string to bytes
 * Uses the standard Bitcoin/ZCash base58 alphabet
 */
function base58Decode(str: string): Buffer {
  if (str.length === 0) {
    throw new Error('Empty base58 string');
  }

  const bytes: number[] = [];

  for (const char of str) {
    const value = BASE58_MAP[char];
    if (value === undefined) {
      throw new Error(`Invalid base58 character: ${char}`);
    }

    // Multiply existing bytes by 58 and add new digit
    let carry = value;
    for (let i = bytes.length - 1; i >= 0; i--) {
      const temp = bytes[i] * 58 + carry;
      bytes[i] = temp % 256;
      carry = Math.floor(temp / 256);
    }

    while (carry > 0) {
      bytes.unshift(carry % 256);
      carry = Math.floor(carry / 256);
    }
  }

  // Handle leading '1' characters (represent leading zero bytes)
  for (const char of str) {
    if (char !== '1') break;
    bytes.unshift(0);
  }

  return Buffer.from(bytes);
}

/**
 * Double SHA256 hash (used for checksum verification)
 */
function doubleSha256(data: Buffer): Buffer {
  const first = crypto.createHash('sha256').update(data).digest();
  return crypto.createHash('sha256').update(first).digest();
}

/**
 * Decoded Flux address information
 */
export interface DecodedFluxAddress {
  hash160: string;      // 20-byte hash in hex
  type: 'p2pkh' | 'p2sh';
  network: 'mainnet' | 'testnet';
}

/**
 * Decode a Flux address to extract the hash160 and determine script type
 *
 * Flux addresses are base58check encoded:
 * - 2 bytes: version
 * - 20 bytes: hash160
 * - 4 bytes: checksum (first 4 bytes of double SHA256)
 *
 * @param address - Flux address string (t1..., t3..., tm..., t2...)
 * @returns Decoded address info or null if invalid/unsupported
 */
export function decodeFluxAddress(address: string): DecodedFluxAddress | null {
  try {
    // Basic validation
    if (!address || address.length < 26 || address.length > 36) {
      return null;
    }

    // Decode base58
    const decoded = base58Decode(address);

    // Flux addresses are exactly 26 bytes: 2 version + 20 hash + 4 checksum
    if (decoded.length !== 26) {
      return null;
    }

    // Split into components
    const payload = decoded.slice(0, 22); // version + hash
    const checksum = decoded.slice(22);   // 4 byte checksum

    // Verify checksum
    const calculatedChecksum = doubleSha256(payload).slice(0, 4);
    if (!checksum.equals(calculatedChecksum)) {
      return null;
    }

    // Extract version (2 bytes, big-endian for Flux)
    const version = (decoded[0] << 8) | decoded[1];

    // Extract hash160 (20 bytes)
    const hash160 = decoded.slice(2, 22).toString('hex');

    // Determine type based on version
    switch (version) {
      case FLUX_MAINNET_P2PKH:
        return { hash160, type: 'p2pkh', network: 'mainnet' };
      case FLUX_MAINNET_P2SH:
        return { hash160, type: 'p2sh', network: 'mainnet' };
      case FLUX_TESTNET_P2PKH:
        return { hash160, type: 'p2pkh', network: 'testnet' };
      case FLUX_TESTNET_P2SH:
        return { hash160, type: 'p2sh', network: 'testnet' };
      default:
        return null; // Unknown version
    }
  } catch {
    return null;
  }
}

/**
 * Reconstruct the scriptPubKey hex from script type and address
 *
 * For standard scripts:
 * - P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
 *          76a914{hash160}88ac
 * - P2SH:  OP_HASH160 <20 bytes> OP_EQUAL
 *          a914{hash160}87
 *
 * @param scriptType - The script type ('pubkeyhash', 'scripthash', etc.)
 * @param address - The Flux address
 * @returns The reconstructed scriptPubKey hex, or null if cannot be reconstructed
 */
export function reconstructScriptPubkey(scriptType: string, address: string): string | null {
  // Can only reconstruct standard types
  if (!isReconstructableScriptType(scriptType)) {
    return null;
  }

  // Skip non-standard addresses
  if (!address || address === 'SHIELDED_OR_NONSTANDARD') {
    return null;
  }

  const decoded = decodeFluxAddress(address);
  if (!decoded) {
    return null;
  }

  // Verify the script type matches the address type
  if (scriptType === 'pubkeyhash' && decoded.type === 'p2pkh') {
    // P2PKH script:
    // OP_DUP (76) OP_HASH160 (a9) PUSH_20 (14) <hash160> OP_EQUALVERIFY (88) OP_CHECKSIG (ac)
    return `76a914${decoded.hash160}88ac`;
  }

  if (scriptType === 'scripthash' && decoded.type === 'p2sh') {
    // P2SH script:
    // OP_HASH160 (a9) PUSH_20 (14) <hash160> OP_EQUAL (87)
    return `a914${decoded.hash160}87`;
  }

  // Type mismatch - shouldn't happen with valid data
  return null;
}

/**
 * Get the scriptPubKey for a UTXO, reconstructing if necessary
 * This is the main function used by the API when returning UTXOs
 *
 * @param storedScript - The script_pubkey from the database (may be null)
 * @param scriptType - The script_type from the database
 * @param address - The address from the database
 * @returns The scriptPubKey hex string
 */
export function getScriptPubkey(
  storedScript: string | null,
  scriptType: string,
  address: string
): string | null {
  // If we have a stored script, use it
  if (storedScript) {
    return storedScript;
  }

  // Try to reconstruct for standard types
  return reconstructScriptPubkey(scriptType, address);
}
