/**
 * Block Hex Parser
 *
 * Parses raw block hex to extract individual transaction hex data
 * This is needed because FluxNode transactions don't have hex in getblock response
 */

import { logger } from '../utils/logger';

export interface ParsedTransaction {
  txid: string;
  hex: string;
  offset: number;
  length: number;
}

export interface ScannedTransaction extends ParsedTransaction {
  index: number;
  version: number;
  fluxNodeType?: number;
}

/**
 * Extract any transaction from raw block hex by txid
 * Scans through all transactions until the target txid is found
 * @param blockHeight - Optional block height to determine if Sapling fields are present (Sapling activated at block 250000)
 */
export function extractTransactionFromBlock(blockHex: string, targetTxid: string, blockHeight?: number): string | null {
  try {
    const buffer = Buffer.from(blockHex, 'hex');
    let offset = 0;

    // Parse block header (Flux uses extended PON header when version >= 100)
    ensureAvailable(buffer, offset, 4, 'block version');
    const blockVersion = buffer.readUInt32LE(offset);
    offset += 4; // version

    // Standard header fields (always present)
    ensureAvailable(buffer, offset, 32 + 32, 'prev hash and merkle root');
    offset += 32; // prev block hash
    offset += 32; // merkle root

    // hashReserved / hashFinalSaplingRoot (always 32 bytes, ALL blocks have this)
    // Pre-Sapling: called hashReserved, usually zeros
    // Post-Sapling (>= block 250000): called hashFinalSaplingRoot, contains Sapling tree root
    const SAPLING_ACTIVATION_HEIGHT = 250000;
    const hasSaplingRoot = blockHeight !== undefined && blockHeight >= SAPLING_ACTIVATION_HEIGHT;

    ensureAvailable(buffer, offset, 32, 'hashReserved/hashFinalSaplingRoot');
    offset += 32; // hashReserved (pre-Sapling) or hashFinalSaplingRoot (post-Sapling)

    ensureAvailable(buffer, offset, 4 + 4, 'nTime and nBits');
    offset += 4;  // nTime
    offset += 4;  // nBits

    if (blockVersion >= 100) {
      // Proof-of-Node header extension
      ensureAvailable(buffer, offset, 32 + 4, 'nodesCollateral');
      offset += 32; // nodesCollateral hash
      offset += 4;  // nodesCollateral index

      const { value: sigLength, size: sigVarSize } = readVarInt(buffer, offset);
      ensureAvailable(buffer, offset, sigVarSize + sigLength, 'block signature');
      offset += sigVarSize + sigLength; // block signature
    } else {
      // Proof-of-Work header extension
      ensureAvailable(buffer, offset, 32, 'block nonce');
      offset += 32; // nNonce

      const { value: solutionLength, size: solVarSize } = readVarInt(buffer, offset);

      // Flux used different Equihash variants over time:
      // Block 0-125110: Equihash(200,9) ~1344 bytes
      // Block 125111-372499: Equihash(144,5) ~100 bytes
      // Block 372500+: ZelHash (125,4) ~168 bytes
      let expectedMaxSize = 2000; // Conservative max
      if (blockHeight !== undefined) {
        if (blockHeight <= 125110) {
          expectedMaxSize = 2000; // Equihash(200,9)
        } else if (blockHeight < 372500) {
          expectedMaxSize = 200; // Equihash(144,5)
        } else {
          expectedMaxSize = 300; // ZelHash(125,4)
        }
      }

      // Sanity check: If we read something massive, offset is wrong
      if (solutionLength > expectedMaxSize) {
        const debugBytes = buffer.slice(offset, Math.min(offset + 20, buffer.length)).toString('hex');
        throw new Error(`Invalid Equihash solution length ${solutionLength} at offset ${offset}. Expected <${expectedMaxSize} bytes. Block structure mismatch. BlockHeight=${blockHeight}, hasSaplingRoot=${hasSaplingRoot}, blockVersion=${blockVersion}, bytes at offset: ${debugBytes}`);
      }

      ensureAvailable(buffer, offset, solVarSize + solutionLength, 'equihash solution');
      offset += solVarSize + solutionLength; // Equihash solution
    }

    // Read transaction count
    const { value: txCount, size: txCountSize } = readVarInt(buffer, offset);
    offset += txCountSize;

    logger.debug('Scanning block for transaction', {
      targetTxid,
      blockSize: buffer.length,
      txCount,
      startOffset: offset,
      blockVersion
    });

    // Scan through each transaction
    for (let txIdx = 0; txIdx < txCount; txIdx++) {
      if (offset + 4 > buffer.length) {
        logger.warn('Reached end of block buffer while scanning transactions', {
          targetTxid,
          txIdx,
          currentOffset: offset,
          bufferLength: buffer.length
        });
        break;
      }

      const txStart = offset;
      ensureAvailable(buffer, offset, 4, 'transaction version');
      const versionRaw = buffer.readUInt32LE(offset);
      const txVersion = versionRaw & 0x7fffffff;

      // Check if this is a FluxNode transaction
      if (txVersion === 3 || txVersion === 5 || txVersion === 6) {
        const nType = buffer.readUInt8(offset + 4);

        if (nType === 2 || nType === 4) {
          // FluxNode transaction - use special parsing
          try {
            const newOffset = parseFluxNodeTransaction(buffer, offset, txStart);
            const txBytes = buffer.slice(txStart, newOffset);
            const txid = calculateFluxNodeTxid(buffer, txStart, txVersion, nType);

            if (txid === targetTxid) {
              const txHex = txBytes.toString('hex');
              logger.debug('Successfully extracted FluxNode transaction from block', {
                txid,
                length: newOffset - txStart,
                offset: txStart,
                txIndex: txIdx,
                version: txVersion,
                nType
              });
              return txHex;
            }

            offset = newOffset;
            continue;
          } catch (err) {
            logger.debug('Failed to parse FluxNode transaction, skipping', {
              offset,
              txIndex: txIdx,
              version: txVersion,
              nType,
              error: (err as Error).message
            });
            offset = Math.min(txStart + 200, buffer.length);
            continue;
          }
        }
      }

      // Regular transaction - parse to get exact length
      try {
        const newOffset = parseStandardTransaction(buffer, offset);
        const txBytes = buffer.slice(txStart, newOffset);
        const txid = calculateTxid(txBytes);

        if (txid === targetTxid) {
          const txHex = txBytes.toString('hex');
          logger.debug('Successfully extracted transaction from block', {
            txid,
            length: newOffset - txStart,
            offset: txStart,
            txIndex: txIdx,
            version: txVersion
          });
          return txHex;
        }

        offset = newOffset;
        logger.debug('Successfully parsed transaction in block scan', {
          txid,
          txIndex: txIdx,
          txStart,
          txEnd: newOffset,
          txLength: newOffset - txStart,
          version: txVersion,
          blockHeight
        });
      } catch (err) {
        logger.warn('Failed to parse transaction in block scan', {
          offset: txStart,
          txIndex: txIdx,
          version: txVersion,
          blockHeight: blockHeight,
          bufferLength: buffer.length,
          error: (err as Error).message,
          stack: (err as Error).stack
        });
        // Can't reliably skip past failed transaction, break out of loop
        break;
      }
    }

    logger.warn('Transaction not found in block after scanning all transactions', {
      targetTxid,
      blockSize: buffer.length,
      txCount
    });
    return null;
  } catch (error: any) {
    logger.error('Failed to extract transaction from block', {
      error: error.message,
      targetTxid,
      stack: error.stack
    });
    return null;
  }
}

/**
 * Scan all transactions in a block and return their parsed metadata.
 * Useful for diagnostics and validating parser offsets.
 * @param blockHeight - Optional block height to determine if Sapling fields are present
 */
export function scanBlockTransactions(blockHex: string, blockHeight?: number): ScannedTransaction[] {
  const results: ScannedTransaction[] = [];

  const buffer = Buffer.from(blockHex, 'hex');
  let offset = 0;
  const traceIndexEnv = process.env.PARSER_TRACE_INDEX;
  const traceIndex = traceIndexEnv !== undefined ? Number(traceIndexEnv) : null;

  ensureAvailable(buffer, offset, 4, 'block version');
  const blockVersion = buffer.readUInt32LE(offset);
  offset += 4;

  // Standard header fields (always present)
  ensureAvailable(buffer, offset, 32 + 32, 'prev hash and merkle root');
  offset += 32; // prev block hash
  offset += 32; // merkle root

  // hashReserved / hashFinalSaplingRoot (always 32 bytes, ALL blocks have this)
  // Pre-Sapling: called hashReserved, usually zeros
  // Post-Sapling (>= block 250000): called hashFinalSaplingRoot, contains Sapling tree root
  const SAPLING_ACTIVATION_HEIGHT = 250000;
  const hasSaplingRoot = blockHeight !== undefined && blockHeight >= SAPLING_ACTIVATION_HEIGHT;

  ensureAvailable(buffer, offset, 32, 'hashReserved/hashFinalSaplingRoot');
  offset += 32; // hashReserved (pre-Sapling) or hashFinalSaplingRoot (post-Sapling)

  ensureAvailable(buffer, offset, 4 + 4, 'nTime and nBits');
  offset += 4;  // nTime
  offset += 4;  // nBits

  if (blockVersion >= 100) {
    ensureAvailable(buffer, offset, 32 + 4, 'nodesCollateral');
    offset += 32;
    offset += 4;

    const { value: sigLength, size: sigVarSize } = readVarInt(buffer, offset);
    ensureAvailable(buffer, offset, sigVarSize + sigLength, 'block signature');
    offset += sigVarSize + sigLength;
  } else {
    ensureAvailable(buffer, offset, 32, 'block nonce');
    offset += 32;

    const { value: solutionLength, size: solVarSize } = readVarInt(buffer, offset);
    ensureAvailable(buffer, offset, solVarSize + solutionLength, 'equihash solution');
    offset += solVarSize + solutionLength;
  }

  const { value: txCount, size: txCountSize } = readVarInt(buffer, offset);
  offset += txCountSize;

  for (let txIdx = 0; txIdx < txCount; txIdx++) {
    try {
      const txStart = offset;
      ensureAvailable(buffer, offset, 4, 'transaction version');
      const versionRaw = buffer.readUInt32LE(offset);
      const txVersion = versionRaw & 0x7fffffff;

      const maybeFluxNode =
        txVersion === 3 ||
        txVersion === 5 ||
        txVersion === 6;

      if (maybeFluxNode) {
        const nType = buffer.readUInt8(offset + 4);
        if (nType === 2 || nType === 4) {
          const newOffset = parseFluxNodeTransaction(buffer, offset, txStart);
          const txBytes = buffer.slice(txStart, newOffset);
          const txid = calculateFluxNodeTxid(buffer, txStart, txVersion, nType);
          results.push({
            txid,
            hex: txBytes.toString('hex'),
            offset: txStart,
            length: newOffset - txStart,
            index: txIdx,
            version: txVersion,
            fluxNodeType: nType,
          });
          offset = newOffset;
          continue;
        }
      }

      const traceStages: Array<{ stage: string; offset: number; info?: string }> | null =
        traceIndex !== null && traceIndex === txIdx ? [] : null;
      const traceFn = traceStages
        ? (stage: string, stageOffset: number, info?: string) => traceStages.push({ stage, offset: stageOffset, info })
        : undefined;

      const newOffset = parseStandardTransaction(buffer, offset, traceFn);
      const txBytes = buffer.slice(txStart, newOffset);
      const txid = calculateTxid(txBytes);

      results.push({
        txid,
        hex: txBytes.toString('hex'),
        offset: txStart,
        length: newOffset - txStart,
        index: txIdx,
        version: txVersion,
      });

      if (traceStages) {
        console.log(`[TRACE] Transaction ${txIdx} (${txid}) stages:`);
        traceStages.forEach((stage) => {
          const infoPart = stage.info !== undefined ? ` value=${stage.info}` : '';
          console.log(`  - ${stage.stage.padEnd(16)} offset=${stage.offset}${infoPart}`);
        });
      }

      offset = newOffset;
    } catch (error: any) {
      throw new Error(`Failed to parse transaction at index ${txIdx}, offset ${offset}: ${error.message}`);
    }
  }

  return results;
}

/**
 * Extract coinbase transaction (first transaction) from raw block hex
 * Coinbase is always the first transaction in a block
 * @param blockHeight - Optional block height to determine if Sapling fields are present
 */
export function extractCoinbaseTransaction(blockHex: string, blockHeight?: number): string | null {
  try {
    const buffer = Buffer.from(blockHex, 'hex');
    let offset = 0;

    // Parse block header (Flux uses extended PON header when version >= 100)
    ensureAvailable(buffer, offset, 4, 'block version');
    const blockVersion = buffer.readUInt32LE(offset);
    offset += 4; // version

    // Standard header fields (always present)
    ensureAvailable(buffer, offset, 32 + 32, 'prev hash and merkle root');
    offset += 32; // prev block hash
    offset += 32; // merkle root

    // hashReserved / hashFinalSaplingRoot (always 32 bytes, ALL blocks have this)
    const SAPLING_ACTIVATION_HEIGHT = 250000;
    const hasSaplingRoot = blockHeight !== undefined && blockHeight >= SAPLING_ACTIVATION_HEIGHT;

    ensureAvailable(buffer, offset, 32, 'hashReserved/hashFinalSaplingRoot');
    offset += 32; // hashReserved (pre-Sapling) or hashFinalSaplingRoot (post-Sapling)

    ensureAvailable(buffer, offset, 4 + 4, 'nTime and nBits');
    offset += 4;  // nTime
    offset += 4;  // nBits

    if (blockVersion >= 100) {
      // Proof-of-Node header extension
      ensureAvailable(buffer, offset, 32 + 4, 'nodesCollateral');
      offset += 32; // nodesCollateral hash
      offset += 4;  // nodesCollateral index

      const { value: sigLength, size: sigVarSize } = readVarInt(buffer, offset);
      ensureAvailable(buffer, offset, sigVarSize + sigLength, 'block signature');
      offset += sigVarSize + sigLength; // block signature
    } else {
      // Proof-of-Work header extension
      ensureAvailable(buffer, offset, 32, 'block nonce');
      offset += 32; // nNonce

      const { value: solutionLength, size: solVarSize } = readVarInt(buffer, offset);

      // Flux used different Equihash variants over time:
      // Block 0-125110: Equihash(200,9) ~1344 bytes
      // Block 125111-372499: Equihash(144,5) ~100 bytes
      // Block 372500+: ZelHash (125,4) ~168 bytes
      let expectedMaxSize = 2000; // Conservative max
      if (blockHeight !== undefined) {
        if (blockHeight <= 125110) {
          expectedMaxSize = 2000; // Equihash(200,9)
        } else if (blockHeight < 372500) {
          expectedMaxSize = 200; // Equihash(144,5)
        } else {
          expectedMaxSize = 300; // ZelHash(125,4)
        }
      }

      // Sanity check: If we read something massive, offset is wrong
      if (solutionLength > expectedMaxSize) {
        const debugBytes = buffer.slice(offset, Math.min(offset + 20, buffer.length)).toString('hex');
        throw new Error(`Invalid Equihash solution length ${solutionLength} at offset ${offset}. Expected <${expectedMaxSize} bytes. Block structure mismatch. BlockHeight=${blockHeight}, hasSaplingRoot=${hasSaplingRoot}, blockVersion=${blockVersion}, bytes at offset: ${debugBytes}`);
      }

      ensureAvailable(buffer, offset, solVarSize + solutionLength, 'equihash solution');
      offset += solVarSize + solutionLength; // Equihash solution
    }

    // Read transaction count
    const { value: txCount, size: txCountSize } = readVarInt(buffer, offset);
    offset += txCountSize;

    if (txCount === 0) {
      logger.warn('Block has no transactions, cannot extract coinbase');
      return null;
    }

    // Coinbase is the first transaction - parse it
    const txStart = offset;
    try {
      const txEnd = parseStandardTransaction(buffer, offset);
      const coinbaseHex = buffer.slice(txStart, txEnd).toString('hex');

      logger.debug('Successfully extracted coinbase transaction from block', {
        length: txEnd - txStart,
        offset: txStart,
        blockVersion
      });

      return coinbaseHex;
    } catch (error: any) {
      logger.error('Failed to parse coinbase transaction from block', {
        error: error.message,
        offset,
        blockVersion
      });
      return null;
    }
  } catch (error: any) {
    logger.error('Failed to extract coinbase transaction from block', {
      error: error.message,
      stack: error.stack
    });
    return null;
  }
}

/**
 * Extract a specific FluxNode transaction from raw block hex
 * Scans for FluxNode transaction patterns and tries different lengths
 * until the txid matches
 * @param blockHeight - Optional block height to determine if Sapling fields are present
 */
export function extractFluxNodeTransaction(blockHex: string, targetTxid: string, blockHeight?: number): string | null {
  try {
    const buffer = Buffer.from(blockHex, 'hex');

    let offset = 0;

    // Parse block header (Flux uses extended PON header when version >= 100)
    ensureAvailable(buffer, offset, 4, 'block version');
    const blockVersion = buffer.readUInt32LE(offset);
    offset += 4; // version

    // Standard header fields (always present)
    ensureAvailable(buffer, offset, 32 + 32, 'prev hash and merkle root');
    offset += 32; // prev block hash
    offset += 32; // merkle root

    // hashReserved / hashFinalSaplingRoot (always 32 bytes, ALL blocks have this)
    const SAPLING_ACTIVATION_HEIGHT = 250000;
    const hasSaplingRoot = blockHeight !== undefined && blockHeight >= SAPLING_ACTIVATION_HEIGHT;

    ensureAvailable(buffer, offset, 32, 'hashReserved/hashFinalSaplingRoot');
    offset += 32; // hashReserved (pre-Sapling) or hashFinalSaplingRoot (post-Sapling)

    ensureAvailable(buffer, offset, 4 + 4, 'nTime and nBits');
    offset += 4;  // nTime
    offset += 4;  // nBits

    if (blockVersion >= 100) {
      // Proof-of-Node header extension
      ensureAvailable(buffer, offset, 32 + 4, 'nodesCollateral');
      offset += 32; // nodesCollateral hash
      offset += 4;  // nodesCollateral index

      const { value: sigLength, size: sigVarSize } = readVarInt(buffer, offset);
      ensureAvailable(buffer, offset, sigVarSize + sigLength, 'block signature');
      offset += sigVarSize + sigLength; // block signature
    } else {
      // Proof-of-Work header extension
      ensureAvailable(buffer, offset, 32, 'block nonce');
      offset += 32; // nNonce

      const { value: solutionLength, size: solVarSize } = readVarInt(buffer, offset);

      // Flux used different Equihash variants over time:
      // Block 0-125110: Equihash(200,9) ~1344 bytes
      // Block 125111-372499: Equihash(144,5) ~100 bytes
      // Block 372500+: ZelHash (125,4) ~168 bytes
      let expectedMaxSize = 2000; // Conservative max
      if (blockHeight !== undefined) {
        if (blockHeight <= 125110) {
          expectedMaxSize = 2000; // Equihash(200,9)
        } else if (blockHeight < 372500) {
          expectedMaxSize = 200; // Equihash(144,5)
        } else {
          expectedMaxSize = 300; // ZelHash(125,4)
        }
      }

      // Sanity check: If we read something massive, offset is wrong
      if (solutionLength > expectedMaxSize) {
        const debugBytes = buffer.slice(offset, Math.min(offset + 20, buffer.length)).toString('hex');
        throw new Error(`Invalid Equihash solution length ${solutionLength} at offset ${offset}. Expected <${expectedMaxSize} bytes. Block structure mismatch. BlockHeight=${blockHeight}, hasSaplingRoot=${hasSaplingRoot}, blockVersion=${blockVersion}, bytes at offset: ${debugBytes}`);
      }

      ensureAvailable(buffer, offset, solVarSize + solutionLength, 'equihash solution');
      offset += solVarSize + solutionLength; // Equihash solution
    }

    // Read transaction count
    const { value: txCount, size: txCountSize } = readVarInt(buffer, offset);
    offset += txCountSize;

    logger.debug('Scanning block for FluxNode transaction', {
      targetTxid,
      blockSize: buffer.length,
      txCount,
      startOffset: offset,
      blockVersion
    });

    // Scan through each transaction
    for (let txIdx = 0; txIdx < txCount; txIdx++) {
      if (offset + 4 > buffer.length) {
        logger.warn('Reached end of block buffer while scanning transactions', {
          targetTxid,
          txIdx,
          currentOffset: offset,
          bufferLength: buffer.length
        });
        break;
      }

      const txStart = offset;
      ensureAvailable(buffer, offset, 4, 'transaction version');
      const versionRaw = buffer.readUInt32LE(offset);
      const txVersion = versionRaw & 0x7fffffff;

      // Check if this is a FluxNode transaction
      if (txVersion === 3 || txVersion === 5 || txVersion === 6) {
        const nType = buffer.readUInt8(offset + 4);

        if (nType === 2 || nType === 4) {
          // This is a FluxNode transaction - use proper parsing
          try {
            const newOffset = parseFluxNodeTransaction(buffer, offset, txStart);

            let txid: string;
            const txBytes = buffer.slice(txStart, newOffset);
            txid = calculateFluxNodeTxid(buffer, txStart, txVersion, nType);

            if (txid === targetTxid) {
              // Found it!
              const txHex = txBytes.toString('hex');
              logger.debug('Successfully extracted FluxNode transaction from block', {
                txid,
                length: newOffset - txStart,
                offset: txStart,
                txIndex: txIdx,
                version: txVersion,
                nType
              });
              return txHex;
            }

            // Not the target txid, but successfully parsed - continue to next tx
            offset = newOffset;
            continue;
          } catch (err) {
            logger.error('Failed to parse FluxNode transaction while scanning block', {
              offset,
              txIndex: txIdx,
              version: txVersion,
              nType,
              error: (err as Error).message
            });
            // Skip ahead cautiously to avoid overrunning buffer
            offset = Math.min(txStart + 200, buffer.length);
            continue;
          }
        }
      }

      // Regular transaction - parse to get exact length
      try {
        offset = parseStandardTransaction(buffer, offset);
      } catch (err) {
        // If parsing fails, try to skip this transaction by searching for next version marker
        logger.debug('Failed to parse transaction, skipping', { offset, error: (err as Error).message });
        offset = Math.min(txStart + 250, buffer.length); // Skip ahead cautiously
      }
    }

    logger.warn('FluxNode transaction not found in block after scanning all transactions', {
      targetTxid,
      blockSize: buffer.length,
      txCount
    });
    return null;
  } catch (error: any) {
    logger.error('Failed to extract FluxNode transaction from block', {
      error: error.message,
      targetTxid,
      stack: error.stack
    });
    return null;
  }
}

/**
 * Legacy function for compatibility
 */
export function parseBlockTransactions(blockHex: string): ParsedTransaction[] {
  // This function is deprecated - use extractFluxNodeTransaction instead
  return [];
}

/**
 * Parse a standard transaction (returns new offset)
 * Handles v1, v2, v3 (Overwinter), and v4 (Sapling) transactions
 */
function parseStandardTransaction(
  buffer: Buffer,
  offset: number,
  trace?: (stage: string, offset: number, info?: string) => void
): number {
  trace?.('start', offset);

  const version = buffer.readUInt32LE(offset);
  offset += 4;
  trace?.('version', offset, `0x${version.toString(16)}`);

  const isOverwintered = (version & 0x80000000) !== 0;
  const versionNumber = version & 0x7fffffff;

  let versionGroupId = 0;
  if (isOverwintered) {
    versionGroupId = buffer.readUInt32LE(offset);
    offset += 4;
    trace?.('versionGroupId', offset, `0x${versionGroupId.toString(16)}`);
  }

  const isSaplingV4 = isOverwintered && versionNumber === 4 && versionGroupId === 0x892f2085;

  const { value: vinCount, size: vinVarIntSize } = readVarInt(buffer, offset);
  offset += vinVarIntSize;
  trace?.('vinCount', offset, String(vinCount));

  // Allow up to 100,000 inputs for large consolidation transactions
  // Flux 2MB blocks can theoretically fit ~48,000 minimal inputs or ~13,500 typical P2PKH inputs
  // Set limit high to handle edge cases while still catching parser offset errors
  if (vinCount > 100000) {
    throw new Error(`Unreasonable vinCount: ${vinCount} at offset ${offset - vinVarIntSize}. Likely parsing from wrong offset. Buffer length: ${buffer.length}`);
  }

  for (let i = 0; i < vinCount; i++) {
    ensureAvailable(buffer, offset, 36, `vin ${i} txid+vout`);
    offset += 32; // prev txid
    offset += 4;  // prev vout

    const { value: scriptLen, size: scriptVarIntSize } = readVarInt(buffer, offset);
    ensureAvailable(buffer, offset, scriptVarIntSize + scriptLen + 4, `vin ${i} script+sequence`);
    offset += scriptVarIntSize;
    offset += scriptLen;

    offset += 4;  // sequence
  }
  trace?.('vin', offset);

  const { value: voutCount, size: voutVarIntSize } = readVarInt(buffer, offset);
  offset += voutVarIntSize;
  trace?.('voutCount', offset, String(voutCount));

  // Allow up to 100,000 outputs for large distribution transactions (e.g., mining payouts)
  // Flux 2MB blocks can theoretically fit ~58,000 minimal outputs or fewer typical outputs
  // Set limit high to handle edge cases while still catching parser offset errors
  if (voutCount > 100000) {
    throw new Error(`Unreasonable voutCount: ${voutCount} at offset ${offset - voutVarIntSize}. Likely parsing from wrong offset. Buffer length: ${buffer.length}`);
  }

  for (let i = 0; i < voutCount; i++) {
    ensureAvailable(buffer, offset, 8, `vout ${i} value`);
    offset += 8; // value
    const { value: scriptLen, size: scriptVarIntSize } = readVarInt(buffer, offset);
    ensureAvailable(buffer, offset, scriptVarIntSize + scriptLen, `vout ${i} script`);
    offset += scriptVarIntSize;
    offset += scriptLen;
  }
  trace?.('vout', offset);

  offset += 4; // locktime
  trace?.('locktime', offset);

  if (isOverwintered && versionNumber >= 3) {
    offset += 4; // expiry height
    trace?.('expiryHeight', offset);
  }

  let nShieldedSpend = 0;
  let nShieldedOutput = 0;

  // All Sapling v4 transactions (version 4 with versionGroupId 0x892f2085) have Sapling fields
  // These fields come BEFORE any JoinSplit data (if present)
  // Structure: valueBalance (8) + vShieldedSpend[] + vShieldedOutput[] + [bindingSig if spends/outputs exist] + [JoinSplits if any]
  if (isSaplingV4) {
    // Sapling v4 transactions with Sapling spends/outputs include these fields
    // valueBalance (8 bytes)
    ensureAvailable(buffer, offset, 8, 'valueBalance');
    const valueBalanceBytes = buffer.slice(offset, offset + 8);
    offset += 8;
    trace?.('valueBalance', offset, valueBalanceBytes.toString('hex'));

    // vShieldedSpend count (VarInt)
    const spendResult = readVarInt(buffer, offset);
    nShieldedSpend = spendResult.value;
    offset += spendResult.size;
    trace?.('saplingSpendCount', offset, String(nShieldedSpend));

    // Flux 2MB blocks can hold ~5,200 shielded spends (384 bytes each)
    // Large consolidation transactions may have 1000+ spends
    if (nShieldedSpend > 6000) {
      throw new Error(`Unreasonable nShieldedSpend count: ${nShieldedSpend}`);
    }

    // Parse each shielded spend (384 bytes each)
    for (let i = 0; i < nShieldedSpend; i++) {
      ensureAvailable(buffer, offset, 32, `shielded spend ${i} cv`);
      offset += 32; // cv
      ensureAvailable(buffer, offset, 32, `shielded spend ${i} anchor`);
      offset += 32; // anchor
      ensureAvailable(buffer, offset, 32, `shielded spend ${i} nullifier`);
      offset += 32; // nullifier
      ensureAvailable(buffer, offset, 32, `shielded spend ${i} rk`);
      offset += 32; // rk
      ensureAvailable(buffer, offset, 192, `shielded spend ${i} zkproof`);
      offset += 192; // zkproof (Groth16)
      ensureAvailable(buffer, offset, 64, `shielded spend ${i} spendAuthSig`);
      offset += 64; // spendAuthSig
    }
    if (nShieldedSpend > 0) {
      trace?.('saplingSpends', offset);
    }

    // vShieldedOutput count (VarInt)
    const outputResult = readVarInt(buffer, offset);
    nShieldedOutput = outputResult.value;
    offset += outputResult.size;
    trace?.('saplingOutputCount', offset, String(nShieldedOutput));

    // Flux 2MB blocks can hold ~2,100 shielded outputs (948 bytes each)
    // Large distribution transactions may have many outputs
    if (nShieldedOutput > 3000) {
      throw new Error(`Unreasonable nShieldedOutput count: ${nShieldedOutput}`);
    }

    // Parse each shielded output (948 bytes each)
    for (let i = 0; i < nShieldedOutput; i++) {
      ensureAvailable(buffer, offset, 32, `shielded output ${i} cv`);
      offset += 32; // cv
      ensureAvailable(buffer, offset, 32, `shielded output ${i} cmu`);
      offset += 32; // cmu
      ensureAvailable(buffer, offset, 32, `shielded output ${i} ephemeralKey`);
      offset += 32; // ephemeralKey
      ensureAvailable(buffer, offset, 192, `shielded output ${i} zkproof`);
      offset += 192; // zkproof (Groth16)
      ensureAvailable(buffer, offset, 580, `shielded output ${i} encCiphertext`);
      offset += 580; // encCiphertext
      ensureAvailable(buffer, offset, 80, `shielded output ${i} outCiphertext`);
      offset += 80; // outCiphertext
    }
    if (nShieldedOutput > 0) {
      trace?.('saplingOutputs', offset);
    }
  }

  const needsBindingSig = isSaplingV4 && (nShieldedSpend > 0 || nShieldedOutput > 0);

  if (versionNumber >= 2) {
    if (offset + 1 > buffer.length) {
      if (needsBindingSig) {
        throw new Error(`Expected JoinSplit count and bindingSig for v4 Sapling transaction at offset ${offset}`);
      }
      return offset;
    }

    const { value: nJoinSplit, size: joinSplitVarIntSize } = readVarInt(buffer, offset);
    offset += joinSplitVarIntSize;
    trace?.('joinSplitCount', offset, String(nJoinSplit));

    if (nJoinSplit > 0) {
      if (nJoinSplit > 100) {
        throw new Error(`Unreasonable nJoinSplit count: ${nJoinSplit} at offset ${offset - joinSplitVarIntSize}`);
      }

      // FLUX-SPECIFIC JoinSplit ciphertext sizes (discovered via empirical testing):
      // v2 Sprout (blocks 0-249,999): 601 bytes (standard Zcash) - VERIFIED WORKING
      // v4 Sapling (blocks 250,000+): 549 bytes (Flux modification) - VERIFIED WORKING
      // Note: Sapling ShieldedOutput ciphertexts use different structure (580+80 bytes)
      const ciphertextSize = isSaplingV4 ? 549 : 601;

      for (let i = 0; i < nJoinSplit; i++) {
        ensureAvailable(buffer, offset, 8, `JoinSplit ${i} vpub_old`);
        offset += 8;
        ensureAvailable(buffer, offset, 8, `JoinSplit ${i} vpub_new`);
        offset += 8;
        ensureAvailable(buffer, offset, 32, `JoinSplit ${i} anchor`);
        offset += 32;
        ensureAvailable(buffer, offset, 64, `JoinSplit ${i} nullifiers`);
        offset += 64;
        ensureAvailable(buffer, offset, 64, `JoinSplit ${i} commitments`);
        offset += 64;
        ensureAvailable(buffer, offset, 32, `JoinSplit ${i} ephemeralKey`);
        offset += 32;
        ensureAvailable(buffer, offset, 32, `JoinSplit ${i} randomSeed`);
        offset += 32;
        ensureAvailable(buffer, offset, 64, `JoinSplit ${i} macs`);
        offset += 64;
        ensureAvailable(buffer, offset, 296, `JoinSplit ${i} proof`);
        offset += 296;
        ensureAvailable(buffer, offset, ciphertextSize, `JoinSplit ${i} ciphertext 0`);
        offset += ciphertextSize;
        ensureAvailable(buffer, offset, ciphertextSize, `JoinSplit ${i} ciphertext 1`);
        offset += ciphertextSize;
      }
      trace?.('joinSplits', offset);

      ensureAvailable(buffer, offset, 32, 'joinSplitPubKey');
      offset += 32;
      ensureAvailable(buffer, offset, 64, 'joinSplitSig');
      offset += 64;
    }
  }

  if (needsBindingSig) {
    ensureAvailable(buffer, offset, 64, 'bindingSig');
    offset += 64;
    trace?.('bindingSig', offset);
  }

  return offset;
}
function parseFluxNodeTransaction(buffer: Buffer, offset: number, txStart: number): number {
  const startOffset = offset;

  // Read version (4 bytes)
  ensureAvailable(buffer, offset, 4, 'FluxNode version');
  const version = buffer.readUInt32LE(offset);
  offset += 4;

  // Read nType (1 byte) - NOT 2 bytes!
  ensureAvailable(buffer, offset, 1, 'FluxNode nType');
  const nType = buffer.readUInt8(offset);
  offset += 1;

  logger.debug('Parsing FluxNode transaction', {
    version,
    nType,
    startOffset,
    currentOffset: offset,
    bufferLength: buffer.length,
    remaining: buffer.length - offset
  });

  // FluxNode confirmation transactions (type 4)
  // Structure from FluxD source code:
  // collateralIn(36) | sigTime(4) | benchmarkTier(1) | benchmarkSigTime(4) | nUpdateType(1) | ip(varstring) | sig(varbytes) | benchmarkSig(varbytes)
  if (nType === 4) {
    // Collateral output (COutPoint: 32 byte hash + 4 byte index)
    ensureAvailable(buffer, offset, 32 + 4, 'FluxNode collateral outpoint');
    offset += 32 + 4;

    // sigTime (uint32_t - 4 bytes)
    ensureAvailable(buffer, offset, 4, 'FluxNode sigTime');
    offset += 4;

    // benchmarkTier (int8_t - 1 byte)
    ensureAvailable(buffer, offset, 1, 'FluxNode benchmarkTier');
    offset += 1;

    // benchmarkSigTime (uint32_t - 4 bytes)
    ensureAvailable(buffer, offset, 4, 'FluxNode benchmarkSigTime');
    offset += 4;

    // nUpdateType (int8_t - 1 byte)
    ensureAvailable(buffer, offset, 1, 'FluxNode nUpdateType');
    offset += 1;

    // IP address (std::string - variable length with VarInt size prefix)
    offset = skipVarSlice(buffer, offset, 'FluxNode IP address');

    // sig (std::vector<unsigned char> - variable length with VarInt size prefix)
    offset = skipVarSlice(buffer, offset, 'FluxNode signature');

    // benchmarkSig (std::vector<unsigned char> - variable length with VarInt size prefix)
    offset = skipVarSlice(buffer, offset, 'FluxNode benchmark signature');

    logger.debug('Parsed FluxNode confirmation transaction', {
      version,
      nType,
      totalLength: offset - txStart
    });
  }
  // FluxNode start transactions (type 2)
  // Version 6 structure from FluxD source code:
  // nFluxTxVersion(4) | collateralIn(36) | collateralPubkey(33) | pubKey(33) | sigTime(4) | sig(varbytes)
  else if (nType === 2) {
    const FLUXNODE_INTERNAL_NORMAL_TX_VERSION = 1;
    const FLUXNODE_INTERNAL_P2SH_TX_VERSION = 2;
    const FLUXNODE_TX_TYPE_NORMAL_BIT = 0x01;
    const FLUXNODE_TX_TYPE_P2SH_BIT = 0x02;
    const FLUXNODE_TX_FEATURE_DELEGATES_BIT = 0x0100;

    let nFluxTxVersion: number | undefined;

    if (version === 6) {
      ensureAvailable(buffer, offset, 4, 'FluxNode nFluxTxVersion');
      nFluxTxVersion = buffer.readUInt32LE(offset);
      offset += 4;
    }

    ensureAvailable(buffer, offset, 32 + 4, 'FluxNode collateral outpoint');
    offset += 32 + 4;

    const treatAsP2SH =
      nFluxTxVersion !== undefined &&
      (
        nFluxTxVersion === FLUXNODE_INTERNAL_P2SH_TX_VERSION ||
        (nFluxTxVersion & FLUXNODE_TX_TYPE_P2SH_BIT) !== 0
      );

    const treatAsNormal =
      nFluxTxVersion === undefined ||
      nFluxTxVersion === FLUXNODE_INTERNAL_NORMAL_TX_VERSION ||
      ((nFluxTxVersion & FLUXNODE_TX_TYPE_NORMAL_BIT) !== 0 && (nFluxTxVersion & FLUXNODE_TX_TYPE_P2SH_BIT) === 0);

    if (treatAsP2SH) {
      offset = skipVarSlice(buffer, offset, 'FluxNode public key (P2SH)');
      offset = skipVarSlice(buffer, offset, 'FluxNode redeem script');
    } else if (treatAsNormal) {
      offset = skipVarSlice(buffer, offset, 'FluxNode collateral pubkey');
      offset = skipVarSlice(buffer, offset, 'FluxNode public key');
    } else {
      offset = skipVarSlice(buffer, offset, 'FluxNode collateral pubkey (fallback)');
      offset = skipVarSlice(buffer, offset, 'FluxNode public key (fallback)');
    }

    ensureAvailable(buffer, offset, 4, 'FluxNode sigTime');
    offset += 4;

    offset = skipVarSlice(buffer, offset, 'FluxNode signature');

    if (nFluxTxVersion !== undefined && (nFluxTxVersion & FLUXNODE_TX_FEATURE_DELEGATES_BIT) !== 0) {
      ensureAvailable(buffer, offset, 1, 'FluxNode delegate flag');
      const usingDelegates = buffer.readUInt8(offset);
      offset += 1;

      if (usingDelegates === 1) {
        const delegateCountInfo = readVarInt(buffer, offset);
        offset += delegateCountInfo.size;

        for (let i = 0; i < delegateCountInfo.value; i++) {
          offset = skipVarSlice(buffer, offset, `FluxNode delegate key ${i}`);
        }
      }
    }

    logger.debug('Parsed FluxNode start transaction', {
      version,
      nType,
      totalLength: offset - txStart
    });
  }
  // Unknown FluxNode transaction type
  else {
    throw new Error(`Unknown FluxNode transaction type: ${nType} (version ${version})`);
  }

  return offset;
}

/**
 * Read VarInt from buffer
 */
function readVarInt(buffer: Buffer, offset: number): { value: number; size: number } {
  ensureAvailable(buffer, offset, 1, 'varint');
  const first = buffer.readUInt8(offset);

  if (first < 0xfd) {
    return { value: first, size: 1 };
  } else if (first === 0xfd) {
    return { value: buffer.readUInt16LE(offset + 1), size: 3 };
  } else if (first === 0xfe) {
    return { value: buffer.readUInt32LE(offset + 1), size: 5 };
  } else {
    // 0xff - 64-bit value, but we'll use 32-bit for safety
    return { value: buffer.readUInt32LE(offset + 1), size: 9 };
  }
}

/**
 * Calculate transaction ID (double SHA256, reversed)
 */
function calculateTxid(txBuffer: Buffer): string {
  const crypto = require('crypto');
  const hash1 = crypto.createHash('sha256').update(txBuffer).digest();
  const hash2 = crypto.createHash('sha256').update(hash1).digest();
  return hash2.reverse().toString('hex');
}

/**
 * Calculate the txid for FluxNode transactions.
 *
 * Flux excludes signature vectors (and optional delegate payloads) when hashing
 * FluxNode start/confirmation transactions. This helper mirrors fluxd's
 * serialization with the SER_GETHASH flag so we can compare txids produced by
 * the daemon even when it refuses to return the full raw hex.
 */
function calculateFluxNodeTxid(buffer: Buffer, txStart: number, version: number, nType: number): string {
  const segments: Buffer[] = [];
  let cursor = txStart;

  const pushSegment = (length: number) => {
    segments.push(buffer.slice(cursor, cursor + length));
    cursor += length;
  };

  // Version (4 bytes) + nType (1 byte)
  pushSegment(4);
  pushSegment(1);

  if (nType === 2) {
    // Start transaction
    if (version === 6) {
      pushSegment(4); // nFluxTxVersion
      const nFluxTxVersion = segments[segments.length - 1].readUInt32LE(0);

      // Determine structure
      const FLUXNODE_INTERNAL_NORMAL_TX_VERSION = 1;
      const FLUXNODE_INTERNAL_P2SH_TX_VERSION = 2;
      const FLUXNODE_TX_TYPE_NORMAL_BIT = 0x01;
      const FLUXNODE_TX_TYPE_P2SH_BIT = 0x02;
      const FLUXNODE_TX_FEATURE_DELEGATES_BIT = 0x0100;

      const isNormalTx = nFluxTxVersion === FLUXNODE_INTERNAL_NORMAL_TX_VERSION ||
        ((nFluxTxVersion & FLUXNODE_TX_TYPE_NORMAL_BIT) !== 0);
      const isP2SHTx = nFluxTxVersion === FLUXNODE_INTERNAL_P2SH_TX_VERSION ||
        ((nFluxTxVersion & FLUXNODE_TX_TYPE_P2SH_BIT) !== 0);
      const hasDelegates = (nFluxTxVersion & FLUXNODE_TX_FEATURE_DELEGATES_BIT) !== 0;

      pushSegment(32 + 4); // collateralIn

      if (isP2SHTx) {
        // pubKey (varstring)
        const { value: pubKeyLen, size: pubKeySize } = readVarInt(buffer, cursor);
        pushSegment(pubKeySize + pubKeyLen);

        // redeem script (varstring)
        const { value: redeemLen, size: redeemSize } = readVarInt(buffer, cursor);
        pushSegment(redeemSize + redeemLen);
      } else if (isNormalTx) {
        // collateralPubkey (varstring)
        const { value: collateralLen, size: collateralSize } = readVarInt(buffer, cursor);
        pushSegment(collateralSize + collateralLen);

        // pubKey (varstring)
        const { value: pubKeyLen, size: pubKeySize } = readVarInt(buffer, cursor);
        pushSegment(pubKeySize + pubKeyLen);
      } else {
        // Fallback - treat as normal
        const { value: collateralLen, size: collateralSize } = readVarInt(buffer, cursor);
        pushSegment(collateralSize + collateralLen);
        const { value: pubKeyLen, size: pubKeySize } = readVarInt(buffer, cursor);
        pushSegment(pubKeySize + pubKeyLen);
      }

      pushSegment(4); // sigTime

      // Signature is excluded from txid
      const { value: sigLen, size: sigSize } = readVarInt(buffer, cursor);
      cursor += sigSize + sigLen;

      if (hasDelegates) {
        pushSegment(1); // fUsingDelegates flag
        const usingDelegates = buffer.readUInt8(cursor - 1) !== 0;
        if (usingDelegates) {
          const { value: delegateBytes, size: delegateSize } = readVarInt(buffer, cursor);
          pushSegment(delegateSize + delegateBytes);
        }
      }
    } else {
      // Version 5 start transaction
      pushSegment(32 + 4); // collateralIn

      const { value: collateralLen, size: collateralSize } = readVarInt(buffer, cursor);
      pushSegment(collateralSize + collateralLen);

      const { value: pubKeyLen, size: pubKeySize } = readVarInt(buffer, cursor);
      pushSegment(pubKeySize + pubKeyLen);

      pushSegment(4); // sigTime

      // Signature excluded
      const { value: sigLen, size: sigSize } = readVarInt(buffer, cursor);
      cursor += sigSize + sigLen;
    }
  } else if (nType === 4) {
    // Confirmation transaction
    pushSegment(32 + 4); // collateralIn
    pushSegment(4);      // sigTime
    pushSegment(1);      // benchmarkTier
    pushSegment(4);      // benchmarkSigTime
    pushSegment(1);      // nUpdateType

    // IP (varstring)
    const { value: ipLen, size: ipSize } = readVarInt(buffer, cursor);
    pushSegment(ipSize + ipLen);

    // sig (varstring) - excluded
    const { value: sigLen, size: sigSize } = readVarInt(buffer, cursor);
    cursor += sigSize + sigLen;

    // benchmarkSig (varstring) - excluded
    const { value: benchmarkLen, size: benchmarkSize } = readVarInt(buffer, cursor);
    cursor += benchmarkSize + benchmarkLen;
  } else {
    throw new Error(`Unknown FluxNode transaction type for txid calculation: ${nType}`);
  }

  const crypto = require('crypto');
  const hash1 = crypto.createHash('sha256').update(Buffer.concat(segments)).digest();
  const hash2 = crypto.createHash('sha256').update(hash1).digest();
  return hash2.reverse().toString('hex');
}

function skipVarSlice(buffer: Buffer, offset: number, field: string): number {
  const { value: length, size } = readVarInt(buffer, offset);
  offset += size;

  if (length === 0) {
    return offset;
  }

  ensureAvailable(buffer, offset, length, field);
  return offset + length;
}

function ensureAvailable(buffer: Buffer, offset: number, bytesNeeded: number, field: string): void {
  if (offset + bytesNeeded > buffer.length) {
    throw new Error(`Unexpected end of buffer while reading ${field} (need ${bytesNeeded} bytes at offset ${offset}, buffer length ${buffer.length})`);
  }
}

/**
 * Parse transaction hex to extract JoinSplit and Sapling shielded data
 * Returns minimal transaction data with shielded components populated
 * Returns values in SATOSHIS (bigint) to avoid floating-point precision loss
 */
export function parseTransactionShieldedData(txHex: string): {
  version: number;
  vjoinsplit?: Array<{ vpub_old: bigint; vpub_new: bigint }>;
  valueBalance?: bigint;
} {
  const buffer = Buffer.from(txHex, 'hex');
  let offset = 0;

  // Read version
  const version = buffer.readUInt32LE(offset);
  offset += 4;

  const isOverwintered = (version & 0x80000000) !== 0;
  const versionNumber = version & 0x7fffffff;

  let versionGroupId = 0;
  if (isOverwintered) {
    versionGroupId = buffer.readUInt32LE(offset);
    offset += 4;
  }

  const isSaplingV4 = isOverwintered && versionNumber === 4 && versionGroupId === 0x892f2085;

  // Skip inputs
  const { value: vinCount, size: vinVarIntSize } = readVarInt(buffer, offset);
  offset += vinVarIntSize;

  for (let i = 0; i < vinCount; i++) {
    offset += 32; // prev txid
    offset += 4;  // prev vout
    const { value: scriptLen, size: scriptVarIntSize } = readVarInt(buffer, offset);
    offset += scriptVarIntSize + scriptLen;
    offset += 4;  // sequence
  }

  // Skip outputs
  const { value: voutCount, size: voutVarIntSize } = readVarInt(buffer, offset);
  offset += voutVarIntSize;

  for (let i = 0; i < voutCount; i++) {
    offset += 8; // value
    const { value: scriptLen, size: scriptVarIntSize } = readVarInt(buffer, offset);
    offset += scriptVarIntSize + scriptLen;
  }

  offset += 4; // locktime

  if (isOverwintered && versionNumber >= 3) {
    offset += 4; // expiry height
  }

  let valueBalance: bigint | undefined;
  const vjoinsplit: Array<{ vpub_old: bigint; vpub_new: bigint }> = [];

  // Sapling v4 transaction handling
  if (isSaplingV4) {
    // All Sapling v4 transactions have valueBalance field (8 bytes)
    // followed by vShieldedSpend count (varint) and vShieldedOutput count (varint)
    // Previous heuristic was wrong: checking firstByte === 0x00 fails for large valueBalance
    // A better check: ensure we have enough bytes for valueBalance + at least 2 varints
    const hasSaplingFields = offset + 10 <= buffer.length;

    if (hasSaplingFields) {
      // Read valueBalance (8 bytes, signed) - keep as satoshis (bigint)
      const valueBalanceBytes = buffer.readBigInt64LE(offset);
      valueBalance = valueBalanceBytes;
      offset += 8;

      // Skip vShieldedSpend
      const { value: nShieldedSpend, size: spendVarIntSize } = readVarInt(buffer, offset);
      offset += spendVarIntSize;
      for (let i = 0; i < nShieldedSpend; i++) {
        offset += 384; // Each spend is 384 bytes
      }

      // Skip vShieldedOutput
      const { value: nShieldedOutput, size: outputVarIntSize } = readVarInt(buffer, offset);
      offset += outputVarIntSize;
      for (let i = 0; i < nShieldedOutput; i++) {
        offset += 948; // Each output is 948 bytes
      }
    }
  }

  // Parse JoinSplits (both v2 and v4 can have these)
  if (versionNumber === 2 || isSaplingV4) {
    const { value: nJoinSplit, size: joinSplitVarIntSize } = readVarInt(buffer, offset);
    offset += joinSplitVarIntSize;

    if (nJoinSplit > 0 && nJoinSplit <= 100) {
      for (let i = 0; i < nJoinSplit; i++) {
        // Read vpub_old (8 bytes) - keep as satoshis (bigint)
        const vpub_old = buffer.readBigInt64LE(offset);
        offset += 8;

        // Read vpub_new (8 bytes) - keep as satoshis (bigint)
        const vpub_new = buffer.readBigInt64LE(offset);
        offset += 8;

        vjoinsplit.push({ vpub_old, vpub_new });

        // Skip rest of JoinSplit structure
        // FLUX-SPECIFIC JoinSplit ciphertext sizes:
        // v2 Sprout: 601 bytes, v4 Sapling: 549 bytes
        const ciphertextSize = isSaplingV4 ? 549 : 601;
        offset += 32;  // anchor
        offset += 64;  // nullifiers
        offset += 64;  // commitments
        offset += 32;  // ephemeralKey
        offset += 32;  // randomSeed
        offset += 64;  // macs
        offset += 296; // proof
        offset += ciphertextSize; // ciphertext 0
        offset += ciphertextSize; // ciphertext 1
      }

      // Skip joinSplitPubKey and joinSplitSig
      offset += 32; // joinSplitPubKey
      offset += 64; // joinSplitSig
    }
  }

  return {
    version: versionNumber,
    vjoinsplit: vjoinsplit.length > 0 ? vjoinsplit : undefined,
    valueBalance,
  };
}
