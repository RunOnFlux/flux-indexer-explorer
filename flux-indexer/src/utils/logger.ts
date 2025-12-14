/**
 * Logger Utility
 *
 * Beautiful, stylish logging for sync watching
 * Winston-based with stunning console output
 */

import winston from 'winston';

// Detect if we're running in a TTY (interactive terminal)
// When not in TTY (e.g., supervisor, docker logs), use simpler output to avoid buffer issues
const isTTY = process.stdout.isTTY === true;

// Add BigInt support to JSON.stringify to prevent serialization errors
// This is used by the logger when stringifying metadata
const originalStringify = JSON.stringify;
(JSON as any).stringify = function(value: any, replacer?: any, space?: any) {
  const bigIntReplacer = (_key: string, val: any) => {
    if (typeof val === 'bigint') {
      return val.toString();
    }
    return val;
  };

  // If a replacer is provided, chain it with our BigInt replacer
  const combinedReplacer = replacer
    ? (key: string, val: any) => {
        const transformed = bigIntReplacer(key, val);
        if (typeof replacer === 'function') {
          return replacer(key, transformed);
        }
        return transformed;
      }
    : bigIntReplacer;

  return originalStringify(value, combinedReplacer, space);
};

// ANSI color codes - these are safe ASCII escape sequences that work everywhere
// Colors are always enabled - only Unicode symbols are disabled for non-TTY
const c = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  underscore: '\x1b[4m',
  blink: '\x1b[5m',
  reverse: '\x1b[7m',

  // Foreground colors
  black: '\x1b[30m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',

  // Bright foreground colors
  brightRed: '\x1b[91m',
  brightGreen: '\x1b[92m',
  brightYellow: '\x1b[93m',
  brightBlue: '\x1b[94m',
  brightMagenta: '\x1b[95m',
  brightCyan: '\x1b[96m',
  brightWhite: '\x1b[97m',

  // Background colors
  bgBlack: '\x1b[40m',
  bgRed: '\x1b[41m',
  bgGreen: '\x1b[42m',
  bgYellow: '\x1b[43m',
  bgBlue: '\x1b[44m',
  bgMagenta: '\x1b[45m',
  bgCyan: '\x1b[46m',
  bgWhite: '\x1b[47m',
};

// Cool symbols (use ASCII fallbacks when not in TTY)
const sym = isTTY ? {
  flux: '⚡',
  block: '█',
  blockEmpty: '░',
  check: '✓',
  cross: '✗',
  warn: '⚠',
  info: 'ℹ',
  arrow: '→',
  arrowRight: '▶',
  bullet: '•',
  star: '★',
  diamond: '◆',
  circle: '●',
  square: '■',
  triangle: '▲',
  heart: '♥',
  lightning: '⚡',
  fire: '🔥',
  rocket: '🚀',
  database: '💾',
  clock: '⏱',
  chart: '📊',
  gear: '⚙',
  link: '🔗',
  box: '📦',
  target: '🎯',
  sparkle: '✨',
  hourglass: '⏳',
} : {
  // ASCII fallbacks for non-TTY
  flux: '*', block: '#', blockEmpty: '.', check: '+', cross: 'x', warn: '!',
  info: 'i', arrow: '->', arrowRight: '>', bullet: '*', star: '*', diamond: '*',
  circle: 'o', square: '#', triangle: '^', heart: '<3', lightning: '*',
  fire: '*', rocket: '^', database: '[D]', clock: '[T]', chart: '[C]',
  gear: '[G]', link: '[L]', box: '[B]', target: '[X]', sparkle: '*', hourglass: '[.]',
};

// Box drawing characters (use ASCII when not in TTY)
const box = isTTY ? {
  topLeft: '╭',
  topRight: '╮',
  bottomLeft: '╰',
  bottomRight: '╯',
  horizontal: '─',
  vertical: '│',
  horizontalBold: '━',
  verticalBold: '┃',
} : {
  topLeft: '+', topRight: '+', bottomLeft: '+', bottomRight: '+',
  horizontal: '-', vertical: '|', horizontalBold: '=', verticalBold: '|',
};

// Create a beautiful progress bar
function createProgressBar(percent: number, width: number = 40): string {
  const filled = Math.round((percent / 100) * width);
  const empty = width - filled;

  // Gradient effect using different block characters
  let bar = '';
  for (let i = 0; i < filled; i++) {
    if (i < width * 0.3) bar += `${c.brightGreen}${sym.block}`;
    else if (i < width * 0.7) bar += `${c.brightCyan}${sym.block}`;
    else bar += `${c.brightMagenta}${sym.block}`;
  }
  bar += `${c.dim}${sym.blockEmpty.repeat(empty)}${c.reset}`;

  return bar;
}

// Format large numbers with commas
function formatNumber(num: number): string {
  return num.toLocaleString();
}

// Format bytes to human readable
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

// Create a styled box around text
function createBox(title: string, content: string[], color: string = c.cyan): string {
  const maxLen = Math.max(title.length, ...content.map(l => l.replace(/\x1b\[[0-9;]*m/g, '').length));
  const width = maxLen + 4;
  const hr = box.horizontal.repeat(width - 2);

  let result = `${color}${box.topLeft}${hr}${box.topRight}${c.reset}\n`;
  result += `${color}${box.vertical}${c.reset} ${c.bright}${title.padEnd(width - 3)}${c.reset}${color}${box.vertical}${c.reset}\n`;
  result += `${color}${box.vertical}${hr}${box.vertical}${c.reset}\n`;

  for (const line of content) {
    const plainLen = line.replace(/\x1b\[[0-9;]*m/g, '').length;
    const padding = width - 3 - plainLen;
    result += `${color}${box.vertical}${c.reset} ${line}${' '.repeat(Math.max(0, padding))}${color}${box.vertical}${c.reset}\n`;
  }

  result += `${color}${box.bottomLeft}${hr}${box.bottomRight}${c.reset}`;
  return result;
}

const logFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json()
);

const consoleFormat = winston.format.combine(
  winston.format.timestamp({ format: 'HH:mm:ss' }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    const text = String(message);
    const m = meta as any;

    // ═══════════════════════════════════════════════════════════════════════════
    // BULK SYNC PROGRESS - The main event!
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Bulk sync progress')) {
      const match = text.match(/(\d+)\/(\d+)/);
      const current = match ? parseInt(match[1], 10) : 0;
      const target = match ? parseInt(match[2], 10) : 1;
      const pct = (current / target) * 100;
      const pctStr = pct.toFixed(2);

      const bar = createProgressBar(pct, 50);
      const blocksRemaining = target - current;
      const speed = parseFloat(m.blocksPerSecond) || 0;

      // Calculate detailed ETA
      let etaDisplay = m.eta || 'calculating...';

      const lines = [
        '',
        `${c.dim}${box.horizontalBold.repeat(70)}${c.reset}`,
        `${c.bright}${c.brightCyan}  ${sym.lightning} FLUX INDEXER ${sym.lightning}${c.reset}  ${c.dim}${timestamp}${c.reset}`,
        `${c.dim}${box.horizontal.repeat(70)}${c.reset}`,
        '',
        `  ${bar} ${c.bright}${c.brightWhite}${pctStr}%${c.reset}`,
        '',
        `  ${c.brightBlue}${sym.box} BLOCKS${c.reset}    ${c.bright}${formatNumber(current)}${c.reset} ${c.dim}/${c.reset} ${formatNumber(target)}  ${c.dim}(${formatNumber(blocksRemaining)} remaining)${c.reset}`,
        `  ${c.brightMagenta}${sym.lightning} SPEED${c.reset}     ${c.bright}${speed.toFixed(1)}${c.reset} ${c.dim}blocks/sec${c.reset}`,
        `  ${c.brightYellow}${sym.hourglass} ETA${c.reset}       ${c.bright}${etaDisplay}${c.reset}`,
      ];

      // Add memory stats if available
      if (m.memory) {
        lines.push(`  ${c.brightGreen}${sym.database} MEMORY${c.reset}    ${c.bright}${m.memory.heapMB}MB${c.reset} heap  ${c.dim}/ ${m.memory.rssMB}MB rss${c.reset}`);
      }

      // Add fetcher stats if available
      if (m.fetcherStats) {
        const fs = m.fetcherStats;
        lines.push(`  ${c.brightCyan}${sym.gear} FETCHER${c.reset}   ${c.dim}queue=${c.reset}${fs.queuedBatches || 0} ${c.dim}pending=${c.reset}${fs.pendingBlocks || 0} ${c.dim}cached=${c.reset}${fs.cachedBlocks || 0}`);
      }

      // Add database stats if available
      if (m.dbStats) {
        lines.push(`  ${c.brightMagenta}${sym.chart} DB${c.reset}        ${c.dim}tx=${c.reset}${formatNumber(m.dbStats.transactions || 0)} ${c.dim}utxos=${c.reset}${formatNumber(m.dbStats.utxos || 0)} ${c.dim}addr=${c.reset}${formatNumber(m.dbStats.addresses || 0)}`);
      }

      lines.push(`${c.dim}${box.horizontalBold.repeat(70)}${c.reset}`);
      lines.push('');

      return lines.join('\n');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BLOCK INDEXED - Show individual block progress
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Block indexed') || text.includes('Indexed block')) {
      const height = m.height || m.blockHeight || 'N/A';
      const txCount = m.txCount || m.transactions || 0;
      const time = m.processingTime || m.time || '';
      return `${c.dim}${timestamp}${c.reset} ${c.green}${sym.check}${c.reset} ${c.dim}Block${c.reset} ${c.bright}#${formatNumber(height)}${c.reset} ${c.dim}${box.vertical}${c.reset} ${c.cyan}${txCount}${c.reset} ${c.dim}txs${c.reset}${time ? ` ${c.dim}${box.vertical}${c.reset} ${c.yellow}${time}${c.reset}` : ''}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BATCH STARTING - Show batch beginning
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Batch starting')) {
      const start = m.startBlock || 0;
      const end = m.endBlock || 0;
      const remaining = m.blocksToSync || 0;
      return `${c.dim}${timestamp}${c.reset} ${c.brightCyan}${sym.arrowRight}${c.reset} ${c.bright}Batch${c.reset} ${c.cyan}${formatNumber(start)}${c.reset}${c.dim}${sym.arrow}${c.reset}${c.cyan}${formatNumber(end)}${c.reset} ${c.dim}${box.vertical}${c.reset} ${c.yellow}${formatNumber(remaining)}${c.reset} ${c.dim}remaining${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BATCH COMPLETE - Batch processing summary
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Batch') && (text.includes('complete') || text.includes('processed'))) {
      const blocks = m.blocks || m.blockCount || 0;
      const speed = m.blocksPerSecond || '';
      const time = m.time || m.duration || '';
      return `${c.dim}${timestamp}${c.reset} ${c.brightGreen}${sym.sparkle}${c.reset} ${c.bright}Batch Done${c.reset} ${c.dim}${box.vertical}${c.reset} ${c.cyan}${formatNumber(blocks)}${c.reset} ${c.dim}blocks${c.reset} ${c.dim}${box.vertical}${c.reset} ${c.magenta}${speed}${c.reset} ${c.dim}blk/s${c.reset}${time ? ` ${c.dim}${box.vertical}${c.reset} ${c.yellow}${time}${c.reset}` : ''}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MEMORY SNAPSHOT - Compact memory stats (from sync-engine)
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Memory snapshot')) {
      const label = m.label || '';
      const heap = m.heapMB || 0;
      const delta = m.deltaHeapMB || 0;
      const rss = m.rssMB || 0;
      const deltaStr = delta >= 0 ? `+${delta}` : `${delta}`;
      return `${c.dim}${timestamp}${c.reset} ${c.brightMagenta}${sym.database}${c.reset} ${c.dim}${label}${c.reset} ${c.bright}${heap}MB${c.reset} ${c.dim}heap${c.reset} ${c.dim}(${c.reset}${delta >= 0 ? c.yellow : c.green}${deltaStr}${c.reset}${c.dim})${c.reset} ${c.dim}rss=${c.reset}${rss}MB`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // MEMORY DETAILS - Detailed memory stats (from block-indexer)
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Memory details')) {
      const label = m.label || '';
      const heap = m.heapMB || 0;
      const delta = m.deltaHeapMB || 0;
      const rss = m.rssMB || 0;
      const ext = m.extMB || 0;
      const arrBuf = m.arrBufMB || 0;
      const blocks = m.blocks || 0;
      const txs = m.txs || 0;
      const cache = m.cacheSize || 0;
      const cacheQ = m.cacheQueueSize || 0;
      const mbPerBlock = m.mbPerBlock || '0';
      const deltaStr = delta >= 0 ? `+${delta}` : `${delta}`;
      return `${c.dim}${timestamp}${c.reset} ${c.brightMagenta}${sym.gear}${c.reset} ${c.dim}${label}${c.reset} ${c.bright}${heap}MB${c.reset}${c.dim}heap${c.reset} ${c.dim}(${c.reset}${delta >= 0 ? c.yellow : c.green}${deltaStr}${c.reset}${c.dim},${c.reset}${c.cyan}${mbPerBlock}${c.reset}${c.dim}MB/blk)${c.reset} ${c.dim}rss=${c.reset}${rss}${c.dim}MB ext=${c.reset}${ext}${c.dim}MB arr=${c.reset}${arrBuf}${c.dim}MB cache=${c.reset}${cache}${c.dim}/${c.reset}${cacheQ} ${c.blue}${formatNumber(blocks)}${c.reset}${c.dim}blks${c.reset} ${c.magenta}${formatNumber(txs)}${c.reset}${c.dim}txs${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DATABASE OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('COPY') || text.includes('bulk insert') || text.includes('Bulk insert')) {
      const table = m.table || '';
      const rows = m.rows || m.count || 0;
      return `${c.dim}${timestamp}${c.reset} ${c.brightMagenta}${sym.database}${c.reset} ${c.dim}DB${c.reset} ${c.bright}${table}${c.reset} ${c.dim}← ${c.reset}${c.cyan}${formatNumber(rows)}${c.reset} ${c.dim}rows${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SUPPLY VERIFICATION
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Supply') && (text.includes('verified') || text.includes('match'))) {
      return `${c.dim}${timestamp}${c.reset} ${c.brightGreen}${sym.check}${c.reset} ${c.green}Supply Verified${c.reset} ${c.dim}at height${c.reset} ${c.bright}${m.height || ''}${c.reset} ${c.dim}${box.vertical}${c.reset} ${c.cyan}${m.supply || m.transparent || ''}${c.reset} ${c.dim}FLUX${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ERRORS - Make them VERY visible!
    // ═══════════════════════════════════════════════════════════════════════════
    if (level.includes('error')) {
      const hLine = box.horizontal.repeat(68);
      const errorBox = [
        '',
        `${c.brightRed}${box.topLeft}${hLine}${box.topRight}${c.reset}`,
        `${c.brightRed}${box.vertical}${c.reset} ${c.bright}${c.bgRed}${c.white} ${sym.cross} ERROR ${c.reset}${' '.repeat(57)}${c.brightRed}${box.vertical}${c.reset}`,
        `${c.brightRed}${box.vertical}${hLine}${box.vertical}${c.reset}`,
        `${c.brightRed}${box.vertical}${c.reset} ${c.bright}${text.substring(0, 66).padEnd(66)}${c.reset} ${c.brightRed}${box.vertical}${c.reset}`,
      ];

      if (m.error || m.message) {
        const errMsg = String(m.error || m.message).substring(0, 66);
        errorBox.push(`${c.brightRed}${box.vertical}${c.reset} ${c.red}${errMsg.padEnd(66)}${c.reset} ${c.brightRed}${box.vertical}${c.reset}`);
      }

      if (m.stack) {
        errorBox.push(`${c.brightRed}${box.vertical}${hLine}${box.vertical}${c.reset}`);
        const stackLines = String(m.stack).split('\n').slice(0, 5);
        for (const line of stackLines) {
          errorBox.push(`${c.brightRed}${box.vertical}${c.reset} ${c.dim}${line.substring(0, 66).padEnd(66)}${c.reset} ${c.brightRed}${box.vertical}${c.reset}`);
        }
      }

      errorBox.push(`${c.brightRed}${box.bottomLeft}${hLine}${box.bottomRight}${c.reset}`);
      errorBox.push('');

      return errorBox.join('\n');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // WARNINGS - Also make visible but less dramatic
    // ═══════════════════════════════════════════════════════════════════════════
    if (level.includes('warn')) {
      const hLine = box.horizontal.repeat(68);
      const warnLines = [
        `${c.brightYellow}${box.topLeft}${hLine}${box.topRight}${c.reset}`,
        `${c.brightYellow}${box.vertical}${c.reset} ${c.bgYellow}${c.black} ${sym.warn} WARNING ${c.reset} ${c.yellow}${text.substring(0, 54).padEnd(54)}${c.reset} ${c.brightYellow}${box.vertical}${c.reset}`,
      ];

      if (Object.keys(meta).length > 0) {
        const metaStr = JSON.stringify(meta).substring(0, 64);
        warnLines.push(`${c.brightYellow}${box.vertical}${c.reset} ${c.dim}${metaStr.padEnd(66)}${c.reset} ${c.brightYellow}${box.vertical}${c.reset}`);
      }

      warnLines.push(`${c.brightYellow}${box.bottomLeft}${hLine}${box.bottomRight}${c.reset}`);

      return warnLines.join('\n');
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // STARTUP MESSAGES
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('Starting') || text.includes('Initializing') || text.includes('Connected')) {
      return `${c.dim}${timestamp}${c.reset} ${c.brightCyan}${sym.rocket}${c.reset} ${c.bright}${text}${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // REORG DETECTION
    // ═══════════════════════════════════════════════════════════════════════════
    if (text.includes('reorg') || text.includes('Reorg')) {
      return `${c.dim}${timestamp}${c.reset} ${c.brightYellow}${sym.warn}${c.reset} ${c.yellow}${c.bright}REORG${c.reset} ${text} ${c.dim}${JSON.stringify(meta)}${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // STATUS / HEARTBEAT (keep minimal)
    // ═══════════════════════════════════════════════════════════════════════════
    if (text === 'Status' || text.includes('heartbeat')) {
      return `${c.dim}${timestamp} ${sym.bullet} heartbeat${c.reset}`;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DEFAULT FORMATTING
    // ═══════════════════════════════════════════════════════════════════════════
    let levelColor = c.white;
    let icon = sym.bullet;

    if (level.includes('info')) {
      levelColor = c.green;
      icon = sym.check;
    } else if (level.includes('debug')) {
      levelColor = c.dim;
      icon = c.dim + sym.bullet;
    }

    let output = `${c.dim}${timestamp}${c.reset} ${levelColor}${icon}${c.reset} ${text}`;

    // Add compact meta if present
    if (Object.keys(meta).length > 0 && Object.keys(meta).length <= 6) {
      const metaStr = JSON.stringify(meta);
      if (metaStr.length < 120) {
        output += ` ${c.dim}${metaStr}${c.reset}`;
      }
    }

    return output;
  })
);

// Create the logger
export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: logFormat,
  transports: [
    new winston.transports.Console({
      format: consoleFormat,
    }),
  ],
});

// Add file transport if LOG_FILE is specified
if (process.env.LOG_FILE) {
  logger.add(
    new winston.transports.File({
      filename: process.env.LOG_FILE,
      format: logFormat,
    })
  );
}

// Add error-only log file for monitoring critical issues during sync
logger.add(
  new winston.transports.File({
    filename: process.env.ERROR_LOG_FILE || '/var/log/indexer-errors.log',
    level: 'error',
    format: logFormat,
  })
);

// Add warning-level log file to catch all parsing warnings and errors
logger.add(
  new winston.transports.File({
    filename: process.env.WARN_LOG_FILE || '/var/log/indexer-warnings.log',
    level: 'warn',
    format: logFormat,
  })
);

// ═══════════════════════════════════════════════════════════════════════════════
// STARTUP BANNER
// ═══════════════════════════════════════════════════════════════════════════════
export function printStartupBanner(): void {
  if (isTTY) {
    const banner = `
${c.brightCyan}
    ███████╗██╗     ██╗   ██╗██╗  ██╗    ██╗███╗   ██╗██████╗ ███████╗██╗  ██╗███████╗██████╗
    ██╔════╝██║     ██║   ██║╚██╗██╔╝    ██║████╗  ██║██╔══██╗██╔════╝╚██╗██╔╝██╔════╝██╔══██╗
    █████╗  ██║     ██║   ██║ ╚███╔╝     ██║██╔██╗ ██║██║  ██║█████╗   ╚███╔╝ █████╗  ██████╔╝
    ██╔══╝  ██║     ██║   ██║ ██╔██╗     ██║██║╚██╗██║██║  ██║██╔══╝   ██╔██╗ ██╔══╝  ██╔══██╗
    ██║     ███████╗╚██████╔╝██╔╝ ██╗    ██║██║ ╚████║██████╔╝███████╗██╔╝ ██╗███████╗██║  ██║
    ╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝    ╚═╝╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝
${c.reset}
${c.dim}    ════════════════════════════════════════════════════════════════════════════════════${c.reset}
${c.brightMagenta}    ⚡${c.reset} ${c.bright}High-Performance Blockchain Indexer${c.reset}          ${c.dim}ClickHouse Edition${c.reset}
${c.brightGreen}    ✓${c.reset} ${c.dim}Columnar Storage${c.reset}  ${c.brightGreen}✓${c.reset} ${c.dim}Bulk Inserts${c.reset}  ${c.brightGreen}✓${c.reset} ${c.dim}Real-time Analytics${c.reset}
${c.dim}    ════════════════════════════════════════════════════════════════════════════════════${c.reset}
`;
    console.log(banner);
  } else {
    // Simple ASCII banner for non-TTY environments
    console.log('');
    console.log('='.repeat(70));
    console.log('  FLUX INDEXER - High-Performance Blockchain Indexer (ClickHouse)');
    console.log('='.repeat(70));
    console.log('');
  }
}

// Export colors and symbols for use elsewhere
export { c as colors, sym as symbols, isTTY };
