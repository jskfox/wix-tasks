#!/usr/bin/env ts-node
/**
 * CLI script to run the Wix price/inventory sync task.
 * 
 * Usage:
 *   npx ts-node src/run-wix-sync.ts              # Run in DRY-RUN mode (uses config)
 *   npx ts-node src/run-wix-sync.ts --limit=10   # Run LIVE with only 10 SKUs (test mode)
 *   npx ts-node src/run-wix-sync.ts --limit=50   # Run LIVE with only 50 SKUs (test mode)
 * 
 * The --limit flag:
 *   - Forces LIVE mode (ignores wix.dry_run setting)
 *   - Only processes N SKUs
 *   - Does NOT update the watermark (so you can re-run)
 *   - Useful for testing before running the full sync
 */

import { PriceInventorySyncTask } from './tasks/price-inventory-sync';
import { closePool } from './services/database';
import { logger } from './utils/logger';

const CTX = 'WixSyncCLI';

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  // Parse --limit=N argument
  let limit: number | null = null;
  for (const arg of args) {
    const match = arg.match(/^--limit=(\d+)$/);
    if (match) {
      limit = parseInt(match[1], 10);
    }
  }

  const task = new PriceInventorySyncTask();

  if (limit) {
    logger.info(CTX, `Running Wix sync in TEST mode with limit=${limit} SKUs`);
    logger.info(CTX, 'This will execute LIVE updates but only for the specified number of SKUs');
    logger.info(CTX, 'Watermark will NOT be updated so you can re-run with more SKUs');
    logger.info(CTX, '─'.repeat(60));
    
    // Temporarily override dry_run to false for test mode
    const originalDryRun = process.env.WIX_DRY_RUN;
    process.env.WIX_DRY_RUN = 'false';
    
    try {
      await task.runWithLimit(limit);
    } finally {
      // Restore original setting
      if (originalDryRun !== undefined) {
        process.env.WIX_DRY_RUN = originalDryRun;
      } else {
        delete process.env.WIX_DRY_RUN;
      }
    }
  } else {
    logger.info(CTX, 'Running Wix sync (uses wix.dry_run setting from config)');
    logger.info(CTX, '─'.repeat(60));
    await task.execute();
  }

  logger.info(CTX, '─'.repeat(60));
  logger.info(CTX, 'Sync completed');
}

main()
  .catch((err) => {
    logger.error(CTX, 'Sync failed:', err.message);
    process.exit(1);
  })
  .finally(async () => {
    await closePool();
    process.exit(0);
  });
