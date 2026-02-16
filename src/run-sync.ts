/**
 * Manual runner for OdooInventorySyncTask.
 *
 * Usage:
 *   npx ts-node src/run-sync.ts                        # full sync (writes to Odoo)
 *   npx ts-node src/run-sync.ts --stock-only           # stock-only sync (lightweight)
 *   DRY_RUN=1 npx ts-node src/run-sync.ts              # full sync dry run
 *   DRY_RUN=1 npx ts-node src/run-sync.ts --stock-only # stock-only dry run
 *
 * Useful for:
 *   - Debugging before deploying
 *   - Testing after code changes
 *   - Inspecting the diff without touching Odoo
 */

import { closeMssqlPool } from './services/mssql';
import { OdooInventorySyncTask, SyncMode } from './tasks/odoo-inventory-sync';
import { logger } from './utils/logger';

const CTX = 'RunSync';

async function main(): Promise<void> {
  const mode: SyncMode = process.argv.includes('--stock-only') ? 'stock-only' : 'full';
  const task = new OdooInventorySyncTask(mode);

  logger.info(CTX, '═══════════════════════════════════════════════════');
  logger.info(CTX, ` Manual run: ${task.name}`);
  logger.info(CTX, ` Mode:       ${task.syncMode}`);
  logger.info(CTX, ` Dry run:    ${task.dryRun ? 'YES' : 'NO'}`);
  logger.info(CTX, '═══════════════════════════════════════════════════');

  try {
    await task.execute();
  } catch (err) {
    logger.error(CTX, `Task failed: ${(err as Error).message}`);
    logger.error(CTX, (err as Error).stack || '');
    process.exitCode = 1;
  } finally {
    await closeMssqlPool();
  }
}

main();
