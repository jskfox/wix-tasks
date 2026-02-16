import { config } from './config';
import { logger } from './utils/logger';
import { registerTask, startAll, stopAll } from './scheduler';
import { closePool } from './services/database';
import { closeMssqlPool } from './services/mssql';

// ── Tasks ────────────────────────────────────────────────────────────────────
import { PriceInventorySyncTask } from './tasks/price-inventory-sync';
import { AbandonedCartsTask } from './tasks/abandoned-carts';
import { OdooChatAnalysisTask } from './tasks/odoo-chat-analysis';
import { OdooChatLeadsTask } from './tasks/odoo-chat-leads';
import { OdooInventorySyncTask } from './tasks/odoo-inventory-sync';

const CTX = 'Main';

async function main(): Promise<void> {
  logger.info(CTX, '═══════════════════════════════════════════════════');
  logger.info(CTX, ' Wix Scheduled Tasks — Proconsa');
  logger.info(CTX, `  Timezone: ${config.timezone}`);
  logger.info(CTX, `  Sucursal: ${config.sucursalWix}`);
  logger.info(CTX, '═══════════════════════════════════════════════════');

  // Register all tasks
  registerTask(new PriceInventorySyncTask());
  registerTask(new AbandonedCartsTask());
  registerTask(new OdooChatAnalysisTask());
  registerTask(new OdooChatLeadsTask());
  registerTask(new OdooInventorySyncTask('full'));       // Daily at 4:00 AM
  registerTask(new OdooInventorySyncTask('stock-only')); // Every hour at :15

  // Start the scheduler
  startAll();

  logger.info(CTX, 'Scheduler running. Press Ctrl+C to stop.');
}

// ── Graceful shutdown ────────────────────────────────────────────────────────
function shutdown(signal: string): void {
  logger.info(CTX, `Received ${signal} — shutting down...`);
  stopAll();
  Promise.all([closePool(), closeMssqlPool()])
    .then(() => {
      logger.info(CTX, 'Shutdown complete');
      process.exit(0);
    })
    .catch((err) => {
      logger.error(CTX, 'Error during shutdown', err);
      process.exit(1);
    });
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

main().catch((err) => {
  logger.error(CTX, 'Fatal error during startup', err);
  process.exit(1);
});
