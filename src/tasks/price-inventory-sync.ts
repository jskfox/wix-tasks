import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { query } from '../services/database';
import {
  queryAllWixProducts,
  updateInventoryVariantsConcurrent,
  WixProduct,
} from '../services/wix-api';
import { getSetting, setSetting } from '../services/settings-db';

const CTX = 'PriceInventorySync';
const WATERMARK_KEY = 'wix.price_inventory_sync.last_processed_timestamp';

interface StockSumRow {
  sku: string;
  total_stock: string;
  precio: string;
  nombre_corto: string;
  last_updated: string;
}

export class PriceInventorySyncTask extends BaseTask {
  readonly name = 'price-inventory-sync';
  readonly description = 'Sincroniza inventario desde PostgreSQL hacia Wix. Suma stock de sucursales filtradas por prefijo. Si el total < umbral, pone stock en 0.';
  // Every hour at minute 5 and 35 (Pacific)
  readonly cronExpression = '5,35 * * * *';

  // Optional limit for testing - set via constructor or runWithLimit()
  private testLimit: number | null = null;
  private forceLive: boolean = false;

  /**
   * Run the sync with a specific limit of SKUs for testing.
   * This allows testing in LIVE mode with only N products.
   * @param limit Number of SKUs to process (e.g., 10 for testing)
   */
  async runWithLimit(limit: number): Promise<void> {
    this.testLimit = limit;
    this.forceLive = true; // Force LIVE mode when using limit
    try {
      await this.execute();
    } finally {
      this.testLimit = null;
      this.forceLive = false;
    }
  }

  async execute(): Promise<void> {
    // When using testLimit, force LIVE mode regardless of config
    const dryRun = this.forceLive ? false : config.wix.dryRun;
    const minThreshold = config.wix.minStockThreshold;
    const branchPrefix = config.wix.branchPrefix;

    const modeLabel = dryRun ? '[DRY-RUN]' : (this.testLimit ? `[LIVE-TEST:${this.testLimit}]` : '[LIVE]');

    if (this.testLimit) {
      logger.info(CTX, `${modeLabel} TEST MODE: Will only process ${this.testLimit} SKUs`);
    }

    logger.info(CTX, `${modeLabel} Starting inventory sync (branches=${branchPrefix}*, threshold=${minThreshold})...`);

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 1: Get ALL products from Wix (with SKU and inventoryItemId)
    // ══════════════════════════════════════════════════════════════════════════
    const wixProducts = await queryAllWixProducts();

    if (wixProducts.length === 0) {
      logger.info(CTX, 'No products found in Wix store, nothing to sync');
      return;
    }

    // Build a map of SKU → WixProduct for quick lookup
    const wixProductMap = new Map<string, WixProduct>();
    for (const product of wixProducts) {
      if (product.sku) {
        wixProductMap.set(product.sku, product);
      }
    }

    const wixSkus = Array.from(wixProductMap.keys());
    logger.info(CTX, `Found ${wixSkus.length} products with SKU in Wix`);

    if (wixSkus.length === 0) {
      logger.info(CTX, 'No products with SKU found in Wix, nothing to sync');
      return;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 2: Query PostgreSQL for stock data of those SKUs
    // ══════════════════════════════════════════════════════════════════════════
    // Get watermark for incremental sync
    const defaultWatermark = '1970-01-01T00:00:00.000Z';
    const lastProcessed = getSetting(WATERMARK_KEY, defaultWatermark);

    logger.info(CTX, `Watermark: processing changes since ${lastProcessed}`);

    // Query stock for SKUs that exist in Wix AND have changes since watermark
    // Build dynamic placeholders for IN clause: $3, $4, $5, ...
    const skuPlaceholders = wixSkus.map((_, i) => `$${i + 3}`).join(', ');
    const result = await query<StockSumRow>(
      `SELECT 
         sku,
         SUM(existencia::numeric) as total_stock,
         MAX(precio) as precio,
         MAX(nombre_corto) as nombre_corto,
         MAX(precio_actualizado) as last_updated
       FROM maestro_precios_sucursal
       WHERE precio_actualizado > $1
         AND sucursal::text LIKE $2
         AND sku IN (${skuPlaceholders})
       GROUP BY sku
       ORDER BY MAX(precio_actualizado) DESC`,
      [lastProcessed, `${branchPrefix}%`, ...wixSkus],
    );

    const changedRows = result.rows;

    if (changedRows.length === 0) {
      logger.info(CTX, `No inventory changes detected for Wix SKUs since ${lastProcessed}`);
      return;
    }

    logger.info(CTX, `Found ${changedRows.length} SKU(s) with changes (out of ${wixSkus.length} Wix SKUs)`);

    // Apply test limit if set
    let rowsToProcess = changedRows;
    if (this.testLimit && changedRows.length > this.testLimit) {
      rowsToProcess = changedRows.slice(0, this.testLimit);
      logger.info(CTX, `${modeLabel} Limiting to ${this.testLimit} SKUs for testing (${changedRows.length - this.testLimit} skipped)`);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 3: Build stock map with threshold logic
    // ══════════════════════════════════════════════════════════════════════════
    const stockMap = new Map<string, { totalStock: number; effectiveStock: number; precio: number }>();

    for (const row of rowsToProcess) {
      const totalStock = parseFloat(row.total_stock) || 0;
      // Apply threshold: if total < minThreshold, set effective stock to 0
      const effectiveStock = totalStock < minThreshold ? 0 : Math.floor(totalStock);
      const precio = parseFloat(row.precio) || 0;

      stockMap.set(row.sku, { totalStock, effectiveStock, precio });

      const status = totalStock < minThreshold ? '⛔ BLOCKED' : '✓';
      logger.debug(CTX, `  ${status} SKU: ${row.sku} | Total: ${totalStock.toFixed(0)} | Effective: ${effectiveStock}`);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 4: Update inventory in Wix (concurrent requests, capped at 10)
    // ══════════════════════════════════════════════════════════════════════════
    const defaultVariantId = '00000000-0000-0000-0000-000000000000';
    let skipped = 0;
    let blocked = 0;

    if (dryRun) {
      let updated = 0;
      for (const [sku, stockInfo] of stockMap) {
        if (stockInfo.effectiveStock === 0) {
          logger.info(CTX, `${modeLabel} Would block SKU ${sku} (total=${stockInfo.totalStock.toFixed(0)} < ${minThreshold})`);
          blocked++;
        } else {
          logger.info(CTX, `${modeLabel} Would update SKU ${sku} → ${stockInfo.effectiveStock} units`);
          updated++;
        }
      }
      logger.info(CTX, `${modeLabel} Sync complete: ${updated} would update, ${blocked} would block (stock<${minThreshold}), ${skipped} skipped`);
    } else {
      // Build the concurrent update payload
      const updateItems: Array<{
        inventoryItemId: string;
        trackQuantity: boolean;
        variants: Array<{ variantId: string; quantity: number; inStock: boolean }>;
        sku: string;
      }> = [];

      for (const [sku, stockInfo] of stockMap) {
        const wixProduct = wixProductMap.get(sku);
        if (!wixProduct?.inventoryItemId) {
          logger.warn(CTX, `⚠ SKU ${sku} has no inventoryItemId, skipping`);
          skipped++;
          continue;
        }
        if (stockInfo.effectiveStock === 0) blocked++;
        updateItems.push({
          inventoryItemId: wixProduct.inventoryItemId,
          trackQuantity: true,
          variants: [{ variantId: defaultVariantId, quantity: stockInfo.effectiveStock, inStock: stockInfo.effectiveStock > 0 }],
          sku,
        });
      }

      const ratePerMin = 180;
      const estSecs = updateItems.length <= ratePerMin
        ? '<1'
        : `~${Math.ceil(updateItems.length / ratePerMin)} min`;
      logger.info(CTX, `Sending ${updateItems.length} inventory updates (max ${ratePerMin}/min, est. ${estSecs}, ${blocked} blocked at 0)...`);
      const { successes, failures } = await updateInventoryVariantsConcurrent(updateItems);

      logger.info(CTX, `${modeLabel} Sync complete: ${successes} updated (${blocked} blocked at 0), ${failures} failures, ${skipped} skipped`);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 5: Update watermark to latest processed timestamp
    // ══════════════════════════════════════════════════════════════════════════
    // Skip watermark update in test mode so we can re-run with more SKUs
    if (this.testLimit) {
      logger.info(CTX, `${modeLabel} Skipping watermark update (test mode)`);
    } else if (changedRows.length > 0) {
      // PostgreSQL returns timestamp - convert to ISO format for consistent storage
      const rawTimestamp = changedRows[0].last_updated;
      const latestTimestamp = new Date(rawTimestamp).toISOString();
      setSetting(WATERMARK_KEY, latestTimestamp);
      logger.info(CTX, `Watermark updated to: ${latestTimestamp}`);
    }
  }
}
