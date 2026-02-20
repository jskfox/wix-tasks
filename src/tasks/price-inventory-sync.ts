import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { query } from '../services/database';
import {
  queryAllWixProducts,
  updateInventoryVariantsConcurrent,
  updateProductPrice,
  WixProduct,
} from '../services/wix-api';
import { getSetting, setSetting } from '../services/settings-db';

const CTX = 'PriceInventorySync';
const WATERMARK_KEY = 'wix.price_inventory_sync.last_processed_timestamp';

interface StockSumRow {
  sku: string;
  total_stock: string;
  last_updated: string;
}

interface PriceRow {
  sku: string;
  precio: string;
  impuesto: string;
  ieps: string;
}

export class PriceInventorySyncTask extends BaseTask {
  readonly name = 'price-inventory-sync';
  readonly description = 'Sincroniza inventario y precios desde PostgreSQL hacia Wix. Stock: suma sucursales por prefijo, umbral mínimo. Precio: sucursal Wix específica, precio+impuesto+ieps redondeado a 2 decimales.';
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

    const sucursalWix = config.sucursalWix;
    logger.info(CTX, `${modeLabel} Starting price+inventory sync (stock branches=${branchPrefix}*, price sucursal=${sucursalWix}, threshold=${minThreshold})...`);

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
    // STEP 2: Query PostgreSQL for stock + price data
    // ══════════════════════════════════════════════════════════════════════════
    const defaultWatermark = '1970-01-01T00:00:00.000Z';
    const lastProcessed = getSetting(WATERMARK_KEY, defaultWatermark);

    logger.info(CTX, `Watermark: processing changes since ${lastProcessed}`);

    const skuPlaceholders = wixSkus.map((_, i) => `$${i + 3}`).join(', ');

    // Stock query: sum across all branches matching branchPrefix, detect changes via watermark
    const stockResult = await query<StockSumRow>(
      `SELECT
         sku,
         SUM(existencia::numeric) AS total_stock,
         MAX(precio_actualizado) AS last_updated
       FROM maestro_precios_sucursal
       WHERE precio_actualizado > $1
         AND sucursal::text LIKE $2
         AND sku IN (${skuPlaceholders})
       GROUP BY sku
       ORDER BY MAX(precio_actualizado) DESC`,
      [lastProcessed, `${branchPrefix}%`, ...wixSkus],
    );

    const changedRows = stockResult.rows;

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

    // Price query: exact sucursalWix, one row per SKU with precio+impuesto+ieps
    const changedSkus = rowsToProcess.map(r => r.sku);
    const priceSkuPlaceholders = changedSkus.map((_, i) => `$${i + 2}`).join(', ');
    const priceResult = await query<PriceRow>(
      `SELECT sku, precio, impuesto, ieps
       FROM maestro_precios_sucursal
       WHERE sucursal = $1
         AND sku IN (${priceSkuPlaceholders})`,
      [sucursalWix, ...changedSkus],
    );
    const priceMap = new Map<string, { precio: number; impuesto: number; ieps: number }>();
    for (const r of priceResult.rows) {
      priceMap.set(r.sku, {
        precio: parseFloat(r.precio) || 0,
        impuesto: parseFloat(r.impuesto) || 0,
        ieps: parseFloat(r.ieps) || 0,
      });
    }
    logger.info(CTX, `Fetched prices for ${priceMap.size} SKUs from sucursal ${sucursalWix}`);

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 3: Build stock + price map
    // ══════════════════════════════════════════════════════════════════════════
    const stockMap = new Map<string, { totalStock: number; effectiveStock: number; precioFinal: number }>();

    for (const row of rowsToProcess) {
      const totalStock = parseFloat(row.total_stock) || 0;
      const effectiveStock = totalStock < minThreshold ? 0 : Math.floor(totalStock);

      const p = priceMap.get(row.sku);
      let precioFinal = 0;
      if (p && p.precio > 0) {
        const iepsMonto = p.precio * (p.ieps / 100);
        const subtotal  = p.precio + iepsMonto;
        const ivaMonto  = subtotal * (p.impuesto / 100);
        precioFinal = Math.round((subtotal + ivaMonto) * 100) / 100;
      }

      stockMap.set(row.sku, { totalStock, effectiveStock, precioFinal });

      const status = totalStock < minThreshold ? '⛔ BLOCKED' : '✓';
      logger.debug(CTX, `  ${status} SKU: ${row.sku} | Stock: ${totalStock.toFixed(0)} → ${effectiveStock} | Precio: $${precioFinal}`);
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
          logger.info(CTX, `${modeLabel} Would block SKU ${sku} (total=${stockInfo.totalStock.toFixed(0)} < ${minThreshold}), precio=$${stockInfo.precioFinal}`);
          blocked++;
        } else {
          logger.info(CTX, `${modeLabel} Would update SKU ${sku} → ${stockInfo.effectiveStock} units, precio=$${stockInfo.precioFinal}`);
          updated++;
        }
      }
      logger.info(CTX, `${modeLabel} Sync complete: ${updated} would update, ${blocked} would block (stock<${minThreshold}), ${skipped} skipped`);
    } else {
      // ── STEP 4a: Inventory updates ────────────────────────────────────────
      const inventoryItems: Array<{
        inventoryItemId: string;
        trackQuantity: boolean;
        variants: Array<{ variantId: string; quantity: number; inStock: boolean }>;
        sku: string;
      }> = [];

      // ── STEP 4b: Price updates ────────────────────────────────────────────
      const priceItems: Array<{ productId: string; price: number; sku: string }> = [];

      for (const [sku, stockInfo] of stockMap) {
        const wixProduct = wixProductMap.get(sku);
        if (!wixProduct) {
          skipped++;
          continue;
        }

        if (wixProduct.inventoryItemId) {
          if (stockInfo.effectiveStock === 0) blocked++;
          inventoryItems.push({
            inventoryItemId: wixProduct.inventoryItemId,
            trackQuantity: true,
            variants: [{ variantId: defaultVariantId, quantity: stockInfo.effectiveStock, inStock: stockInfo.effectiveStock > 0 }],
            sku,
          });
        } else {
          logger.warn(CTX, `⚠ SKU ${sku} has no inventoryItemId, skipping inventory update`);
        }

        if (wixProduct.id && stockInfo.precioFinal > 0) {
          const currentPrice = Math.round((wixProduct.priceData?.price ?? 0) * 100) / 100;
          const newPrice = stockInfo.precioFinal; // already rounded to 2 decimals
          if (currentPrice !== newPrice) {
            priceItems.push({ productId: wixProduct.id, price: newPrice, sku });
          } else {
            logger.debug(CTX, `  = SKU: ${sku} | Precio sin cambio ($${newPrice}), omitiendo`);
          }
        }
      }

      // Inventory
      const ratePerMin = 180;
      const invEst = inventoryItems.length <= ratePerMin ? '<1 min' : `~${Math.ceil(inventoryItems.length / ratePerMin)} min`;
      logger.info(CTX, `Sending ${inventoryItems.length} inventory updates (max ${ratePerMin}/min, est. ${invEst}, ${blocked} blocked at 0)...`);
      const { successes: invOk, failures: invFail } = await updateInventoryVariantsConcurrent(inventoryItems);

      // Prices — use same sliding window rate limiter shared with inventory
      const priceEst = priceItems.length <= ratePerMin ? '<1 min' : `~${Math.ceil(priceItems.length / ratePerMin)} min`;
      logger.info(CTX, `Sending ${priceItems.length} price updates (max ${ratePerMin}/min, est. ${priceEst})...`);
      let priceOk = 0;
      let priceFail = 0;
      await (async () => {
        const windowMs = 60_000;
        const windowTimestamps: number[] = [];
        let inFlight = 0;
        let idx = 0;
        await new Promise<void>((resolve) => {
          let settled = 0;
          const total = priceItems.length;
          if (total === 0) { resolve(); return; }
          function availableSlots(): number {
            const now = Date.now();
            while (windowTimestamps.length > 0 && now - windowTimestamps[0] > windowMs) windowTimestamps.shift();
            return Math.min(ratePerMin - windowTimestamps.length, 20 - inFlight);
          }
          function tryDispatch(): void {
            const slots = availableSlots();
            if (slots <= 0 || idx >= total) {
              if (idx < total && windowTimestamps.length > 0) {
                setTimeout(tryDispatch, windowMs - (Date.now() - windowTimestamps[0]) + 1);
              }
              return;
            }
            const toDispatch = Math.min(slots, total - idx);
            for (let i = 0; i < toDispatch; i++) {
              const item = priceItems[idx++];
              inFlight++;
              windowTimestamps.push(Date.now());
              updateProductPrice(item.productId, item.price)
                .then(() => { priceOk++; })
                .catch((err: unknown) => {
                  priceFail++;
                  logger.warn(CTX, `Failed to update price for SKU ${item.sku}: ${err instanceof Error ? err.message : String(err)}`);
                })
                .finally(() => {
                  inFlight--;
                  settled++;
                  if (settled === total) resolve(); else tryDispatch();
                });
            }
          }
          tryDispatch();
        });
      })();

      logger.info(CTX, `${modeLabel} Sync complete — Inventory: ${invOk} ok, ${invFail} failed (${blocked} blocked at 0) | Prices: ${priceOk} ok, ${priceFail} failed | Skipped: ${skipped}`);
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
