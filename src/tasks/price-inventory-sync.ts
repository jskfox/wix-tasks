import { BaseTask } from './base-task';
import { config, getEmailsForTask } from '../config';
import { logger } from '../utils/logger';
import { query } from '../services/database';
import {
  queryAllWixProducts,
  updateInventoryVariantsConcurrent,
  updateProductPrice,
  WixProduct,
} from '../services/wix-api';
import { getSetting } from '../services/settings-db';
import { sendEmail } from '../services/email';

const CTX = 'PriceInventorySync';

interface StockPriceRow {
  sku: string;
  total_stock: string;
  precio: string | null;
  impuesto: string | null;
  ieps: string | null;
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
    // STEP 2: Query PostgreSQL for stock + price data (all Wix SKUs, no watermark)
    // ══════════════════════════════════════════════════════════════════════════
    // No watermark — compare actual values against Wix on every run.
    // This ensures manual changes in Wix or PostgreSQL are always reconciled.
    const skuPlaceholders = wixSkus.map((_, i) => `$${i + 3}`).join(', ');
    const stockPriceResult = await query<StockPriceRow>(
      `SELECT
         s.sku,
         SUM(s.existencia::numeric)  AS total_stock,
         MAX(p.precio)               AS precio,
         MAX(p.impuesto)             AS impuesto,
         MAX(p.ieps)                 AS ieps
       FROM maestro_precios_sucursal s
       LEFT JOIN maestro_precios_sucursal p
         ON p.sku = s.sku AND p.sucursal = $2
       WHERE s.sucursal::text LIKE $1
         AND s.sku IN (${skuPlaceholders})
       GROUP BY s.sku`,
      [branchPrefix + '%', sucursalWix, ...wixSkus],
    );

    const allRows = stockPriceResult.rows;

    if (allRows.length === 0) {
      logger.info(CTX, 'No SKUs found in PostgreSQL matching Wix catalog, nothing to sync');
      return;
    }

    logger.info(CTX, `Fetched ${allRows.length} SKU(s) from PostgreSQL (out of ${wixSkus.length} Wix SKUs)`);

    // Apply test limit if set
    let rowsToProcess = allRows;
    if (this.testLimit && allRows.length > this.testLimit) {
      rowsToProcess = allRows.slice(0, this.testLimit);
      logger.info(CTX, `${modeLabel} Limiting to ${this.testLimit} SKUs for testing (${allRows.length - this.testLimit} skipped)`);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STEP 3: Build stock + price map
    // ══════════════════════════════════════════════════════════════════════════
    const stockMap = new Map<string, { totalStock: number; effectiveStock: number; precioFinal: number }>();

    for (const row of rowsToProcess) {
      const totalStock = parseFloat(row.total_stock) || 0;
      const effectiveStock = totalStock < minThreshold ? 0 : Math.floor(totalStock);

      const precio   = parseFloat(row.precio   ?? '0') || 0;
      const impuesto = parseFloat(row.impuesto ?? '0') || 0;
      const ieps     = parseFloat(row.ieps     ?? '0') || 0;
      let precioFinal = 0;
      if (precio > 0) {
        const iepsMonto = precio * (ieps / 100);
        const subtotal  = precio + iepsMonto;
        const ivaMonto  = subtotal * (impuesto / 100);
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

      // ── Report data ───────────────────────────────────────────────────────
      const invReport: InvReportRow[] = [];
      const priceReport: PriceReportRow[] = [];

      for (const [sku, stockInfo] of stockMap) {
        const wixProduct = wixProductMap.get(sku);
        if (!wixProduct) {
          skipped++;
          continue;
        }

        const productName = wixProduct.name ?? sku;
        const prevStock = wixProduct.stock?.quantity ?? 0;

        if (wixProduct.inventoryItemId) {
          if (stockInfo.effectiveStock === 0) blocked++;
          if (stockInfo.effectiveStock !== prevStock) {
            inventoryItems.push({
              inventoryItemId: wixProduct.inventoryItemId,
              trackQuantity: true,
              variants: [{ variantId: defaultVariantId, quantity: stockInfo.effectiveStock, inStock: stockInfo.effectiveStock > 0 }],
              sku,
            });
            invReport.push({ sku, name: productName, prevStock, newStock: stockInfo.effectiveStock, blocked: stockInfo.effectiveStock === 0, failed: false });
          } else {
            logger.debug(CTX, `  = SKU: ${sku} | Stock sin cambio (${prevStock}), omitiendo`);
          }
        } else {
          logger.warn(CTX, `⚠ SKU ${sku} has no inventoryItemId, skipping inventory update`);
        }

        if (wixProduct.id && stockInfo.precioFinal > 0) {
          const currentPrice = Math.round((wixProduct.priceData?.price ?? 0) * 100) / 100;
          const newPrice = stockInfo.precioFinal; // already rounded to 2 decimals
          if (currentPrice !== newPrice) {
            priceItems.push({ productId: wixProduct.id, price: newPrice, sku });
            priceReport.push({ sku, name: productName, prevPrice: currentPrice, newPrice, priceDown: newPrice < currentPrice, failed: false });
          } else {
            logger.debug(CTX, `  = SKU: ${sku} | Precio sin cambio ($${newPrice}), omitiendo`);
          }
        }
      }

      // Early exit if nothing changed
      if (inventoryItems.length === 0 && priceItems.length === 0) {
        logger.info(CTX, `${modeLabel} No real changes detected (stock and prices match Wix) — nothing to update`);
        return;
      }

      // Inventory
      const ratePerMin = 180;
      const invEst = inventoryItems.length <= ratePerMin ? '<1 min' : `~${Math.ceil(inventoryItems.length / ratePerMin)} min`;
      logger.info(CTX, `Sending ${inventoryItems.length} inventory updates (max ${ratePerMin}/min, est. ${invEst}, ${blocked} blocked at 0)...`);
      const { successes: invOk, failures: invFail, failedSkus: invFailedSkus } = await updateInventoryVariantsConcurrent(inventoryItems);
      // Mark failed rows in report
      for (const failedSku of (invFailedSkus ?? [])) {
        const row = invReport.find(r => r.sku === failedSku);
        if (row) row.failed = true;
      }

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
                  const row = priceReport.find(r => r.sku === item.sku);
                  if (row) row.failed = true;
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

      // ── STEP 5: Send email report ─────────────────────────────────────────
      if (!this.testLimit) {
        await sendSyncReport({ invReport, priceReport, invOk, invFail, priceOk, priceFail, skipped, blocked, modeLabel, minThreshold });
      }
    }

  }
}

// ─── Email report helper ─────────────────────────────────────────────────────

interface InvReportRow  { sku: string; name: string; prevStock: number; newStock: number; blocked: boolean; failed: boolean; }
interface PriceReportRow { sku: string; name: string; prevPrice: number; newPrice: number; priceDown: boolean; failed: boolean; }

async function sendSyncReport(opts: {
  invReport: InvReportRow[];
  priceReport: PriceReportRow[];
  invOk: number; invFail: number;
  priceOk: number; priceFail: number;
  skipped: number; blocked: number;
  modeLabel: string; minThreshold: number;
}): Promise<void> {
  const recipients = getEmailsForTask('erpPostgresSync');
  if (recipients.length === 0) {
    logger.warn(CTX, 'No email recipients configured for erpPostgresSync — skipping report');
    return;
  }

  const { invReport, priceReport, invOk, invFail, priceOk, priceFail, skipped, blocked, modeLabel, minThreshold } = opts;
  const now = new Date().toLocaleString('es-MX', { timeZone: 'America/Mexico_City', hour12: false });
  const fmt = (n: number) => `$${n.toFixed(2)}`;

  const css = `
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f6f9;margin:0;padding:24px;color:#1a1a2e}
    .wrap{max-width:900px;margin:0 auto}
    h1{font-size:20px;font-weight:700;margin:0 0 4px}
    .sub{color:#666;font-size:13px;margin-bottom:24px}
    .summary{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}
    .stat{background:#fff;border-radius:8px;padding:14px 20px;flex:1;min-width:130px;border-left:4px solid #4f46e5;box-shadow:0 1px 3px rgba(0,0,0,.08)}
    .stat.warn{border-color:#f59e0b}.stat.danger{border-color:#ef4444}.stat.ok{border-color:#10b981}
    .stat-val{font-size:24px;font-weight:700;line-height:1}.stat-lbl{font-size:11px;color:#888;margin-top:4px;text-transform:uppercase;letter-spacing:.5px}
    h2{font-size:15px;font-weight:600;margin:24px 0 10px;padding-bottom:6px;border-bottom:1px solid #e5e7eb}
    table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:24px;font-size:13px}
    th{background:#f8fafc;text-align:left;padding:10px 12px;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#6b7280;font-weight:600;border-bottom:1px solid #e5e7eb}
    td{padding:9px 12px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
    tr:last-child td{border-bottom:none}
    .badge{display:inline-block;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600}
    .badge-blocked{background:#fef3c7;color:#92400e}
    .badge-fail{background:#fee2e2;color:#991b1b}
    .badge-down{background:#fef3c7;color:#92400e}
    .badge-ok{background:#d1fae5;color:#065f46}
    .num{font-family:monospace;font-size:13px}
    .arrow{color:#9ca3af;margin:0 4px}
    .down{color:#ef4444;font-weight:600}
    .up{color:#10b981}
    .footer{text-align:center;font-size:11px;color:#9ca3af;margin-top:24px}
  `;

  // ── Inventory table ──
  const invRows = invReport.map(r => {
    const badge = r.failed
      ? '<span class="badge badge-fail">ERROR</span>'
      : r.blocked
        ? '<span class="badge badge-blocked">BLOQUEADO</span>'
        : '<span class="badge badge-ok">OK</span>';
    const stockChange = r.prevStock === r.newStock
      ? `<span class="num">${r.newStock}</span>`
      : `<span class="num">${r.prevStock}</span><span class="arrow">→</span><span class="num ${r.newStock < r.prevStock ? 'down' : 'up'}">${r.newStock}</span>`;
    return `<tr>
      <td class="num">${r.sku}</td>
      <td>${r.name}</td>
      <td>${stockChange}</td>
      <td>${badge}</td>
    </tr>`;
  }).join('');

  const invTable = invReport.length === 0 ? '<p style="color:#888;font-size:13px">Sin cambios de inventario.</p>' : `
    <table>
      <thead><tr><th>SKU</th><th>Producto</th><th>Stock (anterior → nuevo)</th><th>Estado</th></tr></thead>
      <tbody>${invRows}</tbody>
    </table>`;

  // ── Price table ──
  const priceRows = priceReport.map(r => {
    const badge = r.failed
      ? '<span class="badge badge-fail">ERROR</span>'
      : r.priceDown
        ? '<span class="badge badge-down">↓ BAJÓ</span>'
        : '<span class="badge badge-ok">↑ SUBIÓ</span>';
    const priceChange = `<span class="num">${fmt(r.prevPrice)}</span><span class="arrow">→</span><span class="num ${r.priceDown ? 'down' : 'up'}">${fmt(r.newPrice)}</span>`;
    return `<tr>
      <td class="num">${r.sku}</td>
      <td>${r.name}</td>
      <td>${priceChange}</td>
      <td>${badge}</td>
    </tr>`;
  }).join('');

  const priceTable = priceReport.length === 0 ? '<p style="color:#888;font-size:13px">Sin cambios de precio.</p>' : `
    <table>
      <thead><tr><th>SKU</th><th>Producto</th><th>Precio (anterior → nuevo)</th><th>Estado</th></tr></thead>
      <tbody>${priceRows}</tbody>
    </table>`;

  const totalFails = invFail + priceFail;
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><style>${css}</style></head><body>
  <div class="wrap">
    <h1>Reporte de Sincronización Wix ${modeLabel}</h1>
    <div class="sub">${now} — umbral mínimo de stock: ${minThreshold}</div>

    <div class="summary">
      <div class="stat ok"><div class="stat-val">${invOk}</div><div class="stat-lbl">Inventario OK</div></div>
      <div class="stat warn"><div class="stat-val">${blocked}</div><div class="stat-lbl">Bloqueados (stock=0)</div></div>
      <div class="stat ok"><div class="stat-val">${priceOk}</div><div class="stat-lbl">Precios actualizados</div></div>
      <div class="stat ${totalFails > 0 ? 'danger' : 'ok'}"><div class="stat-val">${totalFails}</div><div class="stat-lbl">Errores</div></div>
      <div class="stat"><div class="stat-val">${skipped}</div><div class="stat-lbl">Omitidos</div></div>
    </div>

    <h2>Cambios de Inventario (${invReport.length})</h2>
    ${invTable}

    <h2>Cambios de Precio (${priceReport.length})</h2>
    ${priceTable}

    <div class="footer">Generado automáticamente por wix-tasks · price-inventory-sync</div>
  </div>
  </body></html>`;

  const totalChanges = invReport.length + priceReport.length;
  const subject = `Wix Sync ${modeLabel} — ${totalChanges} cambio(s) · ${invFail + priceFail > 0 ? `⚠ ${invFail + priceFail} error(es)` : '✓ sin errores'} · ${now}`;

  try {
    await sendEmail({ to: recipients, subject, html });
    logger.info(CTX, `Sync report sent to ${recipients.join(', ')}`);
  } catch (err) {
    logger.warn(CTX, `Failed to send sync report: ${err instanceof Error ? err.message : String(err)}`);
  }
}
