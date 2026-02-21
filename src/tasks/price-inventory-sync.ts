import { BaseTask } from './base-task';
import { config, getEmailsForTask } from '../config';
import { logger } from '../utils/logger';
import { query } from '../services/database';
import {
  queryAllWixProducts,
  updateInventoryVariantsConcurrent,
  updateProductPrice,
  findCollectionByName,
  addProductsToCollection,
  removeProductsFromCollection,
  WixProduct,
} from '../services/wix-api';
import { getSetting } from '../services/settings-db';
import { sendEmail } from '../services/email';
import { sendTeamsSyncNotification } from '../services/teams';

const CTX = 'PriceInventorySync';

interface StockPriceRow {
  sku: string;
  total_stock: string;
  precio: string | null;
  impuesto: string | null;
  ieps: string | null;
  // Promo fields (NULL when SKU is not in promo table for this sucursal)
  promo_precio_regular: string | null;
  promo_precio_promo: string | null;
  promo_descuento: string | null;
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
    // STEP 1: Get ALL products from Wix + resolve collection IDs
    // ══════════════════════════════════════════════════════════════════════════
    const [wixProducts, descuentosId, descuento10Id, tabId, madId, aceId, conId] = await Promise.all([
      queryAllWixProducts(),
      findCollectionByName('Descuentos'),
      findCollectionByName('Descuento10'),
      findCollectionByName('TABLEROS'),
      findCollectionByName('MADERAS'),
      findCollectionByName('ACEROS'),
      findCollectionByName('CONSTRUCCION'),
    ]);
    // Set of collection IDs whose members must be excluded from Descuento10
    const excludedFromDescuento10 = new Set<string>(
      [tabId, madId, aceId, conId].filter((id): id is string => id !== null),
    );
    for (const [name, id] of [['Descuentos', descuentosId], ['Descuento10', descuento10Id]] as [string, string | null][]) {
      if (id) logger.info(CTX, `Colección "${name}" encontrada: ${id}`);
      else    logger.warn(CTX, `Colección "${name}" no encontrada en Wix — se omitirá sincronización`);
    }
    for (const [name, id] of [['TABLEROS', tabId], ['MADERAS', madId], ['ACEROS', aceId], ['CONSTRUCCION', conId]] as [string, string | null][]) {
      if (!id) logger.warn(CTX, `Colección "${name}" no encontrada — sus productos NO serán excluidos de Descuento10`);
    }

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
         SUM(s.existencia::numeric)         AS total_stock,
         MAX(p.precio)                      AS precio,
         MAX(p.impuesto)                    AS impuesto,
         MAX(p.ieps)                        AS ieps,
         MAX(pr.precio_regular::numeric)    AS promo_precio_regular,
         MAX(pr.precio_promo::numeric)      AS promo_precio_promo,
         MAX(pr.descuento::int)             AS promo_descuento
       FROM maestro_precios_sucursal s
       LEFT JOIN maestro_precios_sucursal p
         ON p.sku = s.sku AND p.sucursal = $2
       LEFT JOIN promo pr
         ON pr.sku = s.sku AND pr.sucursal = $2
         -- TODO: add when column exists: AND (pr.vigencia IS NULL OR pr.vigencia >= CURRENT_DATE)
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
    type PromoInfo = { precioRegular: number; precioPromo: number; descuento: number };
    const stockMap = new Map<string, { totalStock: number; effectiveStock: number; precioFinal: number; promo: PromoInfo | null }>();

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

      // Promo: precio_regular and precio_promo already include taxes
      const promoR = row.promo_precio_regular != null ? parseFloat(row.promo_precio_regular) : null;
      const promoP = row.promo_precio_promo   != null ? parseFloat(row.promo_precio_promo)   : null;
      const promoD = row.promo_descuento      != null ? parseInt(row.promo_descuento, 10)     : null;
      const promo: PromoInfo | null = (promoR != null && promoP != null && promoD != null)
        ? { precioRegular: Math.round(promoR * 100) / 100, precioPromo: Math.round(promoP * 100) / 100, descuento: promoD }
        : null;

      stockMap.set(row.sku, { totalStock, effectiveStock, precioFinal, promo });

      const promoTag = promo ? ` | 🏷 PROMO ${promo.descuento}% ($${promo.precioPromo})` : '';
      const status = totalStock < minThreshold ? '⛔ BLOCKED' : '✓';
      logger.debug(CTX, `  ${status} SKU: ${row.sku} | Stock: ${totalStock.toFixed(0)} → ${effectiveStock} | Precio: $${precioFinal}${promoTag}`);
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
      const priceItems: Array<{
        productId: string;
        price: number;  // base price sent to Wix (precio_regular for promo, precioFinal for regular)
        sku: string;
        ribbon: string;
        discount: { type: 'PERCENT' | 'AMOUNT' | 'NONE'; value: number };
      }> = [];

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

        if (wixProduct.id) {
          const { promo } = stockInfo;

          // Determine expected Wix state
          let expectedBasePrice: number;
          let expectedRibbon: string;
          let expectedDiscount: { type: 'PERCENT' | 'AMOUNT' | 'NONE'; value: number };
          let reportNewPrice: number; // effective price the customer pays

          if (promo) {
            expectedBasePrice  = promo.precioRegular;
            expectedRibbon     = 'PROMO';
            // Use AMOUNT discount so Wix shows exactly precio_promo (not a rounded %)
            expectedDiscount   = { type: 'AMOUNT', value: Math.round((promo.precioRegular - promo.precioPromo) * 100) / 100 };
            reportNewPrice     = promo.precioPromo;
          } else {
            if (stockInfo.precioFinal <= 0) continue;
            expectedBasePrice  = stockInfo.precioFinal;
            expectedRibbon     = '';
            expectedDiscount   = { type: 'NONE', value: 0 };
            reportNewPrice     = stockInfo.precioFinal;
          }

          // Compare current Wix state vs expected
          const currentBasePrice     = Math.round((wixProduct.priceData?.price ?? 0) * 100) / 100;
          const currentRibbon        = wixProduct.ribbon ?? '';
          const currentDiscountType  = wixProduct.discount?.type  ?? 'NONE';
          const currentDiscountValue = wixProduct.discount?.value ?? 0;

          const needsUpdate =
            currentBasePrice     !== expectedBasePrice ||
            currentRibbon        !== expectedRibbon ||
            currentDiscountType  !== expectedDiscount.type ||
            currentDiscountValue !== expectedDiscount.value;

          if (needsUpdate) {
            priceItems.push({ productId: wixProduct.id, price: expectedBasePrice, sku, ribbon: expectedRibbon, discount: expectedDiscount });
            // prevPrice = what customer currently pays (discounted or regular)
            const currentEffective = Math.round((wixProduct.priceData?.discountedPrice ?? wixProduct.priceData?.price ?? 0) * 100) / 100;
            const wasPromo = currentRibbon === 'PROMO';
            priceReport.push({ sku, name: productName, prevPrice: currentEffective, newPrice: reportNewPrice, priceDown: reportNewPrice < currentEffective, failed: false, isPromo: !!promo, wasPromo });
          } else {
            logger.debug(CTX, `  = SKU: ${sku} | Precio/promo sin cambio, omitiendo`);
          }
        }
      }

      // ── STEP 4c: Collection membership ───────────────────────────────────
      // "Descuentos"  → productos EN promo
      // "Descuento10" → todos excepto: en promo, en TABLEROS/MADERAS/ACEROS/CONSTRUCCION
      const descuentosAdd:    string[] = [];
      const descuentosRemove: string[] = [];
      const descuento10Add:    string[] = [];
      const descuento10Remove: string[] = [];

      // "Descuentos": iterar stockMap (productos con datos en PostgreSQL)
      if (descuentosId) {
        for (const [sku, stockInfo] of stockMap) {
          const wp = wixProductMap.get(sku);
          if (!wp?.id) continue;
          const colIds     = wp.collectionIds ?? [];
          const inCol      = colIds.includes(descuentosId);
          const shouldBeIn = !!stockInfo.promo;
          if (shouldBeIn && !inCol) descuentosAdd.push(wp.id);
          if (!shouldBeIn && inCol) descuentosRemove.push(wp.id);
        }
        logger.info(CTX, `Descuentos: +${descuentosAdd.length} agregar, -${descuentosRemove.length} quitar`);
      }

      // "Descuento10": iterar catálogo completo de Wix
      if (descuento10Id) {
        for (const wp of wixProducts) {
          if (!wp.id) continue;
          const colIds     = wp.collectionIds ?? [];
          const inD10      = colIds.includes(descuento10Id);
          const inPromo    = !!(wp.sku && stockMap.get(wp.sku)?.promo);
          const inExcluded = [...excludedFromDescuento10].some(id => colIds.includes(id));
          // Products with no stock data (not in PostgreSQL) are assumed in-stock
          const skuStock   = wp.sku ? stockMap.get(wp.sku) : undefined;
          const hasStock   = skuStock ? skuStock.effectiveStock > 0 : true;
          const shouldBeIn = !inPromo && !inExcluded && hasStock;
          if (shouldBeIn && !inD10) descuento10Add.push(wp.id);
          if (!shouldBeIn && inD10) descuento10Remove.push(wp.id);
        }
        logger.info(CTX, `Descuento10: +${descuento10Add.length} agregar, -${descuento10Remove.length} quitar`);
      }

      // Early exit if nothing changed
      if (inventoryItems.length === 0 && priceItems.length === 0 &&
          descuentosAdd.length === 0 && descuentosRemove.length === 0 &&
          descuento10Add.length === 0 && descuento10Remove.length === 0) {
        logger.info(CTX, `${modeLabel} No real changes detected (stock, prices and collections match Wix) — nothing to update`);
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
              updateProductPrice(item.productId, item.price, { ribbon: item.ribbon, discount: item.discount })
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

      // ── STEP 4c: Apply collection updates ────────────────────────────────
      let colFail = 0;
      let descuentosAddOk = 0; let descuentosRemOk = 0;
      let descuento10AddOk = 0; let descuento10RemOk = 0;

      const applyCol = async (colId: string | null, add: string[], rem: string[], name: string): Promise<void> => {
        if (!colId) return;
        try {
          if (add.length > 0) {
            await addProductsToCollection(colId, add);
            logger.info(CTX, `${name}: agregados ${add.length} producto(s)`);
            if (name === 'Descuentos') descuentosAddOk  = add.length;
            else                      descuento10AddOk = add.length;
          }
          if (rem.length > 0) {
            await removeProductsFromCollection(colId, rem);
            logger.info(CTX, `${name}: removidos ${rem.length} producto(s)`);
            if (name === 'Descuentos') descuentosRemOk  = rem.length;
            else                      descuento10RemOk = rem.length;
          }
        } catch (err) {
          colFail++;
          logger.error(CTX, `Error actualizando colección ${name}: ${err instanceof Error ? err.message : String(err)}`);
        }
      };

      await applyCol(descuentosId,  descuentosAdd,  descuentosRemove,  'Descuentos');
      await applyCol(descuento10Id, descuento10Add, descuento10Remove, 'Descuento10');

      const colAddOk = descuentosAddOk + descuento10AddOk;
      const colRemOk = descuentosRemOk + descuento10RemOk;

      logger.info(CTX, `${modeLabel} Sync complete — Inventory: ${invOk} ok, ${invFail} failed (${blocked} blocked at 0) | Prices: ${priceOk} ok, ${priceFail} failed | Descuentos: +${descuentosAddOk} -${descuentosRemOk} | Descuento10: +${descuento10AddOk} -${descuento10RemOk} ${colFail > 0 ? '(error)' : ''} | Skipped: ${skipped}`);

      // ── STEP 5: Send email report ─────────────────────────────────────────
      if (!this.testLimit) {
        await sendSyncReport({ invReport, priceReport, invOk, invFail, priceOk, priceFail, skipped, blocked, descuentosAddOk, descuentosRemOk, descuento10AddOk, descuento10RemOk, colFail, modeLabel, minThreshold });
      }
    }

  }
}

// ─── Email report helper ─────────────────────────────────────────────────────

interface InvReportRow  { sku: string; name: string; prevStock: number; newStock: number; blocked: boolean; failed: boolean; }
interface PriceReportRow { sku: string; name: string; prevPrice: number; newPrice: number; priceDown: boolean; failed: boolean; isPromo: boolean; wasPromo: boolean; }

async function sendSyncReport(opts: {
  invReport: InvReportRow[];
  priceReport: PriceReportRow[];
  invOk: number; invFail: number;
  priceOk: number; priceFail: number;
  skipped: number; blocked: number;
  descuentosAddOk: number; descuentosRemOk: number;
  descuento10AddOk: number; descuento10RemOk: number;
  colFail: number;
  modeLabel: string; minThreshold: number;
}): Promise<void> {
  const recipients = getEmailsForTask('erpPostgresSync');
  if (recipients.length === 0) {
    logger.warn(CTX, 'No email recipients configured for erpPostgresSync — skipping report');
    return;
  }

  const { invReport, priceReport, invOk, invFail, priceOk, priceFail, skipped, blocked, descuentosAddOk, descuentosRemOk, descuento10AddOk, descuento10RemOk, colFail, modeLabel, minThreshold } = opts;
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
    details{background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:12px;overflow:hidden}
    details[open] summary{border-bottom:1px solid #e5e7eb}
    summary{display:flex;align-items:center;justify-content:space-between;padding:13px 16px;cursor:pointer;user-select:none;list-style:none;font-size:14px;font-weight:600}
    summary::-webkit-details-marker{display:none}
    .sec-title{display:flex;align-items:center;gap:10px}
    .sec-icon{font-size:16px}
    .sec-count{font-size:12px;font-weight:600;padding:2px 9px;border-radius:99px}
    .sec-count.blocked{background:#fef3c7;color:#92400e}
    .sec-count.stock{background:#dbeafe;color:#1e40af}
    .sec-count.price{background:#f3e8ff;color:#6b21a8}
    .sec-count.fail{background:#fee2e2;color:#991b1b}
    .sec-count.promo-new{background:#d1fae5;color:#065f46}
    .sec-count.promo-del{background:#fee2e2;color:#991b1b}
    .sec-count.promo-upd{background:#e0f2fe;color:#0369a1}
    .badge-promo-new{background:#d1fae5;color:#065f46}
    .badge-promo-del{background:#fee2e2;color:#991b1b}
    .chevron{font-size:12px;color:#9ca3af;transition:transform .2s}
    details[open] .chevron{transform:rotate(180deg)}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th{background:#f8fafc;text-align:left;padding:9px 14px;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#6b7280;font-weight:600;border-bottom:1px solid #e5e7eb}
    td{padding:9px 14px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
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
    .empty{padding:14px 16px;color:#9ca3af;font-size:13px}
    .footer{text-align:center;font-size:11px;color:#9ca3af;margin-top:24px}
  `;

  // ── Split inventory into blocked vs stock-changed ──
  const blockedRows    = invReport.filter(r => r.blocked);
  const stockRows      = invReport.filter(r => !r.blocked);

  // ── Split price report into promo-aware sections ──
  const promoNewRows = priceReport.filter(r =>  r.isPromo && !r.wasPromo);   // just added to promo
  const promoUpdRows = priceReport.filter(r =>  r.isPromo &&  r.wasPromo);   // was promo, price/discount changed
  const promoDelRows = priceReport.filter(r => !r.isPromo &&  r.wasPromo);   // removed from promo
  const priceDownRows = priceReport.filter(r => !r.isPromo && !r.wasPromo && r.priceDown);
  const priceUpRows   = priceReport.filter(r => !r.isPromo && !r.wasPromo && !r.priceDown);

  const buildInvRow = (r: InvReportRow) => {
    const badge = r.failed ? '<span class="badge badge-fail">ERROR</span>' : '';
    const stockChange = `<span class="num">${r.prevStock}</span><span class="arrow">→</span><span class="num ${r.newStock < r.prevStock ? 'down' : 'up'}">${r.newStock}</span>`;
    return `<tr><td class="num">${r.sku}</td><td>${r.name}</td><td>${stockChange}</td><td>${badge}</td></tr>`;
  };

  const buildPriceRow = (r: PriceReportRow) => {
    let badge = '';
    if (r.failed)       badge = '<span class="badge badge-fail">ERROR</span>';
    else if (r.isPromo && !r.wasPromo)  badge = '<span class="badge badge-promo-new">🆕 PROMO</span>';
    else if (r.isPromo &&  r.wasPromo)  badge = '<span class="badge badge-blocked">🏷 PROMO</span>';
    else if (!r.isPromo && r.wasPromo)  badge = '<span class="badge badge-promo-del">❌ Sin promo</span>';
    const priceChange = `<span class="num">${fmt(r.prevPrice)}</span><span class="arrow">→</span><span class="num ${r.priceDown ? 'down' : 'up'}">${fmt(r.newPrice)}</span>`;
    return `<tr><td class="num">${r.sku}</td><td>${r.name}</td><td>${priceChange}</td><td>${badge}</td></tr>`;
  };

  const invHeaders   = '<thead><tr><th>SKU</th><th>Producto</th><th>Stock (anterior → nuevo)</th><th></th></tr></thead>';
  const priceHeaders = '<thead><tr><th>SKU</th><th>Producto</th><th>Precio (anterior → nuevo)</th><th></th></tr></thead>';

  const section = (id: string, icon: string, title: string, countClass: string, count: number, openByDefault: boolean, content: string) => `
    <details id="${id}"${openByDefault ? ' open' : ''}>
      <summary>
        <span class="sec-title"><span class="sec-icon">${icon}</span>${title}<span class="sec-count ${countClass}">${count}</span></span>
        <span class="chevron">▼</span>
      </summary>
      ${content}
    </details>`;

  const tableOrEmpty = (rows: string, headers: string, empty: string) =>
    rows.length === 0
      ? `<div class="empty">${empty}</div>`
      : `<table>${headers}<tbody>${rows}</tbody></table>`;

  const totalFails = invFail + priceFail;

  const secBlocked = section('sec-blocked', '⛔', 'Bloqueados (stock → 0)', 'blocked', blockedRows.length, true,
    tableOrEmpty(blockedRows.map(buildInvRow).join(''), invHeaders, 'Ningún producto bloqueado.'));

  const secStock = section('sec-stock', '📦', 'Cambios de Stock', 'stock', stockRows.length, true,
    tableOrEmpty(stockRows.map(buildInvRow).join(''), invHeaders, 'Sin cambios de stock.'));

  const secPromoNew = section('sec-promo-new', '🆕', 'Nuevas Promociones', 'promo-new', promoNewRows.length, true,
    tableOrEmpty(promoNewRows.map(buildPriceRow).join(''), priceHeaders, 'Sin nuevas promociones.'));

  const secPromoUpd = section('sec-promo-upd', '🏷', 'Promociones Actualizadas', 'promo-upd', promoUpdRows.length, false,
    tableOrEmpty(promoUpdRows.map(buildPriceRow).join(''), priceHeaders, 'Sin cambios en promociones activas.'));

  const secPromoDel = section('sec-promo-del', '❌', 'Promociones Eliminadas', 'promo-del', promoDelRows.length, true,
    tableOrEmpty(promoDelRows.map(buildPriceRow).join(''), priceHeaders, 'Ninguna promoción eliminada.'));

  const secPriceDown = section('sec-price-down', '↓', 'Precios que Bajaron', 'blocked', priceDownRows.length, false,
    tableOrEmpty(priceDownRows.map(buildPriceRow).join(''), priceHeaders, 'Ningún precio bajó.'));

  const secPriceUp = section('sec-price-up', '↑', 'Precios que Subieron', 'price', priceUpRows.length, false,
    tableOrEmpty(priceUpRows.map(buildPriceRow).join(''), priceHeaders, 'Ningún precio subió.'));

  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><style>${css}</style></head><body>
  <div class="wrap">
    <h1>Reporte de Sincronización Wix ${modeLabel}</h1>
    <div class="sub">${now} — umbral mínimo de stock: ${minThreshold}</div>

    <div class="summary">
      <div class="stat ok"><div class="stat-val">${invOk}</div><div class="stat-lbl">Inventario actualizado</div></div>
      <div class="stat warn"><div class="stat-val">${blocked}</div><div class="stat-lbl">Desactivados (stock=0)</div></div>
      <div class="stat ok"><div class="stat-val">${priceOk}</div><div class="stat-lbl">Precios actualizados</div></div>
      <div class="stat ${colFail > 0 ? 'danger' : 'ok'}"><div class="stat-val">+${descuentosAddOk} / -${descuentosRemOk}</div><div class="stat-lbl">Promos (Descuentos)</div></div>
      <div class="stat ${colFail > 0 ? 'danger' : 'ok'}"><div class="stat-val">+${descuento10AddOk} / -${descuento10RemOk}</div><div class="stat-lbl">Productos para cupón Descuento10</div></div>
      <div class="stat ${totalFails > 0 ? 'danger' : 'ok'}"><div class="stat-val">${totalFails}</div><div class="stat-lbl">Errores</div></div>
      <div class="stat"><div class="stat-val">${skipped}</div><div class="stat-lbl">Omitidos</div></div>
    </div>

    ${secBlocked}
    ${secStock}
    ${secPromoNew}
    ${secPromoUpd}
    ${secPromoDel}
    ${secPriceDown}
    ${secPriceUp}

    <div class="footer">Generado automáticamente por cron wix-tasks · price-inventory-sync</div>
  </div>
  </body></html>`;

  const totalChanges = invReport.length + priceReport.length;
  const subject = `Sincronizar ERP con ecommerce (Wix) ${modeLabel} — ${totalChanges} cambio(s) · ${invFail + priceFail > 0 ? `⚠ ${invFail + priceFail} error(es)` : '✓ sin errores'} · ${now}`;

  try {
    await sendEmail({ to: recipients, subject, html });
    logger.info(CTX, `Sync report sent to ${recipients.join(', ')}`);
  } catch (err) {
    logger.warn(CTX, `Failed to send sync report: ${err instanceof Error ? err.message : String(err)}`);
  }

  const teamsWebhook = config.task.teamsWebhook;
  if (teamsWebhook) {
    const promoNewRows = priceReport.filter(r => r.isPromo && !r.wasPromo);
    const promoDelRows = priceReport.filter(r => !r.isPromo && r.wasPromo);
    try {
      await sendTeamsSyncNotification(teamsWebhook, {
        modeLabel, now,
        invOk, invFail, blocked,
        priceOk, priceFail,
        descuentosAddOk, descuentosRemOk,
        descuento10AddOk, descuento10RemOk,
        colFail, skipped,
        promoNew: promoNewRows.length,
        promoDel: promoDelRows.length,
      });
    } catch (err) {
      logger.warn(CTX, `Failed to send Teams notification: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
}
