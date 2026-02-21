import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { mssqlQuery } from '../services/mssql';
import { query as pgQuery } from '../services/database';
import { executeKw, searchReadAll, OdooRecord } from '../services/odoo';
import { sendTeamsNotification } from '../services/teams';

const CTX = 'OdooPriceSync';
const EMP_ID = config.mssql.empId;

// ── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientXmlRpcError(err: unknown): boolean {
  const msg = (err instanceof Error ? err.message : String(err)).toLowerCase();
  return (
    msg.includes("unknown xml-rpc tag 'title'")
    || msg.includes('socket hang up')
    || msg.includes('econnreset')
    || msg.includes('econnrefused')
    || msg.includes('etimedout')
    || msg.includes('timeout')
    || msg.includes('502')
    || msg.includes('503')
    || msg.includes('504')
  );
}

// ── MSSQL row ────────────────────────────────────────────────────────────────

interface MssqlPriceRow {
  Articulo_Id: string;
  Articulo_Precio1: number;
  AxS_Costo_Actual: number;
}

// ── PostgreSQL promo row ──────────────────────────────────────────────────────

interface PromoRow {
  sku: string;
  precio_promo: string;  // numeric from pg, includes taxes
}

// ═════════════════════════════════════════════════════════════════════════════
// TASK
// ═════════════════════════════════════════════════════════════════════════════

export class OdooPriceSyncTask extends BaseTask {
  readonly name = 'odoo-price-sync';
  readonly description = 'Sincronización rápida de precios y costos desde MSSQL (ERP) hacia Odoo. Solo actualiza list_price y standard_price sin tocar categorías, stock ni imágenes.';
  readonly cronExpression = '45 * * * *'; // Every hour at :45

  private get WRITE_CONCURRENCY() { return Math.max(1, config.odoo.productWriteConcurrency); }
  private get WRITE_MAX_RETRIES() { return Math.max(1, config.odoo.productWriteRetries); }

  async execute(): Promise<void> {
    const t0 = Date.now();
    logger.info(CTX, '─── Starting price/cost sync ───');

    // ── 1. Fetch prices from MSSQL ──────────────────────────────────────
    const SUC_WIX = config.sucursalWix;
    logger.info(CTX, `  Fetching prices from MSSQL (Suc_Codigo_Externo=${SUC_WIX})...`);
    const mssqlStart = Date.now();
    const rows = await mssqlQuery<MssqlPriceRow>(`
      SELECT
        a.Articulo_Id,
        a.Articulo_Precio1,
        ISNULL(axs.AxS_Costo_Actual, a.Articulo_Costo_Actual) AS AxS_Costo_Actual
      FROM Articulo a WITH (NOLOCK)
        LEFT JOIN Sucursal suc WITH (NOLOCK)
          ON suc.Emp_Id = a.Emp_Id AND suc.Suc_Codigo_Externo = '${SUC_WIX}'
        LEFT JOIN Articulo_x_Sucursal axs WITH (NOLOCK)
          ON axs.Emp_Id = a.Emp_Id AND axs.Articulo_Id = a.Articulo_Id
             AND axs.Suc_Id = suc.Suc_Id
      WHERE a.Emp_Id = ${EMP_ID}
        AND a.Articulo_Activo_Venta = 1
        AND a.Articulo_Nombre NOT LIKE '(INS)%'
    `);
    logger.info(CTX, `  MSSQL: ${rows.length} articles in ${((Date.now() - mssqlStart) / 1000).toFixed(1)}s`);

    // Index by SKU
    const mssqlPrices = new Map<string, { listPrice: number; standardPrice: number }>();
    for (const r of rows) {
      const sku = (r.Articulo_Id || '').trim();
      if (!sku) continue;
      mssqlPrices.set(sku, {
        listPrice: r.Articulo_Precio1 || 0,
        standardPrice: r.AxS_Costo_Actual || 0,
      });
    }

    // ── 1b. Fetch promo prices from PostgreSQL ──────────────────────────────
    logger.info(CTX, `  Fetching promo prices from PostgreSQL (sucursal=${SUC_WIX})...`);
    const promoResult = await pgQuery<PromoRow>(
      `SELECT sku, precio_promo::text AS precio_promo
       FROM public.promo
       WHERE sucursal = $1
       -- TODO: add when column exists: AND (vigencia IS NULL OR vigencia >= CURRENT_DATE)`,
      [SUC_WIX],
    );
    const promoMap = new Map<string, number>();
    for (const r of promoResult.rows) {
      const precioPromo = parseFloat(r.precio_promo);
      if (r.sku && !isNaN(precioPromo) && precioPromo > 0) {
        promoMap.set(r.sku.trim(), precioPromo);
      }
    }
    logger.info(CTX, `  PostgreSQL promo: ${promoMap.size} SKU(s) with promo price`);

    // ── 2. Fetch current Odoo prices ────────────────────────────────────
    logger.info(CTX, '  Fetching Odoo product prices...');
    const odooStart = Date.now();
    const odooProducts = await searchReadAll(
      'product.template',
      [['default_code', '!=', false]],
      ['id', 'default_code', 'list_price', 'standard_price'],
      { batchSize: 500 },
    );
    logger.info(CTX, `  Odoo: ${odooProducts.length} products in ${((Date.now() - odooStart) / 1000).toFixed(1)}s`);

    // ── 3. Compute diff ─────────────────────────────────────────────────
    const toUpdate: { odooId: number; changes: Record<string, number> }[] = [];

    for (const p of odooProducts) {
      const sku = (p.default_code as string || '').trim();
      if (!sku) continue;
      const erp = mssqlPrices.get(sku);
      if (!erp) continue;

      // Promo overrides list_price; precio_promo already includes taxes
      const promoPrice = promoMap.get(sku);
      const expectedListPrice = promoPrice ?? erp.listPrice;

      const changes: Record<string, number> = {};
      if (Math.abs((p.list_price as number || 0) - expectedListPrice) > 0.01) {
        changes.list_price = expectedListPrice;
      }
      if (Math.abs((p.standard_price as number || 0) - erp.standardPrice) > 0.01) {
        changes.standard_price = erp.standardPrice;
      }
      if (Object.keys(changes).length > 0) {
        toUpdate.push({ odooId: p.id, changes });
      }
    }

    if (toUpdate.length === 0) {
      logger.info(CTX, '  No price/cost changes detected');
      logger.info(CTX, `─── Price sync done in ${((Date.now() - t0) / 1000).toFixed(1)}s ───`);
      return;
    }

    logger.info(CTX, `  ${toUpdate.length} products need price/cost update`);

    // ── 4. Apply updates with concurrent workers ────────────────────────
    let updated = 0;
    let errors = 0;
    const queue = [...toUpdate];
    const concurrency = this.WRITE_CONCURRENCY;
    const maxRetries = this.WRITE_MAX_RETRIES;

    const workers = Array.from({ length: concurrency }, () => (async () => {
      while (queue.length > 0) {
        const item = queue.shift();
        if (!item) break;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
          try {
            await executeKw<boolean>('product.template', 'write', [[item.odooId], item.changes]);
            updated++;
            break;
          } catch (err) {
            if (attempt < maxRetries && isTransientXmlRpcError(err)) {
              await sleep(250 * attempt);
              continue;
            }
            logger.error(CTX, `  Write failed id=${item.odooId}: ${(err as Error).message}`);
            errors++;
            break;
          }
        }
      }
    })());

    // Progress reporter
    const progressInterval = setInterval(() => {
      const done = updated + errors;
      if (done > 0) {
        logger.info(CTX, `  Progress: ${done}/${toUpdate.length} (${errors} errors)`);
      }
    }, 5000);

    await Promise.all(workers);
    clearInterval(progressInterval);

    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    logger.info(CTX, `  Updated: ${updated}, Errors: ${errors}`);
    logger.info(CTX, `─── Price sync done in ${elapsed}s ───`);

    const now = new Date().toLocaleString('es-MX', { timeZone: 'America/Mexico_City', hour12: false });
    await sendTeamsNotification({
      title: `Odoo Price Sync — ${elapsed}s`,
      subtitle: now,
      hasErrors: errors > 0,
      rows: [
        { name: '📦 Artículos en ERP',      value: String(rows.length) },
        { name: '🏷 SKUs en promo',          value: String(promoMap.size) },
        { name: '💲 Precios actualizados',   value: String(updated) },
        { name: '🚨 Errores',               value: String(errors) },
        { name: '⏱ Tiempo',                 value: `${elapsed}s` },
      ],
    });
  }
}
