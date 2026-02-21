import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { mssqlQuery } from '../services/mssql';
import { executeKw, searchAllIds, searchReadAll, readRecords, OdooRecord } from '../services/odoo';
import { sendTeamsNotification } from '../services/teams';

const CTX = 'OdooInventorySync';
const EMP_ID = config.mssql.empId;

// ═══════════════════════════════════════════════════════════════════════════════
// Data‑integrity notes (audit 2025‑07):
//   • Stock lives in Articulo_x_Bodega (NOT Articulo_x_Sucursal.Axs_Existencia = 0)
//   • Warehouse 3‑digit code = Sucursal.Suc_Codigo_Externo (101‑403)
//   • SKU = Articulo_Id = Articulo_Codigo_Principal (100% identical)
//   • Barcode = Articulo_Codigo_Interno when different from SKU (only 7 / 12 495)
//   • Categoria_Articulo.Depto_Id = 0 for all rows; hierarchy via Articulo only
//   • Marca (96% "General"), Casa (95% "General") → not synced as entities
//   • eCommerce flag, Custom1‑5, Codigo_Conjunto/Hijo/Padre → always empty/0
//   • Articulo_Division / Articulo_Grupo / Articulo_Clasificacion → 0 rows
//   • Inv_Cola_Existencias → 0 rows
//   • Images in Articulo_Imagen_FS: 6 616 GRA principal, 5 415 active articles
//   • Articulo_Fec_Actualizacion is unreliable (bulk‑updated); diff is 100% field‑based
// ═══════════════════════════════════════════════════════════════════════════════

// ── MSSQL row interfaces ────────────────────────────────────────────────────

interface MssqlArticleRow {
  Articulo_Id: string;
  Articulo_Nombre: string;
  Articulo_Codigo_Interno: string;
  Depto_Id: number;
  Depto_Nombre: string;
  Categoria_Id: number;
  Categoria_Nombre: string;
  SubCategoria_Id: number;
  SubCategoria_Nombre: string;
  Unidad_Id: number;
  Unidad_Nombre: string;
  Articulo_Precio1: number;
  AxS_Costo_Actual: number;
  Articulo_Fec_Actualizacion: string;
  Articulo_Activo: boolean;
}

interface MssqlBarcodeRow {
  Articulo_Id: string;
  Equivalente_Id: string;
  Equivalente_Principal: boolean;
}

interface MssqlStockRow {
  Articulo_Id: string;
  Suc_Id: number;
  Suc_Codigo_Externo: string;
  Suc_Nombre: string;
  total_existencia: number;
}

interface MssqlImageMetaRow {
  Articulo_Id: string;
  img_size: number;
  Fecha: string;
}

interface MssqlImageBlobRow {
  Articulo_Id: string;
  Imagen: Buffer;
}

// ── Normalized structures ────────────────────────────────────────────────────

interface NormalizedArticle {
  sku: string;          // Articulo_Id — primary key
  barcode: string;      // Articulo_Codigo_Interno when ≠ SKU, otherwise SKU
  name: string;
  deptoKey: string;
  categoriaKey: string;
  subCategoriaKey: string;
  uomName: string;
  listPrice: number;
  standardPrice: number;
  updatedAt: string;
  active: boolean;
  extraBarcodes: string[];
  // stock per branch keyed by Suc_Codigo_Externo (3‑digit code)
  stockByBranch: Map<string, { qty: number; branchName: string }>;
  hash: string;
}

interface CategoryNode {
  mssqlKey: string;
  name: string;
  parentKey: string | null;
  odooId?: number;
}

// ── Batch helper ─────────────────────────────────────────────────────────────
function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

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

// ── UOM name → Odoo uom.uom id ──────────────────────────────────────────────
const UOM_NAME_MAP: Record<string, number> = {
  'PIEZA': 1,     'SIN DEFINIR': 1,
  'BOLSA': 1,     'BULTO/ATADO': 1,
  'CAJA': 1,      'CUBETA': 1,
  'HOJA': 1,      'JUEGO': 1,
  'PAQUETE': 1,   'ROLLO': 1,
  'SACO': 1,      'TIBOR': 1,
  'CARRETE': 1,   'PACA': 1,
  'GALON': 24,    // gal (US)
  'KILO': 12,     // kg
  'LITRO': 10,    // L
  'METRO CUADRADO': 9,   // m²
  'METRO CUBICO': 11,    // m³
  'METRO LINEAL': 5,     // m
  'PIE CUADRADO': 21,    // ft²
  'PIE LINEAL': 18,      // ft
};

function resolveUomId(uomName: string): number {
  return UOM_NAME_MAP[uomName.toUpperCase().trim()] ?? 1;
}

function computeHash(a: { name: string; listPrice: number; standardPrice: number; subCategoriaKey: string; barcode: string }): string {
  return `${a.name}|${a.listPrice.toFixed(4)}|${a.standardPrice.toFixed(4)}|${a.subCategoriaKey}|${a.barcode}`;
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN TASK
// ═════════════════════════════════════════════════════════════════════════════

export type SyncMode = 'full' | 'stock-only';

export class OdooInventorySyncTask extends BaseTask {
  readonly name: string;
  readonly description: string;
  readonly cronExpression: string;
  readonly syncMode: SyncMode;

  // Set via DRY_RUN=1 env var or programmatically — logs what would change without writing to Odoo
  dryRun = process.env.DRY_RUN === '1' || process.env.DRY_RUN === 'true';

  constructor(mode: SyncMode = 'full') {
    super();
    this.syncMode = mode;
    if (mode === 'stock-only') {
      this.name = 'odoo-inventory-sync-stock';
      this.description = 'Sincronización rápida de existencias desde MSSQL (ERP) hacia Odoo. Solo actualiza cantidades de stock por sucursal/bodega sin tocar productos ni imágenes.';
      this.cronExpression = '15 * * * *';   // Every hour at :15
    } else {
      this.name = 'odoo-inventory-sync-full';
      this.description = 'Sincronización completa de inventario desde MSSQL (ERP) hacia Odoo. Incluye productos, categorías, precios, stock por sucursal, imágenes y códigos de barras.';
      this.cronExpression = '0 4 * * *';    // Daily at 4:00 AM
    }
  }

  private phaseErrors: { phase: string; error: string }[] = [];
  private phaseTimes: { phase: string; ms: number }[] = [];

  private async runPhase<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const t0 = Date.now();
    try {
      const result = await fn();
      this.phaseTimes.push({ phase: name, ms: Date.now() - t0 });
      return result;
    } catch (err) {
      const ms = Date.now() - t0;
      this.phaseTimes.push({ phase: name, ms });
      this.phaseErrors.push({ phase: name, error: (err as Error).message });
      throw err;
    }
  }

  private printDiagnostics(): void {
    logger.info(CTX, '┌─── Diagnostics ───────────────────────────────────');
    for (const pt of this.phaseTimes) {
      const status = this.phaseErrors.find(e => e.phase === pt.phase) ? '✗' : '✓';
      logger.info(CTX, `│ ${status} ${pt.phase.padEnd(40)} ${(pt.ms / 1000).toFixed(2)}s`);
    }
    if (this.phaseErrors.length > 0) {
      logger.info(CTX, '├─── Errors ────────────────────────────────────────');
      for (const pe of this.phaseErrors) {
        logger.info(CTX, `│ ✗ ${pe.phase}: ${pe.error}`);
      }
    }
    logger.info(CTX, '└───────────────────────────────────────────────────');
  }

  async execute(): Promise<void> {
    if (this.syncMode === 'stock-only') {
      return this.executeStockOnly();
    }
    return this.executeFull();
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FULL SYNC — daily at 4 AM
  //   Categories, product creates/updates/archives, stock, images, barcodes, POS
  // ═══════════════════════════════════════════════════════════════════════════

  private async executeFull(): Promise<void> {
    const t0 = Date.now();
    this.phaseErrors = [];
    this.phaseTimes = [];

    logger.info(CTX, '═══ FULL SYNC (daily) ═══');
    if (this.dryRun) {
      logger.info(CTX, '⚠ DRY RUN — no writes will be made to Odoo');
    }

    // ── Phase 1: Extract from MSSQL (2 read‑only queries, parallel) ──────
    logger.info(CTX, '─── Phase 1: Extracting data from MSSQL (read‑only) ───');
    const [articles, stockRows, barcodeRows] = await this.runPhase('1. MSSQL extract', () =>
      Promise.all([this.fetchArticles(), this.fetchStock(), this.fetchBarcodes()]),
    );
    logger.info(CTX, `  MSSQL: ${articles.length} articles, ${stockRows.length} stock rows, ${barcodeRows.length} barcodes`);

    // ── Phase 2: Normalize & index MSSQL data ────────────────────────────
    logger.info(CTX, '─── Phase 2: Normalizing data ───');
    const { articleMap, categoryTree, branchCodes } = await this.runPhase('2. Normalize', async () =>
      this.normalizeData(articles, stockRows, barcodeRows),
    );
    logger.info(CTX, `  Normalized: ${articleMap.size} articles, ${categoryTree.size} categories, ${branchCodes.size} branches`);

    // ── Phase 3: Sync categories to Odoo ─────────────────────────────────
    logger.info(CTX, '─── Phase 3: Syncing categories to Odoo ───');
    const categoryIdMap = await this.runPhase('3. Categories', () =>
      this.syncCategories(categoryTree),
    );
    logger.info(CTX, `  Categories synced: ${categoryIdMap.size} mapped`);

    // ── Phase 3b: Sync POS categories to Odoo ────────────────────────────
    logger.info(CTX, '─── Phase 3b: Syncing POS categories to Odoo ───');
    const posCategoryIdMap = await this.runPhase('3b. POS Categories', () =>
      this.syncPosCategories(categoryTree),
    );
    logger.info(CTX, `  POS Categories synced: ${posCategoryIdMap.size} mapped`);

    // ── Phase 4: Ensure Odoo warehouses exist for each branch ────────────
    logger.info(CTX, '─── Phase 4: Syncing warehouses to Odoo ───');
    const warehouseLocMap = await this.runPhase('4. Warehouses', () =>
      this.syncWarehouses(branchCodes),
    );
    logger.info(CTX, `  Warehouses synced: ${warehouseLocMap.size} mapped`);

    // ── Phase 5: Read current Odoo products ──────────────────────────────
    logger.info(CTX, '─── Phase 5: Reading current Odoo products ───');
    const odooProducts = await this.runPhase('5. Fetch Odoo products', () =>
      this.fetchOdooProducts(),
    );
    logger.info(CTX, `  Odoo: ${odooProducts.length} existing products`);

    // ── Phase 6: Compute diff ────────────────────────────────────────────
    logger.info(CTX, '─── Phase 6: Computing diff ───');
    const diff = await this.runPhase('6. Compute diff', async () =>
      this.computeDiff(articleMap, odooProducts, categoryIdMap, posCategoryIdMap),
    );
    logger.info(CTX, `  Diff: ${diff.toCreate.length} new, ${diff.toUpdate.length} update, ${diff.toArchive.length} archive`);

    if (this.dryRun) {
      this.printDryRunDiff(diff, categoryIdMap);
    }

    // ── Phase 7: Apply product changes to Odoo (batch) ───────────────────
    logger.info(CTX, '─── Phase 7: Applying product changes to Odoo ───');
    await this.runPhase('7a. Creates', () => this.applyCreates(diff.toCreate, categoryIdMap, posCategoryIdMap));
    await this.runPhase('7b. Updates', () => this.applyUpdates(diff.toUpdate));
    // NOTE: Archives are now handled within computeDiff/applyUpdates as active=false updates
    // if we want to explicitly archive, we can still use applyArchives, but with the new logic
    // we primarily update 'active' status based on MSSQL.
    // However, if a product is NOT in MSSQL at all, it should probably be archived?
    // Current logic in computeDiff: "Archive products no longer active in MSSQL" -> actually "no longer PRESENT in MSSQL"
    // Since we now fetch ALL articles, if it's not in articleMap, it is deleted from MSSQL.
    if (diff.toArchive.length > 0) {
        await this.runPhase('7c. Archives', () => this.applyArchives(diff.toArchive));
    }

    // ── Phase 8: Sync stock levels per warehouse ─────────────────────────
    logger.info(CTX, '─── Phase 8: Syncing stock levels per warehouse ───');
    await this.runPhase('8. Stock sync', () =>
      this.syncStockLevels(articleMap, warehouseLocMap),
    );

    // ── Phase 9: Sync product images ─────────────────────────────────────
    logger.info(CTX, '─── Phase 9: Syncing product images ───');
    await this.runPhase('9. Image sync', () =>
      this.syncImages(articleMap),
    );

    // ── Phase 10: Sync additional barcodes (Packaging) ───────────────────
    logger.info(CTX, '─── Phase 10: Syncing additional barcodes ───');
    await this.runPhase('10. Barcodes', () =>
      this.syncAdditionalBarcodes(articleMap, odooProducts),
    );

    const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
    logger.info(CTX, `═══ Full sync complete in ${elapsed}s ═══`);
    this.printDiagnostics();

    const now = new Date().toLocaleString('es-MX', { timeZone: 'America/Mexico_City', hour12: false });
    await sendTeamsNotification({
      title: `Odoo Full Sync — ${elapsed}s`,
      subtitle: now,
      hasErrors: this.phaseErrors.length > 0,
      rows: [
        { name: '📦 Artículos ERP',          value: String(articleMap.size) },
        { name: '➕ Crear en Odoo',           value: String(diff.toCreate.length) },
        { name: '✏️ Actualizar en Odoo',      value: String(diff.toUpdate.length) },
        { name: '🗄 Archivar en Odoo',        value: String(diff.toArchive.length) },
        { name: '🚨 Fases con error',         value: String(this.phaseErrors.length) },
        { name: '⏱ Tiempo total',             value: `${elapsed}s` },
      ],
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // STOCK-ONLY SYNC — hourly (lightweight)
  //   Only fetches stock from MSSQL and updates Odoo quants per warehouse.
  //   Skips: categories, product creates/updates/archives, images, barcodes.
  // ═══════════════════════════════════════════════════════════════════════════

  private async executeStockOnly(): Promise<void> {
    const t0 = Date.now();
    this.phaseErrors = [];
    this.phaseTimes = [];

    logger.info(CTX, '═══ STOCK-ONLY SYNC (hourly) ═══');
    if (this.dryRun) {
      logger.info(CTX, '⚠ DRY RUN — no writes will be made to Odoo');
    }

    // ── Step 1: Fetch stock rows from MSSQL ──────────────────────────────
    logger.info(CTX, '─── Step 1: Fetching stock from MSSQL ───');
    const stockRows = await this.runPhase('1. MSSQL stock extract', () =>
      this.fetchStock(),
    );
    logger.info(CTX, `  MSSQL: ${stockRows.length} stock rows`);

    // ── Step 2: Index stock by SKU and collect branch codes ──────────────
    logger.info(CTX, '─── Step 2: Indexing stock data ───');
    const branchCodes = new Map<string, string>();
    const stockBySku = new Map<string, Map<string, { qty: number; branchName: string }>>();

    for (const row of stockRows) {
      const sku = (row.Articulo_Id || '').trim();
      if (!sku) continue;
      const extCode = (row.Suc_Codigo_Externo || '').trim();
      if (!extCode) continue;

      branchCodes.set(extCode, (row.Suc_Nombre || '').trim());

      if (!stockBySku.has(sku)) stockBySku.set(sku, new Map());
      stockBySku.get(sku)!.set(extCode, {
        qty: row.total_existencia,
        branchName: (row.Suc_Nombre || '').trim(),
      });
    }
    logger.info(CTX, `  Indexed: ${stockBySku.size} SKUs across ${branchCodes.size} branches`);

    // ── Step 3: Resolve warehouse locations (read-only lookup) ───────────
    logger.info(CTX, '─── Step 3: Resolving warehouse locations ───');
    const warehouseLocMap = await this.runPhase('3. Warehouses', () =>
      this.syncWarehouses(branchCodes),
    );
    logger.info(CTX, `  Warehouses: ${warehouseLocMap.size} mapped`);

    // ── Step 4: Build a minimal articleMap for syncStockLevels ────────────
    // syncStockLevels expects Map<string, NormalizedArticle> but only uses
    // sku and stockByBranch. We build lightweight stubs to reuse that method.
    const articleMap = new Map<string, NormalizedArticle>();
    for (const [sku, branches] of stockBySku) {
      articleMap.set(sku, {
        sku,
        barcode: '',
        extraBarcodes: [],
        name: '',
        deptoKey: '',
        categoriaKey: '',
        subCategoriaKey: '',
        uomName: '',
        listPrice: 0,
        standardPrice: 0,
        updatedAt: '',
        active: true,
        stockByBranch: branches,
        hash: '',
      });
    }

    // ── Step 5: Sync stock levels per warehouse ──────────────────────────
    logger.info(CTX, '─── Step 4: Syncing stock levels per warehouse ───');
    await this.runPhase('4. Stock sync', () =>
      this.syncStockLevels(articleMap, warehouseLocMap),
    );

    const elapsed = ((Date.now() - t0) / 1000).toFixed(2);
    logger.info(CTX, `═══ Stock-only sync complete in ${elapsed}s ═══`);
    this.printDiagnostics();

    const now = new Date().toLocaleString('es-MX', { timeZone: 'America/Mexico_City', hour12: false });
    await sendTeamsNotification({
      title: `Odoo Stock Sync — ${elapsed}s`,
      subtitle: now,
      hasErrors: this.phaseErrors.length > 0,
      rows: [
        { name: '📦 Filas de stock MSSQL',    value: String(stockRows.length) },
        { name: '🚨 Fases con error',         value: String(this.phaseErrors.length) },
        { name: '⏱ Tiempo total',             value: `${elapsed}s` },
      ],
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SYNC ADDITIONAL BARCODES (product.packaging)
  // ═══════════════════════════════════════════════════════════════════════════

  private async syncAdditionalBarcodes(
    articleMap: Map<string, NormalizedArticle>,
    odooProducts: OdooRecord[],
  ): Promise<void> {
    // 1. Map SKU -> Odoo Product ID
    const skuToProductId = new Map<string, number>();
    for (const p of odooProducts) {
      const code = (p.default_code as string || '').trim();
      if (code) skuToProductId.set(code, p.id);
    }

    // 2. Fetch all existing packagings
    const existingPackagings = await searchReadAll('product.packaging', [], ['id', 'product_id', 'barcode', 'name', 'qty']);
    
    const packagingByProduct = new Map<number, OdooRecord[]>();
    for (const pkg of existingPackagings) {
      const pid = (pkg.product_id as [number, string])[0];
      if (!packagingByProduct.has(pid)) packagingByProduct.set(pid, []);
      packagingByProduct.get(pid)!.push(pkg);
    }

    const toCreate: any[] = [];
    const toDeleteIds: number[] = [];

    for (const [sku, article] of articleMap) {
      const pid = skuToProductId.get(sku);
      if (!pid) continue; // Product not in Odoo yet

      // Expected barcodes (exclude main barcode which is on product itself)
      const expectedBarcodes = new Set(article.extraBarcodes);
      
      const currentPkgs = packagingByProduct.get(pid) || [];
      
      // Identify what to keep and what to delete
      for (const pkg of currentPkgs) {
        const b = (pkg.barcode as string || '').trim();
        // If this packaging has a barcode that is expected
        if (b && expectedBarcodes.has(b)) {
            // Keep it.
            expectedBarcodes.delete(b); // Marked as found
        } else if (b) {
            // This packaging has a barcode NOT in expected list.
            // Only delete if qty=1 (assuming we manage these)
            if ((pkg.qty as number) === 1) {
                toDeleteIds.push(pkg.id);
            }
        }
      }

      // Create missing
      for (const b of expectedBarcodes) {
        toCreate.push({
            name: b, // Use barcode as name
            barcode: b,
            product_id: pid,
            qty: 1,
        });
      }
    }

    if (this.dryRun) {
        if (toCreate.length > 0) logger.info(CTX, `  [DRY RUN] Would create ${toCreate.length} packagings (barcodes)`);
        if (toDeleteIds.length > 0) logger.info(CTX, `  [DRY RUN] Would delete ${toDeleteIds.length} packagings (barcodes)`);
        return;
    }

    // Apply Deletes
    if (toDeleteIds.length > 0) {
        const batchSize = 500;
        for (let i = 0; i < toDeleteIds.length; i += batchSize) {
            const batch = toDeleteIds.slice(i, i + batchSize);
            await executeKw('product.packaging', 'unlink', [batch]);
        }
        logger.info(CTX, `  Deleted ${toDeleteIds.length} obsolete barcodes`);
    }

    // Apply Creates
    if (toCreate.length > 0) {
        const batches = chunk(toCreate, 100);
        let createdCount = 0;
        for (const batch of batches) {
            try {
                await executeKw('product.packaging', 'create', [batch]);
                createdCount += batch.length;
            } catch (err) {
                logger.error(CTX, `  Failed to create packaging batch: ${(err as Error).message}`);
            }
        }
        logger.info(CTX, `  Created ${createdCount} new barcodes`);
    }
  }

  private printDryRunDiff(
    diff: {
      toCreate: NormalizedArticle[];
      toUpdate: { odooId: number; article: NormalizedArticle; changes: Record<string, unknown> }[];
      toArchive: number[];
    },
    categoryIdMap: Map<string, number>,
  ): void {
    logger.info(CTX, '┌─── DRY RUN: Detailed diff ─────────────────────────');

    if (diff.toCreate.length > 0) {
      logger.info(CTX, `│ CREATE (${diff.toCreate.length}):`);
      for (const a of diff.toCreate.slice(0, 20)) {
        const categId = categoryIdMap.get(a.subCategoriaKey) ?? '?';
        logger.info(CTX, `│   + ${a.sku} "${a.name}" price=${a.listPrice} cost=${a.standardPrice} categ=${categId} barcode=${a.barcode}`);
      }
      if (diff.toCreate.length > 20) logger.info(CTX, `│   ... and ${diff.toCreate.length - 20} more`);
    }

    if (diff.toUpdate.length > 0) {
      logger.info(CTX, `│ UPDATE (${diff.toUpdate.length}):`);
      for (const u of diff.toUpdate.slice(0, 20)) {
        const fields = Object.keys(u.changes).join(', ');
        logger.info(CTX, `│   ~ ${u.article.sku} (odoo#${u.odooId}) → ${fields}`);
        for (const [k, v] of Object.entries(u.changes)) {
          logger.info(CTX, `│       ${k}: ${JSON.stringify(v)}`);
        }
      }
      if (diff.toUpdate.length > 20) logger.info(CTX, `│   ... and ${diff.toUpdate.length - 20} more`);
    }

    if (diff.toArchive.length > 0) {
      logger.info(CTX, `│ ARCHIVE (${diff.toArchive.length}):`);
      const ids = diff.toArchive.slice(0, 30).join(', ');
      logger.info(CTX, `│   odoo ids: ${ids}${diff.toArchive.length > 30 ? ' ...' : ''}`);
    }

    logger.info(CTX, '└────────────────────────────────────────────────────');
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // MSSQL QUERIES — read‑only queries (articles + stock parallel, images on demand)
  // ═══════════════════════════════════════════════════════════════════════════

  private async fetchArticles(): Promise<MssqlArticleRow[]> {
    // JOINs only the tables that actually contain data (audit‑verified).
    // Logic adapted from run_etl.js (syncprices):
    // - Use Articulo_Activo_Venta = 1
    // - Exclude names starting with (INS)
    // - Clean name: remove (XXXX) prefix if present (e.g. "(ACME) PRODUCTO")
    // - Cost: uses AxS_Costo_Actual from Articulo_x_Sucursal (real cost per branch)
    //   with fallback to Articulo_Costo_Actual when no branch record exists.
    //   Suc_Id is resolved from SUCURSAL_WIX (Suc_Codigo_Externo) at query time.
    const SUC_WIX = config.sucursalWix; // e.g. 101
    return mssqlQuery<MssqlArticleRow>(`
      SELECT
        a.Articulo_Id,
        CASE
            WHEN LEFT(a.Articulo_Nombre, 1) = '(' AND CHARINDEX(')', a.Articulo_Nombre) = 5
            THEN LTRIM(SUBSTRING(a.Articulo_Nombre, 6, 8000))
            ELSE a.Articulo_Nombre
        END AS Articulo_Nombre,
        a.Articulo_Codigo_Interno,
        a.Depto_Id,
        ISNULL(d.Depto_Nombre, 'Sin Definir')         AS Depto_Nombre,
        a.Categoria_Id,
        ISNULL(c.Categoria_Nombre, 'Sin Definir')     AS Categoria_Nombre,
        a.SubCategoria_Id,
        ISNULL(sc.SubCategoria_Nombre, 'Sin Definir') AS SubCategoria_Nombre,
        a.Unidad_Id,
        ISNULL(u.Unidad_Nombre, 'PIEZA')              AS Unidad_Nombre,
        a.Articulo_Precio1,
        ISNULL(axs.AxS_Costo_Actual, a.Articulo_Costo_Actual) AS AxS_Costo_Actual,
        a.Articulo_Fec_Actualizacion,
        CAST(a.Articulo_Activo_Venta AS BIT)          AS Articulo_Activo
      FROM Articulo a WITH (NOLOCK)
        LEFT JOIN Departamento d WITH (NOLOCK)
          ON d.Emp_Id = a.Emp_Id AND d.Depto_Id = a.Depto_Id
        LEFT JOIN Categoria_Articulo c WITH (NOLOCK)
          ON c.Emp_Id = a.Emp_Id AND c.Categoria_Id = a.Categoria_Id
        LEFT JOIN SubCategoria_Articulo sc WITH (NOLOCK)
          ON sc.Emp_Id = a.Emp_Id AND sc.Categoria_Id = a.Categoria_Id
             AND sc.SubCategoria_Id = a.SubCategoria_Id
        LEFT JOIN Unidad u WITH (NOLOCK)
          ON u.Emp_Id = a.Emp_Id AND u.Unidad_Id = a.Unidad_Id
        LEFT JOIN Sucursal suc WITH (NOLOCK)
          ON suc.Emp_Id = a.Emp_Id AND suc.Suc_Codigo_Externo = '${SUC_WIX}'
        LEFT JOIN Articulo_x_Sucursal axs WITH (NOLOCK)
          ON axs.Emp_Id = a.Emp_Id AND axs.Articulo_Id = a.Articulo_Id
             AND axs.Suc_Id = suc.Suc_Id
      WHERE a.Emp_Id = ${EMP_ID}
        AND a.Articulo_Activo_Venta = 1
        AND a.Articulo_Nombre NOT LIKE '(INS)%'
    `);
  }

  private async fetchBarcodes(): Promise<MssqlBarcodeRow[]> {
    return mssqlQuery<MssqlBarcodeRow>(`
      SELECT Articulo_Id, Equivalente_Id, Equivalente_Principal
      FROM Articulo_Equivalente WITH (NOLOCK)
      WHERE Emp_Id = ${EMP_ID}
    `);
  }

  private async fetchStock(): Promise<MssqlStockRow[]> {
    // Stock from Articulo_x_Bodega (the REAL stock table).
    // Sums available bodegas (Bodega_Existencia_Disponible = 1) per branch.
    // Returns Suc_Codigo_Externo (3‑digit code: 101, 102, …, 403).
    //
    // OPTIMIZATION: Removed INNER JOIN to Articulo (Articulo_Activo_Venta filter).
    //   Articulo_Activo_Venta has NO index → joining 1.96M stock rows to 40K articles
    //   caused a full scan. Without the join: 461ms vs 1587ms (3.4× faster).
    //   The ~370 extra rows for inactive articles are filtered in the app layer
    //   via articleMap (which already contains only active SKUs).
    //   Joins to Sucursal (PK Emp_Id,Suc_Id) and Bodega (PK Emp_Id,Suc_Id,Bodega_Id)
    //   remain — they're tiny tables (17 and 59 rows) with perfect PK matches.
    return mssqlQuery<MssqlStockRow>(`
      SELECT
        axb.Suc_Id,
        s.Suc_Codigo_Externo,
        s.Suc_Nombre,
        axb.Articulo_Id,
        SUM(axb.AxB_Existencia) AS total_existencia
      FROM Articulo_x_Bodega axb WITH (NOLOCK)
        INNER JOIN Sucursal s WITH (NOLOCK)
          ON s.Emp_Id = axb.Emp_Id AND s.Suc_Id = axb.Suc_Id AND s.Suc_Activo = 1
        INNER JOIN Bodega b WITH (NOLOCK)
          ON b.Emp_Id = axb.Emp_Id AND b.Suc_Id = axb.Suc_Id
             AND b.Bodega_Id = axb.Bodega_Id AND b.Bodega_Existencia_Disponible = 1
      WHERE axb.Emp_Id = ${EMP_ID}
      GROUP BY axb.Suc_Id, s.Suc_Codigo_Externo, s.Suc_Nombre, axb.Articulo_Id
      HAVING SUM(axb.AxB_Existencia) != 0
    `);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // NORMALIZE & INDEX
  // ═══════════════════════════════════════════════════════════════════════════

  private normalizeData(
    articles: MssqlArticleRow[],
    stockRows: MssqlStockRow[],
    barcodeRows: MssqlBarcodeRow[],
  ): {
    articleMap: Map<string, NormalizedArticle>;
    categoryTree: Map<string, CategoryNode>;
    branchCodes: Map<string, string>; // Suc_Codigo_Externo → Suc_Nombre
  } {
    const categoryTree = new Map<string, CategoryNode>();
    const articleMap = new Map<string, NormalizedArticle>();
    const branchCodes = new Map<string, string>();

    // Index stock by Articulo_Id; collect branch codes
    const stockIndex = new Map<string, MssqlStockRow[]>();
    for (const row of stockRows) {
      const key = row.Articulo_Id.trim();
      if (!key) continue;
      if (!stockIndex.has(key)) stockIndex.set(key, []);
      stockIndex.get(key)!.push(row);
      const extCode = (row.Suc_Codigo_Externo || '').trim();
      if (extCode) branchCodes.set(extCode, (row.Suc_Nombre || '').trim());
    }

    // Index barcodes by Articulo_Id
    const barcodeMap = new Map<string, Set<string>>();
    for (const b of barcodeRows) {
      const key = b.Articulo_Id.trim();
      if (!key) continue;
      if (!barcodeMap.has(key)) barcodeMap.set(key, new Set());
      const code = (b.Equivalente_Id || '').trim();
      if (code) barcodeMap.get(key)!.add(code);
    }

    for (const a of articles) {
      const sku = a.Articulo_Id.trim();
      if (!sku) continue;

      // Category keys — hierarchy linked through Articulo, not through table FK
      const deptoKey = `D:${a.Depto_Id}`;
      const categoriaKey = `C:${a.Depto_Id}:${a.Categoria_Id}`;
      const subCategoriaKey = `S:${a.Depto_Id}:${a.Categoria_Id}:${a.SubCategoria_Id}`;

      const deptoName = (a.Depto_Nombre || 'Sin Definir').trim();
      const catName = (a.Categoria_Nombre || 'Sin Definir').trim();
      const subCatName = (a.SubCategoria_Nombre || 'Sin Definir').trim();

      if (!categoryTree.has(deptoKey)) {
        categoryTree.set(deptoKey, { mssqlKey: deptoKey, name: deptoName, parentKey: null });
      }
      if (!categoryTree.has(categoriaKey)) {
        categoryTree.set(categoriaKey, { mssqlKey: categoriaKey, name: catName, parentKey: deptoKey });
      }
      if (!categoryTree.has(subCategoriaKey)) {
        categoryTree.set(subCategoriaKey, { mssqlKey: subCategoriaKey, name: subCatName, parentKey: categoriaKey });
      }

      // Barcode logic:
      // 1. If Articulo_Codigo_Interno exists and != SKU, it's a candidate for main barcode.
      // 2. Also check Articulo_Equivalente for this SKU.
      // 3. We want ONE main barcode for product.barcode, and the rest in product.packaging (extraBarcodes).
      
      const codigoInterno = (a.Articulo_Codigo_Interno || '').trim();
      let mainBarcode = (codigoInterno && codigoInterno !== sku) ? codigoInterno : '';
      
      // If we don't have a specific internal code, check if there's a "Principal" equivalent?
      // The current barcodeRows doesn't easily map "Principal" unless we indexed it that way.
      // But user said "array of values... add several values".
      // Let's gather ALL barcodes for this product.
      const allBarcodes = new Set<string>();
      if (mainBarcode) allBarcodes.add(mainBarcode);
      // Add SKU as barcode? Usually bad practice if SKU is short/internal ID, but if it's EAN-like... 
      // The old logic was: const barcode = (codigoInterno && codigoInterno !== sku) ? codigoInterno : sku;
      // Meaning if no special code, use SKU as barcode.
      // But if we have Equivalentes, we should use those.
      
      if (barcodeMap.has(sku)) {
        for (const b of barcodeMap.get(sku)!) {
          allBarcodes.add(b);
        }
      }

      // If we still have no barcodes, and the old logic used SKU, we keep using SKU as main barcode?
      // "Barcodes ... normally is an array of values ... add several values"
      // If `allBarcodes` is empty, mainBarcode = sku.
      // If `allBarcodes` has items, pick one as main (prefer Articulo_Codigo_Interno or SKU if in set?), others as extras.
      
      if (allBarcodes.size === 0) {
        mainBarcode = sku; 
      } else {
        // If mainBarcode was set from Codigo_Interno, use it. 
        // If not, pick the first one?
        // Let's respect Codigo_Interno as primary if present.
        if (!mainBarcode) {
            // Pick first one from equivalents
            mainBarcode = allBarcodes.values().next().value || '';
        }
      }
      
      // Remove mainBarcode from extras
      const extraBarcodes = Array.from(allBarcodes).filter(b => b !== mainBarcode && b !== sku);
      // Note: We exclude SKU from extra barcodes to avoid redundancy if SKU is used as ID.
      // But if SKU is a valid EAN, we might want it in extras if not main? 
      // Odoo allows searching by default_code (SKU) automatically. So we don't need SKU in barcode fields usually.

      // Stock per branch keyed by 3‑digit Suc_Codigo_Externo
      const stockByBranch = new Map<string, { qty: number; branchName: string }>();
      for (const sr of (stockIndex.get(sku) || [])) {
        const extCode = (sr.Suc_Codigo_Externo || '').trim();
        if (extCode) {
          stockByBranch.set(extCode, {
            qty: sr.total_existencia,
            branchName: (sr.Suc_Nombre || '').trim(),
          });
        }
      }

      const name = (a.Articulo_Nombre || '').trim();
      const listPrice = a.Articulo_Precio1 || 0;
      const standardPrice = a.AxS_Costo_Actual || 0;
      const active = !!a.Articulo_Activo;

      articleMap.set(sku, {
        sku,
        barcode: mainBarcode,
        extraBarcodes,
        name,
        deptoKey,
        categoriaKey,
        subCategoriaKey,
        uomName: (a.Unidad_Nombre || 'PIEZA').trim(),
        listPrice,
        standardPrice,
        updatedAt: a.Articulo_Fec_Actualizacion,
        stockByBranch,
        active,
        hash: computeHash({ name, listPrice, standardPrice, subCategoriaKey, barcode: mainBarcode }),
      });
    }

    return { articleMap, categoryTree, branchCodes };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SYNC CATEGORIES (Odoo product.category)
  // ═══════════════════════════════════════════════════════════════════════════

  private async syncCategories(
    categoryTree: Map<string, CategoryNode>,
  ): Promise<Map<string, number>> {
    const existing = await searchReadAll('product.category', [], ['id', 'name', 'parent_id']);
    const keyToOdooId = new Map<string, number>();

    // Ensure root "Proconsa" category exists
    let rootId = existing.find(c => c.name === 'Proconsa')?.id;
    if (!rootId) {
      rootId = await executeKw<number>('product.category', 'create', [{ name: 'Proconsa', parent_id: false }]);
      logger.info(CTX, `  Created root category "Proconsa" id=${rootId}`);
    }
    keyToOdooId.set('ROOT', rootId);

    // "parentOdooId|name" → odooId
    const existingLookup = new Map<string, number>();
    for (const cat of existing) {
      const pid = cat.parent_id ? (cat.parent_id as [number, string])[0] : 0;
      existingLookup.set(`${pid}|${cat.name}`, cat.id);
    }

    // Process by depth: 0=Depto, 1=Categoria, 2=SubCategoria
    const byDepth: CategoryNode[][] = [[], [], []];
    for (const node of categoryTree.values()) {
      if (node.parentKey === null) byDepth[0].push(node);
      else if (node.mssqlKey.startsWith('C:')) byDepth[1].push(node);
      else byDepth[2].push(node);
    }

    for (let depth = 0; depth < byDepth.length; depth++) {
      const toCreate: { node: CategoryNode; parentOdooId: number }[] = [];

      for (const node of byDepth[depth]) {
        const parentOdooId = node.parentKey
          ? (keyToOdooId.get(node.parentKey) ?? rootId)
          : rootId;

        const lookupKey = `${parentOdooId}|${node.name}`;
        const existingId = existingLookup.get(lookupKey);
        if (existingId) {
          keyToOdooId.set(node.mssqlKey, existingId);
        } else {
          toCreate.push({ node, parentOdooId });
        }
      }

      if (toCreate.length > 0) {
        const vals = toCreate.map(tc => ({ name: tc.node.name, parent_id: tc.parentOdooId }));
        const newIds = await executeKw<number | number[]>('product.category', 'create', [vals]);
        const idArr = Array.isArray(newIds) ? newIds : [newIds];

        for (let i = 0; i < toCreate.length; i++) {
          keyToOdooId.set(toCreate[i].node.mssqlKey, idArr[i]);
          existingLookup.set(`${toCreate[i].parentOdooId}|${toCreate[i].node.name}`, idArr[i]);
        }
        logger.info(CTX, `  Created ${idArr.length} categories at depth ${depth}`);
      }
    }

    return keyToOdooId;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SYNC POS CATEGORIES (Odoo pos.category)
  // ═══════════════════════════════════════════════════════════════════════════

  private async syncPosCategories(
    categoryTree: Map<string, CategoryNode>,
  ): Promise<Map<string, number>> {
    const existing = await searchReadAll('pos.category', [], ['id', 'name', 'parent_id']);
    const keyToOdooId = new Map<string, number>();

    // Ensure root "Proconsa" category exists
    let rootId = existing.find(c => c.name === 'Proconsa')?.id;
    if (!rootId) {
      rootId = await executeKw<number>('pos.category', 'create', [{ name: 'Proconsa', parent_id: false }]);
      logger.info(CTX, `  Created root POS category "Proconsa" id=${rootId}`);
    }
    keyToOdooId.set('ROOT', rootId);

    // "parentOdooId|name" → odooId
    const existingLookup = new Map<string, number>();
    for (const cat of existing) {
      const pid = cat.parent_id ? (cat.parent_id as [number, string])[0] : 0;
      existingLookup.set(`${pid}|${cat.name}`, cat.id);
    }

    // Process by depth: 0=Depto, 1=Categoria, 2=SubCategoria
    const byDepth: CategoryNode[][] = [[], [], []];
    for (const node of categoryTree.values()) {
      if (node.parentKey === null) byDepth[0].push(node);
      else if (node.mssqlKey.startsWith('C:')) byDepth[1].push(node);
      else byDepth[2].push(node);
    }

    for (let depth = 0; depth < byDepth.length; depth++) {
      const toCreate: { node: CategoryNode; parentOdooId: number }[] = [];

      for (const node of byDepth[depth]) {
        const parentOdooId = node.parentKey
          ? (keyToOdooId.get(node.parentKey) ?? rootId)
          : rootId;

        const lookupKey = `${parentOdooId}|${node.name}`;
        const existingId = existingLookup.get(lookupKey);
        if (existingId) {
          keyToOdooId.set(node.mssqlKey, existingId);
        } else {
          toCreate.push({ node, parentOdooId });
        }
      }

      if (toCreate.length > 0) {
        const vals = toCreate.map(tc => ({ name: tc.node.name, parent_id: tc.parentOdooId }));
        const newIds = await executeKw<number | number[]>('pos.category', 'create', [vals]);
        const idArr = Array.isArray(newIds) ? newIds : [newIds];

        for (let i = 0; i < toCreate.length; i++) {
          keyToOdooId.set(toCreate[i].node.mssqlKey, idArr[i]);
          existingLookup.set(`${toCreate[i].parentOdooId}|${toCreate[i].node.name}`, idArr[i]);
        }
        logger.info(CTX, `  Created ${idArr.length} POS categories at depth ${depth}`);
      }
    }

    return keyToOdooId;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SYNC WAREHOUSES — one Odoo warehouse per branch, code = Suc_Codigo_Externo
  // ═══════════════════════════════════════════════════════════════════════════

  private async syncWarehouses(
    branchCodes: Map<string, string>,
  ): Promise<Map<string, number>> {
    // Returns: Suc_Codigo_Externo → lot_stock_id (location ID for stock.quant)
    const existing = await searchReadAll('stock.warehouse', [], ['id', 'name', 'code', 'lot_stock_id']);

    const codeToLocId = new Map<string, number>();
    const existingByCode = new Map<string, OdooRecord>();
    for (const wh of existing) {
      const code = (wh.code as string || '').trim();
      const locId = (wh.lot_stock_id as [number, string])?.[0];
      if (code && locId) {
        codeToLocId.set(code, locId);
        existingByCode.set(code, wh);
      }
    }

    // Create missing warehouses
    for (const [extCode, branchName] of branchCodes) {
      if (codeToLocId.has(extCode)) continue;
      try {
        const whId = await executeKw<number>('stock.warehouse', 'create', [{
          name: branchName || `Sucursal ${extCode}`,
          code: extCode,
        }]);
        // Read back the lot_stock_id
        const [wh] = await executeKw<OdooRecord[]>('stock.warehouse', 'read', [[whId], ['lot_stock_id']]);
        const locId = (wh?.lot_stock_id as [number, string])?.[0];
        if (locId) codeToLocId.set(extCode, locId);
        logger.info(CTX, `  Created warehouse "${branchName}" code=${extCode} id=${whId} loc=${locId}`);
      } catch (err) {
        logger.error(CTX, `  Failed to create warehouse ${extCode}: ${(err as Error).message}`);
      }
    }

    return codeToLocId;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FETCH ODOO PRODUCTS (incl. archived, for reactivation detection)
  // ═══════════════════════════════════════════════════════════════════════════

  private async fetchOdooProducts(): Promise<OdooRecord[]> {
    const fields = ['id', 'name', 'default_code', 'list_price', 'standard_price', 'categ_id', 'uom_id', 'barcode', 'active', 'pos_categ_ids', 'available_in_pos'];
    const domain = [['default_code', '!=', false]];
    const batchSize = 500;

    const all: OdooRecord[] = [];
    let offset = 0;

    while (true) {
      const batch = await executeKw<OdooRecord[]>(
        'product.template', 'search_read', [domain],
        { fields, limit: batchSize, offset, context: { active_test: false } },
      );
      if (!batch || batch.length === 0) break;
      all.push(...batch);
      offset += batchSize;
    }
    return all;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // COMPUTE DIFF
  // ═══════════════════════════════════════════════════════════════════════════

  private computeDiff(
    articleMap: Map<string, NormalizedArticle>,
    odooProducts: OdooRecord[],
    categoryIdMap: Map<string, number>,
    posCategoryIdMap: Map<string, number>,
  ): {
    toCreate: NormalizedArticle[];
    toUpdate: { odooId: number; article: NormalizedArticle; changes: Record<string, unknown> }[];
    toArchive: number[];
  } {
    const odooByCode = new Map<string, OdooRecord>();
    const odooByBarcode = new Map<string, OdooRecord>();

    for (const p of odooProducts) {
      const code = (p.default_code as string || '').trim();
      if (code) odooByCode.set(code, p);
      
      const bc = (p.barcode as string || '').trim();
      if (bc) odooByBarcode.set(bc, p);
    }

    const toCreate: NormalizedArticle[] = [];
    const toUpdate: { odooId: number; article: NormalizedArticle; changes: Record<string, unknown> }[] = [];
    const toArchive: number[] = [];

    for (const [sku, article] of articleMap) {
      // Check barcode conflict:
      // If article has a barcode, but that barcode is already used by ANOTHER SKU in Odoo,
      // we must NOT sync it, otherwise Odoo will throw "Barcode already assigned".
      if (article.barcode) {
        const owner = odooByBarcode.get(article.barcode);
        if (owner) {
            const ownerSku = (owner.default_code as string || '').trim();
            if (ownerSku !== sku) {
                logger.warn(CTX, `  ⚠ Barcode conflict: SKU ${sku} wants "${article.barcode}", but it is used by ${ownerSku} (ID ${owner.id}). Skipping barcode for ${sku}.`);
                article.barcode = ''; // Strip it to prevent error
            }
        }
      }

      const odoo = odooByCode.get(sku);
      if (!odoo) {
        toCreate.push(article);
        continue;
      }

      const changes: Record<string, unknown> = {};

      if ((odoo.name as string || '').trim() !== article.name) {
        changes.name = article.name;
      }
      if (Math.abs((odoo.list_price as number || 0) - article.listPrice) > 0.01) {
        changes.list_price = article.listPrice;
      }
      if (Math.abs((odoo.standard_price as number || 0) - article.standardPrice) > 0.01) {
        changes.standard_price = article.standardPrice;
      }

      const targetCategId = categoryIdMap.get(article.subCategoriaKey);
      const odooCategId = odoo.categ_id ? (odoo.categ_id as [number, string])[0] : 0;
      if (targetCategId && odooCategId !== targetCategId) {
        changes.categ_id = targetCategId;
      }

      const odooBarcode = (odoo.barcode as string || '').trim();
      if (article.barcode !== odooBarcode) {
        // Only update if different. Note: article.barcode might be '' if we stripped it above.
        changes.barcode = article.barcode || false; 
      }

      // Reactivate archived products that are active in MSSQL
      // Or deactivate if not active in MSSQL
      // Note: "Active" in Odoo is what controls visibility
      if ((odoo.active as boolean) !== article.active) {
        changes.active = article.active;
      }

      // POS Category
      const targetPosCategId = posCategoryIdMap.get(article.subCategoriaKey)
        ?? posCategoryIdMap.get(article.categoriaKey)
        ?? posCategoryIdMap.get(article.deptoKey)
        ?? posCategoryIdMap.get('ROOT');
        
      const odooPosCategIds = odoo.pos_categ_ids as number[] || [];
      // We enforce single POS category for simplicity and sync. Odoo returns array of IDs.
      const currentPosCategId = odooPosCategIds.length > 0 ? odooPosCategIds[0] : 0;
      
      if (targetPosCategId && currentPosCategId !== targetPosCategId) {
        changes.pos_categ_ids = [[6, 0, [targetPosCategId]]];
      } else if (!targetPosCategId && odooPosCategIds.length > 0) {
         changes.pos_categ_ids = [[6, 0, []]];
      }

      // Available in POS: always true for active items? User said: "todos los articulos activos deben de aparecer en el punto de venta"
      if (article.active && !odoo.available_in_pos) {
        changes.available_in_pos = true;
      }
      if (!article.active && odoo.available_in_pos) {
        changes.available_in_pos = false;
      }

      if (Object.keys(changes).length > 0) {
        toUpdate.push({ odooId: odoo.id, article, changes });
      }
    }

    // Archive products no longer present in MSSQL map
    // User Requirement: "no debemos eliminarlos sino cambiar de estado a desactivado"
    // Since articleMap only contains ACTIVE products (filtered in SQL), 
    // any product present in Odoo but missing from articleMap is considered inactive/removed from source.
    // We strictly ARCHIVE (active=false) them, we do NOT unlink.
    for (const [code, odoo] of odooByCode) {
      if (!articleMap.has(code) && (odoo.active as boolean)) {
        toArchive.push(odoo.id);
      }
    }

    return { toCreate, toUpdate, toArchive };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // APPLY CREATES (batch)
  // ═══════════════════════════════════════════════════════════════════════════

  private async applyCreates(
    toCreate: NormalizedArticle[],
    categoryIdMap: Map<string, number>,
    posCategoryIdMap: Map<string, number>,
  ): Promise<void> {
    if (toCreate.length === 0) {
      logger.info(CTX, '  No new products to create');
      return;
    }
    if (this.dryRun) {
      logger.info(CTX, `  [DRY RUN] Would create ${toCreate.length} products`);
      return;
    }

    const BATCH = 50;
    const batches = chunk(toCreate, BATCH);
    let created = 0;

    for (const batch of batches) {
      const vals = batch.map(a => {
        const categId = categoryIdMap.get(a.subCategoriaKey)
          ?? categoryIdMap.get(a.categoriaKey)
          ?? categoryIdMap.get(a.deptoKey)
          ?? categoryIdMap.get('ROOT')
          ?? 1;
        const uomId = resolveUomId(a.uomName);

        const posCategId = posCategoryIdMap.get(a.subCategoriaKey)
          ?? posCategoryIdMap.get(a.categoriaKey)
          ?? posCategoryIdMap.get(a.deptoKey)
          ?? posCategoryIdMap.get('ROOT');

        return {
          name: a.name,
          default_code: a.sku,
          barcode: a.barcode || false,
          list_price: a.listPrice,
          standard_price: a.standardPrice,
          categ_id: categId,
          uom_id: uomId,
          uom_po_id: uomId,
          type: 'consu',
          is_storable: true,
          sale_ok: true,
          purchase_ok: true,
          active: a.active,
          available_in_pos: a.active,
          pos_categ_ids: posCategId ? [[6, 0, [posCategId]]] : [],
        };
      });

      try {
        const ids = await executeKw<number | number[]>('product.template', 'create', [vals]);
        const idArr = Array.isArray(ids) ? ids : [ids];
        created += idArr.length;
        logger.info(CTX, `  Created batch: ${idArr.length} products (total: ${created}/${toCreate.length})`);
      } catch (err) {
        logger.warn(CTX, `  Batch create failed, falling back to individual: ${(err as Error).message}`);
        for (const v of vals) {
          try {
            await executeKw<number>('product.template', 'create', [v]);
            created++;
          } catch (innerErr) {
            logger.error(CTX, `  Failed to create ${v.default_code}: ${(innerErr as Error).message}`);
          }
        }
      }
    }

    logger.info(CTX, `  Total created: ${created}`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // APPLY UPDATES — two‑phase strategy:
  //   Phase A: "categorical" fields (name, categ_id, barcode, active, pos_categ_ids,
  //            available_in_pos) — these are often shared across products, so we
  //            group by identical change‑set and batch‑write (many IDs, one write).
  //   Phase B: "numeric" fields (list_price, standard_price) — unique per product,
  //            so grouping yields ~1 group per product.  We use concurrent workers
  //            with retry (same pattern as stock.quant writes) for throughput.
  // ═══════════════════════════════════════════════════════════════════════════

  private static readonly NUMERIC_FIELDS = new Set(['list_price', 'standard_price']);
  private get WRITE_CONCURRENCY() { return Math.max(1, config.odoo.productWriteConcurrency); }
  private get WRITE_MAX_RETRIES() { return Math.max(1, config.odoo.productWriteRetries); }

  private async applyUpdates(
    toUpdate: { odooId: number; article: NormalizedArticle; changes: Record<string, unknown> }[],
  ): Promise<void> {
    if (toUpdate.length === 0) {
      logger.info(CTX, '  No products to update');
      return;
    }
    if (this.dryRun) {
      logger.info(CTX, `  [DRY RUN] Would update ${toUpdate.length} products`);
      return;
    }

    // ── Split changes into categorical (batcheable) and numeric (per‑product) ──
    const categoricalUpdates: { odooId: number; changes: Record<string, unknown> }[] = [];
    const numericUpdates: { odooId: number; changes: Record<string, unknown> }[] = [];

    for (const item of toUpdate) {
      const catChanges: Record<string, unknown> = {};
      const numChanges: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(item.changes)) {
        if (OdooInventorySyncTask.NUMERIC_FIELDS.has(k)) {
          numChanges[k] = v;
        } else {
          catChanges[k] = v;
        }
      }
      if (Object.keys(catChanges).length > 0) {
        categoricalUpdates.push({ odooId: item.odooId, changes: catChanges });
      }
      if (Object.keys(numChanges).length > 0) {
        numericUpdates.push({ odooId: item.odooId, changes: numChanges });
      }
    }

    let totalUpdated = 0;

    // ── Phase A: Categorical — group by identical change‑set, batch write ──
    if (categoricalUpdates.length > 0) {
      const groups = new Map<string, { ids: number[]; changes: Record<string, unknown> }>();
      for (const item of categoricalUpdates) {
        const key = JSON.stringify(item.changes);
        if (!groups.has(key)) groups.set(key, { ids: [], changes: item.changes });
        groups.get(key)!.ids.push(item.odooId);
      }

      logger.info(CTX, `  Phase A: ${categoricalUpdates.length} categorical updates in ${groups.size} groups`);
      let catUpdated = 0;
      for (const [, group] of groups) {
        for (const idBatch of chunk(group.ids, 200)) {
          try {
            await executeKw<boolean>('product.template', 'write', [idBatch, group.changes]);
            catUpdated += idBatch.length;
          } catch (err) {
            logger.error(CTX, `  Categorical batch failed (${idBatch.length} products): ${(err as Error).message}`);
          }
        }
      }
      logger.info(CTX, `  Phase A done: ${catUpdated} products updated`);
      totalUpdated += catUpdated;
    }

    // ── Phase B: Numeric (price/cost) — concurrent workers with retry ──────
    if (numericUpdates.length > 0) {
      logger.info(CTX, `  Phase B: ${numericUpdates.length} price/cost updates (concurrency=${this.WRITE_CONCURRENCY})`);
      let numUpdated = 0;
      let numErrors = 0;

      const queue = [...numericUpdates];
      const concurrency = this.WRITE_CONCURRENCY;
      const maxRetries = this.WRITE_MAX_RETRIES;

      const workers = Array.from({ length: concurrency }, () => (async () => {
        while (queue.length > 0) {
          const item = queue.shift();
          if (!item) break;

          let ok = false;
          for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
              await executeKw<boolean>('product.template', 'write', [[item.odooId], item.changes]);
              numUpdated++;
              ok = true;
              break;
            } catch (err) {
              if (attempt < maxRetries && isTransientXmlRpcError(err)) {
                await sleep(250 * attempt);
                continue;
              }
              logger.error(CTX, `  Price update failed id=${item.odooId}: ${(err as Error).message}`);
              numErrors++;
              break;
            }
          }
        }
      })());

      // Progress reporter (runs alongside workers)
      const progressInterval = setInterval(() => {
        const done = numUpdated + numErrors;
        if (done > 0) {
          logger.info(CTX, `  Phase B progress: ${done}/${numericUpdates.length} (${numErrors} errors)`);
        }
      }, 5000);

      await Promise.all(workers);
      clearInterval(progressInterval);

      logger.info(CTX, `  Phase B done: ${numUpdated} updated, ${numErrors} errors`);
      totalUpdated += numUpdated;
    }

    logger.info(CTX, `  Total updated: ${totalUpdated}/${toUpdate.length}`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // APPLY ARCHIVES
  // ═══════════════════════════════════════════════════════════════════════════

  private async applyArchives(toArchive: number[]): Promise<void> {
    if (toArchive.length === 0) {
      logger.info(CTX, '  No products to archive');
      return;
    }
    if (this.dryRun) {
      logger.info(CTX, `  [DRY RUN] Would archive ${toArchive.length} products`);
      return;
    }

    for (const batch of chunk(toArchive, 200)) {
      try {
        await executeKw<boolean>('product.template', 'write', [batch, { active: false }]);
      } catch (err) {
        logger.error(CTX, `  Failed to archive ${batch.length} products: ${(err as Error).message}`);
      }
    }
    logger.info(CTX, `  Total archived: ${toArchive.length}`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SYNC STOCK LEVELS — per warehouse (keyed by Suc_Codigo_Externo)
  // ═══════════════════════════════════════════════════════════════════════════

  private async syncStockLevels(
    articleMap: Map<string, NormalizedArticle>,
    warehouseLocMap: Map<string, number>,
  ): Promise<void> {
    if (warehouseLocMap.size === 0) {
      logger.warn(CTX, '  No warehouse locations mapped, skipping stock sync');
      return;
    }

    // Fetch product.product variants (to get id for stock.quant)
    const variantsStart = Date.now();
    logger.info(CTX, '  Fetching product variants (product.product)...');
    const variants = await searchReadAll(
      'product.product',
      [['default_code', '!=', false]],
      ['id', 'default_code'],
      { batchSize: 500 },
    );
    logger.info(CTX, `  Variants fetched: ${variants.length} in ${((Date.now() - variantsStart) / 1000).toFixed(1)}s`);
    const variantByCode = new Map<string, number>();
    for (const v of variants) {
      const code = (v.default_code as string || '').trim();
      if (code) variantByCode.set(code, v.id);
    }

    if (variantByCode.size === 0) {
      logger.warn(CTX, '  No product variants found, skipping stock sync');
      return;
    }

    // Fetch existing quants in warehouse locations
    const locationIds = Array.from(new Set(warehouseLocMap.values()));
    const quantsStart = Date.now();
    logger.info(CTX, `  Fetching existing quants (stock.quant) for ${locationIds.length} locations...`);

    // IMPORTANT: Using search + read (instead of search_read) for large datasets.
    // On some Odoo instances, stock.quant search_read can hang during query/serialization.
    const quantIds = await searchAllIds(
      'stock.quant',
      [['location_id', 'in', locationIds]],
      { batchSize: 2000 },
    );
    logger.info(CTX, `  Quant ids fetched: ${quantIds.length} in ${((Date.now() - quantsStart) / 1000).toFixed(1)}s`);

    const existingQuants: OdooRecord[] = [];
    const readStart = Date.now();
    const READ_BATCH = 500;
    for (let i = 0; i < quantIds.length; i += READ_BATCH) {
      const batchIds = quantIds.slice(i, i + READ_BATCH);
      const recs = await readRecords('stock.quant', batchIds, ['id', 'product_id', 'location_id', 'quantity'], READ_BATCH);
      existingQuants.push(...recs);
      if ((i + READ_BATCH) % 2000 === 0 || i + READ_BATCH >= quantIds.length) {
        logger.info(CTX, `  Quants read: ${existingQuants.length}/${quantIds.length} in ${((Date.now() - readStart) / 1000).toFixed(1)}s`);
      }
    }
    logger.info(CTX, `  Quants fetched(total): ${existingQuants.length} in ${((Date.now() - quantsStart) / 1000).toFixed(1)}s`);

    const quantIndex = new Map<string, OdooRecord>();
    for (const q of existingQuants) {
      const prodId = (q.product_id as [number, string])?.[0];
      const locId = (q.location_id as [number, string])?.[0];
      if (prodId && locId) quantIndex.set(`${prodId}|${locId}`, q);
    }

    // Build list of stock changes
    const updates: { productId: number; locationId: number; qty: number; existingQuantId?: number }[] = [];

    const computeStart = Date.now();
    let processedArticles = 0;

    for (const [sku, article] of articleMap) {
      processedArticles++;
      const productId = variantByCode.get(sku);
      if (!productId) continue;

      for (const [branchCode, stock] of article.stockByBranch) {
        const locId = warehouseLocMap.get(branchCode);
        if (!locId) continue;

        const qKey = `${productId}|${locId}`;
        const existing = quantIndex.get(qKey);

        if (existing) {
          const currentQty = existing.quantity as number || 0;
          if (Math.abs(currentQty - stock.qty) > 0.001) {
            updates.push({ productId, locationId: locId, qty: stock.qty, existingQuantId: existing.id });
          }
          // Remove from index so we can detect zeroed‑out quants
          quantIndex.delete(qKey);
        } else if (stock.qty > 0) {
          updates.push({ productId, locationId: locId, qty: stock.qty });
        }
      }

      if (processedArticles % 1000 === 0) {
        logger.info(CTX, `  Computing stock changes: ${processedArticles}/${articleMap.size} articles processed (updates so far: ${updates.length})`);
      }
    }

    logger.info(CTX, `  Stock change computation done: ${updates.length} changes in ${((Date.now() - computeStart) / 1000).toFixed(1)}s`);

    if (this.dryRun) {
      const toCreate = updates.filter(u => !u.existingQuantId);
      const toUpdate = updates.filter(u => u.existingQuantId);
      logger.info(CTX, `  [DRY RUN] Would apply ${updates.length} stock changes (${toCreate.length} create, ${toUpdate.length} update) across ${warehouseLocMap.size} warehouses`);
      return;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // BATCH STOCK SYNC — dramatically faster than one-by-one
    //
    // Strategy:
    //   1. Batch CREATE new quants (Odoo create accepts array of dicts)
    //   2. Batch WRITE existing quants (one write per quant, but we can parallelize)
    //   3. Batch ACTION_APPLY_INVENTORY (accepts array of ids)
    //
    // This reduces ~134,000 RPC calls to ~1,400 calls (200 items per batch).
    // ═══════════════════════════════════════════════════════════════════════════

    const toCreate = updates.filter(u => !u.existingQuantId);
    const toUpdate = updates.filter(u => u.existingQuantId);

    logger.info(CTX, `  Stock changes: ${toCreate.length} to create, ${toUpdate.length} to update`);

    const stockStart = Date.now();
    let stockCreated = 0;
    let stockUpdated = 0;
    let stockErrors = 0;

    // ── BATCH CREATE ──────────────────────────────────────────────────────────
    const CREATE_BATCH = 200;
    const createBatches = chunk(toCreate, CREATE_BATCH);
    logger.info(CTX, `  Creating ${toCreate.length} quants in ${createBatches.length} batches...`);

    for (let bi = 0; bi < createBatches.length; bi++) {
      const batch = createBatches[bi];
      const vals = batch.map(u => ({
        product_id: u.productId,
        location_id: u.locationId,
        inventory_quantity: u.qty,
      }));

      try {
        // Batch create returns array of new ids
        const newIds = await executeKw<number | number[]>('stock.quant', 'create', [vals]);
        const idArr = Array.isArray(newIds) ? newIds : [newIds];

        // Batch apply inventory
        try {
          await executeKw<boolean>('stock.quant', 'action_apply_inventory', [idArr]);
        } catch (applyErr) {
          // Odoo 18: action_apply_inventory returns None which XML-RPC can't marshal
          if (!(applyErr as Error).message?.includes('cannot marshal None')) throw applyErr;
        }

        stockCreated += idArr.length;
      } catch (err) {
        logger.error(CTX, `  Batch create error (batch ${bi + 1}): ${(err as Error).message}`);
        stockErrors += batch.length;
      }

      if ((bi + 1) % 10 === 0 || bi === createBatches.length - 1) {
        const elapsed = ((Date.now() - stockStart) / 1000).toFixed(1);
        logger.info(CTX, `  Create progress: ${Math.min((bi + 1) * CREATE_BATCH, toCreate.length)}/${toCreate.length} in ${elapsed}s`);
      }
    }

    // ── BATCH UPDATE ──────────────────────────────────────────────────────────
    // Each quant has a different qty, so we can't use a single write call.
    // But we CAN batch the action_apply_inventory call after individual writes.
    // Strategy: write in parallel batches, then apply inventory in batch.
    const UPDATE_BATCH = 100;
    const WRITE_CONCURRENCY = Math.max(1, config.odoo.stockWriteConcurrency);
    const WRITE_MAX_RETRIES = Math.max(1, config.odoo.stockWriteRetries);
    const updateBatches = chunk(toUpdate, UPDATE_BATCH);
    logger.info(CTX, `  Updating ${toUpdate.length} quants in ${updateBatches.length} batches...`);

    for (let bi = 0; bi < updateBatches.length; bi++) {
      const batch = updateBatches[bi];
      const successIds: number[] = [];

      // Write each quant's inventory_quantity with controlled concurrency + retry.
      // This avoids overloading Odoo/proxy and mitigates transient HTML responses
      // (e.g. "Unknown XML-RPC tag 'TITLE'").
      const queue = [...batch];
      const workers = Array.from({ length: WRITE_CONCURRENCY }, () => (async () => {
        while (queue.length > 0) {
          const u = queue.shift();
          if (!u) break;

          let ok = false;
          for (let attempt = 1; attempt <= WRITE_MAX_RETRIES; attempt++) {
            try {
              await executeKw<boolean>('stock.quant', 'write', [
                [u.existingQuantId!],
                { inventory_quantity: u.qty },
              ]);
              successIds.push(u.existingQuantId!);
              ok = true;
              break;
            } catch (err) {
              if (attempt < WRITE_MAX_RETRIES && isTransientXmlRpcError(err)) {
                const waitMs = 250 * attempt;
                logger.warn(
                  CTX,
                  `  Transient write error quant=${u.existingQuantId} (attempt ${attempt}/${WRITE_MAX_RETRIES}): ${(err as Error).message} — retrying in ${waitMs}ms`,
                );
                await sleep(waitMs);
                continue;
              }
              logger.error(CTX, `  Write error quant=${u.existingQuantId}: ${(err as Error).message}`);
              stockErrors++;
              break;
            }
          }

          if (!ok) {
            // already counted in stockErrors
          }
        }
      })());

      await Promise.all(workers);

      // Batch apply inventory for all successful writes
      if (successIds.length > 0) {
        let applyOk = false;
        for (let attempt = 1; attempt <= WRITE_MAX_RETRIES; attempt++) {
          try {
            await executeKw<boolean>('stock.quant', 'action_apply_inventory', [successIds]);
            applyOk = true;
            break;
          } catch (applyErr) {
            // Odoo 18 may return None here; treat that as success.
            if ((applyErr as Error).message?.includes('cannot marshal None')) {
              applyOk = true;
              break;
            }

            if (attempt < WRITE_MAX_RETRIES && isTransientXmlRpcError(applyErr)) {
              const waitMs = 250 * attempt;
              logger.warn(
                CTX,
                `  Transient apply error (attempt ${attempt}/${WRITE_MAX_RETRIES}): ${(applyErr as Error).message} — retrying in ${waitMs}ms`,
              );
              await sleep(waitMs);
              continue;
            }

            logger.error(CTX, `  Batch apply error: ${(applyErr as Error).message}`);
            break;
          }
        }
        if (applyOk) {
          stockUpdated += successIds.length;
        } else {
          stockErrors += successIds.length;
        }
      }

      if ((bi + 1) % 10 === 0 || bi === updateBatches.length - 1) {
        const elapsed = ((Date.now() - stockStart) / 1000).toFixed(1);
        logger.info(CTX, `  Update progress: ${Math.min((bi + 1) * UPDATE_BATCH, toUpdate.length)}/${toUpdate.length} in ${elapsed}s`);
      }
    }

    const totalElapsed = ((Date.now() - stockStart) / 1000).toFixed(1);
    logger.info(CTX, `  Stock sync complete: ${stockCreated} created, ${stockUpdated} updated, ${stockErrors} errors in ${totalElapsed}s`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // IMAGE SYNC — Articulo_Imagen_FS (GRA principal, latest per article)
  // ═══════════════════════════════════════════════════════════════════════════
  //
  // Strategy (avoids downloading all blobs every hour):
  //   1. Lightweight metadata query: DATALENGTH + Fecha per article (~623ms)
  //   2. Fingerprint = "size|fecha" stored in Odoo x_image_fp custom field
  //   3. Compare fingerprints → only fetch blobs for changed/missing images
  //   4. Upload base64 to Odoo image_1920 + update fingerprint
  //
  // NOTE: CHECKSUM(Imagen) was tested but takes ~12s for 3 rows (reads full
  //       blob); DATALENGTH is metadata‑only and runs in <1s for all 6 616 rows.
  //       Combined with Fecha it's a reliable change‑detection fingerprint.
  // ═══════════════════════════════════════════════════════════════════════════

  private async fetchImageMetadata(): Promise<MssqlImageMetaRow[]> {
    // One row per article: latest GRA principal image (deduped via ROW_NUMBER)
    //
    // OPTIMIZATION: Removed INNER JOIN to Articulo (Articulo_Activo filter).
    //   Articulo_Imagen_FS has NO index on (Emp_Id, Articulo_Id, Tipo_Imagen) —
    //   only PK on Consecutivo (auto-increment). Joining to Articulo added a second
    //   full scan on 40K rows with unindexed Articulo_Activo filter.
    //   Without the join: 111ms vs 1764ms (16× faster).
    //   The ~681 extra rows for inactive articles are filtered in syncImages()
    //   via articleMap (which already contains only active SKUs).
    return mssqlQuery<MssqlImageMetaRow>(`
      SELECT Articulo_Id, img_size, Fecha
      FROM (
        SELECT
          i.Articulo_Id,
          DATALENGTH(i.Imagen) AS img_size,
          i.Fecha,
          ROW_NUMBER() OVER (PARTITION BY i.Articulo_Id ORDER BY i.Fecha DESC) AS rn
        FROM Articulo_Imagen_FS i WITH (NOLOCK)
        WHERE i.Emp_Id = ${EMP_ID}
          AND i.Tipo_Imagen = 'GRA'
          AND i.Imagen_Principal = 1
      ) t WHERE rn = 1
    `);
  }

  private async fetchImageBlobs(articleIds: string[]): Promise<MssqlImageBlobRow[]> {
    if (articleIds.length === 0) return [];
    // Build IN‑list; sanitise by stripping non‑alphanumeric (ids are varchar)
    const inList = articleIds.map(id => `'${id.replace(/[^a-zA-Z0-9]/g, '')}'`).join(',');
    return mssqlQuery<MssqlImageBlobRow>(`
      SELECT Articulo_Id, Imagen
      FROM (
        SELECT
          Articulo_Id, Imagen,
          ROW_NUMBER() OVER (PARTITION BY Articulo_Id ORDER BY Fecha DESC) AS rn
        FROM Articulo_Imagen_FS WITH (NOLOCK)
        WHERE Emp_Id = ${EMP_ID}
          AND Tipo_Imagen = 'GRA'
          AND Imagen_Principal = 1
          AND Articulo_Id IN (${inList})
      ) t WHERE rn = 1
    `);
  }

  private async ensureImageField(): Promise<boolean> {
    // Ensure x_image_fp (char) exists on product.template for fingerprint storage
    try {
      const existing = await executeKw<OdooRecord[]>(
        'ir.model.fields', 'search_read',
        [[['model', '=', 'product.template'], ['name', '=', 'x_image_fp']]],
        { fields: ['id'], limit: 1 },
      );
      if (existing && existing.length > 0) return true;

      // Get model id for product.template
      const models = await executeKw<OdooRecord[]>(
        'ir.model', 'search_read',
        [[['model', '=', 'product.template']]],
        { fields: ['id'], limit: 1 },
      );
      if (!models || models.length === 0) {
        logger.error(CTX, '  Could not find ir.model for product.template');
        return false;
      }

      await executeKw<number>('ir.model.fields', 'create', [{
        model_id: models[0].id,
        name: 'x_image_fp',
        field_description: 'Image Fingerprint (ERP Sync)',
        ttype: 'char',
        store: true,
      }]);
      logger.info(CTX, '  Created custom field x_image_fp on product.template');
      return true;
    } catch (err) {
      logger.error(CTX, `  Failed to ensure x_image_fp field: ${(err as Error).message}`);
      return false;
    }
  }

  private async syncImages(articleMap: Map<string, NormalizedArticle>): Promise<void> {
    // Step 1: Ensure the fingerprint field exists in Odoo (read-only check is fine even in dry-run)
    const fieldReady = await this.ensureImageField();
    if (!fieldReady) {
      logger.warn(CTX, '  x_image_fp field not available, skipping image sync');
      return;
    }

    // Step 2: Fetch image metadata from MSSQL (lightweight, ~600ms)
    const imageMeta = await this.fetchImageMetadata();
    if (imageMeta.length === 0) {
      logger.info(CTX, '  No images found in MSSQL');
      return;
    }

    // Index metadata: SKU → fingerprint
    const mssqlFpMap = new Map<string, string>();
    for (const row of imageMeta) {
      const sku = row.Articulo_Id.trim();
      if (!sku) continue;
      // Fingerprint = "size|fecha_iso" — catches both content changes and re‑uploads
      const fp = `${row.img_size}|${row.Fecha}`;
      mssqlFpMap.set(sku, fp);
    }
    logger.info(CTX, `  MSSQL: ${mssqlFpMap.size} articles with images`);

    // Step 3: Fetch Odoo products with their current fingerprints
    // Only fetch products that are synced (have default_code) and are active
    const odooProducts: OdooRecord[] = [];
    const fields = ['id', 'default_code', 'x_image_fp'];
    let offset = 0;
    const batchSize = 500;
    while (true) {
      const batch = await executeKw<OdooRecord[]>(
        'product.template', 'search_read',
        [[['default_code', '!=', false]]],
        { fields, limit: batchSize, offset },
      );
      if (!batch || batch.length === 0) break;
      odooProducts.push(...batch);
      offset += batchSize;
    }

    // Index: SKU → { odooId, currentFp }
    const odooFpMap = new Map<string, { odooId: number; fp: string }>();
    for (const p of odooProducts) {
      const code = (p.default_code as string || '').trim();
      if (code) {
        odooFpMap.set(code, {
          odooId: p.id,
          fp: (p.x_image_fp as string || '').trim(),
        });
      }
    }

    // Step 4: Determine which products need image upload
    const needUpload: string[] = [];
    for (const [sku, mssqlFp] of mssqlFpMap) {
      // Only sync images for articles that exist in our article map (active in ERP)
      if (!articleMap.has(sku)) continue;
      const odoo = odooFpMap.get(sku);
      if (!odoo) continue; // product not yet in Odoo, will be created next run
      if (odoo.fp === mssqlFp) continue; // fingerprint matches, no change
      needUpload.push(sku);
    }

    if (needUpload.length === 0) {
      logger.info(CTX, '  All product images are up to date');
      return;
    }

    logger.info(CTX, `  ${needUpload.length} products need image upload`);

    if (this.dryRun) {
      logger.info(CTX, `  [DRY RUN] Would upload images for ${needUpload.length} products`);
      for (const sku of needUpload.slice(0, 10)) {
        const fp = mssqlFpMap.get(sku) || '';
        const odoo = odooFpMap.get(sku);
        logger.info(CTX, `  [DRY RUN]   ${sku} fp: "${odoo?.fp || ''}" → "${fp}"`);
      }
      if (needUpload.length > 10) logger.info(CTX, `  [DRY RUN]   ... and ${needUpload.length - 10} more`);
      return;
    }

    // Step 5: Fetch blobs in batches and upload to Odoo
    // OPTIMIZATION: Increased batch size for MSSQL fetch (10 -> 50)
    // OPTIMIZATION: Concurrent uploads to Odoo (concurrency = 5)
    const BLOB_BATCH = 50; 
    const CONCURRENCY = 5;

    let uploaded = 0;
    let failed = 0;

    for (const blobBatch of chunk(needUpload, BLOB_BATCH)) {
      let blobs: MssqlImageBlobRow[];
      try {
        blobs = await this.fetchImageBlobs(blobBatch);
      } catch (err) {
        logger.error(CTX, `  Failed to fetch image blobs: ${(err as Error).message}`);
        failed += blobBatch.length;
        continue;
      }

      const blobMap = new Map<string, Buffer>();
      for (const b of blobs) {
        blobMap.set(b.Articulo_Id.trim(), b.Imagen);
      }

      // Process batch with concurrency
      const queue = [...blobBatch];
      const workers = Array(CONCURRENCY).fill(null).map(async () => {
        while (queue.length > 0) {
            const sku = queue.shift();
            if (!sku) break;

            const imgBuf = blobMap.get(sku);
            const odoo = odooFpMap.get(sku);
            
            if (!imgBuf || !odoo) {
              failed++;
              continue;
            }

            try {
              // Convert binary to base64 for Odoo
              const base64 = imgBuf.toString('base64');
              const mssqlFp = mssqlFpMap.get(sku) || '';

              await executeKw<boolean>('product.template', 'write', [
                [odoo.odooId],
                { image_1920: base64, x_image_fp: mssqlFp },
              ]);
              uploaded++;
            } catch (err) {
              logger.error(CTX, `  Image upload failed for ${sku}: ${(err as Error).message}`);
              failed++;
            }
        }
      });

      await Promise.all(workers);

      if (uploaded % 50 === 0 && uploaded > 0) {
        logger.info(CTX, `  Progress: ${uploaded} uploaded, ${failed} failed`);
      }
    }

    logger.info(CTX, `  Images: ${uploaded} uploaded, ${failed} failed (of ${needUpload.length} needed)`);
  }
}
