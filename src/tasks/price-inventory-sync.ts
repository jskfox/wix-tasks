import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { query } from '../services/database';
import {
  queryProductsBySku,
  updateProductVariantPrice,
  updateInventoryVariants,
} from '../services/wix-api';

const CTX = 'PriceInventorySync';

interface PriceChangeRow {
  sku: string;
  precio: string;
  existencia: string;
  nombre_corto: string;
  precio_actualizado: string;
}

export class PriceInventorySyncTask extends BaseTask {
  readonly name = 'price-inventory-sync';
  readonly description = 'Sincroniza cambios de precios e inventario desde PostgreSQL hacia la tienda Wix. Detecta cambios recientes y actualiza variantes de producto.';
  // Every hour at minute 5 and 35 (Pacific)
  readonly cronExpression = '5,35 * * * *';

  async execute(): Promise<void> {
    const sucursal = config.sucursalWix;
    logger.info(CTX, `Checking price/inventory changes for sucursal ${sucursal}...`);

    // ── 1. Query recent price changes from PostgreSQL ────────────────────────
    // Use a 35-minute window to ensure we catch everything between runs
    const result = await query<PriceChangeRow>(
      `SELECT sku, precio, existencia, nombre_corto, precio_actualizado
       FROM maestro_precios_sucursal
       WHERE sucursal = $1
         AND precio_actualizado >= NOW() - INTERVAL '35 minutes'
       ORDER BY precio_actualizado DESC`,
      [sucursal],
    );

    const changedRows = result.rows;

    if (changedRows.length === 0) {
      logger.info(CTX, 'No price/inventory changes detected in the last 35 minutes');
      return;
    }

    logger.info(CTX, `Found ${changedRows.length} SKU(s) with recent changes`);

    // ── 2. Log each changed SKU ──────────────────────────────────────────────
    for (const row of changedRows) {
      logger.info(CTX, `  SKU: ${row.sku} | Price: ${row.precio} | Stock: ${row.existencia} | Name: ${row.nombre_corto} | Updated: ${row.precio_actualizado}`);
    }

    // ── 3. Look up these SKUs in Wix to get product IDs ──────────────────────
    const skus = changedRows.map(r => r.sku);

    // TODO: UNCOMMENT WHEN READY FOR PRODUCTION — Wix read operation
    // const wixProducts = await queryProductsBySku(skus);
    // logger.info(CTX, `Matched ${wixProducts.length} Wix product(s) for ${skus.length} SKU(s)`);
    logger.info(CTX, `[DRY-RUN] Would query Wix for ${skus.length} SKU(s): ${skus.join(', ')}`);

    // ── 4. Update prices and inventory in Wix ────────────────────────────────
    // TODO: UNCOMMENT WHEN READY FOR PRODUCTION
    // const skuToRow = new Map(changedRows.map(r => [r.sku, r]));
    //
    // for (const product of wixProducts) {
    //   const productSku = product.sku;
    //   if (!productSku) continue;
    //
    //   const row = skuToRow.get(productSku);
    //   if (!row) continue;
    //
    //   const newPrice = parseFloat(row.precio);
    //   const newStock = parseFloat(row.existencia);
    //
    //   // Update price
    //   if (product.variants && product.variants.length > 0) {
    //     const variantUpdates = product.variants.map(v => ({
    //       choices: v.choices,
    //       price: newPrice,
    //     }));
    //     await updateProductVariantPrice(product.id, variantUpdates);
    //     logger.info(CTX, `Updated price for SKU ${productSku} → $${newPrice}`);
    //   }
    //
    //   // Update inventory
    //   const defaultVariantId = '00000000-0000-0000-0000-000000000000';
    //   await updateInventoryVariants(product.id, true, [
    //     { variantId: defaultVariantId, quantity: Math.floor(newStock) },
    //   ]);
    //   logger.info(CTX, `Updated inventory for SKU ${productSku} → ${Math.floor(newStock)} units`);
    // }

    logger.info(CTX, `[DRY-RUN] Sync cycle complete. ${changedRows.length} SKU(s) would be synced to Wix.`);
  }
}
