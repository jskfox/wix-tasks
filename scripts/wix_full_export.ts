/**
 * Script para exportar TODOS los productos de Wix a CSV
 * Usa el MCP de Wix para obtener los datos
 */

import { writeFileSync } from 'fs';

interface Product {
  id: string;
  name: string;
  sku?: string;
  visible: boolean;
  productType: string;
  price?: { price: number; discountedPrice: number; currency: string };
  stock?: { trackInventory: boolean; quantity?: number; inStock: boolean; inventoryStatus: string };
  variants?: Array<{
    variant: { sku?: string; priceData?: { price: number; discountedPrice: number; currency: string } };
    stock?: { trackQuantity: boolean; quantity?: number; inStock: boolean };
  }>;
  lastUpdated: string;
}

// Este script debe ser ejecutado manualmente haciendo llamadas al MCP
// Ya que las llamadas directas a la API no funcionan con las credenciales actuales

console.log('Para exportar todos los productos de Wix:');
console.log('1. Usa el MCP CallWixSiteAPI con estos parámetros:');
console.log('   - URL: https://www.wixapis.com/stores/v1/products/query');
console.log('   - Method: POST');
console.log('   - Body: {"query": {"paging": {"limit": 100, "offset": OFFSET}}, "includeVariants": true}');
console.log('2. Incrementa OFFSET de 0 a 2500 en pasos de 100');
console.log('3. Total de llamadas necesarias: 26');
console.log('');
console.log('El CSV se generará automáticamente cuando tengas todos los datos.');

function generateCSV(products: Product[]): void {
  const rows: string[] = [];
  rows.push('SKU,Nombre,Precio,Precio con Descuento,Moneda,Cantidad en Stock,En Stock,Estado de Inventario,Rastrea Inventario,Visible,Tipo de Producto,ID Producto,Última Actualización');

  products.forEach(product => {
    const variant = product.variants?.[0]?.variant || {};
    const stock = product.variants?.[0]?.stock || product.stock || {} as any;
    
    const sku = (variant.sku || product.sku || '').replace(/"/g, '""');
    const name = (product.name || '').replace(/"/g, '""');
    const price = product.price?.price || variant.priceData?.price || 0;
    const discountedPrice = product.price?.discountedPrice || variant.priceData?.discountedPrice || price;
    const currency = product.price?.currency || variant.priceData?.currency || 'MXN';
    const quantity = stock.quantity || 0;
    const inStock = stock.inStock ? 'Sí' : 'No';
    const inventoryStatus = product.stock?.inventoryStatus || 'N/A';
    
    let trackInventory = 'N/A';
    if (product.stock?.trackInventory !== undefined) {
      trackInventory = product.stock.trackInventory ? 'Sí' : 'No';
    } else if (stock.trackQuantity !== undefined) {
      trackInventory = stock.trackQuantity ? 'Sí' : 'No';
    }
    
    const visible = product.visible ? 'Sí' : 'No';
    const productType = product.productType || '';
    const productId = product.id || '';
    const lastUpdated = product.lastUpdated || '';
    
    rows.push(`"${sku}","${name}",${price},${discountedPrice},"${currency}",${quantity},"${inStock}","${inventoryStatus}","${trackInventory}","${visible}","${productType}","${productId}","${lastUpdated}"`);
  });

  const csvContent = rows.join('\n');
  writeFileSync('/home/jorge/Dev/wix-tasks/wix_catalog_export.csv', csvContent, 'utf8');
  
  console.log(`\n✓ CSV generado: wix_catalog_export.csv`);
  console.log(`✓ Total productos: ${products.length}`);
}

export { generateCSV };
