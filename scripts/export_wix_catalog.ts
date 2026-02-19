import { config } from '../src/config';
import * as fs from 'fs';
import * as https from 'https';

const BATCH_SIZE = 100;
const OUTPUT_FILE = './wix_catalog_export.csv';

interface WixProduct {
  id: string;
  name: string;
  sku?: string;
  visible: boolean;
  productType: string;
  price?: {
    price: number;
    discountedPrice: number;
    currency: string;
  };
  stock?: {
    trackInventory: boolean;
    quantity: number;
    inStock: boolean;
    inventoryStatus: string;
  };
  variants?: Array<{
    variant: {
      sku?: string;
      priceData?: {
        price: number;
        discountedPrice: number;
        currency: string;
      };
    };
    stock?: {
      trackQuantity: boolean;
      quantity: number;
      inStock: boolean;
    };
  }>;
  lastUpdated: string;
}

async function fetchBatch(offset: number): Promise<WixProduct[]> {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({
      query: {
        paging: {
          limit: BATCH_SIZE,
          offset: offset
        }
      },
      includeVariants: true
    });

    const options = {
      hostname: 'www.wixapis.com',
      port: 443,
      path: '/stores/v1/products/query',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': postData.length,
        'Authorization': config.wix.apiKey
      }
    };

    const req = https.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const result = JSON.parse(data);
          resolve(result.products || []);
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    req.write(postData);
    req.end();
  });
}

async function main() {
  console.log('Iniciando exportación del catálogo de Wix...');
  
  // Primera llamada para obtener el total
  const firstBatch = await fetchBatch(0);
  console.log(`Primera llamada completada: ${firstBatch.length} productos`);
  
  let allProducts: WixProduct[] = [...firstBatch];
  
  // Obtener el resto de productos
  let offset = BATCH_SIZE;
  let hasMore = firstBatch.length === BATCH_SIZE;
  
  while (hasMore) {
    const batch = await fetchBatch(offset);
    console.log(`Offset ${offset}: ${batch.length} productos obtenidos`);
    
    if (batch.length > 0) {
      allProducts = allProducts.concat(batch);
      offset += BATCH_SIZE;
      hasMore = batch.length === BATCH_SIZE;
      
      // Pausa para no saturar la API
      await new Promise(resolve => setTimeout(resolve, 100));
    } else {
      hasMore = false;
    }
  }
  
  console.log(`\nTotal productos obtenidos: ${allProducts.length}`);
  
  // Generar CSV
  const csvRows: string[] = [];
  csvRows.push('SKU,Nombre,Precio,Precio con Descuento,Moneda,Cantidad en Stock,En Stock,Estado de Inventario,Rastrea Inventario,Visible,Tipo de Producto,ID Producto,Última Actualización');
  
  allProducts.forEach(product => {
    const variant = product.variants?.[0]?.variant || {};
    const stock = product.variants?.[0]?.stock || product.stock || {};
    
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
    } else if ('trackQuantity' in stock) {
      trackInventory = (stock as any).trackQuantity ? 'Sí' : 'No';
    }
    
    const visible = product.visible ? 'Sí' : 'No';
    const productType = product.productType || '';
    const productId = product.id || '';
    const lastUpdated = product.lastUpdated || '';
    
    csvRows.push(`"${sku}","${name}",${price},${discountedPrice},"${currency}",${quantity},"${inStock}","${inventoryStatus}","${trackInventory}","${visible}","${productType}","${productId}","${lastUpdated}"`);
  });
  
  const csvContent = csvRows.join('\n');
  fs.writeFileSync(OUTPUT_FILE, csvContent, 'utf8');
  
  console.log(`\n✓ CSV generado exitosamente: ${OUTPUT_FILE}`);
  console.log(`✓ Total de productos: ${allProducts.length}`);
  console.log(`✓ Total de filas (incluyendo encabezado): ${csvRows.length}`);
}

main().catch(console.error);
