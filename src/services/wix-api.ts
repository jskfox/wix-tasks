import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'WixAPI';

interface WixRequestOptions {
  method: 'GET' | 'POST' | 'PATCH' | 'DELETE';
  path: string;
  body?: unknown;
}

async function wixFetch<T = unknown>(options: WixRequestOptions): Promise<T> {
  const url = `${config.wix.baseUrl}${options.path}`;
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Authorization': config.wix.apiKey,
    'wix-site-id': config.wix.siteId,
  };

  logger.debug(CTX, `${options.method} ${url}`);

  const response = await fetch(url, {
    method: options.method,
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Wix API ${options.method} ${options.path} returned ${response.status}: ${errorBody}`);
  }

  return response.json() as Promise<T>;
}

// ─── Abandoned Checkouts ───────────────────────────────────────────────────────

export interface AbandonedCheckoutBuyerInfo {
  email?: string;
  phone?: string;
  firstName?: string;
  lastName?: string;
}

export interface AbandonedCheckoutLineItem {
  catalogReference?: {
    catalogItemId?: string;
    appId?: string;
  };
  productName?: { original?: string };
  quantity?: number;
  price?: { amount?: string; formattedAmount?: string };
  image?: { url?: string };
}

export interface AbandonedCheckout {
  _id: string;
  _createdDate: string;
  _updatedDate?: string;
  status?: string;
  checkoutUrl?: string;
  buyerInfo?: AbandonedCheckoutBuyerInfo;
  contactDetails?: {
    firstName?: string;
    lastName?: string;
    phone?: string;
    email?: string;
    address?: {
      city?: string;
      country?: string;
    };
  };
  lineItems?: AbandonedCheckoutLineItem[];
  subtotal?: { amount?: string; formattedAmount?: string };
  total?: { amount?: string; formattedAmount?: string };
  activities?: Array<{ type?: string; timestamp?: string }>;
}

interface QueryAbandonedCheckoutsResponse {
  abandonedCheckouts: AbandonedCheckout[];
  metadata?: { count?: number; offset?: number; total?: number };
}

export async function queryAbandonedCheckouts(
  filter: Record<string, unknown> = {},
  limit = 100,
  offset = 0,
): Promise<AbandonedCheckout[]> {
  const result = await wixFetch<QueryAbandonedCheckoutsResponse>({
    method: 'POST',
    path: '/ecom/v1/abandoned-checkout/query',
    body: {
      query: {
        filter,
        paging: { limit, offset },
        sort: [{ fieldName: 'createdDate', order: 'DESC' }],
      },
    },
  });
  return result.abandonedCheckouts || [];
}

// ─── Products / Variants ────────────────────────────────────────────────────────

export interface WixProduct {
  id: string;
  name?: string;
  sku?: string;
  variants?: Array<{
    id: string;
    sku?: string;
    choices?: Record<string, string>;
    variant?: {
      priceData?: { price?: number; discountedPrice?: number };
    };
  }>;
}

interface QueryProductsResponse {
  products: WixProduct[];
  metadata?: { count?: number; offset?: number; total?: number };
}

export async function queryProductsBySku(skus: string[]): Promise<WixProduct[]> {
  // Uses Stores Catalog V1 query
  const allProducts: WixProduct[] = [];
  // Query in batches of 100
  for (let i = 0; i < skus.length; i += 100) {
    const batch = skus.slice(i, i + 100);
    const result = await wixFetch<QueryProductsResponse>({
      method: 'POST',
      path: '/stores/v1/products/query',
      body: {
        query: {
          filter: { sku: { $in: batch } },
          paging: { limit: 100 },
        },
        includeVariants: true,
      },
    });
    allProducts.push(...(result.products || []));
  }
  return allProducts;
}

// ─── Update Product Variants (price) ────────────────────────────────────────────

export async function updateProductVariantPrice(
  productId: string,
  variants: Array<{ choices?: Record<string, string>; price: number }>,
): Promise<void> {
  // TODO: UNCOMMENT WHEN READY FOR PRODUCTION — Wix write operation
  // await wixFetch({
  //   method: 'PATCH',
  //   path: `/stores/v1/products/${productId}/variants`,
  //   body: { variants },
  // });
  logger.info(CTX, `[DRY-RUN] Would update price for product ${productId}`, variants);
}

// ─── Inventory ──────────────────────────────────────────────────────────────────

export async function updateInventoryVariants(
  productId: string,
  trackQuantity: boolean,
  variants: Array<{ variantId: string; quantity?: number; inStock?: boolean }>,
): Promise<void> {
  // TODO: UNCOMMENT WHEN READY FOR PRODUCTION — Wix write operation
  // await wixFetch({
  //   method: 'PATCH',
  //   path: `/stores/v2/inventoryItems/${productId}`,
  //   body: {
  //     inventoryItem: {
  //       trackQuantity,
  //       variants,
  //     },
  //   },
  // });
  logger.info(CTX, `[DRY-RUN] Would update inventory for product ${productId}`, { trackQuantity, variants });
}
