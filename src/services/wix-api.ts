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
  visitorId?: string;
  memberId?: string;
  userId?: string;
  contactId?: string;
  email?: string;
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
  physicalProperties?: {
    sku?: string;
    weight?: number;
    shippable?: boolean;
  };
}

export interface AbandonedCheckout {
  id: string;
  createdDate: string;
  updatedDate?: string;
  checkoutId?: string;
  cartId?: string;
  status?: 'ABANDONED' | 'RECOVERED';
  buyerLanguage?: string;
  checkoutUrl?: string;
  buyerInfo?: AbandonedCheckoutBuyerInfo;
  contactDetails?: {
    firstName?: string;
    lastName?: string;
    phone?: string;
    company?: string;
  };
  currency?: string;
  totalPrice?: { amount?: string; convertedAmount?: string; formattedAmount?: string; formattedConvertedAmount?: string };
  subtotalPrice?: { amount?: string; convertedAmount?: string; formattedAmount?: string; formattedConvertedAmount?: string };
  lineItems?: AbandonedCheckoutLineItem[];
  activities?: Array<{ createdDate?: string; type?: string }>;
  checkoutRecoveredDate?: string;
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
  inventoryItemId?: string;
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

/**
 * Query all products from Wix store with pagination.
 * Returns products with their SKU and inventoryItemId for inventory updates.
 */
export async function queryAllWixProducts(): Promise<WixProduct[]> {
  const allProducts: WixProduct[] = [];
  let offset = 0;
  const limit = 100;

  logger.info(CTX, 'Fetching all products from Wix...');

  while (true) {
    const result = await wixFetch<QueryProductsResponse>({
      method: 'POST',
      path: '/stores/v1/products/query',
      body: {
        query: {
          // Empty filter string = all products
          filter: '{}',
          paging: { limit, offset },
        },
        includeVariants: true,
      },
    });

    const products = result.products || [];
    allProducts.push(...products);

    logger.debug(CTX, `  Fetched ${products.length} products (offset=${offset}, total so far=${allProducts.length})`);

    // If we got fewer than limit, we've reached the end
    if (products.length < limit) break;
    offset += limit;
  }

  logger.info(CTX, `Fetched ${allProducts.length} total products from Wix`);
  return allProducts;
}

/**
 * Query products by SKU from Wix store.
 * Filter must be a JSON string as per Wix API v1 requirements.
 */
export async function queryProductsBySku(skus: string[]): Promise<WixProduct[]> {
  const allProducts: WixProduct[] = [];

  // Query in batches of 100
  for (let i = 0; i < skus.length; i += 100) {
    const batch = skus.slice(i, i + 100);
    // Wix API v1 requires filter as a JSON string, not an object
    const filterObj = { sku: { $in: batch } };
    const result = await wixFetch<QueryProductsResponse>({
      method: 'POST',
      path: '/stores/v1/products/query',
      body: {
        query: {
          filter: JSON.stringify(filterObj),
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
  inventoryItemId: string,
  trackQuantity: boolean,
  variants: Array<{ variantId: string; quantity?: number; inStock?: boolean }>,
): Promise<void> {
  await wixFetch({
    method: 'PATCH',
    path: `/stores/v2/inventoryItems/${inventoryItemId}`,
    body: {
      inventoryItem: {
        trackQuantity,
        variants,
      },
    },
  });
}
