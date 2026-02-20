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
  priceData?: { price?: number; discountedPrice?: number };
  stock?: { trackInventory?: boolean; quantity?: number; inStock?: boolean };
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

export async function updateProductPrice(
  productId: string,
  price: number,
): Promise<void> {
  await wixFetch({
    method: 'PATCH',
    path: `/stores/v1/products/${productId}`,
    body: { product: { priceData: { price } } },
  });
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

/**
 * Update inventory for multiple products using a sliding-window rate limiter.
 *
 * Wix REST API limit: 200 requests/minute per instance (official docs).
 * We target 180 req/min (10% safety margin).
 *
 * The sliding window dynamically maximizes throughput:
 *   - Few items  (≤180): all dispatched immediately in parallel → done in ~500ms
 *   - Many items (>180): first 180 go at full speed, then auto-throttles to
 *                        180/min sustaining maximum throughput without ban risk.
 *
 * Estimated time: max(latency, ceil(N/180) minutes)
 *
 * @returns counts of successes and failures
 */
export async function updateInventoryVariantsConcurrent(
  items: Array<{
    inventoryItemId: string;
    trackQuantity: boolean;
    variants: Array<{ variantId: string; quantity?: number; inStock?: boolean }>;
    sku?: string;
  }>,
  ratePerMinute = 180,
  maxConcurrent = 20,
): Promise<{ successes: number; failures: number; failedSkus: string[] }> {
  if (items.length === 0) return { successes: 0, failures: 0, failedSkus: [] };

  const windowMs = 60_000;
  // Timestamps of requests dispatched within the current sliding window
  const windowTimestamps: number[] = [];

  let successes = 0;
  let failures = 0;
  const failedSkus: string[] = [];
  let inFlight = 0;
  let index = 0;

  await new Promise<void>((resolve) => {
    let settled = 0;
    const total = items.length;

    function availableSlots(): number {
      const now = Date.now();
      // Evict timestamps older than 60 seconds
      while (windowTimestamps.length > 0 && now - windowTimestamps[0] > windowMs) {
        windowTimestamps.shift();
      }
      const byRate = ratePerMinute - windowTimestamps.length;
      const byConcurrency = maxConcurrent - inFlight;
      return Math.min(byRate, byConcurrency);
    }

    function tryDispatch(): void {
      const slots = availableSlots();

      if (slots <= 0 || index >= total) {
        // Schedule retry when the oldest window entry expires and frees a slot
        if (index < total && windowTimestamps.length > 0) {
          const wait = windowMs - (Date.now() - windowTimestamps[0]) + 1;
          setTimeout(tryDispatch, wait);
        }
        return;
      }

      // Dispatch as many as current slots allow
      const toDispatch = Math.min(slots, total - index);
      for (let i = 0; i < toDispatch; i++) {
        const item = items[index++];
        inFlight++;
        windowTimestamps.push(Date.now());

        updateInventoryVariants(item.inventoryItemId, item.trackQuantity, item.variants)
          .then(() => { successes++; })
          .catch((err: unknown) => {
            failures++;
            if (item.sku) failedSkus.push(item.sku);
            const msg = err instanceof Error ? err.message : String(err);
            logger.warn(CTX, `Failed to update inventoryItem ${item.inventoryItemId}: ${msg}`);
          })
          .finally(() => {
            inFlight--;
            settled++;
            if (settled === total) {
              resolve();
            } else {
              tryDispatch();
            }
          });
      }
    }

    tryDispatch();
  });

  return { successes, failures, failedSkus };
}
