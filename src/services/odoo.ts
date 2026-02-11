import { createClient, createSecureClient, Client } from 'xmlrpc';
import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'Odoo';

let uid: number | null = null;

function makeClient(rpcPath: string): Client {
  const url = new URL(config.odoo.url);
  const isSecure = url.protocol === 'https:';
  const opts = {
    host: url.hostname,
    port: url.port ? parseInt(url.port) : (isSecure ? 443 : 80),
    path: rpcPath,
  };
  return isSecure ? createSecureClient(opts) : createClient(opts);
}

function commonClient(): Client {
  return makeClient('/xmlrpc/2/common');
}

function objectClient(): Client {
  return makeClient('/xmlrpc/2/object');
}

function callRpc<T>(client: Client, method: string, params: unknown[]): Promise<T> {
  return new Promise((resolve, reject) => {
    client.methodCall(method, params as any[], (err: any, value: any) => {
      if (err) reject(err instanceof Error ? err : new Error(String(err)));
      else resolve(value as T);
    });
  });
}

export async function authenticate(): Promise<number> {
  if (uid !== null) return uid;

  const { db, username, password } = config.odoo;
  logger.info(CTX, `Authenticating as ${username}...`);

  const client = commonClient();
  const result = await callRpc<number | false>(client, 'authenticate', [db, username, password, {}]);

  if (!result) throw new Error('Odoo authentication failed');

  uid = result;
  logger.info(CTX, `Authenticated. UID: ${uid}`);
  return uid;
}

export async function executeKw<T>(
  model: string,
  method: string,
  args: unknown[],
  kwargs?: Record<string, unknown>,
): Promise<T> {
  const userId = await authenticate();
  const { db, password } = config.odoo;
  const client = objectClient();

  const params: unknown[] = [db, userId, password, model, method, args];
  if (kwargs) params.push(kwargs);

  return callRpc<T>(client, 'execute_kw', params);
}

// ── Convenience helpers ────────────────────────────────────────────────────

export interface OdooRecord {
  id: number;
  [key: string]: unknown;
}

export async function searchRead(
  model: string,
  domain: unknown[][],
  fields: string[],
  opts?: { limit?: number; offset?: number; order?: string },
): Promise<OdooRecord[]> {
  return executeKw<OdooRecord[]>(model, 'search_read', [domain], {
    fields,
    ...opts,
  });
}

export async function searchReadAll(
  model: string,
  domain: unknown[][],
  fields: string[],
  opts?: { order?: string; batchSize?: number },
): Promise<OdooRecord[]> {
  const batchSize = opts?.batchSize ?? 200;
  const all: OdooRecord[] = [];
  let offset = 0;

  while (true) {
    const chunk = await searchRead(model, domain, fields, {
      limit: batchSize,
      offset,
      order: opts?.order,
    });
    if (chunk.length === 0) break;
    all.push(...chunk);
    offset += batchSize;
    logger.debug(CTX, `${model} searchReadAll: ${all.length} records`);
  }

  return all;
}

export async function readRecords(
  model: string,
  ids: number[],
  fields: string[],
  batchSize = 500,
): Promise<OdooRecord[]> {
  const all: OdooRecord[] = [];

  for (let i = 0; i < ids.length; i += batchSize) {
    const chunk = ids.slice(i, i + batchSize);
    const records = await executeKw<OdooRecord[]>(model, 'read', [chunk], { fields });
    all.push(...records);
  }

  return all;
}

export async function searchCount(model: string, domain: unknown[][]): Promise<number> {
  return executeKw<number>(model, 'search_count', [domain]);
}
