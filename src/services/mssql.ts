import sql from 'mssql';
import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'MSSQL';

let pool: sql.ConnectionPool | null = null;

function isConnectionError(err: unknown): boolean {
  const msg = (err instanceof Error ? err.message : String(err)).toLowerCase();
  const code = (err as { code?: string }).code?.toLowerCase();
  return (
    msg.includes('connection')
    || msg.includes('connect')
    || msg.includes('login')
    || msg.includes('timeout')
    || msg.includes('econn')
    || msg.includes('esocket')
    || msg.includes('ehostunreach')
    || msg.includes('enetunreach')
    || (code ? ['esocket', 'econnreset', 'econnrefused', 'etimedout'].includes(code) : false)
  );
}

export async function getMssqlPool(): Promise<sql.ConnectionPool> {
  if (pool?.connected) return pool;
  if (pool?.connecting) return pool.connect();

  const sqlConfig: sql.config = {
    server: config.mssql.server,
    port: config.mssql.port,
    database: config.mssql.database,
    user: config.mssql.user,
    password: config.mssql.password,
    options: {
      encrypt: config.mssql.encrypt,
      trustServerCertificate: config.mssql.trustServerCertificate,
    },
    pool: {
      max: 3,
      min: 0,
      idleTimeoutMillis: 30000,
    },
    requestTimeout: 60000,
    connectionTimeout: 15000,
  };

  pool = new sql.ConnectionPool(sqlConfig);
  pool.on('error', (err) => {
    logger.error(CTX, 'Pool error', err.message);
    void pool?.close().catch(() => undefined);
    pool = null;
  });

  try {
    await pool.connect();
    logger.info(CTX, `Connected to ${sqlConfig.server}:${sqlConfig.port}/${sqlConfig.database}`);
    return pool;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(CTX, `Connection failed: ${msg}`);
    try { await pool.close(); } catch (_) {}
    pool = null;
    throw new Error(`MSSQL connection failed: ${msg}`);
  }
}

export async function mssqlQuery<T>(
  queryText: string,
): Promise<T[]> {
  const p = await getMssqlPool();
  const start = Date.now();
  try {
    const result = await p.request().query<T>(queryText);
    const duration = Date.now() - start;
    logger.debug(CTX, `Query executed in ${duration}ms — rows: ${result.recordset.length}`, {
      query: queryText.substring(0, 120),
    });
    return result.recordset;
  } catch (err) {
    if (isConnectionError(err)) {
      await closeMssqlPool();
    }
    throw err;
  }
}

export async function closeMssqlPool(): Promise<void> {
  if (pool) {
    await pool.close();
    pool = null;
    logger.info(CTX, 'Pool closed');
  }
}
