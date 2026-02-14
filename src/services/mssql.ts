import sql from 'mssql';
import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'MSSQL';

let pool: sql.ConnectionPool | null = null;

export async function getMssqlPool(): Promise<sql.ConnectionPool> {
  if (pool?.connected) return pool;

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
  });

  await pool.connect();
  logger.info(CTX, `Connected to ${sqlConfig.server}:${sqlConfig.port}/${sqlConfig.database}`);
  return pool;
}

export async function mssqlQuery<T>(
  queryText: string,
): Promise<T[]> {
  const p = await getMssqlPool();
  const start = Date.now();
  const result = await p.request().query<T>(queryText);
  const duration = Date.now() - start;
  logger.debug(CTX, `Query executed in ${duration}ms — rows: ${result.recordset.length}`, {
    query: queryText.substring(0, 120),
  });
  return result.recordset;
}

export async function closeMssqlPool(): Promise<void> {
  if (pool) {
    await pool.close();
    pool = null;
    logger.info(CTX, 'Pool closed');
  }
}
