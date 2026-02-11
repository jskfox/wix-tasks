import { Pool, PoolConfig, QueryResult, QueryResultRow } from 'pg';
import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'Database';

let pool: Pool | null = null;

export function getPool(overrideConfig?: PoolConfig): Pool {
  if (!pool) {
    const pgConfig: PoolConfig = overrideConfig ?? {
      host: config.pg.host,
      port: config.pg.port,
      database: config.pg.database,
      user: config.pg.user,
      password: config.pg.password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    };
    pool = new Pool(pgConfig);

    pool.on('error', (err) => {
      logger.error(CTX, 'Unexpected error on idle client', err.message);
    });

    logger.info(CTX, `Pool created for ${pgConfig.host}:${pgConfig.port}/${pgConfig.database}`);
  }
  return pool;
}

export async function query<T extends QueryResultRow>(
  text: string,
  params?: unknown[],
): Promise<QueryResult<T>> {
  const p = getPool();
  const start = Date.now();
  const result = await p.query<T>(text, params);
  const duration = Date.now() - start;
  logger.debug(CTX, `Query executed in ${duration}ms — rows: ${result.rowCount}`, { text: text.substring(0, 120) });
  return result;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    logger.info(CTX, 'Pool closed');
  }
}
