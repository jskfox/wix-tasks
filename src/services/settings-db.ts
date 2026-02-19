import Database from 'better-sqlite3';
import * as path from 'path';
import * as fs from 'fs';

// NOTE: Do NOT import logger here — it creates a circular dependency:
//   config → settings-db → logger → config
// Use console.log for bootstrap messages only.
const TAG = '[SettingsDB]';

// ── Types ────────────────────────────────────────────────────────────────────

export interface Setting {
  key: string;
  value: string;
  category: string;
  description: string;
  updated_at: string;
}

// ── Default settings (seeded on first run) ───────────────────────────────────

interface DefaultSetting {
  key: string;
  value: string;
  category: string;
  description: string;
}

const DEFAULTS: DefaultSetting[] = [
  // General
  { key: 'timezone', value: 'America/Los_Angeles', category: 'general', description: 'Zona horaria del sistema (IANA)' },
  { key: 'log_level', value: 'info', category: 'general', description: 'Nivel de log: debug, info, warn, error' },
  { key: 'sucursal_wix', value: '101', category: 'general', description: 'ID de sucursal para Wix' },

  // SMTP (non-sensitive)
  { key: 'smtp.port', value: '587', category: 'smtp', description: 'Puerto del servidor SMTP' },
  { key: 'smtp.secure', value: 'false', category: 'smtp', description: 'Usar TLS para SMTP (true/false)' },
  { key: 'smtp.from', value: '', category: 'smtp', description: 'Dirección de remitente para correos' },

  // Email recipients per task
  { key: 'emails.marketing', value: '', category: 'emails', description: 'Destinatarios generales de marketing (fallback). Separar con coma.' },
  { key: 'emails.abandoned_carts', value: '', category: 'emails', description: 'Destinatarios del reporte de carritos abandonados. Separar con coma.' },
  { key: 'emails.chat_leads', value: '', category: 'emails', description: 'Destinatarios del reporte de leads del chat. Separar con coma.' },
  { key: 'emails.chat_analysis', value: '', category: 'emails', description: 'Destinatarios del reporte de análisis del chat. Separar con coma.' },
  { key: 'emails.erp_postgres_sync', value: '', category: 'emails', description: 'Destinatarios del reporte de sincronización ERP→PostgreSQL. Separar con coma.' },

  // MSSQL (non-sensitive)
  { key: 'mssql.port', value: '1433', category: 'mssql', description: 'Puerto del servidor MSSQL' },
  { key: 'mssql.database', value: 'LDCOM_PROCONSA', category: 'mssql', description: 'Base de datos MSSQL' },
  { key: 'mssql.encrypt', value: 'false', category: 'mssql', description: 'Encriptar conexión MSSQL (true/false)' },
  { key: 'mssql.trust_server_cert', value: 'true', category: 'mssql', description: 'Confiar en certificado del servidor MSSQL (true/false)' },
  { key: 'mssql.emp_id', value: '1', category: 'mssql', description: 'ID de empleado para consultas MSSQL' },

  // PostgreSQL (non-sensitive)
  { key: 'pg.port', value: '5432', category: 'pg', description: 'Puerto del servidor PostgreSQL' },
  { key: 'pg.database', value: 'prices', category: 'pg', description: 'Base de datos PostgreSQL' },

  // Odoo (non-sensitive)
  { key: 'odoo.livechat_channel_id', value: '1', category: 'odoo', description: 'ID del canal de livechat en Odoo' },
  { key: 'odoo.reports_dir', value: './reports', category: 'odoo', description: 'Directorio para reportes generados' },
  { key: 'odoo.stock_write_concurrency', value: '6', category: 'odoo', description: 'Concurrencia de escritura de stock en Odoo' },
  { key: 'odoo.stock_write_retries', value: '4', category: 'odoo', description: 'Reintentos de escritura de stock en Odoo' },
  { key: 'odoo.product_write_concurrency', value: '10', category: 'odoo', description: 'Concurrencia de escritura de productos en Odoo' },
  { key: 'odoo.product_write_retries', value: '3', category: 'odoo', description: 'Reintentos de escritura de productos en Odoo' },
  { key: 'odoo.rpc_timeout_ms', value: '300000', category: 'odoo', description: 'Timeout de RPC en milisegundos' },

  // Wix sync
  { key: 'wix.min_stock_threshold', value: '10', category: 'wix', description: 'Umbral mínimo de stock total. Si el stock sumado de todas las sucursales es menor a este valor, se pone en 0 en Wix.' },
  { key: 'wix.dry_run', value: 'true', category: 'wix', description: 'Modo prueba: no escribe en Wix (true/false). Cambiar a false para activar sincronización real.' },
  { key: 'wix.branch_prefix', value: '1', category: 'wix', description: 'Prefijo de sucursales a incluir en suma de stock (ej: "1" para Mexicali, "4" para Hermosillo).' },

  // ERP→Odoo sync
  { key: 'erp_odoo.dry_run', value: 'false', category: 'erp_odoo', description: 'Modo prueba: no escribe en Odoo (true/false)' },
  { key: 'erp_odoo.max_inventory_rows', value: '50000', category: 'erp_odoo', description: 'Máximo de filas de inventario por ejecución' },
  { key: 'erp_odoo.max_product_rows', value: '20000', category: 'erp_odoo', description: 'Máximo de filas de productos por ejecución' },
  { key: 'erp_odoo.max_image_rows', value: '3000', category: 'erp_odoo', description: 'Máximo de filas de imágenes por ejecución' },

  // State
  { key: 'state_dir', value: './state', category: 'general', description: 'Directorio para archivos de estado (watermarks, snapshots)' },
];

// ── Singleton ────────────────────────────────────────────────────────────────

let db: Database.Database | null = null;

function getDbPath(): string {
  const stateDir = process.env.STATE_DIR || './state';
  if (!fs.existsSync(stateDir)) {
    fs.mkdirSync(stateDir, { recursive: true });
  }
  return path.join(stateDir, 'settings.db');
}

function getDb(): Database.Database {
  if (db) return db;

  const dbPath = getDbPath();
  db = new Database(dbPath);

  // Enable WAL mode for better concurrent read performance
  db.pragma('journal_mode = WAL');

  // Create table if not exists
  db.exec(`
    CREATE TABLE IF NOT EXISTS settings (
      key         TEXT PRIMARY KEY,
      value       TEXT NOT NULL DEFAULT '',
      category    TEXT NOT NULL DEFAULT 'general',
      description TEXT NOT NULL DEFAULT '',
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `);

  // Seed defaults (only inserts if key doesn't exist)
  const insert = db.prepare(`
    INSERT OR IGNORE INTO settings (key, value, category, description, updated_at)
    VALUES (@key, @value, @category, @description, datetime('now'))
  `);

  const seedMany = db.transaction((defaults: DefaultSetting[]) => {
    let seeded = 0;
    for (const d of defaults) {
      const result = insert.run(d);
      if (result.changes > 0) seeded++;
    }
    return seeded;
  });

  const seeded = seedMany(DEFAULTS);
  if (seeded > 0) {
    console.log(`${TAG} Seeded ${seeded} default setting(s) into ${dbPath}`);
  }

  console.log(`${TAG} Settings DB ready: ${dbPath} (${DEFAULTS.length} keys)`);
  return db;
}

// ── Public API ───────────────────────────────────────────────────────────────

/** Get a single setting value. Returns the default if not found. */
export function getSetting(key: string, fallback = ''): string {
  const row = getDb().prepare('SELECT value FROM settings WHERE key = ?').get(key) as { value: string } | undefined;
  return row?.value ?? fallback;
}

/** Get a setting as integer */
export function getSettingInt(key: string, fallback: number): number {
  const val = getSetting(key, String(fallback));
  const parsed = parseInt(val, 10);
  return isNaN(parsed) ? fallback : parsed;
}

/** Get a setting as boolean */
export function getSettingBool(key: string, fallback: boolean): boolean {
  const val = getSetting(key, String(fallback));
  return val === 'true' || val === '1';
}

/** Get a setting as comma-separated list of trimmed strings */
export function getSettingList(key: string): string[] {
  return getSetting(key, '').split(',').map(s => s.trim()).filter(Boolean);
}

/** Set a single setting value */
export function setSetting(key: string, value: string): void {
  getDb().prepare(`
    INSERT INTO settings (key, value, updated_at)
    VALUES (?, ?, datetime('now'))
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
  `).run(key, value);
}

/** Update a setting with optional category and description */
export function updateSetting(key: string, value: string, category?: string, description?: string): void {
  if (category && description) {
    getDb().prepare(`
      INSERT INTO settings (key, value, category, description, updated_at)
      VALUES (?, ?, ?, ?, datetime('now'))
      ON CONFLICT(key) DO UPDATE SET 
        value = excluded.value,
        category = excluded.category,
        description = excluded.description,
        updated_at = datetime('now')
    `).run(key, value, category, description);
  } else {
    setSetting(key, value);
  }
}

/** Get all settings, optionally filtered by category */
export function getAllSettings(category?: string): Setting[] {
  if (category) {
    return getDb().prepare('SELECT * FROM settings WHERE category = ? ORDER BY key').all(category) as Setting[];
  }
  return getDb().prepare('SELECT * FROM settings ORDER BY category, key').all() as Setting[];
}

/** Get all distinct categories */
export function getCategories(): string[] {
  const rows = getDb().prepare('SELECT DISTINCT category FROM settings ORDER BY category').all() as { category: string }[];
  return rows.map(r => r.category);
}

/** Delete a setting */
export function deleteSetting(key: string): boolean {
  const result = getDb().prepare('DELETE FROM settings WHERE key = ?').run(key);
  return result.changes > 0;
}

/** Close the database connection (for graceful shutdown) */
export function closeSettingsDb(): void {
  if (db) {
    db.close();
    db = null;
    console.log(`${TAG} Settings DB closed`);
  }
}

// ── Helper: get email recipients for a task (with fallback) ──────────────────

export function getTaskEmails(taskKey: 'abandoned_carts' | 'chat_leads' | 'chat_analysis' | 'erp_postgres_sync'): string[] {
  const specific = getSettingList(`emails.${taskKey}`);
  if (specific.length > 0) return specific;
  return getSettingList('emails.marketing');
}
