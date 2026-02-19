#!/usr/bin/env ts-node
/**
 * Setup script for initial database configuration.
 * 
 * SAFE TO RUN MULTIPLE TIMES — uses INSERT OR IGNORE, never overwrites existing values.
 * 
 * Usage:
 *   npx ts-node scripts/setup-db.ts          # Apply all settings
 *   npx ts-node scripts/setup-db.ts --dry    # Preview what would be set
 * 
 * On a fresh deploy:
 *   1. Copy .env.example → .env and fill in secrets
 *   2. Run: npx ts-node scripts/setup-db.ts
 *   3. Start the app: bun run dev
 * 
 * On an existing deploy:
 *   - Running this script again is safe — existing values are NOT overwritten
 *   - Only NEW keys (added since last setup) will be inserted
 */

import Database from 'better-sqlite3';
import * as path from 'path';
import * as fs from 'fs';
import * as dotenv from 'dotenv';

dotenv.config();

const dryRun = process.argv.includes('--dry');
const stateDir = process.env.STATE_DIR || './state';
const dbPath = path.join(stateDir, 'settings.db');

// ── Production values ─────────────────────────────────────────────────────────
// Edit these values to match your production environment.
// These are NON-SENSITIVE settings only — secrets stay in .env
//
// IMPORTANT: These values are only applied on first run (INSERT OR IGNORE).
// To change a value after first setup, use the Admin Dashboard or setSetting().

const PRODUCTION_SETTINGS: Array<{ key: string; value: string; category: string; description: string }> = [
  // ── General ──────────────────────────────────────────────────────────────────
  { key: 'timezone',       value: 'America/Los_Angeles', category: 'general', description: 'Zona horaria del sistema (IANA)' },
  { key: 'log_level',      value: 'debug',               category: 'general', description: 'Nivel de log: debug, info, warn, error' },
  { key: 'sucursal_wix',   value: '101',                 category: 'general', description: 'ID de sucursal para Wix' },
  { key: 'state_dir',      value: './state',             category: 'general', description: 'Directorio para archivos de estado (watermarks, snapshots)' },

  // ── SMTP ─────────────────────────────────────────────────────────────────────
  { key: 'smtp.port',   value: '587',  category: 'smtp', description: 'Puerto del servidor SMTP' },
  { key: 'smtp.secure', value: 'false', category: 'smtp', description: 'Usar TLS para SMTP (true/false)' },
  { key: 'smtp.from',   value: 'sistemas@e-proconsa.com', category: 'smtp', description: 'Dirección de remitente para correos' },

  // ── Email recipients ─────────────────────────────────────────────────────────
  { key: 'emails.marketing',        value: 'it@e-proconsa.com',                                              category: 'emails', description: 'Destinatarios generales de marketing (fallback). Separar con coma.' },
  { key: 'emails.abandoned_carts',  value: 'Marketin@e-proconsa.com,rochoa@e-proconsa.com,it@e-proconsa.com', category: 'emails', description: 'Destinatarios del reporte de carritos abandonados. Separar con coma.' },
  { key: 'emails.chat_leads',       value: 'Marketin@e-proconsa.com,rochoa@e-proconsa.com,it@e-proconsa.com', category: 'emails', description: 'Destinatarios del reporte de leads del chat. Separar con coma.' },
  { key: 'emails.chat_analysis',    value: 'it@e-proconsa.com',                                              category: 'emails', description: 'Destinatarios del reporte de análisis del chat. Separar con coma.' },
  { key: 'emails.erp_postgres_sync', value: '',                                                              category: 'emails', description: 'Destinatarios del reporte de sincronización ERP→PostgreSQL. Separar con coma.' },

  // ── MSSQL ────────────────────────────────────────────────────────────────────
  { key: 'mssql.port',             value: '1433',            category: 'mssql', description: 'Puerto del servidor MSSQL' },
  { key: 'mssql.database',         value: 'ldcom_proconsa',  category: 'mssql', description: 'Base de datos MSSQL' },
  { key: 'mssql.encrypt',          value: 'false',           category: 'mssql', description: 'Encriptar conexión MSSQL (true/false)' },
  { key: 'mssql.trust_server_cert', value: 'true',           category: 'mssql', description: 'Confiar en certificado del servidor MSSQL (true/false)' },
  { key: 'mssql.emp_id',           value: '1',               category: 'mssql', description: 'ID de empleado para consultas MSSQL' },

  // ── PostgreSQL ───────────────────────────────────────────────────────────────
  { key: 'pg.port',     value: '5632',   category: 'pg', description: 'Puerto del servidor PostgreSQL' },
  { key: 'pg.database', value: 'prices', category: 'pg', description: 'Base de datos PostgreSQL' },

  // ── Odoo ─────────────────────────────────────────────────────────────────────
  { key: 'odoo.livechat_channel_id',      value: '1',       category: 'odoo', description: 'ID del canal de livechat en Odoo' },
  { key: 'odoo.reports_dir',              value: './reports', category: 'odoo', description: 'Directorio para reportes generados' },
  { key: 'odoo.stock_write_concurrency',  value: '6',       category: 'odoo', description: 'Concurrencia de escritura de stock en Odoo' },
  { key: 'odoo.stock_write_retries',      value: '4',       category: 'odoo', description: 'Reintentos de escritura de stock en Odoo' },
  { key: 'odoo.product_write_concurrency', value: '10',     category: 'odoo', description: 'Concurrencia de escritura de productos en Odoo' },
  { key: 'odoo.product_write_retries',    value: '3',       category: 'odoo', description: 'Reintentos de escritura de productos en Odoo' },
  { key: 'odoo.rpc_timeout_ms',           value: '300000',  category: 'odoo', description: 'Timeout de RPC en milisegundos' },

  // ── Wix sync ─────────────────────────────────────────────────────────────────
  { key: 'wix.min_stock_threshold', value: '10',   category: 'wix', description: 'Umbral mínimo de stock total. Si el stock sumado es menor, se pone en 0 en Wix.' },
  { key: 'wix.dry_run',             value: 'true', category: 'wix', description: 'Modo prueba: no escribe en Wix (true/false). Cambiar a false para activar.' },
  { key: 'wix.branch_prefix',       value: '1',    category: 'wix', description: 'Prefijo de sucursales a incluir en suma de stock (ej: "1" para Mexicali).' },

  // ── Tasks state ─────────────────────────────────────────────────────────────────
  { key: 'task.odoo-inventory-sync-full.enabled',  value: 'true', category: 'tasks', description: 'Habilitar tarea de sincronización completa de inventario Odoo (true/false)' },
  { key: 'task.odoo-inventory-sync-stock.enabled', value: 'true', category: 'tasks', description: 'Habilitar tarea de sincronización de stock Odoo (true/false)' },

  // ── ERP→Odoo sync ────────────────────────────────────────────────────────────
  { key: 'erp_odoo.dry_run',             value: 'false',  category: 'erp_odoo', description: 'Modo prueba: no escribe en Odoo (true/false)' },
  { key: 'erp_odoo.max_inventory_rows',  value: '50000',  category: 'erp_odoo', description: 'Máximo de filas de inventario por ejecución' },
  { key: 'erp_odoo.max_product_rows',    value: '20000',  category: 'erp_odoo', description: 'Máximo de filas de productos por ejecución' },
  { key: 'erp_odoo.max_image_rows',      value: '3000',   category: 'erp_odoo', description: 'Máximo de filas de imágenes por ejecución' },
];

// ── Main ──────────────────────────────────────────────────────────────────────

function main(): void {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║         Wix Tasks — Database Setup Script            ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.log('');

  if (dryRun) {
    console.log('  MODE: DRY RUN — no changes will be written\n');
  }

  if (!fs.existsSync(stateDir)) {
    if (!dryRun) {
      fs.mkdirSync(stateDir, { recursive: true });
      console.log(`  Created state directory: ${stateDir}`);
    } else {
      console.log(`  Would create state directory: ${stateDir}`);
    }
  }

  if (dryRun) {
    console.log(`  Database: ${dbPath}\n`);
    console.log('  Settings that would be applied (INSERT OR IGNORE):\n');
    for (const s of PRODUCTION_SETTINGS) {
      const displayValue = s.value === '' ? '(empty)' : s.value;
      console.log(`  [${s.category}] ${s.key} = ${displayValue}`);
    }
    console.log(`\n  Total: ${PRODUCTION_SETTINGS.length} settings`);
    console.log('\n  Run without --dry to apply.\n');
    return;
  }

  const isNew = !fs.existsSync(dbPath);
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');

  db.exec(`
    CREATE TABLE IF NOT EXISTS settings (
      key         TEXT PRIMARY KEY,
      value       TEXT NOT NULL DEFAULT '',
      category    TEXT NOT NULL DEFAULT 'general',
      description TEXT NOT NULL DEFAULT '',
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `);

  const insert = db.prepare(`
    INSERT OR IGNORE INTO settings (key, value, category, description, updated_at)
    VALUES (@key, @value, @category, @description, datetime('now'))
  `);

  let inserted = 0;
  let skipped = 0;

  const applyAll = db.transaction(() => {
    for (const s of PRODUCTION_SETTINGS) {
      const result = insert.run(s);
      if (result.changes > 0) {
        inserted++;
        const displayValue = s.value === '' ? '(empty)' : s.value;
        console.log(`  ✓ SET   [${s.category}] ${s.key} = ${displayValue}`);
      } else {
        skipped++;
        console.log(`  · SKIP  [${s.category}] ${s.key} (already set)`);
      }
    }
  });

  applyAll();
  db.close();

  console.log('');
  console.log(`  Database: ${dbPath} (${isNew ? 'created' : 'existing'})`);
  console.log(`  Applied:  ${inserted} setting(s)`);
  console.log(`  Skipped:  ${skipped} setting(s) (already had values)`);
  console.log('');

  if (inserted > 0) {
    console.log('  ✅ Setup complete. Review the Admin Dashboard to verify values.');
  } else {
    console.log('  ✅ No changes needed — all settings already configured.');
  }
  console.log('');
}

main();
