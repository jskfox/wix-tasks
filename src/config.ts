import dotenv from 'dotenv';
dotenv.config();

import {
  getSetting, getSettingInt, getSettingBool, getSettingList,
  getTaskEmails as _getTaskEmails,
} from './services/settings-db';

// ═════════════════════════════════════════════════════════════════════════════
// Configuration object
//
// CREDENTIALS & SECRETS  → from .env (never stored in SQLite)
// OPERATIONAL SETTINGS   → from SQLite settings.db (editable via admin UI)
//
// On first run, SQLite is seeded with sensible defaults.
// Env vars for non-sensitive settings are still read as INITIAL SEED overrides
// (if the setting doesn't exist yet in SQLite, the env var value is used).
// After that, SQLite is the source of truth for non-sensitive settings.
// ═════════════════════════════════════════════════════════════════════════════

export const config = {
  // ── Credentials (always from .env) ───────────────────────────────────────
  pg: {
    host: process.env.PG_HOST || 'localhost',
    get port() { return getSettingInt('pg.port', 5432); },
    get database() { return getSetting('pg.database', 'prices'); },
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || '',
  },
  mssql: {
    server: process.env.MSSQL_SERVER || 'localhost',
    get port() { return getSettingInt('mssql.port', 1433); },
    get database() { return getSetting('mssql.database', 'LDCOM_PROCONSA'); },
    user: process.env.MSSQL_USER || '',
    password: process.env.MSSQL_PASSWORD || '',
    get encrypt() { return getSettingBool('mssql.encrypt', false); },
    get trustServerCertificate() { return getSettingBool('mssql.trust_server_cert', true); },
    get empId() { return getSettingInt('mssql.emp_id', 1); },
  },
  get sucursalWix() { return getSettingInt('sucursal_wix', 101); },

  wix: {
    siteId: process.env.WIX_SITE_ID || '',
    apiKey: process.env.WIX_API_KEY || '',
    baseUrl: 'https://www.wixapis.com',
    get minStockThreshold() { return getSettingInt('wix.min_stock_threshold', 10); },
    get dryRun() { return getSettingBool('wix.dry_run', true); },
    get branchPrefix() { return getSetting('wix.branch_prefix', '1'); },
    get teamsWebhook() { return getSetting('wix.teams_webhook', ''); },
  },

  odoo: {
    url: process.env.ODOO_URL || '',
    db: process.env.ODOO_DB || '',
    username: process.env.ODOO_USERNAME || '',
    password: process.env.ODOO_PASSWORD || '',
    get livechatChannelId() { return getSettingInt('odoo.livechat_channel_id', 1); },
    get reportsDir() { return getSetting('odoo.reports_dir', './reports'); },
    get stockWriteConcurrency() { return getSettingInt('odoo.stock_write_concurrency', 6); },
    get stockWriteRetries() { return getSettingInt('odoo.stock_write_retries', 4); },
    get productWriteConcurrency() { return getSettingInt('odoo.product_write_concurrency', 10); },
    get productWriteRetries() { return getSettingInt('odoo.product_write_retries', 3); },
    get rpcTimeoutMs() { return getSettingInt('odoo.rpc_timeout_ms', 300000); },
  },

  smtp: {
    host: process.env.SMTP_HOST || '',
    get port() { return getSettingInt('smtp.port', 587); },
    get secure() { return getSettingBool('smtp.secure', false); },
    user: process.env.SMTP_USER || '',
    pass: process.env.SMTP_PASS || '',
    get from() { return getSetting('smtp.from', ''); },
  },

  // ── Email recipients (from SQLite) ───────────────────────────────────────
  get marketingEmails() { return getSettingList('emails.marketing'); },
  emails: {
    get abandonedCarts() { return getSettingList('emails.abandoned_carts'); },
    get chatLeads() { return getSettingList('emails.chat_leads'); },
    get chatAnalysis() { return getSettingList('emails.chat_analysis'); },
    get erpPostgresSync() { return getSettingList('emails.erp_postgres_sync'); },
  },

  // ── General settings (from SQLite) ───────────────────────────────────────
  get timezone() { return getSetting('timezone', 'America/Los_Angeles'); },
  get logLevel() { return getSetting('log_level', 'info'); },
  get stateDir() { return getSetting('state_dir', './state'); },

  // ── ERP→Odoo sync settings (from SQLite) ─────────────────────────────────
  erpOdoo: {
    get dryRun() { return getSettingBool('erp_odoo.dry_run', false); },
    get maxInventoryRows() { return getSettingInt('erp_odoo.max_inventory_rows', 50000); },
    get maxProductRows() { return getSettingInt('erp_odoo.max_product_rows', 20000); },
    get maxImageRows() { return getSettingInt('erp_odoo.max_image_rows', 3000); },
  },
};

// Helper function to get email recipients for a specific task
export function getEmailsForTask(taskName: 'abandonedCarts' | 'chatLeads' | 'chatAnalysis' | 'erpPostgresSync'): string[] {
  const map = {
    abandonedCarts: 'abandoned_carts' as const,
    chatLeads: 'chat_leads' as const,
    chatAnalysis: 'chat_analysis' as const,
    erpPostgresSync: 'erp_postgres_sync' as const,
  };
  return _getTaskEmails(map[taskName]);
}
