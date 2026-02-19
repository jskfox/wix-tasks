import express from 'express';
import { config } from '../config';
import { logger } from '../utils/logger';
import { getLogBuffer } from '../utils/logger';
import {
  getTaskStates,
  getTaskState,
  triggerTask,
  updateCron,
  setTaskEnabled,
} from '../scheduler';
import {
  getAllSettings,
  getCategories,
  getSetting,
  setSetting,
  deleteSetting,
  Setting,
} from '../services/settings-db';

const CTX = 'AdminServer';
const ADMIN_PORT = parseInt(process.env.ADMIN_PORT || '3800', 10);
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

// ── Sensitive keys to redact from config display ─────────────────────────────

const SENSITIVE_KEYS = new Set([
  'password', 'pass', 'apiKey', 'api_key', 'secret', 'token',
]);

function isSensitive(key: string): boolean {
  const lower = key.toLowerCase();
  return SENSITIVE_KEYS.has(lower) || lower.includes('password') || lower.includes('secret') || lower.includes('token') || lower.includes('apikey');
}

interface ConfigEntry {
  key: string;
  value: string;
  sensitive: boolean;
  editable: boolean;
}

// ── Non-sensitive config keys that can be edited at runtime ───────────────────

const EDITABLE_KEYS = new Set([
  'sucursalWix', 'mssql.empId', 'odoo.livechatChannelId', 'odoo.reportsDir',
  'timezone', 'logLevel', 'smtp.from', 'smtp.port', 'smtp.secure',
  'pg.port', 'pg.database', 'mssql.port', 'mssql.database',
  'mssql.encrypt', 'mssql.trustServerCertificate',
]);

function flattenConfig(obj: Record<string, unknown>, prefix = ''): ConfigEntry[] {
  const result: ConfigEntry[] = [];
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      result.push(...flattenConfig(value as Record<string, unknown>, fullKey));
    } else {
      const sensitive = isSensitive(key);
      result.push({
        key: fullKey,
        value: sensitive ? '••••••••' : String(value),
        sensitive,
        editable: EDITABLE_KEYS.has(fullKey) && !sensitive,
      });
    }
  }
  return result;
}

function setNestedValue(obj: Record<string, any>, path: string, value: any): void {
  const parts = path.split('.');
  let current = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    if (current[parts[i]] && typeof current[parts[i]] === 'object') {
      current = current[parts[i]];
    } else return;
  }
  const lastKey = parts[parts.length - 1];
  const existing = current[lastKey];
  // Coerce to same type
  if (typeof existing === 'number') current[lastKey] = Number(value);
  else if (typeof existing === 'boolean') current[lastKey] = value === 'true' || value === true;
  else current[lastKey] = String(value);
}

// ── Auth: simple token-based session ─────────────────────────────────────────

const activeSessions = new Set<string>();

function generateToken(): string {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let token = '';
  for (let i = 0; i < 48; i++) token += chars[Math.floor(Math.random() * chars.length)];
  return token;
}

// ── Express app ──────────────────────────────────────────────────────────────

export function startAdminServer(): void {
  if (!ADMIN_PASSWORD) {
    logger.warn(CTX, 'ADMIN_PASSWORD not set — admin dashboard is DISABLED for security');
    return;
  }

  const app = express();
  app.use(express.json());

  // ── Auth middleware (skip login endpoint and static assets) ─────────────
  function authMiddleware(req: express.Request, res: express.Response, next: express.NextFunction): void {
    if (req.path === '/api/login' || req.path === '/') {
      next();
      return;
    }
    const token = req.headers['x-auth-token'] as string;
    if (!token || !activeSessions.has(token)) {
      res.status(401).json({ error: 'No autorizado' });
      return;
    }
    next();
  }
  app.use(authMiddleware);

  // ── API: Login ───────────────────────────────────────────────────────────
  app.post('/api/login', (req, res) => {
    const { user, password } = req.body;
    if (user === ADMIN_USER && password === ADMIN_PASSWORD) {
      const token = generateToken();
      activeSessions.add(token);
      logger.info(CTX, `Admin login successful from ${req.ip}`);
      res.json({ token });
    } else {
      logger.warn(CTX, `Failed admin login attempt from ${req.ip}`);
      res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
    }
  });

  // ── API: Logout ──────────────────────────────────────────────────────────
  app.post('/api/logout', (req, res) => {
    const token = req.headers['x-auth-token'] as string;
    activeSessions.delete(token);
    res.json({ ok: true });
  });

  // ── API: List all tasks ──────────────────────────────────────────────────
  app.get('/api/tasks', (_req, res) => {
    res.json(getTaskStates());
  });

  // ── API: Get single task ─────────────────────────────────────────────────
  app.get('/api/tasks/:name', (req, res) => {
    const state = getTaskState(req.params.name);
    if (!state) return res.status(404).json({ error: 'Task not found' });
    res.json(state);
  });

  // ── API: Trigger manual execution ────────────────────────────────────────
  app.post('/api/tasks/:name/run', (req, res) => {
    const state = getTaskState(req.params.name);
    if (!state) return res.status(404).json({ error: 'Task not found' });
    if (state.running) return res.status(409).json({ error: 'Task is already running' });

    res.json({ message: `Task "${req.params.name}" triggered`, running: true });
    triggerTask(req.params.name).catch(() => {/* errors tracked in history */});
  });

  // ── API: Update cron expression ──────────────────────────────────────────
  app.put('/api/tasks/:name/cron', (req, res) => {
    const { cron: newCron } = req.body;
    if (!newCron || typeof newCron !== 'string') {
      return res.status(400).json({ error: 'Missing "cron" in request body' });
    }
    try {
      updateCron(req.params.name, newCron);
      res.json({ message: `Cron updated to "${newCron}"`, cronExpression: newCron });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      res.status(400).json({ error: msg });
    }
  });

  // ── API: Enable/disable task ─────────────────────────────────────────────
  app.put('/api/tasks/:name/enabled', (req, res) => {
    const { enabled } = req.body;
    if (typeof enabled !== 'boolean') {
      return res.status(400).json({ error: 'Missing "enabled" (boolean) in request body' });
    }
    try {
      setTaskEnabled(req.params.name, enabled);
      res.json({ message: `Task "${req.params.name}" ${enabled ? 'enabled' : 'disabled'}`, enabled });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      res.status(400).json({ error: msg });
    }
  });

  // ── API: Logs ────────────────────────────────────────────────────────────
  app.get('/api/logs', (req, res) => {
    const level = req.query.level as string | undefined;
    const context = req.query.context as string | undefined;
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 200;
    res.json(getLogBuffer({ level: level as any, context, limit }));
  });

  // ── API: Settings (SQLite-backed) ──────────────────────────────────────
  app.get('/api/settings', (req, res) => {
    const category = req.query.category as string | undefined;
    res.json(getAllSettings(category || undefined));
  });

  app.get('/api/settings/categories', (_req, res) => {
    res.json(getCategories());
  });

  app.put('/api/settings', (req, res) => {
    const { key, value } = req.body;
    if (!key || value === undefined) {
      return res.status(400).json({ error: 'Missing "key" and "value"' });
    }
    try {
      setSetting(key, String(value));
      logger.info(CTX, `Setting updated: ${key} = ${value}`);
      res.json({ message: `Setting "${key}" updated`, key, value: String(value) });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      res.status(400).json({ error: msg });
    }
  });

  app.delete('/api/settings', (req, res) => {
    const key = req.query.key as string;
    if (!key) {
      return res.status(400).json({ error: 'Missing "key" query param' });
    }
    const deleted = deleteSetting(key);
    if (deleted) {
      logger.info(CTX, `Setting deleted: ${key}`);
      res.json({ message: `Setting "${key}" deleted` });
    } else {
      res.status(404).json({ error: `Setting "${key}" not found` });
    }
  });

  // ── API: Config (legacy — read-only view of merged config) ────────────
  app.get('/api/config', (_req, res) => {
    res.json(flattenConfig(config as unknown as Record<string, unknown>));
  });

  // ── API: Required env vars (informational — no values exposed) ────────
  app.get('/api/env-vars', (_req, res) => {
    const REQUIRED_ENV_VARS = [
      // PostgreSQL
      { name: 'PG_HOST',     example: '192.168.1.10',      description: 'Host del servidor PostgreSQL',          group: 'PostgreSQL' },
      { name: 'PG_USER',     example: 'postgres',           description: 'Usuario de PostgreSQL',                 group: 'PostgreSQL' },
      { name: 'PG_PASSWORD', example: '••••••••',           description: 'Contraseña de PostgreSQL',              group: 'PostgreSQL' },
      // MSSQL
      { name: 'MSSQL_SERVER', example: '192.168.1.20',     description: 'Host del servidor MSSQL (SQL Server)',   group: 'MSSQL' },
      { name: 'MSSQL_USER',   example: 'sa',               description: 'Usuario de MSSQL',                      group: 'MSSQL' },
      { name: 'MSSQL_PASSWORD', example: '••••••••',       description: 'Contraseña de MSSQL',                   group: 'MSSQL' },
      // Wix
      { name: 'WIX_SITE_ID', example: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx', description: 'ID del sitio Wix (Settings → Advanced)', group: 'Wix' },
      { name: 'WIX_API_KEY', example: 'IST.eyJ...',        description: 'API Key de Wix (Headless Settings)',     group: 'Wix' },
      // SMTP
      { name: 'SMTP_HOST',   example: 'smtp.gmail.com',    description: 'Host del servidor SMTP',                group: 'SMTP' },
      { name: 'SMTP_USER',   example: 'user@dominio.com',  description: 'Usuario/email de autenticación SMTP',   group: 'SMTP' },
      { name: 'SMTP_PASS',   example: '••••••••',          description: 'Contraseña de autenticación SMTP',      group: 'SMTP' },
      // Odoo
      { name: 'ODOO_URL',      example: 'https://odoo.empresa.com', description: 'URL base de la instancia Odoo', group: 'Odoo' },
      { name: 'ODOO_DB',       example: 'proconsa',               description: 'Nombre de la base de datos Odoo', group: 'Odoo' },
      { name: 'ODOO_USERNAME', example: 'admin',                  description: 'Usuario de Odoo (login)',         group: 'Odoo' },
      { name: 'ODOO_PASSWORD', example: '••••••••',               description: 'Contraseña del usuario Odoo',    group: 'Odoo' },
      // Admin
      { name: 'ADMIN_PORT',     example: '3800',     description: 'Puerto del dashboard de administración (no es credencial, pero se lee antes de que SQLite esté disponible)', group: 'Admin' },
      { name: 'ADMIN_USER',     example: 'admin',    description: 'Usuario del dashboard',                                          group: 'Admin' },
      { name: 'ADMIN_PASSWORD', example: '••••••••', description: 'Contraseña del dashboard (requerida para activarlo)',             group: 'Admin' },
      // Sistema
      { name: 'STATE_DIR', example: './state', description: 'Ruta donde se almacena settings.db. Debe configurarse en .env porque se lee ANTES de que la base de datos esté disponible', group: 'Sistema' },
    ];

    res.json(REQUIRED_ENV_VARS.map(v => ({
      ...v,
      present: !!process.env[v.name],
    })));
  });

  // ── API: System info ─────────────────────────────────────────────────────
  app.get('/api/system', (_req, res) => {
    res.json({
      nodeVersion: process.version,
      platform: process.platform,
      arch: process.arch,
      uptime: process.uptime(),
      memoryUsage: process.memoryUsage(),
      pid: process.pid,
      cwd: process.cwd(),
      timezone: config.timezone,
    });
  });

  // ── Dashboard HTML ───────────────────────────────────────────────────────
  app.get('/', (_req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.send(getDashboardHtml());
  });

  app.listen(ADMIN_PORT, () => {
    logger.info(CTX, `Admin dashboard running at http://localhost:${ADMIN_PORT}`);
  });
}

// ── Dashboard HTML (self-contained SPA) ──────────────────────────────────────

function getDashboardHtml(): string {
  return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Proconsa Task Admin</title>
<style>
:root{--ink-primary:#e2e2e8;--ink-secondary:#9d9daa;--ink-tertiary:#6b6b78;--ink-muted:#4a4a56;--surface-base:#0f0f14;--surface-1:#16161d;--surface-2:#1c1c25;--surface-3:#24242f;--border-soft:rgba(255,255,255,0.06);--border-std:rgba(255,255,255,0.10);--border-emphasis:rgba(255,255,255,0.16);--accent:#6c8cff;--accent-dim:rgba(108,140,255,0.12);--success:#3dd68c;--success-dim:rgba(61,214,140,0.12);--error:#f06;--error-dim:rgba(255,0,102,0.12);--warn:#f5a623;--warn-dim:rgba(245,166,35,0.12);--radius-sm:6px;--radius-md:10px;--radius-lg:14px;--space-xs:4px;--space-sm:8px;--space-md:16px;--space-lg:24px;--space-xl:32px;--font-mono:'SF Mono','Cascadia Code','JetBrains Mono','Fira Code',monospace;--font-sans:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:var(--font-sans);background:var(--surface-base);color:var(--ink-primary);font-size:14px;line-height:1.5;min-height:100vh}
.shell{display:flex;min-height:100vh}
.sidebar{width:220px;border-right:1px solid var(--border-soft);padding:var(--space-lg) 0;display:flex;flex-direction:column;flex-shrink:0}
.sidebar-brand{padding:0 var(--space-lg);margin-bottom:var(--space-xl)}
.sidebar-brand h1{font-size:15px;font-weight:700;letter-spacing:-0.3px}
.sidebar-brand span{font-size:11px;color:var(--ink-tertiary);font-weight:500;letter-spacing:0.5px;text-transform:uppercase}
.nav-item{display:flex;align-items:center;gap:var(--space-sm);padding:var(--space-sm) var(--space-lg);color:var(--ink-secondary);cursor:pointer;font-size:13px;font-weight:500;transition:all .15s;border-left:2px solid transparent}
.nav-item:hover{color:var(--ink-primary);background:var(--surface-1)}
.nav-item.active{color:var(--accent);background:var(--accent-dim);border-left-color:var(--accent)}
.nav-item svg{width:16px;height:16px;flex-shrink:0}
.sidebar-footer{margin-top:auto;padding:var(--space-md) var(--space-lg);border-top:1px solid var(--border-soft)}
.main{flex:1;padding:var(--space-xl);overflow-y:auto;max-height:100vh}
.page-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:var(--space-lg)}
.page-header h2{font-size:20px;font-weight:700;letter-spacing:-0.4px}
.card{background:var(--surface-1);border:1px solid var(--border-soft);border-radius:var(--radius-md);padding:var(--space-lg);margin-bottom:var(--space-md)}
.task-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:var(--space-md)}
.task-card{background:var(--surface-1);border:1px solid var(--border-soft);border-radius:var(--radius-md);padding:var(--space-lg);transition:all .15s}
.task-card:hover{border-color:var(--border-emphasis)}
.task-card.disabled{opacity:0.6;background:var(--surface-2);border-color:var(--border-soft)}
.task-card.disabled:hover{border-color:var(--border-std)}
.task-header{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:var(--space-sm)}
.task-name{font-size:14px;font-weight:600;font-family:var(--font-mono);letter-spacing:-0.3px}
.task-desc{font-size:12px;color:var(--ink-tertiary);margin-bottom:var(--space-md);line-height:1.4}
.task-meta{display:flex;flex-direction:column;gap:var(--space-xs)}
.task-meta-row{display:flex;align-items:center;gap:var(--space-sm);font-size:12px;color:var(--ink-secondary)}
.task-meta-row code{font-family:var(--font-mono);font-size:12px;background:var(--surface-3);padding:2px 6px;border-radius:4px;color:var(--ink-primary)}
.task-meta-row .next-run{color:var(--accent)}
.task-meta-row .last-run-ok{color:var(--success)}
.task-meta-row .last-run-err{color:var(--error)}
.task-actions{display:flex;gap:var(--space-sm);margin-top:var(--space-md);flex-wrap:wrap}
.badge{display:inline-flex;align-items:center;gap:4px;font-size:11px;font-weight:600;padding:3px 8px;border-radius:20px;text-transform:uppercase;letter-spacing:0.5px;flex-shrink:0}
.badge-success{background:var(--success-dim);color:var(--success)}
.badge-error{background:var(--error-dim);color:var(--error)}
.badge-running{background:var(--accent-dim);color:var(--accent);animation:pulse 1.5s infinite}
.badge-disabled{background:var(--surface-3);color:var(--ink-muted)}
.badge-idle{background:var(--surface-3);color:var(--ink-tertiary)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
.btn{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border:1px solid var(--border-std);border-radius:var(--radius-sm);background:var(--surface-2);color:var(--ink-primary);font-size:12px;font-weight:500;cursor:pointer;transition:all .15s;font-family:var(--font-sans)}
.btn:hover{background:var(--surface-3);border-color:var(--border-emphasis)}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn-primary{background:var(--accent);color:#fff;border-color:var(--accent)}
.btn-primary:hover{background:#5a7aee}
.btn-danger{color:var(--error);border-color:rgba(255,0,102,0.3)}
.btn-danger:hover{background:var(--error-dim)}
.btn-accent{background:var(--accent-dim);color:var(--accent);border-color:var(--accent)}
.btn-sm{padding:4px 10px;font-size:11px}
.log-controls{display:flex;gap:var(--space-sm);margin-bottom:var(--space-md);flex-wrap:wrap;align-items:center}
.log-controls select,.log-controls input{background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);color:var(--ink-primary);padding:6px 10px;font-size:12px;font-family:var(--font-sans)}
.log-controls input{flex:1;min-width:200px}
.log-table{width:100%;border-collapse:collapse;font-family:var(--font-mono);font-size:12px}
.log-table th{text-align:left;padding:var(--space-sm);border-bottom:1px solid var(--border-std);color:var(--ink-tertiary);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;position:sticky;top:0;background:var(--surface-1)}
.log-table td{padding:4px var(--space-sm);border-bottom:1px solid var(--border-soft);vertical-align:top;white-space:nowrap}
.log-table td:last-child{white-space:normal;word-break:break-word;max-width:600px}
.log-table tr:hover{background:var(--surface-2)}
.log-level-debug{color:var(--ink-muted)}.log-level-info{color:var(--ink-secondary)}.log-level-warn{color:var(--warn)}.log-level-error{color:var(--error)}
.log-ts{color:var(--ink-muted);font-size:11px}.log-ctx{color:var(--accent)}
.config-table{width:100%;border-collapse:collapse;font-size:13px}
.config-table th{text-align:left;padding:var(--space-sm);border-bottom:1px solid var(--border-std);color:var(--ink-tertiary);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px}
.config-table td{padding:var(--space-sm);border-bottom:1px solid var(--border-soft);font-family:var(--font-mono);font-size:12px}
.config-table td:first-child{color:var(--accent)}
.config-table tr:hover{background:var(--surface-2)}
.redacted{color:var(--ink-muted);font-style:italic}
.config-edit-input{background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);color:var(--ink-primary);padding:4px 8px;font-size:12px;font-family:var(--font-mono);width:200px}
.config-edit-input:focus{outline:none;border-color:var(--accent)}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:var(--space-md)}
.stat-card{background:var(--surface-1);border:1px solid var(--border-soft);border-radius:var(--radius-md);padding:var(--space-lg);transition:border-color .15s}
.stat-card:hover{border-color:var(--border-emphasis)}
.stat-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--ink-muted);margin-bottom:var(--space-sm)}
.stat-value{font-size:16px;font-weight:600;font-family:var(--font-mono);letter-spacing:-0.3px;font-variant-numeric:tabular-nums;color:var(--ink-primary);line-height:1.4}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:100;backdrop-filter:blur(4px)}
.modal-overlay.hidden{display:none}
.modal{background:var(--surface-1);border:1px solid var(--border-std);border-radius:var(--radius-lg);padding:var(--space-xl);min-width:380px;max-width:560px}
.modal h3{font-size:16px;font-weight:700;margin-bottom:var(--space-md)}
.modal label{display:block;font-size:12px;font-weight:600;color:var(--ink-secondary);margin-bottom:var(--space-xs)}
.modal input[type="text"],.modal input[type="password"]{width:100%;background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);color:var(--ink-primary);padding:8px 12px;font-size:14px;font-family:var(--font-mono);margin-bottom:var(--space-md)}
.modal input:focus{outline:none;border-color:var(--accent)}
.modal-actions{display:flex;gap:var(--space-sm);justify-content:flex-end}
.history-table{width:100%;border-collapse:collapse;font-size:12px;font-family:var(--font-mono)}
.history-table th{text-align:left;padding:var(--space-sm);border-bottom:1px solid var(--border-std);color:var(--ink-tertiary);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px}
.history-table td{padding:4px var(--space-sm);border-bottom:1px solid var(--border-soft)}
.history-table tr:hover{background:var(--surface-2)}
.toast-container{position:fixed;bottom:var(--space-lg);right:var(--space-lg);z-index:200;display:flex;flex-direction:column;gap:var(--space-sm)}
.toast{background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);padding:var(--space-sm) var(--space-md);font-size:13px;animation:slideIn .2s ease-out;max-width:360px}
.toast-success{border-color:var(--success)}.toast-error{border-color:var(--error)}
@keyframes slideIn{from{transform:translateY(10px);opacity:0}to{transform:translateY(0);opacity:1}}
::-webkit-scrollbar{width:6px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border-std);border-radius:3px}::-webkit-scrollbar-thumb:hover{background:var(--border-emphasis)}
.refresh-indicator{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:var(--ink-muted)}
.refresh-dot{width:6px;height:6px;border-radius:50%;background:var(--success);animation:blink 3s infinite}
@keyframes blink{0%,90%,100%{opacity:1}95%{opacity:.3}}
/* Login */
.login-screen{display:flex;align-items:center;justify-content:center;min-height:100vh}
.login-box{background:var(--surface-1);border:1px solid var(--border-std);border-radius:var(--radius-lg);padding:var(--space-xl);width:360px}
.login-box h2{font-size:18px;font-weight:700;margin-bottom:4px}
.login-box .sub{font-size:12px;color:var(--ink-tertiary);margin-bottom:var(--space-lg)}
.login-box label{display:block;font-size:12px;font-weight:600;color:var(--ink-secondary);margin-bottom:var(--space-xs)}
.login-box input{width:100%;background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);color:var(--ink-primary);padding:8px 12px;font-size:14px;margin-bottom:var(--space-md)}
.login-box input:focus{outline:none;border-color:var(--accent)}
.login-error{color:var(--error);font-size:12px;margin-bottom:var(--space-md);display:none}
/* Cron builder */
.cron-builder{display:grid;grid-template-columns:1fr 1fr;gap:var(--space-sm);margin-bottom:var(--space-md)}
.cron-builder select{background:var(--surface-2);border:1px solid var(--border-std);border-radius:var(--radius-sm);color:var(--ink-primary);padding:6px 8px;font-size:12px;font-family:var(--font-sans)}
.cron-builder select:focus{outline:none;border-color:var(--accent)}
.cron-builder label{font-size:11px;color:var(--ink-tertiary);margin-bottom:2px;display:block}
.cron-builder .field{display:flex;flex-direction:column}
.cron-preview{background:var(--surface-3);border-radius:var(--radius-sm);padding:var(--space-sm) var(--space-md);font-family:var(--font-mono);font-size:13px;margin-bottom:var(--space-sm);display:flex;justify-content:space-between;align-items:center}
.cron-preview code{color:var(--accent);font-size:14px}
.cron-preview .human{color:var(--ink-secondary);font-size:12px;font-family:var(--font-sans)}
.cron-tabs{display:flex;gap:2px;margin-bottom:var(--space-md)}
.cron-tab{padding:6px 14px;border-radius:var(--radius-sm);font-size:12px;font-weight:500;cursor:pointer;background:var(--surface-2);color:var(--ink-secondary);border:1px solid var(--border-soft);transition:all .15s}
.cron-tab:hover{color:var(--ink-primary)}
.cron-tab.active{background:var(--accent-dim);color:var(--accent);border-color:var(--accent)}
</style>
</head>
<body>

<!-- Login screen -->
<div id="login-screen" class="login-screen">
  <div class="login-box">
    <h2>Proconsa Task Admin</h2>
    <div class="sub">Ingresa tus credenciales para continuar</div>
    <div id="login-error" class="login-error"></div>
    <label>Usuario</label>
    <input type="text" id="login-user" placeholder="admin" autocomplete="username">
    <label>Contrase&ntilde;a</label>
    <input type="password" id="login-pass" placeholder="••••••••" autocomplete="current-password">
    <button class="btn btn-primary" style="width:100%;justify-content:center;padding:10px" onclick="doLogin()">Iniciar Sesi&oacute;n</button>
  </div>
</div>

<!-- App shell (hidden until login) -->
<div id="app-shell" class="shell" style="display:none">
  <nav class="sidebar">
    <div class="sidebar-brand"><h1>Proconsa</h1><span>Task Admin</span></div>
    <div class="nav-item active" data-page="tasks" onclick="showPage('tasks')">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
      Tareas
    </div>
    <div class="nav-item" data-page="logs" onclick="showPage('logs')">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14,2 14,8 20,8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
      Logs
    </div>
    <div class="nav-item" data-page="config" onclick="showPage('config')">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
      Configuraci&oacute;n
    </div>
    <div class="nav-item" data-page="system" onclick="showPage('system')">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>
      Sistema
    </div>
    <div class="sidebar-footer">
      <button class="btn btn-sm btn-danger" style="width:100%;justify-content:center" onclick="doLogout()">Cerrar Sesi&oacute;n</button>
    </div>
  </nav>
  <div class="main">
    <div id="page-tasks" class="page">
      <div class="page-header"><h2>Tareas Programadas</h2><div class="refresh-indicator"><div class="refresh-dot"></div> Auto-refresh 5s</div></div>
      <div id="task-grid" class="task-grid"></div>
    </div>
    <div id="page-logs" class="page" style="display:none">
      <div class="page-header"><h2>Logs del Sistema</h2><div style="display:flex;gap:var(--space-sm)"><button class="btn btn-sm" onclick="loadLogs()">Actualizar</button><button class="btn btn-sm" id="logs-live-btn" onclick="toggleLiveLogs()">Logs Live: OFF</button></div></div>
      <div class="log-controls">
        <select id="log-level" onchange="loadLogs()"><option value="">Todos</option><option value="debug">Debug</option><option value="info">Info</option><option value="warn">Warn</option><option value="error">Error</option></select>
        <input type="text" id="log-context" placeholder="Filtrar por contexto..." oninput="loadLogs()">
        <select id="log-limit" onchange="loadLogs()"><option value="100">100</option><option value="200" selected>200</option><option value="500">500</option><option value="1000">1000</option></select>
      </div>
      <div class="card" id="log-container" style="padding:0;overflow:auto;max-height:calc(100vh - 200px)">
        <table class="log-table"><thead><tr><th>Hora</th><th>Nivel</th><th>Contexto</th><th>Mensaje</th></tr></thead><tbody id="log-body"></tbody></table>
      </div>
    </div>
    <div id="page-config" class="page" style="display:none">
      <div class="page-header"><h2>Configuraci&oacute;n</h2><button class="btn btn-sm" onclick="loadConfig()">Actualizar</button></div>
      <p style="font-size:12px;color:var(--ink-tertiary);margin-bottom:var(--space-md)">Configuraci&oacute;n almacenada en SQLite. Los cambios aplican inmediatamente y persisten entre reinicios. Las credenciales sensibles se mantienen en variables de entorno (.env).</p>

      <!-- ENV VARS INFO SECTION -->
      <div class="card" style="margin-bottom:var(--space-md);border-left:3px solid var(--warn)">
        <div style="display:flex;align-items:center;gap:var(--space-sm);margin-bottom:var(--space-md);cursor:pointer" onclick="toggleEnvVars()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:16px;height:16px;color:var(--warn);flex-shrink:0"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
          <span style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--warn);background:rgba(234,179,8,0.1);padding:2px 8px;border-radius:4px">Variables de Entorno (.env)</span>
          <span style="font-size:11px;color:var(--ink-tertiary)">Credenciales sensibles &mdash; no se almacenan en base de datos</span>
          <svg id="env-vars-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width:14px;height:14px;color:var(--ink-tertiary);margin-left:auto;transition:transform 0.2s"><polyline points="6,9 12,15 18,9"/></svg>
        </div>
        <div id="env-vars-body" style="display:none">
          <p style="font-size:11px;color:var(--ink-tertiary);margin-bottom:var(--space-md)">Estas variables deben configurarse en el archivo <code style="background:var(--surface-2);padding:1px 5px;border-radius:3px">.env</code> del servidor. No son editables desde el dashboard.</p>
          <table class="config-table" style="width:100%">
            <thead><tr><th style="width:28%">Variable</th><th style="width:20%">Ejemplo / Formato</th><th>Descripci&oacute;n</th><th style="width:12%">Estado</th></tr></thead>
            <tbody id="env-vars-table"></tbody>
          </table>
        </div>
      </div>

      <div style="display:flex;gap:var(--space-sm);margin-bottom:var(--space-md);flex-wrap:wrap" id="settings-category-tabs"></div>
      <div id="settings-container"></div>
    </div>
    <div id="page-system" class="page" style="display:none">
      <div class="page-header"><h2>Sistema</h2><div class="refresh-indicator"><div class="refresh-dot"></div> Auto-refresh 5s</div></div>
      <div id="system-stats" class="stats-grid"></div>
    </div>
  </div>
</div>

<!-- Cron edit modal -->
<div id="cron-modal" class="modal-overlay hidden">
  <div class="modal" style="min-width:480px">
    <h3>Programar Tarea</h3>
    <label>Tarea: <span id="cron-modal-task" style="color:var(--accent);font-family:var(--font-mono)"></span></label>
    <div style="margin-top:var(--space-md)">
      <div class="cron-tabs">
        <div class="cron-tab active" onclick="setCronMode('preset')">Presets</div>
        <div class="cron-tab" onclick="setCronMode('custom')">Personalizado</div>
        <div class="cron-tab" onclick="setCronMode('advanced')">Avanzado</div>
      </div>
      <!-- Preset mode -->
      <div id="cron-preset" class="cron-builder" style="grid-template-columns:1fr">
        <div class="field">
          <label>Frecuencia</label>
          <select id="cron-frequency" onchange="updateCronFromUI()">
            <option value="everyMinute">Cada minuto</option>
            <option value="every5">Cada 5 minutos</option>
            <option value="every10">Cada 10 minutos</option>
            <option value="every15">Cada 15 minutos</option>
            <option value="every30">Cada 30 minutos</option>
            <option value="hourly" selected>Cada hora</option>
            <option value="every2h">Cada 2 horas</option>
            <option value="every4h">Cada 4 horas</option>
            <option value="every6h">Cada 6 horas</option>
            <option value="every12h">Cada 12 horas</option>
            <option value="daily">Diario</option>
            <option value="weekly">Semanal (Lunes)</option>
          </select>
        </div>
        <div class="field" id="preset-at-wrap" style="display:none">
          <label>A las (hora)</label>
          <select id="preset-at-hour" onchange="updateCronFromUI()"></select>
        </div>
        <div class="field" id="preset-at-min-wrap">
          <label>En el minuto</label>
          <select id="preset-at-min" onchange="updateCronFromUI()"></select>
        </div>
      </div>
      <!-- Custom mode -->
      <div id="cron-custom" class="cron-builder" style="display:none">
        <div class="field"><label>Minuto (0-59)</label><select id="cc-min" onchange="updateCronFromUI()"></select></div>
        <div class="field"><label>Hora (0-23)</label><select id="cc-hour" onchange="updateCronFromUI()"></select></div>
        <div class="field"><label>D&iacute;a del mes</label><select id="cc-dom" onchange="updateCronFromUI()"></select></div>
        <div class="field"><label>Mes</label><select id="cc-month" onchange="updateCronFromUI()"></select></div>
        <div class="field"><label>D&iacute;a de la semana</label><select id="cc-dow" onchange="updateCronFromUI()"></select></div>
      </div>
      <!-- Advanced mode -->
      <div id="cron-advanced" style="display:none">
        <label>Expresi&oacute;n Cron</label>
        <input type="text" id="cron-raw-input" placeholder="*/15 * * * *" oninput="updateCronPreview()">
      </div>
      <div class="cron-preview"><div><span class="human" id="cron-human"></span></div><code id="cron-result"></code></div>
    </div>
    <div class="modal-actions">
      <button class="btn" onclick="closeCronModal()">Cancelar</button>
      <button class="btn btn-primary" onclick="saveCron()">Guardar</button>
    </div>
  </div>
</div>

<!-- History modal -->
<div id="history-modal" class="modal-overlay hidden">
  <div class="modal" style="min-width:600px;max-width:700px;max-height:80vh;overflow:auto">
    <h3>Historial de Ejecuciones</h3>
    <p style="font-size:12px;color:var(--ink-tertiary);margin-bottom:var(--space-md)" id="history-modal-task"></p>
    <table class="history-table"><thead><tr><th>Inicio</th><th>Duraci&oacute;n</th><th>Estado</th><th>Error</th></tr></thead><tbody id="history-body"></tbody></table>
    <div class="modal-actions" style="margin-top:var(--space-md)"><button class="btn" onclick="closeHistoryModal()">Cerrar</button></div>
  </div>
</div>

<div class="toast-container" id="toasts"></div>

<script>
// ── Auth state ──────────────────────────────────────────────────────────────
let authToken = sessionStorage.getItem('adminToken') || '';
let currentPage = 'tasks';
let cronEditTask = '';
let cronMode = 'preset';
let refreshInterval = null;
let logsLive = false;
let logsLiveInterval = null;

// ── API helper with auth ────────────────────────────────────────────────────
async function api(path, opts = {}) {
  if (!opts.headers) opts.headers = {};
  if (authToken) opts.headers['x-auth-token'] = authToken;
  const res = await fetch('/api' + path, opts);
  if (res.status === 401 && path !== '/login') { showLoginScreen(); throw new Error('Session expired'); }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

// ── Login / Logout ──────────────────────────────────────────────────────────
function showLoginScreen() {
  authToken = '';
  sessionStorage.removeItem('adminToken');
  document.getElementById('login-screen').style.display = '';
  document.getElementById('app-shell').style.display = 'none';
}
function showApp() {
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('app-shell').style.display = '';
  loadTasks();
  startRefresh();
}
async function doLogin() {
  const user = document.getElementById('login-user').value;
  const password = document.getElementById('login-pass').value;
  const errEl = document.getElementById('login-error');
  try {
    const r = await fetch('/api/login', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({user,password}) });
    const data = await r.json();
    if (!r.ok) { errEl.textContent = data.error || 'Error'; errEl.style.display = ''; return; }
    authToken = data.token;
    sessionStorage.setItem('adminToken', authToken);
    errEl.style.display = 'none';
    showApp();
  } catch(e) { errEl.textContent = e.message; errEl.style.display = ''; }
}
async function doLogout() {
  try { await api('/logout', { method:'POST' }); } catch(e) {}
  showLoginScreen();
}
document.getElementById('login-pass').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });

// ── Toast ───────────────────────────────────────────────────────────────────
function toast(msg, type = 'success') {
  const el = document.createElement('div');
  el.className = 'toast toast-' + type;
  el.textContent = msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => el.remove(), 4000);
}

// ── Navigation ──────────────────────────────────────────────────────────────
function showPage(page) {
  currentPage = page;
  document.querySelectorAll('.page').forEach(p => p.style.display = 'none');
  document.getElementById('page-' + page).style.display = '';
  document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n.dataset.page === page));
  if (page === 'tasks') loadTasks();
  if (page === 'logs') loadLogs();
  if (page === 'logs') startLogsLive();
  if (page !== 'logs') stopLogsLive();
  if (page === 'config') loadConfig();
  if (page === 'system') loadSystem();
}

// ── Server timezone is injected via API ─────────────────────────────────────
let serverTimezone = 'America/Los_Angeles'; // default, will be updated from /api/system

// ── Tasks ───────────────────────────────────────────────────────────────────
async function loadTasks() {
  try {
    const tasks = await api('/tasks');
    document.getElementById('task-grid').innerHTML = tasks.map(t => renderTaskCard(t)).join('');
  } catch (e) { if (e.message !== 'Session expired') console.error(e); }
}
function renderTaskCard(t) {
  const statusBadge = t.running
    ? '<span class="badge badge-running">Ejecutando</span>'
    : !t.enabled
      ? '<span class="badge badge-disabled">Deshabilitada</span>'
      : t.lastRun ? (t.lastRun.status === 'success' ? '<span class="badge badge-success">OK</span>' : '<span class="badge badge-error">Error</span>')
      : '<span class="badge badge-idle">Sin ejecutar</span>';
  const lastInfo = t.lastRun
    ? '<div class="task-meta-row"><span class="last-run-' + (t.lastRun.status === 'success' ? 'ok' : 'err') + '">&#x25CF;</span> &Uacute;ltima: ' + formatTime(t.lastRun.startedAt) + ' (' + formatDuration(t.lastRun.durationMs) + ')</div>'
    : '<div class="task-meta-row" style="color:var(--ink-muted)">Sin ejecuciones a&uacute;n</div>';
  const nextInfo = t.nextRun
    ? '<div class="task-meta-row"><span class="next-run">&#x25B6;</span> Pr&oacute;xima: <span class="next-run">' + formatTime(t.nextRun) + '</span></div>'
    : t.enabled ? '' : '<div class="task-meta-row" style="color:var(--ink-muted)">Deshabilitada</div>';
  return '<div class="task-card' + (!t.enabled ? ' disabled' : '') + '">'
    + '<div class="task-header"><span class="task-name">' + esc(t.name) + '</span>' + statusBadge + '</div>'
    + '<div class="task-desc">' + esc(t.description) + '</div>'
    + '<div class="task-meta">'
    + '<div class="task-meta-row">Cron: <code>' + esc(t.cronExpression) + '</code> &mdash; <span style="color:var(--ink-tertiary)">' + cronToHuman(t.cronExpression) + '</span></div>'
    + lastInfo + nextInfo
    + '</div>'
    + '<div class="task-actions">'
    + '<button class="btn btn-primary btn-sm" onclick="runTask(\\'' + t.name + '\\')" ' + (t.running ? 'disabled' : '') + '>' + (t.running ? 'Ejecutando...' : 'Ejecutar') + '</button>'
    + '<button class="btn btn-sm" onclick="openCronModal(\\'' + t.name + '\\',\\'' + t.cronExpression + '\\')">Programar</button>'
    + '<button class="btn btn-sm" onclick="toggleTask(\\'' + t.name + '\\',' + !t.enabled + ')">' + (t.enabled ? 'Deshabilitar' : 'Habilitar') + '</button>'
    + '<button class="btn btn-sm" onclick="openHistory(\\'' + t.name + '\\')" ' + (t.history.length === 0 ? 'disabled' : '') + '>Historial (' + t.history.length + ')</button>'
    + '</div></div>';
}
async function runTask(name) {
  try { await api('/tasks/' + name + '/run', { method: 'POST' }); toast('Tarea "' + name + '" iniciada'); setTimeout(loadTasks, 500); }
  catch (e) { toast(e.message, 'error'); }
}
async function toggleTask(name, enabled) {
  try {
    await api('/tasks/' + name + '/enabled', { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({enabled}) });
    toast('Tarea "' + name + '" ' + (enabled ? 'habilitada' : 'deshabilitada'));
    loadTasks();
  } catch (e) { toast(e.message, 'error'); }
}

// ── Cron human-readable ─────────────────────────────────────────────────────
function cronToHuman(expr) {
  const p = expr.split(/\\s+/);
  if (p.length !== 5) return expr;
  const [min, hr, dom, mon, dow] = p;
  if (min === '*' && hr === '*') return 'Cada minuto';
  if (min.startsWith('*/')) return 'Cada ' + min.slice(2) + ' minutos';
  if (hr === '*' && dom === '*' && mon === '*' && dow === '*') {
    if (min.includes(',')) return 'Cada hora en minutos ' + min;
    return 'Cada hora en el minuto ' + min;
  }
  if (hr.startsWith('*/')) return 'Cada ' + hr.slice(2) + ' horas en minuto ' + min;
  const days = ['Dom','Lun','Mar','Mi\\u00e9','Jue','Vie','S\\u00e1b'];
  if (dom === '*' && mon === '*' && dow !== '*') return days[parseInt(dow)] + ' a las ' + hr.padStart(2,'0') + ':' + min.padStart(2,'0');
  if (dom === '*' && mon === '*' && dow === '*') return 'Diario a las ' + hr.padStart(2,'0') + ':' + min.padStart(2,'0');
  return expr;
}

// ── Cron modal ──────────────────────────────────────────────────────────────
function initCronSelects() {
  const minSel = document.getElementById('preset-at-min');
  const hrSel = document.getElementById('preset-at-hour');
  const ccMin = document.getElementById('cc-min');
  const ccHour = document.getElementById('cc-hour');
  const ccDom = document.getElementById('cc-dom');
  const ccMonth = document.getElementById('cc-month');
  const ccDow = document.getElementById('cc-dow');
  // Minutes 0-59
  for (let i = 0; i < 60; i++) {
    const o = '<option value="' + i + '">' + String(i).padStart(2,'0') + '</option>';
    minSel.innerHTML += o; ccMin.innerHTML += o;
  }
  // Hours 0-23
  for (let i = 0; i < 24; i++) {
    const o = '<option value="' + i + '">' + String(i).padStart(2,'0') + ':00</option>';
    hrSel.innerHTML += o; ccHour.innerHTML += o;
  }
  // Custom selects with * option
  ccMin.innerHTML = '<option value="*">Cada minuto (*)</option>' + ccMin.innerHTML;
  ccHour.innerHTML = '<option value="*">Cada hora (*)</option>' + ccHour.innerHTML;
  ccDom.innerHTML = '<option value="*">Todos (*)</option>';
  for (let i = 1; i <= 31; i++) ccDom.innerHTML += '<option value="' + i + '">' + i + '</option>';
  ccMonth.innerHTML = '<option value="*">Todos (*)</option>';
  ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'].forEach((m,i) => ccMonth.innerHTML += '<option value="' + (i+1) + '">' + m + '</option>');
  ccDow.innerHTML = '<option value="*">Todos (*)</option>';
  ['Domingo','Lunes','Martes','Mi\\u00e9rcoles','Jueves','Viernes','S\\u00e1bado'].forEach((d,i) => ccDow.innerHTML += '<option value="' + i + '">' + d + '</option>');
}
initCronSelects();

function setCronMode(mode) {
  cronMode = mode;
  document.querySelectorAll('.cron-tab').forEach((t,i) => t.classList.toggle('active', ['preset','custom','advanced'][i] === mode));
  document.getElementById('cron-preset').style.display = mode === 'preset' ? '' : 'none';
  document.getElementById('cron-custom').style.display = mode === 'custom' ? '' : 'none';
  document.getElementById('cron-advanced').style.display = mode === 'advanced' ? '' : 'none';
  updateCronFromUI();
}
function updateCronFromUI() {
  let cron = '';
  if (cronMode === 'preset') {
    const freq = document.getElementById('cron-frequency').value;
    const min = document.getElementById('preset-at-min').value;
    const hr = document.getElementById('preset-at-hour').value;
    const showHr = ['daily','weekly','every2h','every4h','every6h','every12h'].includes(freq);
    document.getElementById('preset-at-wrap').style.display = showHr ? '' : 'none';
    document.getElementById('preset-at-min-wrap').style.display = freq !== 'everyMinute' ? '' : 'none';
    const presets = {
      everyMinute: '* * * * *', every5: '*/' + 5 + ' * * * *', every10: '*/10 * * * *',
      every15: '*/15 * * * *', every30: '*/30 * * * *',
      hourly: min + ' * * * *', every2h: min + ' */' + 2 + ' * * *',
      every4h: min + ' */' + 4 + ' * * *', every6h: min + ' */' + 6 + ' * * *',
      every12h: min + ' */' + 12 + ' * * *',
      daily: min + ' ' + hr + ' * * *', weekly: min + ' ' + hr + ' * * 1'
    };
    cron = presets[freq] || '0 * * * *';
  } else if (cronMode === 'custom') {
    cron = [document.getElementById('cc-min').value, document.getElementById('cc-hour').value, document.getElementById('cc-dom').value, document.getElementById('cc-month').value, document.getElementById('cc-dow').value].join(' ');
  } else {
    cron = document.getElementById('cron-raw-input').value.trim();
  }
  document.getElementById('cron-result').textContent = cron;
  document.getElementById('cron-human').textContent = cronToHuman(cron);
}
function updateCronPreview() { updateCronFromUI(); }

function openCronModal(name, current) {
  cronEditTask = name;
  document.getElementById('cron-modal-task').textContent = name;
  document.getElementById('cron-raw-input').value = current;
  setCronMode('preset');
  // Try to parse current into preset
  const p = current.split(/\\s+/);
  if (p.length === 5) {
    document.getElementById('cron-raw-input').value = current;
    document.getElementById('cron-result').textContent = current;
    document.getElementById('cron-human').textContent = cronToHuman(current);
  }
  document.getElementById('cron-modal').classList.remove('hidden');
}
function closeCronModal() { document.getElementById('cron-modal').classList.add('hidden'); }
async function saveCron() {
  const newCron = document.getElementById('cron-result').textContent.trim();
  if (!newCron) return;
  try {
    await api('/tasks/' + cronEditTask + '/cron', { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({cron:newCron}) });
    toast('Cron actualizado: ' + newCron);
    closeCronModal();
    loadTasks();
  } catch (e) { toast(e.message, 'error'); }
}

// ── History modal ───────────────────────────────────────────────────────────
async function openHistory(name) {
  try {
    const t = await api('/tasks/' + name);
    document.getElementById('history-modal-task').textContent = name;
    document.getElementById('history-body').innerHTML = t.history.map(h =>
      '<tr><td>' + formatTime(h.startedAt) + '</td><td>' + formatDuration(h.durationMs) + '</td>'
      + '<td><span class="badge badge-' + (h.status==='success'?'success':'error') + '">' + h.status + '</span></td>'
      + '<td style="color:var(--error);max-width:300px;word-break:break-word">' + (h.error||'—') + '</td></tr>'
    ).join('');
    document.getElementById('history-modal').classList.remove('hidden');
  } catch (e) { toast(e.message, 'error'); }
}
function closeHistoryModal() { document.getElementById('history-modal').classList.add('hidden'); }

// ── Logs ────────────────────────────────────────────────────────────────────
async function loadLogs() {
  try {
    const params = new URLSearchParams();
    const lv = document.getElementById('log-level').value;
    const ctx = document.getElementById('log-context').value;
    if (lv) params.set('level', lv);
    if (ctx) params.set('context', ctx);
    params.set('limit', document.getElementById('log-limit').value);
    const logs = await api('/logs?' + params);
    const container = document.getElementById('log-container');
    const shouldStick = logsLive || (container.scrollTop + container.clientHeight >= container.scrollHeight - 40);
    const ordered = logs.slice().reverse();
    document.getElementById('log-body').innerHTML = ordered.map(l =>
      '<tr><td class="log-ts">' + formatTime(l.timestamp) + '</td><td class="log-level-' + l.level + '">' + l.level.toUpperCase() + '</td><td class="log-ctx">' + esc(l.context) + '</td><td>' + esc(l.message) + '</td></tr>'
    ).join('');
    if (shouldStick) container.scrollTop = container.scrollHeight;
  } catch (e) { if (e.message !== 'Session expired') console.error(e); }
}

function toggleLiveLogs() {
  logsLive = !logsLive;
  const btn = document.getElementById('logs-live-btn');
  btn.textContent = 'Logs Live: ' + (logsLive ? 'ON' : 'OFF');
  if (logsLive) {
    loadLogs();
    startLogsLive();
  } else {
    stopLogsLive();
  }
}

function startLogsLive() {
  if (!logsLive) return;
  if (logsLiveInterval) return;
  logsLiveInterval = setInterval(() => {
    if (currentPage === 'logs') loadLogs();
  }, 2000);
}

function stopLogsLive() {
  if (logsLiveInterval) {
    clearInterval(logsLiveInterval);
    logsLiveInterval = null;
  }
}

// ── Env Vars (informational) ──────────────────────────────────────────────────
let envVarsLoaded = false;
function toggleEnvVars() {
  const body = document.getElementById('env-vars-body');
  const chevron = document.getElementById('env-vars-chevron');
  const isHidden = body.style.display === 'none';
  body.style.display = isHidden ? 'block' : 'none';
  chevron.style.transform = isHidden ? 'rotate(180deg)' : '';
  if (isHidden && !envVarsLoaded) loadEnvVars();
}
async function loadEnvVars() {
  try {
    const vars = await api('/env-vars');
    envVarsLoaded = true;
    const groups = {};
    vars.forEach(v => { if (!groups[v.group]) groups[v.group] = []; groups[v.group].push(v); });
    let html = '';
    for (const [group, items] of Object.entries(groups)) {
      html += '<tr><td colspan="4" style="padding:8px 4px 2px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:var(--ink-tertiary);border-bottom:1px solid var(--border)">' + esc(group) + '</td></tr>';
      for (const v of items) {
        const badge = v.present
          ? '<span style="font-size:10px;padding:2px 7px;border-radius:3px;background:rgba(34,197,94,0.15);color:#22c55e;font-weight:600">&#10003; Configurada</span>'
          : '<span style="font-size:10px;padding:2px 7px;border-radius:3px;background:rgba(239,68,68,0.15);color:#ef4444;font-weight:600">&#10007; Falta</span>';
        html += '<tr>'
          + '<td style="font-family:var(--font-mono);font-size:12px;color:var(--accent);padding-left:12px">' + esc(v.name) + '</td>'
          + '<td style="font-family:var(--font-mono);font-size:11px;color:var(--ink-tertiary)">' + esc(v.example) + '</td>'
          + '<td style="font-size:11px;color:var(--ink-secondary)">' + esc(v.description) + '</td>'
          + '<td style="text-align:center">' + badge + '</td>'
          + '</tr>';
      }
    }
    document.getElementById('env-vars-table').innerHTML = html;
  } catch (e) { console.error(e); }
}

// ── Settings (SQLite-backed) ─────────────────────────────────────────────────
// Keys that are informational only — managed externally, not editable from the UI
const READONLY_KEYS = new Set([
  'state_dir',  // Controlled by STATE_DIR env var, not SQLite
  'wix.price_inventory_sync.last_processed_timestamp',  // Watermark managed by task
]);

let settingsCategory = '';
async function loadConfig() {
  try {
    const cats = await api('/settings/categories');
    const tabsEl = document.getElementById('settings-category-tabs');
    tabsEl.innerHTML = '<button class="btn btn-sm' + (!settingsCategory ? ' btn-accent' : '') + '" onclick="settingsCategory=\\'\\';loadConfig()">Todas</button>'
      + cats.map(c => '<button class="btn btn-sm' + (settingsCategory === c ? ' btn-accent' : '') + '" onclick="settingsCategory=\\'' + c + '\\';loadConfig()">' + esc(c) + '</button>').join('');

    const url = settingsCategory ? '/settings?category=' + settingsCategory : '/settings';
    const settings = await api(url);

    // Group by category
    const groups = {};
    settings.forEach(s => { if (!groups[s.category]) groups[s.category] = []; groups[s.category].push(s); });

    let html = '';
    for (const [cat, items] of Object.entries(groups)) {
      html += '<div class="card" style="margin-bottom:var(--space-md)">'
        + '<div style="display:flex;align-items:center;gap:var(--space-sm);margin-bottom:var(--space-md)">'
        + '<span style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--accent);background:var(--accent-dim);padding:2px 8px;border-radius:4px">' + esc(cat) + '</span>'
        + '<span style="font-size:11px;color:var(--ink-tertiary)">' + items.length + ' setting(s)</span>'
        + '</div>'
        + '<table class="config-table" style="width:100%"><thead><tr><th style="width:30%">Clave</th><th style="width:45%">Valor</th><th style="width:15%">Descripci&oacute;n</th><th style="width:10%"></th></tr></thead><tbody>';
      for (const s of items) {
        const readonly = READONLY_KEYS.has(s.key);
        if (readonly) {
          html += '<tr style="opacity:0.7">'
            + '<td style="font-family:var(--font-mono);font-size:12px;color:var(--accent)">' + esc(s.key) + '</td>'
            + '<td style="font-family:var(--font-mono);font-size:12px;color:var(--ink-secondary);padding:6px 8px">' + esc(s.value) + '</td>'
            + '<td style="font-size:11px;color:var(--ink-tertiary)">' + esc(s.description || '') + '</td>'
            + '<td style="text-align:center"><span style="font-size:10px;padding:2px 7px;border-radius:3px;background:var(--surface-2);color:var(--ink-tertiary)">solo lectura</span></td>'
            + '</tr>';
        } else {
          html += '<tr>'
            + '<td style="font-family:var(--font-mono);font-size:12px;color:var(--accent)">' + esc(s.key) + '</td>'
            + '<td><input class="config-edit-input" data-key="' + esc(s.key) + '" value="' + esc(s.value) + '" onkeydown="if(event.key===\\'Enter\\')saveSetting(this)" style="width:100%"></td>'
            + '<td style="font-size:11px;color:var(--ink-tertiary)">' + esc(s.description || '') + '</td>'
            + '<td><button class="btn btn-sm" onclick="saveSetting(this.closest(\\'tr\\').querySelector(\\'input\\'))">Guardar</button></td>'
            + '</tr>';
        }
      }
      html += '</tbody></table></div>';
    }
    document.getElementById('settings-container').innerHTML = html;
  } catch (e) { if (e.message !== 'Session expired') console.error(e); }
}
async function saveSetting(input) {
  const key = input.dataset.key;
  const value = input.value;
  try {
    await api('/settings', { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({key,value}) });
    toast('Setting "' + key + '" guardada');
    input.style.borderColor = 'var(--success)';
    setTimeout(() => { input.style.borderColor = ''; }, 1500);
  } catch (e) { toast(e.message, 'error'); }
}

// ── System ──────────────────────────────────────────────────────────────────
async function loadSystem() {
  try {
    const s = await api('/system');
    // Update global timezone from server
    if (s.timezone) serverTimezone = s.timezone;
    const serverTime = new Date().toLocaleString('es-MX', { 
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false, timeZone: serverTimezone 
    });
    document.getElementById('system-stats').innerHTML =
      stat('HORA DEL SERVIDOR', serverTime)
      + stat('ZONA HORARIA', serverTimezone)
      + stat('NODE.JS', s.nodeVersion)
      + stat('PLATAFORMA', s.platform+'/'+s.arch)
      + stat('UPTIME', formatDuration(s.uptime*1000))
      + stat('PID', s.pid)
      + stat('HEAP USADO', formatBytes(s.memoryUsage.heapUsed))
      + stat('HEAP TOTAL', formatBytes(s.memoryUsage.heapTotal))
      + stat('RSS', formatBytes(s.memoryUsage.rss))
      + stat('DIRECTORIO', '<span style="font-size:11px;word-break:break-all">'+s.cwd+'</span>');
  } catch (e) { if (e.message !== 'Session expired') console.error(e); }
}
function stat(label, value) { return '<div class="stat-card"><div class="stat-label">'+label+'</div><div class="stat-value">'+value+'</div></div>'; }

// ── Formatters ──────────────────────────────────────────────────────────────
function formatTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('es-MX', { hour12: false, timeZone: serverTimezone });
}
function formatDuration(ms) {
  if (ms < 1000) return Math.round(ms) + 'ms';
  if (ms < 60000) return (ms/1000).toFixed(1) + 's';
  const m = Math.floor(ms/60000), s = Math.floor((ms%60000)/1000);
  if (m < 60) return m + 'm ' + s + 's';
  return Math.floor(m/60) + 'h ' + (m%60) + 'm';
}
function formatBytes(b) { return b < 1048576 ? (b/1024).toFixed(1)+' KB' : (b/1048576).toFixed(1)+' MB'; }
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// ── Auto-refresh ────────────────────────────────────────────────────────────
function startRefresh() {
  if (refreshInterval) clearInterval(refreshInterval);
  refreshInterval = setInterval(() => {
    if (currentPage === 'tasks') loadTasks();
    if (currentPage === 'system') loadSystem();
  }, 5000);
}

// ── Init ────────────────────────────────────────────────────────────────────
if (authToken) {
  // Validate existing token
  api('/tasks').then(() => showApp()).catch(() => showLoginScreen());
} else {
  showLoginScreen();
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') { closeCronModal(); closeHistoryModal(); } });
</script>
</body>
</html>`;
}
