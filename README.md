# p-scheduler — Proconsa

Scheduler de tareas programadas para Proconsa. Orquesta sincronizaciones entre ERP (MSSQL), Odoo, PostgreSQL y Wix, además de reportes automáticos por correo. Incluye un dashboard de administración web.

## Tareas

| Tarea | Cron | Descripción |
|---|---|---|
| `erp-postgres-sync` | cada 30 min (6am–9pm) | Extrae precios de MSSQL, carga en PostgreSQL con swap atómico, analiza cambios y envía reporte por email |
| `price-inventory-sync` | `:05` y `:35` de cada hora | Lee precios/stock de PostgreSQL (sucursal 101) y sincroniza con Wix Store |
| `odoo-inventory-sync-full` | 4:00 AM diario | Sincronización completa ERP→Odoo: productos, categorías, precios, stock por sucursal, imágenes y códigos de barras |
| `odoo-inventory-sync-stock` | `:15` de cada hora | Sincronización rápida de existencias ERP→Odoo (solo cantidades por sucursal/bodega) |
| `odoo-price-sync` | `:45` de cada hora | Sincroniza lista de precios de MSSQL hacia Odoo |
| `abandoned-carts-report` | 10:00 PM diario | Consulta carritos abandonados del día en Wix y envía reporte HTML por email |
| `odoo-chat-leads` | 8:00 AM diario | Extrae leads potenciales del livechat de Odoo, prioriza por intención de compra y envía resumen |
| `odoo-chat-analysis` | Lunes 7:00 AM | Analiza conversaciones del livechat de los últimos 7 días: intenciones, productos mencionados, emails capturados |

## Requisitos

- Node.js >= 20
- MSSQL (SQL Server) — ERP fuente de datos
- PostgreSQL — base de datos intermedia de precios
- Odoo — ERP destino para inventario y precios
- Wix Store — tienda online destino
- Servidor SMTP para envío de correos

## Setup

```bash
# 1. Instalar dependencias
npm install

# 2. Configurar credenciales (solo secretos)
cp .env.example .env
# Editar .env con tus credenciales

# 3. Inicializar base de datos de configuración
npm run setup        # aplica defaults
npm run setup:dry    # previsualiza sin escribir

# 4. Desarrollo
npm run dev

# 5. Producción
npm run build
npm start
```

> El resto de la configuración (puertos, timeouts, emails de destinatarios, flags dry-run, etc.) se gestiona desde el **Admin Dashboard** en `http://localhost:3800`.

## Configuración

La configuración está dividida en dos capas:

### 1. Variables de entorno (`.env`) — solo credenciales y bootstrap

| Variable | Descripción |
|---|---|
| `PG_HOST` | Host de PostgreSQL |
| `PG_USER` | Usuario de PostgreSQL |
| `PG_PASSWORD` | Contraseña de PostgreSQL |
| `MSSQL_SERVER` | Host del servidor SQL Server |
| `MSSQL_USER` | Usuario de MSSQL |
| `MSSQL_PASSWORD` | Contraseña de MSSQL |
| `WIX_SITE_ID` | Site ID de Wix |
| `WIX_API_KEY` | API Key de Wix (Headless) |
| `SMTP_HOST` | Host del servidor SMTP |
| `SMTP_USER` | Usuario SMTP |
| `SMTP_PASS` | Contraseña SMTP |
| `ODOO_URL` | URL base de la instancia Odoo |
| `ODOO_DB` | Base de datos Odoo |
| `ODOO_USERNAME` | Usuario Odoo |
| `ODOO_PASSWORD` | Contraseña Odoo |
| `ADMIN_PORT` | Puerto del dashboard (default: `3800`) |
| `ADMIN_USER` | Usuario del dashboard (default: `admin`) |
| `ADMIN_PASSWORD` | Contraseña del dashboard |
| `STATE_DIR` | Directorio para `settings.db` (default: `./state`) |

### 2. SQLite — configuración operacional (Admin Dashboard)

Todo lo no-sensible se almacena en `state/settings.db` y es editable en tiempo real desde el dashboard sin reiniciar el proceso:

- Puertos y nombres de bases de datos (`pg.port`, `mssql.database`, etc.)
- Emails de destinatarios por tarea (`emails.marketing`, `emails.abandoned_carts`, etc.)
- Parámetros de sincronización (`wix.dry_run`, `wix.min_stock_threshold`, `erp_odoo.max_inventory_rows`, etc.)
- Nivel de log, zona horaria, concurrencias, timeouts

## Scripts npm

| Script | Descripción |
|---|---|
| `npm run dev` | Modo desarrollo con ts-node y hot-reload |
| `npm start` | Producción (requiere `npm run build` previo) |
| `npm run build` | Compila TypeScript a `dist/` |
| `npm run setup` | Inicializa `settings.db` con valores de producción |
| `npm run setup:dry` | Previsualiza qué valores aplicaría `setup` |
| `npm run wix:sync` | Ejecuta `price-inventory-sync` una vez (modo LIVE) |
| `npm run wix:test` | Ejecuta `price-inventory-sync` con límite de 5 SKUs |

## Admin Dashboard

Disponible en `http://localhost:ADMIN_PORT` (requiere `ADMIN_PASSWORD` configurado).

- **Tareas** — estado, última ejecución, próxima ejecución, ejecución manual
- **Logs** — stream en tiempo real con filtros por nivel y contexto
- **Configuración** — edición de settings SQLite por categoría; panel informativo de variables de entorno con estado (configurada / falta)
- **Sistema** — métricas de proceso (uptime, memoria, versión Node)

## Agregar una Nueva Tarea

1. Crear `src/tasks/mi-tarea.ts` extendiendo `BaseTask`:

```typescript
import { BaseTask } from './base-task';

export class MiTareaTask extends BaseTask {
  readonly name = 'mi-tarea';
  readonly description = 'Descripción de la tarea';
  readonly cronExpression = '0 */6 * * *'; // cada 6 horas

  async execute(): Promise<void> {
    // lógica aquí
  }
}
```

2. Registrar en `src/index.ts`:

```typescript
import { MiTareaTask } from './tasks/mi-tarea';
registerTask(new MiTareaTask());
```

## Deploy en Coolify (Nixpacks)

1. Crear servicio apuntando a `https://github.com/jskfox/p-scheduler`
2. Nixpacks detecta Node.js automáticamente
3. Build: `npm run build` · Start: `npm start`
4. Configurar variables de entorno (sección `.env`) en Coolify
5. Montar volumen persistente en `./state` para que `settings.db` sobreviva redeploys

## Estructura del Proyecto

```
src/
├── index.ts                         # Entry point y registro de tareas
├── scheduler.ts                     # Motor cron genérico
├── config.ts                        # Configuración tipada (.env + SQLite)
├── admin/
│   └── server.ts                    # Dashboard web (Express)
├── tasks/
│   ├── base-task.ts                 # Clase abstracta BaseTask
│   ├── erp-postgres-sync.ts         # ERP MSSQL → PostgreSQL (precios)
│   ├── price-inventory-sync.ts      # PostgreSQL → Wix (precios/stock)
│   ├── odoo-inventory-sync.ts       # ERP MSSQL → Odoo (inventario completo/stock)
│   ├── odoo-price-sync.ts           # ERP MSSQL → Odoo (precios)
│   ├── abandoned-carts.ts           # Wix → Email (carritos abandonados)
│   ├── odoo-chat-leads.ts           # Odoo livechat → Email (leads diarios)
│   └── odoo-chat-analysis.ts        # Odoo livechat → Email (análisis semanal)
├── services/
│   ├── settings-db.ts               # SQLite (configuración persistente)
│   ├── wix-api.ts                   # Cliente REST API Wix
│   ├── odoo.ts                      # Cliente XML-RPC Odoo
│   ├── mssql.ts                     # Pool MSSQL (SQL Server)
│   ├── database.ts                  # Pool PostgreSQL
│   └── email.ts                     # Servicio SMTP (nodemailer)
└── utils/
    └── logger.ts                    # Logger estructurado con timestamps
scripts/
└── setup-db.ts                      # Inicialización de settings.db
```
