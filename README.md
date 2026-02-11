# Wix Scheduled Tasks — Proconsa

Sistema modular de tareas programadas para la tienda Wix de Proconsa. Ejecuta procesos periódicos como reportes de carritos abandonados y sincronización de precios/inventario desde PostgreSQL.

## Tareas Incluidas

| Tarea | Cron | Descripción |
|---|---|---|
| `price-inventory-sync` | `:05` y `:35` de cada hora | Detecta cambios de precio/inventario en PostgreSQL (sucursal 101) y sincroniza con Wix Store |
| `abandoned-carts-report` | `10:00 PM` diario | Consulta carritos abandonados del día en Wix y envía reporte HTML por email al equipo de marketing |

## Requisitos

- Node.js >= 20
- PostgreSQL (base de datos `prices`)
- Credenciales de API de Wix (Proconsa)
- Servidor SMTP para envío de correos

## Setup

```bash
# Instalar dependencias
npm install

# Copiar y configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# Desarrollo
npm run dev

# Producción
npm run build
npm start
```

## Variables de Entorno

| Variable | Descripción | Ejemplo |
|---|---|---|
| `PG_HOST` | Host de PostgreSQL | `localhost` |
| `PG_PORT` | Puerto de PostgreSQL | `5432` |
| `PG_DATABASE` | Base de datos | `prices` |
| `PG_USER` | Usuario de PostgreSQL | `postgres` |
| `PG_PASSWORD` | Contraseña | |
| `SUCURSAL_WIX` | Sucursal que mapea a la tienda Wix | `101` |
| `WIX_SITE_ID` | Site ID de Wix (Proconsa) | |
| `WIX_API_KEY` | API Key de Wix | |
| `SMTP_HOST` | Servidor SMTP | |
| `SMTP_PORT` | Puerto SMTP | `587` |
| `SMTP_SECURE` | Usar TLS | `false` |
| `SMTP_USER` | Usuario SMTP | |
| `SMTP_PASS` | Contraseña SMTP | |
| `SMTP_FROM` | Email remitente | |
| `MARKETING_EMAILS` | Lista de emails destino (separados por coma) | |
| `TZ` | Zona horaria | `America/Los_Angeles` |
| `LOG_LEVEL` | Nivel de log | `info` |

## Agregar una Nueva Tarea

1. Crear un archivo en `src/tasks/mi-tarea.ts`
2. Extender `BaseTask`:

```typescript
import { BaseTask } from './base-task';

export class MiTareaTask extends BaseTask {
  readonly name = 'mi-tarea';
  readonly cronExpression = '0 */6 * * *'; // cada 6 horas

  async execute(): Promise<void> {
    // tu lógica aquí
  }
}
```

3. Registrar en `src/index.ts`:

```typescript
import { MiTareaTask } from './tasks/mi-tarea';
registerTask(new MiTareaTask());
```

## Deploy en Coolify (Nixpacks)

1. Crear un nuevo servicio en Coolify apuntando al repositorio Git
2. Nixpacks detecta automáticamente Node.js por `package.json`
3. Build command: `npm run build` (auto-detectado)
4. Start command: `npm start` (auto-detectado)
5. Configurar todas las variables de entorno en la sección **Environment Variables** de Coolify
6. El proceso corre como servicio long-running — el scheduler mantiene el proceso activo

## Estructura del Proyecto

```
src/
├── index.ts                    # Entry point
├── scheduler.ts                # Motor cron genérico
├── config.ts                   # Configuración tipada desde .env
├── tasks/
│   ├── base-task.ts            # Clase abstracta BaseTask
│   ├── abandoned-carts.ts      # Tarea: carritos abandonados → email
│   └── price-inventory-sync.ts # Tarea: sync precios/inventario
├── services/
│   ├── wix-api.ts              # Cliente REST API de Wix
│   ├── email.ts                # Servicio SMTP (nodemailer)
│   └── database.ts             # Pool PostgreSQL
└── utils/
    └── logger.ts               # Logger con timestamps
```
