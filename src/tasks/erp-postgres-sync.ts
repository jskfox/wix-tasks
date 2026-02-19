import { BaseTask } from './base-task';
import { config, getEmailsForTask } from '../config';
import { logger } from '../utils/logger';
import { mssqlQuery } from '../services/mssql';
import { getPool } from '../services/database';
import { sendEmail } from '../services/email';
import { Readable } from 'stream';
import { from as copyFrom } from 'pg-copy-streams';

const CTX = 'ErpPostgresSync';

// ── Table names ─────────────────────────────────────────────────────────────────
const STAGING_TABLE = 'maestro_precios_sucursal_new';
const LIVE_TABLE = 'maestro_precios_sucursal';
const OLD_TABLE = 'maestro_precios_sucursal_old';

// ── Column order for PostgreSQL COPY ────────────────────────────────────────────
const PG_COLUMNS = [
  'sucursal', 'sku', 'abc', 'tag', 'nombre_corto', 'nombre', 'modelo',
  'departamento', 'categoria', 'subcategoria', 'precio', 'impuesto',
  'ieps', 'costo_total', 'ubicacion', 'precio_actualizado', 'existencia',
  'unidad_simbolo', 'unidad'
];

// ── Interfaces ──────────────────────────────────────────────────────────────────
interface MssqlPriceRow {
  sucursal: number;
  sku: string;
  abc: string;
  tag: string | null;
  nombre_corto: string;
  nombre: string;
  modelo: string;
  departamento: string;
  categoria: string;
  subcategoria: string;
  precio: number;
  impuesto: number;
  ieps: number;
  costo_total: number;
  ubicacion: string;
  precio_actualizado: Date;
  existencia: number;
  unidad_simbolo: string;
  unidad: string;
}

interface MssqlCodigoRow {
  sku: string;
  codigo: string;
}

interface PriceChange {
  zona: string;
  sku: string;
  nombre_corto: string;
  ubicacion: string;
  prioridad: string;
  num_sucursales: number;
  sucursales: string;
  sucursales_precios: string;
  precio_anterior_min: number;
  precio_anterior_max: number;
  nuevo_precio_min: number;
  nuevo_precio_max: number;
  variacion_promedio: number;
  variacion_maxima: number;
  precios_diferentes: boolean;
}

interface AnalysisResults {
  totalChanges: number;
  microChanges: number;
  microChangesRegistros: number;
  minorChanges: number;
  prioridadBaja: PriceChange[];
  prioridadMedia: PriceChange[];
  prioridadAlta: PriceChange[];
  historyInserted: number;
  mexicali: { prioridadAlta: PriceChange[]; prioridadMedia: PriceChange[]; prioridadBaja: PriceChange[] };
  hermosillo: { prioridadAlta: PriceChange[]; prioridadMedia: PriceChange[]; prioridadBaja: PriceChange[] };
}

// ── Helper functions ────────────────────────────────────────────────────────────

function formatValue(val: unknown): string {
  if (val === null || typeof val === 'undefined') {
    return '\\N'; // NULL representation for Postgres COPY
  }
  if (val instanceof Date) {
    return val.toISOString();
  }
  const str = String(val);
  return str.replace(/\\/g, '\\\\')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}

// ── Main MSSQL Query ────────────────────────────────────────────────────────────
const MSSQL_PRICES_QUERY = `
WITH ArticulosActivos AS (
    SELECT
        Emp_Id, Articulo_Id, Categoria_Id, SubCategoria_Id, Depto_Id, Articulo_Nombre,
        Articulo_Nombre_Corto, Articulo_Precio1, Articulo_Modelo, Articulo_Costo_Bruto,
        Articulo_Peso_Pieza, Articulo_ABC, Unidad_Id,
        CASE
            WHEN LEFT(Articulo_Nombre, 1) = '(' AND CHARINDEX(')', Articulo_Nombre) = 5
            THEN LTRIM(SUBSTRING(Articulo_Nombre, 6, 8000))
            ELSE Articulo_Nombre
        END AS Nombre_Limpio,
        CASE
            WHEN LEFT(Articulo_Nombre_Corto , 1) = '(' AND CHARINDEX(')', Articulo_Nombre_Corto) = 5
            THEN LTRIM(SUBSTRING(Articulo_Nombre_Corto, 6, 8000))
            ELSE Articulo_Nombre_Corto
        END AS Nombre_Corto,
        CASE
            WHEN LEFT(Articulo_Nombre, 1) = '(' AND CHARINDEX(')', Articulo_Nombre) = 5
            THEN SUBSTRING(Articulo_Nombre, 2, 3)
            ELSE NULL
        END AS tag
    FROM dbo.Articulo WITH (NOLOCK)
    WHERE Emp_Id = 1 AND Articulo_Activo_Venta = 1 AND Articulo_Nombre NOT LIKE ('(INS)%')
)
SELECT
    CASE axs.Suc_Id
        WHEN 1 THEN 101 WHEN 2 THEN 102 WHEN 3 THEN 103 WHEN 4 THEN 104
        WHEN 5 THEN 105 WHEN 6 THEN 106 WHEN 7 THEN 108 WHEN 8 THEN 109
        WHEN 9 THEN 110 WHEN 10 THEN 112 WHEN 11 THEN 113 WHEN 16 THEN 401
        WHEN 17 THEN 402 WHEN 18 THEN 403
    END AS sucursal,
    a.Articulo_Id as sku,
    a.Articulo_ABC as abc,
    a.tag,
    a.Nombre_Corto as nombre_corto,
    a.Nombre_Limpio as nombre,
    a.Articulo_Modelo as modelo,
    d.Depto_Nombre as departamento,
    c.Categoria_Nombre as categoria,
    sc.SubCategoria_Nombre as subcategoria,
    ROUND(axsc.Precio, 4) as precio,
    ROUND(axsc.Impuesto, 4) as impuesto,
    ROUND(axsc.IEPS, 4) as ieps,
    ROUND(axs.AxS_Costo_Total, 4) as costo_total,
    axb.AxB_Posicion_A1 as ubicacion,
    axs.AxS_Fec_Actualizacion as precio_actualizado,
    ISNULL(axb.AxB_Existencia, 0) AS existencia,
    u.Unidad_Simbolo as unidad_simbolo,
    u.Unidad_Nombre as unidad
FROM dbo.Articulo_x_Sucursal AS axs WITH (NOLOCK)
INNER JOIN ArticulosActivos AS a ON axs.Emp_Id = a.Emp_Id AND axs.Articulo_Id = a.Articulo_Id
INNER JOIN dbo.Articulo_X_Sucursal_Consulta AS axsc WITH (NOLOCK) ON axs.Emp_Id = axsc.Emp_Id AND axs.Suc_Id = axsc.Suc_Id AND axs.Articulo_Id = axsc.Articulo_Id
LEFT JOIN dbo.Articulo_x_Bodega AS axb WITH (NOLOCK) ON axs.Emp_Id = axb.Emp_Id AND axs.Suc_Id = axb.Suc_Id AND axs.Articulo_Id = axb.Articulo_Id AND axb.Bodega_Id = 1
LEFT JOIN dbo.Departamento AS d WITH (NOLOCK) ON a.Emp_Id = d.Emp_Id AND a.Depto_Id = d.Depto_Id
LEFT JOIN dbo.Categoria_Articulo AS c WITH (NOLOCK) ON a.Emp_Id = c.Emp_Id AND a.Categoria_Id = c.Categoria_Id
LEFT JOIN dbo.SubCategoria_Articulo AS sc WITH (NOLOCK) ON a.Emp_Id = sc.Emp_Id AND a.Categoria_Id = sc.Categoria_Id AND a.SubCategoria_Id = sc.SubCategoria_Id
LEFT JOIN dbo.Unidad AS u WITH (NOLOCK) ON a.Unidad_Id = u.Unidad_Id
WHERE axs.Emp_Id = 1 AND axs.Suc_Id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 16, 17, 18);
`;

const MSSQL_CODIGOS_QUERY = `
SELECT ae.Articulo_Id as sku, ae.Equivalente_Id as codigo  
FROM LDCOM_PROCONSA.dbo.Articulo_Equivalente AS ae WITH (NOLOCK);
`;

// ═════════════════════════════════════════════════════════════════════════════
// TASK
// ═════════════════════════════════════════════════════════════════════════════

export class ErpPostgresSyncTask extends BaseTask {
  readonly name = 'erp-postgres-sync';
  readonly description = 'Sincroniza precios y existencias desde ERP (MSSQL) hacia PostgreSQL. Incluye análisis de cambios de precios y notificación por email.';
  // Every 30 minutes from 6am to 9pm Pacific
  readonly cronExpression = '*/30 6-21 * * *';

  private executionLog: string[] = [];

  private log(message: string): void {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] ${message}`;
    logger.info(CTX, message);
    this.executionLog.push(logEntry);
  }

  async execute(): Promise<void> {
    this.executionLog = [];
    this.log('Iniciando proceso ETL de precios ERP → PostgreSQL...');

    const pool = getPool();
    let etlSuccess = false;

    try {
      // ── PASO 1: EXTRAER DATOS DE MSSQL ────────────────────────────────────
      this.log('Extrayendo datos de MSSQL...');
      const t0 = Date.now();
      const data = await mssqlQuery<MssqlPriceRow>(MSSQL_PRICES_QUERY);
      this.log(`Extracción completada: ${data.length} filas en ${((Date.now() - t0) / 1000).toFixed(1)}s`);

      if (data.length === 0) {
        this.log('Advertencia: El query de MSSQL no devolvió datos. Proceso detenido.');
        return;
      }

      // ── PASO 2: PREPARAR STAGING EN POSTGRESQL ────────────────────────────
      this.log('Preparando tabla de staging en PostgreSQL...');
      const client = await pool.connect();

      try {
        await client.query(`DROP TABLE IF EXISTS ${STAGING_TABLE};`);
        await client.query(`CREATE TABLE ${STAGING_TABLE} (LIKE ${LIVE_TABLE} INCLUDING ALL);`);

        // ── PASO 3: CARGAR DATOS CON COPY STREAMING ───────────────────────────
        this.log('Iniciando carga masiva (COPY) a PostgreSQL...');
        const t1 = Date.now();

        const pgStream = client.query(
          copyFrom(`COPY ${STAGING_TABLE} (${PG_COLUMNS.join(',')}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')`)
        );

        const dataStream = new Readable({ read() {} });
        for (const row of data) {
          const csvLine = PG_COLUMNS.map(col => formatValue((row as unknown as Record<string, unknown>)[col])).join('\t') + '\n';
          dataStream.push(csvLine);
        }
        dataStream.push(null);

        await new Promise<void>((resolve, reject) => {
          dataStream.pipe(pgStream)
            .on('finish', resolve)
            .on('error', reject);
        });

        this.log(`Carga masiva completada en ${((Date.now() - t1) / 1000).toFixed(1)}s`);

        // ── PASO 4: SWAP ATÓMICO DE TABLAS ────────────────────────────────────
        this.log('Iniciando swap atómico de tablas...');
        await client.query('BEGIN;');
        await client.query(`DROP TABLE IF EXISTS ${OLD_TABLE};`);
        await client.query(`ALTER TABLE ${LIVE_TABLE} RENAME TO ${OLD_TABLE};`);
        await client.query(`ALTER TABLE ${STAGING_TABLE} RENAME TO ${LIVE_TABLE};`);
        await client.query('COMMIT;');

        this.log('Proceso ETL completado exitosamente.');
        etlSuccess = true;

      } catch (err) {
        await client.query('ROLLBACK;');
        throw err;
      } finally {
        client.release();
      }

      // ── PASO 5: SINCRONIZAR TABLA ARTICULO_CODIGO ─────────────────────────
      try {
        await this.syncArticuloCodigo();
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        this.log(`Error en sincronización de articulo_codigo (no crítico): ${msg}`);
      }

      // ── PASO 6: ACTUALIZAR FECHA DE SINCRONIZACIÓN ────────────────────────
      await this.updateSyncDate();

      // ── PASO 7: ANALIZAR CAMBIOS DE PRECIOS ───────────────────────────────
      try {
        const analysisResults = await this.analyzePriceChanges();
        await this.sendPriceChangeReport(analysisResults);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        this.log(`Error durante análisis post-ETL: ${msg}`);
        await this.sendErrorEmail('Error en Análisis de Precios', msg);
      }

    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.log(`ERROR DURANTE EL PROCESO ETL: ${msg}`);
      await this.sendErrorEmail('Error en Sincronización ERP→PostgreSQL', msg);
    }
  }

  // ── Sync articulo_codigo table ──────────────────────────────────────────────
  private async syncArticuloCodigo(): Promise<void> {
    this.log('Iniciando sincronización de articulo_codigo...');

    const data = await mssqlQuery<MssqlCodigoRow>(MSSQL_CODIGOS_QUERY);
    this.log(`Extracción de códigos: ${data.length} registros`);

    if (data.length === 0) {
      this.log('Advertencia: No se encontraron códigos de artículos en MSSQL.');
      return;
    }

    const pool = getPool();
    const client = await pool.connect();

    try {
      await client.query('TRUNCATE TABLE articulo_codigo;');

      const pgStream = client.query(
        copyFrom("COPY articulo_codigo (sku, codigo) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')")
      );

      const dataStream = new Readable({ read() {} });
      for (const row of data) {
        const line = `${formatValue(row.sku)}\t${formatValue(row.codigo)}\n`;
        dataStream.push(line);
      }
      dataStream.push(null);

      await new Promise<void>((resolve, reject) => {
        dataStream.pipe(pgStream)
          .on('finish', resolve)
          .on('error', reject);
      });

      this.log(`Sincronización de articulo_codigo completada: ${data.length} registros`);
    } finally {
      client.release();
    }
  }

  // ── Update sync_date table ──────────────────────────────────────────────────
  private async updateSyncDate(): Promise<void> {
    const pool = getPool();
    await pool.query(`
      INSERT INTO sync_date (id, ultima_actualizacion) 
      VALUES (1, CURRENT_TIMESTAMP)
      ON CONFLICT (id) 
      DO UPDATE SET ultima_actualizacion = CURRENT_TIMESTAMP
    `);
    this.log('Fecha de sincronización actualizada en sync_date');
  }

  // ── Analyze price changes ───────────────────────────────────────────────────
  private async analyzePriceChanges(): Promise<AnalysisResults> {
    this.log('Iniciando análisis de cambios de precios...');

    const pool = getPool();
    const results: AnalysisResults = {
      totalChanges: 0,
      microChanges: 0,
      microChangesRegistros: 0,
      minorChanges: 0,
      prioridadBaja: [],
      prioridadMedia: [],
      prioridadAlta: [],
      historyInserted: 0,
      mexicali: { prioridadAlta: [], prioridadMedia: [], prioridadBaja: [] },
      hermosillo: { prioridadAlta: [], prioridadMedia: [], prioridadBaja: [] },
    };

    // Insert changes into history table
    this.log('Insertando cambios en tabla history...');
    const insertResult = await pool.query(`
      INSERT INTO history (sku, precio, sucursal, fecha, variacion)
      SELECT 
        n.sku,
        n.precio,
        n.sucursal,
        CURRENT_DATE,
        ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1) as variacion
      FROM maestro_precios_sucursal n
      INNER JOIN maestro_precios_sucursal_old o 
        ON n.sucursal = o.sucursal AND n.sku = o.sku
      WHERE n.precio != o.precio
        AND o.precio > 0
        AND ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1) != 0
      ON CONFLICT DO NOTHING
    `);
    results.historyInserted = insertResult.rowCount || 0;
    this.log(`Registros insertados en history: ${results.historyInserted}`);

    // Count total changes
    const countResult = await pool.query(`
      SELECT COUNT(*) as total
      FROM maestro_precios_sucursal n
      INNER JOIN maestro_precios_sucursal_old o 
        ON n.sucursal = o.sucursal AND n.sku = o.sku
      WHERE n.precio != o.precio
        AND o.precio > 0
        AND ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1) != 0
    `);
    results.totalChanges = parseInt(countResult.rows[0].total);
    this.log(`Total de cambios detectados: ${results.totalChanges}`);

    // Count micro changes (<0.1%)
    const microResult = await pool.query(`
      SELECT COUNT(DISTINCT n.sku) as total_articulos,
             COUNT(*) as total_registros
      FROM maestro_precios_sucursal n
      INNER JOIN maestro_precios_sucursal_old o 
        ON n.sucursal = o.sucursal AND n.sku = o.sku
      WHERE n.precio != o.precio
        AND o.precio > 0
        AND ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1) = 0
    `);
    results.microChanges = parseInt(microResult.rows[0].total_articulos);
    results.microChangesRegistros = parseInt(microResult.rows[0].total_registros);

    // Count minor changes (0.1% - 10%)
    const minorResult = await pool.query(`
      SELECT COUNT(DISTINCT n.sku) as total
      FROM maestro_precios_sucursal n
      INNER JOIN maestro_precios_sucursal_old o 
        ON n.sucursal = o.sucursal AND n.sku = o.sku
      WHERE n.precio != o.precio
        AND o.precio > 0
        AND ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) > 0
        AND ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) < 10
    `);
    results.minorChanges = parseInt(minorResult.rows[0].total);

    // Get significant changes (>=10%) grouped by article and zone
    const significantResult = await pool.query<PriceChange>(`
      WITH cambios AS (
        SELECT 
          n.sucursal,
          n.sku,
          n.nombre_corto,
          n.ubicacion,
          n.precio as nuevo_precio,
          o.precio as precio_anterior,
          ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1) as variacion_porcentaje,
          ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) as variacion_absoluta,
          CASE 
            WHEN n.sucursal IN (101,102,103,104,105,106,108,109,110,112,113) THEN 'Mexicali'
            WHEN n.sucursal IN (401,402,403) THEN 'Hermosillo'
            ELSE 'Otra'
          END as zona,
          CASE 
            WHEN ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) >= 30 THEN 'alta'
            WHEN ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) >= 15 THEN 'media'
            WHEN ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) >= 10 THEN 'baja'
            ELSE 'normal'
          END as prioridad
        FROM maestro_precios_sucursal n
        INNER JOIN maestro_precios_sucursal_old o 
          ON n.sucursal = o.sucursal AND n.sku = o.sku
        WHERE n.precio != o.precio
          AND o.precio > 0
          AND ABS(ROUND(((n.precio - o.precio) / o.precio * 100)::numeric, 1)) >= 10
      ),
      agrupado AS (
        SELECT 
          zona,
          sku,
          nombre_corto,
          MAX(ubicacion) as ubicacion,
          prioridad,
          COUNT(DISTINCT sucursal) as num_sucursales,
          STRING_AGG(sucursal::text, ',' ORDER BY sucursal::text) as sucursales,
          STRING_AGG(sucursal::text || ':' || nuevo_precio::text, ',' ORDER BY sucursal::text) as sucursales_precios,
          MIN(precio_anterior) as precio_anterior_min,
          MAX(precio_anterior) as precio_anterior_max,
          MIN(nuevo_precio) as nuevo_precio_min,
          MAX(nuevo_precio) as nuevo_precio_max,
          AVG(variacion_absoluta) as variacion_promedio,
          MAX(variacion_absoluta) as variacion_maxima,
          CASE 
            WHEN COUNT(DISTINCT nuevo_precio) > 1 THEN true
            ELSE false
          END as precios_diferentes
        FROM cambios
        GROUP BY zona, sku, nombre_corto, prioridad
      )
      SELECT * FROM agrupado
      ORDER BY 
        CASE prioridad 
          WHEN 'alta' THEN 1 
          WHEN 'media' THEN 2 
          WHEN 'baja' THEN 3 
        END,
        variacion_maxima DESC,
        zona,
        sku
    `);

    // Classify results by zone and priority
    for (const change of significantResult.rows) {
      if (change.prioridad === 'alta') {
        results.prioridadAlta.push(change);
      } else if (change.prioridad === 'media') {
        results.prioridadMedia.push(change);
      } else if (change.prioridad === 'baja') {
        results.prioridadBaja.push(change);
      }

      if (change.zona === 'Mexicali') {
        if (change.prioridad === 'alta') results.mexicali.prioridadAlta.push(change);
        else if (change.prioridad === 'media') results.mexicali.prioridadMedia.push(change);
        else if (change.prioridad === 'baja') results.mexicali.prioridadBaja.push(change);
      } else if (change.zona === 'Hermosillo') {
        if (change.prioridad === 'alta') results.hermosillo.prioridadAlta.push(change);
        else if (change.prioridad === 'media') results.hermosillo.prioridadMedia.push(change);
        else if (change.prioridad === 'baja') results.hermosillo.prioridadBaja.push(change);
      }
    }

    this.log(`Análisis completado: ${results.prioridadAlta.length} alta, ${results.prioridadMedia.length} media, ${results.prioridadBaja.length} baja`);
    this.log(`Mexicali: ${results.mexicali.prioridadAlta.length} alta, ${results.mexicali.prioridadMedia.length} media, ${results.mexicali.prioridadBaja.length} baja`);
    this.log(`Hermosillo: ${results.hermosillo.prioridadAlta.length} alta, ${results.hermosillo.prioridadMedia.length} media, ${results.hermosillo.prioridadBaja.length} baja`);

    return results;
  }

  // ── Send price change report email ──────────────────────────────────────────
  private async sendPriceChangeReport(results: AnalysisResults): Promise<void> {
    const totalCambios = results.prioridadAlta.length +
      results.prioridadMedia.length +
      results.prioridadBaja.length +
      results.microChanges +
      results.minorChanges;

    if (totalCambios === 0) {
      this.log('No se detectaron cambios de precio. No se enviará correo.');
      return;
    }

    this.log(`Se detectaron cambios de precio en ${totalCambios} artículos. Enviando notificación...`);

    // Generate reports
    const htmlReport = this.generateHTMLReport(results);
    const csvContent = this.generateCSVReport(results);
    const textReport = this.generateTextReport(results);

    // Build subject
    let subject = 'Sincronización de Precios - Completada';
    if (results.prioridadAlta.length > 0) {
      subject += ` - ${results.prioridadAlta.length} cambios URGENTES (>30%)`;
    } else if (results.prioridadMedia.length > 0) {
      subject += ` - ${results.prioridadMedia.length} cambios a verificar (>15%)`;
    } else if (results.prioridadBaja.length > 0) {
      subject += ` - ${results.prioridadBaja.length} cambios a revisar (>10%)`;
    } else {
      subject += ` - ${totalCambios} cambios menores detectados`;
    }

    // Prepare attachments
    const attachments: Array<{ filename: string; content: string; contentType: string }> = [];
    if (results.prioridadAlta.length > 0 || results.prioridadMedia.length > 0 || results.prioridadBaja.length > 0) {
      const fecha = new Date().toISOString().split('T')[0];
      attachments.push({
        filename: `cambios_precios_${fecha}.csv`,
        content: csvContent,
        contentType: 'text/csv; charset=utf-8'
      });
    }

    // Get recipients from config (specific to this task with fallback to marketing)
    const recipients = getEmailsForTask('erpPostgresSync');
    if (recipients.length === 0) {
      this.log('No hay destinatarios configurados para el reporte de precios (emails.erp_postgres_sync o emails.marketing).');
      return;
    }

    await sendEmail({
      to: recipients,
      subject,
      html: htmlReport,
      text: textReport,
      attachments,
    });

    this.log(`Email enviado: ${subject}`);
  }

  // ── Generate CSV report ─────────────────────────────────────────────────────
  private generateCSVReport(results: AnalysisResults): string {
    let csv = 'Zona\tSKU\tNombre\tUbicacion\tPrecio Anterior\tPrecio Nuevo\tVariacion %\tSucursal(es) con Precio Diferente\n';

    const addChanges = (changes: PriceChange[], zona: string) => {
      for (const change of changes) {
        const variacion = parseFloat(String(change.variacion_maxima));
        const variacionStr = variacion >= 0 ? `+${variacion.toFixed(1)}%` : `${variacion.toFixed(1)}%`;
        const nombreLimpio = (change.nombre_corto || '').replace(/[\t\n\r]/g, ' ');
        const ubicacionLimpia = (change.ubicacion || '').replace(/[\t\n\r]/g, ' ');
        const sucursalInfo = change.precios_diferentes ? (change.sucursales_precios || '') : '';

        const precioAnterior = change.precio_anterior_min === change.precio_anterior_max
          ? `$${parseFloat(String(change.precio_anterior_min)).toFixed(4)}`
          : `$${parseFloat(String(change.precio_anterior_min)).toFixed(4)} - $${parseFloat(String(change.precio_anterior_max)).toFixed(4)}`;

        const precioNuevo = change.nuevo_precio_min === change.nuevo_precio_max
          ? `$${parseFloat(String(change.nuevo_precio_min)).toFixed(4)}`
          : `$${parseFloat(String(change.nuevo_precio_min)).toFixed(4)} - $${parseFloat(String(change.nuevo_precio_max)).toFixed(4)}`;

        csv += `${zona}\t${change.sku}\t${nombreLimpio}\t${ubicacionLimpia}\t${precioAnterior}\t${precioNuevo}\t${variacionStr}\t${sucursalInfo}\n`;
      }
    };

    addChanges(results.mexicali.prioridadAlta, 'Mexicali');
    addChanges(results.mexicali.prioridadMedia, 'Mexicali');
    addChanges(results.mexicali.prioridadBaja, 'Mexicali');
    addChanges(results.hermosillo.prioridadAlta, 'Hermosillo');
    addChanges(results.hermosillo.prioridadMedia, 'Hermosillo');
    addChanges(results.hermosillo.prioridadBaja, 'Hermosillo');

    return csv;
  }

  // ── Generate text report ────────────────────────────────────────────────────
  private generateTextReport(results: AnalysisResults): string {
    let report = `REPORTE DE ANÁLISIS DE CAMBIOS DE PRECIOS\n`;
    report += `==========================================\n`;
    report += `Fecha: ${new Date().toISOString()}\n\n`;
    report += `RESUMEN GENERAL:\n`;
    report += `- Total de cambios detectados: ${results.totalChanges}\n`;
    report += `- Prioridad ALTA (>30%): ${results.prioridadAlta.length} artículos\n`;
    report += `- Prioridad MEDIA (>15%): ${results.prioridadMedia.length} artículos\n`;
    report += `- Prioridad BAJA (>10%): ${results.prioridadBaja.length} artículos\n`;
    report += `- Registros guardados en history: ${results.historyInserted}\n\n`;
    report += `RESUMEN POR ZONA:\n`;
    report += `- Mexicali: ${results.mexicali.prioridadAlta.length} alta, ${results.mexicali.prioridadMedia.length} media, ${results.mexicali.prioridadBaja.length} baja\n`;
    report += `- Hermosillo: ${results.hermosillo.prioridadAlta.length} alta, ${results.hermosillo.prioridadMedia.length} media, ${results.hermosillo.prioridadBaja.length} baja\n\n`;
    report += `LOG DE EJECUCIÓN:\n`;
    report += this.executionLog.join('\n');
    return report;
  }

  // ── Generate HTML report ────────────────────────────────────────────────────
  private generateHTMLReport(results: AnalysisResults): string {
    const fecha = new Date().toLocaleString('es-MX', { timeZone: 'America/Hermosillo' });

    const generatePrioritySection = (title: string, items: PriceChange[], className: string, limit: number): string => {
      if (items.length === 0) return '';

      let section = `
        <details class="${className}">
          <summary>${title} - ${items.length} artículo(s)</summary>
          <div class="priority-content">`;

      items.slice(0, limit).forEach(item => {
        const variacion = parseFloat(String(item.variacion_maxima));
        const variacionClass = variacion >= 0 ? 'positive' : 'negative';
        const variacionStr = variacion >= 0 ? `+${variacion.toFixed(1)}%` : `${variacion.toFixed(1)}%`;

        section += `
          <div class="item">
            <div class="item-header">
              <div>
                <div class="item-sku">SKU: ${item.sku}</div>
                <div class="item-name">${item.nombre_corto}</div>
                ${item.ubicacion ? `<div class="item-location">📍 ${item.ubicacion}</div>` : ''}
              </div>
              <span class="variation ${variacionClass}">${variacionStr}</span>
            </div>
            <div class="item-details">
              <div class="detail-box">
                <div class="detail-label">Sucursales</div>
                <div class="detail-value">${item.num_sucursales} sucursal(es)</div>
              </div>
              <div class="detail-box">
                <div class="detail-label">Precio Anterior</div>
                <div class="detail-value">$${parseFloat(String(item.precio_anterior_min)).toFixed(4)}</div>
              </div>
              <div class="detail-box">
                <div class="detail-label">Precio Nuevo</div>
                <div class="detail-value">$${parseFloat(String(item.nuevo_precio_min)).toFixed(4)}</div>
              </div>
            </div>
            ${item.precios_diferentes ? `<div class="warning">⚠️ <strong>Precios diferentes entre sucursales:</strong> ${item.sucursales_precios}</div>` : ''}
          </div>`;
      });

      if (items.length > limit) {
        section += `<div class="item" style="text-align: center; color: #666;"><em>... y ${items.length - limit} artículo(s) más. Ver archivo CSV adjunto.</em></div>`;
      }

      section += `</div></details>`;
      return section;
    };

    const generateZoneSection = (zoneName: string, zoneData: { prioridadAlta: PriceChange[]; prioridadMedia: PriceChange[]; prioridadBaja: PriceChange[] }, className: string): string => {
      const total = zoneData.prioridadAlta.length + zoneData.prioridadMedia.length + zoneData.prioridadBaja.length;
      if (total === 0) return '';

      return `
        <div class="zone-section">
          <div class="zone-header ${className}">
            <h2>📍 ${zoneName}</h2>
            <div class="zone-stats">
              <div class="zone-stat"><div class="stat-number">${zoneData.prioridadAlta.length}</div><div class="stat-label">Urgentes (>30%)</div></div>
              <div class="zone-stat"><div class="stat-number">${zoneData.prioridadMedia.length}</div><div class="stat-label">Verificar pronto (>15%)</div></div>
              <div class="zone-stat"><div class="stat-number">${zoneData.prioridadBaja.length}</div><div class="stat-label">Revisar (>10%)</div></div>
            </div>
          </div>
          ${generatePrioritySection('Verificar Urgente (>30%)', zoneData.prioridadAlta, 'alta', 10)}
          ${generatePrioritySection('Verificar Pronto (>15%)', zoneData.prioridadMedia, 'media', 10)}
          ${generatePrioritySection('Revisar (>10%)', zoneData.prioridadBaja, 'baja', 5)}
        </div>`;
    };

    return `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Reporte de Sincronización de Precios</title>
  <style>
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f5f5f5; margin: 0; padding: 20px; }
    .container { max-width: 1000px; margin: 0 auto; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px 10px 0 0; }
    .header h1 { margin: 0; font-size: 28px; }
    .header p { margin: 10px 0 0 0; opacity: 0.9; }
    .content { padding: 30px; }
    .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
    .summary-card { background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #667eea; }
    .summary-card h3 { margin: 0 0 10px 0; font-size: 14px; color: #666; text-transform: uppercase; }
    .summary-card .number { font-size: 32px; font-weight: bold; color: #333; margin: 0; }
    .summary-card .label { font-size: 12px; color: #999; margin-top: 5px; }
    .zone-section { margin: 30px 0; }
    .zone-header { color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
    .zone-header.mexicali { background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); }
    .zone-header.hermosillo { background: linear-gradient(135deg, #be123c 0%, #f97316 100%); }
    .zone-header h2 { margin: 0; font-size: 24px; }
    .zone-stats { display: flex; gap: 20px; margin-top: 15px; flex-wrap: wrap; }
    .zone-stat { background: rgba(255,255,255,0.25); padding: 10px 20px; border-radius: 5px; }
    .zone-stat .stat-number { font-size: 24px; font-weight: bold; }
    .zone-stat .stat-label { font-size: 12px; }
    details { border: 1px solid #ddd; border-radius: 8px; margin: 15px 0; }
    details summary { background: #dc3545; color: white; padding: 15px 20px; border-radius: 8px; cursor: pointer; font-weight: bold; list-style: none; }
    details summary::-webkit-details-marker { display: none; }
    details[open] summary { border-radius: 8px 8px 0 0; }
    details.media summary { background: #fd7e14; }
    details.baja summary { background: #ffc107; color: #333; }
    .priority-content { padding: 0; background: white; }
    .item { padding: 20px; border-bottom: 1px solid #eee; }
    .item:last-child { border-bottom: none; }
    .item-header { display: flex; justify-content: space-between; align-items: start; margin-bottom: 10px; }
    .item-sku { font-size: 18px; font-weight: bold; color: #333; }
    .item-name { color: #666; margin-top: 5px; }
    .item-location { color: #999; font-size: 14px; margin-top: 5px; }
    .item-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 15px; }
    .detail-box { background: #f8f9fa; padding: 12px; border-radius: 5px; }
    .detail-label { font-size: 12px; color: #666; margin-bottom: 5px; }
    .detail-value { font-size: 16px; font-weight: bold; color: #333; }
    .variation { display: inline-block; padding: 5px 15px; border-radius: 20px; font-weight: bold; }
    .variation.positive { background: #dc3545; color: white; }
    .variation.negative { background: #28a745; color: white; }
    .warning { background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin-top: 10px; border-radius: 5px; }
    .logs { background: #f8f9fa; padding: 20px; border-radius: 8px; margin-top: 30px; }
    .logs h3 { margin-top: 0; color: #666; }
    .logs pre { background: white; padding: 15px; border-radius: 5px; overflow-x: auto; font-size: 12px; line-height: 1.6; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>📊 Sincronización de Precios Completada</h1>
      <p>${fecha}</p>
    </div>
    <div class="content">
      <h2>Resumen General</h2>
      <div class="summary-grid">
        <div class="summary-card">
          <h3>Total de Artículos</h3>
          <p class="number">${(results.prioridadAlta.length + results.prioridadMedia.length + results.prioridadBaja.length).toLocaleString()}</p>
          <p class="label">Con cambios de precio</p>
        </div>
        <div class="summary-card" style="border-left-color: #dc3545;">
          <h3>Verificar Urgente</h3>
          <p class="number" style="color: #dc3545;">${results.prioridadAlta.length}</p>
          <p class="label">Artículos > 30%</p>
        </div>
        <div class="summary-card" style="border-left-color: #fd7e14;">
          <h3>Verificar Pronto</h3>
          <p class="number" style="color: #fd7e14;">${results.prioridadMedia.length}</p>
          <p class="label">Artículos > 15%</p>
        </div>
        <div class="summary-card" style="border-left-color: #ffc107;">
          <h3>Revisar</h3>
          <p class="number" style="color: #ffc107;">${results.prioridadBaja.length}</p>
          <p class="label">Artículos > 10%</p>
        </div>
      </div>
      ${generateZoneSection('MEXICALI', results.mexicali, 'mexicali')}
      ${generateZoneSection('HERMOSILLO', results.hermosillo, 'hermosillo')}
      <div class="logs">
        <h3>📋 Log de Ejecución</h3>
        <pre>${this.executionLog.join('\n')}</pre>
      </div>
    </div>
  </div>
</body>
</html>`;
  }

  // ── Send error email ────────────────────────────────────────────────────────
  private async sendErrorEmail(title: string, errorMessage: string): Promise<void> {
    const recipients = getEmailsForTask('erpPostgresSync');
    if (recipients.length === 0) return;

    let errorReport = `${title}\n${'='.repeat(title.length)}\n\n`;
    errorReport += `Error: ${errorMessage}\n\n`;
    errorReport += 'LOG DE EJECUCIÓN:\n';
    errorReport += this.executionLog.join('\n');

    await sendEmail({
      to: recipients,
      subject: `ERROR - ${title}`,
      html: `<pre>${errorReport}</pre>`,
      text: errorReport,
    });
  }
}
