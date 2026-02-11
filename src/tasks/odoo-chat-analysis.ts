import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { sendEmail } from '../services/email';
import { searchReadAll, readRecords, OdooRecord } from '../services/odoo';
import * as fs from 'fs';
import * as path from 'path';

const CTX = 'OdooChatAnalysis';

// ── HTML helpers ───────────────────────────────────────────────────────────────

function stripHtml(html: string): string {
  return html.replace(/<[^>]*>/g, '').trim();
}

// ── Pattern maps ───────────────────────────────────────────────────────────────

const INTENT_PATTERNS: Record<string, RegExp> = {
  cotizacion_mayoreo: /cotizaci[oó]n.*mayoreo|mayoreo|precio.*mayoreo/i,
  talleres_clinicas: /taller|cl[ií]nica|capacitaci[oó]n|curso|inscrib/i,
  problema_sitio: /problema.*sitio|no.*funciona|error|no.*carga|no.*puedo/i,
  solo_viendo: /solo.*viendo|nada.*gracias|no.*gracias|solo.*mirando/i,
  busca_producto: /busco|necesito|quiero|donde.*encuentro|tienen/i,
  precio: /precio|costo|cu[aá]nto.*cuesta|cu[aá]nto.*vale/i,
  envio: /env[ií]o|entrega|domicilio|mandan/i,
  ubicacion: /ubicaci[oó]n|direcci[oó]n|donde.*est[aá]n|sucursal/i,
  factura: /factura|facturaci[oó]n|cfdi|rfc/i,
  contratista: /contratista|constructor|obra|proyecto/i,
};

const PRODUCT_PATTERNS: Record<string, RegExp> = {
  'Cemento/Concreto': /cemento|concreto|mortero|mezcla|block|tabique|tabic[oó]n/i,
  'Varilla/Acero': /varilla|acero|alambre|clavo|malla|solera/i,
  'Pisos/Loseta': /piso|loseta|porcelanato|azulejo|cer[aá]mica|adocreto/i,
  'Electricidad': /cable|el[eé]ctric|interruptor|contacto|l[aá]mpara|foco/i,
  'Madera': /madera|triplay|plywood|tabla|poste|viga/i,
  'Plomería': /tubo|tuber[ií]a|v[aá]lvula|llave|conector|plomer[ií]a|tinaco/i,
  'Pintura': /pintura|rodillo|brocha|impermeabilizante|sellador/i,
  'Herramientas': /herramienta|taladro|sierra|martillo|desarmador/i,
  'Ferretería': /tornillo|pija|ancla|bisagra|jaladera|chapa|cerradura/i,
  'Vigueta/Estructura': /vigueta|bovedilla|castillo|armex|estructura/i,
  'Arena/Grava': /arena|grava|piedra|material.*p[eé]treo/i,
  'Impermeabilizante': /impermeabilizante|impermeable|membrana|asfalto/i,
};

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

const INTENT_LABELS: Record<string, string> = {
  cotizacion_mayoreo: '💰 Cotización de Mayoreo',
  talleres_clinicas: '🎓 Talleres/Clínicas',
  problema_sitio: '⚠️ Problemas con el Sitio',
  solo_viendo: '👀 Solo Viendo',
  busca_producto: '🔍 Busca Producto',
  precio: '💲 Consulta de Precio',
  envio: '🚚 Envío/Entrega',
  ubicacion: '📍 Ubicación',
  factura: '🧾 Facturación',
  contratista: '🏗️ Contratista/Constructor',
};

const WEEKDAYS = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'];

// ── Types ──────────────────────────────────────────────────────────────────────

interface SessionAnalysis {
  sessionId: number;
  date: string;
  operator: string;
  active: boolean;
  numMessages: number;
  visitorMessages: string;
  intents: string;
  products: string;
  emails: string;
}

interface AnalysisResult {
  totalSessions: number;
  totalMessages: number;
  sessionsByMonth: Record<string, number>;
  sessionsByWeekday: Record<string, number>;
  sessionsByHour: Record<number, number>;
  intents: Record<string, number>;
  productsMentioned: Record<string, number>;
  emailsCaptured: string[];
  conversations: SessionAnalysis[];
}

// ── Task ───────────────────────────────────────────────────────────────────────

export class OdooChatAnalysisTask extends BaseTask {
  readonly name = 'odoo-chat-analysis';
  // Every Monday at 7:00 AM Pacific
  readonly cronExpression = '0 7 * * 1';

  async execute(): Promise<void> {
    const channelId = config.odoo.livechatChannelId;
    const reportsDir = path.resolve(config.odoo.reportsDir);
    fs.mkdirSync(reportsDir, { recursive: true });

    // ── 1. Fetch all livechat sessions ─────────────────────────────────────
    logger.info(CTX, `Fetching livechat sessions for channel ${channelId}...`);
    const sessions = await searchReadAll(
      'discuss.channel',
      [['livechat_channel_id', '=', channelId]],
      ['name', 'create_date', 'livechat_operator_id', 'anonymous_name',
       'country_id', 'message_ids', 'livechat_active'],
      { order: 'create_date desc' },
    );
    logger.info(CTX, `Fetched ${sessions.length} sessions`);

    // ── 2. Collect and fetch all messages ──────────────────────────────────
    const allMsgIds = new Set<number>();
    for (const s of sessions) {
      for (const id of s.message_ids as number[]) allMsgIds.add(id);
    }
    logger.info(CTX, `Fetching ${allMsgIds.size} messages...`);
    const allMessages = await readRecords(
      'mail.message',
      Array.from(allMsgIds),
      ['body', 'author_id', 'date', 'res_id', 'message_type'],
    );
    logger.info(CTX, `Fetched ${allMessages.length} messages`);

    // ── 3. Analyze ─────────────────────────────────────────────────────────
    const analysis = this.analyze(sessions, allMessages);

    // ── 4. Write CSV files ─────────────────────────────────────────────────
    this.writeCsv(
      path.join(reportsDir, 'chat_conversaciones_detalle.csv'),
      ['session_id', 'date', 'operator', 'active', 'num_messages', 'visitor_messages', 'intents', 'products', 'emails'],
      analysis.conversations.map(c => [
        c.sessionId, c.date, c.operator, c.active, c.numMessages,
        c.visitorMessages, c.intents, c.products, c.emails,
      ]),
    );

    this.writeCsv(
      path.join(reportsDir, 'chat_emails_capturados.csv'),
      ['email'],
      analysis.emailsCaptured.sort().map(e => [e]),
    );

    this.writeCsv(
      path.join(reportsDir, 'chat_metricas.csv'),
      ['metrica', 'valor'],
      [
        ['Total Sesiones', analysis.totalSessions],
        ['Total Mensajes', analysis.totalMessages],
        ['Emails Capturados', analysis.emailsCaptured.length],
        ['---', '---'],
        ...Object.entries(analysis.sessionsByMonth).map(([k, v]) => [`Mes ${k}`, v]),
        ['---', '---'],
        ...Object.entries(analysis.sessionsByWeekday).map(([k, v]) => [k, v]),
        ['---', '---'],
        ...Object.entries(analysis.intents).sort((a, b) => (b[1] as number) - (a[1] as number)).map(([k, v]) => [k, v]),
        ['---', '---'],
        ...Object.entries(analysis.productsMentioned).sort((a, b) => (b[1] as number) - (a[1] as number)).map(([k, v]) => [k, v]),
      ],
    );

    // ── 5. Write Markdown report ───────────────────────────────────────────
    const mdPath = path.join(reportsDir, 'REPORTE_EJECUTIVO_CHAT.md');
    fs.writeFileSync(mdPath, this.buildMarkdown(analysis), 'utf-8');
    logger.info(CTX, `Reports written to ${reportsDir}`);

    // ── 6. Send email summary ──────────────────────────────────────────────
    if (config.marketingEmails.length > 0) {
      const html = this.buildEmailHtml(analysis);
      await sendEmail({
        to: config.marketingEmails,
        subject: `📊 Reporte Semanal Chat proconsa.online — ${analysis.totalSessions} sesiones`,
        html,
        text: `Reporte semanal: ${analysis.totalSessions} sesiones, ${analysis.emailsCaptured.length} emails capturados`,
      });
      logger.info(CTX, `Email sent to ${config.marketingEmails.length} recipient(s)`);
    }
  }

  // ── Analysis logic ─────────────────────────────────────────────────────────

  private analyze(sessions: OdooRecord[], messages: OdooRecord[]): AnalysisResult {
    const msgsBySession = new Map<number, OdooRecord[]>();
    for (const m of messages) {
      const rid = m.res_id as number;
      if (!msgsBySession.has(rid)) msgsBySession.set(rid, []);
      msgsBySession.get(rid)!.push(m);
    }

    const sessionsByMonth: Record<string, number> = {};
    const sessionsByWeekday: Record<string, number> = {};
    const sessionsByHour: Record<number, number> = {};
    const intents: Record<string, number> = {};
    const productsMentioned: Record<string, number> = {};
    const emailsSet = new Set<string>();
    const conversations: SessionAnalysis[] = [];

    for (const session of sessions) {
      const dt = new Date(session.create_date as string);
      const monthKey = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, '0')}`;
      sessionsByMonth[monthKey] = (sessionsByMonth[monthKey] || 0) + 1;
      sessionsByWeekday[WEEKDAYS[dt.getUTCDay()]] = (sessionsByWeekday[WEEKDAYS[dt.getUTCDay()]] || 0) + 1;
      sessionsByHour[dt.getUTCHours()] = (sessionsByHour[dt.getUTCHours()] || 0) + 1;

      const msgs = (msgsBySession.get(session.id) || [])
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));

      const visitorTexts: string[] = [];
      const sessionIntents = new Set<string>();
      const sessionProducts = new Set<string>();
      const sessionEmails: string[] = [];

      for (const msg of msgs) {
        const text = stripHtml(msg.body as string);
        if (!text || text.includes('Reiniciando') || text.includes('abandonó')) continue;

        const authorId = msg.author_id as (false | [number, string]);
        const isVisitor = !authorId || (Array.isArray(authorId) && ![7, 8, 2].includes(authorId[0]));

        if (isVisitor) {
          visitorTexts.push(text);
          const lower = text.toLowerCase();

          const foundEmails = text.match(EMAIL_RE);
          if (foundEmails) {
            for (const e of foundEmails) {
              sessionEmails.push(e.toLowerCase());
              emailsSet.add(e.toLowerCase());
            }
          }

          for (const [key, re] of Object.entries(INTENT_PATTERNS)) {
            if (re.test(lower)) sessionIntents.add(key);
          }
          for (const [key, re] of Object.entries(PRODUCT_PATTERNS)) {
            if (re.test(lower)) sessionProducts.add(key);
          }
        }
      }

      for (const i of sessionIntents) intents[i] = (intents[i] || 0) + 1;
      for (const p of sessionProducts) productsMentioned[p] = (productsMentioned[p] || 0) + 1;

      const op = session.livechat_operator_id as (false | [number, string]);

      conversations.push({
        sessionId: session.id,
        date: session.create_date as string,
        operator: op ? op[1] : 'N/A',
        active: session.livechat_active as boolean,
        numMessages: msgs.length,
        visitorMessages: visitorTexts.slice(0, 5).join(' | '),
        intents: Array.from(sessionIntents).join(', ') || 'sin_clasificar',
        products: Array.from(sessionProducts).join(', ') || 'ninguno',
        emails: sessionEmails.join(', '),
      });
    }

    return {
      totalSessions: sessions.length,
      totalMessages: messages.length,
      sessionsByMonth,
      sessionsByWeekday,
      sessionsByHour,
      intents,
      productsMentioned,
      emailsCaptured: Array.from(emailsSet),
      conversations,
    };
  }

  // ── CSV writer ─────────────────────────────────────────────────────────────

  private writeCsv(filePath: string, headers: string[], rows: unknown[][]): void {
    const escape = (v: unknown): string => {
      const s = String(v ?? '');
      return s.includes(',') || s.includes('"') || s.includes('\n')
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const lines = [headers.map(escape).join(',')];
    for (const row of rows) lines.push(row.map(escape).join(','));
    fs.writeFileSync(filePath, lines.join('\n'), 'utf-8');
    logger.info(CTX, `CSV written: ${filePath} (${rows.length} rows)`);
  }

  // ── Markdown report builder ────────────────────────────────────────────────

  private buildMarkdown(a: AnalysisResult): string {
    const now = new Date();
    const months = Object.keys(a.sessionsByMonth).sort();
    let md = `# REPORTE EJECUTIVO - Análisis de Chat proconsa.online\n\n`;
    md += `**Fecha de generación:** ${now.toISOString().slice(0, 16).replace('T', ' ')}\n\n`;
    md += `**Período analizado:** ${months[0]} a ${months[months.length - 1]}\n\n---\n\n`;

    md += `## 1. RESUMEN EJECUTIVO\n\n| Métrica | Valor |\n|---|---|\n`;
    md += `| Total de sesiones de chat | **${a.totalSessions.toLocaleString()}** |\n`;
    md += `| Total de mensajes | **${a.totalMessages.toLocaleString()}** |\n`;
    md += `| Promedio mensajes por sesión | **${(a.totalMessages / Math.max(a.totalSessions, 1)).toFixed(1)}** |\n`;
    md += `| Emails capturados (únicos) | **${a.emailsCaptured.length}** |\n`;
    md += `| Tasa de captura de email | **${(a.emailsCaptured.length / Math.max(a.totalSessions, 1) * 100).toFixed(1)}%** |\n\n`;

    md += `## 2. TENDENCIA MENSUAL\n\n| Mes | Sesiones | Tendencia |\n|---|---|---|\n`;
    let prev = 0;
    for (const m of months) {
      const c = a.sessionsByMonth[m];
      let trend = '';
      if (prev > 0) {
        const pct = ((c - prev) / prev) * 100;
        trend = pct > 0 ? `📈 +${pct.toFixed(0)}%` : `📉 ${pct.toFixed(0)}%`;
      }
      md += `| ${m} | ${c} | ${trend} |\n`;
      prev = c;
    }

    md += `\n## 3. INTENCIONES DE LOS VISITANTES\n\n| Intención | Sesiones | % |\n|---|---|---|\n`;
    const sortedIntents = Object.entries(a.intents).sort((x, y) => y[1] - x[1]);
    for (const [key, count] of sortedIntents) {
      md += `| ${INTENT_LABELS[key] || key} | ${count} | ${(count / Math.max(a.totalSessions, 1) * 100).toFixed(1)}% |\n`;
    }

    md += `\n## 4. PRODUCTOS MÁS MENCIONADOS\n\n| Categoría | Menciones | % |\n|---|---|---|\n`;
    const sortedProducts = Object.entries(a.productsMentioned).sort((x, y) => y[1] - x[1]);
    for (const [key, count] of sortedProducts) {
      md += `| ${key} | ${count} | ${(count / Math.max(a.totalSessions, 1) * 100).toFixed(1)}% |\n`;
    }

    md += `\n---\n\n### Archivos generados:\n`;
    md += `- \`chat_conversaciones_detalle.csv\`\n`;
    md += `- \`chat_emails_capturados.csv\`\n`;
    md += `- \`chat_metricas.csv\`\n`;
    md += `- \`REPORTE_EJECUTIVO_CHAT.md\`\n`;

    return md;
  }

  // ── HTML email builder ─────────────────────────────────────────────────────

  private buildEmailHtml(a: AnalysisResult): string {
    const dateStr = new Date().toLocaleDateString('es-MX', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
    });

    const intentRows = Object.entries(a.intents)
      .sort((x, y) => y[1] - x[1])
      .slice(0, 8)
      .map(([k, v]) => `<tr><td style="padding:6px 12px;">${INTENT_LABELS[k] || k}</td><td style="padding:6px 12px;text-align:right;">${v}</td></tr>`)
      .join('');

    const productRows = Object.entries(a.productsMentioned)
      .sort((x, y) => y[1] - x[1])
      .slice(0, 8)
      .map(([k, v]) => `<tr><td style="padding:6px 12px;">${k}</td><td style="padding:6px 12px;text-align:right;">${v}</td></tr>`)
      .join('');

    return `<!DOCTYPE html><html><head><meta charset="utf-8"/></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#333;margin:0;padding:20px;background:#f5f5f5;">
      <div style="max-width:700px;margin:0 auto;background:white;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <div style="background:#1a1a2e;color:white;padding:24px 32px;">
          <h1 style="margin:0;font-size:20px;">📊 Reporte Semanal — Chat proconsa.online</h1>
          <p style="margin:8px 0 0;opacity:0.8;font-size:14px;">${dateStr}</p>
        </div>
        <div style="padding:24px 32px;">
          <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:20px;">
            <tr><td style="padding:8px 0;"><strong>Total sesiones</strong></td><td style="text-align:right;">${a.totalSessions.toLocaleString()}</td></tr>
            <tr><td style="padding:8px 0;"><strong>Total mensajes</strong></td><td style="text-align:right;">${a.totalMessages.toLocaleString()}</td></tr>
            <tr><td style="padding:8px 0;"><strong>Emails capturados</strong></td><td style="text-align:right;">${a.emailsCaptured.length}</td></tr>
            <tr><td style="padding:8px 0;"><strong>Tasa de captura</strong></td><td style="text-align:right;">${(a.emailsCaptured.length / Math.max(a.totalSessions, 1) * 100).toFixed(1)}%</td></tr>
          </table>
          <h3 style="margin:16px 0 8px;">Top Intenciones</h3>
          <table style="width:100%;border-collapse:collapse;font-size:13px;background:#f9f9f9;border-radius:4px;">${intentRows}</table>
          <h3 style="margin:16px 0 8px;">Top Productos</h3>
          <table style="width:100%;border-collapse:collapse;font-size:13px;background:#f9f9f9;border-radius:4px;">${productRows}</table>
          <p style="font-size:13px;color:#666;margin-top:20px;">Los reportes completos en CSV y Markdown se guardaron en el servidor.</p>
        </div>
        <div style="background:#f9f9f9;padding:12px 32px;text-align:center;font-size:12px;color:#999;">
          Generado automáticamente — Sistema de Tareas Proconsa
        </div>
      </div>
    </body></html>`;
  }
}
