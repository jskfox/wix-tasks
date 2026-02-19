import { BaseTask } from './base-task';
import { config, getEmailsForTask } from '../config';
import { logger } from '../utils/logger';
import { sendEmail } from '../services/email';
import { searchReadAll, searchRead, readRecords, OdooRecord } from '../services/odoo';
import * as fs from 'fs';
import * as path from 'path';

const CTX = 'OdooChatLeads';

// ── Helpers ────────────────────────────────────────────────────────────────────

function stripHtml(html: string): string {
  return html.replace(/<[^>]*>/g, '').trim();
}

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

const INTENT_PATTERNS: Record<string, RegExp> = {
  cotizacion_mayoreo: /cotizaci[oó]n.*mayoreo|mayoreo|precio.*mayoreo/i,
  talleres_clinicas: /taller|cl[ií]nica|capacitaci[oó]n|curso|inscrib/i,
  problema_sitio: /problema.*sitio|no.*funciona|error|no.*carga|no.*puedo/i,
  solo_viendo: /solo.*viendo|nada.*gracias|no.*gracias|solo.*mirando/i,
  busca_producto: /busco|necesito|quiero|donde.*encuentro|tienen/i,
  precio: /precio|costo|cu[aá]nto.*cuesta|cu[aá]nto.*vale/i,
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

// ── Types ──────────────────────────────────────────────────────────────────────

interface Lead {
  sessionId: number;
  fechaChat: string;
  diasTranscurridos: number;
  priorityNum: number;
  prioridad: string;
  email: string;
  tipoCliente: string;
  intenciones: string;
  productosSolicitados: string;
  resumenVisitante: string;
  sugerenciaAbordaje: string;
  numMensajes: number;
  conversacionCompleta: string;
  nombreOdoo: string;
  telefono: string;
  celular: string;
  ciudad: string;
  estado: string;
  empresa: string;
  puesto: string;
  esClienteExistente: boolean;
  ordenesVenta: number;
  totalFacturado: number;
}

// ── Classification ─────────────────────────────────────────────────────────────

function classifyClientType(intents: Set<string>, products: Set<string>, text: string): string {
  const lower = text.toLowerCase();
  if (intents.has('contratista') || /contratista|constructor|obra grande|proyecto|edificio|residencial|fraccionamiento/.test(lower)) {
    return 'Contratista/Constructor';
  }
  if (intents.has('cotizacion_mayoreo') && products.size >= 2) return 'Mayorista/Distribuidor';
  if (intents.has('cotizacion_mayoreo')) return 'Comprador de Volumen';
  if (/empresa|negocio|compañ[ií]a|sa de cv|s\.a\.|spr|s\.?r\.?l/.test(lower)) return 'Empresa';
  if (/mi casa|remodelaci[oó]n|arreglar|reparar|ba[nñ]o|cocina|cuarto/.test(lower)) return 'Particular/Remodelación';
  if (intents.has('talleres_clinicas')) return 'Profesional en Formación';
  if (intents.has('factura')) return 'Cliente con Facturación';
  return 'Prospecto General';
}

function calculatePriority(daysAgo: number, hasEmail: boolean, intents: Set<string>, products: Set<string>, numMsgs: number): [number, string] {
  let score = 0;
  if (daysAgo <= 3) score += 40;
  else if (daysAgo <= 7) score += 30;
  else if (daysAgo <= 14) score += 20;
  else if (daysAgo <= 30) score += 10;

  if (hasEmail) score += 15;
  if (intents.has('cotizacion_mayoreo')) score += 20;
  if (intents.has('contratista')) score += 15;
  if (intents.has('busca_producto')) score += 10;
  if (intents.has('precio')) score += 10;
  score += Math.min(products.size * 5, 15);
  if (numMsgs >= 8) score += 10;
  else if (numMsgs >= 5) score += 5;
  if (intents.has('solo_viendo')) score -= 15;
  if (intents.has('problema_sitio') && intents.size === 1) score -= 10;

  if (score >= 70) return [1, '🔴 MÁXIMA'];
  if (score >= 50) return [2, '🟠 ALTA'];
  if (score >= 35) return [3, '🟡 MEDIA'];
  if (score >= 20) return [4, '🔵 BAJA'];
  return [5, '⚪ MUY BAJA'];
}

function suggestApproach(intents: Set<string>, products: Set<string>, clientType: string): string {
  const suggestions: string[] = [];
  const prodList = Array.from(products).join(', ');

  if (intents.has('cotizacion_mayoreo')) {
    suggestions.push(products.size > 0
      ? `Enviar cotización personalizada de: ${prodList}`
      : 'Contactar para conocer necesidades de mayoreo y enviar catálogo');
  }
  if (intents.has('contratista') || clientType === 'Contratista/Constructor') {
    suggestions.push('Ofrecer programa de descuentos para contratistas y línea de crédito');
  }
  if (clientType === 'Mayorista/Distribuidor') {
    suggestions.push('Presentar programa de distribución y precios especiales por volumen');
  }
  if (intents.has('talleres_clinicas')) {
    suggestions.push('Invitar a próximos talleres y ofrecer descuento post-capacitación');
  }
  if (intents.has('precio')) {
    suggestions.push('Enviar lista de precios actualizada de los productos consultados');
  }
  if (intents.has('factura')) {
    suggestions.push('Ya es cliente - verificar historial y ofrecer recompra con beneficios');
  }
  if (intents.has('problema_sitio')) {
    suggestions.push('Disculparse por inconvenientes técnicos y ofrecer atención directa');
  }
  if (suggestions.length === 0) {
    suggestions.push(products.size > 0
      ? `Contactar ofreciendo información sobre: ${prodList}`
      : 'Enviar catálogo general y promociones vigentes');
  }
  return suggestions.join(' | ');
}

// ── Task ───────────────────────────────────────────────────────────────────────

export class OdooChatLeadsTask extends BaseTask {
  readonly name = 'odoo-chat-leads-report';
  readonly description = 'Genera reporte diario de leads potenciales desde el livechat de Odoo. Prioriza por intención de compra y envía resumen por correo al equipo de marketing.';
  // Every day at 8:00 AM Pacific
  readonly cronExpression = '0 8 * * *';

  async execute(): Promise<void> {
    const channelId = config.odoo.livechatChannelId;
    const reportsDir = path.resolve(config.odoo.reportsDir);
    fs.mkdirSync(reportsDir, { recursive: true });
    const now = new Date();

    // ── 1. Fetch sessions ──────────────────────────────────────────────────
    logger.info(CTX, `Fetching livechat sessions for channel ${channelId}...`);
    const sessions = await searchReadAll(
      'discuss.channel',
      [['livechat_channel_id', '=', channelId]],
      ['name', 'create_date', 'livechat_operator_id', 'anonymous_name',
       'country_id', 'message_ids', 'livechat_active'],
      { order: 'create_date desc' },
    );
    logger.info(CTX, `Fetched ${sessions.length} sessions`);

    // ── 2. Fetch messages ──────────────────────────────────────────────────
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

    const msgsBySession = new Map<number, OdooRecord[]>();
    for (const m of allMessages) {
      const rid = m.res_id as number;
      if (!msgsBySession.has(rid)) msgsBySession.set(rid, []);
      msgsBySession.get(rid)!.push(m);
    }

    // ── 3. Extract leads ───────────────────────────────────────────────────
    logger.info(CTX, 'Extracting leads...');
    const leads: Lead[] = [];
    const allLeadEmails = new Set<string>();

    for (const session of sessions) {
      const createDt = new Date(session.create_date as string);
      const daysAgo = Math.floor((now.getTime() - createDt.getTime()) / 86400000);

      const msgs = (msgsBySession.get(session.id) || [])
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));

      const visitorTexts: string[] = [];
      const sessionEmails: string[] = [];
      const sessionProducts = new Set<string>();
      const sessionIntents = new Set<string>();
      const fullConversation: string[] = [];

      for (const msg of msgs) {
        const text = stripHtml(msg.body as string);
        if (!text || text.includes('Reiniciando') || text.includes('abandonó')) continue;

        const authorId = msg.author_id as (false | [number, string]);
        const isVisitor = !authorId || (Array.isArray(authorId) && ![7, 8, 2].includes(authorId[0]));

        if (isVisitor) {
          visitorTexts.push(text);
          const lower = text.toLowerCase();
          const foundEmails = text.match(EMAIL_RE);
          if (foundEmails) foundEmails.forEach(e => sessionEmails.push(e.toLowerCase()));

          for (const [key, re] of Object.entries(INTENT_PATTERNS)) {
            if (re.test(lower)) sessionIntents.add(key);
          }
          for (const [key, re] of Object.entries(PRODUCT_PATTERNS)) {
            if (re.test(lower)) sessionProducts.add(key);
          }
          fullConversation.push(`[Visitante]: ${text}`);
        } else {
          const name = Array.isArray(authorId) ? authorId[1] : 'Bot';
          fullConversation.push(`[${name}]: ${text}`);
        }
      }

      if (visitorTexts.length === 0) continue;

      const visitorJoined = visitorTexts.join(' ');
      const clientType = classifyClientType(sessionIntents, sessionProducts, visitorJoined);
      const hasEmail = sessionEmails.length > 0;
      const [priorityNum, priorityLabel] = calculatePriority(daysAgo, hasEmail, sessionIntents, sessionProducts, msgs.length);
      const approach = suggestApproach(sessionIntents, sessionProducts, clientType);

      const primaryEmail = sessionEmails[0] || '';
      if (primaryEmail) allLeadEmails.add(primaryEmail);

      leads.push({
        sessionId: session.id,
        fechaChat: session.create_date as string,
        diasTranscurridos: daysAgo,
        priorityNum,
        prioridad: priorityLabel,
        email: primaryEmail,
        tipoCliente: clientType,
        intenciones: Array.from(sessionIntents).sort().join(', ') || 'sin_clasificar',
        productosSolicitados: Array.from(sessionProducts).sort().join(', ') || 'No especificado',
        resumenVisitante: visitorTexts.slice(0, 6).join(' | '),
        sugerenciaAbordaje: approach,
        numMensajes: msgs.length,
        conversacionCompleta: fullConversation.join('\n'),
        nombreOdoo: '', telefono: '', celular: '', ciudad: '', estado: '',
        empresa: '', puesto: '', esClienteExistente: false, ordenesVenta: 0, totalFacturado: 0,
      });
    }

    logger.info(CTX, `Leads extracted: ${leads.length} (${allLeadEmails.size} with email)`);

    // ── 4. Enrich from Odoo contacts ───────────────────────────────────────
    logger.info(CTX, 'Enriching leads from Odoo contacts...');
    const enriched = new Map<string, OdooRecord | null>();
    const emailArray = Array.from(allLeadEmails);

    for (const email of emailArray) {
      const partners = await searchRead(
        'res.partner',
        [['email', 'ilike', email]],
        ['name', 'phone', 'mobile', 'city', 'state_id', 'company_name',
         'function', 'sale_order_count', 'total_invoiced'],
        { limit: 1 },
      );
      enriched.set(email, partners[0] || null);
    }

    for (const lead of leads) {
      if (!lead.email) continue;
      const p = enriched.get(lead.email);
      if (!p) continue;
      lead.nombreOdoo = (p.name as string) || '';
      lead.telefono = (p.phone as string) || '';
      lead.celular = (p.mobile as string) || '';
      lead.ciudad = (p.city as string) || '';
      const stateId = p.state_id as (false | [number, string]);
      lead.estado = stateId ? stateId[1] : '';
      lead.empresa = (p.company_name as string) || '';
      lead.puesto = (p.function as string) || '';
      lead.ordenesVenta = (p.sale_order_count as number) || 0;
      lead.totalFacturado = (p.total_invoiced as number) || 0;
      lead.esClienteExistente = lead.ordenesVenta > 0 || lead.totalFacturado > 0;
    }

    // ── 5. Sort by priority + recency ──────────────────────────────────────
    leads.sort((a, b) => a.priorityNum - b.priorityNum || a.diasTranscurridos - b.diasTranscurridos);

    const leadsWithEmail = leads.filter(l => l.email);
    const leadsNoEmail = leads.filter(l => !l.email && l.priorityNum <= 3);

    // ── 6. Write CSVs ─────────────────────────────────────────────────────
    this.writeCsv(
      path.join(reportsDir, 'LEADS_SEGUIMIENTO_MARKETING.csv'),
      ['prioridad', 'fecha_chat', 'dias_transcurridos', 'email', 'nombre_odoo',
       'telefono', 'celular', 'tipo_cliente', 'productos_solicitados', 'intenciones',
       'sugerencia_abordaje', 'resumen_visitante', 'ciudad', 'estado', 'empresa',
       'puesto', 'es_cliente_existente', 'ordenes_venta', 'total_facturado',
       'num_mensajes', 'session_id'],
      leadsWithEmail.map(l => [
        l.prioridad, l.fechaChat, l.diasTranscurridos, l.email, l.nombreOdoo,
        l.telefono, l.celular, l.tipoCliente, l.productosSolicitados, l.intenciones,
        l.sugerenciaAbordaje, l.resumenVisitante, l.ciudad, l.estado, l.empresa,
        l.puesto, l.esClienteExistente, l.ordenesVenta, l.totalFacturado,
        l.numMensajes, l.sessionId,
      ]),
    );

    this.writeCsv(
      path.join(reportsDir, 'LEADS_CONVERSACIONES_COMPLETAS.csv'),
      ['prioridad', 'fecha_chat', 'email', 'nombre_odoo', 'tipo_cliente',
       'productos_solicitados', 'conversacion_completa'],
      leadsWithEmail.map(l => [
        l.prioridad, l.fechaChat, l.email, l.nombreOdoo, l.tipoCliente,
        l.productosSolicitados, l.conversacionCompleta,
      ]),
    );

    this.writeCsv(
      path.join(reportsDir, 'LEADS_SIN_EMAIL_OPORTUNIDADES.csv'),
      ['prioridad', 'fecha_chat', 'dias_transcurridos', 'tipo_cliente',
       'productos_solicitados', 'intenciones', 'resumen_visitante', 'num_mensajes'],
      leadsNoEmail.map(l => [
        l.prioridad, l.fechaChat, l.diasTranscurridos, l.tipoCliente,
        l.productosSolicitados, l.intenciones, l.resumenVisitante, l.numMensajes,
      ]),
    );

    // ── 7. Write Markdown report ───────────────────────────────────────────
    const mdPath = path.join(reportsDir, 'REPORTE_SEGUIMIENTO_MARKETING.md');
    fs.writeFileSync(mdPath, this.buildMarkdown(leadsWithEmail, leadsNoEmail, now), 'utf-8');
    logger.info(CTX, `Reports written to ${reportsDir}`);

    // ── 8. Email summary to marketing ──────────────────────────────────────
    const recipients = getEmailsForTask('chatLeads');
    if (recipients.length > 0) {
      const priorityCounts = this.countByPriority(leadsWithEmail);
      const html = this.buildEmailHtml(leadsWithEmail, priorityCounts, now);
      const attachments = [
        {
          filename: 'LEADS_SEGUIMIENTO_MARKETING.csv',
          path: path.join(reportsDir, 'LEADS_SEGUIMIENTO_MARKETING.csv'),
          contentType: 'text/csv',
        },
        {
          filename: 'LEADS_CONVERSACIONES_COMPLETAS.csv',
          path: path.join(reportsDir, 'LEADS_CONVERSACIONES_COMPLETAS.csv'),
          contentType: 'text/csv',
        },
        {
          filename: 'LEADS_SIN_EMAIL_OPORTUNIDADES.csv',
          path: path.join(reportsDir, 'LEADS_SIN_EMAIL_OPORTUNIDADES.csv'),
          contentType: 'text/csv',
        },
        {
          filename: 'REPORTE_SEGUIMIENTO_MARKETING.md',
          path: mdPath,
          contentType: 'text/markdown',
        },
      ];
      await sendEmail({
        to: recipients,
        subject: `🎯 Leads del Chat — ${leadsWithEmail.length} prospectos (${priorityCounts['🔴 MÁXIMA'] || 0} urgentes)`,
        html,
        text: `Reporte de leads: ${leadsWithEmail.length} con email, ${priorityCounts['🔴 MÁXIMA'] || 0} prioridad máxima`,
        attachments,
      });
      logger.info(CTX, `Email sent to ${recipients.length} recipient(s)`);
    } else {
      logger.warn(CTX, 'No email recipients configured (CHAT_LEADS_EMAILS or MARKETING_EMAILS) — skipping email send');
    }
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private countByPriority(leads: Lead[]): Record<string, number> {
    const counts: Record<string, number> = {};
    for (const l of leads) counts[l.prioridad] = (counts[l.prioridad] || 0) + 1;
    return counts;
  }

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
    logger.info(CTX, `CSV: ${filePath} (${rows.length} rows)`);
  }

  // ── Markdown report ────────────────────────────────────────────────────────

  private buildMarkdown(leadsWithEmail: Lead[], leadsNoEmail: Lead[], now: Date): string {
    const pCounts = this.countByPriority(leadsWithEmail);
    const typeCounts: Record<string, number> = {};
    let existingClients = 0;
    for (const l of leadsWithEmail) {
      typeCounts[l.tipoCliente] = (typeCounts[l.tipoCliente] || 0) + 1;
      if (l.esClienteExistente) existingClients++;
    }

    let md = `# REPORTE DE SEGUIMIENTO DE LEADS — Equipo de Marketing\n`;
    md += `## Chat proconsa.online\n\n`;
    md += `**Generado:** ${now.toISOString().slice(0, 16).replace('T', ' ')} UTC\n\n---\n\n`;

    md += `## RESUMEN\n\n| Métrica | Valor |\n|---|---|\n`;
    md += `| Leads con email | **${leadsWithEmail.length}** |\n`;
    md += `| Clientes existentes | **${existingClients}** |\n`;
    md += `| Prospectos nuevos | **${leadsWithEmail.length - existingClients}** |\n`;
    md += `| Oportunidades sin email | **${leadsNoEmail.length}** |\n\n`;

    md += `## PRIORIDAD\n\n| Prioridad | Cantidad |\n|---|---|\n`;
    for (const p of ['🔴 MÁXIMA', '🟠 ALTA', '🟡 MEDIA', '🔵 BAJA', '⚪ MUY BAJA']) {
      md += `| ${p} | ${pCounts[p] || 0} |\n`;
    }

    md += `\n## TOP LEADS — PRIORIDAD MÁXIMA\n\n`;
    const topLeads = leadsWithEmail.filter(l => l.priorityNum === 1).slice(0, 30);
    for (let i = 0; i < topLeads.length; i++) {
      const l = topLeads[i];
      md += `### Lead #${i + 1}\n| Campo | Dato |\n|---|---|\n`;
      md += `| **Email** | ${l.email} |\n`;
      if (l.nombreOdoo) md += `| **Nombre** | ${l.nombreOdoo} |\n`;
      if (l.telefono) md += `| **Teléfono** | ${l.telefono} |\n`;
      if (l.celular) md += `| **Celular** | ${l.celular} |\n`;
      md += `| **Fecha** | ${l.fechaChat} (hace ${l.diasTranscurridos} días) |\n`;
      md += `| **Tipo** | ${l.tipoCliente} |\n`;
      md += `| **Productos** | ${l.productosSolicitados} |\n`;
      md += `| **💡 Abordaje** | ${l.sugerenciaAbordaje} |\n`;
      md += `| **Contexto** | ${l.resumenVisitante.slice(0, 300)} |\n\n`;
    }

    md += `---\n\n### Archivos generados:\n`;
    md += `- \`LEADS_SEGUIMIENTO_MARKETING.csv\`\n`;
    md += `- \`LEADS_CONVERSACIONES_COMPLETAS.csv\`\n`;
    md += `- \`LEADS_SIN_EMAIL_OPORTUNIDADES.csv\`\n`;
    md += `- \`REPORTE_SEGUIMIENTO_MARKETING.md\`\n`;

    return md;
  }

  // ── HTML email ─────────────────────────────────────────────────────────────

  private buildEmailHtml(leads: Lead[], pCounts: Record<string, number>, now: Date): string {
    const dateStr = now.toLocaleDateString('es-MX', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
    });

    const topLeads = leads.filter(l => l.priorityNum <= 2).slice(0, 15);

    const leadRows = topLeads.map((l, i) => `
      <tr style="border-bottom:1px solid #eee;">
        <td style="padding:8px;font-size:13px;">${i + 1}</td>
        <td style="padding:8px;font-size:13px;">
          <strong>${l.nombreOdoo || l.email}</strong><br/>
          <span style="color:#666;">${l.email}</span>
          ${l.telefono ? `<br/>📞 ${l.telefono}` : ''}
        </td>
        <td style="padding:8px;font-size:13px;">${l.tipoCliente}</td>
        <td style="padding:8px;font-size:13px;">${l.productosSolicitados}</td>
        <td style="padding:8px;font-size:13px;">${l.prioridad}</td>
      </tr>`).join('');

    return `<!DOCTYPE html><html><head><meta charset="utf-8"/></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#333;margin:0;padding:20px;background:#f5f5f5;">
      <div style="max-width:800px;margin:0 auto;background:white;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
        <div style="background:#1a1a2e;color:white;padding:24px 32px;">
          <h1 style="margin:0;font-size:20px;">🎯 Reporte de Leads — Chat proconsa.online</h1>
          <p style="margin:8px 0 0;opacity:0.8;font-size:14px;">${dateStr}</p>
        </div>
        <div style="padding:24px 32px;">
          <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:20px;">
            <tr><td style="padding:6px 0;"><strong>Total leads con email</strong></td><td style="text-align:right;">${leads.length}</td></tr>
            <tr><td style="padding:6px 0;">🔴 Prioridad MÁXIMA</td><td style="text-align:right;font-weight:bold;color:#e53e3e;">${pCounts['🔴 MÁXIMA'] || 0}</td></tr>
            <tr><td style="padding:6px 0;">🟠 Prioridad ALTA</td><td style="text-align:right;font-weight:bold;color:#dd6b20;">${pCounts['🟠 ALTA'] || 0}</td></tr>
            <tr><td style="padding:6px 0;">🟡 Prioridad MEDIA</td><td style="text-align:right;">${pCounts['🟡 MEDIA'] || 0}</td></tr>
          </table>

          <h3 style="margin:16px 0 8px;">Leads Prioritarios — Contactar Hoy</h3>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <thead><tr style="background:#f9f9f9;text-align:left;">
              <th style="padding:8px;">#</th>
              <th style="padding:8px;">Contacto</th>
              <th style="padding:8px;">Tipo</th>
              <th style="padding:8px;">Productos</th>
              <th style="padding:8px;">Prioridad</th>
            </tr></thead>
            <tbody>${leadRows}</tbody>
          </table>

          <p style="font-size:13px;color:#666;margin-top:20px;">
            El reporte completo con sugerencias de abordaje, conversaciones y CSV descargable
            se guardó en el servidor. Consulta <code>LEADS_SEGUIMIENTO_MARKETING.csv</code> para la lista completa.
          </p>
        </div>
        <div style="background:#f9f9f9;padding:12px 32px;text-align:center;font-size:12px;color:#999;">
          Generado automáticamente — Sistema de Tareas Proconsa
        </div>
      </div>
    </body></html>`;
  }
}
