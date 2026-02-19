import { BaseTask } from './base-task';
import { config, getEmailsForTask } from '../config';
import { logger } from '../utils/logger';
import { queryAbandonedCheckouts, AbandonedCheckout } from '../services/wix-api';
import { sendEmail } from '../services/email';

const CTX = 'AbandonedCarts';

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Get start-of-day ISO string in the configured timezone (PST/PDT) */
function getStartOfDayISO(tz: string): string {
  // Format current date in the target timezone to extract date parts
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());

  const year = parts.find(p => p.type === 'year')!.value;
  const month = parts.find(p => p.type === 'month')!.value;
  const day = parts.find(p => p.type === 'day')!.value;

  // Build a date string at midnight in the target timezone
  // Use Intl to resolve the UTC offset for that specific date
  const midnightLocal = new Date(`${year}-${month}-${day}T00:00:00`);
  const utcStr = midnightLocal.toLocaleString('en-US', { timeZone: tz, hour12: false });
  const utcDate = new Date(utcStr);
  const offsetMs = midnightLocal.getTime() - utcDate.getTime();
  const startUtc = new Date(midnightLocal.getTime() + offsetMs);
  return startUtc.toISOString();
}

function escHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function formatTimePST(iso: string, tz: string): string {
  return new Date(iso).toLocaleString('es-MX', { timeZone: tz, hour12: true });
}

// ═════════════════════════════════════════════════════════════════════════════
// TASK
// ═════════════════════════════════════════════════════════════════════════════

export class AbandonedCartsTask extends BaseTask {
  readonly name = 'abandoned-carts-report';
  readonly description = 'Genera y envía por correo un reporte diario de carritos abandonados en la tienda Wix. Incluye detalles del cliente, productos, montos y archivo CSV adjunto para seguimiento de telemarketing.';
  // Every day at 10:00 PM Pacific
  readonly cronExpression = '0 22 * * *';

  async execute(): Promise<void> {
    const tz = config.timezone;
    logger.info(CTX, 'Fetching abandoned checkouts for today...');

    // ── 1. Build date filter for today (server timezone) ───────────────────
    // Wix ecom v1 filter: only $gte on createdDate (matching working n8n query)
    const todayStartISO = getStartOfDayISO(tz);
    logger.info(CTX, `  Filter: createdDate >= ${todayStartISO} (tz=${tz})`);

    const filter = {
      createdDate: {
        $gte: todayStartISO,
      },
    };

    // ── 2. Query Wix for abandoned checkouts ───────────────────────────────
    const checkouts = await queryAbandonedCheckouts(filter);

    if (checkouts.length === 0) {
      logger.info(CTX, 'No abandoned checkouts found for today');
      return;
    }

    // Filter only ABANDONED status (exclude RECOVERED)
    const abandoned = checkouts.filter(c => c.status !== 'RECOVERED');
    logger.info(CTX, `Found ${checkouts.length} checkout(s), ${abandoned.length} still abandoned`);

    if (abandoned.length === 0) {
      logger.info(CTX, 'All checkouts were recovered — no report needed');
      return;
    }

    // ── 3. Build the HTML email + CSV attachment ───────────────────────────
    const now = new Date();
    const dateStr = now.toLocaleDateString('es-MX', {
      timeZone: tz,
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
    const dateShort = now.toLocaleDateString('es-MX', { timeZone: tz });

    const html = this.buildEmailHtml(abandoned, dateStr, tz);
    const csv = this.buildCsv(abandoned, tz);

    // ── 4. Send via SMTP ───────────────────────────────────────────────────
    const recipients = getEmailsForTask('abandonedCarts');
    if (recipients.length === 0) {
      logger.warn(CTX, 'No email recipients configured (ABANDONED_CARTS_EMAILS or MARKETING_EMAILS) — skipping email send');
      return;
    }

    await sendEmail({
      to: recipients,
      subject: `🛒 Carritos Abandonados del ${dateShort} — ${abandoned.length} pendiente(s)`,
      html,
      text: `Reporte de ${abandoned.length} carrito(s) abandonado(s) — ${dateShort}. Ver archivo CSV adjunto para seguimiento.`,
      attachments: [
        {
          filename: `carritos-abandonados-${dateShort.replace(/\//g, '-')}.csv`,
          content: csv,
          contentType: 'text/csv; charset=utf-8',
        },
      ],
    });

    logger.info(CTX, `Report sent to ${recipients.length} recipient(s) with CSV attachment`);
  }

  // ── CSV for telemarketing team ─────────────────────────────────────────────

  private buildCsv(checkouts: AbandonedCheckout[], tz: string): string {
    const BOM = '\uFEFF'; // UTF-8 BOM for Excel compatibility
    const header = ['#', 'Fecha/Hora', 'Nombre', 'Email', 'Teléfono', 'Empresa', 'Productos (SKU)', 'Total', 'Estatus', 'Link Checkout'];

    const rows = checkouts.map((c, idx) => {
      const contact = c.contactDetails;
      const name = [contact?.firstName, contact?.lastName].filter(Boolean).join(' ') || '';
      const email = c.buyerInfo?.email || '';
      const phone = contact?.phone || '';
      const company = contact?.company || '';
      const time = formatTimePST(c.createdDate, tz);
      const products = (c.lineItems || [])
        .map(li => {
          const pName = li.productName?.original || 'Producto';
          const sku = li.physicalProperties?.sku ? ` [${li.physicalProperties.sku}]` : '';
          return `${pName}${sku} x${li.quantity || 1}`;
        })
        .join(' | ');
      const total = c.totalPrice?.formattedAmount || c.totalPrice?.amount || '';
      const status = c.status || 'ABANDONED';
      const url = c.checkoutUrl || '';

      return [idx + 1, time, name, email, phone, company, products, total, status, url]
        .map(v => `"${String(v).replace(/"/g, '""')}"`)
        .join(',');
    });

    return BOM + header.join(',') + '\n' + rows.join('\n');
  }

  // ── HTML email ─────────────────────────────────────────────────────────────

  private buildEmailHtml(checkouts: AbandonedCheckout[], dateStr: string, tz: string): string {
    // Summary stats
    const totalAmount = checkouts.reduce((sum, c) => {
      return sum + parseFloat(c.totalPrice?.amount || '0');
    }, 0);
    const currency = checkouts[0]?.currency || 'MXN';

    const rows = checkouts.map((c, idx) => {
      const contact = c.contactDetails;
      const name = escHtml([contact?.firstName, contact?.lastName].filter(Boolean).join(' ') || 'Sin nombre');
      const email = escHtml(c.buyerInfo?.email || 'Sin email');
      const phone = escHtml(contact?.phone || 'Sin teléfono');
      const company = contact?.company ? `<br/>🏢 ${escHtml(contact.company)}` : '';
      const checkoutUrl = c.checkoutUrl || '#';
      const total = c.totalPrice?.formattedAmount || c.totalPrice?.amount || '—';
      const time = formatTimePST(c.createdDate, tz);

      const itemsList = (c.lineItems || [])
        .map(li => {
          const pName = escHtml(li.productName?.original || 'Producto');
          const sku = li.physicalProperties?.sku ? `<span style="color: #999; font-size: 11px;"> [SKU: ${escHtml(li.physicalProperties.sku)}]</span>` : '';
          const qty = li.quantity || 1;
          const price = li.price?.formattedAmount || li.price?.amount || '—';
          return `<li style="margin-bottom: 4px;">${pName}${sku} × ${qty} — ${price}</li>`;
        })
        .join('');

      const statusColor = c.status === 'RECOVERED' ? '#22c55e' : '#ef4444';
      const statusLabel = c.status === 'RECOVERED' ? 'Recuperado' : 'Abandonado';

      return `
        <tr style="border-bottom: 1px solid #e0e0e0;">
          <td style="padding: 12px; vertical-align: top; color: #999;">${idx + 1}</td>
          <td style="padding: 12px; vertical-align: top;">
            <strong>${name}</strong>${company}<br/>
            <span style="color: #555;">📧 <a href="mailto:${email}" style="color: #0070f3; text-decoration: none;">${email}</a></span><br/>
            <span style="color: #555;">📞 <a href="tel:${phone}" style="color: #0070f3; text-decoration: none;">${phone}</a></span><br/>
            <span style="color: #999; font-size: 12px;">🕐 ${escHtml(time)}</span>
          </td>
          <td style="padding: 12px; vertical-align: top;">
            <ul style="margin: 0; padding-left: 16px; font-size: 13px;">${itemsList || '<li>Sin productos</li>'}</ul>
          </td>
          <td style="padding: 12px; vertical-align: top; text-align: right;">
            <strong>${total}</strong><br/>
            <span style="display: inline-block; margin-top: 4px; padding: 2px 8px; border-radius: 10px; font-size: 11px; color: white; background: ${statusColor};">${statusLabel}</span>
          </td>
          <td style="padding: 12px; vertical-align: top; text-align: center;">
            <a href="${checkoutUrl}" style="display: inline-block; background: #0070f3; color: white; padding: 8px 16px; border-radius: 6px; text-decoration: none; font-size: 13px; font-weight: 500; margin-bottom: 6px;">
              Recuperar
            </a><br/>
            <button onclick="copyLink('${checkoutUrl}')" style="background: #f3f4f6; color: #374151; border: 1px solid #d1d5db; padding: 6px 12px; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 500;">
              📋 Copiar Link
            </button>
          </td>
        </tr>`;
    }).join('');

    return `
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset="utf-8"/>
        <script>
          function copyLink(url) {
            if (navigator.clipboard && navigator.clipboard.writeText) {
              navigator.clipboard.writeText(url).then(function() {
                alert('✅ Link copiado al portapapeles');
              }).catch(function() {
                fallbackCopy(url);
              });
            } else {
              fallbackCopy(url);
            }
          }
          function fallbackCopy(url) {
            var textarea = document.createElement('textarea');
            textarea.value = url;
            textarea.style.position = 'fixed';
            textarea.style.opacity = '0';
            document.body.appendChild(textarea);
            textarea.select();
            try {
              document.execCommand('copy');
              alert('✅ Link copiado al portapapeles');
            } catch (err) {
              alert('❌ No se pudo copiar. Link: ' + url);
            }
            document.body.removeChild(textarea);
          }
        </script>
      </head>
      <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #333; margin: 0; padding: 20px; background: #f5f5f5;">
        <div style="max-width: 960px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">

          <!-- Header -->
          <div style="background: #1a1a2e; color: white; padding: 24px 32px;">
            <h1 style="margin: 0; font-size: 22px;">🛒 Reporte de Carritos Abandonados</h1>
            <p style="margin: 8px 0 0; opacity: 0.8; font-size: 14px;">${escHtml(dateStr)}</p>
          </div>

          <!-- Summary stats -->
          <div style="display: flex; padding: 20px 32px; background: #f0f4ff; gap: 24px; flex-wrap: wrap;">
            <div style="flex: 1; min-width: 120px; text-align: center;">
              <div style="font-size: 28px; font-weight: 700; color: #1a1a2e;">${checkouts.length}</div>
              <div style="font-size: 12px; color: #666; margin-top: 4px;">Carritos abandonados</div>
            </div>
            <div style="flex: 1; min-width: 120px; text-align: center;">
              <div style="font-size: 28px; font-weight: 700; color: #ef4444;">$${totalAmount.toFixed(2)}</div>
              <div style="font-size: 12px; color: #666; margin-top: 4px;">Valor total (${currency})</div>
            </div>
            <div style="flex: 1; min-width: 120px; text-align: center;">
              <div style="font-size: 28px; font-weight: 700; color: #0070f3;">${checkouts.filter(c => c.buyerInfo?.email).length}</div>
              <div style="font-size: 12px; color: #666; margin-top: 4px;">Con email</div>
            </div>
            <div style="flex: 1; min-width: 120px; text-align: center;">
              <div style="font-size: 28px; font-weight: 700; color: #22c55e;">${checkouts.filter(c => c.contactDetails?.phone).length}</div>
              <div style="font-size: 12px; color: #666; margin-top: 4px;">Con teléfono</div>
            </div>
          </div>

          <!-- Instructions -->
          <div style="padding: 20px 32px 8px;">
            <p style="font-size: 14px; color: #666; margin: 0;">
              Los siguientes clientes dejaron un carrito de compra sin completar hoy.
              <strong>Favor de dar seguimiento por teléfono o email para recuperar la venta.</strong>
              Se adjunta un archivo CSV con los datos para importar en su herramienta de seguimiento.
            </p>
          </div>

          <!-- Table -->
          <div style="padding: 16px 32px 24px; overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
              <thead>
                <tr style="background: #f9f9f9; text-align: left;">
                  <th style="padding: 10px 12px; width: 30px;">#</th>
                  <th style="padding: 10px 12px;">Contacto</th>
                  <th style="padding: 10px 12px;">Productos</th>
                  <th style="padding: 10px 12px; text-align: right;">Total</th>
                  <th style="padding: 10px 12px; text-align: center; width: 100px;">Acción</th>
                </tr>
              </thead>
              <tbody>
                ${rows}
              </tbody>
            </table>
          </div>

          <!-- Footer -->
          <div style="background: #f9f9f9; padding: 16px 32px; text-align: center; font-size: 12px; color: #999;">
            Reporte generado automáticamente por el sistema de tareas programadas — Proconsa
          </div>
        </div>
      </body>
      </html>`;
  }
}
