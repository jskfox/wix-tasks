import { BaseTask } from './base-task';
import { config } from '../config';
import { logger } from '../utils/logger';
import { queryAbandonedCheckouts, AbandonedCheckout } from '../services/wix-api';
import { sendEmail } from '../services/email';

const CTX = 'AbandonedCarts';

export class AbandonedCartsTask extends BaseTask {
  readonly name = 'abandoned-carts-report';
  // Every day at 10:00 PM Pacific
  readonly cronExpression = '0 22 * * *';

  async execute(): Promise<void> {
    logger.info(CTX, 'Fetching abandoned checkouts for today...');

    // ── 1. Build date filter for today (Pacific) ─────────────────────────────
    const now = new Date();
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);

    const filter = {
      _createdDate: {
        $gte: todayStart.toISOString(),
        $lte: now.toISOString(),
      },
    };

    // ── 2. Query Wix for abandoned checkouts ─────────────────────────────────
    const checkouts = await queryAbandonedCheckouts(filter);

    if (checkouts.length === 0) {
      logger.info(CTX, 'No abandoned checkouts found for today');
      return;
    }

    logger.info(CTX, `Found ${checkouts.length} abandoned checkout(s) for today`);

    // ── 3. Build the HTML email ──────────────────────────────────────────────
    const html = this.buildEmailHtml(checkouts, now);
    const text = `Reporte de ${checkouts.length} carrito(s) abandonado(s) — ${now.toLocaleDateString('es-MX')}`;

    // ── 4. Send via SMTP ─────────────────────────────────────────────────────
    if (config.marketingEmails.length === 0) {
      logger.warn(CTX, 'No MARKETING_EMAILS configured — skipping email send');
      return;
    }

    await sendEmail({
      to: config.marketingEmails,
      subject: `🛒 Carritos Abandonados del ${now.toLocaleDateString('es-MX')} — ${checkouts.length} pendiente(s)`,
      html,
      text,
    });

    logger.info(CTX, `Report sent to ${config.marketingEmails.length} recipient(s)`);
  }

  private buildEmailHtml(checkouts: AbandonedCheckout[], date: Date): string {
    const dateStr = date.toLocaleDateString('es-MX', {
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });

    const rows = checkouts.map((c, idx) => {
      const contact = c.contactDetails || c.buyerInfo || {};
      const name = [contact.firstName, contact.lastName].filter(Boolean).join(' ') || 'N/A';
      const email = contact.email || 'N/A';
      const phone = contact.phone || 'N/A';
      const checkoutUrl = c.checkoutUrl || '#';
      const total = c.total?.formattedAmount || c.total?.amount || '—';

      const itemsList = (c.lineItems || [])
        .map(li => {
          const pName = li.productName?.original || 'Producto';
          const qty = li.quantity || 1;
          const price = li.price?.formattedAmount || li.price?.amount || '—';
          return `<li>${pName} × ${qty} — ${price}</li>`;
        })
        .join('');

      return `
        <tr style="border-bottom: 1px solid #e0e0e0;">
          <td style="padding: 12px; vertical-align: top;">${idx + 1}</td>
          <td style="padding: 12px; vertical-align: top;">
            <strong>${name}</strong><br/>
            📧 ${email}<br/>
            📞 ${phone}
          </td>
          <td style="padding: 12px; vertical-align: top;">
            <ul style="margin: 0; padding-left: 16px;">${itemsList || '<li>Sin productos</li>'}</ul>
          </td>
          <td style="padding: 12px; vertical-align: top; text-align: right;"><strong>${total}</strong></td>
          <td style="padding: 12px; vertical-align: top; text-align: center;">
            <a href="${checkoutUrl}" style="background: #0070f3; color: white; padding: 6px 14px; border-radius: 4px; text-decoration: none; font-size: 13px;">
              Recuperar
            </a>
          </td>
        </tr>`;
    }).join('');

    return `
      <!DOCTYPE html>
      <html>
      <head><meta charset="utf-8"/></head>
      <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #333; margin: 0; padding: 20px; background: #f5f5f5;">
        <div style="max-width: 900px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
          <div style="background: #1a1a2e; color: white; padding: 24px 32px;">
            <h1 style="margin: 0; font-size: 22px;">🛒 Reporte de Carritos Abandonados</h1>
            <p style="margin: 8px 0 0; opacity: 0.8; font-size: 14px;">${dateStr} — ${checkouts.length} carrito(s) pendiente(s)</p>
          </div>

          <div style="padding: 24px 32px;">
            <p style="font-size: 14px; color: #666; margin-top: 0;">
              Los siguientes clientes dejaron un carrito de compra sin completar hoy.
              Favor de dar seguimiento por teléfono para recuperar la venta.
            </p>

            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
              <thead>
                <tr style="background: #f9f9f9; text-align: left;">
                  <th style="padding: 10px 12px; width: 30px;">#</th>
                  <th style="padding: 10px 12px;">Contacto</th>
                  <th style="padding: 10px 12px;">Productos</th>
                  <th style="padding: 10px 12px; text-align: right;">Total</th>
                  <th style="padding: 10px 12px; text-align: center;">Acción</th>
                </tr>
              </thead>
              <tbody>
                ${rows}
              </tbody>
            </table>
          </div>

          <div style="background: #f9f9f9; padding: 16px 32px; text-align: center; font-size: 12px; color: #999;">
            Reporte generado automáticamente por el sistema de tareas programadas — Proconsa
          </div>
        </div>
      </body>
      </html>`;
  }
}
