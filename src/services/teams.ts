import { logger } from '../utils/logger';
import { config } from '../config';

const CTX = 'Teams';

// ─── Generic notification ────────────────────────────────────────────────────

export interface TeamsRow { name: string; value: string }

export async function sendTeamsNotification(opts: {
  title: string;
  subtitle?: string;
  rows: TeamsRow[];
  hasErrors: boolean;
}): Promise<void> {
  const webhookUrl = config.task.teamsWebhook;
  if (!webhookUrl) return;

  const statusEmoji = opts.hasErrors ? '⚠️' : '✅';
  const body = {
    '@type': 'MessageCard',
    '@context': 'https://schema.org/extensions',
    themeColor: opts.hasErrors ? 'FF0000' : '00B050',
    summary: opts.title,
    sections: [
      {
        activityTitle: `${statusEmoji} ${opts.title}`,
        activitySubtitle: opts.subtitle ?? '',
        facts: opts.rows.map(r => ({ name: r.name, value: r.value })),
        markdown: true,
      },
    ],
  };

  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      logger.error(CTX, `Teams webhook failed: ${res.status} ${res.statusText} — ${text}`);
    } else {
      logger.info(CTX, `Teams notification sent: ${opts.title}`);
    }
  } catch (err) {
    logger.warn(CTX, `Teams notification error: ${err instanceof Error ? err.message : String(err)}`);
  }
}

export interface TeamsSyncSummary {
  modeLabel: string;
  now: string;
  invOk: number;
  invFail: number;
  blocked: number;
  priceOk: number;
  priceFail: number;
  descuentosAddOk: number;
  descuentosRemOk: number;
  descuento10AddOk: number;
  descuento10RemOk: number;
  colFail: number;
  skipped: number;
  promoNew: number;
  promoDel: number;
}

export async function sendTeamsSyncNotification(webhookUrl: string, s: TeamsSyncSummary): Promise<void> {
  const totalFails = s.invFail + s.priceFail + s.colFail;
  const statusEmoji = totalFails > 0 ? '⚠️' : '✅';
  const statusText  = totalFails > 0 ? `${totalFails} error(es)` : 'Sin errores';

  const rows = [
    `📦 Inventario actualizado: ${s.invOk}`,
    `⛔ Desactivados (stock=0): ${s.blocked}`,
    `💲 Precios actualizados: ${s.priceOk}`,
    `🆕 Nuevas promos: ${s.promoNew}`,
    `❌ Promos eliminadas: ${s.promoDel}`,
    `🏷 Categoría Promos(Descuentos): +${s.descuentosAddOk} / -${s.descuentosRemOk}`,
    `🎟 Productos para cupón Descuento10: +${s.descuento10AddOk} / -${s.descuento10RemOk}`,
    `⏭ Omitidos: ${s.skipped}`,
    `🚨 Errores: ${totalFails}`,
  ];

  // MessageCard format — supported by Power Automate Workflows without extra configuration
  const body = {
    '@type': 'MessageCard',
    '@context': 'https://schema.org/extensions',
    themeColor: totalFails > 0 ? 'FF0000' : '00B050',
    summary: `Sincronización ERP con eCommerce (Wix) ${s.modeLabel}`,
    sections: [
      {
        activityTitle: `${statusEmoji} Wix Sync ${s.modeLabel} — ${statusText}`,
        activitySubtitle: s.now,
        facts: rows.map(r => {
          const [name, ...rest] = r.split(': ');
          return { name: name.trim(), value: rest.join(': ').trim() };
        }),
        markdown: true,
      },
    ],
  };

  const res = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    logger.error(CTX, `Teams webhook failed: ${res.status} ${res.statusText} — ${text}`);
  } else {
    logger.info(CTX, 'Teams notification sent');
  }
}
