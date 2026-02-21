import { logger } from '../utils/logger';

const CTX = 'Teams';

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

  const lines = [
    `${statusEmoji} **Wix Sync ${s.modeLabel} — ${statusText}**`,
    `🕐 ${s.now}`,
    ``,
    `📦 Inventario OK: **${s.invOk}**`,
    `⛔ Bloqueados (stock=0): **${s.blocked}**`,
    `💲 Precios actualizados: **${s.priceOk}**`,
    `🆕 Nuevas promos: **${s.promoNew}**`,
    `❌ Promos eliminadas: **${s.promoDel}**`,
    `🏷 Col. Descuentos: **+${s.descuentosAddOk} / -${s.descuentosRemOk}**`,
    `🎟 Col. Descuento10: **+${s.descuento10AddOk} / -${s.descuento10RemOk}**`,
    `⏭ Omitidos: **${s.skipped}**`,
    `🚨 Errores: **${totalFails}**`,
  ];

  const body = { text: lines.join('\n') };

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
