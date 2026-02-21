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

  const facts = [
    { name: '📦 Inventario OK',        value: String(s.invOk) },
    { name: '⛔ Bloqueados (stock=0)', value: String(s.blocked) },
    { name: '💲 Precios actualizados', value: String(s.priceOk) },
    { name: '🆕 Nuevas promos',        value: String(s.promoNew) },
    { name: '❌ Promos eliminadas',    value: String(s.promoDel) },
    { name: '🏷 Col. Descuentos',      value: `+${s.descuentosAddOk} / -${s.descuentosRemOk}` },
    { name: '🎟 Col. Descuento10',     value: `+${s.descuento10AddOk} / -${s.descuento10RemOk}` },
    { name: '⏭ Omitidos',             value: String(s.skipped) },
    { name: '🚨 Errores',              value: String(totalFails) },
  ];

  const body = {
    type: 'message',
    attachments: [
      {
        contentType: 'application/vnd.microsoft.card.adaptive',
        content: {
          $schema: 'http://adaptivecards.io/schemas/adaptive-card.json',
          type: 'AdaptiveCard',
          version: '1.4',
          body: [
            {
              type: 'TextBlock',
              size: 'Medium',
              weight: 'Bolder',
              text: `${statusEmoji} Wix Sync ${s.modeLabel} — ${statusText}`,
            },
            {
              type: 'TextBlock',
              text: s.now,
              isSubtle: true,
              spacing: 'None',
            },
            {
              type: 'FactSet',
              facts,
            },
          ],
        },
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
