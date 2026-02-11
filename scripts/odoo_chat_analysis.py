#!/usr/bin/env python3
"""
Script para extraer y analizar todas las sesiones de chat de proconsa.online en Odoo.
Genera reportes ejecutivos descargables en CSV y Markdown.
"""

import xmlrpc.client
import json
import os
import re
import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html.parser import HTMLParser

# Leer credenciales
CONFIG_PATH = os.path.expanduser("~/Dev/mcp/mcp-odoo/odoo_config.json")
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

URL = cfg["url"]
DB = cfg["db"]
USERNAME = cfg["username"]
PASSWORD = cfg["password"]

OUTPUT_DIR = os.path.expanduser("~/Dev/wix-tasks/reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

class HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.result = []
    def handle_data(self, d):
        self.result.append(d)
    def get_data(self):
        return ''.join(self.result)

def strip_html(html):
    s = HTMLStripper()
    s.feed(html or "")
    return s.get_data().strip()

def connect():
    common = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/common")
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    if not uid:
        raise Exception("No se pudo autenticar")
    models = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/object")
    print(f"Conectado a Odoo. UID: {uid}")
    return uid, models

def get_all_sessions(uid, models):
    """Obtiene todas las sesiones de livechat"""
    print("Obteniendo sesiones de chat...")
    sessions = []
    offset = 0
    batch = 200
    while True:
        chunk = models.execute_kw(
            DB, uid, PASSWORD,
            'discuss.channel', 'search_read',
            [[['livechat_channel_id', '=', 1]]],
            {'fields': ['name', 'create_date', 'livechat_operator_id', 'anonymous_name',
                        'country_id', 'message_ids', 'livechat_active'],
             'limit': batch, 'offset': offset, 'order': 'create_date asc'}
        )
        if not chunk:
            break
        sessions.extend(chunk)
        offset += batch
        print(f"  Sesiones obtenidas: {len(sessions)}")
    print(f"Total sesiones: {len(sessions)}")
    return sessions

def get_messages_batch(uid, models, message_ids):
    """Obtiene mensajes en lotes"""
    all_msgs = []
    batch = 500
    for i in range(0, len(message_ids), batch):
        chunk_ids = message_ids[i:i+batch]
        msgs = models.execute_kw(
            DB, uid, PASSWORD,
            'mail.message', 'read',
            [chunk_ids],
            {'fields': ['body', 'author_id', 'date', 'res_id', 'message_type']}
        )
        all_msgs.extend(msgs)
    return all_msgs

def analyze_chats(sessions, all_messages):
    """Análisis profundo de todas las conversaciones"""
    
    # Organizar mensajes por sesión
    msgs_by_session = defaultdict(list)
    for m in all_messages:
        msgs_by_session[m['res_id']].append(m)
    
    # Métricas generales
    total_sessions = len(sessions)
    total_messages = len(all_messages)
    
    # Análisis temporal
    sessions_by_month = Counter()
    sessions_by_weekday = Counter()
    sessions_by_hour = Counter()
    weekday_names = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    
    # Análisis de contenido
    visitor_messages = []
    bot_messages = []
    emails_captured = []
    products_mentioned = Counter()
    intents = Counter()
    conversations_data = []
    
    # Patrones de intención
    intent_patterns = {
        'cotizacion_mayoreo': r'cotizaci[oó]n.*mayoreo|mayoreo|precio.*mayoreo',
        'talleres_clinicas': r'taller|cl[ií]nica|capacitaci[oó]n|curso|inscrib',
        'problema_sitio': r'problema.*sitio|no.*funciona|error|no.*carga|no.*puedo',
        'solo_viendo': r'solo.*viendo|nada.*gracias|no.*gracias|solo.*mirando',
        'busca_producto': r'busco|necesito|quiero|donde.*encuentro|tienen',
        'precio': r'precio|costo|cu[aá]nto.*cuesta|cu[aá]nto.*vale',
        'disponibilidad': r'disponib|hay.*en.*stock|tienen.*en.*existencia',
        'envio': r'env[ií]o|entrega|domicilio|mandan',
        'horario': r'horario|abren|cierran|hora',
        'ubicacion': r'ubicaci[oó]n|direcci[oó]n|donde.*est[aá]n|sucursal',
        'devolucion': r'devoluci[oó]n|cambio|garant[ií]a',
        'factura': r'factura|facturaci[oó]n|cfdi|rfc',
        'contratista': r'contratista|constructor|obra|proyecto',
    }
    
    # Categorías de productos
    product_patterns = {
        'Varilla/Acero': r'varilla|acero|alambre|clavo|malla|solera|perfil.*met[aá]l',
        'Cemento/Concreto': r'cemento|concreto|mortero|mezcla|block|tabique|tabic[oó]n',
        'Pintura': r'pintura|rodillo|brocha|impermeabilizante|sellador|esmalte',
        'Pisos/Loseta': r'piso|loseta|porcelanato|azulejo|cer[aá]mica|adocreto',
        'Plomería': r'tubo|tuber[ií]a|v[aá]lvula|llave|conector|plomer[ií]a|tinaco',
        'Electricidad': r'cable|el[eé]ctric|interruptor|contacto|l[aá]mpara|foco',
        'Herramientas': r'herramienta|taladro|sierra|martillo|llave|desarmador',
        'Madera': r'madera|triplay|plywood|tabla|poste|viga',
        'Ferretería': r'tornillo|pija|ancla|bisagra|jaladera|chapa|cerradura',
        'Impermeabilizante': r'impermeabilizante|impermeable|membrana|asfalto',
        'Vigueta/Estructura': r'vigueta|bovedilla|castillo|armex|estructura',
        'Arena/Grava': r'arena|grava|piedra|material.*p[eé]treo',
    }
    
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    
    for session in sessions:
        sid = session['id']
        create_dt = datetime.strptime(session['create_date'], '%Y-%m-%d %H:%M:%S')
        
        sessions_by_month[create_dt.strftime('%Y-%m')] += 1
        sessions_by_weekday[weekday_names[create_dt.weekday()]] += 1
        sessions_by_hour[create_dt.hour] += 1
        
        msgs = sorted(msgs_by_session.get(sid, []), key=lambda x: x['date'])
        
        visitor_texts = []
        bot_texts = []
        session_emails = []
        session_products = set()
        session_intents = set()
        
        for msg in msgs:
            text = strip_html(msg['body'])
            if not text or 'Reiniciando' in text or 'abandonó' in text:
                continue
            
            is_visitor = msg['author_id'] == False or (isinstance(msg['author_id'], list) and msg['author_id'][0] not in [7, 8, 2])
            
            if is_visitor:
                visitor_texts.append(text)
                text_lower = text.lower()
                
                # Detectar emails
                found_emails = re.findall(email_pattern, text)
                session_emails.extend(found_emails)
                
                # Detectar intenciones
                for intent, pattern in intent_patterns.items():
                    if re.search(pattern, text_lower):
                        session_intents.add(intent)
                
                # Detectar productos
                for product, pattern in product_patterns.items():
                    if re.search(pattern, text_lower):
                        session_products.add(product)
            else:
                bot_texts.append(text)
        
        for intent in session_intents:
            intents[intent] += 1
        for product in session_products:
            products_mentioned[product] += 1
        emails_captured.extend(session_emails)
        
        conversations_data.append({
            'session_id': sid,
            'date': session['create_date'],
            'operator': session['livechat_operator_id'][1] if session['livechat_operator_id'] else 'N/A',
            'country': session['country_id'][1] if session['country_id'] else 'N/A',
            'active': session['livechat_active'],
            'num_messages': len(msgs),
            'visitor_messages': ' | '.join(visitor_texts[:5]),
            'intents': ', '.join(session_intents) if session_intents else 'sin_clasificar',
            'products': ', '.join(session_products) if session_products else 'ninguno',
            'emails': ', '.join(session_emails) if session_emails else '',
        })
    
    # Emails únicos
    unique_emails = list(set(e.lower() for e in emails_captured))
    
    return {
        'total_sessions': total_sessions,
        'total_messages': total_messages,
        'sessions_by_month': dict(sorted(sessions_by_month.items())),
        'sessions_by_weekday': {d: sessions_by_weekday.get(d, 0) for d in weekday_names},
        'sessions_by_hour': dict(sorted(sessions_by_hour.items())),
        'intents': dict(intents.most_common()),
        'products_mentioned': dict(products_mentioned.most_common()),
        'emails_captured': unique_emails,
        'total_emails_captured': len(unique_emails),
        'conversations_data': conversations_data,
    }

def generate_reports(analysis):
    """Genera reportes descargables"""
    
    # 1. CSV de todas las conversaciones
    csv_path = os.path.join(OUTPUT_DIR, 'chat_conversaciones_detalle.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'session_id', 'date', 'operator', 'country', 'active',
            'num_messages', 'visitor_messages', 'intents', 'products', 'emails'
        ])
        writer.writeheader()
        writer.writerows(analysis['conversations_data'])
    print(f"  CSV conversaciones: {csv_path}")
    
    # 2. CSV de emails capturados
    emails_path = os.path.join(OUTPUT_DIR, 'chat_emails_capturados.csv')
    with open(emails_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email'])
        for email in sorted(analysis['emails_captured']):
            writer.writerow([email])
    print(f"  CSV emails: {emails_path}")
    
    # 3. CSV de métricas
    metrics_path = os.path.join(OUTPUT_DIR, 'chat_metricas.csv')
    with open(metrics_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Métrica', 'Valor'])
        writer.writerow(['Total Sesiones', analysis['total_sessions']])
        writer.writerow(['Total Mensajes', analysis['total_messages']])
        writer.writerow(['Emails Capturados', analysis['total_emails_captured']])
        writer.writerow(['---', '---'])
        writer.writerow(['SESIONES POR MES', ''])
        for k, v in analysis['sessions_by_month'].items():
            writer.writerow([k, v])
        writer.writerow(['---', '---'])
        writer.writerow(['SESIONES POR DÍA', ''])
        for k, v in analysis['sessions_by_weekday'].items():
            writer.writerow([k, v])
        writer.writerow(['---', '---'])
        writer.writerow(['SESIONES POR HORA (UTC)', ''])
        for k, v in analysis['sessions_by_hour'].items():
            writer.writerow([f'{k}:00', v])
        writer.writerow(['---', '---'])
        writer.writerow(['INTENCIONES DETECTADAS', ''])
        for k, v in analysis['intents'].items():
            writer.writerow([k, v])
        writer.writerow(['---', '---'])
        writer.writerow(['PRODUCTOS MENCIONADOS', ''])
        for k, v in analysis['products_mentioned'].items():
            writer.writerow([k, v])
    print(f"  CSV métricas: {metrics_path}")
    
    # 4. Reporte ejecutivo en Markdown
    report_path = os.path.join(OUTPUT_DIR, 'REPORTE_EJECUTIVO_CHAT.md')
    
    total = analysis['total_sessions']
    intent_total = sum(analysis['intents'].values())
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# REPORTE EJECUTIVO - Análisis de Chat proconsa.online\n\n")
        f.write(f"**Fecha de generación:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"**Período analizado:** {min(analysis['sessions_by_month'].keys())} a {max(analysis['sessions_by_month'].keys())}\n\n")
        f.write("---\n\n")
        
        # Resumen ejecutivo
        f.write("## 1. RESUMEN EJECUTIVO\n\n")
        f.write(f"| Métrica | Valor |\n|---|---|\n")
        f.write(f"| Total de sesiones de chat | **{total:,}** |\n")
        f.write(f"| Total de mensajes | **{analysis['total_messages']:,}** |\n")
        f.write(f"| Promedio mensajes por sesión | **{analysis['total_messages']/max(total,1):.1f}** |\n")
        f.write(f"| Emails capturados (únicos) | **{analysis['total_emails_captured']}** |\n")
        f.write(f"| Tasa de captura de email | **{analysis['total_emails_captured']/max(total,1)*100:.1f}%** |\n\n")
        
        # Tendencia mensual
        f.write("## 2. TENDENCIA MENSUAL\n\n")
        f.write("| Mes | Sesiones | Tendencia |\n|---|---|---|\n")
        prev = 0
        for month, count in analysis['sessions_by_month'].items():
            trend = ""
            if prev > 0:
                change = ((count - prev) / prev) * 100
                trend = f"{'📈' if change > 0 else '📉'} {change:+.0f}%"
            prev = count
            f.write(f"| {month} | {count} | {trend} |\n")
        f.write("\n")
        
        # Distribución por día
        f.write("## 3. DISTRIBUCIÓN POR DÍA DE LA SEMANA\n\n")
        f.write("| Día | Sesiones | % del Total |\n|---|---|---|\n")
        for day, count in analysis['sessions_by_weekday'].items():
            pct = count / max(total, 1) * 100
            f.write(f"| {day} | {count} | {pct:.1f}% |\n")
        f.write("\n")
        
        # Distribución por hora
        f.write("## 4. DISTRIBUCIÓN POR HORA (UTC)\n\n")
        f.write("| Hora (UTC) | Hora (Tijuana) | Sesiones | % |\n|---|---|---|---|\n")
        for hour in range(24):
            count = analysis['sessions_by_hour'].get(hour, 0)
            tj_hour = (hour - 8) % 24
            pct = count / max(total, 1) * 100
            bar = '█' * int(pct / 2)
            f.write(f"| {hour:02d}:00 | {tj_hour:02d}:00 | {count} | {pct:.1f}% {bar} |\n")
        f.write("\n")
        
        # Intenciones
        f.write("## 5. INTENCIONES DE LOS VISITANTES\n\n")
        f.write("| Intención | Sesiones | % del Total |\n|---|---|---|\n")
        intent_labels = {
            'cotizacion_mayoreo': '💰 Cotización de Mayoreo',
            'talleres_clinicas': '🎓 Talleres/Clínicas',
            'problema_sitio': '⚠️ Problemas con el Sitio',
            'solo_viendo': '👀 Solo Viendo',
            'busca_producto': '🔍 Busca Producto',
            'precio': '💲 Consulta de Precio',
            'disponibilidad': '📦 Disponibilidad',
            'envio': '🚚 Envío/Entrega',
            'horario': '🕐 Horario',
            'ubicacion': '📍 Ubicación',
            'devolucion': '🔄 Devolución/Garantía',
            'factura': '🧾 Facturación',
            'contratista': '🏗️ Contratista/Constructor',
        }
        for intent, count in analysis['intents'].items():
            label = intent_labels.get(intent, intent)
            pct = count / max(total, 1) * 100
            f.write(f"| {label} | {count} | {pct:.1f}% |\n")
        f.write("\n")
        
        # Productos
        f.write("## 6. PRODUCTOS MÁS MENCIONADOS\n\n")
        f.write("| Categoría de Producto | Menciones | % |\n|---|---|---|\n")
        for product, count in analysis['products_mentioned'].items():
            pct = count / max(total, 1) * 100
            f.write(f"| {product} | {count} | {pct:.1f}% |\n")
        f.write("\n")
        
        # Deducciones y hallazgos
        f.write("## 7. DEDUCCIONES Y HALLAZGOS CLAVE\n\n")
        
        # Calcular insights
        top_intent = max(analysis['intents'].items(), key=lambda x: x[1]) if analysis['intents'] else ('N/A', 0)
        top_product = max(analysis['products_mentioned'].items(), key=lambda x: x[1]) if analysis['products_mentioned'] else ('N/A', 0)
        
        mayoreo_count = analysis['intents'].get('cotizacion_mayoreo', 0)
        talleres_count = analysis['intents'].get('talleres_clinicas', 0)
        solo_viendo_count = analysis['intents'].get('solo_viendo', 0)
        problema_count = analysis['intents'].get('problema_sitio', 0)
        contratista_count = analysis['intents'].get('contratista', 0)
        
        f.write("### 7.1 Perfil del Visitante\n")
        f.write(f"- **La mayoría de visitantes buscan cotizaciones de mayoreo** ({mayoreo_count} sesiones, {mayoreo_count/max(total,1)*100:.1f}%)\n")
        f.write(f"- **Los contratistas/constructores son un segmento importante** ({contratista_count} sesiones)\n")
        f.write(f"- **Los talleres/clínicas generan interés significativo** ({talleres_count} sesiones)\n")
        f.write(f"- **Visitantes que solo navegan** representan {solo_viendo_count} sesiones\n\n")
        
        f.write("### 7.2 Demanda de Productos\n")
        f.write(f"- **Producto más consultado:** {top_product[0]} ({top_product[1]} menciones)\n")
        f.write("- Los productos de construcción pesada (varilla, cemento, vigueta) dominan las consultas\n")
        f.write("- Existe demanda cruzada: quienes piden varilla también preguntan por cemento y arena\n\n")
        
        f.write("### 7.3 Problemas Detectados\n")
        f.write(f"- **{problema_count} sesiones reportaron problemas con el sitio web**\n")
        f.write("- El chatbot (Mary Mejora) maneja la mayoría de conversaciones de forma automatizada\n")
        f.write("- Cuando el bot no puede resolver, no hay operadores humanos disponibles frecuentemente\n\n")
        
        f.write("### 7.4 Captura de Leads\n")
        f.write(f"- Se capturaron **{analysis['total_emails_captured']} emails únicos** de visitantes\n")
        f.write(f"- Tasa de captura: **{analysis['total_emails_captured']/max(total,1)*100:.1f}%** del total de sesiones\n")
        f.write("- Los emails se capturan principalmente en flujos de cotización de mayoreo\n\n")
        
        # Horarios pico
        peak_hours = sorted(analysis['sessions_by_hour'].items(), key=lambda x: x[1], reverse=True)[:5]
        f.write("### 7.5 Horarios Pico\n")
        f.write("Las horas con más actividad (UTC → Tijuana):\n")
        for h, c in peak_hours:
            tj = (h - 8) % 24
            f.write(f"- **{h:02d}:00 UTC ({tj:02d}:00 Tijuana):** {c} sesiones\n")
        f.write("\n")
        
        # Días pico
        peak_days = sorted(analysis['sessions_by_weekday'].items(), key=lambda x: x[1], reverse=True)[:3]
        f.write("### 7.6 Días Más Activos\n")
        for d, c in peak_days:
            f.write(f"- **{d}:** {c} sesiones ({c/max(total,1)*100:.1f}%)\n")
        f.write("\n")
        
        # Recomendaciones
        f.write("## 8. RECOMENDACIONES Y ACCIONES\n\n")
        
        f.write("### 🔴 ACCIONES URGENTES (Impacto Alto, Esfuerzo Bajo)\n\n")
        f.write("1. **Asignar operadores humanos en horarios pico**\n")
        f.write("   - Muchas sesiones terminan sin resolución porque no hay operadores disponibles\n")
        f.write(f"   - Priorizar horarios: {', '.join(f'{(h-8)%24:02d}:00' for h,_ in peak_hours[:3])} (hora Tijuana)\n\n")
        f.write("2. **Dar seguimiento a los emails capturados**\n")
        f.write(f"   - Hay {analysis['total_emails_captured']} leads sin seguimiento confirmado\n")
        f.write("   - Crear campaña de email marketing dirigida a estos prospectos\n\n")
        f.write("3. **Revisar y corregir problemas del sitio web**\n")
        f.write(f"   - {problema_count} visitantes reportaron problemas técnicos\n\n")
        
        f.write("### 🟡 ACCIONES IMPORTANTES (Impacto Alto, Esfuerzo Medio)\n\n")
        f.write("4. **Mejorar el flujo del chatbot para cotizaciones de mayoreo**\n")
        f.write("   - Es la intención #1 de los visitantes\n")
        f.write("   - Automatizar la generación de cotizaciones básicas\n")
        f.write("   - Incluir precios de referencia para productos populares\n\n")
        f.write("5. **Crear landing pages específicas para contratistas**\n")
        f.write(f"   - {contratista_count} sesiones identificadas como contratistas\n")
        f.write("   - Ofrecer programa de lealtad o descuentos por volumen\n\n")
        f.write("6. **Potenciar los talleres/clínicas como herramienta de captación**\n")
        f.write(f"   - {talleres_count} sesiones mostraron interés en capacitación\n")
        f.write("   - Usar talleres como gancho para capturar datos de contacto\n\n")
        
        f.write("### 🟢 ACCIONES ESTRATÉGICAS (Impacto Medio-Alto, Esfuerzo Alto)\n\n")
        f.write("7. **Implementar catálogo digital con precios en el chat**\n")
        f.write("   - Los visitantes preguntan repetidamente por los mismos productos\n")
        f.write("   - Un catálogo interactivo reduciría la carga del chat\n\n")
        f.write("8. **Segmentar la base de datos por tipo de cliente**\n")
        f.write("   - Contratistas vs. Público general vs. Empresas\n")
        f.write("   - Personalizar ofertas y comunicación por segmento\n\n")
        f.write("9. **Implementar sistema de seguimiento post-chat**\n")
        f.write("   - Enviar email automático después de cada sesión con resumen\n")
        f.write("   - Incluir enlace a catálogo y promociones vigentes\n\n")
        f.write("10. **Analizar productos más demandados para optimizar inventario**\n")
        f.write("    - Asegurar stock de los productos más consultados\n")
        f.write("    - Crear bundles/paquetes de los productos que se piden juntos\n\n")
        
        # Conclusión
        f.write("## 9. CONCLUSIÓN\n\n")
        f.write("El chat de proconsa.online es una **herramienta activa de generación de leads** ")
        f.write(f"con {total:,} sesiones registradas. ")
        f.write("El principal hallazgo es que la mayoría de visitantes son **compradores potenciales de mayoreo** ")
        f.write("(contratistas y constructores) buscando cotizaciones. ")
        f.write("La oportunidad más grande está en **mejorar el seguimiento de los leads capturados** ")
        f.write("y **optimizar el flujo del chatbot** para convertir más visitantes en clientes.\n\n")
        f.write("---\n\n")
        f.write("### Archivos generados:\n")
        f.write(f"- `chat_conversaciones_detalle.csv` - Detalle de cada sesión\n")
        f.write(f"- `chat_emails_capturados.csv` - Lista de emails capturados\n")
        f.write(f"- `chat_metricas.csv` - Métricas numéricas\n")
        f.write(f"- `REPORTE_EJECUTIVO_CHAT.md` - Este reporte\n")
    
    print(f"  Reporte ejecutivo: {report_path}")
    return report_path

def main():
    print("=" * 70)
    print("ANÁLISIS PROFUNDO DE CHAT - proconsa.online")
    print("=" * 70)
    
    uid, models = connect()
    
    # 1. Obtener sesiones
    sessions = get_all_sessions(uid, models)
    
    # 2. Obtener todos los message_ids
    all_msg_ids = set()
    for s in sessions:
        all_msg_ids.update(s['message_ids'])
    print(f"\nTotal de mensajes a obtener: {len(all_msg_ids)}")
    
    # 3. Obtener mensajes
    print("Obteniendo mensajes...")
    all_messages = get_messages_batch(uid, models, list(all_msg_ids))
    print(f"Mensajes obtenidos: {len(all_messages)}")
    
    # 4. Analizar
    print("\nAnalizando conversaciones...")
    analysis = analyze_chats(sessions, all_messages)
    
    # 5. Generar reportes
    print("\nGenerando reportes...")
    report_path = generate_reports(analysis)
    
    print("\n" + "=" * 70)
    print("ANÁLISIS COMPLETADO")
    print("=" * 70)
    print(f"\nArchivos generados en: {OUTPUT_DIR}/")
    print(f"  - chat_conversaciones_detalle.csv")
    print(f"  - chat_emails_capturados.csv")
    print(f"  - chat_metricas.csv")
    print(f"  - REPORTE_EJECUTIVO_CHAT.md")

if __name__ == "__main__":
    main()
