#!/usr/bin/env python3
"""
Script para generar reporte de seguimiento de leads del chat proconsa.online.
Extrae leads con email, cruza con Odoo, prioriza y genera reporte para marketing.
"""

import xmlrpc.client
import json
import os
import re
import csv
from collections import defaultdict
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

NOW = datetime.utcnow()

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
             'limit': batch, 'offset': offset, 'order': 'create_date desc'}
        )
        if not chunk:
            break
        sessions.extend(chunk)
        offset += batch
        print(f"  Sesiones: {len(sessions)}")
    return sessions

def get_messages_batch(uid, models, message_ids):
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

def enrich_from_odoo(uid, models, emails):
    """Busca información adicional de los emails en res.partner"""
    print(f"Enriqueciendo {len(emails)} emails con datos de Odoo...")
    enriched = {}
    batch = 50
    email_list = list(emails)
    for i in range(0, len(email_list), batch):
        chunk = email_list[i:i+batch]
        for email in chunk:
            partners = models.execute_kw(
                DB, uid, PASSWORD,
                'res.partner', 'search_read',
                [[['email', 'ilike', email]]],
                {'fields': ['name', 'email', 'phone', 'mobile', 'street', 'city',
                            'state_id', 'country_id', 'company_name', 'function',
                            'category_id', 'comment', 'type', 'is_company',
                            'sale_order_count', 'total_invoiced'],
                 'limit': 1}
            )
            if partners:
                p = partners[0]
                enriched[email.lower()] = {
                    'odoo_id': p['id'],
                    'name': p['name'],
                    'phone': p.get('phone') or '',
                    'mobile': p.get('mobile') or '',
                    'street': p.get('street') or '',
                    'city': p.get('city') or '',
                    'state': p['state_id'][1] if p.get('state_id') else '',
                    'company': p.get('company_name') or '',
                    'function': p.get('function') or '',
                    'is_company': p.get('is_company', False),
                    'categories': ', '.join([str(c) for c in p.get('category_id', [])]) if p.get('category_id') else '',
                    'sale_orders': p.get('sale_order_count', 0),
                    'total_invoiced': p.get('total_invoiced', 0),
                }
            else:
                enriched[email.lower()] = None
        if i % 100 == 0 and i > 0:
            print(f"  Procesados: {i}/{len(email_list)}")
    return enriched

# Patrones
email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

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

def classify_client_type(intents, products, visitor_texts_joined):
    """Clasifica el tipo de cliente potencial"""
    text = visitor_texts_joined.lower()
    
    if 'contratista' in intents or re.search(r'contratista|constructor|obra grande|proyecto|edificio|residencial|fraccionamiento', text):
        return 'Contratista/Constructor'
    if 'cotizacion_mayoreo' in intents and len(products) >= 2:
        return 'Mayorista/Distribuidor'
    if 'cotizacion_mayoreo' in intents:
        return 'Comprador de Volumen'
    if re.search(r'empresa|negocio|compañ[ií]a|sa de cv|s\.a\.|spr|s\.?r\.?l', text):
        return 'Empresa'
    if re.search(r'mi casa|remodelaci[oó]n|arreglar|reparar|ba[nñ]o|cocina|cuarto', text):
        return 'Particular/Remodelación'
    if 'talleres_clinicas' in intents:
        return 'Profesional en Formación'
    if 'factura' in intents:
        return 'Cliente con Facturación'
    return 'Prospecto General'

def calculate_priority(days_ago, has_email, intents, products, num_messages):
    """Calcula prioridad del lead: 1=Máxima, 5=Baja"""
    score = 0
    
    # Recencia (más reciente = más prioridad)
    if days_ago <= 3:
        score += 40
    elif days_ago <= 7:
        score += 30
    elif days_ago <= 14:
        score += 20
    elif days_ago <= 30:
        score += 10
    else:
        score += 0
    
    # Tiene email
    if has_email:
        score += 15
    
    # Intención de compra
    if 'cotizacion_mayoreo' in intents:
        score += 20
    if 'contratista' in intents:
        score += 15
    if 'busca_producto' in intents:
        score += 10
    if 'precio' in intents:
        score += 10
    
    # Productos específicos mencionados
    score += min(len(products) * 5, 15)
    
    # Engagement (más mensajes = más interés)
    if num_messages >= 8:
        score += 10
    elif num_messages >= 5:
        score += 5
    
    # Penalizar
    if 'solo_viendo' in intents:
        score -= 15
    if 'problema_sitio' in intents and len(intents) == 1:
        score -= 10
    
    if score >= 70:
        return 1, '🔴 MÁXIMA'
    elif score >= 50:
        return 2, '🟠 ALTA'
    elif score >= 35:
        return 3, '🟡 MEDIA'
    elif score >= 20:
        return 4, '🔵 BAJA'
    else:
        return 5, '⚪ MUY BAJA'

def suggest_approach(intents, products, client_type, visitor_texts):
    """Genera sugerencia de abordaje para el equipo de marketing"""
    suggestions = []
    
    if 'cotizacion_mayoreo' in intents:
        if products:
            suggestions.append(f"Enviar cotización personalizada de: {', '.join(products)}")
        else:
            suggestions.append("Contactar para conocer necesidades de mayoreo y enviar catálogo")
    
    if 'contratista' in intents or client_type == 'Contratista/Constructor':
        suggestions.append("Ofrecer programa de descuentos para contratistas y línea de crédito")
    
    if client_type == 'Mayorista/Distribuidor':
        suggestions.append("Presentar programa de distribución y precios especiales por volumen")
    
    if 'talleres_clinicas' in intents:
        suggestions.append("Invitar a próximos talleres y ofrecer descuento post-capacitación")
    
    if 'precio' in intents:
        suggestions.append("Enviar lista de precios actualizada de los productos consultados")
    
    if 'factura' in intents:
        suggestions.append("Ya es cliente - verificar historial y ofrecer recompra con beneficios")
    
    if 'problema_sitio' in intents:
        suggestions.append("Disculparse por inconvenientes técnicos y ofrecer atención directa")
    
    if not suggestions:
        if products:
            suggestions.append(f"Contactar ofreciendo información sobre: {', '.join(products)}")
        else:
            suggestions.append("Enviar catálogo general y promociones vigentes")
    
    return ' | '.join(suggestions)

def main():
    print("=" * 70)
    print("GENERACIÓN DE REPORTE DE SEGUIMIENTO DE LEADS")
    print("=" * 70)
    
    uid, models = connect()
    
    # 1. Obtener sesiones
    sessions = get_all_sessions(uid, models)
    print(f"Total sesiones: {len(sessions)}")
    
    # 2. Obtener mensajes
    all_msg_ids = set()
    for s in sessions:
        all_msg_ids.update(s['message_ids'])
    print(f"Obteniendo {len(all_msg_ids)} mensajes...")
    all_messages = get_messages_batch(uid, models, list(all_msg_ids))
    print(f"Mensajes obtenidos: {len(all_messages)}")
    
    # Organizar mensajes por sesión
    msgs_by_session = defaultdict(list)
    for m in all_messages:
        msgs_by_session[m['res_id']].append(m)
    
    # 3. Extraer leads con datos completos
    print("\nExtrayendo leads de las conversaciones...")
    leads = []
    all_lead_emails = set()
    
    for session in sessions:
        sid = session['id']
        create_dt = datetime.strptime(session['create_date'], '%Y-%m-%d %H:%M:%S')
        days_ago = (NOW - create_dt).days
        
        msgs = sorted(msgs_by_session.get(sid, []), key=lambda x: x['date'])
        
        visitor_texts = []
        session_emails = []
        session_products = set()
        session_intents = set()
        full_conversation = []
        
        for msg in msgs:
            text = strip_html(msg['body'])
            if not text or 'Reiniciando' in text or 'abandonó' in text:
                continue
            
            is_visitor = msg['author_id'] == False or (isinstance(msg['author_id'], list) and msg['author_id'][0] not in [7, 8, 2])
            
            if is_visitor:
                visitor_texts.append(text)
                text_lower = text.lower()
                
                found_emails = re.findall(email_pattern, text)
                session_emails.extend(found_emails)
                
                for intent, pattern in intent_patterns.items():
                    if re.search(pattern, text_lower):
                        session_intents.add(intent)
                
                for product, pattern in product_patterns.items():
                    if re.search(pattern, text_lower):
                        session_products.add(product)
                
                full_conversation.append(f"[Visitante]: {text}")
            else:
                author_name = msg['author_id'][1] if isinstance(msg['author_id'], list) else 'Bot'
                full_conversation.append(f"[{author_name}]: {text}")
        
        # Solo incluir sesiones donde el visitante escribió algo
        if not visitor_texts:
            continue
        
        visitor_joined = ' '.join(visitor_texts)
        client_type = classify_client_type(session_intents, session_products, visitor_joined)
        has_email = len(session_emails) > 0
        priority_num, priority_label = calculate_priority(
            days_ago, has_email, session_intents, session_products, len(msgs)
        )
        approach = suggest_approach(session_intents, session_products, client_type, visitor_texts)
        
        primary_email = session_emails[0].lower().strip() if session_emails else ''
        if primary_email:
            all_lead_emails.add(primary_email)
        
        leads.append({
            'session_id': sid,
            'fecha_chat': session['create_date'],
            'dias_transcurridos': days_ago,
            'priority_num': priority_num,
            'prioridad': priority_label,
            'email': primary_email,
            'tipo_cliente': client_type,
            'intenciones': ', '.join(sorted(session_intents)) if session_intents else 'sin_clasificar',
            'productos_solicitados': ', '.join(sorted(session_products)) if session_products else 'No especificado',
            'resumen_visitante': ' | '.join(visitor_texts[:6]),
            'sugerencia_abordaje': approach,
            'num_mensajes': len(msgs),
            'conversacion_completa': '\n'.join(full_conversation),
            # Campos para enriquecer después
            'nombre_odoo': '',
            'telefono': '',
            'celular': '',
            'ciudad': '',
            'estado': '',
            'empresa': '',
            'puesto': '',
            'es_empresa': False,
            'ordenes_venta': 0,
            'total_facturado': 0,
            'es_cliente_existente': False,
        })
    
    print(f"Leads extraídos: {len(leads)}")
    print(f"Leads con email: {len(all_lead_emails)}")
    
    # 4. Enriquecer con datos de Odoo
    enriched = enrich_from_odoo(uid, models, all_lead_emails)
    
    for lead in leads:
        email = lead['email']
        if email and email in enriched and enriched[email]:
            data = enriched[email]
            lead['nombre_odoo'] = data['name']
            lead['telefono'] = data['phone']
            lead['celular'] = data['mobile']
            lead['ciudad'] = data['city']
            lead['estado'] = data['state']
            lead['empresa'] = data['company']
            lead['puesto'] = data['function']
            lead['es_empresa'] = data['is_company']
            lead['ordenes_venta'] = data['sale_orders']
            lead['total_facturado'] = data['total_invoiced']
            lead['es_cliente_existente'] = data['sale_orders'] > 0 or data['total_invoiced'] > 0
    
    # 5. Ordenar por prioridad y recencia
    leads.sort(key=lambda x: (x['priority_num'], x['dias_transcurridos']))
    
    # Filtrar solo leads con email para el reporte principal
    leads_with_email = [l for l in leads if l['email']]
    leads_without_email = [l for l in leads if not l['email']]
    
    print(f"\nLeads con email (para seguimiento directo): {len(leads_with_email)}")
    print(f"Leads sin email (para análisis): {len(leads_without_email)}")
    
    # 6. Generar CSV principal de seguimiento
    csv_path = os.path.join(OUTPUT_DIR, 'LEADS_SEGUIMIENTO_MARKETING.csv')
    csv_fields = [
        'prioridad', 'fecha_chat', 'dias_transcurridos', 'email', 'nombre_odoo',
        'telefono', 'celular', 'tipo_cliente', 'productos_solicitados',
        'intenciones', 'sugerencia_abordaje', 'resumen_visitante',
        'ciudad', 'estado', 'empresa', 'puesto',
        'es_cliente_existente', 'ordenes_venta', 'total_facturado',
        'num_mensajes', 'session_id'
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(leads_with_email)
    print(f"CSV seguimiento: {csv_path}")
    
    # 7. CSV de conversaciones completas (para referencia)
    conv_path = os.path.join(OUTPUT_DIR, 'LEADS_CONVERSACIONES_COMPLETAS.csv')
    with open(conv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'prioridad', 'fecha_chat', 'email', 'nombre_odoo', 'tipo_cliente',
            'productos_solicitados', 'conversacion_completa'
        ], extrasaction='ignore')
        writer.writeheader()
        writer.writerows(leads_with_email)
    print(f"CSV conversaciones: {conv_path}")
    
    # 8. CSV de leads sin email (oportunidades perdidas)
    no_email_path = os.path.join(OUTPUT_DIR, 'LEADS_SIN_EMAIL_OPORTUNIDADES.csv')
    with open(no_email_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'prioridad', 'fecha_chat', 'dias_transcurridos', 'tipo_cliente',
            'productos_solicitados', 'intenciones', 'resumen_visitante', 'num_mensajes'
        ], extrasaction='ignore')
        writer.writeheader()
        writer.writerows([l for l in leads_without_email if l['priority_num'] <= 3])
    print(f"CSV sin email: {no_email_path}")
    
    # 9. Generar reporte Markdown para marketing
    report_path = os.path.join(OUTPUT_DIR, 'REPORTE_SEGUIMIENTO_MARKETING.md')
    
    # Estadísticas para el reporte
    priority_counts = defaultdict(int)
    type_counts = defaultdict(int)
    existing_clients = 0
    new_prospects = 0
    
    for l in leads_with_email:
        priority_counts[l['prioridad']] += 1
        type_counts[l['tipo_cliente']] += 1
        if l['es_cliente_existente']:
            existing_clients += 1
        else:
            new_prospects += 1
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# REPORTE DE SEGUIMIENTO DE LEADS - Equipo de Marketing\n")
        f.write(f"## Chat proconsa.online\n\n")
        f.write(f"**Generado:** {NOW.strftime('%Y-%m-%d %H:%M')} UTC\n\n")
        f.write(f"**Período de datos:** Octubre 2025 - Febrero 2026\n\n")
        f.write("---\n\n")
        
        # Resumen ejecutivo
        f.write("## RESUMEN EJECUTIVO\n\n")
        f.write(f"Se identificaron **{len(leads_with_email)} leads con email** de las 1,582 sesiones de chat.\n")
        f.write(f"Adicionalmente hay **{len([l for l in leads_without_email if l['priority_num'] <= 3])} sesiones sin email** con intención de compra detectada.\n\n")
        
        f.write("| Métrica | Valor |\n|---|---|\n")
        f.write(f"| Leads con email para seguimiento | **{len(leads_with_email)}** |\n")
        f.write(f"| Clientes existentes en Odoo | **{existing_clients}** |\n")
        f.write(f"| Prospectos nuevos | **{new_prospects}** |\n")
        f.write(f"| Leads sin email (oportunidades perdidas) | **{len(leads_without_email)}** |\n\n")
        
        # Distribución por prioridad
        f.write("## DISTRIBUCIÓN POR PRIORIDAD\n\n")
        f.write("| Prioridad | Cantidad | Acción Sugerida |\n|---|---|---|\n")
        priority_actions = {
            '🔴 MÁXIMA': 'Contactar HOY - Lead caliente reciente con intención clara de compra',
            '🟠 ALTA': 'Contactar en 24-48 hrs - Interés fuerte, necesita seguimiento rápido',
            '🟡 MEDIA': 'Contactar esta semana - Interés moderado, enviar información',
            '🔵 BAJA': 'Incluir en campaña de email - Interés bajo o antiguo',
            '⚪ MUY BAJA': 'Solo nurturing automático - Sin intención clara de compra',
        }
        for p in ['🔴 MÁXIMA', '🟠 ALTA', '🟡 MEDIA', '🔵 BAJA', '⚪ MUY BAJA']:
            count = priority_counts.get(p, 0)
            action = priority_actions.get(p, '')
            f.write(f"| {p} | {count} | {action} |\n")
        f.write("\n")
        
        # Distribución por tipo de cliente
        f.write("## TIPOS DE CLIENTE IDENTIFICADOS\n\n")
        f.write("| Tipo de Cliente | Cantidad | Estrategia |\n|---|---|---|\n")
        type_strategies = {
            'Contratista/Constructor': 'Programa de lealtad, crédito, precios especiales por volumen',
            'Comprador de Volumen': 'Cotización personalizada, descuentos por cantidad',
            'Mayorista/Distribuidor': 'Programa de distribución, precios de mayoreo',
            'Empresa': 'Cuenta corporativa, facturación, línea de crédito',
            'Particular/Remodelación': 'Asesoría técnica, paquetes de remodelación',
            'Profesional en Formación': 'Talleres, descuentos post-capacitación, fidelización temprana',
            'Cliente con Facturación': 'Recompra, programa de puntos, ofertas exclusivas',
            'Prospecto General': 'Catálogo general, promociones vigentes, nurturing',
        }
        for t, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            strategy = type_strategies.get(t, 'Enviar catálogo general')
            f.write(f"| {t} | {count} | {strategy} |\n")
        f.write("\n")
        
        # TOP LEADS - Prioridad máxima y alta
        f.write("---\n\n")
        f.write("## 🔴 LEADS PRIORIDAD MÁXIMA - CONTACTAR HOY\n\n")
        top_leads = [l for l in leads_with_email if l['priority_num'] == 1]
        
        if top_leads:
            for i, lead in enumerate(top_leads[:30], 1):
                f.write(f"### Lead #{i}\n")
                f.write(f"| Campo | Dato |\n|---|---|\n")
                f.write(f"| **Email** | {lead['email']} |\n")
                if lead['nombre_odoo']:
                    f.write(f"| **Nombre (Odoo)** | {lead['nombre_odoo']} |\n")
                if lead['telefono']:
                    f.write(f"| **Teléfono** | {lead['telefono']} |\n")
                if lead['celular']:
                    f.write(f"| **Celular** | {lead['celular']} |\n")
                f.write(f"| **Fecha del chat** | {lead['fecha_chat']} |\n")
                f.write(f"| **Hace** | {lead['dias_transcurridos']} días |\n")
                f.write(f"| **Tipo de cliente** | {lead['tipo_cliente']} |\n")
                f.write(f"| **Productos solicitados** | {lead['productos_solicitados']} |\n")
                if lead['ciudad'] or lead['estado']:
                    f.write(f"| **Ubicación** | {lead['ciudad']}, {lead['estado']} |\n")
                if lead['empresa']:
                    f.write(f"| **Empresa** | {lead['empresa']} |\n")
                if lead['es_cliente_existente']:
                    f.write(f"| **Cliente existente** | ✅ Sí ({lead['ordenes_venta']} órdenes, ${lead['total_facturado']:,.2f} facturado) |\n")
                else:
                    f.write(f"| **Cliente existente** | ❌ No - PROSPECTO NUEVO |\n")
                f.write(f"| **💡 Cómo abordarlo** | {lead['sugerencia_abordaje']} |\n")
                f.write(f"| **Lo que dijo** | {lead['resumen_visitante'][:300]} |\n")
                f.write("\n")
        else:
            f.write("No hay leads de prioridad máxima en este momento.\n\n")
        
        # LEADS PRIORIDAD ALTA
        f.write("---\n\n")
        f.write("## 🟠 LEADS PRIORIDAD ALTA - CONTACTAR EN 24-48 HRS\n\n")
        high_leads = [l for l in leads_with_email if l['priority_num'] == 2]
        
        if high_leads:
            for i, lead in enumerate(high_leads[:30], 1):
                f.write(f"### Lead #{i}\n")
                f.write(f"| Campo | Dato |\n|---|---|\n")
                f.write(f"| **Email** | {lead['email']} |\n")
                if lead['nombre_odoo']:
                    f.write(f"| **Nombre** | {lead['nombre_odoo']} |\n")
                if lead['telefono'] or lead['celular']:
                    f.write(f"| **Teléfono** | {lead['telefono'] or lead['celular']} |\n")
                f.write(f"| **Fecha** | {lead['fecha_chat']} (hace {lead['dias_transcurridos']} días) |\n")
                f.write(f"| **Tipo** | {lead['tipo_cliente']} |\n")
                f.write(f"| **Productos** | {lead['productos_solicitados']} |\n")
                if lead['es_cliente_existente']:
                    f.write(f"| **Ya es cliente** | ✅ ({lead['ordenes_venta']} órdenes) |\n")
                f.write(f"| **💡 Abordaje** | {lead['sugerencia_abordaje']} |\n")
                f.write(f"| **Contexto** | {lead['resumen_visitante'][:200]} |\n")
                f.write("\n")
        else:
            f.write("No hay leads de prioridad alta en este momento.\n\n")
        
        # LEADS PRIORIDAD MEDIA - Resumen compacto
        f.write("---\n\n")
        f.write("## 🟡 LEADS PRIORIDAD MEDIA - CONTACTAR ESTA SEMANA\n\n")
        medium_leads = [l for l in leads_with_email if l['priority_num'] == 3]
        
        if medium_leads:
            f.write(f"**Total: {len(medium_leads)} leads** (ver CSV para detalle completo)\n\n")
            f.write("| # | Email | Nombre | Tipo | Productos | Hace (días) | Abordaje |\n")
            f.write("|---|---|---|---|---|---|---|\n")
            for i, lead in enumerate(medium_leads[:50], 1):
                name = lead['nombre_odoo'][:25] if lead['nombre_odoo'] else '-'
                products = lead['productos_solicitados'][:30]
                approach = lead['sugerencia_abordaje'][:50]
                f.write(f"| {i} | {lead['email']} | {name} | {lead['tipo_cliente']} | {products} | {lead['dias_transcurridos']} | {approach} |\n")
            f.write("\n")
        
        # Insights y recomendaciones
        f.write("---\n\n")
        f.write("## INSIGHTS PARA EL EQUIPO DE MARKETING\n\n")
        
        f.write("### 1. Patrón de Comportamiento del Visitante\n")
        f.write("- El flujo típico es: **Bienvenida → Cotización de mayoreo → Especifica producto → Deja email**\n")
        f.write("- Los visitantes que mencionan ser **contratistas** tienen mayor probabilidad de conversión\n")
        f.write("- Los que preguntan por **múltiples productos** son compradores de volumen\n\n")
        
        f.write("### 2. Ventana de Oportunidad\n")
        f.write("- **0-3 días:** Lead caliente, contactar inmediatamente\n")
        f.write("- **4-7 días:** Aún tiene interés, enviar cotización personalizada\n")
        f.write("- **8-14 días:** Puede haber comprado en otro lado, ofrecer valor diferencial\n")
        f.write("- **15+ días:** Incluir en campaña de nurturing por email\n\n")
        
        f.write("### 3. Script Sugerido para Primer Contacto\n\n")
        f.write("**Para cotización de mayoreo:**\n")
        f.write("> \"Hola [Nombre], soy [Tu nombre] de Proconsa. Recibimos tu solicitud de cotización ")
        f.write("de [producto] a través de nuestro chat. Te envío la cotización adjunta con precios ")
        f.write("especiales de mayoreo. ¿Tienes alguna duda o necesitas agregar algo más?\"\n\n")
        
        f.write("**Para contratistas:**\n")
        f.write("> \"Hola [Nombre], soy [Tu nombre] de Proconsa. Vi que nos contactaste por [producto]. ")
        f.write("Tenemos un programa especial para contratistas con precios preferenciales y línea de crédito. ")
        f.write("¿Te gustaría que te explique los beneficios?\"\n\n")
        
        f.write("**Para talleres/clínicas:**\n")
        f.write("> \"Hola [Nombre], gracias por tu interés en nuestros talleres. La próxima clínica es ")
        f.write("[tema] el [fecha]. Es totalmente gratuita. ¿Te confirmo tu lugar?\"\n\n")
        
        f.write("### 4. Métricas de Seguimiento Sugeridas\n")
        f.write("- **Tasa de contacto:** % de leads contactados vs. total\n")
        f.write("- **Tasa de respuesta:** % de leads que respondieron al contacto\n")
        f.write("- **Tasa de conversión:** % de leads que realizaron compra\n")
        f.write("- **Ticket promedio:** Valor promedio de venta de leads del chat\n")
        f.write("- **Tiempo de respuesta:** Horas entre chat y primer contacto\n\n")
        
        # Archivos generados
        f.write("---\n\n")
        f.write("## ARCHIVOS GENERADOS\n\n")
        f.write("| Archivo | Descripción | Uso |\n|---|---|---|\n")
        f.write("| `LEADS_SEGUIMIENTO_MARKETING.csv` | Lista completa de leads con email, priorizada | **Archivo principal de trabajo** |\n")
        f.write("| `LEADS_CONVERSACIONES_COMPLETAS.csv` | Conversaciones íntegras de cada lead | Referencia para contexto antes de llamar |\n")
        f.write("| `LEADS_SIN_EMAIL_OPORTUNIDADES.csv` | Sesiones con intención de compra pero sin email | Análisis de oportunidades perdidas |\n")
        f.write("| `REPORTE_SEGUIMIENTO_MARKETING.md` | Este reporte | Guía de trabajo para el equipo |\n")
    
    print(f"Reporte marketing: {report_path}")
    
    # Resumen final
    print("\n" + "=" * 70)
    print("REPORTE DE SEGUIMIENTO COMPLETADO")
    print("=" * 70)
    print(f"\nLeads con email (seguimiento directo): {len(leads_with_email)}")
    print(f"  🔴 Prioridad MÁXIMA: {priority_counts.get('🔴 MÁXIMA', 0)}")
    print(f"  🟠 Prioridad ALTA:   {priority_counts.get('🟠 ALTA', 0)}")
    print(f"  🟡 Prioridad MEDIA:  {priority_counts.get('🟡 MEDIA', 0)}")
    print(f"  🔵 Prioridad BAJA:   {priority_counts.get('🔵 BAJA', 0)}")
    print(f"  ⚪ Prioridad MUY BAJA: {priority_counts.get('⚪ MUY BAJA', 0)}")
    print(f"\nClientes existentes: {existing_clients}")
    print(f"Prospectos nuevos:   {new_prospects}")
    print(f"\nArchivos en: {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
