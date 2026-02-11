#!/usr/bin/env python3
"""
Script para agregar masivamente contactos con email a la mailing list de Odoo.
Mailing List: "Contactos con Email" (ID: 3)
"""

import xmlrpc.client
import sys
import time
import json
import os

# Leer credenciales desde odoo_config.json del MCP
CONFIG_PATH = os.path.expanduser("~/Dev/mcp/mcp-odoo/odoo_config.json")
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

URL = cfg["url"]
DB = cfg["db"]
USERNAME = cfg["username"]
PASSWORD = cfg["password"]

MAILING_LIST_ID = 3
BATCH_SIZE = 50  # Contactos por lote

def connect():
    common = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/common")
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    if not uid:
        print("ERROR: No se pudo autenticar con Odoo")
        sys.exit(1)
    print(f"Autenticado correctamente. UID: {uid}")
    models = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/object")
    return uid, models

def get_partners_with_email(uid, models):
    """Obtiene todos los partners con email"""
    partners = models.execute_kw(
        DB, uid, PASSWORD,
        'res.partner', 'search_read',
        [[['email', '!=', False]]],
        {'fields': ['name', 'email'], 'order': 'id asc'}
    )
    print(f"Total de contactos con email encontrados: {len(partners)}")
    return partners

def get_existing_mailing_contacts(uid, models):
    """Obtiene emails ya existentes en la mailing list para evitar duplicados"""
    contacts = models.execute_kw(
        DB, uid, PASSWORD,
        'mailing.contact', 'search_read',
        [[['list_ids', 'in', [MAILING_LIST_ID]]]],
        {'fields': ['email']}
    )
    existing_emails = set(c['email'].strip().lower() for c in contacts if c['email'])
    print(f"Contactos ya existentes en la mailing list: {len(existing_emails)}")
    return existing_emails

def clean_email(email_str):
    """Limpia el email, tomando solo el primero si hay varios"""
    if not email_str:
        return None
    # Algunos emails tienen saltos de línea o múltiples emails
    email = email_str.strip().split('\n')[0].strip()
    email = email.split(',')[0].strip()
    email = email.split(';')[0].strip()
    # Quitar caracteres no válidos al inicio
    email = email.lstrip(': ')
    if '@' not in email:
        return None
    return email

def create_mailing_contacts(uid, models, partners, existing_emails):
    """Crea contactos de mailing en lotes"""
    total = len(partners)
    created = 0
    skipped = 0
    errors = 0
    
    batch = []
    
    for i, partner in enumerate(partners):
        email = clean_email(partner['email'])
        if not email:
            skipped += 1
            continue
            
        if email.lower() in existing_emails:
            skipped += 1
            continue
        
        batch.append({
            'name': partner['name'],
            'email': email,
            'list_ids': [[6, 0, [MAILING_LIST_ID]]]
        })
        existing_emails.add(email.lower())  # Evitar duplicados dentro del mismo proceso
        
        if len(batch) >= BATCH_SIZE:
            try:
                models.execute_kw(
                    DB, uid, PASSWORD,
                    'mailing.contact', 'create',
                    [batch]
                )
                created += len(batch)
                progress = ((i + 1) / total) * 100
                print(f"  Progreso: {progress:.1f}% - Creados: {created} | Omitidos: {skipped} | Errores: {errors}")
            except Exception as e:
                errors += len(batch)
                print(f"  Error en lote: {e}")
            batch = []
    
    # Procesar último lote
    if batch:
        try:
            models.execute_kw(
                DB, uid, PASSWORD,
                'mailing.contact', 'create',
                [batch]
            )
            created += len(batch)
        except Exception as e:
            errors += len(batch)
            print(f"  Error en último lote: {e}")
    
    return created, skipped, errors

def main():
    print("=" * 60)
    print("CARGA MASIVA DE CONTACTOS A MAILING LIST DE ODOO")
    print("=" * 60)
    
    print("\n1. Conectando a Odoo...")
    uid, models = connect()
    
    print("\n2. Obteniendo contactos con email...")
    partners = get_partners_with_email(uid, models)
    
    print("\n3. Verificando contactos existentes en la mailing list...")
    existing_emails = get_existing_mailing_contacts(uid, models)
    
    print(f"\n4. Creando contactos de mailing en lotes de {BATCH_SIZE}...")
    start_time = time.time()
    created, skipped, errors = create_mailing_contacts(uid, models, partners, existing_emails)
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"  Total partners con email: {len(partners)}")
    print(f"  Contactos creados:        {created}")
    print(f"  Contactos omitidos:       {skipped} (duplicados o email inválido)")
    print(f"  Errores:                  {errors}")
    print(f"  Tiempo total:             {elapsed:.1f} segundos")
    print("=" * 60)

if __name__ == "__main__":
    main()
