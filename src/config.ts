import dotenv from 'dotenv';
dotenv.config();

export const config = {
  pg: {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT || '5432', 10),
    database: process.env.PG_DATABASE || 'prices',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || '',
  },
  mssql: {
    server: process.env.MSSQL_SERVER || 'localhost',
    port: parseInt(process.env.MSSQL_PORT || '1433', 10),
    database: process.env.MSSQL_DATABASE || 'LDCOM_PROCONSA',
    user: process.env.MSSQL_USER || '',
    password: process.env.MSSQL_PASSWORD || '',
    encrypt: process.env.MSSQL_ENCRYPT === 'true',
    trustServerCertificate: process.env.MSSQL_TRUST_SERVER_CERT !== 'false',
    empId: parseInt(process.env.MSSQL_EMP_ID || '1', 10),
  },
  sucursalWix: parseInt(process.env.SUCURSAL_WIX || '101', 10),

  wix: {
    siteId: process.env.WIX_SITE_ID || '',
    apiKey: process.env.WIX_API_KEY || '',
    baseUrl: 'https://www.wixapis.com',
  },

  odoo: {
    url: process.env.ODOO_URL || '',
    db: process.env.ODOO_DB || '',
    username: process.env.ODOO_USERNAME || '',
    password: process.env.ODOO_PASSWORD || '',
    livechatChannelId: parseInt(process.env.ODOO_LIVECHAT_CHANNEL_ID || '1', 10),
    reportsDir: process.env.ODOO_REPORTS_DIR || './reports',
  },

  smtp: {
    host: process.env.SMTP_HOST || '',
    port: parseInt(process.env.SMTP_PORT || '587', 10),
    secure: process.env.SMTP_SECURE === 'true',
    user: process.env.SMTP_USER || '',
    pass: process.env.SMTP_PASS || '',
    from: process.env.SMTP_FROM || '',
  },
  marketingEmails: (process.env.MARKETING_EMAILS || '').split(',').map(e => e.trim()).filter(Boolean),

  timezone: process.env.TZ || 'America/Los_Angeles',
  logLevel: process.env.LOG_LEVEL || 'info',
};
