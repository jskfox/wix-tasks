import nodemailer from 'nodemailer';
import { config } from '../config';
import { logger } from '../utils/logger';

const CTX = 'Email';

let transporter: nodemailer.Transporter | null = null;

function getTransporter(): nodemailer.Transporter {
  if (!transporter) {
    transporter = nodemailer.createTransport({
      host: config.smtp.host,
      port: config.smtp.port,
      secure: config.smtp.secure,
      auth: {
        user: config.smtp.user,
        pass: config.smtp.pass,
      },
    });
    logger.info(CTX, `Transporter created: ${config.smtp.host}:${config.smtp.port}`);
  }
  return transporter;
}

export interface SendEmailOptions {
  to: string | string[];
  subject: string;
  html: string;
  text?: string;
}

export async function sendEmail(options: SendEmailOptions): Promise<void> {
  const t = getTransporter();
  const recipients = Array.isArray(options.to) ? options.to.join(', ') : options.to;

  logger.info(CTX, `Sending email to: ${recipients} | Subject: ${options.subject}`);

  const info = await t.sendMail({
    from: config.smtp.from,
    to: recipients,
    subject: options.subject,
    html: options.html,
    text: options.text,
  });

  logger.info(CTX, `Email sent: ${info.messageId}`);
}

export async function verifyConnection(): Promise<boolean> {
  try {
    const t = getTransporter();
    await t.verify();
    logger.info(CTX, 'SMTP connection verified');
    return true;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logger.error(CTX, `SMTP connection failed: ${msg}`);
    return false;
  }
}
