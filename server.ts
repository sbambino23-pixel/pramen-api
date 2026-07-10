// v5.18.0 — Journey reframe LIVE: expanded mix arrays (9 card types), removed startup purge
import { Hono } from "hono";
import { cors } from "hono/cors";
import { streamSSE } from "hono/streaming";
import { serve } from "@hono/node-server";
import { randomUUID, createHash, createSign, createHmac, randomBytes, timingSafeEqual } from "crypto";
import { gunzipSync } from "zlib";
import { readFileSync } from "fs";
import http2 from "http2";
import pg from "pg";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────
interface StoredMember { userId: string; name: string; streakCount: number; lastPrayedDate: string | null; lastPrayedLocalDate?: string | null; lastPrayedTimezone?: string | null; joinedAt: string; canPost?: boolean; notificationsMuted?: boolean; role?: string; avatarUrl?: string; lastSeenAt?: string | null; visible?: boolean; }
interface StoredPrayerRequest { id: string; requesterUserId: string; requesterName: string; text: string; timestamp: string; isAnonymous: boolean; prayedByUserIds: string[]; generatedPrayer?: string; targetUserId?: string; targetType: "circle" | "personal"; status: "active" | "prayed" | "answered"; }
interface StoredCircle { id: string; name: string; code: string; emoji: string; avatarUrl?: string; creatorUserId: string; members: StoredMember[]; prayerRequests: StoredPrayerRequest[]; createdAt: string; }

// ─── Config ──────────────────────────────────────────────────────────
const POSTHOG_API_KEY = process.env.POSTHOG_API_KEY || "";
const POSTHOG_HOST = process.env.POSTHOG_HOST || "https://us.i.posthog.com";
const POSTHOG_READ_HOST = "https://us.posthog.com";
const POSTHOG_PERSONAL_KEY = process.env.POSTHOG_PERSONAL_API_KEY || "";
const POSTHOG_PROJECT_ID = "359922";
const PLAUSIBLE_API_KEY = process.env.PLAUSIBLE_API_KEY || "";
const PLAUSIBLE_SITE_ID = "pramen.app";
const DASHBOARD_SECRET = process.env.DASHBOARD_SECRET || "pramen_dash_2026";
const APPLE_CUT = 0.15;
const ASC_KEY_ID = process.env.ASC_KEY_ID || "";
const ASC_ISSUER_ID = process.env.ASC_ISSUER_ID || "";
const ASC_PRIVATE_KEY = (process.env.ASC_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const PRAMEN_APP_ID = "6759958354";
const REVENUECAT_SECRET_KEY = process.env.REVENUECAT_SECRET_KEY || "";
const APNS_KEY_ID = process.env.APNS_KEY_ID || "";
const APNS_TEAM_ID = process.env.APNS_TEAM_ID || "5QTJL794PU";
const APNS_BUNDLE_ID = process.env.APNS_BUNDLE_ID || "app.rork.faithlock-app-vkrdyww";
const APNS_PRIVATE_KEY = (process.env.APNS_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const APNS_HOST = process.env.APNS_SANDBOX === "true" ? "api.sandbox.push.apple.com" : "api.push.apple.com";
const ADMIN_USER_ID = process.env.ADMIN_USER_ID || "";
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
let geminiBackoffUntil = 0;
const GEMINI_BACKOFF_MS = 6 * 60 * 60 * 1000;
function isGeminiAvailable(): boolean { return Date.now() >= geminiBackoffUntil; }
function markGeminiRateLimited(): void { geminiBackoffUntil = Date.now() + GEMINI_BACKOFF_MS; console.log(`[Gemini] Rate limited — backing off until ${new Date(geminiBackoffUntil).toISOString()}`); }
const RESEND_API_KEY = process.env.RESEND_API_KEY || "";
const RESEND_FROM_EMAIL = process.env.RESEND_FROM_EMAIL || "prAmen <hello@pramen.app>";
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "";
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "pramen-media";
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL || "";
const LOOPS_WEBHOOK_SECRET = process.env.LOOPS_WEBHOOK_SECRET || "";
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || "AIzaSyD0WUdkjl_HBAoaojLM073AK0CuFgb5rro";
const YT_CHANNEL_HANDLE = "fatherjohnprays";
const INSTAGRAM_ACCESS_TOKEN = process.env.INSTAGRAM_ACCESS_TOKEN || "";
const INSTAGRAM_ACCOUNT_ID = process.env.INSTAGRAM_ACCOUNT_ID || "26555458837479438";
const TIKTOK_CLIENT_KEY = process.env.TIKTOK_CLIENT_KEY || "sbawviqnjkoz6ihr4n";
const TIKTOK_CLIENT_SECRET = process.env.TIKTOK_CLIENT_SECRET || "Un5pRilh8Nr0vrgDul2iV0BQ82OSzOIr";
const TIKTOK_REDIRECT_URI = "https://web-production-88ed0.up.railway.app/auth/tiktok/callback";
const TIKTOK_ACCESS_TOKEN = process.env.TIKTOK_ACCESS_TOKEN || "";

// ─── R2 Storage ──────────────────────────────────────────────────────
const s3 = R2_ACCOUNT_ID ? new S3Client({ region: "auto", endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`, credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY } }) : null;
async function uploadMedia(fileData: ArrayBuffer, filename: string, contentType: string): Promise<string> {
  if (!s3) throw new Error("Storage not configured");
  const ext = filename.split(".").pop() || "bin";
  const key = `posts/${Date.now()}-${randomUUID().substring(0, 8)}.${ext}`;
  await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(fileData), ContentType: contentType }));
  return `${R2_PUBLIC_URL}/${key}`;
}
const MAX_FILE_SIZE = 50 * 1024 * 1024;
const ALLOWED_MEDIA: Record<string, string[]> = { image: ["image/jpeg","image/png","image/heic","image/heif"], video: ["video/mp4","video/quicktime"], audio: ["audio/mpeg","audio/mp4","audio/x-m4a","audio/aac","audio/wav","audio/x-wav"] };

// ─── PostHog Helpers ─────────────────────────────────────────────────
function trackEvent(distinctId: string, event: string, properties?: Record<string, any>) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ api_key: POSTHOG_API_KEY, event, distinct_id: distinctId, properties: { ...properties, $lib: "pramen-backend", platform: "ios" }, timestamp: new Date().toISOString() }),
  }).catch((err) => console.error("[PostHog] Track error:", err.message));
}
function identifyUser(distinctId: string, userProperties: Record<string, any>) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ api_key: POSTHOG_API_KEY, event: "$identify", distinct_id: distinctId, properties: { $set: userProperties }, timestamp: new Date().toISOString() }),
  }).catch((err) => console.error("[PostHog] Identify error:", err.message));
}

// ─── Apple JWT ───────────────────────────────────────────────────────
function generateASCToken(): string {
  if (!ASC_KEY_ID || !ASC_ISSUER_ID || !ASC_PRIVATE_KEY) { console.log("[ASC] Missing config:", { key: !!ASC_KEY_ID, issuer: !!ASC_ISSUER_ID, pk: ASC_PRIVATE_KEY.length }); return ""; }
  try {
    const header = Buffer.from(JSON.stringify({ alg: "ES256", kid: ASC_KEY_ID, typ: "JWT" })).toString("base64url");
    const now = Math.floor(Date.now() / 1000);
    const payload = Buffer.from(JSON.stringify({ iss: ASC_ISSUER_ID, iat: now, exp: now + 1200, aud: "appstoreconnect-v1" })).toString("base64url");
    const signer = createSign("SHA256");
    signer.update(`${header}.${payload}`);
    const signature = signer.sign({ key: ASC_PRIVATE_KEY, dsaEncoding: "ieee-p1363" }, "base64url");
    return `${header}.${payload}.${signature}`;
  } catch (err: any) { console.error("[ASC] JWT error:", err.message); return ""; }
}

// ─── APNs JWT & Push ─────────────────────────────────────────────────
let apnsJwtCache: { token: string; generatedAt: number } | null = null;

function generateAPNsJWT(): string {
  if (!APNS_KEY_ID || !APNS_PRIVATE_KEY || !APNS_TEAM_ID) return "";
  if (apnsJwtCache && Date.now() - apnsJwtCache.generatedAt < 50 * 60 * 1000) return apnsJwtCache.token;
  try {
    const header = Buffer.from(JSON.stringify({ alg: "ES256", kid: APNS_KEY_ID })).toString("base64url");
    const now = Math.floor(Date.now() / 1000);
    const payload = Buffer.from(JSON.stringify({ iss: APNS_TEAM_ID, iat: now })).toString("base64url");
    const signer = createSign("SHA256");
    signer.update(`${header}.${payload}`);
    const signature = signer.sign({ key: APNS_PRIVATE_KEY, dsaEncoding: "ieee-p1363" }, "base64url");
    const jwt = `${header}.${payload}.${signature}`;
    apnsJwtCache = { token: jwt, generatedAt: Date.now() };
    return jwt;
  } catch (err: any) { console.error("[APNs] JWT error:", err.message); return ""; }
}

interface PushPayload { title: string; body: string; type: string; circleCode?: string; circleName?: string; extra?: Record<string, any>; }

function recordPushResult(userId: string | undefined, status: string): void {
  if (!userId) return;
  pool.query("UPDATE users SET last_push_status=$1, last_push_at=NOW() WHERE id=$2", [status, userId]).catch(() => {});
}

function sendPush(deviceToken: string, payload: PushPayload, forUserId?: string): void {
  const jwt = generateAPNsJWT();
  if (!jwt || !deviceToken) { recordPushResult(forUserId, "no-jwt-or-token"); return; }
  const apnsPayload = JSON.stringify({ aps: { alert: { title: payload.title, body: payload.body }, sound: "default", badge: 1, "mutable-content": 1 }, type: payload.type, circleCode: payload.circleCode || "", circleName: payload.circleName || "", ...(payload.extra || {}) });
  try {
    const client = http2.connect(`https://${APNS_HOST}`);
    client.on("error", (err) => { console.error("[APNs] Connection error:", err.message); recordPushResult(forUserId, "conn-error"); client.close(); });
    const req = client.request({ ":method": "POST", ":path": `/3/device/${deviceToken}`, authorization: `bearer ${jwt}`, "apns-topic": APNS_BUNDLE_ID, "apns-push-type": "alert", "apns-priority": "10", "apns-expiration": "0", "content-type": "application/json" });
    req.on("response", (headers) => { const status = headers[":status"]; if (status === 200) { recordPushResult(forUserId, "200"); } if (status !== 200) { let body = ""; req.on("data", (chunk: Buffer) => { body += chunk.toString(); }); req.on("end", () => { console.log(`[APNs] Push failed status=${status} token=${deviceToken.substring(0, 8)}... body=${body}`); recordPushResult(forUserId, `${status}:${body.substring(0, 60)}`); client.close(); }); return; } });
    req.on("error", (err) => { console.error("[APNs] Request error:", err.message); });
    req.on("end", () => { client.close(); });
    req.write(apnsPayload); req.end();
  } catch (err: any) { console.error("[APNs] Send error:", err.message); }
}

async function pushToUser(userId: string, payload: PushPayload): Promise<void> {
  // Store notification in DB
  try {
    await pool.query("INSERT INTO notifications (user_id, type, title, body, data) VALUES ($1,$2,$3,$4,$5)",
      [userId, payload.type, payload.title, payload.body, JSON.stringify({ circleCode: payload.circleCode || "", circleName: payload.circleName || "", ...(payload.extra || {}) })]);
  } catch (err: any) { console.error("[Notify] Store error:", err.message); }

  // Check user preferences — default is ON for all types
  let shouldPush = true;
  try {
    const prefs = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [userId]);
    if (prefs.rows[0]) {
      const p = prefs.rows[0];
      const prefMap: Record<string, string> = { prayer_request: "prayer_requests", prayer_request_personal: "prayer_requests", prayer_request_prayed: "prayer_requests", prayer_answered: "prayer_requests", member_joined: "circle_members", streak_milestone: "streak_milestones", streak_at_risk: "streak_reminders", last_one_standing: "streak_reminders", promoted_to_admin: "admin_promotions", removed_from_circle: "removed_from_circle" };
      const col = prefMap[payload.type];
      if (col && p[col] === false) shouldPush = false;
    }
  } catch {}

  if (!shouldPush) return;

  try {
    const result = await pool.query("SELECT device_token FROM users WHERE id=$1 AND device_token IS NOT NULL", [userId]);
    if (result.rows[0]?.device_token) {
      console.log(`[Push] Sending ${payload.type} to ${userId.substring(0,8)}… token=${result.rows[0].device_token.substring(0,12)}…`);
      sendPush(result.rows[0].device_token, payload, userId);
    } else {
      console.log(`[Push] No device_token for user ${userId.substring(0,8)}… (type=${payload.type}). User must open app with notifications enabled.`);
    }
  } catch (err: any) { console.error("[APNs] pushToUser error:", err.message); }
}

async function pushToCircleMembers(circle: StoredCircle, excludeUserId: string, payload: PushPayload): Promise<void> {
  const memberIds = circle.members.filter((m) => m.userId !== excludeUserId && !m.notificationsMuted).map((m) => m.userId);
  for (const uid of memberIds) { pushToUser(uid, payload); }
}

// ═══════════════════════════════════════════════════════════════════
// v5.9.1 — PUSH NOTIFICATION LOCALIZATION
// ═══════════════════════════════════════════════════════════════════
// Every server-side push is translated into the recipient's selected app language
// (stored in users.language). Per-recipient lookup ensures Samy in French and
// Charles in English each get their notification in their own language.

type Lang = "en" | "fr" | "es" | "pt";

async function getUserLanguage(userId: string): Promise<Lang> {
  try {
    const r = await pool.query("SELECT language FROM users WHERE id=$1", [userId]);
    const raw = (r.rows[0]?.language || "en").toString().toLowerCase();
    if (raw === "fr" || raw === "es" || raw === "pt") return raw;
    return "en";
  } catch { return "en"; }
}

const PUSH_STRINGS: Record<string, Partial<Record<Lang, string>> & { en: string }> = {
  // Referrals
  referral_both_title: {
    en: "🎉 You both got 30 days free!",
    fr: "🎉 Vous avez tous les deux 30 jours gratuits !",
    es: "🎉 ¡Ambos obtuvieron 30 días gratis!",
    pt: "🎉 Vocês dois ganharam 30 dias grátis!",
  },
  referral_both_body: {
    en: "Your friend just subscribed. You've both been upgraded to 30 days of premium.",
    fr: "Votre ami vient de s'abonner. Vous bénéficiez tous les deux de 30 jours de premium.",
    es: "Tu amigo se acaba de suscribir. Ambos tienen 30 días de premium.",
    pt: "Seu amigo acabou de assinar. Vocês dois têm 30 dias de premium.",
  },
  referral_reward_title: {
    en: "🎉 Welcome! 30 days free unlocked",
    fr: "🎉 Bienvenue ! 30 jours gratuits débloqués",
    es: "🎉 ¡Bienvenido! 30 días gratis desbloqueados",
    pt: "🎉 Bem-vindo! 30 dias grátis desbloqueados",
  },
  referral_reward_body: {
    en: "Thanks to your friend's invite, you both get 30 days of premium free.",
    fr: "Grâce à l'invitation de votre ami, vous bénéficiez tous les deux de 30 jours de premium gratuits.",
    es: "Gracias a la invitación de tu amigo, ambos obtienen 30 días de premium gratis.",
    pt: "Graças ao convite do seu amigo, vocês dois ganham 30 dias de premium grátis.",
  },
  referral_confirmed_title: {
    en: "🎉 Referral confirmed!",
    fr: "🎉 Parrainage confirmé !",
    es: "🎉 ¡Referido confirmado!",
    pt: "🎉 Indicação confirmada!",
  },
  referral_confirmed_body: {
    en: "Someone you invited just subscribed. Check your rewards!",
    fr: "Une personne que vous avez invitée vient de s'abonner. Consultez vos récompenses !",
    es: "Alguien a quien invitaste acaba de suscribirse. ¡Revisa tus recompensas!",
    pt: "Alguém que você convidou acabou de assinar. Confira suas recompensas!",
  },

  // Member joined
  member_joined_title: {
    en: "👥 {name} joined {circle}!",
    fr: "👥 {name} a rejoint {circle} !",
    es: "👥 ¡{name} se unió a {circle}!",
    pt: "👥 {name} entrou em {circle}!",
  },
  member_joined_body: {
    en: "{count} members are now praying together",
    fr: "{count} membres prient maintenant ensemble",
    es: "{count} miembros ahora oran juntos",
    pt: "{count} membros agora oram juntos",
  },

  // Streak milestone
  streak_milestone_title: {
    en: "🔥 {name} hit a {count}-day streak!",
    fr: "🔥 {name} a atteint {count} jours de suite !",
    es: "🔥 ¡{name} alcanzó una racha de {count} días!",
    pt: "🔥 {name} atingiu {count} dias seguidos!",
  },
  streak_milestone_body: {
    en: "Celebrate their dedication in {circle}",
    fr: "Célébrez leur constance dans {circle}",
    es: "Celebra su dedicación en {circle}",
    pt: "Celebre a dedicação deles em {circle}",
  },

  // Removed from circle
  removed_from_circle_title: {
    en: "You've been removed from a circle",
    fr: "Vous avez été retiré d'un cercle",
    es: "Has sido eliminado de un círculo",
    pt: "Você foi removido de um círculo",
  },
  removed_from_circle_body: {
    en: "You've been removed from {circle}.",
    fr: "Vous avez été retiré de {circle}.",
    es: "Has sido eliminado de {circle}.",
    pt: "Você foi removido de {circle}.",
  },

  // Prayer request (personal — sent to one specific member)
  prayer_request_personal_title: {
    en: "🙏 {name} is asking you to pray",
    fr: "🙏 {name} vous demande de prier",
    es: "🙏 {name} te pide que ores",
    pt: "🙏 {name} está pedindo que você ore",
  },
  prayer_request_personal_title_anon: {
    en: "🙏 Someone is asking you to pray",
    fr: "🙏 Quelqu'un vous demande de prier",
    es: "🙏 Alguien te pide que ores",
    pt: "🙏 Alguém está pedindo que você ore",
  },

  // Prayer request (broadcast to circle)
  prayer_request_title: {
    en: "📿 New prayer request in {circle}",
    fr: "📿 Nouvelle demande de prière dans {circle}",
    es: "📿 Nueva petición de oración en {circle}",
    pt: "📿 Novo pedido de oração em {circle}",
  },
  prayer_request_body_named: {
    en: "{name} shared a prayer request",
    fr: "{name} a partagé une demande de prière",
    es: "{name} compartió una petición de oración",
    pt: "{name} compartilhou um pedido de oração",
  },
  prayer_request_body_anon: {
    en: "Someone shared a prayer request",
    fr: "Quelqu'un a partagé une demande de prière",
    es: "Alguien compartió una petición de oración",
    pt: "Alguém compartilhou um pedido de oração",
  },

  // v5 Phase 8 — LR2/gathering/partner pushes, all four languages LIVE
  // (review waived by Samy Jun 12; drafts from v5-sacred-translations.md §C).
  room_invite_live_title: { en: "{name} is praying in {circle} now \u{1F64F}", fr: "{name} prie dans {circle} en ce moment \u{1F64F}", es: "{name} está orando en {circle} ahora \u{1F64F}", pt: "{name} está orando em {circle} agora \u{1F64F}" },
  room_invite_live_body: { en: "Join {name}", fr: "Rejoignez {name}", es: "Únete a {name}", pt: "Junte-se a {name}" },
  gathering_t10_title: { en: "{name}'s gathering begins soon \u{1F64F}", fr: "Le rassemblement de {name} commence bientôt \u{1F64F}", es: "El encuentro de {name} comienza pronto \u{1F64F}", pt: "O encontro de {name} começa em breve \u{1F64F}" },
  gathering_t10_body: { en: "{circle} prays in 10 minutes", fr: "{circle} prie dans 10 minutes", es: "{circle} ora en 10 minutos", pt: "{circle} ora em 10 minutos" },
  gathering_t0_title: { en: "{name}'s gathering is starting", fr: "Le rassemblement de {name} commence", es: "El encuentro de {name} está comenzando", pt: "O encontro de {name} está começando" },
  gathering_t0_body: { en: "Enter the prayer room \u{1F64F}", fr: "Entrez dans la salle de prière \u{1F64F}", es: "Entra a la sala de oración \u{1F64F}", pt: "Entre na sala de oração \u{1F64F}" },
  partner_accepted_title: { en: "{name} said yes \u{1F64F}", fr: "{name} a dit oui \u{1F64F}", es: "{name} aceptó \u{1F64F}", pt: "{name} aceitou \u{1F64F}" },
  partner_accepted_body: { en: "You're prayer partners now — 30 days of premium for you both.", fr: "Vous êtes partenaires de prière — 30 jours de premium pour vous deux.", es: "Ahora son compañeros de oración — 30 días de premium para los dos.", pt: "Agora vocês são parceiros de oração — 30 dias de premium para os dois." },
  partner_grace_title: { en: "{name} covered your {weekday} \u{1F64F}", fr: "{name} a couvert votre {weekday} \u{1F64F}", es: "{name} cubrió tu {weekday} \u{1F64F}", pt: "{name} cobriu sua {weekday} \u{1F64F}" },
  partner_grace_body: { en: "Your shared streak is safe. Pray today to keep it going together.", fr: "Votre série commune est sauvée. Priez aujourd'hui pour la poursuivre ensemble.", es: "Su racha compartida está a salvo. Oren hoy para mantenerla juntos.", pt: "Sua sequência compartilhada está salva. Orem hoje para mantê-la juntos." },

  // Volley — someone prayed WITH you, unprompted (v5 Phase 4)
  prayed_with_you_title: {
    en: "\u{1F64F} {name} prayed with you",
    fr: "\u{1F64F} {name} a pri\u00e9 avec vous",
    es: "\u{1F64F} {name} or\u00f3 contigo",
    pt: "\u{1F64F} {name} orou com voc\u00ea",
  },
  prayed_with_you_body: {
    en: "Pray for {name} back",
    fr: "Priez pour {name} en retour",
    es: "Ora por {name} tambi\u00e9n",
    pt: "Ore por {name} tamb\u00e9m",
  },

  // Prayer request prayed (someone prayed for your request)
  prayer_request_prayed_title: {
    en: "🙏 {name} prayed for your request",
    fr: "🙏 {name} a prié pour votre demande",
    es: "🙏 {name} oró por tu petición",
    pt: "🙏 {name} orou pelo seu pedido",
  },

  // Prayer answered
  prayer_answered_title: {
    en: "🙌 Prayer answered!",
    fr: "🙌 Prière exaucée !",
    es: "🙌 ¡Oración respondida!",
    pt: "🙌 Oração atendida!",
  },
  prayer_answered_body: {
    en: "{name}'s prayer was answered. {count} people prayed.",
    fr: "La prière de {name} a été exaucée. {count} personnes ont prié.",
    es: "La oración de {name} fue respondida. {count} personas oraron.",
    pt: "A oração de {name} foi atendida. {count} pessoas oraram.",
  },

  // Promoted to admin
  promoted_to_admin_title: {
    en: "You're now an admin 🙏",
    fr: "Vous êtes maintenant administrateur 🙏",
    es: "Ahora eres administrador 🙏",
    pt: "Agora você é administrador 🙏",
  },
  promoted_to_admin_body: {
    en: "{name} made you an admin of {circle}.",
    fr: "{name} vous a nommé administrateur de {circle}.",
    es: "{name} te hizo administrador de {circle}.",
    pt: "{name} tornou você administrador de {circle}.",
  },

  // Streak at risk
  streak_at_risk_title: {
    en: "Your {count}-day streak is at risk",
    fr: "Votre série de {count} jours est en danger",
    es: "Tu racha de {count} días está en riesgo",
    pt: "Sua sequência de {count} dias está em risco",
  },
  streak_at_risk_body: {
    en: "You haven't prayed today. Don't let your streak slip.",
    fr: "Vous n'avez pas prié aujourd'hui. Ne laissez pas votre série s'interrompre.",
    es: "No has orado hoy. No dejes que tu racha se pierda.",
    pt: "Você não orou hoje. Não deixe sua sequência quebrar.",
  },

  // Billing issue
  billing_issue_title: {
    en: "Your prayer subscription needs attention",
    fr: "Votre abonnement de prière nécessite votre attention",
    es: "Tu suscripción de oración necesita atención",
    pt: "Sua assinatura de oração precisa de atenção",
  },
  billing_issue_body: {
    en: "There was a problem with your payment. Update your payment method to keep your Prayer Circles and streak going.",
    fr: "Un problème est survenu avec votre paiement. Mettez à jour votre moyen de paiement pour garder vos Cercles de Prière et votre série.",
    es: "Hubo un problema con tu pago. Actualiza tu método de pago para mantener tus Círculos de Oración y tu racha.",
    pt: "Houve um problema com seu pagamento. Atualize seu método de pagamento para manter seus Círculos de Oração e sua sequência.",
  },

  // Last one standing
  last_one_standing_title: {
    en: "Everyone in {circle} prayed today",
    fr: "Tout le monde dans {circle} a prié aujourd'hui",
    es: "Todos en {circle} oraron hoy",
    pt: "Todos em {circle} oraram hoje",
  },
  last_one_standing_body: {
    en: "You're the last one. We're waiting for you.",
    fr: "Vous êtes le dernier. Nous vous attendons.",
    es: "Eres el último. Te estamos esperando.",
    pt: "Você é o último. Estamos esperando você.",
  },
};

function t(lang: Lang, key: string, params?: Record<string, string | number>): string {
  const entry = PUSH_STRINGS[key];
  if (!entry) return key; // missing key — return raw for easier debugging
  let s = entry[lang] || entry.en || key;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      s = s.replace(new RegExp(`\\{${k}\\}`, "g"), String(v));
    }
  }
  return s;
}

interface LocalizedPushPayload {
  titleKey: string;
  bodyKey: string;
  titleParams?: Record<string, string | number>;
  bodyParams?: Record<string, string | number>;
  type: string;
  circleCode?: string;
  circleName?: string;
  extra?: Record<string, any>;
}

async function pushToUserLocalized(userId: string, p: LocalizedPushPayload): Promise<void> {
  const lang = await getUserLanguage(userId);
  const title = t(lang, p.titleKey, p.titleParams);
  const body = t(lang, p.bodyKey, p.bodyParams);
  await pushToUser(userId, { title, body, type: p.type, circleCode: p.circleCode, circleName: p.circleName, extra: p.extra });
}

async function pushToCircleMembersLocalized(circle: StoredCircle, excludeUserId: string, p: LocalizedPushPayload): Promise<void> {
  const memberIds = circle.members.filter((m) => m.userId !== excludeUserId && !m.notificationsMuted).map((m) => m.userId);
  for (const uid of memberIds) { pushToUserLocalized(uid, p); }
}

// v5.8.2 — silent background push for real-time cross-circle sync.
// Sends content-available:1 to wake iOS app in background so it can refresh
// the circle data immediately, not wait for the next 30s polling tick.
function sendSilentPush(deviceToken: string, circleCode: string): void {
  const jwt = generateAPNsJWT();
  if (!jwt || !deviceToken) return;
  const apnsPayload = JSON.stringify({ aps: { "content-available": 1 }, type: "silent_sync", circleCode });
  try {
    const client = http2.connect(`https://${APNS_HOST}`);
    client.on("error", (err) => { console.error("[APNs silent] Connection error:", err.message); client.close(); });
    const req = client.request({ ":method": "POST", ":path": `/3/device/${deviceToken}`, authorization: `bearer ${jwt}`, "apns-topic": APNS_BUNDLE_ID, "apns-push-type": "background", "apns-priority": "5", "apns-expiration": "0", "content-type": "application/json" });
    req.on("response", (headers) => { const status = headers[":status"]; if (status !== 200) { let body = ""; req.on("data", (chunk: Buffer) => { body += chunk.toString(); }); req.on("end", () => { console.log(`[APNs silent] Push failed status=${status} token=${deviceToken.substring(0, 8)}...`); client.close(); }); return; } });
    req.on("error", (err) => { console.error("[APNs silent] Request error:", err.message); });
    req.on("end", () => { client.close(); });
    req.write(apnsPayload); req.end();
  } catch (err: any) { console.error("[APNs silent] Send error:", err.message); }
}

// ═══════════════════════════════════════════════════════════════════
// v5.9.0 — SSE live events for real-time cross-device sync.
// Every connected user holds a persistent HTTP connection. When any user's
// prayer state changes (or any other event worth broadcasting), the server
// pushes a JSON line to every relevant connected client within ~100ms.
// ═══════════════════════════════════════════════════════════════════
type SseClient = { userId: string; send: (event: { type: string; [k: string]: any }) => Promise<void>; close: () => void };
const sseClients = new Map<string, Set<SseClient>>(); // userId -> clients

function addSseClient(userId: string, client: SseClient): void {
  let set = sseClients.get(userId);
  if (!set) { set = new Set(); sseClients.set(userId, set); }
  set.add(client);
}
function removeSseClient(userId: string, client: SseClient): void {
  const set = sseClients.get(userId);
  if (!set) return;
  set.delete(client);
  if (set.size === 0) sseClients.delete(userId);
}
async function sendSseToUser(userId: string, event: { type: string; [k: string]: any }): Promise<void> {
  const set = sseClients.get(userId);
  if (!set || set.size === 0) return;
  for (const client of set) {
    try { await client.send(event); } catch { /* client gone, will be cleaned up on next heartbeat */ }
  }
}
async function broadcastCircleUpdate(circle: StoredCircle, excludeUserId: string): Promise<void> {
  const event = { type: "circle_updated", code: circle.code, updatedAt: new Date().toISOString() };
  const recipients = circle.members.filter(m => m.userId !== excludeUserId).map(m => m.userId);
  for (const uid of recipients) { await sendSseToUser(uid, event); }
}

// ═══════════════════════════════════════════════════════════════════
// v5 Phase 4 — THE VOLLEY
// BRIGHT LINE: fireVolley is ONLY called from handlers that record a REAL
// prayer by a REAL user (request pray, daily prayer pray, pray-back after
// Amen). Never call it from cron, seeds, previews, or anywhere synthetic.
// ═══════════════════════════════════════════════════════════════════
async function fireVolley(by: { id: string; name: string }, recipientUserId: string, context: "request" | "daily" | "volley", requestId: string | null, circleCode: string | null): Promise<string | null> {
  if (!recipientUserId || recipientUserId === by.id) return null;
  let volleyId: string | null = null;
  try {
    const ins = await pool.query(
      "INSERT INTO volley_events (by_user_id, by_name, recipient_user_id, context, request_id, circle_code) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
      [by.id, by.name || "Someone", recipientUserId, context, requestId, circleCode]
    );
    volleyId = ins.rows[0]?.id || null;
  } catch (err: any) { console.error("[Volley] insert error:", err.message); }

  const eventData = {
    type: "prayed_with_you",
    volleyId,
    byUserId: by.id,
    byName: by.name || "Someone",
    context,
    requestId: requestId || "",
    circleCode: circleCode || "",
    occurredAt: new Date().toISOString()
  };
  // SSE to recipient if connected
  sendSseToUser(recipientUserId, eventData).catch(() => {});
  // Named push (localized) — covers offline recipients
  (async () => {
    try {
      const lang = await getUserLanguage(recipientUserId);
      const name = by.name || "Someone";
      await pushToUser(recipientUserId, {
        title: t(lang, "prayed_with_you_title", { name }),
        body: t(lang, "prayed_with_you_body", { name }),
        type: "prayed_with_you",
        circleCode: circleCode || "",
        circleName: "",
        extra: { volleyId: volleyId || "", byUserId: by.id, byName: name, context, requestId: requestId || "" }
      });
    } catch {}
  })();
  trackEvent(by.id, "prayed_with_you_sent", { context, circle_code: circleCode || "" });
  return volleyId;
}

async function pushSilentSyncToCircle(circle: StoredCircle, excludeUserId: string): Promise<void> {
  const memberIds = circle.members.filter((m) => m.userId !== excludeUserId).map((m) => m.userId);
  if (memberIds.length === 0) return;
  try {
    const result = await pool.query("SELECT id, device_token FROM users WHERE id = ANY($1) AND device_token IS NOT NULL", [memberIds]);
    for (const row of result.rows) {
      if (row.device_token) sendSilentPush(row.device_token, circle.code);
    }
  } catch (err: any) { console.error("[Silent sync] pushSilentSyncToCircle error:", err.message); }
}

// ─── Admin Helpers ───────────────────────────────────────────────────
function isAdmin(userId: string): boolean { return ADMIN_USER_ID !== "" && userId === ADMIN_USER_ID; }
function isCircleAdmin(userId: string, circle: StoredCircle, deviceUserId?: string): boolean { if (isAdmin(userId)) return true; const m = circle.members.find(m => m.userId === userId || (deviceUserId && m.userId === deviceUserId)); return m?.role === "creator" || m?.role === "admin"; }
function isCircleCreator(userId: string, circle: StoredCircle): boolean { return circle.creatorUserId === userId || (circle.members.find(m => m.userId === userId)?.role === "creator"); }
function isMemberOfCircle(userId: string, circle: StoredCircle, deviceUserId?: string): boolean { return circle.members.some(m => m.userId === userId || (deviceUserId && m.userId === deviceUserId)) || isAdmin(userId); }

// ─── Postgres ────────────────────────────────────────────────────────
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_URL?.includes("localhost") ? false : { rejectUnauthorized: false } });

async function initDb(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query(`CREATE TABLE IF NOT EXISTS circles (code TEXT PRIMARY KEY, data JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, apple_user_id TEXT UNIQUE, google_user_id TEXT UNIQUE, email TEXT, name TEXT NOT NULL DEFAULT '', auth_provider TEXT DEFAULT 'apple', auth_token TEXT UNIQUE NOT NULL, device_user_id TEXT, trial_start_date TIMESTAMPTZ, trial_end_date TIMESTAMPTZ, subscription_status TEXT DEFAULT 'none', email_opt_in BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS google_user_id TEXT UNIQUE`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider TEXT DEFAULT 'apple'`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_in BOOLEAN DEFAULT false`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS device_token TEXT`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS device_token_updated_at TIMESTAMPTZ`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT`).catch(() => {});
    // v5.9.1 — store user's selected app language for server-side push notification localization
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'en'`).catch(() => {});
    // v5.20.2 — normalized, matchable email (null for Apple relay). Entitlement/
    // answer matching (Phase 3) runs exclusively on this. Additive, nullable,
    // metadata-only. Stored `email` keeps the raw provider value as-is.
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS verified_email TEXT`).catch(() => {});
    // v5.20.4 — magic-link sign-in tokens. Token hashed at rest, single-use
    // (used_at), short expiry. Additive table.
    await client.query(`CREATE TABLE IF NOT EXISTS magic_links (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      email TEXT NOT NULL,
      token_hash TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      used_at TIMESTAMPTZ,
      ip TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ml_email ON magic_links(email)`).catch(() => {});
    // v5.20.5 — 6-digit code fallback (desktop-email / 50+ case). Same row, same
    // TTL + single-use. Hashed at rest like the token.
    await client.query(`ALTER TABLE magic_links ADD COLUMN IF NOT EXISTS code_hash TEXT`).catch(() => {});
    // v5.20.6 — Recovery-merge: tombstone + grace columns; conflict queue.
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS account_status TEXT DEFAULT 'active'`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS merged_into TEXT`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS grace_until TIMESTAMPTZ`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS merge_conflicts (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      uuid_a TEXT NOT NULL,
      uuid_b TEXT NOT NULL,
      reason TEXT NOT NULL,
      data_states JSONB,
      status TEXT NOT NULL DEFAULT 'open',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      resolved_at TIMESTAMPTZ
    )`);
    // v5.21.0 — Phase 3 web funnel. Quiz answers + lead store, keyed by
    // normalized email. Real table behind recovery-merge transfer item 5
    // (web quiz / pending_intake). user_id links the pending web user.
    await client.query(`CREATE TABLE IF NOT EXISTS web_quiz (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      email TEXT NOT NULL UNIQUE,
      user_id TEXT,
      first_name TEXT,
      answers JSONB NOT NULL DEFAULT '{}'::jsonb,
      quiet_time TEXT,
      door TEXT,
      status TEXT NOT NULL DEFAULT 'lead',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_web_quiz_user ON web_quiz(user_id)`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS user_data (user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, streak_count INTEGER DEFAULT 0, highest_streak INTEGER DEFAULT 0, total_prayers INTEGER DEFAULT 0, total_minutes INTEGER DEFAULT 0, last_prayed_date TIMESTAMPTZ, sessions JSONB DEFAULT '[]'::jsonb, preferences JSONB DEFAULT '{}'::jsonb, circle_codes TEXT[] DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`ALTER TABLE user_data ADD COLUMN IF NOT EXISTS last_prayed_local_date TEXT`).catch(() => {});
    await client.query(`ALTER TABLE user_data ADD COLUMN IF NOT EXISTS last_prayed_timezone TEXT`).catch(() => {});
    // v5.15.0 — circle engagement tracking for hybrid prayer model + gamification
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ`).catch(() => {});
    // v5.15.6 — store RevenueCat customer ID for webhook resolution
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS rc_customer_id TEXT`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_rc_customer_id ON users(rc_customer_id) WHERE rc_customer_id IS NOT NULL`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS circle_engagement (circle_code TEXT NOT NULL, user_id TEXT NOT NULL, day DATE NOT NULL, action_count INTEGER DEFAULT 0, actions JSONB DEFAULT '[]'::jsonb, created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (circle_code, user_id, day))`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_engagement_circle_day ON circle_engagement(circle_code, day)`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_apple_user_id ON users(apple_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_google_user_id ON users(google_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_auth_token ON users(auth_token)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_device_user_id ON users(device_user_id)`);
    await client.query(`CREATE TABLE IF NOT EXISTS link_clicks (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, utm_source TEXT, utm_medium TEXT, utm_campaign TEXT, utm_content TEXT, referrer TEXT, user_agent TEXT, clicked_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_link_clicks_date ON link_clicks(clicked_at)`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_web_metrics (date DATE PRIMARY KEY, visitors INT DEFAULT 0, pageviews INT DEFAULT 0, bounce_rate REAL DEFAULT 0, visit_duration_avg REAL DEFAULT 0, app_store_clicks INT DEFAULT 0, top_sources JSONB DEFAULT '[]', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_revenue (date DATE PRIMARY KEY, new_subscribers INT DEFAULT 0, renewals INT DEFAULT 0, cancellations INT DEFAULT 0, revenue_gross REAL DEFAULT 0, revenue_net REAL DEFAULT 0, mrr REAL DEFAULT 0, plan_breakdown JSONB DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_product_metrics (date DATE PRIMARY KEY, dau INT DEFAULT 0, new_users INT DEFAULT 0, prayers_logged INT DEFAULT 0, circles_created INT DEFAULT 0, invites_accepted INT DEFAULT 0, encouragements_sent INT DEFAULT 0, paywall_views INT DEFAULT 0, plan_taps INT DEFAULT 0, scripture_views INT DEFAULT 0, signups INT DEFAULT 0, account_deletions INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_app_store_metrics (date DATE PRIMARY KEY, impressions INT DEFAULT 0, product_page_views INT DEFAULT 0, app_units INT DEFAULT 0, conversion_rate REAL DEFAULT 0, proceeds REAL DEFAULT 0, active_devices INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS revenue_events (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT, event_type TEXT NOT NULL, plan TEXT, product_id TEXT, price REAL DEFAULT 0, currency TEXT DEFAULT 'USD', environment TEXT DEFAULT 'production', created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS circle_posts (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, circle_code TEXT NOT NULL, author_user_id TEXT NOT NULL, author_name TEXT NOT NULL DEFAULT '', content TEXT, media_type TEXT, media_url TEXT, media_filename TEXT, media_size_bytes INTEGER, status TEXT NOT NULL DEFAULT 'published', scheduled_at TIMESTAMPTZ, published_at TIMESTAMPTZ DEFAULT NOW(), created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_posts_circle_feed ON circle_posts(circle_code, status, published_at DESC)`);
    await client.query(`ALTER TABLE circle_posts ADD COLUMN IF NOT EXISTS tagged_user_ids TEXT[] DEFAULT '{}'`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS post_reactions (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, post_id TEXT NOT NULL, user_id TEXT NOT NULL, emoji TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(post_id, user_id, emoji))`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_reactions_post ON post_reactions(post_id)`);
    await client.query(`CREATE TABLE IF NOT EXISTS post_replies (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, post_id TEXT NOT NULL, author_user_id TEXT NOT NULL, author_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_replies_post ON post_replies(post_id, created_at)`);
    await client.query(`CREATE TABLE IF NOT EXISTS invite_tokens (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, token TEXT UNIQUE NOT NULL, circle_code TEXT NOT NULL, inviter_user_id TEXT NOT NULL, inviter_name TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', accepted_by_user_id TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), accepted_at TIMESTAMPTZ)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_invite_token ON invite_tokens(token)`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_reflections (date DATE PRIMARY KEY, verse TEXT NOT NULL, reference TEXT NOT NULL, reflection TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    // v5.15.0 — daily circle prayers + who prayed them
    await client.query(`CREATE TABLE IF NOT EXISTS circle_daily_prayers (circle_code TEXT NOT NULL, date DATE NOT NULL, prayer_text TEXT NOT NULL, topic TEXT, prayed_by TEXT[] DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (circle_code, date))`);
    // v5 Phase 8 L10n — per-language daily prayers (en row = canonical for prayed_by)
    await client.query(`ALTER TABLE circle_daily_prayers ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'en'`).catch(() => {});
    await client.query(`ALTER TABLE circle_daily_prayers DROP CONSTRAINT IF EXISTS circle_daily_prayers_pkey`).catch(() => {});
    await client.query(`ALTER TABLE circle_daily_prayers ADD CONSTRAINT circle_daily_prayers_pkey PRIMARY KEY (circle_code, date, language)`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_circle_daily_prayers_date ON circle_daily_prayers(date)`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS seasonal_verses (date DATE NOT NULL, lang TEXT NOT NULL DEFAULT 'en', verse TEXT NOT NULL, reference TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (date, lang))`);
    await client.query(`CREATE TABLE IF NOT EXISTS favorites (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, title TEXT, source TEXT NOT NULL DEFAULT 'app', prayer_text TEXT, prayer_id TEXT, media_url TEXT, media_type TEXT, media_filename TEXT, transcript TEXT, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id, is_deleted, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, type TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, data JSONB DEFAULT '{}', is_read BOOLEAN DEFAULT false, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_deleted, created_at DESC)`);
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_push_status TEXT`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_push_at TIMESTAMPTZ`).catch(() => {});
    // v5 Live Room 2.0 (live-room-2-spec.md) — presence-driven, member-
    // initiated. The fixed-band model is DEAD (bands table left in place,
    // disabled, unused). Liveness IS presence, never schedule.
    await client.query(`UPDATE live_bands SET enabled=false`).catch(() => {});
    await client.query(`ALTER TABLE live_prayer_sessions ADD COLUMN IF NOT EXISTS anchor_type TEXT DEFAULT 'daily'`).catch(() => {});
    await client.query(`ALTER TABLE live_prayer_sessions ADD COLUMN IF NOT EXISTS intention_text TEXT`).catch(() => {});
    await client.query(`ALTER TABLE live_prayer_sessions ADD COLUMN IF NOT EXISTS host_id TEXT`).catch(() => {});
    await client.query(`ALTER TABLE live_prayer_sessions ADD COLUMN IF NOT EXISTS ended_at TIMESTAMPTZ`).catch(() => {});
    // HARD anti-spam (§5): one "X is praying now" per member per circle per 4h
    await client.query(`CREATE TABLE IF NOT EXISTS room_notify_log (recipient_user_id TEXT NOT NULL, circle_code TEXT NOT NULL, notified_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (recipient_user_id, circle_code))`);
    // v5 Phase 6.1 — Prayer Partner (one partner per user; spec locked #4)
    await client.query(`CREATE TABLE IF NOT EXISTS partnerships (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_a TEXT NOT NULL, user_b TEXT, status TEXT NOT NULL DEFAULT 'pending', invite_code TEXT UNIQUE NOT NULL, shared_streak INT DEFAULT 0, last_advanced_date DATE, grace_a INT DEFAULT 1, grace_b INT DEFAULT 1, created_at TIMESTAMPTZ DEFAULT NOW(), accepted_at TIMESTAMPTZ)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_partnerships_users ON partnerships(user_a, user_b)`).catch(() => {});
    // Scheduled gatherings = member posts (§4)
    await client.query(`CREATE TABLE IF NOT EXISTS gathering_posts (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, circle_code TEXT NOT NULL, host_id TEXT NOT NULL, host_name TEXT, at_time TIMESTAMPTZ NOT NULL, intention TEXT, joiners TEXT[] DEFAULT '{}', reminded_10 BOOLEAN DEFAULT false, reminded_0 BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    // BRIGHT LINE: presence/counts derive ONLY from live_participants rows,
    // which are ONLY written by the authenticated /live/join + heartbeat +
    // prayed endpoints. No seed path, no synthetic attendees, ever.
    await client.query(`CREATE TABLE IF NOT EXISTS live_prayer_sessions (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, circle_code TEXT NOT NULL, band_key TEXT NOT NULL, scheduled_for TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL, status TEXT NOT NULL DEFAULT 'live', together BOOLEAN DEFAULT false, present_count INT DEFAULT 0, nudged BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE (circle_code, band_key, scheduled_for))`);
    await client.query(`CREATE TABLE IF NOT EXISTS live_prayer_participants (session_id TEXT NOT NULL, user_id TEXT NOT NULL, user_name TEXT, joined_at TIMESTAMPTZ DEFAULT NOW(), last_seen TIMESTAMPTZ DEFAULT NOW(), praying BOOLEAN DEFAULT false, prayed_at TIMESTAMPTZ, left_at TIMESTAMPTZ, PRIMARY KEY (session_id, user_id))`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_live_sessions_status ON live_prayer_sessions(status, window_end)`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS volley_events (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, by_user_id TEXT NOT NULL, by_name TEXT NOT NULL, recipient_user_id TEXT NOT NULL, context TEXT NOT NULL, request_id TEXT, circle_code TEXT, prayed_back BOOLEAN DEFAULT false, occurred_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_volley_recipient ON volley_events(recipient_user_id, occurred_at DESC)`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS notification_preferences (user_id TEXT PRIMARY KEY, encouragements BOOLEAN DEFAULT true, prayers_shared BOOLEAN DEFAULT true, prayer_requests BOOLEAN DEFAULT true, circle_posts BOOLEAN DEFAULT true, post_replies BOOLEAN DEFAULT true, post_reactions BOOLEAN DEFAULT true, circle_members BOOLEAN DEFAULT true, streak_milestones BOOLEAN DEFAULT true, streak_freeze BOOLEAN DEFAULT true, admin_promotions BOOLEAN DEFAULT true, removed_from_circle BOOLEAN DEFAULT true, churches_shared BOOLEAN DEFAULT true, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`ALTER TABLE notification_preferences ADD COLUMN IF NOT EXISTS admin_promotions BOOLEAN DEFAULT true`).catch(() => {});
    await client.query(`ALTER TABLE notification_preferences ADD COLUMN IF NOT EXISTS removed_from_circle BOOLEAN DEFAULT true`).catch(() => {});
    await client.query(`ALTER TABLE notification_preferences ADD COLUMN IF NOT EXISTS churches_shared BOOLEAN DEFAULT true`).catch(() => {});
    await client.query(`ALTER TABLE notification_preferences ADD COLUMN IF NOT EXISTS streak_reminders BOOLEAN DEFAULT true`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS encouragements (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, message TEXT NOT NULL, is_prayed BOOLEAN DEFAULT false, prayed_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`ALTER TABLE encouragements ADD COLUMN IF NOT EXISTS is_prayed BOOLEAN DEFAULT false`).catch(() => {});
    await client.query(`ALTER TABLE encouragements ADD COLUMN IF NOT EXISTS prayed_at TIMESTAMPTZ`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_encouragements_recipient ON encouragements(recipient_user_id, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS prayer_shares (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, prayer_id TEXT, prayer_title TEXT, prayer_text TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_prayer_shares_recipient ON prayer_shares(recipient_user_id, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS shared_prayers (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, favorite_id TEXT, note TEXT, prayer_text TEXT, prayer_title TEXT, source TEXT, media_url TEXT, media_type TEXT, transcript TEXT, is_saved BOOLEAN DEFAULT false, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_shared_prayers_recipient ON shared_prayers(recipient_user_id, is_deleted, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS referral_codes (user_id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_referral_code ON referral_codes(code)`);
    await client.query(`CREATE TABLE IF NOT EXISTS referrals (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, referrer_user_id TEXT NOT NULL, referred_user_id TEXT, referred_email TEXT, status TEXT NOT NULL DEFAULT 'pending', confirmed_at TIMESTAMPTZ, reversed_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_user_id, status)`);
    await client.query(`CREATE TABLE IF NOT EXISTS referral_rewards (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, tier INT NOT NULL, reward_type TEXT NOT NULL, granted_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(user_id, tier))`);
    await client.query(`CREATE TABLE IF NOT EXISTS church_profiles (place_id TEXT PRIMARY KEY, name TEXT, address TEXT, lat REAL, lng REAL, phone TEXT, website TEXT, rating REAL, rating_count INT, denomination TEXT, opening_hours JSONB, photos JSONB DEFAULT '[]', enrichment_status TEXT DEFAULT 'pending', year_founded TEXT, architectural_style TEXT, patron_saint TEXT, diocese TEXT, description TEXT, notable_features JSONB DEFAULT '[]', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS saved_churches (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, place_id TEXT NOT NULL, church_name TEXT, address TEXT, lat REAL, lng REAL, tags TEXT[] DEFAULT '{}', review TEXT, notes TEXT, rating INT, photos JSONB DEFAULT '[]', is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_saved_churches_user ON saved_churches(user_id, is_deleted)`);
    await client.query(`CREATE TABLE IF NOT EXISTS church_shares (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', saved_church_id TEXT NOT NULL, circle_code TEXT NOT NULL, note TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS invite_emails (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, referrer_user_id TEXT NOT NULL, friend_name TEXT NOT NULL, friend_email TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'sent', referral_code TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_invite_emails_referrer ON invite_emails(referrer_user_id, created_at DESC)`);
    // Enforce one reaction per user per post (drop old unique constraint, add new one)
    await client.query(`ALTER TABLE post_reactions DROP CONSTRAINT IF EXISTS post_reactions_post_id_user_id_emoji_key`).catch(() => {});
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_reactions_one_per_user ON post_reactions(post_id, user_id)`).catch(() => {});
    // v5.3.0 — one ask per day per (asker, target) in a circle
    await client.query(`CREATE TABLE IF NOT EXISTS prayer_ask_log (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, circle_code TEXT NOT NULL, asker_user_id TEXT NOT NULL, target_user_id TEXT, day DATE NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_prayer_ask_log_unique ON prayer_ask_log (circle_code, asker_user_id, COALESCE(target_user_id, ''), day)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_prayer_ask_log_lookup ON prayer_ask_log (circle_code, asker_user_id, day)`);
    // v5.8.3 — throttle table for last-one-standing and similar once-per-day pushes
    await client.query(`CREATE TABLE IF NOT EXISTS push_throttle (throttle_key TEXT PRIMARY KEY, sent_date TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_ad_metrics (date DATE NOT NULL, channel TEXT NOT NULL, campaign TEXT NOT NULL DEFAULT 'all', spend NUMERIC DEFAULT 0, impressions INTEGER DEFAULT 0, clicks INTEGER DEFAULT 0, installs INTEGER DEFAULT 0, trials INTEGER DEFAULT 0, subscriptions INTEGER DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (date, channel, campaign))`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_organic_metrics (date DATE NOT NULL, channel TEXT NOT NULL, views INTEGER DEFAULT 0, subscribers_gained INTEGER DEFAULT 0, likes INTEGER DEFAULT 0, comments INTEGER DEFAULT 0, shares INTEGER DEFAULT 0, watch_hours REAL DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (date, channel))`);
    // v5.12.0 — promo codes for influencer outreach
    await client.query(`CREATE TABLE IF NOT EXISTS promo_codes (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, code TEXT UNIQUE NOT NULL, duration TEXT NOT NULL DEFAULT 'monthly', campaign TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL DEFAULT 'admin', redeemed_by_user_id TEXT, redeemed_at TIMESTAMPTZ, expires_at TIMESTAMPTZ, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_promo_codes_code ON promo_codes(code)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_promo_codes_campaign ON promo_codes(campaign)`);
    // v5.15.0 — outreach contacts tracking
    await client.query(`CREATE TABLE IF NOT EXISTS outreach_contacts (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      platform TEXT NOT NULL,
      handle TEXT NOT NULL,
      name TEXT NOT NULL DEFAULT '',
      followers TEXT DEFAULT '',
      category TEXT DEFAULT '',
      contact_method TEXT DEFAULT '',
      contact_email TEXT DEFAULT '',
      rate TEXT DEFAULT '',
      promo_code TEXT DEFAULT '',
      status TEXT NOT NULL DEFAULT 'sent',
      outreach_date DATE DEFAULT CURRENT_DATE,
      their_response TEXT DEFAULT '',
      our_reply TEXT DEFAULT '',
      next_step TEXT DEFAULT '',
      notes TEXT DEFAULT '',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_outreach_platform ON outreach_contacts(platform)`);
    // ═══════════════════════════════════════════════════════════════════
    // v5.16.0 — JOURNEYS Phase 0: schema migrations
    // ═══════════════════════════════════════════════════════════════════
    // 1. Add family column to existing circles table
    await client.query(`ALTER TABLE circles ADD COLUMN IF NOT EXISTS family TEXT`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_circles_family ON circles(family)`).catch(() => {});
    // 2. Journey instances
    await client.query(`CREATE TABLE IF NOT EXISTS journey_instances (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      user_id TEXT NOT NULL REFERENCES users(id),
      template_key TEXT NOT NULL,
      family TEXT NOT NULL,
      mode TEXT NOT NULL,
      unit TEXT NOT NULL DEFAULT 'day',
      length_days INT,
      current_day INT NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'active',
      prayed_for_name TEXT,
      circle_id TEXT,
      partner_id TEXT,
      started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_action_at TIMESTAMPTZ
    )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ji_user ON journey_instances(user_id)`).catch(() => {});
    await client.query(`CREATE INDEX IF NOT EXISTS idx_ji_family ON journey_instances(family)`).catch(() => {});
    // 3. Idempotent cache of generated daily actions
    await client.query(`CREATE TABLE IF NOT EXISTS journey_daily_actions (
      instance_id TEXT NOT NULL REFERENCES journey_instances(id),
      day INT NOT NULL,
      lang TEXT NOT NULL,
      type TEXT NOT NULL,
      phase_label TEXT NOT NULL,
      content_json JSONB NOT NULL,
      generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (instance_id, day, lang)
    )`);
    // 4. Partner requests (journey-aware, mutual consent)
    await client.query(`CREATE TABLE IF NOT EXISTS partner_requests (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      from_user TEXT NOT NULL REFERENCES users(id),
      to_user TEXT NOT NULL REFERENCES users(id),
      from_instance TEXT REFERENCES journey_instances(id),
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
    // ═══════════════════════════════════════════════════════════════════
    // v5.16.3 — JOURNEYS Phase 3: partner privacy + blocks
    // ═══════════════════════════════════════════════════════════════════
    await client.query(`ALTER TABLE journey_instances ADD COLUMN IF NOT EXISTS open_to_partner BOOLEAN NOT NULL DEFAULT FALSE`).catch(() => {});
    // v5.20.0 — Tier-1: record the manifest door + dominant emotion so the content
    // matrix can select scripture by {spine x phase x emotion}. Nullable/additive.
    await client.query(`ALTER TABLE journey_instances ADD COLUMN IF NOT EXISTS door TEXT`).catch(() => {});
    await client.query(`ALTER TABLE journey_instances ADD COLUMN IF NOT EXISTS dominant_emotion TEXT`).catch(() => {});
    // v5.20.18 — quiet time as LOCAL time-of-day "HH:mm" (never UTC). Feeds reminder scheduling.
    await client.query(`ALTER TABLE journey_instances ADD COLUMN IF NOT EXISTS quiet_time TEXT`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS partner_blocks (
      blocker TEXT NOT NULL REFERENCES users(id),
      blocked TEXT NOT NULL REFERENCES users(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (blocker, blocked)
    )`);
    console.log("DB initialized (v5.16.3 — Journeys Phase 0-3 schema)");
  } catch (err) { console.error("DB init failed:", err); } finally { client.release(); }
}

// ─── Circle Cache ────────────────────────────────────────────────────
const circles = new Map<string, StoredCircle>();
async function loadAllFromDb(): Promise<void> { try { const r = await pool.query("SELECT code, data, family FROM circles"); for (const row of r.rows) { const circle = row.data as StoredCircle; (circle as any).family = row.family || "drawing_closer"; circles.set(row.code, circle); } console.log(`Loaded ${circles.size} circles`); } catch (err) { console.error("Load circles:", err); } }
async function saveCircleToDb(circle: StoredCircle): Promise<void> { const k = circle.code.toUpperCase(); circles.set(k, circle); const family = (circle as any).family || "drawing_closer"; try { await pool.query(`INSERT INTO circles (code,data,family,updated_at) VALUES ($1,$2,$3,NOW()) ON CONFLICT (code) DO UPDATE SET data=$2,family=$3,updated_at=NOW()`, [k, JSON.stringify(circle), family]); } catch (err) { console.error("Save circle:", err); } }
async function deleteCircleFromDb(code: string): Promise<boolean> { const k = code.toUpperCase(); const e = circles.delete(k); try { await pool.query("DELETE FROM circles WHERE code=$1", [k]); } catch {} return e; }
function getCircle(code: string): StoredCircle | undefined { return circles.get(code.toUpperCase()); }
function generateCircleCode(): string { const ch = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; let c = ""; for (let i = 0; i < 6; i++) c += ch[Math.floor(Math.random() * ch.length)]; if (circles.has(c)) return generateCircleCode(); return c; }

// ─── Auth Helpers ────────────────────────────────────────────────────
function generateAuthToken(): string { return randomUUID() + "-" + randomUUID(); }
async function getUserByToken(token: string) { if (!token) return null; try { const r = await pool.query("SELECT * FROM users WHERE auth_token=$1", [token]); return r.rows[0] || null; } catch { return null; } }
async function getUserByAppleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE apple_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByGoogleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE google_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
// v5.20.2 — Email normalization + verified-email semantics (locked).
//   normalizeEmail: trim + lowercase (null-safe). Used on BOTH sides of every
//     email comparison; stored values are NOT backfilled yet (waits for the
//     collision-review + UNIQUE constraint).
//   verifiedEmailFor: the matchable email. Google → verified; Apple non-relay →
//     verified; Apple relay (@privaterelay.appleid.com) → null (never matchable,
//     recovery-flow only); magic-link → verified.
function normalizeEmail(e?: string | null): string | null {
  if (!e) return null;
  const t = e.trim().toLowerCase();
  return t || null;
}
function verifiedEmailFor(provider: string, rawEmail?: string | null): string | null {
  const norm = normalizeEmail(rawEmail);
  if (!norm) return null;
  if (provider === "apple" && norm.endsWith("@privaterelay.appleid.com")) return null;
  return norm; // google, apple-non-relay, magic → matchable
}
// v5.21.0 — RC Web Billing App User ID, derived deterministically from email.
// MUST byte-match the iOS JourneyRouter.canonicalRCUserID(email) so a web
// purchase and the app's post-sign-in logIn land on the SAME RC customer.
function rcAppUserIdForEmail(email: string): string {
  const norm = (email || "").trim().toLowerCase();
  return "rcu_" + createHash("sha256").update(norm).digest("hex");
}
// Comparison uses lower(trim()) on BOTH sides so it matches legacy un-normalized rows.
async function getUserByEmail(email: string) { try { const r = await pool.query("SELECT * FROM users WHERE lower(trim(email))=lower(trim($1))", [email]); return r.rows[0] || null; } catch { return null; } }

// ═══════════════════════════════════════════════════════════════════
// v5.20.4 — Reusable mail module + magic-link sign-in.
// Provider-agnostic: MAIL_PROVIDER env ("resend" default | "postmark"), key
// from env only. Resend→Postmark is a config swap. Dark until a key is set
// (send() no-ops + logs; endpoints still function). Serves magic-link now,
// recovery-merge + lifecycle email later.
// ═══════════════════════════════════════════════════════════════════
const MAIL_PROVIDER = process.env.MAIL_PROVIDER || "resend";
const MAIL_FROM = process.env.MAIL_FROM || "prAmen <signin@pramen.app>";
async function sendMail(opts: { to: string; subject: string; html: string; text: string; from?: string }): Promise<{ ok: boolean; provider: string; skipped?: boolean; error?: string; id?: string; status?: number }> {
  try {
    const resendKey = process.env.RESEND_API_KEY;
    const postmarkKey = process.env.POSTMARK_API_KEY;
    if (MAIL_PROVIDER === "resend" && resendKey) {
      const res = await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: { Authorization: `Bearer ${resendKey}`, "Content-Type": "application/json" },
        body: JSON.stringify({ from: opts.from || MAIL_FROM, to: [opts.to], subject: opts.subject, html: opts.html, text: opts.text }),
      });
      const body: any = await res.json().catch(() => ({}));
      if (!res.ok) return { ok: false, provider: "resend", status: res.status, error: typeof body?.message === "string" ? body.message : `HTTP ${res.status}` };
      return { ok: true, provider: "resend", id: body?.id, status: res.status };
    }
    if (MAIL_PROVIDER === "postmark" && postmarkKey) {
      const res = await fetch("https://api.postmarkapp.com/email", {
        method: "POST",
        headers: { "X-Postmark-Server-Token": postmarkKey, "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ From: MAIL_FROM, To: opts.to, Subject: opts.subject, HtmlBody: opts.html, TextBody: opts.text }),
      });
      if (!res.ok) return { ok: false, provider: "postmark", error: `HTTP ${res.status}` };
      return { ok: true, provider: "postmark" };
    }
    console.log(`[mail] SKIPPED (no ${MAIL_PROVIDER} key) → ${opts.to}: ${opts.subject}`);
    return { ok: false, provider: MAIL_PROVIDER, skipped: true };
  } catch (err: any) { return { ok: false, provider: MAIL_PROVIDER, error: err.message }; }
}
function mailConfigured(): boolean {
  return (MAIL_PROVIDER === "resend" && !!process.env.RESEND_API_KEY) || (MAIL_PROVIDER === "postmark" && !!process.env.POSTMARK_API_KEY);
}

// ── Magic-link security primitives ──────────────────────────────────
const MAGIC_TTL_MS = 10 * 60 * 1000; // 10 minutes
function hashToken(raw: string): string { return createHash("sha256").update(raw).digest("hex"); }
function constantTimeEqualHex(a: string, b: string): boolean {
  try { const ba = Buffer.from(a, "hex"), bb = Buffer.from(b, "hex"); if (ba.length !== bb.length) return false; return timingSafeEqual(ba, bb); } catch { return false; }
}
// In-memory rate limiter (single instance): per-email + per-IP sliding window.
const magicRate = { byEmail: new Map<string, number[]>(), byIp: new Map<string, number[]>() };
function rateOk(map: Map<string, number[]>, key: string, max: number, windowMs: number): boolean {
  const now = Date.now();
  const arr = (map.get(key) || []).filter(t => now - t < windowMs);
  if (arr.length >= max) { map.set(key, arr); return false; }
  arr.push(now); map.set(key, arr); return true;
}
async function issueMagicLink(normEmail: string, ip: string): Promise<{ raw: string; code: string }> {
  const raw = randomBytes(32).toString("base64url");
  // 6-digit code from a CSPRNG (fallback for the desktop-email / 50+ case).
  const code = String(100000 + (randomBytes(4).readUInt32BE(0) % 900000));
  const expiresAt = new Date(Date.now() + MAGIC_TTL_MS).toISOString();
  await pool.query("INSERT INTO magic_links (email, token_hash, code_hash, expires_at, ip) VALUES ($1,$2,$3,$4,$5)", [normEmail, hashToken(raw), hashToken(code), expiresAt, ip]);
  return { raw, code };
}
// Verify + single-use consume via token OR 6-digit code; create-or-link the
// user (magic-link → verified email). Prefetch-safe: only THIS (called from a
// POST) consumes — GET/prefetch never touches token state.
async function consumeMagicToken(normEmail: string, rawToken?: string | null, code?: string | null, deviceUserId?: string | null, createIfMissing: boolean = true): Promise<{ user?: any; isNewUser?: boolean; verified?: boolean; error?: string }> {
  const tokenH = rawToken ? hashToken(rawToken) : null;
  const codeH = code ? hashToken(String(code)) : null;
  if (!tokenH && !codeH) return { error: "missing_credential" };
  const rows = (await pool.query("SELECT id, token_hash, code_hash FROM magic_links WHERE email=$1 AND used_at IS NULL AND expires_at > now() ORDER BY created_at DESC LIMIT 5", [normEmail])).rows;
  let match: any = null;
  for (const r of rows) {
    if (tokenH && constantTimeEqualHex(r.token_hash, tokenH)) { match = r; break; }
    if (codeH && r.code_hash && constantTimeEqualHex(r.code_hash, codeH)) { match = r; break; }
  }
  if (!match) return { error: "invalid_or_expired" };
  const upd = await pool.query("UPDATE magic_links SET used_at=now() WHERE id=$1 AND used_at IS NULL RETURNING id", [match.id]);
  if (upd.rows.length === 0) return { error: "already_used" }; // race guard → single-use
  let user = await getUserByEmail(normEmail);
  let isNewUser = false;
  const ve = verifiedEmailFor("magic", normEmail);
  // Recovery mode: credential proven, but don't fabricate an account.
  if (!user && !createIfMissing) return { verified: true, user: null };
  if (!user) {
    isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID();
    await pool.query("INSERT INTO users (id,email,verified_email,name,auth_provider,auth_token,device_user_id,subscription_status) VALUES ($1,$2,$3,'','email',$4,$5,'none')", [userId, normEmail, ve, authToken, deviceUserId || null]);
    await pool.query("INSERT INTO user_data (user_id) VALUES ($1)", [userId]);
    user = { id: userId, email: normEmail, name: "", auth_token: authToken, subscription_status: "none", trial_start_date: null, trial_end_date: null, device_user_id: deviceUserId || null };
    trackEvent(userId, "user_signed_up", { auth_provider: "magic" });
  } else {
    await pool.query("UPDATE users SET verified_email=COALESCE(verified_email,$1),updated_at=NOW() WHERE id=$2", [ve, user.id]);
  }
  return { user, isNewUser };
}
function magicEmailHtml(link: string, code: string): string {
  return `<div style="font-family:Georgia,serif;color:#2b2118;max-width:440px;margin:0 auto;padding:24px">
    <p style="font-size:20px">Your prAmen sign-in link</p>
    <p style="color:#6a5c48">Tap below to sign in. This works once and expires in 10 minutes.</p>
    <p style="margin:20px 0"><a href="${link}" style="background:#b9612d;color:#fff;padding:14px 24px;border-radius:12px;text-decoration:none">Sign in to prAmen</a></p>
    <p style="color:#6a5c48">Or enter this code in the app:</p>
    <p style="font-size:30px;letter-spacing:6px;font-weight:bold;color:#b9612d;margin:6px 0">${code}</p>
    <p style="color:#9c8f7c;font-size:12px">If you didn't request this, you can ignore it.</p></div>`;
}
// Verify-attempt rate limit (per email) — brute-force protection for the 6-digit code.
const magicVerifyRate = new Map<string, number[]>();
async function getUserData(userId: string) { try { const r = await pool.query("SELECT * FROM user_data WHERE user_id=$1", [userId]); if (r.rows[0]) { const d = r.rows[0]; let streakCount = d.streak_count || 0; if (streakCount > 0 && d.last_prayed_date) { const now = new Date(); const lastPrayed = new Date(d.last_prayed_date); const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const yesterday = new Date(today.getTime() - 86400000); const lastPrayedDay = new Date(lastPrayed.getFullYear(), lastPrayed.getMonth(), lastPrayed.getDate()); if (lastPrayedDay < yesterday) { streakCount = 0; await pool.query("UPDATE user_data SET streak_count=0, updated_at=NOW() WHERE user_id=$1", [userId]); } } return { streakCount, highestStreak: d.highest_streak, totalPrayers: d.total_prayers, totalMinutes: d.total_minutes, lastPrayedDate: d.last_prayed_date, sessions: d.sessions || [], preferences: d.preferences || {}, circleCodes: d.circle_codes || [] }; } return null; } catch { return null; } }
function getUserCircleCodes(...userIds: string[]): string[] { const ids = new Set(userIds.filter(Boolean)); const codes: string[] = []; for (const [code, circle] of circles) { if (circle.members.some(m => ids.has(m.userId))) codes.push(code); } return codes; }
async function migrateCircleMembership(oldId: string, newId: string, name: string) { for (const [, c] of circles) { const m = c.members.find(m => m.userId === oldId); if (m) { m.userId = newId; if (name) m.name = name; await saveCircleToDb(c); } if (c.creatorUserId === oldId) { c.creatorUserId = newId; await saveCircleToDb(c); } } }
async function requireAuth(c: any): Promise<any | null> { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return null; const u = await getUserByToken(ah.replace("Bearer ", "")); if (u) { pool.query("UPDATE users SET last_seen_at=NOW() WHERE id=$1", [u.id]).catch(() => {}); } return u; }

// ─── Circle Engagement (v5.15.0) ─────────────────────────────────────
async function recordCircleEngagement(circleCode: string, userId: string, actionType: string, extra?: Record<string, any>): Promise<void> {
  try {
    const today = new Date().toISOString().split("T")[0];
    const action = { type: actionType, at: new Date().toISOString(), ...extra };
    await pool.query(
      `INSERT INTO circle_engagement (circle_code, user_id, day, action_count, actions) VALUES ($1,$2,$3,1,$4::jsonb)
       ON CONFLICT (circle_code, user_id, day) DO UPDATE SET action_count = circle_engagement.action_count + 1, actions = circle_engagement.actions || $4::jsonb`,
      [circleCode, userId, today, JSON.stringify([action])]
    );
  } catch (err: any) { console.error("[Engagement] Record error:", err.message); }
}

function getMemberLastSeen(member: StoredMember): { isActive: boolean; daysSinceLastSeen: number } {
  if (!member.lastSeenAt) {
    // Fallback: use lastPrayedDate or joinedAt
    const fallback = member.lastPrayedDate || member.joinedAt;
    if (!fallback) return { isActive: true, daysSinceLastSeen: 0 }; // New members default to active
    const days = Math.floor((Date.now() - new Date(fallback).getTime()) / (86400000));
    return { isActive: days <= 14, daysSinceLastSeen: days };
  }
  const days = Math.floor((Date.now() - new Date(member.lastSeenAt).getTime()) / (86400000));
  return { isActive: days <= 14, daysSinceLastSeen: days };
}

async function getCircleEngagementForDay(circleCode: string, day: string): Promise<Set<string>> {
  try {
    const r = await pool.query("SELECT user_id FROM circle_engagement WHERE circle_code=$1 AND day=$2", [circleCode, day]);
    return new Set(r.rows.map((row: any) => row.user_id));
  } catch { return new Set(); }
}

function computeCircleTier(engagedCount: number, activeCount: number): string | null {
  if (activeCount <= 0 || engagedCount <= 0) return null;
  const pct = (engagedCount / activeCount) * 100;
  // Scale thresholds by circle size
  if (activeCount <= 5) {
    // Small circles: 50/65/75
    if (pct >= 75) return "gold";
    if (pct >= 65) return "silver";
    if (pct >= 50) return "bronze";
  } else if (activeCount <= 15) {
    // Medium circles: 30/50/65
    if (pct >= 65) return "gold";
    if (pct >= 50) return "silver";
    if (pct >= 30) return "bronze";
  } else {
    // Large circles (16+): 20/40/60
    if (pct >= 60) return "gold";
    if (pct >= 40) return "silver";
    if (pct >= 20) return "bronze";
  }
  return null;
}

async function computeConsecutiveGoldDays(circleCode: string, circle: StoredCircle): Promise<number> {
  try {
    const today = new Date();
    let consecutive = 0;
    for (let i = 0; i < 7; i++) {
      const checkDate = new Date(today.getTime() - i * 86400000);
      const dayStr = checkDate.toISOString().split("T")[0];
      const engaged = await getCircleEngagementForDay(circleCode, dayStr);
      const activeMembers = circle.members.filter(m => {
        const info = getMemberLastSeen(m);
        return info.isActive;
      });
      if (activeMembers.length === 0) break;
      const engagedActive = activeMembers.filter(m => engaged.has(m.userId)).length;
      const tier = computeCircleTier(engagedActive, activeMembers.length);
      if (tier === "gold") consecutive++;
      else break;
    }
    return consecutive;
  } catch { return 0; }
}

// ─── Daily Circle Prayers (v5.15.0) ──────────────────────────────────
const COMMUNITY_CIRCLE_TOPICS: Record<string, string> = {
  "DS8RSY": "night prayers and evening peace",
  "LE2AA4": "morning prayers and starting the day with God",
  "Z4KTHN": "prayers for hard days and difficult seasons",
  "NGZX5G": "stillness, rest, and finding peace in God's presence",
  "TW6HHP": "praying for each other and intercession"
};

function isCommunityCircle(code: string): boolean { return !!COMMUNITY_CIRCLE_TOPICS[code]; }

// v5.15.6 — check if a community circle member is inactive (21+ days since last activity)
function isCommunityMemberActive(m: StoredMember): boolean {
  const lastActivity = m.lastPrayedDate || m.joinedAt;
  if (!lastActivity) return false;
  const daysSince = (Date.now() - new Date(lastActivity).getTime()) / (1000 * 60 * 60 * 24);
  return daysSince <= 21;
}

function getCirclePrayerTopic(circle: StoredCircle): string {
  const communityTopic = COMMUNITY_CIRCLE_TOPICS[circle.code];
  if (communityTopic) return communityTopic;
  // Personal circles: derive from name + active requests
  const recentRequests = circle.prayerRequests.filter(r => r.status === "active").slice(0, 3).map(r => r.text).join("; ");
  if (recentRequests) return `the circle "${circle.name}" where members are praying about: ${recentRequests}`;
  return `the prayer circle "${circle.name}"`;
}

// Seeded fallback prayers — used whenever Gemini is unavailable (missing key,
// rate limited, API error). A circle must NEVER have a null prayer day: a
// seeded human-written prayer is honest; a missing prayer is not.
// Rotation is deterministic per circle+date so circles differ and days vary.
const FALLBACK_CIRCLE_PRAYERS_BY_LANG: Record<Lang, Record<string, string[]>> = {
  // EN pool verbatim (order is LOAD-BEARING: the daily hash picks
  // pool[hash % length] — identical order across languages means the same
  // slot resolves to the SAME prayer in each tongue, matching the
  // prayed_by-on-en-row model. DO NOT REORDER ANY POOL.)
  en: {
  night: [
    "Father, as this day closes, we lay it all down before You. The things we finished and the things we could not. Quiet our minds, settle our hearts, and watch over everyone in this circle tonight. Let us rest knowing You stay awake. Amen.",
    "Lord, the evening has come and we are tired. Thank You for carrying us through this day. Forgive what needs forgiving, heal what aches, and give each of us the deep rest that comes from trusting You. We sleep in Your hands. Amen.",
    "God of peace, gather this circle under Your wing tonight. For everyone lying awake with worry, bring stillness. For everyone weary, bring sleep. Remind us that the world is Yours to hold, not ours. We release it and rest. Amen.",
    "Father, before we sleep we thank You. For breath, for this circle, for mercy that met us today. Guard our homes and the people we love through the night, and wake us tomorrow with hearts ready to seek You first. Amen.",
    "Lord, the dark does not frighten You and so it need not frighten us. Stay close to each member of this circle tonight. Quiet every anxious thought, and let Your peace, which passes understanding, keep our hearts and minds. Amen."
  ],
  morning: [
    "Father, this morning is Yours before it is ours. Set our hearts in order before the day pulls at them. Give everyone in this circle clarity for the work ahead, patience for the people we meet, and eyes to notice You all day long. Amen.",
    "Lord, thank You for waking us to a new day. Your mercies are new this morning, just as You promised. Walk ahead of this circle today. Open the right doors, close the wrong ones, and keep our feet steady on Your path. Amen.",
    "God, before the noise begins, we come to You first. Fill each of us with what we will need today. Strength for the hard parts, gentleness for the people around us, and gratitude for it all. Lead this circle hour by hour. Amen.",
    "Father, the day is unwritten and we trust You with the writing. Bless the work of our hands, guard our words, and let everyone in this circle carry Your peace into every room they enter today. Amen.",
    "Lord, morning by morning You are faithful. We give You the first minutes of this day and ask You to shape the rest of it. Keep this circle close to You and close to each other until evening comes. Amen."
  ],
  hardDays: [
    "Father, some of us are carrying things today that feel too heavy. You said come to Me, all who are weary. So we come. Hold the ones who are barely holding on, and let this circle be proof that no one walks through the hard days alone. Amen.",
    "Lord, You are near to the brokenhearted, and we are counting on that today. For every burden in this circle, seen and unseen, we ask for Your strength. Not the kind that pretends everything is fine, but the kind that endures. Amen.",
    "God, when the valley is long, You are still Shepherd. Walk with each person in this circle through what they are facing. Give grace for today only, and enough light for the next step. We trust You with the rest. Amen.",
    "Father, You do not waste pain. For everyone here in a difficult season, bring comfort that is real and hope that does not embarrass. Teach us to carry each other's burdens, and carry the ones we cannot. Amen.",
    "Lord, we will not pretend today is easy. But we know You are good, even now. Steady the ones who are shaking, soften the ones who have gone numb, and remind this whole circle that weeping may stay the night, but joy comes with the morning. Amen."
  ],
  stillness: [
    "Father, slow us down. The world is loud and our hearts have been running. In this moment, we choose stillness. Be God, and let us stop trying to be. Fill this circle with the quiet confidence of people who know who holds them. Amen.",
    "Lord, You said be still and know. So we are stopping, right here, to know You. Settle our racing thoughts, loosen our gripped hands, and teach everyone in this circle that rest is not laziness. It is trust. Amen.",
    "God of the quiet waters, lead us beside them today. Restore what the rushing has worn down in each of us. Let this circle learn the unforced rhythms of grace, and find that Your yoke really is easy and Your burden light. Amen.",
    "Father, we do not have to earn Your love today, and we needed to remember that. In the stillness, speak. We are listening. Give this circle the courage to rest in You while everything else keeps moving. Amen.",
    "Lord, even the sea obeyed when You said peace, be still. Say it over us now. Over our minds, our schedules, our worries. Let this circle sit in Your presence and leave more whole than we came. Amen."
  ],
  intercession: [
    "Father, we bring each other to You by name today. You know every need in this circle before we speak it. Strengthen the weary ones, encourage the discouraged ones, and let each of us feel the weight of being genuinely prayed for. Amen.",
    "Lord, thank You for the people in this circle. What a grace, to not pray alone. Hear every silent request carried here today. Meet needs we have not said out loud, and knit us closer together as we lift each other up. Amen.",
    "God, You told us to carry one another's burdens, so today we do. For every member of this circle, we ask Your provision, Your protection, and Your nearness. Let no one here wonder whether anyone is praying for them. Someone is. Amen.",
    "Father, make us faithful intercessors. Quick to pray, slow to forget. Bless each person in this circle today in the exact place they need it most, and let them sense, somehow, that they were lifted up. Amen.",
    "Lord, two or three are gathered here in Your name, and You promised to be among us. So be among us. Take every request in this circle, spoken and unspoken, and answer according to Your perfect wisdom and love. Amen."
  ],
  general: [
    "Father, thank You for this circle and this day. Whatever each of us is walking through, meet us there. Give us grateful hearts, honest prayers, and the kind of faith that shows up tomorrow too. We are Yours. Amen.",
    "Lord, You have been our help in days past and You will be our hope in days ahead. Bless every member of this circle today. Provide what is lacking, heal what is hurting, and keep us walking together toward You. Amen.",
    "God, we pause together to say thank You. For family, for friendship, for mercy we did not deserve. Teach this circle to count blessings instead of problems, and to bring both back to You. Amen.",
    "Father, give us strength for today. Not for the whole year, just today. Guard our families, guide our decisions, and let everyone in this circle end this day a little closer to You than they began it. Amen.",
    "Lord, lead us. Where we are confused, bring clarity. Where we are afraid, bring courage. Where we are proud, bring humility. This circle belongs to You, and we trust You with everything in it. Amen.",
    "Father, You see each person in this circle right now, exactly where they sit. Thank You that none of us is invisible to You. Draw near to every heart here, and make our prayers today honest, simple, and full of faith. Amen."
  ]
  },
  fr: {
  night: [
    "Père, à l'heure où ce jour s'achève, nous déposons tout devant Toi. Ce que nous avons accompli et ce que nous n'avons pas pu. Apaise nos pensées, calme nos cœurs, et veille cette nuit sur chacun de ce cercle. Que nous reposions, sachant que Toi, Tu veilles. Amen.",
    "Seigneur, le soir est venu et nous sommes fatigués. Merci de nous avoir portés tout au long de ce jour. Pardonne ce qui doit l'être, guéris ce qui fait mal, et donne à chacun le repos profond qui naît de la confiance en Toi. Nous nous endormons dans Tes mains. Amen.",
    "Dieu de paix, rassemble ce cercle sous Ton aile cette nuit. À ceux qui veillent dans l'inquiétude, donne le calme. À ceux qui sont épuisés, donne le sommeil. Rappelle nous que le monde est entre Tes mains, non les nôtres. Nous le remettons et nous reposons. Amen.",
    "Père, avant de nous endormir, nous Te rendons grâce. Pour le souffle, pour ce cercle, pour la miséricorde qui nous a rejoints aujourd'hui. Garde nos maisons et ceux que nous aimons tout au long de la nuit, et réveille nous demain le cœur prêt à Te chercher d'abord. Amen.",
    "Seigneur, les ténèbres ne T'effraient pas, et elles n'ont donc pas à nous effrayer. Demeure proche de chaque membre de ce cercle cette nuit. Apaise toute pensée anxieuse, et que Ta paix, qui surpasse toute intelligence, garde nos cœurs et nos pensées. Amen."
  ],
  morning: [
    "Père, ce matin est à Toi avant d'être à nous. Mets de l'ordre dans nos cœurs avant que le jour ne les emporte. Donne à chacun de ce cercle la clarté pour la tâche qui vient, la patience envers ceux que nous rencontrerons, et des yeux pour Te reconnaître tout au long du jour. Amen.",
    "Seigneur, merci de nous réveiller pour un jour nouveau. Tes compassions se renouvellent ce matin, comme Tu l'as promis. Marche devant ce cercle aujourd'hui. Ouvre les bonnes portes, ferme les mauvaises, et affermis nos pas sur Ton chemin. Amen.",
    "Dieu, avant que le bruit ne commence, nous venons d'abord à Toi. Remplis chacun de nous de ce dont nous aurons besoin aujourd'hui. La force pour les moments difficiles, la douceur envers ceux qui nous entourent, et la gratitude pour tout cela. Conduis ce cercle heure après heure. Amen.",
    "Père, le jour n'est pas encore écrit, et nous T'en confions l'écriture. Bénis l'œuvre de nos mains, garde nos paroles, et que chacun de ce cercle porte Ta paix dans chaque lieu où il entrera aujourd'hui. Amen.",
    "Seigneur, matin après matin, Tu es fidèle. Nous Te donnons les premières minutes de ce jour et Te demandons d'en façonner le reste. Garde ce cercle proche de Toi et proche les uns des autres jusqu'à la venue du soir. Amen."
  ],
  hardDays: [
    "Père, certains d'entre nous portent aujourd'hui des fardeaux qui semblent trop lourds. Tu as dit : venez à moi, vous tous qui êtes fatigués. Alors nous venons. Soutiens ceux qui tiennent à peine, et que ce cercle soit la preuve que nul ne traverse seul les jours difficiles. Amen.",
    "Seigneur, Tu es près de ceux qui ont le cœur brisé, et c'est sur cela que nous comptons aujourd'hui. Pour chaque fardeau de ce cercle, visible et caché, nous demandons Ta force. Non celle qui fait semblant que tout va bien, mais celle qui tient bon. Amen.",
    "Dieu, lorsque la vallée est longue, Tu demeures le Berger. Marche avec chaque personne de ce cercle à travers ce qu'elle affronte. Donne la grâce pour aujourd'hui seulement, et assez de lumière pour le pas suivant. Nous Te confions le reste. Amen.",
    "Père, Tu ne gaspilles pas la douleur. Pour chacun ici qui traverse une saison difficile, apporte une consolation véritable et une espérance qui ne déçoit pas. Apprends nous à porter les fardeaux les uns des autres, et porte ceux que nous ne pouvons pas. Amen.",
    "Seigneur, nous ne prétendrons pas qu'aujourd'hui est facile. Mais nous savons que Tu es bon, même maintenant. Affermis ceux qui tremblent, adoucis ceux qui se sont engourdis, et rappelle à tout ce cercle que le soir peuvent venir les pleurs, mais qu'au matin revient l'allégresse. Amen."
  ],
  stillness: [
    "Père, ralentis nous. Le monde est bruyant et nos cœurs n'ont cessé de courir. En cet instant, nous choisissons le calme. Sois Dieu, et laisse nous cesser de vouloir l'être. Remplis ce cercle de la confiance paisible de ceux qui savent qui les tient. Amen.",
    "Seigneur, Tu as dit : Arrêtez, et sachez. Alors nous nous arrêtons, ici même, pour Te connaître. Apaise nos pensées agitées, desserre nos mains crispées, et enseigne à chacun de ce cercle que le repos n'est pas paresse. Il est confiance. Amen.",
    "Dieu des eaux paisibles, conduis nous auprès d'elles aujourd'hui. Restaure en chacun de nous ce que la précipitation a épuisé. Que ce cercle apprenne le rythme paisible de la grâce, et découvre que Ton joug est doux et Ton fardeau léger. Amen.",
    "Père, nous n'avons pas à mériter Ton amour aujourd'hui, et nous avions besoin de nous en souvenir. Dans le silence, parle. Nous écoutons. Donne à ce cercle le courage de se reposer en Toi tandis que tout le reste continue de s'agiter. Amen.",
    "Seigneur, même la mer T'a obéi lorsque Tu as dit : Silence ! tais-toi ! Dis le maintenant sur nous. Sur nos pensées, nos emplois du temps, nos inquiétudes. Que ce cercle se tienne en Ta présence et reparte plus entier qu'il n'est venu. Amen."
  ],
  intercession: [
    "Père, nous T'apportons les uns les autres par leur nom aujourd'hui. Tu connais chaque besoin de ce cercle avant même que nous le disions. Fortifie ceux qui sont fatigués, encourage ceux qui sont découragés, et que chacun de nous ressente le poids d'être véritablement porté dans la prière. Amen.",
    "Seigneur, merci pour les personnes de ce cercle. Quelle grâce de ne pas prier seuls. Entends chaque requête silencieuse portée ici aujourd'hui. Réponds aux besoins que nous n'avons pas exprimés à voix haute, et unis nous plus étroitement tandis que nous nous soutenons les uns les autres. Amen.",
    "Dieu, Tu nous as dit de porter les fardeaux les uns des autres, alors aujourd'hui nous le faisons. Pour chaque membre de ce cercle, nous demandons Ta provision, Ta protection et Ta présence. Que nul ici ne se demande si quelqu'un prie pour lui. Quelqu'un le fait. Amen.",
    "Père, fais de nous de fidèles intercesseurs. Prompts à prier, lents à oublier. Bénis chaque personne de ce cercle aujourd'hui à l'endroit précis où elle en a le plus besoin, et qu'elle perçoive, d'une manière ou d'une autre, qu'elle a été portée devant Toi. Amen.",
    "Seigneur, deux ou trois sont assemblés ici en Ton nom, et Tu as promis d'être au milieu de nous. Alors sois au milieu de nous. Prends chaque requête de ce cercle, exprimée ou tue, et réponds selon Ta parfaite sagesse et Ton amour. Amen."
  ],
  general: [
    "Père, merci pour ce cercle et pour ce jour. Quoi que chacun de nous traverse, rejoins nous là. Donne nous des cœurs reconnaissants, des prières sincères, et le genre de foi qui sera encore là demain. Nous sommes à Toi. Amen.",
    "Seigneur, Tu as été notre secours aux jours passés, et Tu seras notre espérance aux jours à venir. Bénis aujourd'hui chaque membre de ce cercle. Pourvois à ce qui manque, guéris ce qui fait souffrir, et garde nous marchant ensemble vers Toi. Amen.",
    "Dieu, nous nous arrêtons ensemble pour Te dire merci. Pour la famille, pour l'amitié, pour la miséricorde que nous n'avions pas méritée. Apprends à ce cercle à compter ses bénédictions plutôt que ses problèmes, et à Te rapporter les unes comme les autres. Amen.",
    "Père, donne nous de la force pour aujourd'hui. Pas pour l'année entière, juste pour aujourd'hui. Garde nos familles, guide nos décisions, et que chacun de ce cercle achève ce jour un peu plus proche de Toi qu'il ne l'a commencé. Amen.",
    "Seigneur, conduis nous. Là où nous sommes dans la confusion, apporte la clarté. Là où nous avons peur, apporte le courage. Là où nous sommes orgueilleux, apporte l'humilité. Ce cercle T'appartient, et nous Te confions tout ce qu'il contient. Amen.",
    "Père, Tu vois chaque personne de ce cercle en cet instant même, exactement là où elle se trouve. Merci de ce que nul d'entre nous ne T'est invisible. Approche Toi de chaque cœur ici, et rends nos prières d'aujourd'hui sincères, simples, et pleines de foi. Amen."
  ]
},
  es: {
  night: [
    "Padre, al cerrarse este día, lo dejamos todo delante de Ti. Lo que terminamos y lo que no pudimos. Aquieta nuestra mente, calma nuestro corazón, y cuida esta noche a cada uno de este círculo. Que descansemos sabiendo que Tú permaneces despierto. Amén.",
    "Señor, ha llegado la noche y estamos cansados. Gracias por sostenernos a lo largo de este día. Perdona lo que deba ser perdonado, sana lo que duele, y danos a cada uno el descanso profundo que nace de confiar en Ti. Dormimos en Tus manos. Amén.",
    "Dios de paz, reúne esta noche a este círculo bajo Tu ala. A quienes velan con preocupación, dales calma. A quienes están agotados, dales sueño. Recuérdanos que el mundo está en Tus manos, no en las nuestras. Lo soltamos y descansamos. Amén.",
    "Padre, antes de dormir, te damos gracias. Por el aliento, por este círculo, por la misericordia que hoy nos alcanzó. Guarda nuestros hogares y a quienes amamos durante la noche, y despiértanos mañana con el corazón dispuesto a buscarte primero. Amén.",
    "Señor, la oscuridad no Te atemoriza, y por eso no tiene por qué atemorizarnos. Permanece cerca de cada miembro de este círculo esta noche. Aquieta todo pensamiento de ansiedad, y que Tu paz, que sobrepasa todo entendimiento, guarde nuestros corazones y nuestros pensamientos. Amén."
  ],
  morning: [
    "Padre, esta mañana es Tuya antes de ser nuestra. Pon en orden nuestro corazón antes de que el día lo arrastre. Da a cada uno de este círculo claridad para la tarea que viene, paciencia hacia quienes encontremos, y ojos para reconocerte a lo largo del día. Amén.",
    "Señor, gracias por despertarnos a un nuevo día. Tus misericordias son nuevas esta mañana, tal como prometiste. Camina delante de este círculo hoy. Abre las puertas correctas, cierra las equivocadas, y afirma nuestros pasos en Tu camino. Amén.",
    "Dios, antes de que comience el ruido, venimos primero a Ti. Llena a cada uno de nosotros con lo que necesitaremos hoy. Fuerza para los momentos difíciles, ternura hacia quienes nos rodean, y gratitud por todo ello. Guía a este círculo hora tras hora. Amén.",
    "Padre, el día aún no está escrito, y a Ti te confiamos su escritura. Bendice la obra de nuestras manos, guarda nuestras palabras, y que cada uno de este círculo lleve Tu paz a cada lugar donde entre hoy. Amén.",
    "Señor, mañana tras mañana, Tú eres fiel. Te damos los primeros minutos de este día y te pedimos que des forma al resto. Mantén a este círculo cerca de Ti y cerca los unos de los otros hasta que llegue la noche. Amén."
  ],
  hardDays: [
    "Padre, algunos de nosotros llevamos hoy cargas que se sienten demasiado pesadas. Tú dijiste: vengan a mí todos los que están cansados. Por eso venimos. Sostén a quienes apenas se sostienen, y que este círculo sea la prueba de que nadie atraviesa solo los días difíciles. Amén.",
    "Señor, Tú estás cerca de los quebrantados de corazón, y en eso confiamos hoy. Por cada carga de este círculo, visible y oculta, te pedimos Tu fuerza. No la que finge que todo está bien, sino la que persevera. Amén.",
    "Dios, cuando el valle es largo, Tú sigues siendo el Pastor. Camina con cada persona de este círculo a través de lo que enfrenta. Da gracia solo para hoy, y luz suficiente para el siguiente paso. A Ti te confiamos lo demás. Amén.",
    "Padre, Tú no desperdicias el dolor. Para cada uno aquí que atraviesa una temporada difícil, trae consuelo verdadero y esperanza que no avergüenza. Enséñanos a sobrellevar las cargas los unos de los otros, y lleva Tú las que no podemos. Amén.",
    "Señor, no fingiremos que hoy es fácil. Pero sabemos que Tú eres bueno, incluso ahora. Sostén a quienes tiemblan, ablanda a quienes se han quedado insensibles, y recuérdale a todo este círculo que por la noche durará el lloro, pero a la mañana vendrá la alegría. Amén."
  ],
  stillness: [
    "Padre, haz que vayamos más despacio. El mundo es ruidoso y nuestro corazón no ha dejado de correr. En este momento, elegimos la quietud. Sé Dios, y déjanos dejar de intentar serlo. Llena este círculo con la confianza serena de quienes saben quién los sostiene. Amén.",
    "Señor, Tú dijiste: estad quietos, y conoced. Por eso nos detenemos, aquí mismo, para conocerte. Aquieta nuestros pensamientos acelerados, afloja nuestras manos apretadas, y enseña a cada uno de este círculo que el descanso no es pereza. Es confianza. Amén.",
    "Dios de las aguas de reposo, llévanos junto a ellas hoy. Restaura en cada uno de nosotros lo que la prisa ha desgastado. Que este círculo aprenda el ritmo sereno de la gracia, y descubra que Tu yugo es fácil y ligera Tu carga. Amén.",
    "Padre, no tenemos que ganarnos Tu amor hoy, y necesitábamos recordarlo. En el silencio, habla. Estamos escuchando. Da a este círculo el valor de descansar en Ti mientras todo lo demás sigue moviéndose. Amén.",
    "Señor, hasta el mar Te obedeció cuando dijiste: Calla, enmudece. Dilo ahora sobre nosotros. Sobre nuestra mente, nuestras agendas, nuestras preocupaciones. Que este círculo se quede en Tu presencia y se marche más entero de como llegó. Amén."
  ],
  intercession: [
    "Padre, hoy te traemos los unos a los otros por su nombre. Tú conoces cada necesidad de este círculo antes de que la digamos. Fortalece a los cansados, anima a los desanimados, y que cada uno de nosotros sienta el peso de ser verdaderamente llevado en oración. Amén.",
    "Señor, gracias por las personas de este círculo. Qué gracia es no orar solos. Escucha cada petición silenciosa que se trae aquí hoy. Suple las necesidades que no hemos dicho en voz alta, y únenos más estrechamente mientras nos sostenemos los unos a los otros. Amén.",
    "Dios, Tú nos dijiste que sobrellevemos las cargas los unos de los otros, así que hoy lo hacemos. Por cada miembro de este círculo, te pedimos Tu provisión, Tu protección y Tu cercanía. Que nadie aquí se pregunte si alguien ora por él. Alguien lo hace. Amén.",
    "Padre, haznos intercesores fieles. Prontos para orar, lentos para olvidar. Bendice hoy a cada persona de este círculo en el lugar exacto donde más lo necesita, y que perciba, de algún modo, que fue llevada delante de Ti. Amén.",
    "Señor, dos o tres estamos congregados aquí en Tu nombre, y Tú prometiste estar en medio de nosotros. Por eso, está en medio de nosotros. Toma cada petición de este círculo, dicha y no dicha, y responde según Tu perfecta sabiduría y Tu amor. Amén."
  ],
  general: [
    "Padre, gracias por este círculo y por este día. Sea lo que sea que cada uno esté atravesando, encuéntranos allí. Danos corazones agradecidos, oraciones sinceras, y el tipo de fe que también aparece mañana. Somos Tuyos. Amén.",
    "Señor, Tú has sido nuestro auxilio en los días pasados, y serás nuestra esperanza en los días por venir. Bendice hoy a cada miembro de este círculo. Provee lo que falta, sana lo que duele, y mantennos caminando juntos hacia Ti. Amén.",
    "Dios, nos detenemos juntos para darte gracias. Por la familia, por la amistad, por la misericordia que no merecíamos. Enseña a este círculo a contar sus bendiciones en lugar de sus problemas, y a traerte ambas cosas. Amén.",
    "Padre, danos fuerza para hoy. No para todo el año, solo para hoy. Guarda a nuestras familias, guía nuestras decisiones, y que cada uno de este círculo termine este día un poco más cerca de Ti de lo que lo comenzó. Amén.",
    "Señor, guíanos. Donde estamos confundidos, trae claridad. Donde tenemos miedo, trae valor. Donde somos orgullosos, trae humildad. Este círculo te pertenece, y te confiamos todo lo que hay en él. Amén.",
    "Padre, Tú ves a cada persona de este círculo en este mismo instante, exactamente donde está. Gracias porque ninguno de nosotros es invisible para Ti. Acércate a cada corazón aquí, y haz que nuestras oraciones de hoy sean sinceras, sencillas y llenas de fe. Amén."
  ]
},
  pt: {
  night: [
    "Pai, ao chegar o fim deste dia, depomos tudo diante de Ti. O que concluímos e o que não conseguimos. Aquieta a nossa mente, serena o nosso coração, e vela esta noite por cada um deste círculo. Que descansemos sabendo que Tu permaneces desperto. Amém.",
    "Senhor, a noite chegou e estamos cansados. Obrigado por nos carregares ao longo deste dia. Perdoa o que precisa ser perdoado, cura o que dói, e dá a cada um o descanso profundo que nasce de confiar em Ti. Adormecemos nas Tuas mãos. Amém.",
    "Deus de paz, reúne esta noite este círculo debaixo da Tua asa. Aos que velam com preocupação, concede serenidade. Aos que estão exaustos, concede sono. Lembra nos de que o mundo está nas Tuas mãos, não nas nossas. Nós o entregamos e descansamos. Amém.",
    "Pai, antes de dormir, damos Te graças. Pelo fôlego, por este círculo, pela misericórdia que hoje nos alcançou. Guarda os nossos lares e aqueles que amamos ao longo da noite, e desperta nos amanhã com o coração pronto para Te buscar primeiro. Amém.",
    "Senhor, as trevas não Te assustam, e por isso não precisam nos assustar. Permanece perto de cada membro deste círculo esta noite. Aquieta todo pensamento ansioso, e que a Tua paz, que excede todo o entendimento, guarde os nossos corações e as nossas mentes. Amém."
  ],
  morning: [
    "Pai, esta manhã é Tua antes de ser nossa. Põe em ordem o nosso coração antes que o dia o arraste. Dá a cada um deste círculo clareza para a tarefa que vem, paciência para com aqueles que encontrarmos, e olhos para Te reconhecer ao longo do dia. Amém.",
    "Senhor, obrigado por nos despertares para um novo dia. As Tuas misericórdias são novas esta manhã, como prometeste. Caminha diante deste círculo hoje. Abre as portas certas, fecha as erradas, e firma os nossos passos no Teu caminho. Amém.",
    "Deus, antes que o barulho comece, vimos primeiro a Ti. Enche cada um de nós com aquilo de que precisaremos hoje. Força para os momentos difíceis, ternura para com os que nos cercam, e gratidão por tudo isso. Conduz este círculo hora após hora. Amém.",
    "Pai, o dia ainda não está escrito, e a Ti confiamos a sua escrita. Abençoa a obra das nossas mãos, guarda as nossas palavras, e que cada um deste círculo leve a Tua paz a cada lugar onde entrar hoje. Amém.",
    "Senhor, manhã após manhã, Tu és fiel. Damos Te os primeiros minutos deste dia e Te pedimos que dês forma ao restante. Guarda este círculo perto de Ti e perto uns dos outros até que a noite chegue. Amém."
  ],
  hardDays: [
    "Pai, alguns de nós carregam hoje fardos que parecem pesados demais. Tu disseste: vinde a mim todos os que estais cansados. Por isso viemos. Ampara os que mal conseguem se manter de pé, e que este círculo seja a prova de que ninguém atravessa sozinho os dias difíceis. Amém.",
    "Senhor, Tu estás perto dos que têm o coração quebrantado, e é nisso que confiamos hoje. Por cada fardo deste círculo, visível e oculto, pedimos a Tua força. Não a que finge que está tudo bem, mas a que persevera. Amém.",
    "Deus, quando o vale é longo, Tu continuas sendo o Pastor. Caminha com cada pessoa deste círculo através daquilo que enfrenta. Concede graça apenas para hoje, e luz suficiente para o próximo passo. A Ti confiamos o restante. Amém.",
    "Pai, Tu não desperdiças a dor. Para cada um aqui que atravessa uma estação difícil, traz consolo verdadeiro e esperança que não envergonha. Ensina nos a levar as cargas uns dos outros, e leva Tu as que não conseguimos. Amém.",
    "Senhor, não fingiremos que hoje é fácil. Mas sabemos que Tu és bom, mesmo agora. Firma os que tremem, abranda os que ficaram insensíveis, e lembra a todo este círculo que o choro pode durar uma noite, mas a alegria vem pela manhã. Amém."
  ],
  stillness: [
    "Pai, faz nos ir mais devagar. O mundo é barulhento e o nosso coração não tem parado de correr. Neste momento, escolhemos a quietude. Sê Deus, e deixa nos parar de tentar sê lo. Enche este círculo com a confiança serena de quem sabe quem o sustenta. Amém.",
    "Senhor, Tu disseste: aquietai-vos, e sabei. Por isso nos detemos, aqui mesmo, para Te conhecer. Aquieta os nossos pensamentos agitados, afrouxa as nossas mãos cerradas, e ensina a cada um deste círculo que o descanso não é preguiça. É confiança. Amém.",
    "Deus das águas tranquilas, conduz nos a elas hoje. Restaura em cada um de nós aquilo que a pressa desgastou. Que este círculo aprenda o ritmo tranquilo da graça, e descubra que o Teu jugo é suave e o Teu fardo é leve. Amém.",
    "Pai, não precisamos merecer o Teu amor hoje, e precisávamos lembrar disso. No silêncio, fala. Estamos ouvindo. Dá a este círculo a coragem de descansar em Ti enquanto tudo o mais continua a se mover. Amém.",
    "Senhor, até o mar Te obedeceu quando disseste: Cala-te, aquieta-te. Dize o agora sobre nós. Sobre a nossa mente, os nossos compromissos, as nossas preocupações. Que este círculo permaneça na Tua presença e parta mais inteiro do que chegou. Amém."
  ],
  intercession: [
    "Pai, hoje Te trazemos uns aos outros pelo nome. Tu conheces cada necessidade deste círculo antes mesmo de a dizermos. Fortalece os cansados, anima os desanimados, e que cada um de nós sinta o peso de ser verdadeiramente levado em oração. Amém.",
    "Senhor, obrigado pelas pessoas deste círculo. Que graça é não orar sozinhos. Ouve cada pedido silencioso trazido aqui hoje. Supre as necessidades que não dissemos em voz alta, e une nos mais estreitamente enquanto sustentamos uns aos outros. Amém.",
    "Deus, Tu nos disseste para levar as cargas uns dos outros, então hoje nós o fazemos. Por cada membro deste círculo, pedimos a Tua provisão, a Tua proteção e a Tua presença. Que ninguém aqui se pergunte se alguém ora por ele. Alguém ora. Amém.",
    "Pai, faz de nós intercessores fiéis. Prontos para orar, lentos para esquecer. Abençoa hoje cada pessoa deste círculo no lugar exato onde mais precisa, e que perceba, de algum modo, que foi levada diante de Ti. Amém.",
    "Senhor, dois ou três estamos reunidos aqui em Teu nome, e Tu prometeste estar no meio de nós. Por isso, está no meio de nós. Toma cada pedido deste círculo, dito e não dito, e responde segundo a Tua perfeita sabedoria e o Teu amor. Amém."
  ],
  general: [
    "Pai, obrigado por este círculo e por este dia. Seja o que for que cada um esteja atravessando, encontra nos ali. Dá nos corações gratos, orações sinceras, e o tipo de fé que também aparece amanhã. Somos Teus. Amém.",
    "Senhor, Tu foste o nosso amparo nos dias passados, e serás a nossa esperança nos dias que virão. Abençoa hoje cada membro deste círculo. Provê o que falta, cura o que dói, e guarda nos caminhando juntos em direção a Ti. Amém.",
    "Deus, paramos juntos para Te dizer obrigado. Pela família, pela amizade, pela misericórdia que não merecíamos. Ensina a este círculo a contar as suas bênçãos em vez dos seus problemas, e a trazer ambas a Ti. Amém.",
    "Pai, dá nos força para hoje. Não para o ano inteiro, apenas para hoje. Guarda as nossas famílias, guia as nossas decisões, e que cada um deste círculo termine este dia um pouco mais perto de Ti do que o começou. Amém.",
    "Senhor, conduz nos. Onde estamos confusos, traz clareza. Onde temos medo, traz coragem. Onde somos orgulhosos, traz humildade. Este círculo Te pertence, e Te confiamos tudo o que há nele. Amém.",
    "Pai, Tu vês cada pessoa deste círculo neste exato instante, exatamente onde está. Obrigado porque nenhum de nós é invisível para Ti. Aproxima Te de cada coração aqui, e torna as nossas orações de hoje sinceras, simples e cheias de fé. Amém."
  ]
},
};

const PRAYER_BUCKET_KEYS = ["night", "morning", "hardDays", "stillness", "intercession", "general"];

function fallbackBucketForTopic(topic: string): string {
  // Journey days pass a bucket key directly (e.g. "hardDays") — use it as-is.
  // (Previously "hardDays" fell through to "general" because the keyword check
  // below looks for "hard days" with a space, which never matches the key.)
  if (PRAYER_BUCKET_KEYS.includes(topic)) return topic;
  const t = topic.toLowerCase();
  if (t.includes("night") || t.includes("evening")) return "night";
  if (t.includes("morning")) return "morning";
  if (t.includes("hard days") || t.includes("hard day") || t.includes("difficult") || t.includes("hardship")) return "hardDays";
  if (t.includes("stillness") || t.includes("rest")) return "stillness";
  if (t.includes("each other") || t.includes("intercession")) return "intercession";
  return "general";
}

function seededFallbackPrayer(code: string, date: string, topic: string, lang: Lang = "en"): string {
  const byLang = FALLBACK_CIRCLE_PRAYERS_BY_LANG[lang] ?? FALLBACK_CIRCLE_PRAYERS_BY_LANG.en;
  const bucket = fallbackBucketForTopic(topic);
  const pool = byLang[bucket] ?? FALLBACK_CIRCLE_PRAYERS_BY_LANG.en[bucket] ?? FALLBACK_CIRCLE_PRAYERS_BY_LANG.en.general;
  const key = code + date;                       // SAME key as before — index stable across languages
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
  return pool[hash % pool.length];
}

// ═══════════════════════════════════════════════════════════════════
// v5.16.0 — JOURNEYS: Family config (versioned in source, not DB rows)
// ═══════════════════════════════════════════════════════════════════
// Each family maps to existing prayer buckets so prayer-type journey days
// reuse the localized FALLBACK_CIRCLE_PRAYERS_BY_LANG pool — zero new authoring.
type JourneyFamily = "loss" | "health" | "waiting" | "new_life" | "drawing_closer" | "hardship" | "relationships";

interface JourneyFamilyConfig {
  key: JourneyFamily;
  name: Record<Lang, string>;
  buckets: string[];  // prayer bucket keys from FALLBACK_CIRCLE_PRAYERS_BY_LANG
}

const JOURNEY_FAMILIES: JourneyFamilyConfig[] = [
  {
    key: "loss",
    name: { en: "Walking Through Loss", fr: "Traverser le deuil", es: "Caminando a trav\u00e9s de la p\u00e9rdida", pt: "Caminhando atrav\u00e9s da perda" },
    buckets: ["hardDays", "stillness"]
  },
  {
    key: "health",
    name: { en: "Facing a Health Challenge", fr: "Face \u00e0 un d\u00e9fi de sant\u00e9", es: "Enfrentando un desaf\u00edo de salud", pt: "Enfrentando um desafio de sa\u00fade" },
    buckets: ["intercession", "morning"]
  },
  {
    key: "waiting",
    name: { en: "In a Season of Waiting", fr: "Dans une saison d\u2019attente", es: "En una temporada de espera", pt: "Numa esta\u00e7\u00e3o de espera" },
    buckets: ["stillness", "morning"]
  },
  {
    key: "new_life",
    name: { en: "Welcoming New Life", fr: "Accueillir une nouvelle vie", es: "Dando la bienvenida a una nueva vida", pt: "Acolhendo uma nova vida" },
    buckets: ["morning", "general"]
  },
  {
    key: "drawing_closer",
    name: { en: "Drawing Closer to God", fr: "Se rapprocher de Dieu", es: "Acerc\u00e1ndose m\u00e1s a Dios", pt: "Aproximando-se de Deus" },
    buckets: ["morning", "night", "general"]
  },
  {
    key: "hardship",
    name: { en: "Through a Hard Season", fr: "\u00c0 travers une saison difficile", es: "A trav\u00e9s de una temporada dif\u00edcil", pt: "Atrav\u00e9s de uma esta\u00e7\u00e3o dif\u00edcil" },
    buckets: ["hardDays", "stillness"]
  },
  {
    key: "relationships",
    name: { en: "Strengthening Relationships", fr: "Renforcer les relations", es: "Fortaleciendo relaciones", pt: "Fortalecendo rela\u00e7\u00f5es" },
    buckets: ["intercession", "general"]
  }
];

const JOURNEY_FAMILIES_BY_KEY: Record<string, JourneyFamilyConfig> = {};
for (const f of JOURNEY_FAMILIES) JOURNEY_FAMILIES_BY_KEY[f.key] = f;

// ─── Journey display names (user-facing, localized) ─────────────
const JOURNEY_DISPLAY_NAMES: Record<string, Record<Lang, string>> = {
  drawing_closer:        { en: "Drawing Closer to God", fr: "Se rapprocher de Dieu", es: "Acercándose a Dios", pt: "Aproximando-se de Deus" },
  through_a_hard_season: { en: "Through a Hard Season", fr: "Traverser une saison difficile", es: "A través de una temporada difícil", pt: "Atravessando uma fase difícil" },
  expecting:             { en: "Expecting a Child", fr: "En attendant bébé", es: "En espera", pt: "À espera" },
  walking_through_grief:       { en: "Walking Through Grief", fr: "Traverser le deuil", es: "Caminando a través del duelo", pt: "Caminhando através do luto" },
  through_illness_and_healing: { en: "Through Illness and Healing", fr: "À travers la maladie et la guérison", es: "A través de la enfermedad y la sanación", pt: "Através da doença e da cura" },
  the_season_of_waiting:       { en: "The Season of Waiting", fr: "La saison de l'attente", es: "La temporada de espera", pt: "A estação da espera" },
  praying_for_someone:         { en: "Praying for Someone", fr: "Prier pour quelqu'un", es: "Orando por alguien", pt: "Orando por alguém" },
};
const JOURNEY_FAMILY_DISPLAY_FALLBACK: Record<string, Record<Lang, string>> = {
  loss:          { en: "Carrying a Loss", fr: "Porter un deuil", es: "Cargando una pérdida", pt: "Carregando uma perda" },
  health:        { en: "Through a Health Challenge", fr: "Face à un défi de santé", es: "Enfrentando un desafío de salud", pt: "Enfrentando um desafio de saúde" },
  waiting:       { en: "Waiting on God", fr: "Dans l'attente de Dieu", es: "Esperando en Dios", pt: "Esperando em Deus" },
  relationships: { en: "Navigating a Relationship", fr: "Naviguer une relation", es: "Navegando una relación", pt: "Navegando um relacionamento" },
};
function journeyDisplayName(templateKey: string, family: string, lang: Lang): string {
  const byTemplate = JOURNEY_DISPLAY_NAMES[templateKey];
  if (byTemplate) return byTemplate[lang] || byTemplate.en;
  const byFamily = JOURNEY_FAMILY_DISPLAY_FALLBACK[family];
  if (byFamily) return byFamily[lang] || byFamily.en;
  // Ultimate fallback: use the JOURNEY_FAMILIES name
  const familyConfig = JOURNEY_FAMILIES_BY_KEY[family];
  if (familyConfig) return familyConfig.name[lang] || familyConfig.name.en;
  return templateKey;
}

// ═══════════════════════════════════════════════════════════════════
// v5.16.1 — JOURNEYS Phase 1: Templates + Generator + Endpoints
// Drop-in authored config from §K content file.
// ═══════════════════════════════════════════════════════════════════

type JourneyTone = "gentle" | "grateful" | "steadying" | "honest" | "tender" | "lifting";
type JourneyActionType = "prayer" | "reflection" | "meditation" | "small_act" | "scripture" | "gratitude" | "journal" | "rest" | "confession" | "encouragement";

interface JourneyPhase {
  label: Record<Lang, string>;
  dayStart: number;
  dayEnd: number;
  tone: JourneyTone;
  mix: JourneyActionType[];
}

interface JourneyScriptEntry {
  day: number;
  type: JourneyActionType;
  content: Record<string, { title: string; body: string; scriptureRef?: string; prompt?: string }>;
}

interface JourneyTemplate {
  key: string;
  family: JourneyFamily;
  mode: "fixed" | "open";
  lengthDays?: number;
  name: Record<Lang, string>;
  oneLiner: Record<Lang, string>;
  phases: JourneyPhase[];
  script?: JourneyScriptEntry[];
}

interface JourneyDailyAction {
  type: JourneyActionType;
  phaseLabel: string;
  content: { title: string; body: string; scriptureRef?: string; prompt?: string };
  completionLabel?: string;
}

const COMPLETION_LABELS: Record<JourneyActionType, Record<Lang, string>> = {
  prayer:        { en: "Amen", fr: "Amen", es: "Amén", pt: "Amém" },
  scripture:     { en: "I receive this", fr: "Je reçois cela", es: "Lo recibo", pt: "Eu recebo isso" },
  reflection:    { en: "I sat with this", fr: "J'ai médité", es: "Me quedé con esto", pt: "Fiquei com isso" },
  small_act:     { en: "I did it", fr: "Je l'ai fait", es: "Lo hice", pt: "Eu fiz" },
  gratitude:     { en: "Done", fr: "Fait", es: "Hecho", pt: "Feito" },
  journal:       { en: "I wrote it", fr: "J'ai écrit", es: "Lo escribí", pt: "Eu escrevi" },
  rest:          { en: "I rested", fr: "Je me suis reposé", es: "Descansé", pt: "Eu descansei" },
  confession:    { en: "I was honest", fr: "J'ai été honnête", es: "Fui honesto", pt: "Fui honesto" },
  encouragement: { en: "Amen", fr: "Amen", es: "Amén", pt: "Amém" },
  meditation:    { en: "I sat with this", fr: "J'ai médité", es: "Me quedé con esto", pt: "Fiquei com isso" },
};

// Legacy type mapper — old app versions only know prayer/reflection/meditation/small_act.
// Map new types to the closest legacy equivalent so old apps don't crash on decode.
const LEGACY_TYPE_MAP: Record<JourneyActionType, JourneyActionType> = {
  prayer: "prayer",
  reflection: "reflection",
  meditation: "meditation",
  small_act: "small_act",
  scripture: "meditation",       // scripture → meditation (contemplative)
  gratitude: "reflection",       // gratitude → reflection (prompt-based)
  journal: "reflection",         // journal → reflection (prompt-based)
  rest: "meditation",            // rest → meditation (contemplative)
  confession: "reflection",      // confession → reflection (prompt-based)
  encouragement: "prayer",       // encouragement → prayer (receive, don't do)
};

// ═══════════════════════════════════════════════════════════════════
// v5.20.0 — TIER 1 SPINE MODEL + JOURNEY MANIFEST (single source of truth)
// Michael's 50+ taxonomy: 7 marketing "doors" collapse to 3 content SPINES.
// A door = one manifest row → an existing spine/template. Adding a future ad
// angle later = one new row, NOT a newly authored journey. Dedicated per-door
// content pools + verified scripture (indexed by emotional core) land in
// Phase 2; this slice establishes the model and propagates grief-safety by
// safety_class. The app never sees a catalog — one journey at a time.
// ═══════════════════════════════════════════════════════════════════

type JourneySpine = "in_my_body" | "grieving_loss" | "carrying_someone" | "drawing_closer";
type SafetyClass = "crisis" | "loss" | "carrying" | "standard";

interface JourneyDoor {
  key: string;                 // route key, e.g. "body/diagnosis"
  name: Record<Lang, string>;  // ad-facing emotional label (the exact sentence the user is living)
  spine: JourneySpine;
  templateKey: string;         // resolves to JOURNEY_TEMPLATES (closest existing template for now; dedicated ones in Phase 2)
  family: JourneyFamily;
  mode: "fixed" | "open";
  length?: number;             // in `unit`; omit for open/ongoing
  unit: "day" | "week";
  pacing: "acute" | "standard" | "ongoing";
  safetyClass: SafetyClass;
  tokens: string[];            // tokens the shared router must capture for this door
  adExclude?: boolean;         // true = in-app only, never in paid creative (Motherhood)
  scriptureCores: string[];    // emotional cores this door needs verified verses for (gap tracker)
}

const EN = (s: string): Record<Lang, string> => ({ en: s, fr: s, es: s, pt: s }); // localized door names authored in Phase 2

const TIER1_DOORS: Record<string, JourneyDoor> = {
  // ── SPINE 1 — "In my own body": fear + strength-for-today-only, never the whole road, never promise a cure
  "body/diagnosis": {
    key: "body/diagnosis", name: EN("Facing a Serious Health Diagnosis"), spine: "in_my_body",
    templateKey: "through_illness_and_healing", family: "health", mode: "fixed", length: 30, unit: "day",
    pacing: "standard", safetyClass: "crisis",
    tokens: ["who_self", "dominant_emotion", "phase"], scriptureCores: ["fear", "strength_for_today"],
  },
  "body/results": {
    key: "body/results", name: EN("Waiting for Medical Test Results"), spine: "in_my_body",
    templateKey: "body_test_results", family: "health", mode: "fixed", length: 14, unit: "day",
    pacing: "acute", safetyClass: "crisis",
    tokens: ["who_self", "dominant_emotion"], scriptureCores: ["anxiety", "waiting", "peace"],
  },
  "body/chronic": {
    key: "body/chronic", name: EN("Living With Chronic Pain or Illness"), spine: "in_my_body",
    templateKey: "body_chronic", family: "health", mode: "open", unit: "day",
    pacing: "ongoing", safetyClass: "crisis",
    tokens: ["who_self", "dominant_emotion"], scriptureCores: ["endurance", "presence", "strength_for_today"],
  },
  // ── SPINE 2 — "Grieving a loss": the existing grief engine + ALL its safety rules, unchanged
  "grief/spouse": {
    key: "grief/spouse", name: EN("Grieving the Loss of a Spouse"), spine: "grieving_loss",
    templateKey: "walking_through_grief", family: "loss", mode: "fixed", length: 30, unit: "day",
    pacing: "standard", safetyClass: "loss",
    tokens: ["who_self", "phase"], scriptureCores: ["grief", "comfort", "loneliness"],
  },
  // ── SPINE 3 — "Carrying someone I love": love + helplessness + tend-your-own-heart, no restoration promise, no guilt
  "carry/child": {
    key: "carry/child", name: EN("Praying for an Adult Child Who Has Walked Away"), spine: "carrying_someone",
    templateKey: "praying_for_someone", family: "relationships", mode: "fixed", length: 30, unit: "day",
    pacing: "standard", safetyClass: "carrying",
    tokens: ["who_someone", "carrying_name", "dominant_emotion"], scriptureCores: ["helplessness", "surrender", "hope_held_loosely"],
  },
  "carry/addiction": {
    key: "carry/addiction", name: EN("Watching Someone You Love Fight Addiction"), spine: "carrying_someone",
    templateKey: "praying_for_someone", family: "relationships", mode: "fixed", length: 30, unit: "day",
    pacing: "standard", safetyClass: "carrying",
    tokens: ["who_someone", "carrying_name", "dominant_emotion"], scriptureCores: ["helplessness", "boundaries", "tend_own_heart"],
  },
  "carry/caregiver": {
    key: "carry/caregiver", name: EN("Caring for a Sick Spouse"), spine: "carrying_someone",
    templateKey: "praying_for_someone", family: "relationships", mode: "fixed", length: 30, unit: "day",
    pacing: "standard", safetyClass: "carrying",
    tokens: ["who_someone", "carrying_name", "dominant_emotion"], scriptureCores: ["exhaustion", "being_seen", "strength"],
  },
  // ── In-app only — routing preserved, EXCLUDED from all paid creative
  "faith/motherhood": {
    key: "faith/motherhood", name: EN("Motherhood"), spine: "drawing_closer",
    templateKey: "expecting", family: "new_life", mode: "fixed", length: 40, unit: "week",
    pacing: "standard", safetyClass: "standard",
    tokens: ["who_self"], adExclude: true, scriptureCores: ["belovedness"],
  },
  "faith/grow": {
    key: "faith/grow", name: EN("Grow in my faith"), spine: "drawing_closer",
    templateKey: "drawing_closer", family: "drawing_closer", mode: "open", unit: "day",
    pacing: "ongoing", safetyClass: "standard",
    tokens: ["who_self"], scriptureCores: ["presence"],
  },
};

function doorForKey(key: string): JourneyDoor | null { return TIER1_DOORS[key] || null; }
function doorsForSpine(spine: JourneySpine): JourneyDoor[] { return Object.values(TIER1_DOORS).filter(d => d.spine === spine); }

// ═══════════════════════════════════════════════════════════════════════════
// v5.24.0 — v2.5 dominant-emotion derivation (APPROVED #4, Samy 2026-07-10).
// Replaces the parked FEELING_TO_CORE for funnel users. Scores by mirror-answer
// INDEX (0-4) — label text is NEVER load-bearing (Samy amendment a-1). 19×8 tag
// table is the reviewed artifact; weak-signal defaults to the door's intrinsic
// core; ties break intrinsic → pastoral-priority → earliest.
// ═══════════════════════════════════════════════════════════════════════════
const MIRROR_TAGS: Record<string, string[]> = {
  health:     ["shock","fear_dread","guilt_shame","fear_dread","unanswered_prayer","hidden_pain","anger_at_God","anticipatory_future"],
  grief:      ["grief_loss","grief_loss","grief_loss","guilt_shame","grief_loss","unanswered_prayer","anger_at_God","grief_loss"],
  child:      ["guilt_shame","guilt_shame","grief_loss","relational","hidden_pain","unanswered_prayer","anticipatory_future","relational"],
  caregiver:  ["guilt_shame","exhaustion","hidden_pain","guilt_shame","hidden_pain","guilt_shame","exhaustion","anticipatory_future"],
  addiction:  ["guilt_shame","guilt_shame","fear_dread","guilt_shame","hidden_pain","anger_at_God","doubt_of_faith","anticipatory_future"],
  lonely:     ["hidden_pain","hidden_pain","hidden_pain","unworthiness","grief_loss","hidden_pain","anticipatory_future","hidden_pain"],
  family:     ["relational","guilt_shame","exhaustion","exhaustion","grief_loss","unanswered_prayer","anticipatory_future","relational"],
  season:     ["shock","hidden_pain","grief_loss","grief_loss","unworthiness","grief_loss","anticipatory_future","anticipatory_future"],
  financial:  ["fear_dread","unworthiness","guilt_shame","guilt_shame","anticipatory_future","doubt_of_faith","guilt_shame","fear_dread"],
  s_abandoned:["abandonment","doubt_of_faith","abandonment","doubt_of_faith","hidden_pain","abandonment","abandonment","doubt_of_faith"],
  s_angry:    ["guilt_shame","anger_at_God","anger_at_God","unanswered_prayer","hidden_pain","anger_at_God","anger_at_God","anticipatory_future"],
  s_forgiven: ["guilt_shame","guilt_shame","guilt_shame","unworthiness","unworthiness","guilt_shame","guilt_shame","guilt_shame"],
  s_tragedy:  ["shock","doubt_of_faith","doubt_of_faith","hidden_pain","doubt_of_faith","unanswered_prayer","grief_loss","doubt_of_faith"],
  s_prayers:  ["unanswered_prayer","doubt_of_faith","doubt_of_faith","unanswered_prayer","unanswered_prayer","abandonment","unanswered_prayer","unanswered_prayer"],
  s_drifted:  ["grief_loss","grief_loss","unworthiness","doubt_of_faith","fear_dread","grief_loss","unworthiness","fear_dread"],
  s_doubt:    ["doubt_of_faith","doubt_of_faith","doubt_of_faith","doubt_of_faith","doubt_of_faith","hidden_pain","doubt_of_faith","doubt_of_faith"],
  s_unworthy: ["unworthiness","unworthiness","unworthiness","unworthiness","unworthiness","unworthiness","unworthiness","unworthiness"],
  s_darkness: ["darkness","darkness","darkness","darkness","hidden_pain","hidden_pain","hidden_pain","doubt_of_faith"],
  s_church:   ["relational","relational","abandonment","relational","doubt_of_faith","relational","grief_loss","doubt_of_faith"],
};
const INTRINSIC_CORE: Record<string, string> = {
  health:"fear_dread", grief:"grief_loss", child:"relational", caregiver:"exhaustion", addiction:"fear_dread",
  lonely:"hidden_pain", family:"relational", season:"grief_loss", financial:"fear_dread",
  s_abandoned:"abandonment", s_angry:"anger_at_God", s_forgiven:"guilt_shame", s_tragedy:"doubt_of_faith",
  s_prayers:"unanswered_prayer", s_drifted:"relational", s_doubt:"doubt_of_faith", s_unworthy:"unworthiness",
  s_darkness:"darkness", s_church:"relational",
};
const PASTORAL_PRIORITY = ["abandonment","doubt_of_faith","anger_at_God","unworthiness","guilt_shame","grief_loss","unanswered_prayer","fear_dread","anticipatory_future","relational","exhaustion","hidden_pain","darkness","shock"];
const OPENING_TONES = ["reconnecting","returning","scaffolding","seeking"];
// Michael's funnel journey names (display). Funnel-authoritative for funnel users —
// carried through the handoff so the app names the journey with the name the user
// saw on the paywall, WITHOUT renaming backend door.name (avoids in-app disruption
// + the health-q2 3-door naming mismatch). Launch-5 use existing templates.
const FUNNEL_JOURNEY_NAMES: Record<string, string> = {
  health:"Strength for the Road Ahead", grief:"Walking Through the Valley", child:"The Prayer a Parent Prays",
  caregiver:"The One Who Carries Others", addiction:"Letting Go and Letting God", lonely:"You Are Not Invisible",
  family:"When Home Feels Like a Stranger", season:"The Next Chapter", financial:"Peace That Passes Understanding",
  s_abandoned:"Where Are You, God", s_angry:"Holy Ground for Hard Feelings", s_forgiven:"The Road Back",
  s_tragedy:"Faith After the Fire", s_prayers:"When Heaven Feels Silent", s_drifted:"Coming Home",
  s_doubt:"Honest Before God", s_unworthy:"Beloved", s_darkness:"Light in the Dark", s_church:"Back to the Source",
};

function deriveDominantEmotion(pathKey: string | null | undefined, mirrorAnswers: any): { dominant_emotion: string | null; secondary_emotion: string | null; confidence: string } {
  const tags = pathKey ? MIRROR_TAGS[pathKey] : null;
  const intrinsic = pathKey ? (INTRINSIC_CORE[pathKey] || null) : null;
  if (!tags || !Array.isArray(mirrorAnswers) || mirrorAnswers.length === 0) return { dominant_emotion: intrinsic, secondary_emotion: null, confidence: "path_default" };
  // Score by INDEX (0-4). Non-numbers (or legacy label strings) → 0, never load-bearing.
  const scores = tags.map((_, i) => { const v = mirrorAnswers[i]; return (typeof v === "number" && v >= 0 && v <= 4) ? v : 0; });
  const maxScore = Math.max(...scores);
  if (maxScore < 2) return { dominant_emotion: intrinsic, secondary_emotion: null, confidence: "path_default" };
  const tied = scores.map((s, i) => (s === maxScore ? i : -1)).filter((i) => i >= 0);
  let slot: number;
  if (tied.length === 1) slot = tied[0];
  else {
    const intrinsicSlot = tied.find((i) => tags[i] === intrinsic);
    if (intrinsicSlot !== undefined) slot = intrinsicSlot;
    else slot = tied.slice().sort((a, b) => PASTORAL_PRIORITY.indexOf(tags[a]) - PASTORAL_PRIORITY.indexOf(tags[b]))[0];
  }
  const dominant = tags[slot];
  const distinct = Array.from(new Set(scores)).sort((a, b) => b - a);
  let secondary: string | null = null;
  if (distinct.length > 1) {
    const sSlot = scores.findIndex((s, i) => s === distinct[1] && tags[i] !== dominant);
    secondary = sSlot >= 0 ? tags[sSlot] : null;
  }
  return { dominant_emotion: dominant, secondary_emotion: secondary, confidence: maxScore >= 3 ? "high" : "moderate" };
}
function journeyOpeningTone(faithIdx: any): string | null { return (typeof faithIdx === "number" && faithIdx >= 0 && faithIdx < OPENING_TONES.length) ? OPENING_TONES[faithIdx] : null; }

// Templates that host any loss/crisis door inherit the grief "receiving phase"
// safety window (days 1-10 = held, not tasked with outward action). Derived from
// the manifest so future crisis doors get the protection automatically.
const RECEIVING_SAFETY_TEMPLATES: Set<string> = new Set(
  Object.values(TIER1_DOORS)
    .filter(d => d.safetyClass === "loss" || d.safetyClass === "crisis")
    .map(d => d.templateKey)
);

// Master gate for the Tier-1 spine re-architecture. Default OFF ⇒ byte-identical
// to pre-Tier-1 behavior: legacy intake routing, grief-only receiving phase,
// tone-pool scripture. Flip with env TIER1_ENABLED=true.
const TIER1_ENABLED = process.env.TIER1_ENABLED === "true";

// -- 1. JOURNEY_TEMPLATES (3 Phase-1 starters) -----------------------

const JOURNEY_TEMPLATES: Record<string, JourneyTemplate> = {
  drawing_closer: {
    key: "drawing_closer",
    family: "drawing_closer",
    mode: "open",
    name: { en: "Drawing Closer", fr: "Se rapprocher", es: "M\u00e1s cerca", pt: "Mais perto" },
    oneLiner: {
      en: "A simple daily prayer to build the habit, one day at a time.",
      fr: "Une pri\u00e8re quotidienne toute simple, un jour \u00e0 la fois.",
      es: "Una oraci\u00f3n diaria sencilla, un d\u00eda a la vez.",
      pt: "Uma ora\u00e7\u00e3o di\u00e1ria simples, um dia de cada vez.",
    },
    phases: [
      { label: { en: "Arrive", fr: "Arriver", es: "Llegar", pt: "Chegar" }, dayStart: 1, dayEnd: 3, tone: "gentle", mix: ["prayer", "scripture", "reflection"] },
      { label: { en: "Make space", fr: "Faire de la place", es: "Hacer espacio", pt: "Abrir espa\u00e7o" }, dayStart: 4, dayEnd: 7, tone: "gentle", mix: ["prayer", "gratitude", "rest", "scripture"] },
      { label: { en: "Give thanks", fr: "Rendre gr\u00e2ce", es: "Dar gracias", pt: "Dar gra\u00e7as" }, dayStart: 8, dayEnd: 11, tone: "grateful", mix: ["prayer", "gratitude", "small_act", "journal"] },
      { label: { en: "Carry it with you", fr: "Le garder en toi", es: "Ll\u00e9valo contigo", pt: "Leve com voc\u00ea" }, dayStart: 12, dayEnd: 9999, tone: "steadying", mix: ["prayer", "scripture", "reflection", "encouragement", "small_act"] },
    ],
  },
  through_a_hard_season: {
    key: "through_a_hard_season",
    family: "hardship",
    mode: "fixed",
    lengthDays: 30,
    name: {
      en: "Through a Hard Season",
      fr: "Traverser une saison difficile",
      es: "A trav\u00e9s de una temporada dif\u00edcil",
      pt: "Atravessando uma fase dif\u00edcil",
    },
    oneLiner: {
      en: "For when life feels heavy \u2014 30 days of walking it together.",
      fr: "Quand la vie p\u00e8se \u2014 30 jours pour la traverser ensemble.",
      es: "Cuando la vida pesa: 30 d\u00edas para atravesarla juntos.",
      pt: "Quando a vida pesa: 30 dias para atravess\u00e1-la juntos.",
    },
    phases: [
      { label: { en: "Steady", fr: "Tenir bon", es: "Af\u00edrmate", pt: "Firme-se" }, dayStart: 1, dayEnd: 6, tone: "steadying", mix: ["prayer", "scripture", "rest"] },
      { label: { en: "Honest", fr: "En v\u00e9rit\u00e9", es: "Con verdad", pt: "Com verdade" }, dayStart: 7, dayEnd: 14, tone: "honest", mix: ["prayer", "confession", "reflection", "journal"] },
      { label: { en: "Held", fr: "Se laisser porter", es: "Dejarse sostener", pt: "Deixar-se amparar" }, dayStart: 15, dayEnd: 21, tone: "tender", mix: ["prayer", "rest", "small_act", "encouragement"] },
      { label: { en: "Turn", fr: "Le tournant", es: "El giro", pt: "A virada" }, dayStart: 22, dayEnd: 27, tone: "steadying", mix: ["prayer", "scripture", "gratitude", "reflection"] },
      { label: { en: "Lift", fr: "Reprendre souffle", es: "Respirar de nuevo", pt: "Recobrar o f\u00f4lego" }, dayStart: 28, dayEnd: 30, tone: "lifting", mix: ["prayer", "encouragement", "gratitude"] },
    ],
  },
  expecting: {
    key: "expecting",
    family: "new_life",
    mode: "fixed",
    lengthDays: 40, // UNIT = WEEKS
    name: { en: "Expecting", fr: "En attendant b\u00e9b\u00e9", es: "En espera", pt: "\u00c0 espera" },
    oneLiner: {
      en: "A prayer for each week, from now until your little one arrives.",
      fr: "Une pri\u00e8re pour chaque semaine, jusqu'\u00e0 l'arriv\u00e9e de ton petit.",
      es: "Una oraci\u00f3n para cada semana, hasta que llegue tu peque\u00f1o.",
      pt: "Uma ora\u00e7\u00e3o para cada semana, at\u00e9 a chegada do seu pequeno.",
    },
    phases: [
      { label: { en: "Hidden beginnings", fr: "Les d\u00e9buts cach\u00e9s", es: "Comienzos ocultos", pt: "Come\u00e7os ocultos" }, dayStart: 1, dayEnd: 13, tone: "tender", mix: ["prayer", "scripture", "journal", "reflection"] },
      { label: { en: "Growing", fr: "La croissance", es: "Creciendo", pt: "Crescendo" }, dayStart: 14, dayEnd: 27, tone: "grateful", mix: ["prayer", "gratitude", "small_act", "encouragement"] },
      { label: { en: "Nearing", fr: "L'approche", es: "Se acerca", pt: "Aproximando-se" }, dayStart: 28, dayEnd: 39, tone: "steadying", mix: ["prayer", "rest", "scripture", "small_act"] },
      { label: { en: "Arrival", fr: "L'arriv\u00e9e", es: "La llegada", pt: "A chegada" }, dayStart: 40, dayEnd: 40, tone: "lifting", mix: ["prayer", "encouragement", "gratitude"] },
    ],
  },
  walking_through_grief: {
    key: "walking_through_grief",
    family: "loss",
    mode: "fixed",
    lengthDays: 30,
    name: {
      en: "Walking Through Grief",
      fr: "Traverser le deuil",
      es: "Caminando a trav\u00e9s del duelo",
      pt: "Caminhando atrav\u00e9s do luto",
    },
    oneLiner: {
      en: "For someone carrying a loss. We sit with you.",
      fr: "Pour quelqu\u2019un qui porte un deuil. Nous sommes l\u00e0 avec vous.",
      es: "Para alguien que carga una p\u00e9rdida. Estamos contigo.",
      pt: "Para algu\u00e9m carregando uma perda. Estamos com voc\u00ea.",
    },
    phases: [
      { label: { en: "Just here", fr: "Simplement l\u00e0", es: "Solo aqu\u00ed", pt: "Apenas aqui" }, dayStart: 1, dayEnd: 10, tone: "gentle", mix: ["prayer", "rest", "scripture"] },
      { label: { en: "Carrying it", fr: "Le porter", es: "Carg\u00e1ndolo", pt: "Carregando" }, dayStart: 11, dayEnd: 20, tone: "tender", mix: ["prayer", "journal", "reflection", "rest"] },
      { label: { en: "Carrying it forward", fr: "Le porter plus loin", es: "Llev\u00e1ndolo adelante", pt: "Levando adiante" }, dayStart: 21, dayEnd: 30, tone: "tender", mix: ["prayer", "scripture", "encouragement", "gratitude"] },
    ],
  },
  through_illness_and_healing: {
    key: "through_illness_and_healing",
    family: "health",
    mode: "fixed",
    lengthDays: 30,
    name: {
      en: "Through Illness and Healing",
      fr: "\u00c0 travers la maladie et la gu\u00e9rison",
      es: "A trav\u00e9s de la enfermedad y la sanaci\u00f3n",
      pt: "Atrav\u00e9s da doen\u00e7a e da cura",
    },
    oneLiner: {
      en: "For someone facing a health struggle. Grace for today.",
      fr: "Pour quelqu\u2019un face \u00e0 un combat de sant\u00e9. La gr\u00e2ce pour aujourd\u2019hui.",
      es: "Para alguien enfrentando una lucha de salud. Gracia para hoy.",
      pt: "Para algu\u00e9m enfrentando uma luta de sa\u00fade. Gra\u00e7a para hoje.",
    },
    phases: [
      { label: { en: "The weight of it", fr: "Le poids de tout cela", es: "El peso de todo", pt: "O peso de tudo isso" }, dayStart: 1, dayEnd: 10, tone: "gentle", mix: ["prayer", "scripture", "small_act", "rest"] },
      { label: { en: "Strength for today", fr: "La force pour aujourd\u2019hui", es: "Fuerza para hoy", pt: "For\u00e7a para hoje" }, dayStart: 11, dayEnd: 20, tone: "steadying", mix: ["prayer", "encouragement", "journal", "reflection"] },
      { label: { en: "Hope held honestly", fr: "L\u2019espoir tenu honn\u00eatement", es: "Esperanza sostenida con honestidad", pt: "Esperan\u00e7a mantida com honestidade" }, dayStart: 21, dayEnd: 30, tone: "lifting", mix: ["prayer", "gratitude", "scripture", "encouragement"] },
    ],
  },
  // v5.20.13 \u2014 Tier-1 dedicated arc: CHRONIC. mode:open \u21d2 no lengthDays, no
  // final phase, no denominator, no graduation. Structurally cannot end on a
  // finish line. Steady companionship "for as long as you need."
  body_chronic: {
    key: "body_chronic",
    family: "health",
    mode: "open",
    name: {
      en: "Living With It",
      fr: "Vivre avec",
      es: "Viviendo con esto",
      pt: "Vivendo com isso",
    },
    oneLiner: {
      en: "One day at a time, for as long as you need.",
      fr: "Un jour \u00e0 la fois, aussi longtemps qu\u2019il le faut.",
      es: "Un d\u00eda a la vez, todo el tiempo que necesites.",
      pt: "Um dia de cada vez, pelo tempo que precisar.",
    },
    phases: [
      { label: { en: "Just here", fr: "Simplement pr\u00e9sent", es: "Simplemente aqu\u00ed", pt: "Apenas aqui" }, dayStart: 1, dayEnd: 14, tone: "gentle", mix: ["prayer", "scripture", "rest", "reflection"] },
      { label: { en: "Enough for today", fr: "Assez pour aujourd\u2019hui", es: "Suficiente para hoy", pt: "O bastante para hoje" }, dayStart: 15, dayEnd: 99999, tone: "steadying", mix: ["prayer", "scripture", "encouragement", "rest", "reflection"] },
    ],
  },
  // v5.20.13 \u2014 Tier-1 dedicated arc: TEST RESULTS. Short acute wait. Final phase
  // is "steadying", NOT "lifting" \u2014 the outcome is unknown, so the arc must not
  // build toward resolution. Graduation line is outcome-neutral.
  body_test_results: {
    key: "body_test_results",
    family: "health",
    mode: "fixed",
    lengthDays: 14,
    name: {
      en: "Waiting for Results",
      fr: "En attendant les r\u00e9sultats",
      es: "Esperando resultados",
      pt: "Esperando resultados",
    },
    oneLiner: {
      en: "For the not-knowing. One breath at a time.",
      fr: "Pour l\u2019incertitude. Un souffle \u00e0 la fois.",
      es: "Para la incertidumbre. Un respiro a la vez.",
      pt: "Para a incerteza. Um f\u00f4lego de cada vez.",
    },
    phases: [
      { label: { en: "The waiting", fr: "L\u2019attente", es: "La espera", pt: "A espera" }, dayStart: 1, dayEnd: 7, tone: "honest", mix: ["prayer", "scripture", "rest", "confession"] },
      { label: { en: "Held in the not-knowing", fr: "Tenu dans l\u2019incertitude", es: "Sostenido en la incertidumbre", pt: "Amparado na incerteza" }, dayStart: 8, dayEnd: 14, tone: "steadying", mix: ["prayer", "scripture", "reflection", "rest"] },
    ],
  },
  the_season_of_waiting: {
    key: "the_season_of_waiting",
    family: "waiting",
    mode: "fixed",
    lengthDays: 30,
    name: {
      en: "The Season of Waiting",
      fr: "La saison de l\u2019attente",
      es: "La temporada de espera",
      pt: "A esta\u00e7\u00e3o da espera",
    },
    oneLiner: {
      en: "For someone in an unanswered stretch. Stay in it.",
      fr: "Pour quelqu\u2019un dans une p\u00e9riode sans r\u00e9ponse. Restez-y.",
      es: "Para alguien en un tramo sin respuesta. Qu\u00e9date.",
      pt: "Para algu\u00e9m em um per\u00edodo sem resposta. Fique.",
    },
    phases: [
      { label: { en: "The ache of unanswered", fr: "La douleur de l\u2019absence de r\u00e9ponse", es: "El dolor de lo sin respuesta", pt: "A dor do sem resposta" }, dayStart: 1, dayEnd: 10, tone: "honest", mix: ["prayer", "confession", "scripture", "rest"] },
      { label: { en: "Faithful in the meantime", fr: "Fid\u00e8le entre-temps", es: "Fiel mientras tanto", pt: "Fiel enquanto isso" }, dayStart: 11, dayEnd: 20, tone: "steadying", mix: ["prayer", "journal", "scripture", "reflection"] },
      { label: { en: "Trusting the timing", fr: "Faire confiance au temps", es: "Confiando en el tiempo", pt: "Confiando no tempo" }, dayStart: 21, dayEnd: 30, tone: "lifting", mix: ["prayer", "encouragement", "gratitude", "scripture"] },
    ],
  },
  praying_for_someone: {
    key: "praying_for_someone",
    family: "relationships",
    mode: "fixed",
    lengthDays: 30,
    name: {
      en: "Praying for Someone",
      fr: "Prier pour quelqu\u2019un",
      es: "Orando por alguien",
      pt: "Orando por algu\u00e9m",
    },
    oneLiner: {
      en: "For someone carrying a strained relationship. Lift them first.",
      fr: "Pour quelqu\u2019un portant une relation tendue. Priez d\u2019abord pour eux.",
      es: "Para alguien cargando una relaci\u00f3n tensa. Eleva a esa persona primero.",
      pt: "Para algu\u00e9m carregando um relacionamento tenso. Eleve-os primeiro.",
    },
    phases: [
      { label: { en: "Bring them to God", fr: "Les porter devant Dieu", es: "Ll\u00e9valos ante Dios", pt: "Leve-os diante de Deus" }, dayStart: 1, dayEnd: 10, tone: "gentle", mix: ["prayer", "scripture", "small_act", "journal"] },
      { label: { en: "Your own heart first", fr: "Ton propre c\u0153ur d\u2019abord", es: "Tu propio coraz\u00f3n primero", pt: "Seu pr\u00f3prio cora\u00e7\u00e3o primeiro" }, dayStart: 11, dayEnd: 20, tone: "honest", mix: ["prayer", "confession", "reflection", "rest"] },
      { label: { en: "Hope for restoration, held loosely", fr: "Esp\u00e9rer la restauration, sans forcer", es: "Esperanza de restauraci\u00f3n, sin forzar", pt: "Esperan\u00e7a de restaura\u00e7\u00e3o, sem for\u00e7ar" }, dayStart: 21, dayEnd: 30, tone: "gentle", mix: ["prayer", "encouragement", "gratitude", "scripture"] },
    ],
  },
};

// -- 2. SMALL_ACT_POOLS_BY_FAMILY ------------------------------------

const SMALL_ACT_POOLS_BY_FAMILY: Record<JourneyFamily, Record<Lang, string>[]> = {
  new_life: [
    { en: "Place a hand where your baby rests and breathe slowly.", fr: "Pose une main l\u00e0 o\u00f9 ton b\u00e9b\u00e9 repose et respire doucement.", es: "Pon una mano donde descansa tu beb\u00e9 y respira despacio.", pt: "Coloque a m\u00e3o onde o seu beb\u00ea repousa e respire devagar." },
    { en: "Write down one hope you have for your child.", fr: "\u00c9cris un espoir que tu portes pour ton enfant.", es: "Escribe una esperanza que tienes para tu hijo.", pt: "Escreva uma esperan\u00e7a que voc\u00ea tem para o seu filho." },
    { en: "Say their name out loud, if you've chosen one.", fr: "Dis son pr\u00e9nom \u00e0 voix haute, si tu en as choisi un.", es: "Di su nombre en voz alta, si ya lo elegiste.", pt: "Diga o nome dele em voz alta, se j\u00e1 escolheu um." },
    { en: "Tell someone you trust how you really feel today.", fr: "Confie \u00e0 une personne de confiance ce que tu ressens vraiment aujourd'hui.", es: "Cu\u00e9ntale a alguien de confianza c\u00f3mo te sientes de verdad hoy.", pt: "Conte a algu\u00e9m de confian\u00e7a como voc\u00ea realmente se sente hoje." },
    { en: "Rest ten minutes without your phone.", fr: "Repose-toi dix minutes sans ton t\u00e9l\u00e9phone.", es: "Descansa diez minutos sin tu tel\u00e9fono.", pt: "Descanse dez minutos sem o celular." },
    { en: "Write a short note for your child to read one day.", fr: "\u00c9cris un petit mot que ton enfant lira un jour.", es: "Escribe una nota breve para que tu hijo la lea alg\u00fan d\u00eda.", pt: "Escreva um bilhete curto para o seu filho ler um dia." },
    { en: "Choose one small thing to prepare this week.", fr: "Choisis une petite chose \u00e0 pr\u00e9parer cette semaine.", es: "Elige una cosa peque\u00f1a para preparar esta semana.", pt: "Escolha uma coisa pequena para preparar esta semana." },
    { en: "Thank your body for what it is doing.", fr: "Remercie ton corps pour ce qu\u2019il accomplit.", es: "Agradece a tu cuerpo por lo que est\u00e1 haciendo.", pt: "Agrade\u00e7a ao seu corpo pelo que ele est\u00e1 fazendo." },
  ],
  drawing_closer: [
    { en: "Put your phone away for the first ten minutes after waking.", fr: "Range ton t\u00e9l\u00e9phone les dix premi\u00e8res minutes apr\u00e8s le r\u00e9veil.", es: "Deja el tel\u00e9fono los primeros diez minutos tras despertar.", pt: "Deixe o celular de lado nos primeiros dez minutos ap\u00f3s acordar." },
    { en: "Write down one thing you are grateful for.", fr: "Note une chose pour laquelle tu es reconnaissant.", es: "Escribe una cosa por la que est\u00e9s agradecido.", pt: "Escreva uma coisa pela qual voc\u00ea \u00e9 grato." },
    { en: "Say a short prayer for someone before you see them today.", fr: "Dis une courte pri\u00e8re pour quelqu\u2019un avant de le voir aujourd\u2019hui.", es: "Reza una oraci\u00f3n breve por alguien antes de verlo hoy.", pt: "Fa\u00e7a uma ora\u00e7\u00e3o breve por algu\u00e9m antes de v\u00ea-lo hoje." },
    { en: "Step outside and be still for two minutes.", fr: "Sors et reste immobile deux minutes.", es: "Sal afuera y qu\u00e9date quieto dos minutos.", pt: "Saia e fique em sil\u00eancio por dois minutos." },
    { en: "Send a kind message to someone you\u2019ve meant to reach.", fr: "Envoie un message bienveillant \u00e0 quelqu\u2019un que tu voulais joindre.", es: "Env\u00eda un mensaje amable a alguien que quer\u00edas contactar.", pt: "Envie uma mensagem gentil a algu\u00e9m que voc\u00ea queria contatar." },
    { en: "Read one verse slowly, twice.", fr: "Lis un verset lentement, deux fois.", es: "Lee un vers\u00edculo despacio, dos veces.", pt: "Leia um vers\u00edculo devagar, duas vezes." },
    { en: "Pause before your next meal and give thanks.", fr: "Marque une pause avant ton prochain repas et rends gr\u00e2ce.", es: "Haz una pausa antes de tu pr\u00f3xima comida y da gracias.", pt: "Fa\u00e7a uma pausa antes da pr\u00f3xima refei\u00e7\u00e3o e d\u00ea gra\u00e7as." },
    { en: "Forgive yourself for one thing today.", fr: "Pardonne-toi une chose aujourd\u2019hui.", es: "Perd\u00f3nate una cosa hoy.", pt: "Perdoe a si mesmo por uma coisa hoje." },
  ],
  hardship: [
    { en: "Name the one thing weighing on you, out loud or on paper.", fr: "Nomme ce qui te p\u00e8se le plus, \u00e0 voix haute ou par \u00e9crit.", es: "Nombra lo que m\u00e1s te pesa, en voz alta o por escrito.", pt: "D\u00ea nome ao que mais pesa em voc\u00ea, em voz alta ou no papel." },
    { en: "Ask one person for one small piece of help today.", fr: "Demande \u00e0 une personne une petite aide aujourd\u2019hui.", es: "P\u00eddele a alguien una peque\u00f1a ayuda hoy.", pt: "Pe\u00e7a a algu\u00e9m uma pequena ajuda hoje." },
    { en: "Step outside and breathe slowly for two minutes.", fr: "Sors et respire lentement deux minutes.", es: "Sal afuera y respira despacio dos minutos.", pt: "Saia e respire devagar por dois minutos." },
    { en: "Write down one thing that is still good in your life.", fr: "Note une chose qui va encore bien dans ta vie.", es: "Escribe una cosa que sigue estando bien en tu vida.", pt: "Escreva uma coisa que ainda est\u00e1 boa na sua vida." },
    { en: "Do one small task you\u2019ve been putting off.", fr: "Accomplis une petite t\u00e2che que tu repousses.", es: "Haz una peque\u00f1a tarea que has estado posponiendo.", pt: "Fa\u00e7a uma pequena tarefa que voc\u00ea vem adiando." },
    { en: "Let yourself rest without guilt for ten minutes.", fr: "Accorde-toi dix minutes de repos sans culpabilit\u00e9.", es: "Perm\u00edtete descansar sin culpa diez minutos.", pt: "Permita-se descansar sem culpa por dez minutos." },
    { en: "Tell someone you trust how heavy it feels.", fr: "Dis \u00e0 une personne de confiance \u00e0 quel point c\u2019est lourd.", es: "Dile a alguien de confianza lo pesado que se siente.", pt: "Diga a algu\u00e9m de confian\u00e7a como est\u00e1 pesado." },
    { en: "Drink some water and eat something today.", fr: "Bois de l\u2019eau et mange quelque chose aujourd\u2019hui.", es: "Bebe agua y come algo hoy.", pt: "Beba \u00e1gua e coma algo hoje." },
  ],
  loss: [
    { en: "Just breathe with us for one minute.", fr: "Respire simplement avec nous une minute.", es: "Solo respira con nosotros un minuto.", pt: "Apenas respire conosco por um minuto." },
    { en: "Let someone pray for you.", fr: "Laisse quelqu\u2019un prier pour toi.", es: "Deja que alguien ore por ti.", pt: "Deixe algu\u00e9m orar por voc\u00ea." },
    { en: "Name one absence you feel today.", fr: "Nomme une absence que tu ressens aujourd\u2019hui.", es: "Nombra una ausencia que sientes hoy.", pt: "D\u00ea nome a uma aus\u00eancia que voc\u00ea sente hoje." },
    { en: "Hold one good memory for a minute.", fr: "Garde un bon souvenir une minute.", es: "Sostén un buen recuerdo un minuto.", pt: "Segure uma boa lembran\u00e7a por um minuto." },
    { en: "Sit with us. You don\u2019t have to speak.", fr: "Reste avec nous. Tu n\u2019as pas besoin de parler.", es: "Si\u00e9ntate con nosotros. No tienes que hablar.", pt: "Sente-se conosco. Voc\u00ea n\u00e3o precisa falar." },
    { en: "Be prayed for. That\u2019s enough today.", fr: "Laisse-toi porter par la pri\u00e8re. C\u2019est suffisant aujourd\u2019hui.", es: "Deja que oren por ti. Eso es suficiente hoy.", pt: "Seja alvo de ora\u00e7\u00e3o. Isso \u00e9 suficiente hoje." },
    { en: "Name the numbness if it\u2019s there. It counts too.", fr: "Nomme l\u2019engourdissement s\u2019il est l\u00e0. Il compte aussi.", es: "Nombra el entumecimiento si est\u00e1 ah\u00ed. Tambi\u00e9n cuenta.", pt: "D\u00ea nome ao entorpecimento se ele estiver a\u00ed. Ele tamb\u00e9m conta." },
  ],
  health: [
    { en: "Pray for just today. Not the diagnosis, not next month.", fr: "Prie juste pour aujourd\u2019hui. Pas le diagnostic, pas le mois prochain.", es: "Ora solo por hoy. No el diagn\u00f3stico, no el mes que viene.", pt: "Ore apenas por hoje. N\u00e3o pelo diagn\u00f3stico, n\u00e3o pelo pr\u00f3ximo m\u00eas." },
    { en: "Name one fear and set it down here.", fr: "Nomme une peur et d\u00e9pose-la ici.", es: "Nombra un miedo y d\u00e9jalo aqu\u00ed.", pt: "D\u00ea nome a um medo e deixe-o aqui." },
    { en: "One minute of real rest.", fr: "Une minute de vrai repos.", es: "Un minuto de verdadero descanso.", pt: "Um minuto de descanso real." },
    { en: "Pray for the people in this with you.", fr: "Prie pour les personnes qui traversent cela avec toi.", es: "Ora por las personas que est\u00e1n en esto contigo.", pt: "Ore pelas pessoas que est\u00e3o nisso com voc\u00ea." },
    { en: "Name what you\u2019re hoping for, without pretending it\u2019s certain.", fr: "Nomme ce que tu esp\u00e8res, sans pr\u00e9tendre que c\u2019est certain.", es: "Nombra lo que esperas, sin pretender que es seguro.", pt: "D\u00ea nome ao que voc\u00ea espera, sem fingir que \u00e9 certo." },
    { en: "One kind sentence to yourself.", fr: "Une phrase bienveillante pour toi-m\u00eame.", es: "Una frase amable para ti mismo.", pt: "Uma frase gentil para si mesmo." },
    { en: "Be prayed for. You kept showing up.", fr: "Laisse-toi porter par la pri\u00e8re. Tu as continu\u00e9 \u00e0 te pr\u00e9senter.", es: "Deja que oren por ti. Seguiste apareciendo.", pt: "Seja alvo de ora\u00e7\u00e3o. Voc\u00ea continuou aparecendo." },
  ],
  waiting: [
    { en: "Name what you\u2019re waiting for. Say it plainly.", fr: "Nomme ce que tu attends. Dis-le simplement.", es: "Nombra lo que est\u00e1s esperando. Dilo con sencillez.", pt: "D\u00ea nome ao que voc\u00ea est\u00e1 esperando. Diga com simplicidade." },
    { en: "Pray in the meantime, not just for the end of it.", fr: "Prie entre-temps, pas seulement pour la fin.", es: "Ora mientras tanto, no solo por el final.", pt: "Ore enquanto isso, n\u00e3o s\u00f3 pelo fim." },
    { en: "Tell God the silence is hard.", fr: "Dis \u00e0 Dieu que le silence est dur.", es: "Dile a Dios que el silencio es dif\u00edcil.", pt: "Diga a Deus que o sil\u00eancio \u00e9 dif\u00edcil." },
    { en: "Pray for one person who\u2019s also in a wait.", fr: "Prie pour une personne qui attend aussi.", es: "Ora por una persona que tambi\u00e9n est\u00e1 esperando.", pt: "Ore por uma pessoa que tamb\u00e9m est\u00e1 esperando." },
    { en: "Name one thing you still trust, even now.", fr: "Nomme une chose en laquelle tu as encore confiance, m\u00eame maintenant.", es: "Nombra una cosa en la que a\u00fan conf\u00edas, incluso ahora.", pt: "D\u00ea nome a uma coisa em que voc\u00ea ainda confia, mesmo agora." },
    { en: "A prayer to keep your heart soft in the wait.", fr: "Une pri\u00e8re pour garder ton c\u0153ur tendre dans l\u2019attente.", es: "Una oraci\u00f3n para mantener tu coraz\u00f3n tierno en la espera.", pt: "Uma ora\u00e7\u00e3o para manter seu cora\u00e7\u00e3o suave na espera." },
    { en: "Be prayed for. You stayed in it.", fr: "Laisse-toi porter par la pri\u00e8re. Tu es rest\u00e9.", es: "Deja que oren por ti. Te quedaste.", pt: "Seja alvo de ora\u00e7\u00e3o. Voc\u00ea ficou." },
  ],
  relationships: [
    { en: "Hold this person before God for one minute.", fr: "Tiens cette personne devant Dieu une minute.", es: "Sostén a esta persona ante Dios un minuto.", pt: "Segure esta pessoa diante de Deus por um minuto." },
    { en: "Name what hurts, not who\u2019s right.", fr: "Nomme ce qui fait mal, pas qui a raison.", es: "Nombra lo que duele, no qui\u00e9n tiene raz\u00f3n.", pt: "D\u00ea nome ao que d\u00f3i, n\u00e3o a quem est\u00e1 certo." },
    { en: "Ask God to show you your own part.", fr: "Demande \u00e0 Dieu de te montrer ta part.", es: "P\u00eddele a Dios que te muestre tu parte.", pt: "Pe\u00e7a a Deus que mostre a sua parte." },
    { en: "Take one small step toward letting go.", fr: "Fais un petit pas vers le l\u00e2cher-prise.", es: "Da un peque\u00f1o paso hacia soltar.", pt: "D\u00ea um pequeno passo em dire\u00e7\u00e3o a soltar." },
    { en: "Pray something genuinely good for them.", fr: "Prie quelque chose de sinc\u00e8rement bon pour eux.", es: "Ora algo genuinamente bueno por ellos.", pt: "Ore algo genuinamente bom por eles." },
    { en: "Pray for restoration, then hand the result over.", fr: "Prie pour la restauration, puis remets le r\u00e9sultat.", es: "Ora por la restauraci\u00f3n, luego entrega el resultado.", pt: "Ore pela restaura\u00e7\u00e3o, depois entregue o resultado." },
    { en: "Be prayed for. You chose love all week.", fr: "Laisse-toi porter par la pri\u00e8re. Tu as choisi l\u2019amour toute la semaine.", es: "Deja que oren por ti. Elegiste el amor toda la semana.", pt: "Seja alvo de ora\u00e7\u00e3o. Voc\u00ea escolheu o amor a semana toda." },
  ],
};

// -- 3. REFLECTION_TEMPLATES (keyed by tone) -------------------------

const REFLECTION_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle: {
    en: "Where did you notice a moment of quiet today? Stay with it for one breath, and offer it up.",
    fr: "O\u00f9 as-tu remarqu\u00e9 un moment de calme aujourd\u2019hui ? Reste avec lui le temps d\u2019un souffle, et offre-le.",
    es: "\u00bfD\u00f3nde notaste un momento de calma hoy? Qu\u00e9date con \u00e9l un respiro y ofr\u00e9celo.",
    pt: "Onde voc\u00ea notou um momento de calma hoje? Fique com ele por um respiro e ofere\u00e7a-o.",
  },
  grateful: {
    en: "Name one gift from today you might have rushed past. Let yourself feel thankful for it.",
    fr: "Nomme un cadeau d\u2019aujourd\u2019hui que tu as peut-\u00eatre laiss\u00e9 filer. Laisse-toi en \u00eatre reconnaissant.",
    es: "Nombra un regalo de hoy que quiz\u00e1s pasaste por alto. Perm\u00edtete agradecerlo.",
    pt: "D\u00ea nome a um presente de hoje que talvez voc\u00ea tenha deixado passar. Permita-se agradecer por ele.",
  },
  steadying: {
    en: "What is one thing still steady beneath you right now? Rest your weight there.",
    fr: "Qu\u2019est-ce qui reste solide sous tes pieds en ce moment ? Pose-y ton poids.",
    es: "\u00bfQu\u00e9 sigue firme bajo tus pies ahora mismo? Apoya all\u00ed tu peso.",
    pt: "O que ainda est\u00e1 firme sob os seus p\u00e9s agora? Apoie ali o seu peso.",
  },
  honest: {
    en: "What are you carrying that you haven\u2019t said out loud? Say it here, plainly. Nothing is too much.",
    fr: "Que portes-tu sans l\u2019avoir dit \u00e0 voix haute ? Dis-le ici, simplement. Rien n\u2019est de trop.",
    es: "\u00bfQu\u00e9 cargas que no has dicho en voz alta? Dilo aqu\u00ed, con sencillez. Nada es demasiado.",
    pt: "O que voc\u00ea carrega e ainda n\u00e3o disse em voz alta? Diga aqui, com simplicidade. Nada \u00e9 demais.",
  },
  tender: {
    en: "Be gentle with yourself for a moment. What do you most need to hear right now?",
    fr: "Sois doux envers toi un instant. Qu\u2019as-tu le plus besoin d\u2019entendre maintenant ?",
    es: "S\u00e9 amable contigo un momento. \u00bfQu\u00e9 es lo que m\u00e1s necesitas escuchar ahora?",
    pt: "Seja gentil consigo por um momento. O que voc\u00ea mais precisa ouvir agora?",
  },
  lifting: {
    en: "Where did you glimpse a little light today? Let it grow as you pray.",
    fr: "O\u00f9 as-tu entrevu un peu de lumi\u00e8re aujourd\u2019hui ? Laisse-la grandir en priant.",
    es: "\u00bfD\u00f3nde vislumbraste un poco de luz hoy? Deja que crezca mientras rezas.",
    pt: "Onde voc\u00ea vislumbrou um pouco de luz hoje? Deixe-a crescer enquanto reza.",
  },
};

// -- 4. MEDITATION_TEMPLATES (keyed by tone) -------------------------

const MEDITATION_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle: {
    en: "Read today\u2019s verse slowly, twice. Then sit in silence and let one word settle. Carry that word with you.",
    fr: "Lis le verset du jour lentement, deux fois. Puis reste en silence et laisse un mot se d\u00e9poser. Emporte ce mot avec toi.",
    es: "Lee el vers\u00edculo de hoy despacio, dos veces. Luego qu\u00e9date en silencio y deja que una palabra se asiente. Ll\u00e9vala contigo.",
    pt: "Leia o vers\u00edculo de hoje devagar, duas vezes. Depois fique em sil\u00eancio e deixe uma palavra repousar. Leve-a com voc\u00ea.",
  },
  grateful: {
    en: "Receive today\u2019s verse as a gift. Rest in it without rushing. Leave with one thing to thank God for.",
    fr: "Re\u00e7ois le verset du jour comme un cadeau. Demeure en lui sans te presser. Repars avec une raison de rendre gr\u00e2ce \u00e0 Dieu.",
    es: "Recibe el vers\u00edculo de hoy como un regalo. Permanece en \u00e9l sin prisa. Vete con un motivo para dar gracias a Dios.",
    pt: "Receba o vers\u00edculo de hoje como um presente. Permane\u00e7a nele sem pressa. Saia com um motivo para agradecer a Deus.",
  },
  steadying: {
    en: "Let today\u2019s verse be solid ground. Breathe slowly and stand on it. Carry its steadiness into your day.",
    fr: "Que le verset du jour soit un sol ferme. Respire lentement et tiens-toi dessus. Emporte sa stabilit\u00e9 dans ta journ\u00e9e.",
    es: "Que el vers\u00edculo de hoy sea suelo firme. Respira despacio y af\u00edrmate en \u00e9l. Lleva su firmeza a tu d\u00eda.",
    pt: "Que o vers\u00edculo de hoje seja ch\u00e3o firme. Respire devagar e firme-se nele. Leve a sua firmeza para o seu dia.",
  },
  honest: {
    en: "Bring today\u2019s verse your real self \u2014 nothing hidden. Sit with it honestly. Let it meet you where you are.",
    fr: "Apporte au verset du jour ton vrai visage \u2014 rien de cach\u00e9. Demeure avec lui en v\u00e9rit\u00e9. Laisse-le te rejoindre l\u00e0 o\u00f9 tu es.",
    es: "Lleva al vers\u00edculo de hoy tu verdadero yo, sin esconder nada. Qu\u00e9date con \u00e9l con sinceridad. Deja que te encuentre donde est\u00e1s.",
    pt: "Leve ao vers\u00edculo de hoje o seu verdadeiro eu, sem esconder nada. Fique com ele com sinceridade. Deixe-o encontrar voc\u00ea onde est\u00e1.",
  },
  tender: {
    en: "Let today\u2019s verse hold you gently. Rest in it like rest itself. Carry its tenderness with you.",
    fr: "Laisse le verset du jour te porter avec douceur. Repose-toi en lui comme dans le repos m\u00eame. Emporte sa tendresse avec toi.",
    es: "Deja que el vers\u00edculo de hoy te sostenga con ternura. Descansa en \u00e9l como en el descanso mismo. Lleva su ternura contigo.",
    pt: "Deixe o vers\u00edculo de hoje segurar voc\u00ea com ternura. Descanse nele como no pr\u00f3prio descanso. Leve a sua ternura com voc\u00ea.",
  },
  lifting: {
    en: "Let today\u2019s verse lift your eyes. Sit with the hope in it. Carry that hope into what\u2019s next.",
    fr: "Que le verset du jour rel\u00e8ve ton regard. Demeure avec l\u2019esp\u00e9rance qu\u2019il porte. Emporte cette esp\u00e9rance vers la suite.",
    es: "Que el vers\u00edculo de hoy levante tu mirada. Qu\u00e9date con la esperanza que encierra. Ll\u00e9vala hacia lo que viene.",
    pt: "Que o vers\u00edculo de hoje levante o seu olhar. Fique com a esperan\u00e7a que ele traz. Leve essa esperan\u00e7a para o que vem.",
  },
};

// -- 4b. NEW CARD TYPE TEMPLATES (Journey Reframe) ----------------------

const SCRIPTURE_TEMPLATES: Record<JourneyTone, Record<Lang, { body: string; scriptureRef: string }>> = {
  gentle:    { en: { body: "He is close to the brokenhearted and saves those who are crushed in spirit.", scriptureRef: "Psalm 34:18" }, fr: { body: "L'Éternel est près de ceux qui ont le cœur brisé.", scriptureRef: "Psaume 34:18" }, es: { body: "El Señor está cerca de los quebrantados de corazón.", scriptureRef: "Salmo 34:18" }, pt: { body: "O Senhor está perto dos que têm o coração partido.", scriptureRef: "Salmo 34:18" } },
  grateful:  { en: { body: "Give thanks in all circumstances; for this is God's will for you.", scriptureRef: "1 Thessalonians 5:18" }, fr: { body: "Rendez grâces en toutes choses.", scriptureRef: "1 Thessaloniciens 5:18" }, es: { body: "Dad gracias en todo.", scriptureRef: "1 Tesalonicenses 5:18" }, pt: { body: "Deem graças em todas as circunstâncias.", scriptureRef: "1 Tessalonicenses 5:18" } },
  steadying: { en: { body: "Be still and know that I am God.", scriptureRef: "Psalm 46:10" }, fr: { body: "Arrêtez, et sachez que je suis Dieu.", scriptureRef: "Psaume 46:10" }, es: { body: "Quédense quietos, reconozcan que yo soy Dios.", scriptureRef: "Salmo 46:10" }, pt: { body: "Aquietem-se e saibam que eu sou Deus.", scriptureRef: "Salmo 46:10" } },
  honest:    { en: { body: "My grace is sufficient for you, for my power is made perfect in weakness.", scriptureRef: "2 Corinthians 12:9" }, fr: { body: "Ma grâce te suffit, car ma puissance s'accomplit dans la faiblesse.", scriptureRef: "2 Corinthiens 12:9" }, es: { body: "Te basta con mi gracia, pues mi poder se perfecciona en la debilidad.", scriptureRef: "2 Corintios 12:9" }, pt: { body: "A minha graça te basta, porque o meu poder se aperfeiçoa na fraqueza.", scriptureRef: "2 Coríntios 12:9" } },
  tender:    { en: { body: "Blessed are those who mourn, for they will be comforted.", scriptureRef: "Matthew 5:4" }, fr: { body: "Heureux les affligés, car ils seront consolés.", scriptureRef: "Matthieu 5:4" }, es: { body: "Bienaventurados los que lloran, porque serán consolados.", scriptureRef: "Mateo 5:4" }, pt: { body: "Bem-aventurados os que choram, pois serão consolados.", scriptureRef: "Mateus 5:4" } },
  lifting:   { en: { body: "For you created my inmost being; you knit me together in my mother's womb.", scriptureRef: "Psalm 139:13" }, fr: { body: "C'est toi qui as formé mes reins, qui m'as tissé dans le sein de ma mère.", scriptureRef: "Psaume 139:13" }, es: { body: "Tú creaste mis entrañas; me formaste en el vientre de mi madre.", scriptureRef: "Salmo 139:13" }, pt: { body: "Tu criaste o meu íntimo e me teceste no ventre de minha mãe.", scriptureRef: "Salmo 139:13" } },
};

// ═══════════════════════════════════════════════════════════════════
// v5.20.0 — VERIFIED SCRIPTURE POOL, INDEXED BY EMOTIONAL CORE
// Bright line: NO AI-generated verses. Every entry is exact NIV text sourced
// from a verified source (Bible Gateway), so the same fear/grief/anxiety verse
// serves multiple Tier-1 doors. en is verified now; fr/es/pt verified
// translations are a Phase-2 authoring task (NOT to be generated). This pool is
// the library the Phase-2 content matrix will select from by {spine × phase ×
// dominant-emotion}; getTodayAction still uses the tone pool until then.
// ═══════════════════════════════════════════════════════════════════
// v5.20.15 — per-language verse slots. `en` filled/verified; fr/es/pt slots
// added by Samy's sourcing. A language only serves core verses once ALL its
// slots are filled (see TIER1_SCRIPTURE_READY) — a check, not a memory.
const SCRIPTURE_BY_CORE: Record<string, Partial<Record<Lang, { body: string; scriptureRef: string }>>> = {
  fear:              { en: { body: "So do not fear, for I am with you; do not be dismayed, for I am your God. I will strengthen you and help you.", scriptureRef: "Isaiah 41:10" } },
  strength_for_today:{ en: { body: "Because of the Lord's great love we are not consumed, for his compassions never fail. They are new every morning.", scriptureRef: "Lamentations 3:22-23" } },
  anxiety:           { en: { body: "Do not be anxious about anything, but in every situation, by prayer and petition, with thanksgiving, present your requests to God.", scriptureRef: "Philippians 4:6" } },
  waiting:           { en: { body: "Wait for the Lord; be strong and take heart and wait for the Lord.", scriptureRef: "Psalm 27:14" } },
  peace:             { en: { body: "You will keep in perfect peace those whose minds are steadfast, because they trust in you.", scriptureRef: "Isaiah 26:3" } },
  endurance:         { en: { body: "Therefore we do not lose heart. Though outwardly we are wasting away, yet inwardly we are being renewed day by day.", scriptureRef: "2 Corinthians 4:16" } },
  presence:          { en: { body: "And surely I am with you always, to the very end of the age.", scriptureRef: "Matthew 28:20" } },
  loneliness:        { en: { body: "God sets the lonely in families.", scriptureRef: "Psalm 68:6" } },
  helplessness:      { en: { body: "Cast all your anxiety on him because he cares for you.", scriptureRef: "1 Peter 5:7" } },
  surrender:         { en: { body: "Trust in the Lord with all your heart and lean not on your own understanding; in all your ways submit to him, and he will make your paths straight.", scriptureRef: "Proverbs 3:5-6" } },
  hope_held_loosely: { en: { body: "Be joyful in hope, patient in affliction, faithful in prayer.", scriptureRef: "Romans 12:12" } },
  boundaries:        { en: { body: "For each one should carry their own load.", scriptureRef: "Galatians 6:5" } },
  tend_own_heart:    { en: { body: "Above all else, guard your heart, for everything you do flows from it.", scriptureRef: "Proverbs 4:23" } },
  exhaustion:        { en: { body: "Come to me, all you who are weary and burdened, and I will give you rest.", scriptureRef: "Matthew 11:28" } },
  being_seen:        { en: { body: "You are the God who sees me.", scriptureRef: "Genesis 16:13" } },
  strength:          { en: { body: "But those who hope in the Lord will renew their strength. They will soar on wings like eagles; they will run and not grow weary.", scriptureRef: "Isaiah 40:31" } },
  // already present in the tone pool — mirrored here so the core index is complete
  grief:             { en: { body: "He is close to the brokenhearted and saves those who are crushed in spirit.", scriptureRef: "Psalm 34:18" } },
  comfort:           { en: { body: "Blessed are those who mourn, for they will be comforted.", scriptureRef: "Matthew 5:4" } },
  belovedness:       { en: { body: "For you created my inmost being; you knit me together in my mother's womb.", scriptureRef: "Psalm 139:13" } },
  // v5.25.0 — v2.5 funnel cores (Samy-approved verse picks, 2026-07-10). Several sit
  // ONE verse from an outcome promise; truncation is load-bearing (see APPROVED_CLAUSES
  // + the scripture_clause_gate self-test). Lament register: validate, never resolve.
  anticipatory_future:{ en: { body: "Fear not, for I have redeemed you; I have summoned you by name; you are mine. When you pass through the waters, I will be with you; and when you pass through the rivers, they will not sweep over you.", scriptureRef: "Isaiah 43:1-2" } },
  guilt_shame:       { en: { body: "Therefore, there is now no condemnation for those who are in Christ Jesus.", scriptureRef: "Romans 8:1" } },
  // Psalm 34:17 FIRST CLAUSE ONLY — the "delivers them from all their troubles" continuation must NOT render.
  unanswered_prayer: { en: { body: "The righteous cry out, and the Lord hears them.", scriptureRef: "Psalm 34:17" } },
  // Psalm 13:1-2 ONLY — vv5-6 ("But I trust in your unfailing love…") must NOT render. No resolution; the rail carries it.
  anger_at_God:      { en: { body: "How long, Lord? Will you forget me forever? How long will you hide your face from me? How long must I wrestle with my thoughts and day after day have sorrow in my heart?", scriptureRef: "Psalm 13:1-2" } },
  doubt_of_faith:    { en: { body: "Immediately the boy's father exclaimed, “I do believe; help me overcome my unbelief!”", scriptureRef: "Mark 9:24" } },
  relational:        { en: { body: "But while he was still a long way off, his father saw him and was filled with compassion for him; he ran to his son, threw his arms around him and kissed him.", scriptureRef: "Luke 15:20" } },
  darkness:          { en: { body: "If I say, “Surely the darkness will hide me and the light become night around me,” even the darkness will not be dark to you; the night will shine like the day, for darkness is as light to you.", scriptureRef: "Psalm 139:11-12" } },
};

// v5.25.0 — Structural readiness-gate registry (Samy item #4): the canonical
// approved clause for each funnel core. The gate verifies rendered slot text ==
// this verbatim, and that no forbidden continuation (an outcome-promise sitting
// one verse away) leaks in. Checked at boot, never by memory.
const APPROVED_CLAUSES: Record<string, { body: string; scriptureRef: string; forbidden?: string[] }> = {
  anticipatory_future: { body: "Fear not, for I have redeemed you; I have summoned you by name; you are mine. When you pass through the waters, I will be with you; and when you pass through the rivers, they will not sweep over you.", scriptureRef: "Isaiah 43:1-2" },
  guilt_shame:         { body: "Therefore, there is now no condemnation for those who are in Christ Jesus.", scriptureRef: "Romans 8:1" },
  unanswered_prayer:   { body: "The righteous cry out, and the Lord hears them.", scriptureRef: "Psalm 34:17", forbidden: ["delivers them", "all their troubles"] },
  anger_at_God:        { body: "How long, Lord? Will you forget me forever? How long will you hide your face from me? How long must I wrestle with my thoughts and day after day have sorrow in my heart?", scriptureRef: "Psalm 13:1-2", forbidden: ["trust in your unfailing love", "rejoice in your salvation", "sing the Lord's praise", "he has been good to me"] },
  doubt_of_faith:      { body: "Immediately the boy's father exclaimed, “I do believe; help me overcome my unbelief!”", scriptureRef: "Mark 9:24" },
  relational:          { body: "But while he was still a long way off, his father saw him and was filled with compassion for him; he ran to his son, threw his arms around him and kissed him.", scriptureRef: "Luke 15:20" },
  darkness:            { body: "If I say, “Surely the darkness will hide me and the light become night around me,” even the darkness will not be dark to you; the night will shine like the day, for darkness is as light to you.", scriptureRef: "Psalm 139:11-12" },
};

// v5.25.0 — the gate (item #4). Structural, at boot: what renders must equal the
// approved clause verbatim and carry NO forbidden outcome-promise continuation.
const scriptureClauseGate = (() => {
  const rows = Object.entries(APPROVED_CLAUSES).map(([core, ap]) => {
    const slot = SCRIPTURE_BY_CORE[core]?.en;
    const leaked = (ap.forbidden || []).filter((f) => (slot?.body || "").toLowerCase().includes(f.toLowerCase()));
    return { core, ref: ap.scriptureRef, body_verbatim: slot?.body === ap.body, ref_match: slot?.scriptureRef === ap.scriptureRef, no_forbidden_continuation: leaked.length === 0, leaked };
  });
  return { PASS: rows.every((r) => r.body_verbatim && r.ref_match && r.no_forbidden_continuation), cores_checked: rows.length, rows };
})();
console.log(`[v5.25.0] scripture clause gate:`, scriptureClauseGate.PASS ? "PASS" : "FAIL");

function scriptureForCore(core: string, lang: Lang = "en"): { body: string; scriptureRef: string } | null {
  const entry = SCRIPTURE_BY_CORE[core];
  if (!entry) return null;
  if (entry[lang]) return entry[lang]!;
  // Loud, never silent: a missing localized slot is a sourcing gap, not a no-op.
  if (entry.en) { console.warn(`[scripture] core "${core}" has no "${lang}" verse — English fallback; fill SCRIPTURE_BY_CORE.${core}.${lang}`); return entry.en; }
  return null;
}
// Readiness CHECK (computed, not hardcoded): a language may serve core verses
// only when EVERY core used by a Tier-1 door has that language's slot filled.
// Flipping TIER1_ENABLED can never serve half-English scripture to fr/es/pt.
const TIER1_CORES_IN_USE: string[] = Array.from(new Set(Object.values(TIER1_DOORS).flatMap((d: any) => d.scriptureCores || [])));
const TIER1_SCRIPTURE_READY: Record<Lang, boolean> = (["en", "fr", "es", "pt"] as Lang[]).reduce((acc, lang) => {
  acc[lang] = TIER1_CORES_IN_USE.length > 0 && TIER1_CORES_IN_USE.every((core) => !!SCRIPTURE_BY_CORE[core]?.[lang]);
  return acc;
}, {} as Record<Lang, boolean>);
console.log(`[v5.20.15] Tier-1 scripture readiness:`, JSON.stringify(TIER1_SCRIPTURE_READY), `(cores in use: ${TIER1_CORES_IN_USE.length})`);
// v5.20.17 — denominator policy per Tier-1 door (proof of the standing rule).
const DENOMINATOR_POLICY: Record<string, any> = Object.fromEntries(
  Object.entries(TIER1_DOORS).filter(([k]) => !k.startsWith("faith/")).map(([k, d]: [string, any]) => {
    const len = d.length ?? null;
    const shows = d.mode !== "open" && !!len && !["loss", "relationships"].includes(d.family);
    return [k, { family: d.family, mode: d.mode, length: len, showsDenominator: shows }];
  })
);

// Content matrix: choose the scripture emotional core for a scripture card by
// {door × phase × dominant-emotion}. Prefer the user's named emotion when the
// door serves it; else cycle the door's cores by phase. null → no door → caller
// falls back to the tone pool.
function pickScriptureCore(door: string | null | undefined, phaseIndex: number, dominantEmotion: string | null | undefined): string | null {
  if (!door) return null;
  const d = TIER1_DOORS[door];
  if (!d || !d.scriptureCores.length) return null;
  if (dominantEmotion && d.scriptureCores.includes(dominantEmotion)) return dominantEmotion;
  const n = d.scriptureCores.length;
  const idx = ((phaseIndex % n) + n) % n;
  return d.scriptureCores[idx];
}

const GRATITUDE_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle:    { en: "Name one thing you didn't lose in all of this.", fr: "Nomme une chose que tu n'as pas perdue dans tout cela.", es: "Nombra una cosa que no perdiste en todo esto.", pt: "Diga uma coisa que você não perdeu em tudo isso." },
  grateful:  { en: "Three things you didn't earn but received anyway.", fr: "Trois choses que tu n'as pas méritées mais que tu as reçues quand même.", es: "Tres cosas que no te ganaste pero recibiste de todas formas.", pt: "Três coisas que você não mereceu mas recebeu assim mesmo." },
  steadying: { en: "One friendship that got stronger through this.", fr: "Une amitié qui s'est renforcée à travers cela.", es: "Una amistad que se fortaleció a través de esto.", pt: "Uma amizade que ficou mais forte com tudo isso." },
  honest:    { en: "One thing that's clearer now than it was a month ago.", fr: "Une chose qui est plus claire maintenant qu'il y a un mois.", es: "Una cosa que ahora está más clara que hace un mes.", pt: "Uma coisa que agora está mais clara do que há um mês." },
  tender:    { en: "One memory that still makes you smile.", fr: "Un souvenir qui te fait encore sourire.", es: "Un recuerdo que todavía te hace sonreír.", pt: "Uma memória que ainda te faz sorrir." },
  lifting:   { en: "One thing about this season that surprised you in a good way.", fr: "Une chose de cette saison qui t'a agréablement surprise.", es: "Una cosa de esta temporada que te sorprendió para bien.", pt: "Uma coisa desta estação que te surpreendeu de forma boa." },
};

const JOURNAL_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle:    { en: "Write a letter to the person you were before this started.", fr: "Écris une lettre à la personne que tu étais avant que tout cela commence.", es: "Escribe una carta a la persona que eras antes de que esto empezara.", pt: "Escreva uma carta para a pessoa que você era antes de tudo isso começar." },
  grateful:  { en: "Write down three things you want in your next chapter.", fr: "Note trois choses que tu veux dans ton prochain chapitre.", es: "Escribe tres cosas que quieres en tu próximo capítulo.", pt: "Escreva três coisas que você quer no próximo capítulo." },
  steadying: { en: "What question would you ask God if you knew He'd answer out loud?", fr: "Quelle question poserais-tu à Dieu si tu savais qu'Il répondrait à voix haute?", es: "¿Qué pregunta le harías a Dios si supieras que respondería en voz alta?", pt: "Que pergunta você faria a Deus se soubesse que Ele responderia em voz alta?" },
  honest:    { en: "Write about the version of yourself on the other side of this.", fr: "Écris sur la version de toi-même de l'autre côté de tout cela.", es: "Escribe sobre la versión de ti mismo al otro lado de esto.", pt: "Escreva sobre a versão de si mesmo do outro lado disso tudo." },
  tender:    { en: "Write to them. What do you want them to know?", fr: "Écris-leur. Que veux-tu qu'ils sachent?", es: "Escríbeles. ¿Qué quieres que sepan?", pt: "Escreva para eles. O que você quer que saibam?" },
  lifting:   { en: "Write about what freedom actually looks like for you.", fr: "Écris sur ce à quoi la liberté ressemble vraiment pour toi.", es: "Escribe sobre cómo se ve realmente la libertad para ti.", pt: "Escreva sobre como realmente é a liberdade para você." },
};

const REST_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle:    { en: "Today, just breathe. You showed up. That's enough.", fr: "Aujourd'hui, respire simplement. Tu t'es présenté. C'est suffisant.", es: "Hoy, solo respira. Te presentaste. Eso es suficiente.", pt: "Hoje, apenas respire. Você apareceu. Isso é suficiente." },
  grateful:  { en: "You are doing more than you think. Today, let 'enough' be enough.", fr: "Tu fais plus que tu ne le penses. Aujourd'hui, laisse 'suffisant' être suffisant.", es: "Estás haciendo más de lo que crees. Hoy, deja que 'suficiente' sea suficiente.", pt: "Você está fazendo mais do que pensa. Hoje, deixe 'suficiente' ser suficiente." },
  steadying: { en: "You've done hard work this week. Today, just exist. No processing. No progress. Just be.", fr: "Tu as fait un travail difficile cette semaine. Aujourd'hui, existe simplement.", es: "Has hecho un trabajo duro esta semana. Hoy, solo existe.", pt: "Você fez um trabalho difícil esta semana. Hoje, apenas exista." },
  honest:    { en: "You don't have to white-knuckle today. Just don't pick it up. That's enough.", fr: "Tu n'as pas besoin de serrer les dents aujourd'hui. Ne reprends pas. C'est suffisant.", es: "No tienes que apretar los dientes hoy. Solo no lo retomes. Eso es suficiente.", pt: "Você não precisa apertar os dentes hoje. Apenas não pegue de volta. Isso é suficiente." },
  tender:    { en: "Grief is exhausting. Today, you don't have to be strong.", fr: "Le deuil est épuisant. Aujourd'hui, tu n'as pas besoin d'être fort.", es: "El duelo es agotador. Hoy, no tienes que ser fuerte.", pt: "O luto é exaustivo. Hoje, você não precisa ser forte." },
  lifting:   { en: "Treatment is work. Recovery is work. Today, rest is the assignment.", fr: "Le traitement est un travail. La récupération est un travail. Aujourd'hui, le repos est la mission.", es: "El tratamiento es trabajo. La recuperación es trabajo. Hoy, descansar es la tarea.", pt: "O tratamento é trabalho. A recuperação é trabalho. Hoje, descansar é a tarefa." },
};

const CONFESSION_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle:    { en: "What are you avoiding right now?", fr: "Qu'est-ce que tu évites en ce moment?", es: "¿Qué estás evitando en este momento?", pt: "O que você está evitando agora?" },
  grateful:  { en: "What part of this makes you feel guilty? Say it out loud.", fr: "Quelle partie de tout cela te fait culpabiliser? Dis-le à voix haute.", es: "¿Qué parte de esto te hace sentir culpable? Dilo en voz alta.", pt: "Que parte disso faz você se sentir culpado? Diga em voz alta." },
  steadying: { en: "What's one thing you haven't said out loud?", fr: "Quelle est une chose que tu n'as pas dite à voix haute?", es: "¿Cuál es una cosa que no has dicho en voz alta?", pt: "Qual é uma coisa que você não disse em voz alta?" },
  honest:    { en: "When was the last time you almost gave in? What stopped you?", fr: "Quand as-tu failli céder pour la dernière fois? Qu'est-ce qui t'a arrêté?", es: "¿Cuándo fue la última vez que casi cediste? ¿Qué te detuvo?", pt: "Quando foi a última vez que quase cedeu? O que te parou?" },
  tender:    { en: "What emotion are you not letting yourself feel?", fr: "Quelle émotion ne te permets-tu pas de ressentir?", es: "¿Qué emoción no te estás permitiendo sentir?", pt: "Que emoção você não está se permitindo sentir?" },
  lifting:   { en: "What are you not telling your loved ones about how you feel?", fr: "Que ne dis-tu pas à tes proches sur ce que tu ressens?", es: "¿Qué no les estás diciendo a tus seres queridos sobre cómo te sientes?", pt: "O que você não está dizendo aos seus entes queridos sobre como se sente?" },
};

const ENCOURAGEMENT_TEMPLATES: Record<JourneyTone, Record<Lang, string>> = {
  gentle:    { en: "You showed up again. That matters more than you think.", fr: "Tu t'es présenté à nouveau. Cela compte plus que tu ne le penses.", es: "Te presentaste de nuevo. Eso importa más de lo que crees.", pt: "Você apareceu de novo. Isso importa mais do que você pensa." },
  grateful:  { en: "Your child doesn't need a perfect parent. They need a present one. You're here.", fr: "Ton enfant n'a pas besoin d'un parent parfait. Il a besoin d'un parent présent. Tu es là.", es: "Tu hijo no necesita un padre perfecto. Necesita uno presente. Estás aquí.", pt: "Seu filho não precisa de um pai perfeito. Precisa de um presente. Você está aqui." },
  steadying: { en: "Faith isn't feeling certain. It's showing up uncertain and staying anyway.", fr: "La foi ce n'est pas se sentir certain. C'est se présenter incertain et rester quand même.", es: "La fe no es sentirse seguro. Es presentarse inseguro y quedarse de todas formas.", pt: "Fé não é se sentir seguro. É aparecer inseguro e ficar mesmo assim." },
  honest:    { en: "Ten days. You're still here. You're still walking. That's not nothing.", fr: "Dix jours. Tu es encore là. Tu marches encore. Ce n'est pas rien.", es: "Diez días. Sigues aquí. Sigues caminando. Eso no es nada.", pt: "Dez dias. Você ainda está aqui. Ainda está caminhando. Isso não é pouco." },
  tender:    { en: "They would be proud of you for doing this. For not running from it.", fr: "Ils seraient fiers de toi pour avoir fait cela. Pour ne pas avoir fui.", es: "Estarían orgullosos de ti por hacer esto. Por no huir de ello.", pt: "Eles ficariam orgulhosos de você por fazer isso. Por não fugir disso." },
  lifting:   { en: "You are not your diagnosis. You are the person fighting it.", fr: "Tu n'es pas ton diagnostic. Tu es la personne qui le combat.", es: "No eres tu diagnóstico. Eres la persona que lo combate.", pt: "Você não é seu diagnóstico. Você é a pessoa que luta contra ele." },
};

// -- 5. GRADUATION_MESSAGES (gender-neutral, outcome-neutral) ----------
// Never assumes a happy ending. Centers on companionship.
// No "congratulations," "complete," "finished," or outcome-specific language.

const GRADUATION_MESSAGES: Record<string, Record<Lang, string>> = {
  through_a_hard_season: {
    en: "Thirty days walked. Whatever this season holds, you didn't walk it alone.",
    fr: "Trente jours de marche. Quelle que soit cette saison, tu ne l\u2019as pas travers\u00e9e dans la solitude.",
    es: "Treinta d\u00edas de camino. Sea lo que sea esta temporada, no la caminaste en soledad.",
    pt: "Trinta dias de caminhada. Seja o que for esta fase, voc\u00ea n\u00e3o a atravessou em solid\u00e3o.",
  },
  expecting: {
    en: "Forty weeks of prayer. However this chapter unfolds, you were never alone in it.",
    fr: "Quarante semaines de pri\u00e8re. Quelle que soit la suite de ce chapitre, tu n\u2019as jamais \u00e9t\u00e9 sans pr\u00e9sence.",
    es: "Cuarenta semanas de oraci\u00f3n. Como sea que siga este cap\u00edtulo, nunca hubo soledad en el camino.",
    pt: "Quarenta semanas de ora\u00e7\u00e3o. Como quer que este cap\u00edtulo se desenrole, nunca houve solid\u00e3o no caminho.",
  },
  walking_through_grief: {
    en: "Thirty days of sitting with you. However grief moves, you were never alone in it.",
    fr: "Trente jours \u00e0 tes c\u00f4t\u00e9s. Quoi que le deuil apporte, tu n\u2019\u00e9tais jamais seul.",
    es: "Treinta d\u00edas a tu lado. Sea como sea el duelo, nunca estuviste en soledad.",
    pt: "Trinta dias ao seu lado. Seja como for o luto, voc\u00ea nunca esteve em solid\u00e3o.",
  },
  through_illness_and_healing: {
    en: "Thirty days of showing up. Whatever this body carries, you carried it with company.",
    fr: "Trente jours de pr\u00e9sence. Quoi que ce corps porte, tu l\u2019as port\u00e9 accompagn\u00e9.",
    es: "Treinta d\u00edas de presentarte. Lo que sea que este cuerpo cargue, lo cargaste acompa\u00f1ado.",
    pt: "Trinta dias de presen\u00e7a. O que quer que este corpo carregue, voc\u00ea o carregou acompanhado.",
  },
  the_season_of_waiting: {
    en: "Thirty days of staying in it. Whatever the answer becomes, you waited with God.",
    fr: "Trente jours de pers\u00e9v\u00e9rance. Quelle que soit la r\u00e9ponse, tu as attendu avec Dieu.",
    es: "Treinta d\u00edas de perseverancia. Sea cual sea la respuesta, esperaste con Dios.",
    pt: "Trinta dias de perseveran\u00e7a. Seja qual for a resposta, voc\u00ea esperou com Deus.",
  },
  praying_for_someone: {
    en: "Thirty days of choosing love. Whatever happens between you, you brought it to God first.",
    fr: "Trente jours \u00e0 choisir l\u2019amour. Quoi qu\u2019il advienne entre vous, tu l\u2019as d\u2019abord port\u00e9 devant Dieu.",
    es: "Treinta d\u00edas de elegir el amor. Pase lo que pase entre ustedes, lo llevaste primero ante Dios.",
    pt: "Trinta dias de escolher o amor. Aconte\u00e7a o que acontecer entre voc\u00eas, voc\u00ea levou primeiro a Deus.",
  },
  // v5.20.13 \u2014 outcome-neutral: never presumes the result. No "good news", no relief-on-arrival.
  body_test_results: {
    en: "Two weeks of waiting with God. Whatever the results say, you didn't wait alone.",
    fr: "Deux semaines d\u2019attente avec Dieu. Quels que soient les r\u00e9sultats, tu n\u2019as pas attendu seul.",
    es: "Dos semanas esperando con Dios. Digan lo que digan los resultados, no esperaste en soledad.",
    pt: "Duas semanas esperando com Deus. Seja qual for o resultado, voc\u00ea n\u00e3o esperou sozinho.",
  },
};

// -- 6. Daily-action generator ----------------------------------------
// SAME hash function as seededFallbackPrayer: deterministic, stable.

function journeyHash(key: string): number {
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
  return hash;
}

function resolveJourneyPhase(template: JourneyTemplate, currentDay: number): JourneyPhase {
  for (const phase of template.phases) {
    if (currentDay >= phase.dayStart && currentDay <= phase.dayEnd) return phase;
  }
  // Open journeys past last phase: repeat final phase
  return template.phases[template.phases.length - 1];
}

async function getTodayAction(instance: any, lang: Lang): Promise<JourneyDailyAction> {
  const day = instance.current_day;
  const instanceId = instance.id;
  const family: JourneyFamily = instance.family;
  const templateKey = instance.template_key;
  const prayedForName: string | null = instance.prayed_for_name || null;

  // 1. CHECK CACHE — idempotency guarantee
  try {
    const cached = await pool.query(
      "SELECT type, phase_label, content_json FROM journey_daily_actions WHERE instance_id=$1 AND day=$2 AND lang=$3",
      [instanceId, day, lang]
    );
    if (cached.rows.length > 0) {
      const row = cached.rows[0];
      const cachedType = row.type as JourneyActionType;
      return { type: cachedType, phaseLabel: row.phase_label, content: row.content_json, completionLabel: COMPLETION_LABELS[cachedType]?.[lang] || COMPLETION_LABELS[cachedType]?.en || "Done" };
    }
  } catch (err: any) { console.error("[Journey] Cache read error:", err.message); }

  // 2. Resolve phase
  const template = JOURNEY_TEMPLATES[templateKey];
  if (!template) throw new Error(`Unknown journey template: ${templateKey}`);
  const phase = resolveJourneyPhase(template, day);
  const phaseLabel = phase.label[lang] || phase.label.en;

  // 2b. CHECK SCRIPT — hand-authored day-by-day content overrides hash-based selection
  if (template.script) {
    const entry = template.script.find(e => e.day === day);
    if (entry) {
      const localized = entry.content[lang] || entry.content.en;
      const scriptedAction: JourneyDailyAction = {
        type: entry.type,
        phaseLabel,
        content: localized,
        completionLabel: COMPLETION_LABELS[entry.type]?.[lang] || COMPLETION_LABELS[entry.type]?.en || "Done",
      };
      // Cache scripted content
      try {
        await pool.query(
          "INSERT INTO journey_daily_actions (instance_id, day, lang, type, phase_label, content_json) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (instance_id, day, lang) DO NOTHING",
          [instanceId, day, lang, entry.type, phaseLabel, JSON.stringify(localized)]
        );
      } catch (err: any) { console.error("[Journey] Script cache write error:", err.message); }
      return scriptedAction;
    }
  }

  // 3. Pick type deterministically (fallback for unscripted days)
  const hashKey = instanceId + ":" + day;
  const h = journeyHash(hashKey);
  const actionType = phase.mix[h % phase.mix.length];

  // 4. Dispatch by type
  let content: { title: string; body: string; scriptureRef?: string; prompt?: string };

  switch (actionType) {
    case "prayer": {
      const familyConfig = JOURNEY_FAMILIES_BY_KEY[family];
      const buckets = familyConfig?.buckets || ["general"];
      const bucket = buckets[day % buckets.length];
      // Reuse seededFallbackPrayer with a synthetic code derived from instance ID
      const syntheticCode = instanceId.substring(0, 8);
      const syntheticDate = String(day);
      const prayerText = seededFallbackPrayer(syntheticCode, syntheticDate, bucket, lang);
      let title = lang === "fr" ? "Pri\u00e8re du jour" : lang === "es" ? "Oraci\u00f3n del d\u00eda" : lang === "pt" ? "Ora\u00e7\u00e3o do dia" : "Today's prayer";
      let body = prayerText;
      if (prayedForName) {
        const forPrefix = lang === "fr" ? `Aujourd\u2019hui nous prions pour ${prayedForName}. ` : lang === "es" ? `Hoy oramos por ${prayedForName}. ` : lang === "pt" ? `Hoje oramos por ${prayedForName}. ` : `Today we pray for ${prayedForName}. `;
        body = forPrefix + body;
      }
      content = { title, body };
      break;
    }
    case "reflection": {
      const reflText = REFLECTION_TEMPLATES[phase.tone]?.[lang] || REFLECTION_TEMPLATES[phase.tone]?.en || REFLECTION_TEMPLATES.gentle.en;
      let title = lang === "fr" ? "R\u00e9flexion" : lang === "es" ? "Reflexi\u00f3n" : lang === "pt" ? "Reflex\u00e3o" : "Reflection";
      let body = reflText;
      if (prayedForName) {
        body = body + (lang === "fr" ? ` Porte ${prayedForName} dans ta pri\u00e8re.` : lang === "es" ? ` Lleva a ${prayedForName} en tu oraci\u00f3n.` : lang === "pt" ? ` Leve ${prayedForName} na sua ora\u00e7\u00e3o.` : ` Carry ${prayedForName} in your prayer.`);
      }
      content = { title, body, prompt: reflText };
      break;
    }
    case "meditation": {
      const medText = MEDITATION_TEMPLATES[phase.tone]?.[lang] || MEDITATION_TEMPLATES[phase.tone]?.en || MEDITATION_TEMPLATES.gentle.en;
      let title = lang === "fr" ? "M\u00e9ditation" : lang === "es" ? "Meditaci\u00f3n" : lang === "pt" ? "Medita\u00e7\u00e3o" : "Meditation";
      let body = medText;
      if (prayedForName) {
        body = body + (lang === "fr" ? ` Porte ${prayedForName} dans ta pri\u00e8re.` : lang === "es" ? ` Lleva a ${prayedForName} en tu oraci\u00f3n.` : lang === "pt" ? ` Leve ${prayedForName} na sua ora\u00e7\u00e3o.` : ` Carry ${prayedForName} in your prayer.`);
      }
      content = { title, body };
      break;
    }
    case "small_act": {
      const actsPool = SMALL_ACT_POOLS_BY_FAMILY[family] || [];
      if (actsPool.length === 0) {
        // Fallback to drawing_closer pool if family pool is empty
        const fallbackPool = SMALL_ACT_POOLS_BY_FAMILY.drawing_closer;
        const act = fallbackPool[h % fallbackPool.length];
        const actText = act[lang] || act.en;
        content = { title: lang === "fr" ? "Petit geste" : lang === "es" ? "Peque\u00f1o gesto" : lang === "pt" ? "Pequeno gesto" : "Small act", body: actText };
      } else {
        const act = actsPool[h % actsPool.length];
        const actText = act[lang] || act.en;
        content = { title: lang === "fr" ? "Petit geste" : lang === "es" ? "Peque\u00f1o gesto" : lang === "pt" ? "Pequeno gesto" : "Small act", body: actText };
      }
      break;
    }
    case "scripture": {
      // Tier-1 content matrix: prefer a verse chosen by the door's emotional core
      // {spine × phase × dominant-emotion}. en verified now; other langs fall back
      // to the localized tone pool until verified translations land. Legacy
      // instances (no door) also fall back → unchanged behavior.
      const phaseIndex = template.phases.indexOf(phase);
      const core = TIER1_ENABLED ? pickScriptureCore(instance.door, phaseIndex, instance.dominant_emotion) : null;
      // Core verses only for a language whose slots are ALL filled; else the
      // localized tone pool (which IS translated). Readiness is a live check.
      const byCore = (core && TIER1_SCRIPTURE_READY[lang]) ? scriptureForCore(core, lang) : null;
      const scrData = byCore || SCRIPTURE_TEMPLATES[phase.tone]?.[lang] || SCRIPTURE_TEMPLATES[phase.tone]?.en || SCRIPTURE_TEMPLATES.gentle.en;
      const scrTitle = lang === "fr" ? "Écriture" : lang === "es" ? "Escritura" : lang === "pt" ? "Escritura" : "Scripture";
      content = { title: scrTitle, body: scrData.body, scriptureRef: scrData.scriptureRef };
      break;
    }
    case "gratitude": {
      const gratText = GRATITUDE_TEMPLATES[phase.tone]?.[lang] || GRATITUDE_TEMPLATES[phase.tone]?.en || GRATITUDE_TEMPLATES.gentle.en;
      const gratTitle = lang === "fr" ? "Gratitude" : lang === "es" ? "Gratitud" : lang === "pt" ? "Gratidão" : "Gratitude";
      content = { title: gratTitle, body: gratText };
      break;
    }
    case "journal": {
      let jrnlText = JOURNAL_TEMPLATES[phase.tone]?.[lang] || JOURNAL_TEMPLATES[phase.tone]?.en || JOURNAL_TEMPLATES.gentle.en;
      // v5.20.15 — the "Write to them…" tender prompt gets relationship-specific
      // ONLY when a name was explicitly captured. No prayedForName ⇒ neutral wording.
      // Never infer who this journey is about.
      if (phase.tone === "tender" && prayedForName) {
        jrnlText = lang === "fr" ? `Écris à ${prayedForName}. Que veux-tu qu'ils sachent?`
          : lang === "es" ? `Escríbele a ${prayedForName}. ¿Qué quieres que sepan?`
          : lang === "pt" ? `Escreva para ${prayedForName}. O que você quer que saibam?`
          : `Write to ${prayedForName}. What do you want them to know?`;
      }
      const jrnlTitle = lang === "fr" ? "Journal" : lang === "es" ? "Diario" : lang === "pt" ? "Diário" : "Journal";
      content = { title: jrnlTitle, body: jrnlText, prompt: jrnlText };
      break;
    }
    case "rest": {
      const restText = REST_TEMPLATES[phase.tone]?.[lang] || REST_TEMPLATES[phase.tone]?.en || REST_TEMPLATES.gentle.en;
      const restTitle = lang === "fr" ? "Repos" : lang === "es" ? "Descanso" : lang === "pt" ? "Descanso" : "Rest";
      content = { title: restTitle, body: restText };
      break;
    }
    case "confession": {
      const confText = CONFESSION_TEMPLATES[phase.tone]?.[lang] || CONFESSION_TEMPLATES[phase.tone]?.en || CONFESSION_TEMPLATES.gentle.en;
      const confTitle = lang === "fr" ? "Confession" : lang === "es" ? "Confesión" : lang === "pt" ? "Confissão" : "Confession";
      content = { title: confTitle, body: confText, prompt: confText };
      break;
    }
    case "encouragement": {
      const encText = ENCOURAGEMENT_TEMPLATES[phase.tone]?.[lang] || ENCOURAGEMENT_TEMPLATES[phase.tone]?.en || ENCOURAGEMENT_TEMPLATES.gentle.en;
      const encTitle = lang === "fr" ? "Encouragement" : lang === "es" ? "Ánimo" : lang === "pt" ? "Encorajamento" : "Encouragement";
      content = { title: encTitle, body: encText };
      break;
    }
    default:
      content = { title: "Prayer", body: "Take a moment to pray." };
  }

  // 5. Cache — INSERT ON CONFLICT DO NOTHING for idempotency
  try {
    await pool.query(
      "INSERT INTO journey_daily_actions (instance_id, day, lang, type, phase_label, content_json) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (instance_id, day, lang) DO NOTHING",
      [instanceId, day, lang, actionType, phaseLabel, JSON.stringify(content)]
    );
  } catch (err: any) { console.error("[Journey] Cache write error:", err.message); }

  return { type: actionType, phaseLabel, content, completionLabel: COMPLETION_LABELS[actionType]?.[lang] || COMPLETION_LABELS[actionType]?.en || "Done" };
}

const PRAYER_LANG_NAMES: Record<string, string> = { en: "English", fr: "French", es: "Latin-American Spanish", pt: "Brazilian Portuguese" };

async function circleMemberLanguages(circle: StoredCircle): Promise<string[]> {
  // Generate only languages this circle's members actually use (+ en
  // canonical) — keeps Gemini quota sane (the original outage was quota).
  const ids = circle.members.map(m => m.userId).filter(Boolean);
  const langs = new Set<string>(["en"]);
  if (ids.length > 0) {
    try {
      const r = await pool.query("SELECT DISTINCT language FROM users WHERE id = ANY($1) AND language IS NOT NULL", [ids]);
      for (const row of r.rows) { if (["en","fr","es","pt"].includes(row.language)) langs.add(row.language); }
    } catch {}
  }
  return [...langs];
}

async function generateCircleDailyPrayers(): Promise<void> {
  const today = new Date().toISOString().split("T")[0];
  let generated = 0;
  let seeded = 0;
  // Gemini availability is checked per-pass; a 429 mid-loop downgrades the
  // REST of the pass to seeded fallbacks instead of aborting (the old `return`
  // left every remaining circle with no prayer for up to 6h of backoff).
  let geminiUp = !!GEMINI_API_KEY && isGeminiAvailable();
  for (const [code, circle] of circles) {
    if (circle.members.length === 0) continue;
    const wantedLangs = await circleMemberLanguages(circle);
    const topic = getCirclePrayerTopic(circle);
    for (const lang of wantedLangs) {
      const existing = await pool.query("SELECT 1 FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language=$3", [code, today, lang]);
      if (existing.rows.length > 0) continue;
      let prayer = "";
      if (geminiUp) {
        try {
          const res = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                system_instruction: { parts: [{ text: `You write short, heartfelt prayers (40-60 words) for prayer groups, in ${PRAYER_LANG_NAMES[lang] || "English"}. Address God formally in the liturgical register of that language. Write in second person addressing God. Never use dashes or hyphens as punctuation. Be warm, scriptural, personal. The prayer should feel like something a group would pray together. Return ONLY the prayer text in ${PRAYER_LANG_NAMES[lang] || "English"}, nothing else. No quotes around it.` }] },
                contents: [{ role: "user", parts: [{ text: `Write today's shared prayer for a prayer circle focused on: ${topic}. It should feel fresh and specific to today, not generic.` }] }]
              })
            }
          );
          if (res.ok) {
            const data = (await res.json()) as any;
            prayer = (data.candidates?.[0]?.content?.parts?.[0]?.text || "").trim();
            if (prayer.length <= 20) prayer = "";
          } else if (res.status === 429) {
            markGeminiRateLimited();
            geminiUp = false;
            console.log(`[CirclePrayer] Gemini 429 at ${code}/${lang} — seeding the rest of this pass`);
          } else {
            console.error(`[CirclePrayer] Gemini ${res.status} for ${code}/${lang}`);
          }
        } catch (err: any) {
          console.error(`[CirclePrayer] Gemini error for ${code}/${lang}: ${err.message}`);
        }
      }
      if (prayer) {
        generated++;
      } else {
        // Sacred ×4 seeded pools (v5-fallback-prayers-translations.md):
        // same daily slot resolves to the same prayer in every language.
        prayer = seededFallbackPrayer(code, today, topic, lang as Lang);
        seeded++;
      }
      await pool.query(
        "INSERT INTO circle_daily_prayers (circle_code, date, prayer_text, topic, language) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
        [code, today, prayer, topic, lang]
      );
    }
  }
  if (generated > 0 || seeded > 0) console.log(`[CirclePrayer] Daily prayers: ${generated} generated, ${seeded} seeded (fallback)`);
}

// ─── Plausible Pull ──────────────────────────────────────────────────
async function pullPlausibleMetrics(): Promise<void> {
  if (!PLAUSIBLE_API_KEY) { console.log("[Plausible] No key"); return; }
  try {
    const h = { Authorization: `Bearer ${PLAUSIBLE_API_KEY}` }; const base = "https://plausible.io/api/v1/stats";
    const ar = await fetch(`${base}/aggregate?site_id=${PLAUSIBLE_SITE_ID}&period=day&metrics=visitors,pageviews,bounce_rate,visit_duration`, { headers: h }); const ad = (await ar.json()) as any;
    let clicks = 0; try { const cr = await fetch(`${base}/aggregate?site_id=${PLAUSIBLE_SITE_ID}&period=day&metrics=events&filters=event:name==App%20Store%20Click`, { headers: h }); const cd = (await cr.json()) as any; clicks = cd?.results?.events?.value || 0; } catch {}
    let src: any[] = []; try { const sr = await fetch(`${base}/breakdown?site_id=${PLAUSIBLE_SITE_ID}&period=day&property=visit:source&limit=5`, { headers: h }); const sd = (await sr.json()) as any; src = sd?.results || []; } catch {}
    const today = new Date().toISOString().split("T")[0]; const m = ad?.results || {};
    await pool.query(`INSERT INTO daily_web_metrics (date,visitors,pageviews,bounce_rate,visit_duration_avg,app_store_clicks,top_sources,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (date) DO UPDATE SET visitors=$2,pageviews=$3,bounce_rate=$4,visit_duration_avg=$5,app_store_clicks=$6,top_sources=$7,updated_at=NOW()`, [today, m.visitors?.value||0, m.pageviews?.value||0, m.bounce_rate?.value||0, m.visit_duration?.value||0, clicks, JSON.stringify(src)]);
    console.log(`[Plausible] ${today}: ${m.visitors?.value||0} visitors, ${clicks} clicks`);
  } catch (err: any) { console.error("[Plausible]", err.message); }
}

async function backfillPlausible(): Promise<void> {
  if (!PLAUSIBLE_API_KEY) return;
  try {
    const h = { Authorization: `Bearer ${PLAUSIBLE_API_KEY}` }; const base = "https://plausible.io/api/v1/stats";
    const existing = await pool.query("SELECT COUNT(*) as c FROM daily_web_metrics");
    if (parseInt(existing.rows[0]?.c || "0") > 3) { console.log("[Plausible] Historical data exists, skipping backfill"); return; }
    const tsRes = await fetch(`${base}/timeseries?site_id=${PLAUSIBLE_SITE_ID}&period=30d&metrics=visitors,pageviews,bounce_rate,visit_duration`, { headers: h });
    if (!tsRes.ok) { console.error("[Plausible] Timeseries failed:", tsRes.status); return; }
    const tsData = (await tsRes.json()) as any; const days = tsData?.results || [];
    let clicksByDay: Record<string, number> = {};
    try { const clRes = await fetch(`${base}/timeseries?site_id=${PLAUSIBLE_SITE_ID}&period=30d&metrics=events&filters=event:name==App%20Store%20Click`, { headers: h }); if (clRes.ok) { const clData = (await clRes.json()) as any; for (const d of (clData?.results || [])) clicksByDay[d.date] = d.events || 0; } } catch {}
    let inserted = 0;
    for (const day of days) { if (!day.date || day.visitors === 0) continue; await pool.query(`INSERT INTO daily_web_metrics (date,visitors,pageviews,bounce_rate,visit_duration_avg,app_store_clicks,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date) DO UPDATE SET visitors=$2,pageviews=$3,bounce_rate=$4,visit_duration_avg=$5,app_store_clicks=$6,updated_at=NOW()`, [day.date, day.visitors||0, day.pageviews||0, day.bounce_rate||0, day.visit_duration||0, clicksByDay[day.date]||0]); inserted++; }
    console.log(`[Plausible] Backfilled ${inserted} days of historical data`);
  } catch (err: any) { console.error("[Plausible] Backfill error:", err.message); }
}

async function pullAppleSalesReport(): Promise<void> {
  const token = generateASCToken(); if (!token) return;
  try {
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split("T")[0];
    const vendorRes = await fetch("https://api.appstoreconnect.apple.com/v1/salesReports?filter[reportType]=SALES&filter[reportSubType]=SUMMARY&filter[frequency]=DAILY&filter[vendorNumber]=93967404&filter[reportDate]=" + yesterday, { headers: { Authorization: `Bearer ${token}`, Accept: "application/a-gzip" } });
    if (vendorRes.ok) {
      const buf = Buffer.from(await vendorRes.arrayBuffer());
      let text: string; try { text = gunzipSync(buf).toString("utf-8"); } catch { text = buf.toString("utf-8"); }
      console.log("[Apple Sales] Got report, length:", text.length);
      const lines = text.split("\n").filter(l => l.trim());
      if (lines.length > 1) { let firstTimeDownloads = 0; let totalProceeds = 0; for (let i = 1; i < lines.length; i++) { const cols = lines[i].split("\t"); const pt = (cols[6]||"").trim(); const u = parseInt(cols[7] || "0") || 0; totalProceeds += parseFloat(cols[8] || "0") || 0; if (pt==="1F"||pt==="F1"||pt==="FI1"||pt==="1"||pt==="3F"||pt==="3"||pt==="3T") firstTimeDownloads += u; }
        const dateStr = yesterday;
        await pool.query(`INSERT INTO daily_app_store_metrics (date,app_units,proceeds,updated_at) VALUES ($1,$2,$3,NOW()) ON CONFLICT (date) DO UPDATE SET app_units=$2,proceeds=$3,updated_at=NOW()`, [dateStr, firstTimeDownloads, totalProceeds]);
        console.log(`[Apple Sales] ${dateStr}: ${firstTimeDownloads} first-time downloads, $${totalProceeds} proceeds`); }
    } else { const errText = await vendorRes.text().catch(() => ""); console.log("[Apple Sales] Report not available:", vendorRes.status, errText.substring(0, 200)); }
  } catch (err: any) { console.error("[Apple Sales]", err.message); }
}

// ─── Hono App ────────────────────────────────────────────────────────
const app = new Hono();
app.use("*", cors());
// v5.8.4 — explicit no-cache on every API response so iOS URLSession doesn't
// serve stale GETs. Without this, iOS falls back to protocol-cache heuristics
// and can return cached responses for minutes, making cross-circle sync feel
// laggy even though the server has fresh data.
app.use("*", async (c, next) => {
  await next();
  if (c.req.path.startsWith("/api/")) {
    c.header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
    c.header("Pragma", "no-cache");
    c.header("Expires", "0");
  }
});
app.onError((err, c) => { console.error("Error:", err); return c.json({ error: "Internal error", detail: err.message }, 500); });

// TikTok domain verification
app.get("/tiktokvaEaRnqCoAhlOUYQ7qdLl6Hgh8JExbRL.txt", (c) => { c.header("Content-Type", "text/plain"); return c.text("tiktok-developers-site-verification=vaEaRnqCoAhlOUYQ7qdLl6Hgh8JExbRL"); });

// TikTok OAuth — Step 1: redirect user to TikTok authorization
app.get("/auth/tiktok", (c) => {
  const scopes = "user.info.basic,user.info.profile,user.info.stats,video.list";
  const state = randomUUID().substring(0, 16);
  const url = `https://www.tiktok.com/v2/auth/authorize/?client_key=${TIKTOK_CLIENT_KEY}&response_type=code&scope=${encodeURIComponent(scopes)}&redirect_uri=${encodeURIComponent(TIKTOK_REDIRECT_URI)}&state=${state}`;
  return c.redirect(url);
});

// TikTok OAuth — Step 2: receive code, exchange for token
app.get("/auth/tiktok/callback", async (c) => {
  const code = c.req.query("code") || "";
  const error = c.req.query("error") || "";
  if (error || !code) return c.html(`<h2>TikTok Auth Failed</h2><p>Error: ${error || "No code received"}</p><p><a href="/auth/tiktok">Try again</a></p>`);
  try {
    // Exchange code for access token
    const tokenRes = await fetch("https://open.tiktokapis.com/v2/oauth/token/", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded", "Cache-Control": "no-cache" },
      body: new URLSearchParams({ client_key: TIKTOK_CLIENT_KEY, client_secret: TIKTOK_CLIENT_SECRET, code, grant_type: "authorization_code", redirect_uri: TIKTOK_REDIRECT_URI })
    });
    const tokenText = await tokenRes.text();
    let tokenData: any;
    try { tokenData = JSON.parse(tokenText); } catch { return c.html(`<h2>Token Parse Error</h2><pre>${tokenText}</pre>`); }
    if (tokenData.error || !tokenData.access_token) {
      return c.html(`<h2>Token Exchange Failed</h2><pre>${JSON.stringify(tokenData, null, 2)}</pre><p><a href="/auth/tiktok">Try again</a></p>`);
    }
    // Fetch user info to confirm it works
    const userRes = await fetch("https://open.tiktokapis.com/v2/user/info/?fields=open_id,display_name,avatar_url,follower_count,following_count,likes_count,video_count", {
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const userData = (await userRes.json()) as any;
    return c.html(`
      <html><body style="font-family:system-ui;max-width:600px;margin:40px auto;padding:20px">
      <h2>TikTok Connected!</h2>
      <p><strong>User:</strong> ${userData.data?.user?.display_name || "Unknown"}</p>
      <p><strong>Followers:</strong> ${userData.data?.user?.follower_count || 0}</p>
      <p><strong>Videos:</strong> ${userData.data?.user?.video_count || 0}</p>
      <h3>Save these to Railway env vars:</h3>
      <p><strong>TIKTOK_ACCESS_TOKEN:</strong></p>
      <textarea style="width:100%;height:80px;font-size:12px">${tokenData.access_token}</textarea>
      <p><strong>TIKTOK_REFRESH_TOKEN:</strong></p>
      <textarea style="width:100%;height:80px;font-size:12px">${tokenData.refresh_token || "none"}</textarea>
      <p><strong>TIKTOK_OPEN_ID:</strong></p>
      <textarea style="width:100%;height:40px;font-size:12px">${tokenData.open_id || userData.data?.user?.open_id || ""}</textarea>
      <p style="color:gray;font-size:12px">Token expires in ${Math.round((tokenData.expires_in || 0) / 3600)} hours. Refresh token expires in ${Math.round((tokenData.refresh_expires_in || 0) / 86400)} days.</p>
      <h3>Scopes granted:</h3>
      <pre>${tokenData.scope || "unknown"}</pre>
      <h3>Raw response:</h3>
      <pre style="font-size:10px;background:#f5f5f5;padding:10px;overflow:auto">${JSON.stringify(tokenData, null, 2)}</pre>
      </body></html>
    `);
  } catch (err: any) { return c.html(`<h2>Error</h2><pre>${err.message}</pre><p><a href="/auth/tiktok">Try again</a></p>`); }
});

// Admin video upload for Buffer scheduling
app.post("/api/admin/upload-video", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  if (!s3) return c.json({ error: "Storage not configured" }, 500);
  try {
    const body = await c.req.parseBody();
    const file = body["file"] as any;
    if (!file || typeof file === "string") return c.json({ error: "No file provided. Send as multipart form with field name 'file'." }, 400);
    const arrayBuffer = await file.arrayBuffer();
    if (arrayBuffer.byteLength > 100 * 1024 * 1024) return c.json({ error: "File too large (100MB max)" }, 400);
    const fileName = file.name || `upload-${Date.now()}.mp4`;
    const key = `videos/${Date.now()}-${fileName}`;
    await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(arrayBuffer), ContentType: file.type || "video/mp4" }));
    const url = `${R2_PUBLIC_URL}/${key}`;
    return c.json({ url, key, size: arrayBuffer.byteLength });
  } catch (err: any) { return c.json({ error: "Internal error", detail: err.message }, 500); }
});

let p0PurgeReport: any = null; // v5.20.1 — raw counts from the one-time phase0proof purge
let normSelfTest: any = null;  // v5.20.2 — email normalization self-test result
let magicSelfTest: any = null; // v5.20.4 — magic-link round-trip self-test result
let mergeSelfTest: any = null; // v5.20.6 — recovery-merge E2E self-test result
let webFunnelSelfTest: any = null; // v5.21.0 — web funnel round-trip self-test
let webQuizV25SelfTest: any = null; // v5.23.0 — v2.5 full-token E2E (s_* path → purchase-sim → handoff)
let demoGrantProof: any = null;    // v5.22.0 — one-off RC demo-entitlement lifecycle proof
let mailProof: any = null;         // v5.26.1 — one-off Resend round-trip + conflict-alert proof
let worstDayPreview: any = null; // v5.20.13 — generated worst-day cards per door
let griefWhoFlags: any = null;   // v5.20.14 — grief who-assumption audit flags
let griefDay13Sample: any = null; // v5.20.15 — raw after-fix grief journal sample
// v5.20.14 — behind ADMIN_SECRET (no unauthenticated generation output in prod).
app.get("/journeys/worst-day-preview", (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (!process.env.ADMIN_SECRET || key !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  return c.json(worstDayPreview || { pending: true });
});
app.get("/", (c) => c.json({ status: "ok", service: "prAmen API", version: "5.26.1", p0_purge: p0PurgeReport, norm_selftest: normSelfTest, magic_selftest: magicSelfTest, merge_selftest: mergeSelfTest, web_funnel_selftest: webFunnelSelfTest, web_quiz_v25_selftest: webQuizV25SelfTest, scripture_clause_gate: scriptureClauseGate, demo_grant_proof: demoGrantProof, mail_proof: mailProof, tier1_scripture_ready: TIER1_SCRIPTURE_READY, denominator_policy: DENOMINATOR_POLICY, circles: circles.size, posthog: !!POSTHOG_API_KEY, posthog_read: !!POSTHOG_PERSONAL_KEY, plausible: !!PLAUSIBLE_API_KEY, apple: !!ASC_KEY_ID, revenuecat_api: !!REVENUECAT_SECRET_KEY, apns: !!APNS_KEY_ID, storage: !!R2_ACCOUNT_ID, admin: !!ADMIN_USER_ID, dashboard: "/dashboard?key=..." }));

// v5.6.0 — APNs payload now spreads `extra` fields (requestId, senderUserId, etc.) at top level so iOS can deep-link to specific request on tap.
// Prevents Dubai-vs-Paris disagreement when prayers cross the UTC day boundary.
function todayInTimezone(timezone: string): string {
  try {
    return new Date().toLocaleDateString("en-CA", { timeZone: timezone });
  } catch {
    return new Date().toISOString().split("T")[0];
  }
}
function prayedTodayInOwnTZ(member: { lastPrayedDate?: string | null; lastPrayedLocalDate?: string | null; lastPrayedTimezone?: string | null; }): boolean {
  if (member.lastPrayedLocalDate && member.lastPrayedTimezone) {
    return todayInTimezone(member.lastPrayedTimezone) === member.lastPrayedLocalDate;
  }
  if (!member.lastPrayedDate) return false;
  return Date.now() - new Date(member.lastPrayedDate).getTime() < 24 * 60 * 60 * 1000;
}
app.get("/api/circles/health", (c) => c.json({ status: "ok", circles: circles.size, sseClients: Array.from(sseClients.values()).reduce((sum, set) => sum + set.size, 0) }));

// v5.9.0 — Server-Sent Events endpoint for live cross-device sync.
// iOS opens a persistent connection when the app enters foreground.
// Server streams JSON events (circle_updated, keepalive) until the client disconnects.
// Every fan-out broadcasts here so clients refresh in ~100ms instead of waiting for polling.
app.get("/api/events", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Unauthorized" }, 401);
  const userId = u.device_user_id || u.id;
  return streamSSE(c, async (stream) => {
    const client: SseClient = {
      userId,
      send: async (event) => {
        await stream.writeSSE({ data: JSON.stringify(event), event: event.type });
      },
      close: () => { try { stream.abort(); } catch {} },
    };
    addSseClient(userId, client);
    // initial hello so client knows stream is live
    await stream.writeSSE({ data: JSON.stringify({ type: "connected", ts: Date.now() }), event: "connected" });
    // heartbeat every 25s to keep Railway / iOS from closing the idle connection
    const heartbeat = setInterval(() => {
      stream.writeSSE({ data: JSON.stringify({ type: "keepalive", ts: Date.now() }), event: "keepalive" }).catch(() => {});
    }, 25000);
    // keep the handler alive until the client disconnects
    await new Promise<void>((resolve) => {
      stream.onAbort(() => {
        clearInterval(heartbeat);
        removeSseClient(userId, client);
        resolve();
      });
    });
  });
});

// ─── Lightweight sync check — returns updated_at timestamps for user's circles ───
app.get("/api/circles/sync-check", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const userCircleCodes = getUserCircleCodes(u.id, u.device_user_id);
  if (userCircleCodes.length === 0) return c.json({ circles: [] });
  try {
    const result = await pool.query("SELECT code, updated_at FROM circles WHERE code = ANY($1)", [userCircleCodes]);
    const today = new Date().toISOString().split("T")[0];
    const circleStates = await Promise.all(result.rows.map(async (r: any) => {
      const ci = getCircle(r.code);
      const prayedToday = ci ? ci.members.filter(m => prayedTodayInOwnTZ(m)).length : 0;
      const engaged = await getCircleEngagementForDay(r.code, today);
      const activeCount = ci ? ci.members.filter(m => getMemberLastSeen(m).isActive).length : 0;
      const engagedActive = ci ? ci.members.filter(m => getMemberLastSeen(m).isActive && (engaged.has(m.userId) || prayedTodayInOwnTZ(m))).length : 0;
      const tier = computeCircleTier(engagedActive, activeCount);
      return { code: r.code, updatedAt: r.updated_at, prayedToday, totalMembers: ci?.members.length || 0, activeMembers: activeCount, engagedToday: engagedActive, tier };
    }));
    return c.json({ circles: circleStates, serverTime: new Date().toISOString() });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// ─── Push Diagnostics ────────────────────────────────────────────────
app.get("/api/dashboard/push-status", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const allUsers = await pool.query("SELECT id, name, device_token, device_token_updated_at FROM users ORDER BY created_at DESC LIMIT 50");
  const withTokens = allUsers.rows.filter((u: any) => u.device_token);
  const withoutTokens = allUsers.rows.filter((u: any) => !u.device_token);
  return c.json({
    apns_configured: !!APNS_KEY_ID && !!APNS_PRIVATE_KEY,
    apns_host: APNS_HOST,
    total_users: allUsers.rows.length,
    users_with_token: withTokens.length,
    users_without_token: withoutTokens.length,
    tokens: allUsers.rows.map((u: any) => ({
      id: u.id.substring(0, 12) + "…",
      name: u.name || "—",
      has_token: !!u.device_token,
      token_prefix: u.device_token ? u.device_token.substring(0, 16) + "…" : null,
      token_updated: u.device_token_updated_at || null
    }))
  });
});

// ═══════════════════════════════════════════════════════════════════
// ─── AUTH ────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/auth/apple", async (c) => {
  try {
    const body = await c.req.json(); const { appleUserId, email, fullName, identityToken, deviceUserId } = body;
    if (!appleUserId) return c.json({ error: "appleUserId required" }, 400);
    let user = await getUserByAppleId(appleUserId); let isNewUser = false;
    let oldDeviceUserId: string | null = null;
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      if (email && !user.email) { const ve = verifiedEmailFor("apple", email); await pool.query("UPDATE users SET email=$1,verified_email=COALESCE(verified_email,$2),updated_at=NOW() WHERE id=$3", [email, ve, user.id]); user.email = email; user.verified_email = ve; }
      oldDeviceUserId = user.device_user_id;
      if (deviceUserId && deviceUserId !== user.device_user_id) {
        if (user.device_user_id) await migrateCircleMembership(user.device_user_id, user.id, user.name || "");
        await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
      }
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || "";
      // v5.13.7 — new users start as 'none'. Real trial status comes from RevenueCat webhooks only.
      await pool.query(`INSERT INTO users (id,apple_user_id,email,verified_email,name,auth_token,device_user_id,subscription_status) VALUES ($1,$2,$3,$4,$5,$6,$7,'none')`, [userId, appleUserId, email||null, verifiedEmailFor("apple", email), userName, authToken, deviceUserId||null]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, apple_user_id: appleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: null, trial_end_date: null, subscription_status: "none" };
      trackEvent(userId, "user_signed_up", { auth_provider: "apple", has_email: !!email, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name || "");
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, avatarUrl: user.avatar_url || null, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.id, user.device_user_id || "", oldDeviceUserId || "") });
  } catch (e: any) { return c.json({ error: "Auth failed", detail: e.message }, 500); }
});

app.post("/api/auth/google", async (c) => {
  try {
    const body = await c.req.json(); const { googleUserId, email, fullName, idToken, deviceUserId } = body;
    if (!googleUserId || !email) return c.json({ error: "googleUserId and email required" }, 400);
    let verified = !idToken; if (idToken) { try { const tr = await fetch(`https://oauth2.googleapis.com/tokeninfo?id_token=${idToken}`); if (tr.ok) { const td = (await tr.json()) as any; if (td.sub === googleUserId) verified = true; } } catch {} }
    let user = await getUserByGoogleId(googleUserId); let isNewUser = false;
    if (!user) { const ex = await getUserByEmail(email); if (ex) { await pool.query("UPDATE users SET google_user_id=$1,verified_email=COALESCE(verified_email,$2),auth_provider=CASE WHEN auth_provider='apple' THEN 'apple+google' ELSE 'google' END,updated_at=NOW() WHERE id=$3", [googleUserId, verifiedEmailFor("google", email), ex.id]); user = ex; } }
    let oldDeviceUserIdG: string | null = null;
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      oldDeviceUserIdG = user.device_user_id;
      if (deviceUserId && deviceUserId !== user.device_user_id) {
        if (user.device_user_id) await migrateCircleMembership(user.device_user_id, user.id, user.name || "");
        await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
      }
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || email.split("@")[0];
      // v5.13.7 — new users start as 'none'. Real trial status comes from RevenueCat webhooks only.
      await pool.query(`INSERT INTO users (id,google_user_id,email,verified_email,name,auth_provider,auth_token,device_user_id,subscription_status) VALUES ($1,$2,$3,$4,$5,'google',$6,$7,'none')`, [userId, googleUserId, email, verifiedEmailFor("google", email), userName, authToken, deviceUserId||null]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, google_user_id: googleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: null, trial_end_date: null, subscription_status: "none" };
      trackEvent(userId, "user_signed_up", { auth_provider: "google", has_email: true, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name || "");
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, avatarUrl: user.avatar_url || null, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.id, user.device_user_id || "", oldDeviceUserIdG || "") });
  } catch (e: any) { return c.json({ error: "Auth failed", detail: e.message }, 500); }
});

// Admin: migrate circle membership from old device ID to server user ID
app.post("/api/admin/migrate-circles", async (c) => {
  if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const { oldUserId, newUserId, userName } = await c.req.json();
  if (!oldUserId || !newUserId) return c.json({ error: "oldUserId and newUserId required" }, 400);
  await migrateCircleMembership(oldUserId, newUserId, userName || "");
  const codes = getUserCircleCodes(newUserId);
  return c.json({ success: true, migratedCircleCodes: codes });
});

app.put("/api/user/email-opt-in", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { optIn } = await c.req.json(); await pool.query("UPDATE users SET email_opt_in=$1,updated_at=NOW() WHERE id=$2", [!!optIn, u.id]); if (optIn) trackEvent(u.id, "email_opt_in", { email: u.email }); return c.json({ success: true }); });
app.get("/api/admin/user-lookup", async (c) => { const key = c.req.query("key") || c.req.header("X-Admin-Secret"); if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403); const q = c.req.query("q") || ""; if (!q) return c.json({ error: "?q= required (email, userId, or deviceUserId)" }, 400); const r = await pool.query("SELECT id,name,email,auth_provider,created_at,subscription_status,trial_start_date,trial_end_date,device_user_id,avatar_url FROM users WHERE id=$1 OR lower(trim(email))=lower(trim($1)) OR device_user_id=$1", [q]); if (r.rows.length === 0) return c.json({ error: "User not found" }, 404); return c.json({ user: r.rows[0] }); });

// v5.20.3 — TEMPORARY: duplicate-email collision list, reviewed before the
// v5.26.0 — Resend mail proofs (ADMIN_SECRET). (a) round-trip: send a test email,
// return the raw Resend id/status/error. ?from= overrides sender to test the
// onboarding domain before pramen.app verifies. ?to= defaults to founder email.
app.get("/admin/mail-proof", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (!process.env.ADMIN_SECRET || key !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const to = c.req.query("to") || MERGE_ALERT_EMAIL;
  const from = c.req.query("from") || undefined; // e.g. "onboarding@resend.dev" pre-verification
  const r = await sendMail({ to, from, subject: "prAmen mail round-trip proof", html: "<p>prAmen Resend round-trip. If you can read this, the API + domain are live.</p>", text: "prAmen Resend round-trip." });
  return c.json({ ranAt: new Date().toISOString(), mail_from: from || MAIL_FROM, to, mail_configured: mailConfigured(), resend: r });
});

// v5.26.0 — merge-conflict founder-alert proof (item 2d). Fires ONE synthetic
// conflict alert (no DB row; pure alert path) from MAIL_FROM → MERGE_ALERT_EMAIL.
app.get("/admin/merge-conflict-test", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (!process.env.ADMIN_SECRET || key !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const reason = "SYNTHETIC_TEST (dual-entitlement) — proof only, no real conflict";
  const r = await sendMail({ to: MERGE_ALERT_EMAIL, subject: `prAmen merge conflict (${reason})`, html: `<p>${reason}</p><p>This is a synthetic founder-alert proof. No accounts were merged.</p>`, text: `${reason} — synthetic proof.` });
  return c.json({ ranAt: new Date().toISOString(), alert_to: MERGE_ALERT_EMAIL, mail_from: MAIL_FROM, resend: r });
});

// UNIQUE constraint lands, then this endpoint is removed. Key = ADMIN_SECRET
// (env). Minimal fields only: normalized email, count, per-account uuid /
// created_at / auth_provider. No names, no journey data, no tokens.
app.get("/admin/email-collisions", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (!process.env.ADMIN_SECRET || key !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  try {
    const rows = (await pool.query(`
      SELECT lower(trim(email)) AS norm_email, count(*)::int AS count,
             json_agg(json_build_object('uuid', id, 'created_at', created_at, 'auth_provider', auth_provider) ORDER BY created_at) AS accounts
        FROM users
       WHERE email IS NOT NULL AND trim(email) <> ''
         AND lower(trim(email)) NOT LIKE '%@privaterelay.appleid.com'
       GROUP BY lower(trim(email))
      HAVING count(*) > 1
       ORDER BY count(*) DESC, norm_email
    `)).rows;
    return c.json({
      collision_groups: rows.length,
      total_accounts_involved: rows.reduce((s: number, r: any) => s + r.count, 0),
      collisions: rows,
      note: "grouped by lower(trim(email)); Apple relay excluded; UNIQUE not yet applied",
    });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});
app.get("/api/admin/user-deep", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403);
  const q = c.req.query("q") || "";
  if (!q) return c.json({ error: "?q= required" }, 400);
  const user = await pool.query("SELECT * FROM users WHERE id=$1 OR lower(trim(email))=lower(trim($1)) OR device_user_id=$1", [q]);
  if (!user.rows[0]) return c.json({ error: "User not found" }, 404);
  const u = user.rows[0];
  const referralCode = await pool.query("SELECT * FROM referral_codes WHERE user_id=$1", [u.id]);
  const referrals = await pool.query("SELECT * FROM referrals WHERE referrer_user_id=$1 OR referred_user_id=$1 ORDER BY created_at", [u.id]);
  const inviteTokens = await pool.query("SELECT * FROM invite_tokens WHERE inviter_user_id=$1 ORDER BY created_at", [u.id]);
  const inviteEmails = await pool.query("SELECT * FROM invite_emails WHERE referrer_user_id=$1 ORDER BY created_at", [u.id]);
  const circles = getUserCircleCodes(u.id, u.device_user_id);
  const userData = await getUserData(u.id);
  const promoCodes = await pool.query("SELECT * FROM promo_codes WHERE redeemed_by_user_id=$1", [u.id]);
  return c.json({ user: u, referral_code: referralCode.rows[0] || null, referrals: referrals.rows, invite_tokens: inviteTokens.rows, invite_emails: inviteEmails.rows, circles, user_data: userData, promo_codes: promoCodes.rows });
});
app.get("/api/admin/recent-users", async (c) => { const key = c.req.query("key") || c.req.header("X-Admin-Secret"); if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403); const days = parseInt(c.req.query("days") || "3"); const r = await pool.query("SELECT id, name, email, auth_provider, created_at, subscription_status, trial_start_date, trial_end_date, device_user_id, device_token IS NOT NULL as has_push_token FROM users WHERE created_at > NOW() - INTERVAL '1 day' * $1 ORDER BY created_at DESC", [days]); return c.json({ count: r.rows.length, users: r.rows }); });
// v5.15.5 — One-time nudge for billing zombie users (lapsed trials with push tokens)
app.post("/api/admin/nudge-billing-zombies", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403);
  try {
    // Find users created in the last 20 days with status none/expired/cancelled who have push tokens
    const zombies = await pool.query(`
      SELECT u.id, u.name, u.email, u.subscription_status, u.created_at
      FROM users u
      WHERE u.device_token IS NOT NULL
      AND u.subscription_status IN ('none', 'expired', 'cancelled', 'billing_issue')
      AND u.created_at > NOW() - INTERVAL '20 days'
      ORDER BY u.created_at DESC
    `);
    if (zombies.rows.length === 0) return c.json({ sent: 0, message: "No zombie users found" });
    let sent = 0;
    const results: any[] = [];
    for (const user of zombies.rows) {
      const throttleKey = `billing_zombie_nudge_${user.id}`;
      const existing = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
      if (existing.rows.length > 0) { results.push({ id: user.id, name: user.name, status: "already_sent" }); continue; }
      // Check if user is in any circle
      const inCircle = Array.from(circles.values()).some(ci => ci.members.some(m => m.userId === user.id));
      const title = inCircle ? "Your Prayer Circle misses you" : "Your prayer journey is waiting";
      const body = inCircle
        ? "Your circle is still praying together. Come back and join them. Your free trial is ready when you are."
        : "You started something beautiful. Open prAmen and pray with your community today. Your free trial is ready.";
      await pushToUser(user.id, { title, body, type: "billing_issue" });
      await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);
      sent++;
      trackEvent(user.id, "billing_zombie_nudge_sent", { subscription_status: user.subscription_status, in_circle: inCircle });
      results.push({ id: user.id, name: user.name, email: user.email, status: "sent", in_circle: inCircle });
    }
    return c.json({ sent, total_candidates: zombies.rows.length, results });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});
app.get("/api/admin/email-list", async (c) => { if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const r = await pool.query("SELECT email,name,auth_provider,created_at FROM users WHERE email_opt_in=true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"); return c.json({ count: r.rows.length, emails: r.rows }); });
app.post("/api/auth/verify", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ valid: false }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ valid: false }, 401); return c.json({ valid: true, user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id) }); });
app.post("/api/auth/logout", async (c) => { const ah = c.req.header("Authorization"); if (!ah) return c.json({ success: true }); await pool.query("UPDATE users SET auth_token=$1,updated_at=NOW() WHERE auth_token=$2", [generateAuthToken(), ah.replace("Bearer ", "")]); return c.json({ success: true }); });

// v5.20.4 — Magic-link "Continue with Email". Deployed DARK (endpoints live but
// no UI path until the iOS flag + RESEND_API_KEY land). Single-use, 10-min TTL,
// token hashed at rest, constant-time verify, rate-limited per email + per IP.
app.post("/api/auth/magic-link/request", async (c) => {
  try {
    const { email } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || !norm.includes("@")) return c.json({ error: "Valid email required" }, 400);
    const ip = (c.req.header("x-forwarded-for") || "").split(",")[0].trim() || c.req.header("x-real-ip") || "unknown";
    if (!rateOk(magicRate.byEmail, norm, 3, MAGIC_TTL_MS)) return c.json({ error: "Too many requests. Try again in a few minutes." }, 429);
    if (!rateOk(magicRate.byIp, ip, 10, MAGIC_TTL_MS)) return c.json({ error: "Too many requests. Try again in a few minutes." }, 429);
    const { raw, code } = await issueMagicLink(norm, ip);
    const link = `https://pramen.app/auth/magic?token=${raw}&email=${encodeURIComponent(norm)}`;
    const mail = await sendMail({ to: norm, subject: "Your prAmen sign-in link", html: magicEmailHtml(link, code), text: `Sign in to prAmen: ${link}\nOr enter this code in the app: ${code}\nThis works once and expires in 10 minutes.` });
    // Anti-enumeration: always generic success regardless of whether the user exists.
    return c.json({ ok: true, delivered: mail.ok, mailConfigured: mailConfigured() });
  } catch { return c.json({ error: "Request failed" }, 500); }
});
app.post("/api/auth/magic-link/verify", async (c) => {
  try {
    const { email, token, code, deviceUserId } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || (!token && !code)) return c.json({ error: "email and (token or code) required" }, 400);
    // Brute-force guard on the 6-digit code: 5 verify attempts / email / 10 min.
    if (!rateOk(magicVerifyRate, norm, 5, MAGIC_TTL_MS)) return c.json({ error: "Too many attempts. Request a new link." }, 429);
    const r = await consumeMagicToken(norm, token ? String(token) : null, code ? String(code) : null, deviceUserId);
    if (r.error || !r.user) return c.json({ error: "Invalid or expired link" }, 401);
    const u = r.user;
    // Skip-questionnaire handoff: if this email quizzed on the web, hand the app
    // the answers so it pre-builds the journey instead of re-asking.
    const wq = (await pool.query("SELECT answers, quiet_time, door, status FROM web_quiz WHERE email=$1", [norm])).rows[0] || null;
    // v5.24.0 — derive dominant_emotion + opening tone at handoff (APPROVED #4).
    const wqa: any = wq?.answers || {};
    const derived = wq ? { ...deriveDominantEmotion(wqa.pathKey, wqa.mirrorAnswers), journey_opening_tone: journeyOpeningTone(wqa.faithIdx), journey_name: wqa.pathKey ? (FUNNEL_JOURNEY_NAMES[wqa.pathKey] || null) : null } : null;
    const prebuiltIntake = wq ? { answers: wq.answers, quietTime: wq.quiet_time, door: wq.door, status: wq.status, derived } : null;
    return c.json({ user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null, isNewUser: r.isNewUser }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id || ""), prebuiltIntake });
  } catch (err: any) { return c.json({ error: "Verify failed", detail: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// v5.20.6 — RECOVERY-MERGE ("Already purchased on our website?"). Dark: the
// iOS paywall entry is flag-gated; the server merge runs only after proven
// email control. Implements the approved decision table exactly.
// ═══════════════════════════════════════════════════════════════════
const MERGE_ALERT_EMAIL = process.env.MERGE_ALERT_EMAIL || "sbambino23@gmail.com";
function hasEntitlement(u: any): boolean {
  const s = (u?.subscription_status || "").toLowerCase();
  return !!s && !["none", "expired", "cancelled", "canceled", "unknown", ""].includes(s);
}
async function hasJourney(userId: string): Promise<boolean> {
  const r = await pool.query("SELECT 1 FROM journey_instances WHERE user_id=$1 AND status='active' LIMIT 1", [userId]);
  return r.rows.length > 0;
}
function posthogAlias(loserId: string, survivorId: string): void {
  if (!POSTHOG_API_KEY) return;
  fetch("https://us.i.posthog.com/capture/", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ api_key: POSTHOG_API_KEY, event: "$create_alias", distinct_id: survivorId, properties: { alias: loserId } }),
  }).catch(() => {});
}
// v5.22.0 — DEMO allowlist (temporary; replaces magic-link until Resend is live).
// Server-side only; NEVER hardcode emails in the binary. Empty env = inert.
// REMOVAL item on the go-live checklist once magic-link ships.
const DEMO_ALLOWLIST: Set<string> = new Set((process.env.DEMO_ALLOWLIST || "").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean));
function isDemoAllowlisted(normEmail: string): boolean { return DEMO_ALLOWLIST.size > 0 && DEMO_ALLOWLIST.has(normEmail); }
// Demo entitlement = RC promotional (monthly), same proven grant/revoke machinery.
async function grantDemoEntitlement(rcUserId: string): Promise<boolean> {
  if (rcUserId.startsWith("demograntproof-")) return false; // proof handles RC directly
  if (!REVENUECAT_SECRET_KEY) return false;
  try {
    const r = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(rcUserId)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) });
    return r.ok;
  } catch { return false; }
}
async function grantMergeGrace(rcUserId: string): Promise<boolean> {
  await pool.query("UPDATE users SET grace_until = now() + interval '7 days', updated_at=NOW() WHERE id=$1", [rcUserId]).catch(() => {});
  if (rcUserId.startsWith("mergeselftest-")) return false; // self-test: DB grace only, never touch RC
  if (!REVENUECAT_SECRET_KEY) return false;
  try {
    const r = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(rcUserId)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "weekly" }) });
    return r.ok;
  } catch { return false; }
}
// Atomic transfer loser → survivor; idempotent (tombstone check); single-homes email.
async function mergeAccounts(loserId: string, survivorId: string, email: string): Promise<{ merged: boolean; alreadyMerged?: boolean }> {
  if (loserId === survivorId) return { merged: false };
  const l = (await pool.query("SELECT account_status FROM users WHERE id=$1", [loserId])).rows[0];
  if (l?.account_status === "merged") return { merged: false, alreadyMerged: true };
  await pool.query("UPDATE journey_instances SET user_id=$1 WHERE user_id=$2", [survivorId, loserId]);
  await pool.query("UPDATE partner_requests SET from_user=$1 WHERE from_user=$2", [survivorId, loserId]).catch(() => {});
  await pool.query("UPDATE partner_requests SET to_user=$1 WHERE to_user=$2", [survivorId, loserId]).catch(() => {});
  await pool.query("UPDATE partner_blocks SET blocker=$1 WHERE blocker=$2", [survivorId, loserId]).catch(() => {});
  await pool.query("UPDATE partner_blocks SET blocked=$1 WHERE blocked=$2", [survivorId, loserId]).catch(() => {});
  const ld = (await pool.query("SELECT * FROM user_data WHERE user_id=$1", [loserId])).rows[0];
  if (ld) {
    await pool.query("INSERT INTO user_data (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", [survivorId]);
    await pool.query(`UPDATE user_data SET streak_count=GREATEST(streak_count,$1), highest_streak=GREATEST(highest_streak,$2), total_prayers=total_prayers+$3, total_minutes=total_minutes+$4, updated_at=NOW() WHERE user_id=$5`, [ld.streak_count || 0, ld.highest_streak || 0, ld.total_prayers || 0, ld.total_minutes || 0, survivorId]);
    await pool.query("DELETE FROM user_data WHERE user_id=$1", [loserId]);
  }
  for (const [, ci] of circles) {
    let changed = false;
    for (const m of ci.members) if (m.userId === loserId) { m.userId = survivorId; changed = true; }
    const seen = new Set<string>(); ci.members = ci.members.filter((m: any) => { if (seen.has(m.userId)) return false; seen.add(m.userId); return true; });
    if (changed) await saveCircleToDb(ci);
  }
  await pool.query("UPDATE web_quiz SET user_id=$1, updated_at=now() WHERE user_id=$2", [survivorId, loserId]).catch(() => {}); // transfer item 5 (web quiz / pending_intake)
  posthogAlias(loserId, survivorId);
  await pool.query("UPDATE users SET account_status='merged', merged_into=$1, verified_email=NULL, grace_until=NULL, auth_token=$2, updated_at=NOW() WHERE id=$3", [survivorId, generateAuthToken(), loserId]);
  await pool.query("UPDATE users SET verified_email=$1, account_status='active', updated_at=NOW() WHERE id=$2", [email, survivorId]);
  // A tombstoned account must never retain entitlement: revoke any grace promo on the loser.
  // The survivor keeps its own entitlement — we never grant the survivor here, so no double-grant.
  if (REVENUECAT_SECRET_KEY && !loserId.startsWith("mergeselftest-")) {
    fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(loserId)}/entitlements/premium/revoke_promotionals`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" } }).catch(() => {});
  }
  return { merged: true };
}
async function reportConflict(A: any, B: any, reason: string, states: any): Promise<any> {
  const existing = (await pool.query("SELECT id FROM merge_conflicts WHERE ((uuid_a=$1 AND uuid_b=$2) OR (uuid_a=$2 AND uuid_b=$1)) AND status='open' LIMIT 1", [A.id, B.id])).rows[0];
  let conflictId = existing?.id;
  if (!conflictId) {
    conflictId = (await pool.query("INSERT INTO merge_conflicts (uuid_a, uuid_b, reason, data_states, status) VALUES ($1,$2,$3,$4,'open') RETURNING id", [A.id, B.id, reason, JSON.stringify(states)])).rows[0].id;
    if (mailConfigured()) sendMail({ to: MERGE_ALERT_EMAIL, subject: `prAmen merge conflict (${reason})`, html: `<p>${reason}</p><p>A=${A.id}<br>B=${B.id}</p><p>${JSON.stringify(states)}</p>`, text: `${reason}: A=${A.id} B=${B.id} ${JSON.stringify(states)}` }).catch(() => {});
  }
  // Grace never locks out a proven payer: if the verified-email account is entitled, grant B.
  let graceGranted = false;
  if (hasEntitlement(A)) graceGranted = await grantMergeGrace(B.id);
  return { action: "report", reason, conflictId, graceGranted, accessImmediate: graceGranted || hasEntitlement(B) };
}
// The decision table, encoded.
async function resolveRecovery(normEmail: string, bUserId: string): Promise<any> {
  const A = (await pool.query("SELECT * FROM users WHERE lower(trim(verified_email))=lower(trim($1)) AND COALESCE(account_status,'active')<>'merged' ORDER BY created_at LIMIT 1", [normEmail])).rows[0] || null;
  const B = (await pool.query("SELECT * FROM users WHERE id=$1", [bUserId])).rows[0];
  if (!B) return { error: "no_session" };
  if (!A) { await pool.query("UPDATE users SET verified_email=$1, updated_at=NOW() WHERE id=$2", [normEmail, bUserId]); return { action: "set_email", survivor: bUserId }; } // P1
  if (A.id === B.id) return { action: "noop", survivor: B.id }; // P2
  const aEnt = hasEntitlement(A), bEnt = hasEntitlement(B);
  const aJrny = await hasJourney(A.id), bJrny = await hasJourney(B.id);
  if (aEnt && bEnt) return await reportConflict(A, B, "dual_entitlement", { aEnt, bEnt, aJrny, bJrny }); // step 3
  if (aJrny && bJrny) return await reportConflict(A, B, "dual_journey", { aEnt, bEnt, aJrny, bJrny }); // step 4
  const survivor = aEnt ? A.id : (bEnt ? B.id : B.id); // step 5: entitled one, else B
  const loser = survivor === A.id ? B.id : A.id;
  const res = await mergeAccounts(loser, survivor, normEmail);
  return { action: "merge", survivor, loser, ...res };
}
// Recovery endpoint: prove email control (no account fabricated), then resolve.
app.post("/api/auth/recover/verify", async (c) => {
  try {
    const ah = c.req.header("Authorization");
    const B = ah?.startsWith("Bearer ") ? await getUserByToken(ah.replace("Bearer ", "")) : null;
    if (!B) return c.json({ error: "auth required" }, 401);
    const { email, token, code } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || (!token && !code)) return c.json({ error: "email and (token or code) required" }, 400);
    if (!rateOk(magicVerifyRate, norm, 5, MAGIC_TTL_MS)) return c.json({ error: "Too many attempts. Request a new link." }, 429);
    const cred = await consumeMagicToken(norm, token ? String(token) : null, code ? String(code) : null, null, false);
    if (cred.error || !cred.verified) return c.json({ error: "Invalid or expired link" }, 401);
    const outcome = await resolveRecovery(norm, B.id);
    let survivor: any = outcome.survivor;
    if (survivor && (outcome.action === "merge" || outcome.action === "set_email" || outcome.action === "noop")) {
      const s = (await pool.query("SELECT id, auth_token FROM users WHERE id=$1", [survivor])).rows[0];
      if (s) survivor = { userId: s.id, authToken: s.auth_token }; // app re-fires RC logIn on this id
    }
    return c.json({ ...outcome, survivor });
  } catch (err: any) { return c.json({ error: "Recovery failed", detail: err.message }, 500); }
});
// v5.20.6 — TEMPORARY conflict queue (same ADMIN_SECRET pattern, minimal fields).
app.get("/admin/merge-conflicts", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (!process.env.ADMIN_SECRET || key !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const rows = (await pool.query("SELECT id, uuid_a, uuid_b, reason, data_states, status, created_at, resolved_at FROM merge_conflicts ORDER BY created_at DESC LIMIT 200")).rows;
  return c.json({ open: rows.filter((r: any) => r.status === "open").length, total: rows.length, conflicts: rows });
});

// ═══════════════════════════════════════════════════════════════════
// v5.21.0 — PHASE 3 WEB FUNNEL (pramen.app /quiz → checkout → app).
// Dark: endpoints live; go-live gated on Resend + RC Web Billing + AASA.
// ═══════════════════════════════════════════════════════════════════
const WEB_ORIGIN = process.env.WEB_ORIGIN || "https://pramen.app";
// v5.22.2 — remote-configurable web quiz URL (gate-screen "Start my journey").
// Env-driven, never hardcoded in the app: set WEB_QUIZ_URL to the preview URL
// for the demo; becomes https://pramen.app/quiz at launch (the default).
const WEB_QUIZ_URL = process.env.WEB_QUIZ_URL || "https://pramen.app/quiz";
// v5.24.0 — §E phased launch. Q1 on real traffic renders only these doors; each
// funnel pathKey is added the day its arc ships. Launch-5 first (existing
// templates). Overridable via DOORS_LIVE env (comma-sep) without a deploy.
const DOORS_LIVE = (process.env.DOORS_LIVE || "health,grief,child,caregiver,addiction").split(",").map((s) => s.trim()).filter(Boolean);
// Lightweight client config (the iOS gate reads webQuizUrl; the quiz reads doorsLive).
app.get("/api/config", (c) => c.json({ webQuizUrl: WEB_QUIZ_URL, doorsLive: DOORS_LIVE }));
// Ensure a lightweight pending user exists for a web email (so the RC identity,
// magic-link, and app sign-in all resolve to ONE account). Returns the userId.
async function ensureWebUser(normEmail: string, firstName?: string | null): Promise<string> {
  const existing = await getUserByEmail(normEmail);
  if (existing) {
    if (firstName && !existing.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [firstName, existing.id]);
    if (!existing.verified_email) await pool.query("UPDATE users SET verified_email=$1,updated_at=NOW() WHERE id=$2", [normEmail, existing.id]);
    return existing.id;
  }
  const userId = randomUUID();
  await pool.query("INSERT INTO users (id,email,verified_email,name,auth_provider,auth_token,subscription_status) VALUES ($1,$2,$2,$3,'web',$4,'none')", [userId, normEmail, firstName || "", generateAuthToken()]);
  await pool.query("INSERT INTO user_data (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", [userId]);
  trackEvent(userId, "user_signed_up", { auth_provider: "web" });
  return userId;
}

// 1. EMAIL CAPTURE (Lead). Creates the pending user + the web_quiz record.
app.post("/api/web/lead", async (c) => {
  try {
    const { email, firstName, answers, quietTime, door } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || !norm.includes("@")) return c.json({ error: "valid_email_required" }, 400);
    const name = typeof firstName === "string" ? firstName.trim().slice(0, 80) : null;
    const qt = (typeof quietTime === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(quietTime.trim())) ? quietTime.trim() : null;
    const userId = await ensureWebUser(norm, name);
    await pool.query(
      `INSERT INTO web_quiz (email, user_id, first_name, answers, quiet_time, door, status)
       VALUES ($1,$2,$3,COALESCE($4::jsonb,'{}'::jsonb),$5,$6,'lead')
       ON CONFLICT (email) DO UPDATE SET user_id=EXCLUDED.user_id, first_name=COALESCE(EXCLUDED.first_name, web_quiz.first_name),
         answers=CASE WHEN $4 IS NULL THEN web_quiz.answers ELSE web_quiz.answers || $4::jsonb END,
         quiet_time=COALESCE(EXCLUDED.quiet_time, web_quiz.quiet_time), door=COALESCE(EXCLUDED.door, web_quiz.door), updated_at=now()`,
      [norm, userId, name, answers ? JSON.stringify(answers) : null, qt, typeof door === "string" ? door : null]
    );
    const eventId = `lead_${userId}_${Date.now()}`;
    trackEvent(userId, "web_lead_captured", { has_answers: !!answers });
    sendMetaCAPIEvent({ eventName: "Lead", eventId, userId, email: norm, price: 0, currency: "USD" }).catch(() => {});
    return c.json({ ok: true, userId, rcAppUserId: rcAppUserIdForEmail(norm), eventId });
  } catch (err: any) { return c.json({ error: "lead_failed", detail: err.message }, 500); }
});

// 2. QUIZ ANSWERS (full set or incremental; merged into answers JSONB).
app.post("/api/web/quiz", async (c) => {
  try {
    const { email, answers, quietTime, door, complete } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || !norm.includes("@")) return c.json({ error: "valid_email_required" }, 400);
    if (!answers || typeof answers !== "object") return c.json({ error: "answers_object_required" }, 400);
    const qt = (typeof quietTime === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(quietTime.trim())) ? quietTime.trim() : null;
    const userId = await ensureWebUser(norm, null);
    const status = complete ? "quiz_complete" : "lead";
    await pool.query(
      `INSERT INTO web_quiz (email, user_id, answers, quiet_time, door, status)
       VALUES ($1,$2,$3::jsonb,$4,$5,$6)
       ON CONFLICT (email) DO UPDATE SET user_id=EXCLUDED.user_id,
         answers = web_quiz.answers || $3::jsonb,
         quiet_time=COALESCE(EXCLUDED.quiet_time, web_quiz.quiet_time),
         door=COALESCE(EXCLUDED.door, web_quiz.door),
         status=CASE WHEN $6='quiz_complete' THEN 'quiz_complete' ELSE web_quiz.status END, updated_at=now()`,
      [norm, userId, JSON.stringify(answers), qt, typeof door === "string" ? door : null, status]
    );
    if (complete) trackEvent(userId, "web_quiz_completed", {});
    return c.json({ ok: true, userId });
  } catch (err: any) { return c.json({ error: "quiz_failed", detail: err.message }, 500); }
});

// 3. PURCHASE COMPLETE (called by the RC-success redirect). Issues a magic link
// for the app handoff + fires the server-side Purchase event. Entitlement itself
// is granted by RC Web Billing → RC webhook (single source of truth).
app.post("/api/web/purchase-complete", async (c) => {
  try {
    const { email, value, currency } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || !norm.includes("@")) return c.json({ error: "valid_email_required" }, 400);
    const userId = await ensureWebUser(norm, null);
    await pool.query("UPDATE web_quiz SET status='purchased', updated_at=now() WHERE email=$1", [norm]);
    const ip = (c.req.header("x-forwarded-for") || "").split(",")[0].trim() || "web";
    const { raw, code } = await issueMagicLink(norm, ip);
    const link = `${WEB_ORIGIN}/auth/magic?token=${raw}&email=${encodeURIComponent(norm)}`;
    const mail = await sendMail({ to: norm, subject: "Open prAmen — you're all set", html: magicEmailHtml(link, code), text: `Open prAmen: ${link}\nOr enter code ${code}. Works once, expires in 10 minutes.` });
    const eventId = `purchase_${userId}_${Date.now()}`;
    trackEvent(userId, "web_purchase", { value: value ?? null, currency: currency ?? "USD" });
    sendMetaCAPIEvent({ eventName: "Purchase", eventId, userId, email: norm, price: typeof value === "number" ? value : 23.99, currency: currency || "USD" }).catch(() => {});
    // Magic link + code returned for the desktop/no-app fallback page (never auto-verified).
    return c.json({ ok: true, delivered: mail.ok, mailConfigured: mailConfigured(), magicLink: link, eventId });
  } catch (err: any) { return c.json({ error: "purchase_complete_failed", detail: err.message }, 500); }
});

// v5.22.0 — DEMO SIGN-IN (allowlist). TEMPORARY — replaced by magic-link at
// go-live (REMOVAL item). Allowlisted email → user + demo entitlement + auth.
// Non-allowlisted → { allowlisted:false } (client shows "coming soon", not a
// dead "check your email"). Empty DEMO_ALLOWLIST env = inert (all false).
app.post("/api/auth/demo-signin", async (c) => {
  try {
    const { email, deviceUserId } = await c.req.json();
    const norm = normalizeEmail(email);
    if (!norm || !norm.includes("@")) return c.json({ error: "valid_email_required" }, 400);
    if (!isDemoAllowlisted(norm)) {
      console.log(`[demo-signin] NOT allowlisted: ${norm}`);
      return c.json({ allowlisted: false });
    }
    const userId = await ensureWebUser(norm, null);
    if (deviceUserId) await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, userId]).catch(() => {});
    const demoGranted = await grantDemoEntitlement(userId); // RC promo (monthly) → app logs in with userId
    const u = (await pool.query("SELECT * FROM users WHERE id=$1", [userId])).rows[0];
    console.log(`[demo-signin] ALLOWLISTED auth: ${norm} → user ${userId.substring(0, 8)} rc_granted=${demoGranted}`);
    trackEvent(userId, "demo_signin", { rc_granted: demoGranted });
    return c.json({ allowlisted: true, demoGranted, user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null, isNewUser: false }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id || "") });
  } catch (err: any) { return c.json({ error: "demo_signin_failed", detail: err.message }, 500); }
});
app.delete("/api/auth/account", async (c) => {
  const ah = c.req.header("Authorization");
  if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const u = await getUserByToken(ah.replace("Bearer ", ""));
  if (!u) return c.json({ error: "Not found" }, 404);
  // Remove user from ALL circles before deleting account
  for (const [code, ci] of circles.entries()) {
    const memberIdx = ci.members.findIndex(m => m.userId === u.id);
    if (memberIdx !== -1) {
      ci.members.splice(memberIdx, 1);
      await saveCircleToDb(ci);
    }
  }
  await pool.query("DELETE FROM user_data WHERE user_id=$1", [u.id]);
  await pool.query("DELETE FROM users WHERE id=$1", [u.id]);
  trackEvent(u.id, "account_deleted", {});
  return c.json({ success: true });
});

// v5.13.1 — admin push notification endpoint
app.post("/api/admin/push", async (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const { userIds, title, body } = await c.req.json();
  if (!userIds?.length || !title || !body) return c.json({ error: "userIds, title, body required" }, 400);
  let sent = 0;
  for (const uid of userIds) {
    try { await pushToUser(uid, { title, body, type: "admin_push" }); sent++; } catch {}
  }
  return c.json({ success: true, sent, total: userIds.length });
});

// ═══════════════════════════════════════════════════════════════════
// ─── DATA SYNC + DEVICE TOKEN ───────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); return c.json({ user: { id: u.id, name: u.name, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id) }); });
app.put("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const b = await c.req.json(); try { await pool.query(`INSERT INTO user_data (user_id,streak_count,highest_streak,total_prayers,total_minutes,last_prayed_date,last_prayed_local_date,last_prayed_timezone,sessions,preferences,circle_codes,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW()) ON CONFLICT (user_id) DO UPDATE SET streak_count=$2,highest_streak=GREATEST(user_data.highest_streak,$3),total_prayers=$4,total_minutes=$5,last_prayed_date=$6,last_prayed_local_date=COALESCE($7,user_data.last_prayed_local_date),last_prayed_timezone=COALESCE($8,user_data.last_prayed_timezone),sessions=$9,preferences=$10,circle_codes=$11,updated_at=NOW()`, [u.id, b.streakCount||0, b.highestStreak||0, b.totalPrayers||0, b.totalMinutes||0, b.lastPrayedDate||null, b.lastPrayedLocalDate||null, b.lastPrayedTimezone||null, JSON.stringify(b.sessions||[]), JSON.stringify(b.preferences||{}), b.circleCodes||[]]); if (b.userName && b.userName !== u.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [b.userName, u.id]); return c.json({ status: "ok", synced: true }); } catch (e: any) { return c.json({ error: "Sync failed", detail: e.message }, 500); } });
app.put("/api/user/name", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { name } = await c.req.json(); if (!name?.trim()) return c.json({ error: "Name required" }, 400); await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [name.trim(), u.id]); for (const [, ci] of circles) { const m = ci.members.find(m => m.userId === u.id || m.userId === u.device_user_id); if (m) { m.name = name.trim(); await saveCircleToDb(ci); } } return c.json({ success: true, name: name.trim() }); });
app.put("/api/user/device-token", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { deviceToken } = await c.req.json(); if (!deviceToken) return c.json({ error: "deviceToken required" }, 400); await pool.query("UPDATE users SET device_token=$1, device_token_updated_at=NOW(), updated_at=NOW() WHERE id=$2", [deviceToken, u.id]); console.log(`[Token] Stored device token for ${u.id.substring(0,8)}… token=${deviceToken.substring(0,12)}…`); return c.json({ success: true }); });
// v5.9.1 — user language sync so server-side push notifications arrive in the right language
app.put("/api/user/language", async (c) => {
  const ah = c.req.header("Authorization");
  if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const u = await getUserByToken(ah.replace("Bearer ", ""));
  if (!u) return c.json({ error: "Unauthorized" }, 401);
  const { language } = await c.req.json();
  const raw = (language || "").toString().toLowerCase();
  const allowed: Lang[] = ["en", "fr", "es", "pt"];
  const lang: Lang = (allowed as string[]).includes(raw) ? (raw as Lang) : "en";
  await pool.query("UPDATE users SET language=$1, updated_at=NOW() WHERE id=$2", [lang, u.id]);
  console.log(`[Lang] Set language=${lang} for user ${u.id.substring(0,8)}…`);
  return c.json({ success: true, language: lang });
});

// v5.15.6 — iOS sends its RC customer ID after successful Purchases.shared.logIn()
app.put("/api/user/rc-id", async (c) => {
  const ah = c.req.header("Authorization");
  if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const u = await getUserByToken(ah.replace("Bearer ", ""));
  if (!u) return c.json({ error: "Unauthorized" }, 401);
  const { rcCustomerId } = await c.req.json();
  if (!rcCustomerId) return c.json({ error: "rcCustomerId required" }, 400);
  await pool.query("UPDATE users SET rc_customer_id=$1, updated_at=NOW() WHERE id=$2", [rcCustomerId, u.id]);
  console.log(`[RC-ID] Linked rc_customer_id=${rcCustomerId.substring(0,12)}… to user ${u.id.substring(0,8)}…`);
  return c.json({ success: true });
});

app.put("/api/user/avatar", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  if (!s3) return c.json({ error: "Storage not configured" }, 500);
  const body = await c.req.parseBody();
  const file = body.avatar as File | undefined;
  if (!file || file.size === 0) return c.json({ error: "No image provided" }, 400);
  if (file.size > 5 * 1024 * 1024) return c.json({ error: "Image too large. Maximum 5 MB." }, 413);
  const allowed = ["image/jpeg", "image/png", "image/heic", "image/heif"];
  if (!allowed.includes(file.type)) return c.json({ error: "Unsupported format. Use JPEG or PNG." }, 422);
  try {
    const ext = file.name.split(".").pop() || "jpg";
    const key = `avatars/${u.id}.${ext}`;
    await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(await file.arrayBuffer()), ContentType: file.type }));
    const avatarUrl = `${R2_PUBLIC_URL}/${key}`;
    await pool.query("UPDATE users SET avatar_url=$1, updated_at=NOW() WHERE id=$2", [avatarUrl, u.id]);
    for (const [, ci] of circles) {
      const m = ci.members.find(m => m.userId === u.id);
      if (m) { m.avatarUrl = avatarUrl; await saveCircleToDb(ci); }
    }
    trackEvent(u.id, "avatar_updated", {});
    return c.json({ avatarUrl });
  } catch (err: any) { return c.json({ error: "Upload failed. Try again." }, 500); }
});

app.get("/api/user/avatar", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  return c.json({ avatarUrl: u.avatar_url || null });
});

// ─── Social Proof (for paywall) ──────────────────────────────────
app.get("/api/stats/social-proof", async (c) => {
  // v5.13.6 — public endpoint for landing page social proof (no auth required, CORS enabled)
  c.header("Access-Control-Allow-Origin", "*");
  try {
    const totalPrayers = await pool.query("SELECT COALESCE(SUM(total_prayers),0) as total FROM user_data").catch(() => ({ rows: [{ total: 0 }] }));
    const totalUsers = await pool.query("SELECT COUNT(*) as count FROM users").catch(() => ({ rows: [{ count: 0 }] }));
    const activeLast7d = await pool.query("SELECT COUNT(*) as count FROM user_data WHERE last_prayed_date >= CURRENT_DATE - INTERVAL '7 days'").catch(() => ({ rows: [{ count: 0 }] }));
    const prayersThisWeek = await pool.query("SELECT COALESCE(SUM(total_prayers),0) as total FROM user_data WHERE last_prayed_date >= CURRENT_DATE - INTERVAL '7 days'").catch(() => ({ rows: [{ total: 0 }] }));
    return c.json({
      totalPrayers: parseInt(totalPrayers.rows[0]?.total || 0),
      totalUsers: parseInt(totalUsers.rows[0]?.count || 0),
      activeThisWeek: parseInt(activeLast7d.rows[0]?.count || 0),
      prayersThisWeek: parseInt(prayersThisWeek.rows[0]?.total || 0),
      activeCircles: circles.size,
      languages: 4
    });
  } catch { return c.json({ totalPrayers: 0, totalUsers: 0, activeThisWeek: 0, prayersThisWeek: 0, activeCircles: 0, languages: 4 }); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── EVENT CAPTURE ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/v1/events/capture", async (c) => { try { const { event, distinct_id, properties } = await c.req.json(); if (!event || !distinct_id) return c.json({ error: "Missing fields" }, 400); trackEvent(distinct_id, event, { ...properties, app_version: c.req.header("X-App-Version") || "unknown" }); return c.json({ status: "ok" }); } catch { return c.json({ error: "Error" }, 500); } });
app.post("/api/v1/events/capture/batch", async (c) => { try { const { events: el } = await c.req.json(); if (!Array.isArray(el) || !el.length) return c.json({ error: "Empty" }, 400); if (el.length > 50) return c.json({ error: "Max 50" }, 400); for (const e of el) trackEvent(e.distinct_id, e.event, e.properties); return c.json({ status: "ok", count: el.length }); } catch { return c.json({ error: "Error" }, 500); } });
app.post("/api/v1/events/identify", async (c) => { try { const { distinct_id, properties } = await c.req.json(); if (!distinct_id) return c.json({ error: "Missing" }, 400); identifyUser(distinct_id, { language: properties?.language, subscription_status: properties?.subscription_status, subscription_plan: properties?.subscription_plan, circle_count: properties?.circle_count, total_prayers: properties?.total_prayers, current_streak: properties?.current_streak }); return c.json({ status: "ok" }); } catch { return c.json({ error: "Error" }, 500); } });

// ═══════════════════════════════════════════════════════════════════
// ─── REVENUECAT WEBHOOK ─────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
const RC_MAP: Record<string,string> = { INITIAL_PURCHASE: "subscription_started", RENEWAL: "subscription_renewed", CANCELLATION: "subscription_cancelled", UNCANCELLATION: "subscription_reactivated", EXPIRATION: "subscription_expired", BILLING_ISSUE: "billing_issue_detected", PRODUCT_CHANGE: "subscription_plan_changed", NON_RENEWING_PURCHASE: "lifetime_purchased" };
function rcPlan(pid: string) { if (pid.includes("monthly")) return "monthly"; if (pid.includes("yearly")) return "yearly"; if (pid.includes("lifetime")) return "lifetime"; return "unknown"; }
function rcStatus(t: string) { if (["INITIAL_PURCHASE","RENEWAL","UNCANCELLATION"].includes(t)) return "active"; if (t==="CANCELLATION") return "cancelled"; if (t==="EXPIRATION") return "expired"; if (t==="BILLING_ISSUE") return "billing_issue"; if (t==="NON_RENEWING_PURCHASE") return "lifetime"; return "unknown"; }

// ═══════════════════════════════════════════════════════════════════
// ─── META CAPI (Conversions API for iOS attribution) ────────────
// ═══════════════════════════════════════════════════════════════════
const META_CAPI_ACCESS_TOKEN = process.env.META_CAPI_ACCESS_TOKEN || "";
const META_PIXEL_ID = process.env.META_PIXEL_ID || "";
const META_CAPI_ENDPOINT = process.env.META_CAPI_ENDPOINT || "";

function sha256Hash(value: string): string {
  return createHash("sha256").update(value.trim().toLowerCase()).digest("hex");
}

function mapRcEventToMeta(rcEventType: string, periodType: string): string | null {
  if (rcEventType === "INITIAL_PURCHASE" && periodType === "TRIAL") return "StartTrial";
  if (rcEventType === "INITIAL_PURCHASE") return "Subscribe";
  if (rcEventType === "RENEWAL") return "Subscribe";
  if (rcEventType === "NON_RENEWING_PURCHASE") return "Purchase";
  return null;
}

async function sendMetaCAPIEvent(params: {
  eventName: string;
  userId: string;
  email?: string | null;
  price: number;
  currency: string;
  eventId: string;
}): Promise<void> {
  if (!META_CAPI_ACCESS_TOKEN || !META_CAPI_ENDPOINT || !META_PIXEL_ID) {
    console.log("[Meta CAPI] Not configured, skipping");
    return;
  }
  try {
    const userData: Record<string, any> = {
      external_id: [sha256Hash(params.userId)],
    };
    if (params.email) {
      userData.em = [sha256Hash(params.email)];
    }
    const payload = {
      data: [{
        event_name: params.eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id: params.eventId,
        action_source: "website",
        event_source_url: "https://pramen.app",
        user_data: userData,
        custom_data: {
          currency: params.currency || "USD",
          value: params.price || 0,
        },
      }],
    };
    const url = `${META_CAPI_ENDPOINT}?access_token=${META_CAPI_ACCESS_TOKEN}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      const data = (await res.json()) as any;
      console.log(`[Meta CAPI] ✓ ${params.eventName} sent. events_received=${data.events_received || 0} event_id=${params.eventId.substring(0,12)}`);
    } else {
      const errText = await res.text().catch(() => "");
      console.error(`[Meta CAPI] ✗ Failed status=${res.status} body=${errText.substring(0, 300)}`);
    }
  } catch (err: any) {
    console.error("[Meta CAPI] Send error:", err.message);
  }
}

app.post("/webhooks/revenuecat", async (c) => {
  try {
    const ah = c.req.header("Authorization"); const sec = process.env.REVENUECAT_WEBHOOK_SECRET;
    if (sec && ah !== `Bearer ${sec}`) return c.json({ error: "Unauthorized" }, 401);
    const body = await c.req.json(); const ev = body.event; if (!ev?.type) return c.json({ error: "Invalid" }, 400);
    const name = RC_MAP[ev.type]; if (!name) return c.json({ status: "skipped" });
    const rcUid = ev.app_user_id; const pid = ev.product_id || ""; const plan = rcPlan(pid); const status = rcStatus(ev.type);
    let resolvedUid = rcUid; const candidateIds = [rcUid, ev.original_app_user_id, ...(ev.aliases || [])].filter(Boolean);
    for (const candidateId of candidateIds) { if (!candidateId || candidateId.startsWith("$RCAnonymous")) continue; try { const match = await pool.query("SELECT id FROM users WHERE id=$1 OR device_user_id=$1 LIMIT 1", [candidateId]); if (match.rows.length > 0) { resolvedUid = match.rows[0].id; break; } } catch {} }
    if (resolvedUid.startsWith("$RCAnonymous")) { try { const match = await pool.query("SELECT id FROM users WHERE device_user_id=$1 LIMIT 1", [rcUid]); if (match.rows.length > 0) resolvedUid = match.rows[0].id; } catch {} }
    // v5.15.6 — resolve via rc_customer_id column (set by iOS after successful RC login)
    if (resolvedUid.startsWith("$RCAnonymous")) { for (const cid of candidateIds) { if (!cid) continue; try { const match = await pool.query("SELECT id FROM users WHERE rc_customer_id=$1 LIMIT 1", [cid]); if (match.rows.length > 0) { resolvedUid = match.rows[0].id; break; } } catch {} } }
    await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, resolvedUid]).catch(() => {});
    if (resolvedUid !== rcUid) await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, rcUid]).catch(() => {});
    // v5.15.5 — sync circle visibility on subscription changes
    if (["active", "lifetime"].includes(status)) {
      // Subscribe → make visible in all circles
      for (const [, circle] of circles) {
        const member = circle.members.find(m => m.userId === resolvedUid || m.userId === rcUid);
        if (member && !member.visible) { member.visible = true; saveCircleToDb(circle).catch(() => {}); }
      }
    } else if (["cancelled", "expired"].includes(status)) {
      // Cancel/expire → hide in community circles only (private circle members stay visible)
      for (const [, circle] of circles) {
        if (!isCommunityCircle(circle.code)) continue;
        const member = circle.members.find(m => m.userId === resolvedUid || m.userId === rcUid);
        if (member && member.visible !== false) { member.visible = false; saveCircleToDb(circle).catch(() => {}); }
      }
    }
    trackEvent(resolvedUid, name, { plan, product_id: pid, price: ev.price, currency: ev.currency, store: ev.store, environment: ev.environment, rc_original_id: rcUid, $revenue: ev.price || 0, $currency: ev.currency || "USD" });
    // Meta CAPI: server-side event for iOS ad attribution.
    // Skips sandbox/test environments to avoid polluting ad optimization.
    if (ev.environment === "PRODUCTION" || !ev.environment) {
      const metaEventName = mapRcEventToMeta(ev.type, ev.period_type || "NORMAL");
      if (metaEventName) {
        let userEmail: string | null = null;
        try {
          const userRow = await pool.query("SELECT email FROM users WHERE id=$1 LIMIT 1", [resolvedUid]);
          userEmail = userRow.rows[0]?.email || null;
          if (userEmail && userEmail.includes("privaterelay.appleid.com")) userEmail = null;
        } catch {}
        sendMetaCAPIEvent({
          eventName: metaEventName,
          userId: resolvedUid,
          email: userEmail,
          price: ev.price || 0,
          currency: ev.currency || "USD",
          eventId: ev.id || `${ev.type}_${resolvedUid}_${Date.now()}`,
        }).catch((err) => console.error("[Meta CAPI] Async error:", err.message));
      }
    }
    identifyUser(resolvedUid, { subscription_status: status, subscription_plan: plan, last_revenue_event: name });
    try { const price = ev.price || 0; const net = price * (1 - APPLE_CUT); const today = new Date().toISOString().split("T")[0];
      await pool.query(`INSERT INTO revenue_events (user_id,event_type,plan,product_id,price,currency,environment) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [resolvedUid, name, plan, pid, price, ev.currency||"USD", ev.environment||"production"]);
      if (name === "subscription_started" || name === "lifetime_purchased") await pool.query(`INSERT INTO daily_revenue (date,new_subscribers,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET new_subscribers=daily_revenue.new_subscribers+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_renewed") await pool.query(`INSERT INTO daily_revenue (date,renewals,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET renewals=daily_revenue.renewals+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_cancelled" || name === "subscription_expired") await pool.query(`INSERT INTO daily_revenue (date,cancellations) VALUES ($1,1) ON CONFLICT (date) DO UPDATE SET cancellations=daily_revenue.cancellations+1,updated_at=NOW()`, [today]);
    } catch (e: any) { console.error("[Revenue]", e.message); }
    if (name === "subscription_started" || name === "lifetime_purchased") {
      try {
        const ref = await pool.query("UPDATE referrals SET status='confirmed', confirmed_at=NOW() WHERE referred_user_id=$1 AND status='pending' RETURNING referrer_user_id", [resolvedUid]);
        if (ref.rows[0]) {
          const referrerId = ref.rows[0].referrer_user_id;
          if (REVENUECAT_SECRET_KEY) {
            try {
              const r1 = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(referrerId)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) });
              if (r1.ok) console.log(`[Referral] Granted 30d to referrer ${referrerId.substring(0, 8)}`);
              else console.error(`[Referral] Grant referrer failed: ${r1.status} ${await r1.text().catch(() => "")}`);
            } catch (err: any) { console.error("[Referral] Grant referrer error:", err.message); }
            try {
              const r2 = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(resolvedUid)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) });
              if (r2.ok) console.log(`[Referral] Granted 30d to referred ${resolvedUid.substring(0, 8)}`);
              else console.error(`[Referral] Grant referred failed: ${r2.status} ${await r2.text().catch(() => "")}`);
            } catch (err: any) { console.error("[Referral] Grant referred error:", err.message); }
          }
          pushToUserLocalized(referrerId, { titleKey: "referral_both_title", bodyKey: "referral_both_body", type: "referral_confirmed" });
          pushToUserLocalized(resolvedUid, { titleKey: "referral_reward_title", bodyKey: "referral_reward_body", type: "referral_reward" });
          trackEvent(referrerId, "referral_30d_granted", { referred_user_id: resolvedUid });
          trackEvent(resolvedUid, "referral_30d_granted_referred", { referrer_user_id: referrerId });
        }
      } catch {}
    }
    // v5.15.3 — push notification on billing issue to recover failed payments
    if (ev.type === "BILLING_ISSUE" && !resolvedUid.startsWith("$RCAnonymous")) {
      pushToUserLocalized(resolvedUid, { titleKey: "billing_issue_title", bodyKey: "billing_issue_body", type: "billing_issue" }).catch(() => {});
    }
    console.log(`[RC] ${ev.type} → ${name} | rc:${rcUid.substring(0,12)} → resolved:${resolvedUid.substring(0,12)} ${plan}`); return c.json({ status: "ok" });
  } catch (e) { console.error("[RC]", e); return c.json({ error: "Error" }, 500); }
});

// ─── LOOPS WEBHOOK — email events → PostHog ─────────────────────
app.post("/webhooks/loops", async (c) => {
  try {
    const rawBody = await c.req.text();

    // Verify webhook signature if secret is configured
    if (LOOPS_WEBHOOK_SECRET) {
      const webhookId = c.req.header("webhook-id") || "";
      const timestamp = c.req.header("webhook-timestamp") || "";
      const signature = c.req.header("webhook-signature") || "";
      if (!webhookId || !timestamp || !signature) return c.json({ error: "Missing webhook headers" }, 401);
      const now = Math.floor(Date.now() / 1000);
      if (Math.abs(now - parseInt(timestamp, 10)) > 300) return c.json({ error: "Timestamp too old" }, 401);
      const secretBytes = Buffer.from(LOOPS_WEBHOOK_SECRET.replace(/^whsec_/, ""), "base64");
      const signedContent = `${webhookId}.${timestamp}.${rawBody}`;
      const expected = createHmac("sha256", secretBytes).update(signedContent).digest("base64");
      const providedSig = signature.split(" ").find(s => s.startsWith("v1,"));
      const providedHash = providedSig ? providedSig.slice(3) : "";
      if (!providedHash || !Buffer.from(expected).equals(Buffer.from(providedHash))) return c.json({ error: "Invalid signature" }, 401);
    }

    const body = JSON.parse(rawBody);
    const event = body.eventName;
    if (!event) return c.json({ status: "ignored" });

    // Only track email engagement events
    const trackableEvents: Record<string, string> = {
      "email.delivered": "loops_email_delivered",
      "email.opened": "loops_email_opened",
      "email.clicked": "loops_email_clicked",
      "email.bounced": "loops_email_bounced",
      "email.softBounced": "loops_email_soft_bounced",
      "email.hardBounced": "loops_email_hard_bounced",
      "loop.email.sent": "loops_email_sent",
      "campaign.email.sent": "loops_email_sent",
    };

    const phEvent = trackableEvents[event];
    if (!phEvent) return c.json({ status: "ignored", event });

    // Resolve user — prefer userId from contactIdentity, fall back to email lookup
    const identity = body.contactIdentity || body.contact || {};
    let userId = identity.userId || null;
    const email = identity.email || "";

    if (!userId && email) {
      const res = await pool.query("SELECT id FROM users WHERE lower(trim(email)) = lower(trim($1)) LIMIT 1", [email]);
      if (res.rows.length > 0) userId = res.rows[0].id;
    }

    if (!userId) {
      console.log(`[Loops] No user found for ${email || "unknown"} — skipping`);
      return c.json({ status: "skipped" });
    }

    const emailInfo = body.email || {};
    trackEvent(userId, phEvent, {
      email_subject: emailInfo.subject || "",
      email_id: emailInfo.id || emailInfo.emailMessageId || "",
      source_type: body.sourceType || "",
      contact_email: email,
    });

    console.log(`[Loops] ${phEvent} → ${userId.substring(0, 12)} | ${emailInfo.subject || "no subject"}`);
    return c.json({ status: "ok" });
  } catch (e: any) { console.error("[Loops webhook]", e.message); return c.json({ error: "Error" }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLES ────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/circles", async (c) => { const b = await c.req.json(); if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const code = generateCircleCode(); const ci: StoredCircle = { id: randomUUID(), name: b.name || "Prayer Circle", code, emoji: b.emoji || "cross.fill", creatorUserId: b.userId, members: [{ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString(), role: "creator" }], prayerRequests: [], createdAt: new Date().toISOString() }; await saveCircleToDb(ci); trackEvent(b.userId, "circle_created", { circle_id: ci.id, circle_code: code, circle_name: ci.name }); return c.json({ circle: ci }, 201); });
app.get("/api/circles/:code", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); try { const memberIds = ci.members.map(m => m.userId).filter(Boolean); if (memberIds.length > 0) { const avatars = await pool.query("SELECT id, device_user_id, avatar_url, name FROM users WHERE id = ANY($1) OR device_user_id = ANY($1)", [memberIds]); const avatarMap: Record<string, { avatar_url: string | null; name: string }> = {}; for (const row of avatars.rows) { avatarMap[row.id] = { avatar_url: row.avatar_url, name: row.name }; if (row.device_user_id) avatarMap[row.device_user_id] = { avatar_url: row.avatar_url, name: row.name }; }
  // v5.15.6 — community circles: hide non-subscribers AND inactive 14+ days. Private circles show everyone.
  const visibleMembers = isCommunityCircle(ci.code) ? ci.members.filter(m => (m.visible !== false || m.userId === ci.creatorUserId) && (isCommunityMemberActive(m) || m.userId === ci.creatorUserId)) : ci.members;
  const enriched = { ...ci, members: visibleMembers.map(m => ({ ...m, avatarUrl: avatarMap[m.userId]?.avatar_url || m.avatarUrl || null, name: avatarMap[m.userId]?.name || m.name })) }; return c.json({ circle: enriched }); } } catch (err: any) { console.error("[Circle] Avatar enrich error:", err.message); } return c.json({ circle: ci }); });
const joinNotifCooldowns = new Map<string, number>(); // creatorId:code -> last notif timestamp
app.post("/api/circles/:code/join", async (c) => { const code = c.req.param("code").toUpperCase(); let b; try { b = await c.req.json(); } catch { return c.json({ error: "Invalid body" }, 400); } if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); if (ci.members.find(m => m.userId === b.userId)) return c.json({ circle: ci }); const community = isCommunityCircle(code); let memberVisible = true; if (community) { const userRow = await pool.query("SELECT subscription_status FROM users WHERE id=$1", [b.userId]).then(r => r.rows[0]).catch(() => null); const hasAccess = userRow && userRow.subscription_status && userRow.subscription_status !== "none"; memberVisible = !!hasAccess; } ci.members.push({ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString(), visible: memberVisible }); await saveCircleToDb(ci); trackEvent(b.userId, "circle_invite_accepted", { circle_code: code, circle_size: ci.members.length, source: b.source || "unknown" }); if (b.source === "code_entry") { trackEvent(b.userId, "invite_code_entered", { circle_code: code }); trackEvent(b.userId, "invite_accepted", { circle_code: code, via: "code" }); } else if (b.source === "link") { trackEvent(b.userId, "invite_accepted", { circle_code: code, via: "link" }); } trackEvent(ci.creatorUserId, "circle_member_joined", { circle_code: code, circle_size: ci.members.length, new_member_name: b.userName }); if (!community) { const cooldownKey = `${ci.creatorUserId}:${code}`; const lastNotif = joinNotifCooldowns.get(cooldownKey) || 0; const now = Date.now(); if (now - lastNotif > 5 * 60 * 1000) { joinNotifCooldowns.set(cooldownKey, now); pushToUserLocalized(ci.creatorUserId, { titleKey: "member_joined_title", titleParams: { name: b.userName || "Someone", circle: ci.name }, bodyKey: "member_joined_body", bodyParams: { count: ci.members.length }, type: "member_joined", circleCode: code, circleName: ci.name }); } } return c.json({ circle: ci }); });
app.put("/api/circles/:code", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); if (b.name) ci.name = b.name; if (b.emoji) ci.emoji = b.emoji; await saveCircleToDb(ci); return c.json({ circle: ci }); });

// C4: Circle avatar upload
app.put("/api/circles/:code/avatar", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code);
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isCircleAdmin(u.id, ci, u.device_user_id)) return c.json({ error: "Only admins can change the circle picture." }, 403);
  if (!s3) return c.json({ error: "Storage not configured" }, 500);
  const body = await c.req.parseBody();
  const file = body.avatar as File | undefined;
  if (!file || file.size === 0) return c.json({ error: "No image provided" }, 400);
  if (file.size > 5 * 1024 * 1024) return c.json({ error: "Image too large. Maximum 5 MB." }, 413);
  const allowed = ["image/jpeg", "image/png", "image/heic", "image/heif"];
  if (!allowed.includes(file.type)) return c.json({ error: "Unsupported format. Use JPEG or PNG." }, 422);
  try {
    const ext = file.name.split(".").pop() || "jpg";
    const key = `circles/${code}.${ext}`;
    await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(await file.arrayBuffer()), ContentType: file.type }));
    const avatarUrl = `${R2_PUBLIC_URL}/${key}`;
    ci.avatarUrl = avatarUrl;
    await saveCircleToDb(ci);
    trackEvent(u.id, "circle_avatar_updated", { circle_code: code });
    return c.json({ avatarUrl });
  } catch (err: any) { return c.json({ error: "Upload failed. Try again." }, 500); }
});
app.put("/api/circles/:code/members/:userId/status", async (c) => {
  const ci = getCircle(c.req.param("code"));
  if (!ci) return c.json({ error: "Not found" }, 404);
  const m = ci.members.find(m => m.userId === c.req.param("userId"));
  if (!m) return c.json({ error: "Member not found" }, 404);
  const b = await c.req.json();
  const old = m.streakCount;
  // v5.8.3 — capture previous state so we can detect ACTUAL transitions
  // (and not re-fire pushes on every idempotent re-PUT from client refresh loops).
  const prevLastPrayedDate = m.lastPrayedDate;
  const prevLastPrayedLocalDate = m.lastPrayedLocalDate;
  if (b.streakCount !== undefined) m.streakCount = b.streakCount;
  if (b.lastPrayedDate !== undefined) m.lastPrayedDate = b.lastPrayedDate;
  if (b.lastPrayedLocalDate !== undefined) m.lastPrayedLocalDate = b.lastPrayedLocalDate;
  if (b.lastPrayedTimezone !== undefined) m.lastPrayedTimezone = b.lastPrayedTimezone;
  if (b.name !== undefined) m.name = b.name;
  const prayedStateChanged =
    (b.lastPrayedDate !== undefined && b.lastPrayedDate !== prevLastPrayedDate) ||
    (b.lastPrayedLocalDate !== undefined && b.lastPrayedLocalDate !== prevLastPrayedLocalDate);
  await saveCircleToDb(ci);

  // v5.7.0 — fan out prayed-state + streak + name to every OTHER circle this user belongs to,
  // so "prayed today" is a single global user-level fact, consistent across every circle.
  // Per-circle attributes (role, canPost, notificationsMuted, avatarUrl) intentionally NOT propagated.
  const targetUserId = c.req.param("userId");
  const thisCode = c.req.param("code").toUpperCase();
  // v5.8.3 — propagate only on real change, not on every idempotent client refresh PUT.
  const streakChanged = b.streakCount !== undefined && b.streakCount !== old;
  const nameChanged = b.name !== undefined && b.name !== m.name; // m.name already updated above, so this is always false; kept for semantic clarity
  const propagate = prayedStateChanged || streakChanged || (b.name !== undefined);
  if (propagate) {
    for (const [otherCode, otherCircle] of circles) {
      if (otherCode === thisCode) continue;
      const om = otherCircle.members.find(mm => mm.userId === targetUserId);
      if (!om) continue;
      if (b.lastPrayedDate !== undefined) om.lastPrayedDate = b.lastPrayedDate;
      if (b.lastPrayedLocalDate !== undefined) om.lastPrayedLocalDate = b.lastPrayedLocalDate;
      if (b.lastPrayedTimezone !== undefined) om.lastPrayedTimezone = b.lastPrayedTimezone;
      if (b.streakCount !== undefined) om.streakCount = b.streakCount;
      if (b.name !== undefined) om.name = b.name;
      await saveCircleToDb(otherCircle);
      // v5.9.0 — live push to connected SSE clients (sub-second, reliable)
      broadcastCircleUpdate(otherCircle, targetUserId).catch(() => {});
      // v5.8.2 — silent APNs push as background fallback (best effort)
      pushSilentSyncToCircle(otherCircle, targetUserId).catch(() => {});
    }
  }
  if (propagate) {
    // v5.9.0 — live push to THIS circle's connected members
    broadcastCircleUpdate(ci, targetUserId).catch(() => {});
    // v5.8.2 — silent APNs push fallback
    pushSilentSyncToCircle(ci, targetUserId).catch(() => {});
  }

  // v5.15.2 — sync prayer state to user_data so checkStreakAtRisk sees current data.
  // Previously only daily-prayer endpoint updated user_data, leaving regular prayers invisible
  // to the streak-at-risk check, causing false "streak at risk" notifications.
  if (prayedStateChanged && b.lastPrayedDate) {
    try {
      await pool.query(
        `UPDATE user_data SET last_prayed_date=$1, last_prayed_local_date=$2, last_prayed_timezone=$3, updated_at=NOW(),
         streak_count = $4::int,
         highest_streak = GREATEST(highest_streak, COALESCE($4::int, streak_count))
         WHERE user_id=$5`,
        [b.lastPrayedDate, b.lastPrayedLocalDate || null, b.lastPrayedTimezone || null, b.streakCount || 0, c.req.param("userId")]
      );
    } catch {}
  }
  // v5.8.3 — only check last-one-standing on real prayer transitions, not idempotent re-PUTs
  if (prayedStateChanged) { checkLastOneStanding(ci, c.req.param("userId")).catch(() => {}); }
  if (b.streakCount !== undefined && b.streakCount > old && [3,7,14,30,60,90,180,365].includes(b.streakCount)) {
    trackEvent(c.req.param("userId"), "streak_milestone", { streak_count: b.streakCount, circle_code: c.req.param("code").toUpperCase() });
    // v5.15.0 — removed push notification for other members' streak milestones (spam for multi-circle users)
    // Milestones are still tracked as events and visible in the circle activity feed
  }
  return c.json({ circle: ci });
});
app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase(); const uid = c.req.param("userId");
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  const ah = c.req.header("Authorization");
  if (ah?.startsWith("Bearer ")) {
    const u = await getUserByToken(ah.replace("Bearer ", ""));
    if (u && uid !== u.id) {
      if (isCircleAdmin(u.id, ci, u.device_user_id)) {
        const target = ci.members.find(m => m.userId === uid);
        if (!target) return c.json({ error: "This member wasn't found in the circle." }, 404);
        if (target.role === "creator") return c.json({ error: "This action isn't allowed." }, 422);
        ci.members = ci.members.filter(m => m.userId !== uid);
        trackEvent(uid, "circle_removed_by_admin", { circle_code: code, removed_by: u.id });
        ci.members.length === 0 ? await deleteCircleFromDb(code) : await saveCircleToDb(ci);
        pushToUserLocalized(uid, { titleKey: "removed_from_circle_title", bodyKey: "removed_from_circle_body", bodyParams: { circle: ci.name }, type: "removed_from_circle", circleCode: code, circleName: ci.name });
        return c.json({ success: true });
      }
    }
  }
  ci.members = ci.members.filter(m => m.userId !== uid);
  trackEvent(uid, "circle_left", { circle_code: code });
  ci.members.length === 0 ? await deleteCircleFromDb(code) : await saveCircleToDb(ci);
  return c.json({ success: true });
});
app.delete("/api/circles/:code", async (c) => { const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); trackEvent(ci.creatorUserId, "circle_deleted", { circle_code: code }); await deleteCircleFromDb(code); /* v5.9.1 — expire any outstanding invite tokens for this circle so share links stop working */ try { await pool.query("UPDATE invite_tokens SET status='expired' WHERE circle_code=$1 AND status='pending'", [code]); } catch (err: any) { console.error("[Circle delete] Expire invites error:", err.message); } return c.json({ success: true }); });

// ─── Member status endpoint (for Circle Today widget) ───
app.get("/api/circles/:code/member-status", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  const memberIds = ci.members.map(m => m.userId).filter(Boolean);
  let avatarMap: Record<string, string | null> = {};
  try { const av = await pool.query("SELECT id, device_user_id, avatar_url FROM users WHERE id = ANY($1) OR device_user_id = ANY($1)", [memberIds]); for (const r of av.rows) { avatarMap[r.id] = r.avatar_url; if (r.device_user_id) avatarMap[r.device_user_id] = r.avatar_url; } } catch {}
  // v5.15.6 — community circles: hide non-subscribers AND inactive 14+ days. Private circles show everyone.
  // Always show the current user + circle creator regardless of activity.
  const visibleOnly = isCommunityCircle(ci.code) ? ci.members.filter(m => (m.visible !== false || m.userId === ci.creatorUserId || m.userId === u.id) && (isCommunityMemberActive(m) || m.userId === ci.creatorUserId || m.userId === u.id)) : ci.members;
  const members = visibleOnly.map(m => {
    const prayedToday = prayedTodayInOwnTZ(m);
    // v5.15.3 — compute real streak: if lastPrayedDate is 2+ days ago, streak is 0
    let effectiveStreak = m.streakCount || 0;
    if (effectiveStreak > 0 && m.lastPrayedDate) {
      const lastPrayed = new Date(m.lastPrayedDate);
      const now = new Date();
      const diffMs = now.getTime() - lastPrayed.getTime();
      const diffDays = diffMs / (1000 * 60 * 60 * 24);
      if (diffDays > 2) effectiveStreak = 0;
    } else if (effectiveStreak > 0 && !m.lastPrayedDate) {
      effectiveStreak = 0;
    }
    return { userId: m.userId, name: m.name, avatarUrl: avatarMap[m.userId] || m.avatarUrl || null, prayedToday, prayedAt: prayedToday ? m.lastPrayedDate : null, streakCount: effectiveStreak, role: m.role || "member" };
  });
  return c.json({ members, totalMembers: visibleOnly.length, prayedToday: members.filter(m => m.prayedToday).length });
});

// ─── Ask status: which targets has this user already asked today? ────────
app.get("/api/circles/:code/ask-status", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.query("userId");
  if (!userId) return c.json({ error: "userId required" }, 400);
  const today = new Date().toISOString().split("T")[0];
  try {
    const result = await pool.query("SELECT target_user_id FROM prayer_ask_log WHERE circle_code=$1 AND asker_user_id=$2 AND day=$3", [code, userId, today]);
    let askedCircleToday = false;
    const askedUserIdsToday: string[] = [];
    for (const row of result.rows) {
      if (!row.target_user_id) askedCircleToday = true;
      else askedUserIdsToday.push(row.target_user_id);
    }
    return c.json({ askedCircleToday, askedUserIdsToday });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// ─── Prayer Requests (Unified: supports circle-wide + personal targeting) ────────────────
app.post("/api/circles/:code/prayer-requests", async (c) => {
  const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404);
  const b = await c.req.json();
  const reqId = randomUUID();
  const targetUserId = b.targetUserId || null;
  const targetType = targetUserId ? "personal" : "circle";
  // v5 (Jun 10): one-ask-per-day quota REMOVED per Samy — asks are unlimited.
  // prayer_ask_log is still written below for analytics, but never blocks.
  const codeUpper = c.req.param("code").toUpperCase();
  const today = new Date().toISOString().split("T")[0];
  // Auto-generate default text if user didn't type anything (unified nudge flow)
  let requestText = (b.text || "").trim();
  if (!requestText) {
    if (targetType === "personal") {
      const defaults = ["Hey, praying today? Join us 🙏", "Thinking of you. Let's pray together today.", "Praying for you today. Want to join?", "You're in my prayers today 🙏"];
      requestText = defaults[Math.floor(Math.random() * defaults.length)];
    } else {
      requestText = "Let's pray together today 🙏";
    }
  }
  const newReq: StoredPrayerRequest = { id: reqId, requesterUserId: b.userId, requesterName: b.isAnonymous ? "Anonymous" : b.userName || "Someone", text: requestText, timestamp: new Date().toISOString(), isAnonymous: b.isAnonymous || false, prayedByUserIds: [], targetUserId: targetUserId || undefined, targetType, status: "active" };
  ci.prayerRequests.unshift(newReq);
  await saveCircleToDb(ci);
  // Log the ask for today's quota (silent on race / dup)
  if (b.userId) {
    try { await pool.query("INSERT INTO prayer_ask_log (circle_code, asker_user_id, target_user_id, day) VALUES ($1,$2,$3,$4)", [codeUpper, b.userId, targetUserId, today]); } catch (err: any) { /* unique conflict race — safe to ignore */ }
  }
  trackEvent(b.userId, "prayer_request_created", { circle_code: c.req.param("code").toUpperCase(), is_anonymous: b.isAnonymous || false, target_type: targetType });
  recordCircleEngagement(c.req.param("code").toUpperCase(), b.userId, targetType === "personal" ? "sent_nudge" : "created_request");
  if (targetType === "personal" && targetUserId) {
    // Personal request — only notify the target
    const targetMember = ci.members.find(m => m.userId === targetUserId);
    // v5.9.1 — title is translated to recipient's language; body stays as author-written text
    (async () => {
      try {
        const lang = await getUserLanguage(targetUserId);
        const title = t(lang, b.isAnonymous ? "prayer_request_personal_title_anon" : "prayer_request_personal_title", { name: b.userName || "Someone" });
        const body = requestText.length > 60 ? requestText.substring(0, 60) + "..." : requestText;
        await pushToUser(targetUserId, { title, body, type: "prayer_request_personal", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name, extra: { requestId: reqId, senderUserId: b.userId, senderName: b.userName || "Someone" } });
      } catch {}
    })();
  } else {
    // Circle-wide request — notify all members
    pushToCircleMembersLocalized(ci, b.userId, { titleKey: "prayer_request_title", titleParams: { circle: ci.name }, bodyKey: b.isAnonymous ? "prayer_request_body_anon" : "prayer_request_body_named", bodyParams: { name: b.userName || "Someone" }, type: "prayer_request", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name, extra: { requestId: reqId } });
  }
  // Async Lumi prayer generation (only for user-written requests, not auto-generated nudges)
  if (GEMINI_API_KEY && isGeminiAvailable() && b.text && b.text.trim().length > 10) {
    const circleCode = c.req.param("code");
    (async () => {
      try {
        const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            system_instruction: { parts: [{ text: "You write short, heartfelt prayers (under 60 words) based on prayer requests. Write in second person addressing God. Never use dashes or hyphens as punctuation. Be warm, scriptural, personal. Return ONLY the prayer text, nothing else." }] },
            contents: [{ role: "user", parts: [{ text: `Write a short prayer for this request: "${b.text}"` }] }]
          })
        });
        if (res.ok) {
          const data = (await res.json()) as any;
          const prayer = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
          if (prayer.trim()) {
            const freshCircle = getCircle(circleCode);
            if (freshCircle) {
              const req2 = freshCircle.prayerRequests.find(r => r.id === reqId);
              if (req2) { req2.generatedPrayer = prayer.trim(); await saveCircleToDb(freshCircle); }
            }
          }
        } else if (res.status === 429) { markGeminiRateLimited(); }
      } catch (err: any) { console.error("[Lumi Prayer]", err.message); }
    })();
  }
  return c.json({ circle: ci });
});

app.post("/api/circles/:code/prayer-requests/:rid/pray", async (c) => {
  const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404);
  const req = ci.prayerRequests.find(r => r.id === c.req.param("rid")); if (!req) return c.json({ error: "Not found" }, 404);
  const b = await c.req.json();
  if (!req.prayedByUserIds.includes(b.userId)) {
    req.prayedByUserIds.push(b.userId);
    // Update status for personal requests
    if (req.targetType === "personal" && req.targetUserId === b.userId) { req.status = "prayed"; }
    trackEvent(b.userId, "prayer_request_prayed", { circle_code: c.req.param("code").toUpperCase(), target_type: req.targetType || "circle" });
    recordCircleEngagement(c.req.param("code").toUpperCase(), b.userId, "prayed_for_request");
    if (req.requesterUserId !== b.userId) {
      const prayerName = ci.members.find(m => m.userId === b.userId)?.name || "Someone";
      // v5 Phase 4 — THE VOLLEY: named, reciprocal signal replaces the old
      // one-way receipt push. SSE prayed_with_you + localized push with
      // one-tap pray-back. Fired ONLY here, on a real recorded prayer.
      fireVolley({ id: b.userId, name: prayerName }, req.requesterUserId, "request", c.req.param("rid"), c.req.param("code").toUpperCase()).catch(() => {});
      // Update the sender's notification with "prayed" status
      try { await pool.query("UPDATE notifications SET data = data || $1::jsonb WHERE user_id=$2 AND type IN ('prayer_request','prayer_request_personal') AND data->>'requestId'=$3 ORDER BY created_at DESC LIMIT 1", [JSON.stringify({ recipientPrayed: true, prayedByName: prayerName }), req.requesterUserId, c.req.param("rid")]); } catch {}
    }
  }
  await saveCircleToDb(ci);
  return c.json({ circle: ci });
});
app.delete("/api/circles/:code/prayer-requests/:rid", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const before = ci.prayerRequests.length; ci.prayerRequests = ci.prayerRequests.filter(r => r.id !== c.req.param("rid")); if (ci.prayerRequests.length === before) return c.json({ error: "Not found" }, 404); await saveCircleToDb(ci); return c.json({ success: true }); });

// Mark prayer request as answered
app.put("/api/circles/:code/prayer-requests/:rid/answered", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404);
  const req = ci.prayerRequests.find(r => r.id === c.req.param("rid"));
  if (!req) return c.json({ error: "Not found" }, 404);
  if (req.requesterUserId !== u.id && !isCircleAdmin(u.id, ci, u.device_user_id)) return c.json({ error: "Only the requester can mark this as answered." }, 403);
  req.status = "answered";
  await saveCircleToDb(ci);
  trackEvent(u.id, "prayer_request_answered", { circle_code: c.req.param("code").toUpperCase(), prayer_count: req.prayedByUserIds.length });
  // Notify circle members that the prayer was answered
  const requesterName = req.isAnonymous ? "Someone" : req.requesterName;
  pushToCircleMembersLocalized(ci, u.id, { titleKey: "prayer_answered_title", bodyKey: "prayer_answered_body", bodyParams: { name: requesterName, count: req.prayedByUserIds.length }, type: "prayer_answered", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name, extra: { requestId: c.req.param("rid") } });
  return c.json({ success: true, request: req });
});
app.get("/api/circles/:code/info", (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const cr = ci.members.find(m => m.userId === ci.creatorUserId); return c.json({ name: ci.name, emoji: ci.emoji, memberCount: ci.members.length, creatorName: cr?.name || null }); });

// ─── Admin: posting rights + mute + role management ──────────────────
app.get("/api/circles/:code/members-list", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const ci = getCircle(c.req.param("code"));
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  const memberIds = ci.members.map(m => m.userId).filter(Boolean);
  let avatarMap: Record<string, string | null> = {};
  try { const av = await pool.query("SELECT id, device_user_id, avatar_url FROM users WHERE id = ANY($1) OR device_user_id = ANY($1)", [memberIds]); for (const r of av.rows) { avatarMap[r.id] = r.avatar_url; if (r.device_user_id) avatarMap[r.device_user_id] = r.avatar_url; } } catch {}
  return c.json({
    members: ci.members.map(m => ({
      userId: m.userId,
      name: m.name,
      avatarUrl: avatarMap[m.userId] || m.avatarUrl || null,
      role: m.role || "member"
    }))
  });
});

app.put("/api/admin/circles/:code/members/:userId/posting-rights", async (c) => { const sec = c.req.header("X-Admin-Secret"); const ah = c.req.header("Authorization"); let auth = false; if (sec && sec === process.env.ADMIN_SECRET) auth = true; if (ah?.startsWith("Bearer ")) { const u = await getUserByToken(ah.replace("Bearer ", "")); if (u && (isAdmin(u.id) || isCircleAdmin(u.id, getCircle(c.req.param("code"))!, u.device_user_id))) auth = true; } if (!auth) return c.json({ error: "Forbidden" }, 403); const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const m = ci.members.find(m => m.userId === c.req.param("userId")); if (!m) return c.json({ error: "Member not found" }, 404); const { canPost } = await c.req.json(); m.canPost = !!canPost; await saveCircleToDb(ci); return c.json({ success: true, userId: m.userId, canPost: m.canPost }); });
app.put("/api/circles/:code/mute", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401); const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const m = ci.members.find((m: StoredMember) => m.userId === u.id); if (!m) return c.json({ error: "Not a member" }, 403); const { muted } = await c.req.json(); m.notificationsMuted = !!muted; await saveCircleToDb(ci); return c.json({ success: true, muted: m.notificationsMuted }); });

app.post("/api/circles/:code/members/:userId/promote", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code);
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isCircleAdmin(u.id, ci, u.device_user_id)) return c.json({ error: "You don't have permission to do this." }, 403);
  const targetId = c.req.param("userId");
  const m = ci.members.find(m => m.userId === targetId);
  if (!m) return c.json({ error: "This member wasn't found in the circle." }, 404);
  if (m.role === "creator") return c.json({ error: "This action isn't allowed." }, 422);
  if (m.role === "admin") return c.json({ error: "This member is already an admin." }, 422);
  m.role = "admin"; m.canPost = true;
  await saveCircleToDb(ci);
  trackEvent(u.id, "circle_member_promoted", { circle_code: code, target_user_id: targetId });
  pushToUserLocalized(targetId, { titleKey: "promoted_to_admin_title", bodyKey: "promoted_to_admin_body", bodyParams: { name: u.name || "Someone", circle: ci.name }, type: "promoted_to_admin", circleCode: code, circleName: ci.name });
  return c.json({ userId: targetId, role: "admin" });
});

app.post("/api/circles/:code/members/:userId/demote", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code);
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isCircleAdmin(u.id, ci, u.device_user_id)) return c.json({ error: "You don't have permission to do this." }, 403);
  const targetId = c.req.param("userId");
  if (targetId === u.id) return c.json({ error: "This action isn't allowed." }, 422);
  const m = ci.members.find(m => m.userId === targetId);
  if (!m) return c.json({ error: "This member wasn't found in the circle." }, 404);
  if (m.role === "creator") return c.json({ error: "This action isn't allowed." }, 422);
  if (m.role !== "admin") return c.json({ error: "This member is not an admin." }, 422);
  m.role = "member";
  await saveCircleToDb(ci);
  trackEvent(u.id, "circle_member_demoted", { circle_code: code, target_user_id: targetId });
  return c.json({ userId: targetId, role: "member" });
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLE INVITES ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/circles/:code/invite", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "You need to be logged in." }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "You don't have permission to do this." }, 403);
  await pool.query("UPDATE invite_tokens SET status='expired' WHERE circle_code=$1 AND inviter_user_id=$2 AND status='pending'", [code, u.id]);
  const token = randomUUID().replace(/-/g, "").substring(0, 16);
  await pool.query("INSERT INTO invite_tokens (token, circle_code, inviter_user_id, inviter_name) VALUES ($1,$2,$3,$4)", [token, code, u.id, u.name || "Someone"]);
  trackEvent(u.id, "invite_link_generated", { circle_code: code, circle_name: ci.name });
  return c.json({ inviteUrl: `https://pramen.app/invite/${token}`, token });
});

app.get("/api/invites/:token", async (c) => {
  const token = c.req.param("token");
  const result = await pool.query("SELECT * FROM invite_tokens WHERE token=$1", [token]);
  if (!result.rows[0]) return c.json({ error: "This invite link is invalid." }, 404);
  const inv = result.rows[0];
  if (inv.status !== "pending") return c.json({ error: "This invite link has already been used or is no longer valid." }, 410);
  const ci = getCircle(inv.circle_code);
  // v5.9.1 — if the circle was deleted after the invite was generated, expire the invite
  // and return a clear error instead of pretending the invite is valid with fallback name.
  if (!ci) {
    await pool.query("UPDATE invite_tokens SET status='expired' WHERE token=$1", [token]);
    return c.json({ error: "This circle no longer exists." }, 410);
  }
  // P6.0 instrumentation: any fetch of a pending invite = link opened
  trackEvent(inv.inviter_user_id || "unknown", "invite_link_opened", { circle_code: inv.circle_code, token });
  return c.json({ circleCode: inv.circle_code, circleName: ci.name, inviterName: inv.inviter_name, status: inv.status });
});

app.post("/api/invites/:token/accept", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "You need to be logged in to join this circle." }, 401);
  const token = c.req.param("token");
  const result = await pool.query("SELECT * FROM invite_tokens WHERE token=$1", [token]);
  if (!result.rows[0]) return c.json({ error: "This invite link is invalid." }, 404);
  const inv = result.rows[0];
  if (inv.status !== "pending") return c.json({ error: "This invite link has already been used or is no longer valid." }, 410);
  const ci = getCircle(inv.circle_code); if (!ci) return c.json({ error: "This circle no longer exists." }, 404);
  const alreadyMember = ci.members.some(m => m.userId === u.id);
  if (!alreadyMember) {
    // v5.15.5 — set visibility based on circle type (community = sub required, private = always visible)
    let inviteVisible = true;
    if (isCommunityCircle(inv.circle_code)) {
      const userRow = await pool.query("SELECT subscription_status FROM users WHERE id=$1", [u.id]).then(r => r.rows[0]).catch(() => null);
      inviteVisible = !!(userRow && userRow.subscription_status && userRow.subscription_status !== "none");
    }
    ci.members.push({ userId: u.id, name: u.name || "", streakCount: 0, lastPrayedDate: null, joinedAt: new Date().toISOString(), visible: inviteVisible });
    await saveCircleToDb(ci);
    trackEvent(u.id, "circle_invite_accepted", { circle_code: inv.circle_code, circle_name: ci.name, invite_token: token });
    pushToUserLocalized(ci.creatorUserId, { titleKey: "member_joined_title", titleParams: { name: u.name || "Someone", circle: ci.name }, bodyKey: "member_joined_body", bodyParams: { count: ci.members.length }, type: "member_joined", circleCode: inv.circle_code, circleName: ci.name, extra: { memberName: u.name || "Someone" } });
  }
  await pool.query("UPDATE invite_tokens SET status='accepted', accepted_by_user_id=$1, accepted_at=NOW() WHERE token=$2", [u.id, token]);
  return c.json({ circleCode: inv.circle_code, circleName: ci.name });
});

// ═══════════════════════════════════════════════════════════════════
// ─── LUMI — BIBLE COMPANION (Gemini Flash) ──────────────────────
// ═══════════════════════════════════════════════════════════════════

app.get("/api/lumi/daily-reflection", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const today = new Date().toISOString().split("T")[0];
  try {
    const existing = await pool.query("SELECT * FROM daily_reflections WHERE date=$1", [today]);
    if (existing.rows[0]) return c.json({ verse: existing.rows[0].verse, reference: existing.rows[0].reference, reflection: existing.rows[0].reflection });
    const reflection = await generateDailyReflection();
    if (reflection) return c.json(reflection);
    return c.json({ verse: "Be still, and know that I am God.", reference: "Psalm 46:10", reflection: "In the noise of daily life, God invites us to pause. This isn't passive — it's an act of trust, a choice to let go and remember who holds everything together." });
  } catch (err: any) {
    console.error("[Lumi] Daily reflection error:", err.message);
    return c.json({ verse: "Be still, and know that I am God.", reference: "Psalm 46:10", reflection: "In the noise of daily life, God invites us to pause. This isn't passive — it's an act of trust, a choice to let go and remember who holds everything together." });
  }
});

async function generateDailyReflection(): Promise<{ verse: string; reference: string; reflection: string } | null> {
  if (!GEMINI_API_KEY) return null;
  if (!isGeminiAvailable()) { console.log("[Lumi] Skipping daily reflection — Gemini rate limited"); return null; }
  const today = new Date().toISOString().split("T")[0];
  const existing = await pool.query("SELECT * FROM daily_reflections WHERE date=$1", [today]);
  if (existing.rows[0]) return { verse: existing.rows[0].verse, reference: existing.rows[0].reference, reflection: existing.rows[0].reflection };
  try {
    const dayOfYear = Math.floor((Date.now() - new Date(new Date().getFullYear(), 0, 0).getTime()) / 86400000);
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: "You are a Bible verse curator. Respond ONLY with valid JSON, no markdown, no backticks, no extra text. Never use dashes or hyphens as punctuation in the reflection." }] },
          contents: [{ role: "user", parts: [{ text: `Select a meaningful Bible verse for the ${getLiturgicalSeason()} liturgical season, day ${dayOfYear} of the year. Choose verses appropriate to this season's themes. Return JSON: {"verse": "the full verse text", "reference": "Book Chapter:Verse", "reflection": "2-3 warm sentences about what this verse means and why it matters today, written in the voice of a gentle pastoral guide named Lumi. Do not use any dashes."}` }] }],
        }),
      }
    );
    if (!res.ok) { if (res.status === 429) markGeminiRateLimited(); console.error("[Lumi] Daily generation failed:", res.status); return null; }
    const data = (await res.json()) as any;
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
    const clean = text.replace(/```json|```/g, "").trim();
    const parsed = JSON.parse(clean);
    if (parsed.verse && parsed.reference && parsed.reflection) {
      await pool.query("INSERT INTO daily_reflections (date, verse, reference, reflection) VALUES ($1,$2,$3,$4) ON CONFLICT (date) DO NOTHING", [today, parsed.verse, parsed.reference, parsed.reflection]);
      console.log(`[Lumi] Generated daily reflection: ${parsed.reference}`);
      return parsed;
    }
    return null;
  } catch (err: any) { console.error("[Lumi] Generation error:", err.message); return null; }
}

// ─── Liturgical Season ─────────────────────────────────────────────
function computeEasterDate(year: number): Date {
  const a = year % 19, b = Math.floor(year / 100), c2 = year % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3), h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c2 / 4), k = c2 % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(year, month - 1, day);
}

function getLiturgicalSeason(date: Date = new Date()): string {
  const y = date.getFullYear(); const m = date.getMonth(); const d = date.getDate();
  const easter = computeEasterDate(y);
  const eDay = easter.getTime();
  const today = new Date(y, m, d).getTime();
  const day = 86400000;
  const ashWed = eDay - 46 * day;
  const palmSun = eDay - 7 * day;
  const pentecost = eDay + 49 * day;
  const christmas = new Date(y, 11, 25).getTime();
  let adventStart = christmas;
  let count = 0;
  for (let i = 1; i <= 28; i++) {
    const check = new Date(christmas - i * day);
    if (check.getDay() === 0) { count++; if (count === 4) { adventStart = check.getTime(); break; } }
  }
  if (today >= adventStart && today < christmas) return "advent";
  if (today >= christmas && today <= new Date(y, 0, 6).getTime() + (m === 11 ? 365 * day : 0)) return "christmas";
  if (m === 0 && d <= 6) return "christmas";
  if (today >= palmSun && today < eDay) return "holyWeek";
  if (today >= ashWed && today < palmSun) return "lent";
  if (today >= eDay && today <= pentecost) return "easter";
  return "ordinaryTime";
}

app.get("/api/seasonal/verse-of-the-day", async (c) => {
  const season = getLiturgicalSeason();
  const today = new Date().toISOString().split("T")[0];
  const langRaw = (c.req.query("lang") || "en").toLowerCase();
  const lang = ["en", "fr", "es", "pt"].includes(langRaw) ? langRaw : "en";
  // Per-language cache (v5.8.0)
  try {
    const existing = await pool.query("SELECT * FROM seasonal_verses WHERE date=$1 AND lang=$2", [today, lang]);
    if (existing.rows[0]) {
      return c.json({ verse: existing.rows[0].verse, reference: existing.rows[0].reference, season, lang });
    }
  } catch {}
  if (GEMINI_API_KEY) {
    const seasonNames: Record<string, string> = { advent: "Advent", christmas: "Christmas", lent: "Lent", holyWeek: "Holy Week", easter: "Easter", ordinaryTime: "Ordinary Time" };
    const langNames: Record<string, string> = { en: "English", fr: "French", es: "Spanish", pt: "Portuguese" };
    try {
      const dayOfYear = Math.floor((Date.now() - new Date(new Date().getFullYear(), 0, 0).getTime()) / 86400000);
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: `You are a Bible verse curator. Respond ONLY with valid JSON, no markdown, no backticks. Return both the verse text AND the book/chapter/verse reference in ${langNames[lang]}.` }] },
          contents: [{ role: "user", parts: [{ text: `Select a Bible verse appropriate for the ${seasonNames[season] || "Ordinary Time"} liturgical season, day ${dayOfYear}. Return JSON: {"verse": "full verse text in ${langNames[lang]}", "reference": "Book Chapter:Verse formatted in ${langNames[lang]}"}` }] }],
        }),
      });
      if (res.ok) {
        const data = (await res.json()) as any;
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
        const parsed = JSON.parse(text.replace(/```json|```/g, "").trim());
        if (parsed.verse && parsed.reference) {
          try {
            await pool.query("INSERT INTO seasonal_verses (date, lang, verse, reference) VALUES ($1,$2,$3,$4) ON CONFLICT (date, lang) DO NOTHING", [today, lang, parsed.verse, parsed.reference]);
          } catch {}
          return c.json({ verse: parsed.verse, reference: parsed.reference, season, lang });
        }
      }
    } catch {}
  }
  // Per-language fallback
  const fallbacks: Record<string, { verse: string; reference: string }> = {
    en: { verse: "Be still, and know that I am God.", reference: "Psalm 46:10" },
    fr: { verse: "Arrêtez, et sachez que je suis Dieu.", reference: "Psaume 46:10" },
    es: { verse: "Estad quietos, y conoced que yo soy Dios.", reference: "Salmo 46:10" },
    pt: { verse: "Aquietai-vos, e sabei que eu sou Deus.", reference: "Salmo 46:10" },
  };
  const fb = fallbacks[lang] || fallbacks.en;
  return c.json({ verse: fb.verse, reference: fb.reference, season, lang });
});

// ═══════════════════════════════════════════════════════════════════
// ─── NOTIFICATIONS ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/notifications", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const filter = c.req.query("filter") || "all"; let q = "SELECT * FROM notifications WHERE user_id=$1 AND is_deleted=false"; if (filter === "unread") q += " AND is_read=false"; q += " ORDER BY created_at DESC LIMIT 100"; const r = await pool.query(q, [u.id]); const unread = await pool.query("SELECT COUNT(*) as count FROM notifications WHERE user_id=$1 AND is_deleted=false AND is_read=false", [u.id]); return c.json({ notifications: r.rows, unreadCount: parseInt(unread.rows[0]?.count || "0") }); });
app.post("/api/notifications/read", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const { notificationIds, all } = await c.req.json(); if (all) { await pool.query("UPDATE notifications SET is_read=true WHERE user_id=$1 AND is_read=false", [u.id]); } else if (Array.isArray(notificationIds) && notificationIds.length > 0) { await pool.query("UPDATE notifications SET is_read=true WHERE id = ANY($1) AND user_id=$2", [notificationIds, u.id]); } return c.json({ success: true }); });
app.delete("/api/notifications/:notificationId", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); await pool.query("UPDATE notifications SET is_deleted=true WHERE id=$1 AND user_id=$2", [c.req.param("notificationId"), u.id]); return c.body(null, 204); });
app.delete("/api/notifications", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); await pool.query("UPDATE notifications SET is_deleted=true WHERE user_id=$1 AND is_deleted=false", [u.id]); return c.json({ success: true }); });
app.get("/api/notifications/preferences", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const r = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [u.id]); if (r.rows[0]) { const p = r.rows[0]; return c.json({ prayer_requests: p.prayer_requests, circle_members: p.circle_members, streak_milestones: p.streak_milestones, streak_reminders: p.streak_reminders ?? true, admin_promotions: p.admin_promotions, removed_from_circle: p.removed_from_circle }); } return c.json({ prayer_requests: true, circle_members: true, streak_milestones: true, streak_reminders: true, admin_promotions: true, removed_from_circle: true }); });
app.patch("/api/notifications/preferences", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const body = await c.req.json(); const cols = ["prayer_requests","circle_members","streak_milestones","streak_reminders","admin_promotions","removed_from_circle"]; await pool.query("INSERT INTO notification_preferences (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", [u.id]); for (const col of cols) { if (body[col] !== undefined) { await pool.query(`UPDATE notification_preferences SET ${col}=$1, updated_at=NOW() WHERE user_id=$2`, [!!body[col], u.id]); } } const r = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [u.id]); const p = r.rows[0]; return c.json({ prayer_requests: p.prayer_requests, circle_members: p.circle_members, streak_milestones: p.streak_milestones, streak_reminders: p.streak_reminders ?? true, admin_promotions: p.admin_promotions, removed_from_circle: p.removed_from_circle }); });

// ═══════════════════════════════════════════════════════════════════
// ─── REFERRALS & REWARDS ────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
function generateReferralCode(name: string): string { const prefix = (name || "PRAY").replace(/[^A-Z]/gi, "").substring(0, 5).toUpperCase() || "PRAY"; const digits = Math.floor(1000 + Math.random() * 9000); return `${prefix}${digits}`; }


// v5.9.4 — public validation endpoint so iOS can verify a referral code exists and belongs to someone
// (was previously only a client-side regex check).
app.get("/api/referrals/validate/:code", async (c) => {
  const code = c.req.param("code").toUpperCase();
  if (!/^[A-Z0-9-]{4,12}$/.test(code)) return c.json({ valid: false, reason: "bad_format" }, 400);
  // Check promo codes first
  const promo = await pool.query("SELECT id, duration, campaign, status, expires_at FROM promo_codes WHERE code=$1", [code]);
  if (promo.rows[0]) {
    const p = promo.rows[0];
    if (p.status === "redeemed") return c.json({ valid: false, reason: "already_used" });
    if (p.status === "expired" || (p.expires_at && new Date(p.expires_at) < new Date())) return c.json({ valid: false, reason: "expired" });
    const durationLabel: Record<string, string> = { daily: "1 day", three_day: "3 days", weekly: "7 days", monthly: "30 days", two_month: "60 days", three_month: "90 days", six_month: "6 months", yearly: "1 year", lifetime: "lifetime" };
    return c.json({ valid: true, type: "promo", referrerName: "prAmen Team", durationLabel: durationLabel[p.duration] || p.duration });
  }
  // Then check referral codes
  const result = await pool.query("SELECT rc.user_id, u.name FROM referral_codes rc LEFT JOIN users u ON u.id = rc.user_id WHERE rc.code=$1", [code]);
  if (!result.rows[0]) return c.json({ valid: false, reason: "not_found" });
  return c.json({ valid: true, type: "referral", referrerName: result.rows[0].name || null });
});

app.get("/api/referrals/me", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]); const code = codeResult.rows[0]?.code || null; const referrals = await pool.query("SELECT id, referred_user_id, status, created_at, confirmed_at FROM referrals WHERE referrer_user_id=$1 ORDER BY created_at DESC", [u.id]); const enriched = []; for (const ref of referrals.rows) { let name = null; if (ref.referred_user_id) { const usr = await pool.query("SELECT name FROM users WHERE id=$1", [ref.referred_user_id]); name = usr.rows[0]?.name || null; } enriched.push({ ...ref, referred_name: name }); } const confirmedCount = referrals.rows.filter((r: any) => r.status === "confirmed").length; return c.json({ code, link: code ? `https://pramen.app/ref/${code}` : null, referrals: enriched, confirmedCount }); });
app.post("/api/referrals/generate", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const existing = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]); if (existing.rows[0]) return c.json({ code: existing.rows[0].code, link: `https://pramen.app/ref/${existing.rows[0].code}` }); let code = ""; for (let attempt = 0; attempt < 10; attempt++) { code = generateReferralCode(u.name || "PRAY"); const collision = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [code]); if (collision.rows.length === 0) break; } await pool.query("INSERT INTO referral_codes (user_id, code) VALUES ($1, $2)", [u.id, code]); trackEvent(u.id, "referral_code_generated", { code }); return c.json({ code, link: `https://pramen.app/ref/${code}` }); });
app.post("/api/referrals/track", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { referralCode, newUserEmail } = await c.req.json();
  if (!referralCode) return c.json({ error: "referralCode required" }, 400);
  const normalizedCode = referralCode.toUpperCase();

  // Check if it's a promo code first
  const promo = await pool.query("SELECT * FROM promo_codes WHERE code=$1", [normalizedCode]);
  if (promo.rows[0]) {
    const p = promo.rows[0];
    if (p.status === "redeemed") return c.json({ error: "This promo code has already been used." }, 409);
    if (p.status === "expired" || (p.expires_at && new Date(p.expires_at) < new Date())) return c.json({ error: "This promo code has expired." }, 410);
    const existingPromo = await pool.query("SELECT id FROM promo_codes WHERE redeemed_by_user_id=$1 AND status='redeemed'", [u.id]);
    if (existingPromo.rows.length > 0) return c.json({ error: "You have already redeemed a promo code." }, 409);
    if (!REVENUECAT_SECRET_KEY) return c.json({ error: "Subscription service unavailable." }, 500);
    try {
      const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(u.id)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: p.duration }) });
      if (!rcRes.ok) return c.json({ error: "Could not activate promo. Please try again." }, 500);
      await pool.query("UPDATE promo_codes SET status='redeemed', redeemed_by_user_id=$1, redeemed_at=NOW() WHERE id=$2", [u.id, p.id]);
      const durationLabel: Record<string, string> = { daily: "1 day", three_day: "3 days", weekly: "7 days", monthly: "30 days", two_month: "60 days", three_month: "90 days", six_month: "6 months", yearly: "1 year", lifetime: "lifetime" };
      console.log(`[Promo] Code ${normalizedCode} redeemed by ${u.id.substring(0, 8)} (${u.email || "no email"}) — ${durationLabel[p.duration]} premium`);
      trackEvent(u.id, "promo_code_redeemed", { code: normalizedCode, campaign: p.campaign, duration: p.duration });
      return c.json({ referralId: p.id, trialDays: p.duration === "monthly" ? 30 : 0, discountApplied: true, promoRedeemed: true, durationLabel: durationLabel[p.duration] });
    } catch (err: any) { return c.json({ error: "Could not activate promo. Please try again." }, 500); }
  }

  // Otherwise handle as referral code
  const referrer = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [normalizedCode]);
  if (!referrer.rows[0]) return c.json({ error: "Invalid referral code" }, 404);
  const referrerId = referrer.rows[0].user_id;
  if (u.id === referrerId) return c.json({ error: "You cannot refer yourself." }, 422);
  const dup = await pool.query("SELECT id FROM referrals WHERE referrer_user_id=$1 AND referred_user_id=$2", [referrerId, u.id]);
  if (dup.rows.length > 0) return c.json({ error: "This referral has already been tracked." }, 409);
  const r = await pool.query("INSERT INTO referrals (referrer_user_id, referred_user_id, referred_email) VALUES ($1,$2,$3) RETURNING id", [referrerId, u.id, newUserEmail || null]);
  trackEvent(referrerId, "referral_tracked", { referred_user_id: u.id, code: referralCode });
  const now = new Date(); const te30 = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
  await pool.query("UPDATE users SET trial_end_date=$1, updated_at=NOW() WHERE id=$2 AND subscription_status='trial'", [te30, u.id]);
  if (REVENUECAT_SECRET_KEY) { try { const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(u.id)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) }); if (rcRes.ok) console.log(`[Referral] RC promotional 30d granted to referred ${u.id.substring(0, 8)}`); else console.error(`[Referral] RC promotional failed: ${rcRes.status}`); } catch (err: any) { console.error("[Referral] RC promotional error:", err.message); } }
  console.log(`[Referral] Extended trial to 30d for ${u.id.substring(0, 8)}… (code: ${referralCode})`);
  return c.json({ referralId: r.rows[0].id, trialDays: 30, discountApplied: true });
});
app.post("/api/referrals/confirm", async (c) => { const sec = c.req.header("X-Admin-Secret"); if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const { referredUserId } = await c.req.json(); if (!referredUserId) return c.json({ error: "referredUserId required" }, 400); const ref = await pool.query("UPDATE referrals SET status='confirmed', confirmed_at=NOW() WHERE referred_user_id=$1 AND status='pending' RETURNING referrer_user_id", [referredUserId]); if (ref.rows[0]) { pushToUserLocalized(ref.rows[0].referrer_user_id, { titleKey: "referral_confirmed_title", bodyKey: "referral_confirmed_body", type: "referral_confirmed" }); } return c.json({ confirmed: ref.rows.length }); });
app.post("/api/referrals/reverse", async (c) => { const sec = c.req.header("X-Admin-Secret"); if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const { referredUserId } = await c.req.json(); if (!referredUserId) return c.json({ error: "referredUserId required" }, 400); await pool.query("UPDATE referrals SET status='reversed', reversed_at=NOW() WHERE referred_user_id=$1 AND status='confirmed'", [referredUserId]); return c.json({ reversed: true }); });

// ─── ADMIN: GRANT TRIAL ─────────────────────────────────────────
app.post("/api/admin/grant-trial", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403);
  const { email, userId, duration, reason } = await c.req.json();
  if (!email && !userId) return c.json({ error: "email or userId required" }, 400);
  const dur = duration || "monthly";
  const validDurations = ["daily", "three_day", "weekly", "monthly", "two_month", "three_month", "six_month", "yearly", "lifetime"];
  if (!validDurations.includes(dur)) return c.json({ error: `Invalid duration. Valid: ${validDurations.join(", ")}` }, 400);
  let targetUserId = userId;
  if (!targetUserId && email) {
    const userResult = await pool.query("SELECT id FROM users WHERE lower(trim(email))=lower(trim($1))", [email.toLowerCase()]);
    if (!userResult.rows[0]) return c.json({ error: `No user found with email: ${email}` }, 404);
    targetUserId = userResult.rows[0].id;
  }
  if (!REVENUECAT_SECRET_KEY) return c.json({ error: "RevenueCat not configured" }, 500);
  try {
    const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(targetUserId)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: dur }) });
    if (!rcRes.ok) { const errText = await rcRes.text().catch(() => ""); return c.json({ error: `RevenueCat error: ${rcRes.status} ${errText}` }, 500); }
    const durationLabel: Record<string, string> = { daily: "1 day", three_day: "3 days", weekly: "7 days", monthly: "30 days", two_month: "60 days", three_month: "90 days", six_month: "180 days", yearly: "365 days", lifetime: "lifetime" };
    console.log(`[Admin] Granted ${durationLabel[dur]} trial to ${targetUserId.substring(0, 8)} (${email || "no email"}) reason: ${reason || "none"}`);
    trackEvent(targetUserId, "admin_trial_granted", { duration: dur, reason: reason || "influencer" });
    return c.json({ success: true, userId: targetUserId, duration: dur, durationLabel: durationLabel[dur] });
  } catch (err: any) { return c.json({ error: `Grant failed: ${err.message}` }, 500); }
});

// ─── Promo Codes ────────────────────────────────────────────────
function generatePromoCode(): string {
  const ch = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "PRAY";
  for (let i = 0; i < 4; i++) code += ch[Math.floor(Math.random() * ch.length)];
  return code;
}

// Generate promo codes (admin)
app.post("/api/admin/promo-codes/generate", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403);
  const { count, duration, campaign, expiresInDays } = await c.req.json();
  const qty = Math.min(Math.max(parseInt(count) || 1, 1), 50);
  const dur = duration || "monthly";
  const validDurations = ["daily", "three_day", "weekly", "monthly", "two_month", "three_month", "six_month", "yearly", "lifetime"];
  if (!validDurations.includes(dur)) return c.json({ error: `Invalid duration. Valid: ${validDurations.join(", ")}` }, 400);
  const camp = campaign || "influencer";
  const expiresAt = expiresInDays ? new Date(Date.now() + parseInt(expiresInDays) * 86400000).toISOString() : null;
  const codes: string[] = [];
  for (let i = 0; i < qty; i++) {
    let code = generatePromoCode();
    let attempts = 0;
    while (attempts < 10) {
      try {
        await pool.query("INSERT INTO promo_codes (code, duration, campaign, expires_at) VALUES ($1, $2, $3, $4)", [code, dur, camp, expiresAt]);
        codes.push(code);
        break;
      } catch {
        code = generatePromoCode();
        attempts++;
      }
    }
  }
  console.log(`[Promo] Generated ${codes.length} codes for campaign "${camp}" (${dur})`);
  return c.json({ success: true, codes, count: codes.length, duration: dur, campaign: camp, expiresAt });
});

// List promo codes (admin)
app.get("/api/admin/promo-codes", async (c) => {
  const key = c.req.query("key") || c.req.header("X-Admin-Secret");
  if (key !== process.env.ADMIN_SECRET && key !== DASHBOARD_SECRET) return c.json({ error: "Forbidden" }, 403);
  const campaign = c.req.query("campaign");
  let q = "SELECT pc.*, u.name as redeemed_by_name, u.email as redeemed_by_email FROM promo_codes pc LEFT JOIN users u ON pc.redeemed_by_user_id = u.id";
  const params: any[] = [];
  if (campaign) { q += " WHERE pc.campaign=$1"; params.push(campaign); }
  q += " ORDER BY pc.created_at DESC LIMIT 200";
  const r = await pool.query(q, params);
  const summary = { total: r.rows.length, active: r.rows.filter((x: any) => x.status === "active").length, redeemed: r.rows.filter((x: any) => x.status === "redeemed").length, expired: r.rows.filter((x: any) => x.status === "expired").length };
  return c.json({ codes: r.rows, summary });
});

// Redeem promo code (authenticated user from the app)
app.post("/api/promo-codes/redeem", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { code } = await c.req.json();
  if (!code) return c.json({ error: "Please enter a promo code." }, 400);
  const normalized = code.trim().toUpperCase();
  const r = await pool.query("SELECT * FROM promo_codes WHERE code=$1", [normalized]);
  if (!r.rows[0]) return c.json({ error: "Invalid promo code." }, 404);
  const promo = r.rows[0];
  if (promo.status === "redeemed") return c.json({ error: "This promo code has already been used." }, 409);
  if (promo.status === "expired" || (promo.expires_at && new Date(promo.expires_at) < new Date())) {
    await pool.query("UPDATE promo_codes SET status='expired' WHERE id=$1", [promo.id]);
    return c.json({ error: "This promo code has expired." }, 410);
  }
  // Check if user already has an active promo
  const existing = await pool.query("SELECT id FROM promo_codes WHERE redeemed_by_user_id=$1 AND status='redeemed'", [u.id]);
  if (existing.rows.length > 0) return c.json({ error: "You have already redeemed a promo code." }, 409);
  // Grant RC promotional entitlement
  if (!REVENUECAT_SECRET_KEY) return c.json({ error: "Subscription service unavailable." }, 500);
  try {
    const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(u.id)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: promo.duration }) });
    if (!rcRes.ok) { const errText = await rcRes.text().catch(() => ""); return c.json({ error: "Could not activate promo. Please try again." }, 500); }
    await pool.query("UPDATE promo_codes SET status='redeemed', redeemed_by_user_id=$1, redeemed_at=NOW() WHERE id=$2", [u.id, promo.id]);
    const durationLabel: Record<string, string> = { daily: "1 day", three_day: "3 days", weekly: "7 days", monthly: "30 days", two_month: "60 days", three_month: "90 days", six_month: "6 months", yearly: "1 year", lifetime: "lifetime" };
    console.log(`[Promo] Code ${normalized} redeemed by ${u.id} (${u.email || "no email"}) — ${durationLabel[promo.duration]} premium`);
    trackEvent(u.id, "promo_code_redeemed", { code: normalized, campaign: promo.campaign, duration: promo.duration });
    return c.json({ success: true, duration: promo.duration, durationLabel: durationLabel[promo.duration], message: `You now have ${durationLabel[promo.duration]} of premium access!` });
  } catch (err: any) { return c.json({ error: "Could not activate promo. Please try again." }, 500); }
});

// ─── Outreach Contacts API ──────────────────────────────────────
app.get("/api/dashboard/outreach", async (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const platform = c.req.query("platform");
    let q = "SELECT * FROM outreach_contacts";
    const params: string[] = [];
    if (platform) { q += " WHERE platform=$1"; params.push(platform); }
    q += " ORDER BY outreach_date DESC, created_at DESC";
    const result = await pool.query(q, params);
    const contacts = result.rows;
    const summary = {
      total: contacts.length,
      by_platform: {} as Record<string, number>,
      by_status: {} as Record<string, number>,
      replied: contacts.filter((c: any) => c.their_response).length,
      response_rate: contacts.length > 0 ? Math.round(contacts.filter((c: any) => c.their_response).length / contacts.length * 100) : 0
    };
    contacts.forEach((c: any) => {
      summary.by_platform[c.platform] = (summary.by_platform[c.platform] || 0) + 1;
      summary.by_status[c.status] = (summary.by_status[c.status] || 0) + 1;
    });
    return c.json({ contacts, summary });
  } catch (err: any) { return c.json({ error: "Outreach fetch failed", detail: err.message }, 500); }
});

app.post("/api/dashboard/outreach", async (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const body = await c.req.json();
    const contacts = Array.isArray(body) ? body : [body];
    let stored = 0;
    for (const ct of contacts) {
      await pool.query(`INSERT INTO outreach_contacts (platform, handle, name, followers, category, contact_method, contact_email, rate, promo_code, status, outreach_date, their_response, our_reply, next_step, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT DO NOTHING`,
        [ct.platform||'', ct.handle||'', ct.name||'', ct.followers||'', ct.category||'', ct.contact_method||'', ct.contact_email||'', ct.rate||'', ct.promo_code||'', ct.status||'sent', ct.outreach_date||new Date().toISOString().slice(0,10), ct.their_response||'', ct.our_reply||'', ct.next_step||'', ct.notes||'']);
      stored++;
    }
    return c.json({ success: true, stored });
  } catch (err: any) { return c.json({ error: "Outreach save failed", detail: err.message }, 500); }
});

app.put("/api/dashboard/outreach/:id", async (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const id = c.req.param("id");
    const body = await c.req.json();
    const fields: string[] = [];
    const values: any[] = [];
    let idx = 1;
    for (const key of ['status', 'their_response', 'our_reply', 'next_step', 'notes', 'rate', 'contact_email']) {
      if (body[key] !== undefined) { fields.push(`${key}=$${idx}`); values.push(body[key]); idx++; }
    }
    if (fields.length === 0) return c.json({ error: "No fields to update" }, 400);
    fields.push(`updated_at=NOW()`);
    values.push(id);
    await pool.query(`UPDATE outreach_contacts SET ${fields.join(',')} WHERE id=$${idx}`, values);
    return c.json({ success: true });
  } catch (err: any) { return c.json({ error: "Update failed", detail: err.message }, 500); }
});

app.get("/api/referrals/circle/:code", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ count: 0 }); const memberIds = ci.members.map(m => m.userId); const result = await pool.query("SELECT COUNT(*) as count FROM referrals WHERE referrer_user_id=$1 AND referred_user_id = ANY($2) AND status='confirmed'", [u.id, memberIds]); return c.json({ count: parseInt(result.rows[0]?.count || "0") }); });
app.post("/api/referrals/invite-batch", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { invites } = await c.req.json();
  if (!Array.isArray(invites) || invites.length === 0) return c.json({ error: "Please add at least one valid email address." }, 400);
  if (invites.length > 5) return c.json({ error: "Maximum 5 invites at a time." }, 422);
  const recent = await pool.query("SELECT COUNT(*) as count FROM invite_emails WHERE referrer_user_id=$1 AND created_at > NOW() - INTERVAL '24 hours'", [u.id]);
  if (parseInt(recent.rows[0]?.count || "0") >= 5) return c.json({ error: "You've reached the invite limit for today. Try again tomorrow." }, 429);
  let codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]);
  if (!codeResult.rows[0]) { const code = generateReferralCode(u.name || "PRAY"); await pool.query("INSERT INTO referral_codes (user_id, code) VALUES ($1, $2) ON CONFLICT DO NOTHING", [u.id, code]); codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]); }
  const referralCode = codeResult.rows[0]?.code || ""; const referralLink = `https://pramen.app/ref/${referralCode}`; const referrerName = u.name || "A friend";
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  let sent = 0; const failed: { email: string; reason: string }[] = []; const alreadyMembers: { email: string }[] = [];
  for (const inv of invites) {
    const name = (inv.name || "").trim(); const email = (inv.email || "").trim().toLowerCase();
    if (!name || name.length < 2 || !emailRegex.test(email)) { failed.push({ email: email || "invalid", reason: "Invalid name or email" }); continue; }
    const existing = await pool.query("SELECT id FROM users WHERE lower(trim(email))=lower(trim($1))", [email]);
    if (existing.rows.length > 0) { alreadyMembers.push({ email }); continue; }
    if (RESEND_API_KEY) {
      try {
        const htmlBody = `<div style="font-family: -apple-system, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px 16px; color: #2C1810;"><p style="font-size: 18px; font-weight: 600; color: #C0735A;">prAmen</p><p>Hi ${name},</p><p>${referrerName} wants to pray alongside you.</p><p>They invited you to join <strong>prAmen</strong> — a daily prayer app that helps Christians build a simple, meaningful prayer habit.</p><p>As their guest, you get <strong>30 days of premium free</strong>.</p><p style="text-align: center; margin: 28px 0;"><a href="${referralLink}" style="display: inline-block; background: #C0735A; color: #fff; text-decoration: none; padding: 14px 32px; border-radius: 24px; font-weight: 600; font-size: 16px;">Join prAmen</a></p><p style="color: #9E7E6E; font-size: 14px;">The offer is waiting for you. No pressure.</p><p style="color: #9E7E6E; font-size: 14px;">— The prAmen team</p><hr style="border: none; border-top: 1px solid #E0D4C4; margin: 24px 0;"><p style="color: #9E7E6E; font-size: 11px;">You received this because ${referrerName} invited you. <a href="https://pramen.app" style="color: #9E7E6E;">Unsubscribe</a></p></div>`;
        const res = await fetch("https://api.resend.com/emails", { method: "POST", headers: { Authorization: `Bearer ${RESEND_API_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ from: RESEND_FROM_EMAIL, to: email, subject: `${referrerName} is praying for you 🙏 — Join them on prAmen`, html: htmlBody, text: `Hi ${name},\n\n${referrerName} wants to pray alongside you. They invited you to join prAmen.\n\nJoin here: ${referralLink}\n\n— The prAmen team` }) });
        if (res.ok) { await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'sent',$4)", [u.id, name, email, referralCode]); await pool.query("INSERT INTO referrals (referrer_user_id, referred_email, status) VALUES ($1,$2,'pending') ON CONFLICT DO NOTHING", [u.id, email]); sent++; }
        else { const errText = await res.text().catch(() => ""); console.error("[Invite] Resend error:", res.status, errText.substring(0, 200)); failed.push({ email, reason: "Delivery failed" }); await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'failed',$4)", [u.id, name, email, referralCode]); }
      } catch (err: any) { console.error("[Invite] Send error:", err.message); failed.push({ email, reason: "Delivery failed" }); }
    } else { await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'stored',$4)", [u.id, name, email, referralCode]); await pool.query("INSERT INTO referrals (referrer_user_id, referred_email, status) VALUES ($1,$2,'pending') ON CONFLICT DO NOTHING", [u.id, email]); sent++; }
  }
  trackEvent(u.id, "onboarding_invites_sent", { sent, failed: failed.length, already_members: alreadyMembers.length });
  return c.json({ sent, failed, alreadyMembers });
});


// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLE ACTIVITY FEED ───────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
// v5.15.0 — Personal engagement score
function computeEngagementTier(actionCount: number): string {
  if (actionCount >= 16) return "pillar";
  if (actionCount >= 6) return "devoted";
  if (actionCount >= 1) return "faithful";
  return "none";
}

app.get("/api/user/engagement-score", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const userId = u.id;
  const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString().split("T")[0];
  try {
    // Get all engagement actions for this user in the last 7 days
    const r = await pool.query(
      "SELECT day, action_count, actions FROM circle_engagement WHERE user_id=$1 AND day >= $2::date ORDER BY day DESC",
      [userId, sevenDaysAgo]
    );
    let totalActions = 0;
    let dailyPrayers = 0, prayedForRequests = 0, sentRequests = 0, sentNudges = 0;
    for (const row of r.rows) {
      totalActions += row.action_count || 0;
      const actions = row.actions || [];
      for (const a of actions) {
        if (a.type === "prayed_daily_prayer") dailyPrayers++;
        else if (a.type === "prayed_for_request") prayedForRequests++;
        else if (a.type === "created_request") sentRequests++;
        else if (a.type === "sent_nudge") sentNudges++;
      }
    }
    const tier = computeEngagementTier(totalActions);
    const nextTierAt = tier === "pillar" ? null : tier === "devoted" ? 16 : tier === "faithful" ? 6 : 1;
    return c.json({
      totalActions, tier, nextTierAt,
      breakdown: { dailyPrayers, prayedForRequests, sentRequests, sentNudges },
      daysActive: r.rows.length
    });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// v5.15.0 — Daily circle prayer endpoints
app.get("/api/circles/:code/daily-prayer", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  const today = new Date().toISOString().split("T")[0];
  // L10n: text in the REQUESTER's language (en fallback); prayed_by is shared
  // across languages and lives canonically on the en row.
  const reqLang = await getUserLanguage(u.id);
  const pick = `SELECT
      COALESCE(
        (SELECT prayer_text FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language=$3),
        (SELECT prayer_text FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language='en')
      ) AS prayer_text,
      (SELECT topic FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language='en') AS topic,
      (SELECT prayed_by FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language='en') AS prayed_by`;
  const r = await pool.query(pick, [code, today, reqLang]);
  if (!r.rows[0]?.prayer_text) {
    // Try generating on-the-fly if not yet generated
    await generateCircleDailyPrayers();
    const retry = await pool.query(pick, [code, today, reqLang]);
    if (!retry.rows[0]?.prayer_text) return c.json({ prayer: null });
    const row = retry.rows[0];
    const prayedBy = row.prayed_by || [];
    const userId = u.id;
    const hasPrayed = prayedBy.includes(userId);
    // Get names for prayed_by user IDs
    let prayedByNames: { userId: string; name: string }[] = [];
    if (prayedBy.length > 0) {
      try {
        const names = await pool.query("SELECT id, name FROM users WHERE id = ANY($1)", [prayedBy]);
        prayedByNames = names.rows.map((n: any) => ({ userId: n.id, name: n.name || "Someone" }));
      } catch {}
    }
    return c.json({ prayer: { text: row.prayer_text, topic: row.topic, prayedCount: prayedBy.length, hasPrayed, prayedBy: prayedByNames, totalMembers: ci.members.length } });
  }
  const row = r.rows[0];
  const prayedBy = row.prayed_by || [];
  const userId = u.id;
  const hasPrayed = prayedBy.includes(userId);
  let prayedByNames: { userId: string; name: string }[] = [];
  if (prayedBy.length > 0) {
    try {
      const names = await pool.query("SELECT id, name FROM users WHERE id = ANY($1)", [prayedBy]);
      prayedByNames = names.rows.map((n: any) => ({ userId: n.id, name: n.name || "Someone" }));
    } catch {}
  }
  return c.json({ prayer: { text: row.prayer_text, topic: row.topic, prayedCount: prayedBy.length, hasPrayed, prayedBy: prayedByNames, totalMembers: ci.members.length } });
});

app.post("/api/circles/:code/daily-prayer/pray", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  const today = new Date().toISOString().split("T")[0];
  const userId = u.id;
  // Add user to prayed_by array (rowCount tells us if this was a NEW pray)
  const upd = await pool.query(
    "UPDATE circle_daily_prayers SET prayed_by = array_append(prayed_by, $1) WHERE circle_code=$2 AND date=$3 AND NOT ($1 = ANY(prayed_by))",
    [userId, code, today]
  );
  // v5 Phase 4 — THE VOLLEY (daily): you prayed the same prayer the others
  // prayed today. Each of them feels it, by name. Real prayers only:
  // fires only when this user NEWLY prayed (rowCount > 0).
  if ((upd.rowCount || 0) > 0) {
    try {
      const prev = await pool.query("SELECT prayed_by FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language='en'", [code, today]);
      const all: string[] = prev.rows[0]?.prayed_by || [];
      const myName = ci.members.find(m => m.userId === userId)?.name || u.name || "Someone";
      for (const otherId of all) {
        if (otherId === userId) continue;
        fireVolley({ id: userId, name: myName }, otherId, "daily", null, code).catch(() => {});
      }
    } catch {}
  }
  // Record as circle engagement
  await recordCircleEngagement(code, userId, "prayed_daily_prayer");
  trackEvent(userId, "circle_daily_prayer_prayed", { circle_code: code, circle_name: ci.name });
  // Update streak — daily prayer counts as a prayer
  try {
    const todayDate = new Date().toISOString();
    const localDate = new Date().toISOString().split("T")[0];
    await pool.query(
      `UPDATE user_data SET last_prayed_date=$1, last_prayed_local_date=$2, updated_at=NOW(),
       streak_count = CASE
         WHEN last_prayed_date IS NULL THEN 1
         WHEN last_prayed_date::date = CURRENT_DATE THEN streak_count
         WHEN last_prayed_date::date = CURRENT_DATE - 1 THEN streak_count + 1
         ELSE 1
       END,
       highest_streak = GREATEST(highest_streak, CASE
         WHEN last_prayed_date IS NULL THEN 1
         WHEN last_prayed_date::date = CURRENT_DATE THEN streak_count
         WHEN last_prayed_date::date = CURRENT_DATE - 1 THEN streak_count + 1
         ELSE 1
       END)
       WHERE user_id=$3`,
      [todayDate, localDate, userId]
    );
  } catch {}
  // Get updated count
  const r = await pool.query("SELECT prayed_by FROM circle_daily_prayers WHERE circle_code=$1 AND date=$2 AND language='en'", [code, today]);
  const prayedBy = r.rows[0]?.prayed_by || [];
  // Check if tier changed — notify circle
  const activeCount = ci.members.filter(m => getMemberLastSeen(m).isActive).length;
  const tier = computeCircleTier(prayedBy.length, activeCount);
  return c.json({ success: true, prayedCount: prayedBy.length, tier });
});

// v5.15.0 — Circle engagement + health endpoint (hybrid prayer model)
app.get("/api/circles/:code/engagement", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code);
  if (!ci) return c.json({ error: "Not found" }, 404);
  const today = new Date().toISOString().split("T")[0];
  const engagedToday = await getCircleEngagementForDay(code, today);
  // Get last_seen_at for all members
  const memberIds = ci.members.map(m => m.userId).filter(Boolean);
  let lastSeenMap: Record<string, string> = {};
  try {
    const r = await pool.query("SELECT id, last_seen_at FROM users WHERE id = ANY($1)", [memberIds]);
    for (const row of r.rows) { if (row.last_seen_at) lastSeenMap[row.id] = row.last_seen_at; }
  } catch {}
  // Get 7-day engagement scores for all members
  const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString().split("T")[0];
  let memberEngagementScores: Record<string, number> = {};
  try {
    const eScores = await pool.query(
      "SELECT user_id, SUM(action_count) as total FROM circle_engagement WHERE user_id = ANY($1) AND day >= $2::date GROUP BY user_id",
      [memberIds, sevenDaysAgo]
    );
    for (const row of eScores.rows) { memberEngagementScores[row.user_id] = parseInt(row.total) || 0; }
  } catch {}
  const enrichedMembers = ci.members.map(m => {
    const lastSeen = lastSeenMap[m.userId] || m.lastPrayedDate || m.joinedAt;
    const daysSince = lastSeen ? Math.floor((Date.now() - new Date(lastSeen).getTime()) / 86400000) : 0;
    const isActive = daysSince <= 14;
    const engagementScore = memberEngagementScores[m.userId] || 0;
    return {
      userId: m.userId, name: m.name, streakCount: m.streakCount,
      lastPrayedDate: m.lastPrayedDate, lastPrayedLocalDate: m.lastPrayedLocalDate,
      lastPrayedTimezone: m.lastPrayedTimezone, joinedAt: m.joinedAt,
      role: m.role, avatarUrl: m.avatarUrl,
      prayedToday: prayedTodayInOwnTZ(m),
      engagedToday: engagedToday.has(m.userId) || prayedTodayInOwnTZ(m),
      isActive, daysSinceLastSeen: daysSince,
      engagementScore, engagementTier: computeEngagementTier(engagementScore)
    };
  });
  const activeMembers = enrichedMembers.filter(m => m.isActive);
  const engagedActive = activeMembers.filter(m => m.engagedToday).length;
  const healthPercent = activeMembers.length > 0 ? Math.round((engagedActive / activeMembers.length) * 100) : 0;
  const tier = computeCircleTier(engagedActive, activeMembers.length);
  const consecutiveGoldDays = tier === "gold" ? await computeConsecutiveGoldDays(code, ci) : 0;
  return c.json({
    members: enrichedMembers,
    circleHealth: {
      activeMembers: activeMembers.length, totalMembers: ci.members.length,
      engagedToday: engagedActive, healthPercent, tier,
      consecutiveGoldDays, wePrayedTogetherBadge: consecutiveGoldDays >= 7
    }
  });
});

// ═══════════════════════════════════════════════════════════════════
// ─── LIVE ROOM ENDPOINTS (v5 Phase 5) ───────────────────────────
// ═══════════════════════════════════════════════════════════════════
// LR2: the room is ALWAYS OPEN. "Live" = someone is actually inside.
// BRIGHT LINE: presence lists/counts derive only from participant rows of the
// active session; empty room = empty list; no seeding, no carryover.
app.get("/api/circles/:code/live", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  const live = await pool.query("SELECT id, anchor_type, intention_text, host_id, scheduled_for FROM live_prayer_sessions WHERE circle_code=$1 AND ended_at IS NULL AND status='live' ORDER BY scheduled_for DESC LIMIT 1", [code]);
  if (live.rows.length === 0) {
    return c.json({ status: "open", session: null, present: [], presentCount: 0, memberCount: ci.members.length });
  }
  const sess = live.rows[0];
  const present = await getSessionPresence(sess.id);
  if (present.length === 0) {
    // stale session (everyone vanished without leave) — close it honestly
    await pool.query("UPDATE live_prayer_sessions SET ended_at=NOW(), status='completed' WHERE id=$1", [sess.id]);
    return c.json({ status: "open", session: null, present: [], presentCount: 0, memberCount: ci.members.length });
  }
  return c.json({
    status: "occupied",
    session: { sessionId: sess.id, anchorType: sess.anchor_type || "daily", intention: sess.intention_text || "", hostId: sess.host_id || "", startedAt: sess.scheduled_for },
    present,
    presentCount: present.length,
    memberCount: ci.members.length
  });
});

// Enter the room (Start Sheet). Creates the session if empty, joins if
// occupied (joiners land mid-prayer — the arc never restarts).
app.post("/api/circles/:code/room/enter", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  const b = await c.req.json().catch(() => ({}));
  const anchorType = ["daily", "intention", "silent"].includes(b.anchorType) ? b.anchorType : "daily";
  const intention = (b.intention || "").toString().substring(0, 200);
  const notify = ["quiet", "circle", "members"].includes(b.notify) ? b.notify : "quiet";
  const memberIds: string[] = Array.isArray(b.memberIds) ? b.memberIds : [];

  // get-or-create the active session
  let sessionId: string;
  let anchorOut = anchorType;
  let intentionOut = intention;
  const live = await pool.query("SELECT id, anchor_type, intention_text FROM live_prayer_sessions WHERE circle_code=$1 AND ended_at IS NULL AND status='live' ORDER BY scheduled_for DESC LIMIT 1", [code]);
  if (live.rows.length > 0) {
    sessionId = live.rows[0].id;
    anchorOut = live.rows[0].anchor_type || "daily";   // joiner adopts host's anchor
    intentionOut = live.rows[0].intention_text || "";
  } else {
    const ins = await pool.query(
      "INSERT INTO live_prayer_sessions (circle_code, band_key, scheduled_for, window_end, status, anchor_type, intention_text, host_id) VALUES ($1,'open',NOW(),NOW() + INTERVAL '24 hours','live',$2,$3,$4) RETURNING id",
      [code, anchorType, intention || null, u.id]
    );
    sessionId = ins.rows[0].id;
  }
  const myName = ci.members.find(m => m.userId === u.id)?.name || u.name || "Someone";
  await pool.query(
    `INSERT INTO live_prayer_participants (session_id, user_id, user_name) VALUES ($1,$2,$3)
     ON CONFLICT (session_id, user_id) DO UPDATE SET last_seen=NOW(), left_at=NULL`,
    [sessionId, u.id, myName]
  );
  trackEvent(u.id, "room_entered", { circle_code: code, anchor_type: anchorOut, notify_choice: notify });

  // Notify (§5): rate-limited ONE per recipient per circle per 4h. Quiet = truly silent.
  if (notify !== "quiet") {
    const firstName = myName.split(" ")[0];
    const targets = notify === "members"
      ? ci.members.filter(m => memberIds.includes(m.userId) && m.userId !== u.id && !m.notificationsMuted)
      : ci.members.filter(m => m.userId !== u.id && !m.notificationsMuted);
    for (const m of targets) {
      const gate = await pool.query(
        `INSERT INTO room_notify_log (recipient_user_id, circle_code) VALUES ($1,$2)
         ON CONFLICT (recipient_user_id, circle_code)
         DO UPDATE SET notified_at=NOW() WHERE room_notify_log.notified_at < NOW() - INTERVAL '4 hours'
         RETURNING recipient_user_id`,
        [m.userId, code]
      );
      if (gate.rows.length === 0) {
        trackEvent(u.id, "room_notify_suppressed_ratelimit", { circle_code: code, recipient: m.userId });
        continue;
      }
      (async () => {
        const lang = await getUserLanguage(m.userId);
        await pushToUser(m.userId, { title: t(lang, "room_invite_live_title", { name: firstName, circle: ci.name }), body: t(lang, "room_invite_live_body", { name: firstName }), type: "room_invite_live", circleCode: code, circleName: ci.name, extra: { sessionId } });
      })().catch(() => {});
    }
  }
  await broadcastLivePresence(sessionId, code);
  return c.json({ sessionId, anchorType: anchorOut, intention: intentionOut });
});

app.post("/api/live/:sessionId/leave", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const sessionId = c.req.param("sessionId");
  await pool.query("UPDATE live_prayer_participants SET left_at=NOW() WHERE session_id=$1 AND user_id=$2", [sessionId, u.id]);
  const sess = await pool.query("SELECT circle_code FROM live_prayer_sessions WHERE id=$1", [sessionId]);
  const circleCode = sess.rows[0]?.circle_code || "";
  // Last person leaving closes the session — together computed from REAL Amens
  const remaining = await pool.query("SELECT COUNT(*)::int AS n FROM live_prayer_participants WHERE session_id=$1 AND left_at IS NULL", [sessionId]);
  if ((remaining.rows[0]?.n || 0) === 0) {
    const parts = await pool.query("SELECT user_id, prayed_at FROM live_prayer_participants WHERE session_id=$1", [sessionId]);
    const prayed = parts.rows.filter((p: any) => p.prayed_at);
    await pool.query("UPDATE live_prayer_sessions SET ended_at=NOW(), status='completed', together=$1, present_count=$2 WHERE id=$3 AND ended_at IS NULL", [prayed.length >= 2, parts.rows.length, sessionId]);
    if (prayed.length >= 2) {
      for (const p of prayed) { trackEvent(p.user_id, "together_prayer", { context: "live_room", partner_count: prayed.length - 1 }); }
    }
  }
  if (circleCode) { await broadcastLivePresence(sessionId, circleCode); }
  return c.json({ ok: true });
});

app.post("/api/live/:sessionId/heartbeat", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const sessionId = c.req.param("sessionId");
  const b = await c.req.json().catch(() => ({}));
  const praying = !!b.praying;
  const r = await pool.query(
    "UPDATE live_prayer_participants SET last_seen=NOW(), praying=$1 WHERE session_id=$2 AND user_id=$3 RETURNING (SELECT circle_code FROM live_prayer_sessions WHERE id=$2) AS circle_code",
    [praying, sessionId, u.id]
  );
  if (r.rows.length === 0) return c.json({ error: "Not in session" }, 404);
  if (b.statusChanged === true) { await broadcastLivePresence(sessionId, r.rows[0].circle_code); }
  return c.json({ ok: true });
});

// Amen inside the room. together flips ONLY when >=2 REAL participants prayed.
app.post("/api/live/:sessionId/prayed", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const sessionId = c.req.param("sessionId");
  const r = await pool.query(
    "UPDATE live_prayer_participants SET prayed_at=NOW(), praying=false, last_seen=NOW() WHERE session_id=$1 AND user_id=$2 RETURNING session_id",
    [sessionId, u.id]
  );
  if (r.rows.length === 0) return c.json({ error: "Not in session" }, 404);
  const sess = await pool.query("SELECT circle_code FROM live_prayer_sessions WHERE id=$1", [sessionId]);
  const circleCode = sess.rows[0]?.circle_code || "";
  const prayedCount = await pool.query("SELECT COUNT(DISTINCT user_id)::int AS n FROM live_prayer_participants WHERE session_id=$1 AND prayed_at IS NOT NULL", [sessionId]);
  const n = prayedCount.rows[0]?.n || 0;
  if (n >= 2) { await pool.query("UPDATE live_prayer_sessions SET together=true WHERE id=$1", [sessionId]); }
  trackEvent(u.id, "room_prayed", { circle_code: circleCode, session_id: sessionId, prayed_count: n });
  await broadcastLivePresence(sessionId, circleCode);
  return c.json({ ok: true, together: n >= 2, prayedCount: n });
});

// ── Gatherings (§4): member posts, join for reminders ──────────────
app.post("/api/circles/:code/gatherings", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  const b = await c.req.json();
  const atTime = new Date(b.atTime || "");
  if (isNaN(atTime.getTime()) || atTime.getTime() < Date.now()) return c.json({ error: "Time must be in the future" }, 400);
  const myName = ci.members.find(m => m.userId === u.id)?.name || u.name || "Someone";
  const intention = (b.intention || "").toString().substring(0, 200) || null;
  const ins = await pool.query(
    "INSERT INTO gathering_posts (circle_code, host_id, host_name, at_time, intention, joiners) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
    [code, u.id, myName, atTime.toISOString(), intention, [u.id]]
  );
  trackEvent(u.id, "gathering_posted", { circle_code: code });
  return c.json({ id: ins.rows[0].id });
});

app.post("/api/gatherings/:id/join", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const id = c.req.param("id");
  const r = await pool.query(
    "UPDATE gathering_posts SET joiners = array_append(joiners, $1) WHERE id=$2 AND NOT ($1 = ANY(joiners)) RETURNING circle_code",
    [u.id, id]
  );
  if (r.rows.length > 0) { trackEvent(u.id, "gathering_joined", { circle_code: r.rows[0].circle_code }); }
  return c.json({ ok: true });
});

app.get("/api/circles/:code/gatherings", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const code = c.req.param("code").toUpperCase();
  const r = await pool.query(
    "SELECT id, host_id, host_name, at_time, intention, joiners FROM gathering_posts WHERE circle_code=$1 AND at_time > NOW() - INTERVAL '30 minutes' ORDER BY at_time ASC LIMIT 10",
    [code]
  );
  const ci = getCircle(code);
  return c.json({ gatherings: r.rows.map((g: any) => ({
    id: g.id, hostId: g.host_id, hostName: g.host_name || "Someone", atTime: g.at_time,
    intention: g.intention || "",
    joinerCount: (g.joiners || []).length,
    joined: (g.joiners || []).includes(u.id),
    joinerNames: (g.joiners || []).map((uid: string) => ci?.members.find(m => m.userId === uid)?.name || null).filter(Boolean)
  })) });
});

// ═══════════════════════════════════════════════════════════════════
// v5 Phase 6.1 — PRAYER PARTNER (the viral engine, invite-only)
// One partner per user. The shared streak advances ONLY when both prayed
// that day (any prayer counts). Grace: one save of a missed day, framed
// warmly. Codes ride the P6.0 spine: "PR" prefix, text survives installs.
// ═══════════════════════════════════════════════════════════════════

async function userPrayedOn(userId: string, dateISO: string): Promise<boolean> {
  // any prayer counts: user_data last-prayed OR circle engagement that day
  try {
    const ud = await pool.query("SELECT 1 FROM user_data WHERE user_id=$1 AND (last_prayed_date::date = $2::date OR last_prayed_local_date = $2)", [userId, dateISO]);
    if (ud.rows.length > 0) return true;
    const eng = await pool.query("SELECT 1 FROM circle_engagement WHERE user_id=$1 AND day=$2 LIMIT 1", [userId, dateISO]);
    return eng.rows.length > 0;
  } catch { return false; }
}

// Reconcile one partnership through yesterday: advance when both prayed,
// consume ONE grace when exactly one missed, reset otherwise. Idempotent.
async function reconcilePartnership(p: any): Promise<any> {
  if (p.status !== "active" || !p.user_b) return p;
  const today = new Date().toISOString().split("T")[0];
  const yesterday = new Date(Date.now() - 86400000).toISOString().split("T")[0];
  let { shared_streak, last_advanced_date, grace_a, grace_b } = p;
  const lastAdv = last_advanced_date ? new Date(last_advanced_date).toISOString().split("T")[0] : null;

  // settle yesterday if unsettled
  if (lastAdv !== today && lastAdv !== yesterday && shared_streak > 0) {
    const aPrayed = await userPrayedOn(p.user_a, yesterday);
    const bPrayed = await userPrayedOn(p.user_b, yesterday);
    if (aPrayed && bPrayed) {
      shared_streak += 1; last_advanced_date = yesterday;
    } else if (aPrayed && !bPrayed && grace_b > 0) {
      // A prayed, B missed — B's grace covers the day. Warm framing, both told.
      grace_b -= 1; shared_streak += 1; last_advanced_date = yesterday;
      notifyGraceCoveredReal(p.user_a, p.user_b, yesterday).catch(() => {});
    } else if (!aPrayed && bPrayed && grace_a > 0) {
      grace_a -= 1; shared_streak += 1; last_advanced_date = yesterday;
      notifyGraceCoveredReal(p.user_b, p.user_a, yesterday).catch(() => {});
    } else {
      shared_streak = 0;
    }
  }
  // advance for today when both have prayed
  if (lastAdv !== today) {
    const aToday = await userPrayedOn(p.user_a, today);
    const bToday = await userPrayedOn(p.user_b, today);
    if (aToday && bToday) { shared_streak += 1; last_advanced_date = today; }
  }
  if (shared_streak !== p.shared_streak || String(last_advanced_date) !== String(p.last_advanced_date) || grace_a !== p.grace_a || grace_b !== p.grace_b) {
    await pool.query("UPDATE partnerships SET shared_streak=$1, last_advanced_date=$2, grace_a=$3, grace_b=$4 WHERE id=$5", [shared_streak, last_advanced_date, grace_a, grace_b, p.id]);
  }
  return { ...p, shared_streak, last_advanced_date, grace_a, grace_b };
}

// Warm grace push: "Alex covered your Tuesday."
async function notifyGraceCoveredReal(coveredById: string, coveredUserId: string, day: string): Promise<void> {
  try {
    const byName = (await pool.query("SELECT name FROM users WHERE id=$1", [coveredById])).rows[0]?.name || "Your partner";
    const first = byName.split(" ")[0];
    const lang = await getUserLanguage(coveredUserId);
    // Weekday rendered in the RECIPIENT's language (cross-cutting flag #C)
    const localeMap: Record<string, string> = { en: "en-US", fr: "fr-FR", es: "es-419", pt: "pt-BR" };
    const dayName = new Date(day + "T12:00:00Z").toLocaleDateString(localeMap[lang] || "en-US", { weekday: "long" });
    await pushToUser(coveredUserId, { title: t(lang, "partner_grace_title", { name: first, weekday: dayName }), body: t(lang, "partner_grace_body", {}), type: "partner_grace_used", circleCode: "", circleName: "", extra: {} });
    trackEvent(coveredUserId, "grace_used", { covered_by: coveredById });
  } catch {}
}

function partnerCode(): string {
  const chars = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
  let c = "PR";
  for (let i = 0; i < 4; i++) c += chars[Math.floor(Math.random() * chars.length)];
  return c;
}

async function grantPartnerPremium(userId: string): Promise<void> {
  if (!REVENUECAT_SECRET_KEY) return;
  try {
    const r = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(userId)}/entitlements/premium/promotional`, {
      method: "POST",
      headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ duration: "monthly" })
    });
    if (!r.ok) console.error(`[Partner] RC grant failed ${r.status} for ${userId.substring(0,8)}`);
  } catch (err: any) { console.error("[Partner] RC grant error:", err.message); }
}

app.post("/api/partner/invite", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const existing = await pool.query("SELECT id, status FROM partnerships WHERE (user_a=$1 OR user_b=$1) AND status='active'", [u.id]);
  if (existing.rows.length > 0) return c.json({ error: "You already have a prayer partner." }, 409);
  // reuse a pending invite if one exists
  const pending = await pool.query("SELECT invite_code FROM partnerships WHERE user_a=$1 AND status='pending'", [u.id]);
  if (pending.rows[0]) {
    trackEvent(u.id, "partner_invite_sent", { reused: true });
    return c.json({ code: pending.rows[0].invite_code, link: `https://pramen.app/join/${pending.rows[0].invite_code}` });
  }
  const code = partnerCode();
  await pool.query("INSERT INTO partnerships (user_a, invite_code) VALUES ($1,$2)", [u.id, code]);
  trackEvent(u.id, "partner_invite_sent", { reused: false });
  return c.json({ code, link: `https://pramen.app/join/${code}` });
});

app.post("/api/partner/accept", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const b = await c.req.json();
  const code = String(b.code || "").toUpperCase().trim();
  const p = await pool.query("SELECT * FROM partnerships WHERE invite_code=$1 AND status='pending'", [code]);
  if (!p.rows[0]) return c.json({ error: "This partner invite isn't valid anymore." }, 404);
  const row = p.rows[0];
  if (row.user_a === u.id) return c.json({ error: "You can't partner with yourself." }, 422);
  const mine = await pool.query("SELECT 1 FROM partnerships WHERE (user_a=$1 OR user_b=$1) AND status='active'", [u.id]);
  if (mine.rows.length > 0) return c.json({ error: "You already have a prayer partner." }, 409);
  const theirs = await pool.query("SELECT 1 FROM partnerships WHERE (user_a=$1 OR user_b=$1) AND status='active'", [row.user_a]);
  if (theirs.rows.length > 0) return c.json({ error: "They already have a prayer partner." }, 409);
  await pool.query("UPDATE partnerships SET user_b=$1, status='active', accepted_at=NOW() WHERE id=$2", [u.id, row.id]);
  trackEvent(u.id, "partner_invite_accepted", {});
  trackEvent(u.id, "invite_accepted", { via: "partner_code" });
  // Two-sided 30-day premium — server-side RC promotional entitlements,
  // same proven mechanism as the referral grant (3.1.1-safe: no client
  // purchase path involved, works with Purchases.logIn backend UUIDs).
  grantPartnerPremium(u.id).catch(() => {});
  grantPartnerPremium(row.user_a).catch(() => {});
  const myName = u.name || "Someone";
  (async () => {
    const lang = await getUserLanguage(row.user_a);
    await pushToUser(row.user_a, { title: t(lang, "partner_accepted_title", { name: myName.split(" ")[0] }), body: t(lang, "partner_accepted_body", {}), type: "partner_accepted", circleCode: "", circleName: "", extra: {} });
  })().catch(() => {});
  return c.json({ success: true });
});

app.get("/api/partner", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const p = await pool.query("SELECT * FROM partnerships WHERE (user_a=$1 OR user_b=$1) AND status IN ('pending','active') ORDER BY created_at DESC LIMIT 1", [u.id]);
  if (!p.rows[0]) return c.json({ partnership: null });
  let row = p.rows[0];
  if (row.status === "active") { row = await reconcilePartnership(row); }
  const partnerId = row.user_a === u.id ? row.user_b : row.user_a;
  let partnerName: string | null = null;
  let partnerPrayedToday = false;
  if (partnerId) {
    const pn = await pool.query("SELECT name FROM users WHERE id=$1", [partnerId]);
    partnerName = pn.rows[0]?.name || "Your partner";
    partnerPrayedToday = await userPrayedOn(partnerId, new Date().toISOString().split("T")[0]);
  }
  const myGrace = row.user_a === u.id ? row.grace_a : row.grace_b;
  const partnerGrace = row.user_a === u.id ? row.grace_b : row.grace_a;
  return c.json({ partnership: {
    id: row.id, status: row.status, inviteCode: row.invite_code,
    partnerName, partnerPrayedToday,
    sharedStreak: row.shared_streak || 0,
    myGrace, partnerGrace,
    iPrayedToday: await userPrayedOn(u.id, new Date().toISOString().split("T")[0])
  } });
});

// ═══════════════════════════════════════════════════════════════════
// ─── VOLLEY ENDPOINTS (v5 Phase 4) ──────────────────────────────
// ═══════════════════════════════════════════════════════════════════
// Pray-back: called by the client AFTER the user actually prayed (Amen
// completed) — never on tap. Closes the loop by name.
app.post("/api/volley/pray-back", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const b = await c.req.json();
  const volleyId = String(b.volleyId || "");
  const toUserId = String(b.toUserId || "");
  const circleCode = b.circleCode ? String(b.circleCode).toUpperCase() : null;
  if (!toUserId) return c.json({ error: "toUserId required" }, 400);
  // Verify the original volley targeted THIS user (bright line: only the
  // real recipient of a real prayer can close the loop)
  if (volleyId) {
    const orig = await pool.query("SELECT recipient_user_id, prayed_back FROM volley_events WHERE id=$1", [volleyId]);
    if (!orig.rows[0]) return c.json({ error: "Volley not found" }, 404);
    if (orig.rows[0].recipient_user_id !== u.id) return c.json({ error: "Not yours to close" }, 403);
    await pool.query("UPDATE volley_events SET prayed_back=true WHERE id=$1", [volleyId]);
  }
  const newId = await fireVolley({ id: u.id, name: u.name || "Someone" }, toUserId, "volley", null, circleCode);
  trackEvent(u.id, "volley_loop_closed", { circle_code: circleCode || "" });
  trackEvent(u.id, "together_prayer", { context: "volley", partner_count: 1 });
  return c.json({ success: true, volleyId: newId });
});

// Debug (dashboard-gated): did volleys fire, and were pushes stored?
app.get("/api/dashboard/volley-debug", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const volleys = await pool.query("SELECT id, by_user_id, by_name, recipient_user_id, context, request_id, circle_code, prayed_back, occurred_at FROM volley_events ORDER BY occurred_at DESC LIMIT 20");
  const pushes = await pool.query("SELECT user_id, type, title, body, data, created_at FROM notifications ORDER BY created_at DESC LIMIT 25");
  const tokens = await pool.query("SELECT id, name, (device_token IS NOT NULL) AS has_token, last_push_status, last_push_at, last_seen_at FROM users ORDER BY last_seen_at DESC NULLS LAST LIMIT 15");
  // Recent requests across circles w/ requester+target identity (diagnose targeting)
  const reqs: any[] = [];
  for (const [code, circle] of circles) {
    for (const r of circle.prayerRequests.slice(0, 3)) {
      reqs.push({ circle: code, id: r.id, requester: r.requesterName, requesterUserId: r.requesterUserId, targetUserId: r.targetUserId || null, targetType: r.targetType || "circle", text: r.text.substring(0, 40), at: r.timestamp });
    }
  }
  reqs.sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());
  // Members of circles w/ mute flags + ids (diagnose recipient resolution)
  const members: any[] = [];
  for (const [code, circle] of circles) {
    for (const m of circle.members) {
      members.push({ circle: code, name: m.name, userId: m.userId, muted: !!m.notificationsMuted });
    }
  }
  return c.json({ volleys: volleys.rows, pushes: pushes.rows, recentUsers: tokens.rows, recentRequests: reqs.slice(0, 12), members: members.slice(0, 60) });
});

// Pending volleys for the Today feed card: real, recent, not yet prayed back
app.get("/api/volley/pending", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Unauthorized" }, 401);
  const r = await pool.query(
    "SELECT id, by_user_id, by_name, context, request_id, circle_code, occurred_at FROM volley_events WHERE recipient_user_id=$1 AND prayed_back=false AND occurred_at > NOW() - INTERVAL '48 hours' ORDER BY occurred_at DESC LIMIT 10",
    [u.id]
  );
  return c.json({ volleys: r.rows.map((row: any) => ({ id: row.id, byUserId: row.by_user_id, byName: row.by_name, context: row.context, requestId: row.request_id || "", circleCode: row.circle_code || "", occurredAt: row.occurred_at })) });
});

app.get("/api/circles/:code/activity", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const code = c.req.param("code").toUpperCase(); const ci = getCircle(code);
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  const limit = Math.min(parseInt(c.req.query("limit") || "50"), 100);
  const activities: any[] = [];
  // Get member avatars
  const memberIds = ci.members.map(m => m.userId).filter(Boolean);
  let avatarMap: Record<string, string | null> = {};
  try { const av = await pool.query("SELECT id, device_user_id, avatar_url FROM users WHERE id = ANY($1) OR device_user_id = ANY($1)", [memberIds]); for (const r of av.rows) { avatarMap[r.id] = r.avatar_url; if (r.device_user_id) avatarMap[r.device_user_id] = r.avatar_url; } } catch {}
  // v5.15.6 — community circles: hide non-subscribers AND inactive 14+ days in feed
  const visibleMemberIds = new Set(
    isCommunityCircle(ci.code) ? ci.members.filter(m => (m.visible !== false || m.userId === ci.creatorUserId) && (isCommunityMemberActive(m) || m.userId === ci.creatorUserId)).map(m => m.userId) : ci.members.map(m => m.userId)
  );
  // Recent prayers (from member status updates)
  for (const m of ci.members) {
    if (!visibleMemberIds.has(m.userId)) continue;
    if (m.lastPrayedDate) {
      const prayedAt = new Date(m.lastPrayedDate);
      const now = new Date();
      const hoursDiff = (now.getTime() - prayedAt.getTime()) / (1000 * 60 * 60);
      if (hoursDiff <= 48) {
        activities.push({ id: `prayer-${m.userId}-${m.lastPrayedDate}`, memberName: m.name, memberAvatarUrl: avatarMap[m.userId] || null, action: "prayed", detail: null, timestamp: m.lastPrayedDate });
      }
    }
  }
  // Recent prayer requests (filter personal requests to only show to target)
  const userId = c.req.query("userId") || u.id;
  for (const req of ci.prayerRequests.slice(0, 10)) {
    // Skip personal requests not meant for this user (unless they're the requester)
    if (req.targetType === "personal" && req.targetUserId && req.targetUserId !== userId && req.requesterUserId !== userId) continue;
    activities.push({ id: `request-${req.id}`, memberName: req.requesterName, memberAvatarUrl: avatarMap[req.requesterUserId] || null, action: req.targetType === "personal" ? "prayer_request_personal" : "prayer_request", detail: req.text.length > 80 ? req.text.substring(0, 80) + "..." : req.text, timestamp: req.timestamp, targetUserId: req.targetUserId || null, status: req.status || "active", prayedCount: req.prayedByUserIds.length });
    // "Prayed for" activity (only visible members)
    for (const uid of req.prayedByUserIds) {
      if (!visibleMemberIds.has(uid)) continue;
      const prayerMember = ci.members.find(m => m.userId === uid);
      if (prayerMember) {
        activities.push({ id: `prayed-for-${req.id}-${uid}`, memberName: prayerMember.name, memberAvatarUrl: avatarMap[uid] || null, action: "prayed_for_request", detail: req.requesterName, timestamp: req.timestamp });
      }
    }
  }
  // v5 Phase 4 — volley entries: real prayed-with moments in this circle
  try {
    const vr = await pool.query(
      "SELECT v.id, v.by_name, v.recipient_user_id, v.occurred_at FROM volley_events v WHERE v.circle_code=$1 AND v.occurred_at > NOW() - INTERVAL '7 days' ORDER BY v.occurred_at DESC LIMIT 15",
      [code]
    );
    for (const row of vr.rows) {
      const recipName = ci.members.find(m => m.userId === row.recipient_user_id)?.name || "someone";
      activities.push({ id: `volley-${row.id}`, memberName: row.by_name, memberAvatarUrl: null, action: "prayed_with", detail: recipName, timestamp: row.occurred_at });
    }
  } catch {}
  // Sort by timestamp descending
  activities.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
  return c.json({ activities: activities.slice(0, limit) });
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLE STREAK ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
function computeCircleStreak(circle: StoredCircle): number {
  if (circle.members.length === 0) return 0;
  const today = new Date().toISOString().split("T")[0];
  const yesterday = new Date(Date.now() - 86400000).toISOString().split("T")[0];
  // For a circle streak to be valid, EVERY member must currently have an active
  // individual streak (prayed today, or prayed yesterday with today not yet counted).
  // The circle streak equals the minimum of all members' individual streak counts,
  // since the group chain is only as strong as its weakest link.
  let minStreak = Infinity;
  for (const m of circle.members) {
    if (!m.lastPrayedDate) return 0;
    const memberDate = new Date(m.lastPrayedDate).toISOString().split("T")[0];
    const streakActive = memberDate === today || memberDate === yesterday;
    if (!streakActive) return 0;
    const memberStreak = m.streakCount || 0;
    if (memberStreak < minStreak) minStreak = memberStreak;
  }
  return minStreak === Infinity ? 0 : minStreak;
}

app.get("/api/circles/:code/streak", async (c) => {
  const ci = getCircle(c.req.param("code"));
  if (!ci) return c.json({ error: "Not found" }, 404);
  const streak = computeCircleStreak(ci);
  const prayedToday = ci.members.filter(m => prayedTodayInOwnTZ(m));
  return c.json({ circleStreak: streak, prayedToday: prayedToday.length, totalMembers: ci.members.length, allPrayedToday: prayedToday.length === ci.members.length });
});

// ═══════════════════════════════════════════════════════════════════
// v5 Phase 5 — LIVE ROOM (the flagship)
// Presence is REAL people only: every row in live_prayer_participants comes
// from an authenticated join by a live client. Counts are computed, never set.
// ═══════════════════════════════════════════════════════════════════

function timeNowInTz(tz: string): string {
  try {
    return new Intl.DateTimeFormat("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date());
  } catch { return "??:??"; }
}

const PRESENCE_STALE_MS = 30 * 1000; // heartbeat every ~10s; 3 misses = gone

type LivePresence = { userId: string; name: string; status: "here" | "praying" | "prayed" };

async function getSessionPresence(sessionId: string): Promise<LivePresence[]> {
  const r = await pool.query(
    "SELECT user_id, user_name, praying, prayed_at, last_seen, left_at FROM live_prayer_participants WHERE session_id=$1 ORDER BY joined_at ASC",
    [sessionId]
  );
  const now = Date.now();
  const present: LivePresence[] = [];
  for (const row of r.rows) {
    if (row.left_at) continue;
    const stale = now - new Date(row.last_seen).getTime() > PRESENCE_STALE_MS;
    if (row.prayed_at) {
      present.push({ userId: row.user_id, name: row.user_name || "Someone", status: "prayed" });
    } else if (!stale) {
      present.push({ userId: row.user_id, name: row.user_name || "Someone", status: row.praying ? "praying" : "here" });
    }
    // stale + never prayed = silently gone (honest counts)
  }
  return present;
}

async function broadcastLivePresence(sessionId: string, circleCode: string): Promise<void> {
  const ci = getCircle(circleCode); if (!ci) return;
  const present = await getSessionPresence(sessionId);
  const event = { type: "live_presence_updated", sessionId, circleCode, present, presentCount: present.length };
  for (const m of ci.members) { sendSseToUser(m.userId, event).catch(() => {}); }
}

// LR2 §4: 1-minute tick fires gathering reminders to EXPLICIT JOINERS only
// (T-10 and T-0). No band scheduling exists anymore — rooms open on entry.
async function gatheringReminderTick(): Promise<void> {
  try {
    const soon = await pool.query(
      "SELECT id, circle_code, host_id, host_name, at_time, intention, joiners, reminded_10, reminded_0 FROM gathering_posts WHERE at_time > NOW() - INTERVAL '30 minutes' AND at_time < NOW() + INTERVAL '11 minutes' AND (reminded_10=false OR reminded_0=false)"
    );
    for (const g of soon.rows) {
      const ci = getCircle(g.circle_code); if (!ci) continue;
      const hostFirst = (g.host_name || "Someone").split(" ")[0];
      const minsAway = (new Date(g.at_time).getTime() - Date.now()) / 60000;
      const recipients: string[] = [...(g.joiners || [])];
      if (!g.reminded_10 && minsAway <= 10 && minsAway > 0) {
        await pool.query("UPDATE gathering_posts SET reminded_10=true WHERE id=$1 AND reminded_10=false", [g.id]);
        for (const uid of recipients) {
          (async () => {
            const lang = await getUserLanguage(uid);
            await pushToUser(uid, { title: t(lang, "gathering_t10_title", { name: hostFirst }), body: t(lang, "gathering_t10_body", { circle: ci.name }), type: "gathering_reminder", circleCode: g.circle_code, circleName: ci.name, extra: { gatheringId: g.id } });
          })().catch(() => {});
        }
        trackEvent(g.host_id, "gathering_reminder_fired", { circle_code: g.circle_code, at: "t-10", joiner_count: recipients.length });
      }
      if (!g.reminded_0 && minsAway <= 0) {
        await pool.query("UPDATE gathering_posts SET reminded_0=true WHERE id=$1 AND reminded_0=false", [g.id]);
        for (const uid of recipients) {
          (async () => {
            const lang = await getUserLanguage(uid);
            await pushToUser(uid, { title: t(lang, "gathering_t0_title", { name: hostFirst }), body: t(lang, "gathering_t0_body", {}), type: "gathering_reminder", circleCode: g.circle_code, circleName: ci.name, extra: { gatheringId: g.id, starting: true } });
          })().catch(() => {});
        }
        trackEvent(g.host_id, "gathering_reminder_fired", { circle_code: g.circle_code, at: "t-0", joiner_count: recipients.length });
      }
    }
  } catch (err: any) { console.error("[Gathering] tick error:", err.message); }
}

// ═══════════════════════════════════════════════════════════════════
// ─── LOOPS EVENT HELPER (module-level for access from scheduled functions) ──
// ═══════════════════════════════════════════════════════════════════
async function sendLoopsEvent(email: string, eventName: string, properties?: Record<string, any>): Promise<void> {
  const loopsKey = process.env.LOOPS_API_KEY || "";
  if (!loopsKey || !email) return;
  try {
    const res = await fetch("https://app.loops.so/api/v1/events/send", {
      method: "POST",
      headers: { Authorization: `Bearer ${loopsKey}`, "Content-Type": "application/json" },
      body: JSON.stringify({ email, eventName, ...(properties || {}) })
    });
    if (res.ok) console.log(`[Loops] Event '${eventName}' sent to ${email}`);
    else console.error(`[Loops] Event '${eventName}' failed: ${res.status}`);
  } catch (err: any) { console.error(`[Loops] Event error: ${err.message}`); }
}

// ═══════════════════════════════════════════════════════════════════
// ─── STREAK AT RISK PUSH (scheduled) ────────────────────────────
// ═══════════════════════════════════════════════════════════════════
async function checkStreakAtRisk(): Promise<void> {
  try {
    const today = new Date().toISOString().split("T")[0];
    // Find users who have a streak > 0 but haven't prayed today (UTC check)
    const result = await pool.query(
      "SELECT ud.user_id, ud.streak_count, ud.last_prayed_date, ud.last_prayed_local_date, ud.last_prayed_timezone FROM user_data ud WHERE ud.streak_count > 0 AND (ud.last_prayed_date IS NULL OR ud.last_prayed_date::date < $1::date)",
      [today]
    );
    let sent = 0;
    for (const row of result.rows) {
      if (row.streak_count >= 3) {
        // Check 1: did they pray today per their own timezone (from user_data sync)?
        if (row.last_prayed_local_date && row.last_prayed_timezone) {
          const localToday = todayInTimezone(row.last_prayed_timezone);
          if (row.last_prayed_local_date === localToday) continue;
        }
        // Check 2: did they pray today in their own timezone via circle data?
        let prayedInLocalTZ = false;
        for (const [, circle] of circles) {
          const member = circle.members.find(m => m.userId === row.user_id);
          if (member && prayedTodayInOwnTZ(member)) {
            prayedInLocalTZ = true;
            break;
          }
        }
        if (prayedInLocalTZ) continue; // Skip — they DID pray today in their timezone
        // Check 3: did they have any circle engagement today? (catches daily prayer pray)
        try {
          const engCheck = await pool.query("SELECT 1 FROM circle_engagement WHERE user_id=$1 AND day=$2 LIMIT 1", [row.user_id, today]);
          if (engCheck.rows.length > 0) continue;
        } catch {}
        // Check 4: re-read user_data to catch race with status PUT sync (belt-and-suspenders)
        try {
          const freshCheck = await pool.query("SELECT last_prayed_date FROM user_data WHERE user_id=$1 AND last_prayed_date::date >= $2::date", [row.user_id, today]);
          if (freshCheck.rows.length > 0) continue;
        } catch {}
        pushToUserLocalized(row.user_id, {
          titleKey: "streak_at_risk_title",
          titleParams: { count: row.streak_count },
          bodyKey: "streak_at_risk_body",
          type: "streak_at_risk"
        });
        trackEvent(row.user_id, "streak_at_risk_push", { streak_count: row.streak_count });
        // v5.15.1 — fire Loops event for streak-at-risk email
        try {
          const emailRow = await pool.query("SELECT email FROM users WHERE id=$1 AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com'", [row.user_id]);
          if (emailRow.rows[0]?.email) sendLoopsEvent(emailRow.rows[0].email, "streak_at_risk_24h", { streakCount: row.streak_count });
        } catch {}
        sent++;
      }
    }
    console.log(`[Streak] Checked ${result.rows.length} at risk, sent ${sent} nudges`);
    // v5.15.3 — streak decay: reset streaks in user_data for users who missed 2+ days
    try {
      const decayResult = await pool.query(
        `UPDATE user_data SET streak_count = 0 WHERE streak_count > 0 AND last_prayed_date IS NOT NULL AND last_prayed_date::date < (CURRENT_DATE - INTERVAL '1 day')::date RETURNING user_id, streak_count`
      );
      if (decayResult.rows.length > 0) {
        console.log(`[Streak] Decayed ${decayResult.rows.length} stale streaks in user_data`);
        // Also reset streaks in circle member data
        for (const row of decayResult.rows) {
          for (const [, circle] of circles) {
            const member = circle.members.find(m => m.userId === row.user_id);
            if (member && member.streakCount > 0) {
              member.streakCount = 0;
              saveCircleToDb(circle).catch(() => {});
            }
          }
        }
      }
    } catch (decayErr: any) { console.error("[Streak] Decay error:", decayErr.message); }
  } catch (err: any) { console.error("[Streak] At-risk check error:", err.message); }
}

// ─── Last One Standing detection (called after prayer mark) ──────
async function checkLastOneStanding(circle: StoredCircle, prayerUserId: string): Promise<void> {
  // v5.8.1 — use prayedTodayInOwnTZ so the "last one standing" check respects each member's own local day.
  // Previously used UTC date comparison, which caused false-positive pushes for users in timezones
  // where their local "today" prayer mapped to yesterday's UTC date.
  const notPrayedToday = circle.members.filter(m => {
    if (m.userId === prayerUserId) return false;
    return !prayedTodayInOwnTZ(m);
  });
  if (notPrayedToday.length === 1) {
    // One person left - send them a gentle nudge
    const lastOne = notPrayedToday[0];
    // v5.8.3 — throttle: at most one last-one-standing push per (user, circle) per calendar day in the user's own TZ.
    // Belt-and-braces safety net so even if an upstream bug re-triggers this function, the recipient cannot be spammed.
    const tz = lastOne.lastPrayedTimezone || "UTC";
    const userToday = todayInTimezone(tz);
    const throttleKey = `last_one_${lastOne.userId}_${circle.code}`;
    try {
      const existing = await pool.query("SELECT sent_date FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
      if (existing.rows[0]?.sent_date === userToday) {
        return;
      }
      await pool.query(
        "INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1,$2) ON CONFLICT (throttle_key) DO UPDATE SET sent_date=$2",
        [throttleKey, userToday]
      );
    } catch (err) {
      // if throttle table not ready, fall through to send (conservative first-time behavior)
    }
    pushToUserLocalized(lastOne.userId, {
      titleKey: "last_one_standing_title",
      titleParams: { circle: circle.name },
      bodyKey: "last_one_standing_body",
      type: "last_one_standing",
      circleCode: circle.code,
      circleName: circle.name
    });
    trackEvent(lastOne.userId, "last_one_standing_nudge", { circle_code: circle.code });
  }
  if (notPrayedToday.length === 0) {
    // Everyone prayed! Celebrate
    trackEvent(prayerUserId, "circle_all_prayed", { circle_code: circle.code, member_count: circle.members.length });
  }
}

// ═══════════════════════════════════════════════════════════════════
// ─── POSTHOG EVENTS ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/dashboard/events", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  if (!POSTHOG_PERSONAL_KEY) return c.json({ error: "POSTHOG_PERSONAL_API_KEY not set" }, 500);
  try {
    const limit = Math.min(parseInt(c.req.query("limit") || "1000"), 2000);
    const daysBack = parseInt(c.req.query("days") || "14");
    const res = await fetch(`${POSTHOG_READ_HOST}/api/projects/${POSTHOG_PROJECT_ID}/events/?limit=${limit}`, { headers: { Authorization: `Bearer ${POSTHOG_PERSONAL_KEY}` } });
    if (!res.ok) { const txt = await res.text().catch(() => ""); console.error("[PostHog Read]", res.status, txt.substring(0,200)); return c.json({ error: "PostHog " + res.status, detail: txt.substring(0,200) }, 500); }
    const raw = ((await res.json()) as any).results || [];
    const exclude = new Set(["samy_setup", "samy_test"]);
    const events = raw.filter((e: any) => !exclude.has(e.distinct_id)).map((e: any) => ({ event: e.event, timestamp: e.timestamp, user: e.distinct_id?.substring(0, 8) || "?", full_user_id: e.distinct_id, properties: { type: e.properties?.type, plan: e.properties?.plan, price: e.properties?.price, trigger: e.properties?.trigger, duration: e.properties?.duration_seconds, streak: e.properties?.streak_day, is_first_open: e.properties?.is_first_open, circle_code: e.properties?.circle_code, circle_name: e.properties?.circle_name, content_type: e.properties?.content_type, step_name: e.properties?.step_name, step_index: e.properties?.step_index, member_count: e.properties?.member_count, city: e.properties?.$geoip_city_name || e.properties?.$set?.$geoip_city_name, country: e.properties?.$geoip_country_name || e.properties?.$set?.$geoip_country_name } }));
    // v5.10.4 — merge PostHog users by DB cross-ref + timestamp-based matching
    const dbUsers = await pool.query("SELECT id, device_user_id, name, email FROM users").catch(() => ({ rows: [] }));
    const idToCanonical: Record<string, string> = {};
    const idToName: Record<string, string> = {};
    for (const row of dbUsers.rows) {
      const canonical = row.id;
      const name = row.name || "";
      if (row.id) { idToCanonical[row.id] = canonical; if (name) idToName[canonical] = name; }
      if (row.device_user_id) { idToCanonical[row.device_user_id] = canonical; }
    }
    // Phase 1: direct ID match from DB
    for (const e of events) { if (idToCanonical[e.full_user_id]) { e.full_user_id = idToCanonical[e.full_user_id]; } }
    // Phase 2: proximity-based merge — if a known auth user ID and an unknown device ID
    // have ANY events within 10 seconds of each other, they're the same person
    const knownAuthIds = new Set(dbUsers.rows.map((r: any) => r.id).filter(Boolean));
    // Group all event timestamps by user ID
    const userTimestamps: Record<string, number[]> = {};
    for (const e of events) {
      if (!userTimestamps[e.full_user_id]) userTimestamps[e.full_user_id] = [];
      userTimestamps[e.full_user_id].push(new Date(e.timestamp).getTime());
    }
    const mergeMap: Record<string, string> = {};
    const unknownIds = Object.keys(userTimestamps).filter(id => !knownAuthIds.has(id) && !idToCanonical[id]);
    const authIds = Object.keys(userTimestamps).filter(id => knownAuthIds.has(id));
    for (const unknownId of unknownIds) {
      const unknownTs = userTimestamps[unknownId];
      for (const authId of authIds) {
        if (mergeMap[unknownId]) break; // already merged
        const authTs = userTimestamps[authId];
        // Check if any events from both IDs are within 10 seconds
        for (const ut of unknownTs) {
          let matched = false;
          for (const at of authTs) {
            if (Math.abs(ut - at) < 10000) { matched = true; break; }
          }
          if (matched) { mergeMap[unknownId] = authId; break; }
        }
      }
    }
    // Apply merges
    for (const e of events) { if (mergeMap[e.full_user_id]) { e.full_user_id = mergeMap[e.full_user_id]; } }
    // Update display names
    for (const e of events) { e.user = (idToName[e.full_user_id] || e.full_user_id).substring(0, 16); }
    const uMap: Record<string, any> = {};
    for (const e of events) { if (!uMap[e.full_user_id]) uMap[e.full_user_id] = { id: e.user, full_id: e.full_user_id, name: idToName[e.full_user_id] || "", events: [], counts: {} as Record<string,number>, first_seen: e.timestamp, last_seen: e.timestamp, city: "", country: "", max_streak: 0, plan_taps: 0 }; const u = uMap[e.full_user_id]; u.events.push(e); u.counts[e.event] = (u.counts[e.event]||0)+1; if (e.timestamp < u.first_seen) u.first_seen = e.timestamp; if (e.timestamp > u.last_seen) u.last_seen = e.timestamp; if (e.properties.city) u.city = e.properties.city; if (e.properties.country) u.country = e.properties.country; if (e.properties.streak) { const s = parseInt(e.properties.streak); if (s > u.max_streak) u.max_streak = s; } if (e.event === "paywall_plan_selected") u.plan_taps++; }
    const users = Object.values(uMap).sort((a: any, b: any) => b.events.length - a.events.length);
    const ec: Record<string,number> = {}; for (const e of events) { if (e.event !== "$identify") ec[e.event] = (ec[e.event]||0)+1; }
    const fn = { first_open: new Set<string>(), onboarding: new Set<string>(), paywall: new Set<string>(), plan_tap: new Set<string>(), prayer: new Set<string>(), circle: new Set<string>(), signup: new Set<string>(), scripture: new Set<string>() };
    for (const e of events) { const u = e.full_user_id; if (e.properties.is_first_open === true || e.properties.is_first_open === "True") fn.first_open.add(u); if (e.event === "onboarding_completed") fn.onboarding.add(u); if (e.event === "paywall_viewed") fn.paywall.add(u); if (e.event === "paywall_plan_selected") fn.plan_tap.add(u); if (e.event === "prayer_logged") fn.prayer.add(u); if (e.event === "circle_created") fn.circle.add(u); if (e.event === "user_signed_up") fn.signup.add(u); if (e.event === "scripture_viewed") fn.scripture.add(u); }
    // v5.13.0 — onboarding step funnel with user names + conversion + cancellation funnel
    const stepOrder = ["welcome","language","circle_tutorial","topics","first_prayer_completed","first_prayer_skipped","sign_in","community_circles_joined","community_circles_skipped","community_circles_auto_joined","circle_created","circle_shared_code","invite_skipped","circle_shared_social","social_share_skipped","circle_share_skipped","reminders","paywall","converted"];
    const stepUsers: Record<string, { name: string, id: string }[]> = {};
    for (const s of stepOrder) stepUsers[s] = [];
    for (const e of events) {
      if (e.event === "onboarding_step_completed" && e.properties.step_name) {
        const sn = e.properties.step_name as string;
        if (stepUsers[sn] && !stepUsers[sn].find((u: any) => u.id === e.full_user_id)) {
          stepUsers[sn].push({ name: idToName[e.full_user_id] || e.full_user_id.substring(0,8), id: e.full_user_id });
        }
      }
      if (e.event === "paywall_viewed" && e.properties.trigger === "first") {
        if (!stepUsers["paywall"].find((u: any) => u.id === e.full_user_id)) {
          stepUsers["paywall"].push({ name: idToName[e.full_user_id] || e.full_user_id.substring(0,8), id: e.full_user_id });
        }
      }
      if (e.event === "subscription_started") {
        if (!stepUsers["converted"].find((u: any) => u.id === e.full_user_id)) {
          stepUsers["converted"].push({ name: idToName[e.full_user_id] || e.full_user_id.substring(0,8), id: e.full_user_id });
        }
      }
    }
    const onboardingFunnel = stepOrder.map(s => ({ step: s, count: stepUsers[s].length, users: stepUsers[s] }));

    // Cancellation funnel — for each cancelled user, trace what they did
    const cancelledUsers: { name: string, id: string, plan: string, actions: string[] }[] = [];
    for (const e of events) {
      if (e.event === "subscription_cancelled" || e.event === "subscription_expired") {
        const uid = e.full_user_id;
        if (cancelledUsers.find(u => u.id === uid)) continue;
        const userEvents = events.filter((ev: any) => ev.full_user_id === uid && ev.event !== "$identify").sort((a: any, b: any) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
        const actions: string[] = [];
        const actionSet = new Set<string>();
        for (const ue of userEvents) {
          let label = "";
          if (ue.event === "app_opened" && !actionSet.has("opened")) { label = "Opened app"; actionSet.add("opened"); }
          else if (ue.event === "onboarding_completed" && !actionSet.has("onboarded")) { label = "Completed onboarding"; actionSet.add("onboarded"); }
          else if (ue.event === "subscription_started" && !actionSet.has("subscribed")) { label = "Started trial" + (ue.properties.plan ? " (" + ue.properties.plan + ")" : ""); actionSet.add("subscribed"); }
          else if (ue.event === "circle_created" && !actionSet.has("circle")) { label = "Created circle" + (ue.properties.circle_name ? " \"" + ue.properties.circle_name + "\"" : ""); actionSet.add("circle"); }
          else if (ue.event === "prayer_logged" && !actionSet.has("prayed")) { label = "Prayed"; actionSet.add("prayed"); }
          else if ((ue.event === "circle_invite_social_tapped" || ue.event === "circle_invite_code_tapped") && !actionSet.has("invited")) { label = "Invited to circle"; actionSet.add("invited"); }
          else if (ue.event === "onboarding_step_completed" && (ue.properties.step_name === "invite_code" || ue.properties.step_name === "circle_shared_code") && !actionSet.has("invited")) { label = "Sent invite code"; actionSet.add("invited"); }
          else if (ue.event === "onboarding_step_completed" && (ue.properties.step_name === "invite_story" || ue.properties.step_name === "circle_shared_social") && !actionSet.has("invited")) { label = "Shared story"; actionSet.add("invited"); }
          else if (ue.event === "onboarding_step_completed" && (ue.properties.step_name === "invite_later" || ue.properties.step_name === "circle_share_skipped" || ue.properties.step_name === "invite_skipped") && !actionSet.has("skip_invite")) { label = "Skipped invite"; actionSet.add("skip_invite"); }
          else if (ue.event === "onboarding_step_completed" && ue.properties.step_name === "social_share_skipped" && !actionSet.has("skip_social")) { label = "Skipped social share"; actionSet.add("skip_social"); }
          else if ((ue.event === "subscription_cancelled" || ue.event === "subscription_expired") && !actionSet.has("cancelled")) { label = "Cancelled"; actionSet.add("cancelled"); }
          if (label) actions.push(label);
        }
        cancelledUsers.push({ name: idToName[uid] || uid.substring(0,8), id: uid, plan: e.properties.plan || "?", actions });
      }
    }
    const cancellationFunnel = cancelledUsers;

    const topics: Record<string,number> = {}; for (const e of events) { if (e.event === "prayer_logged" && e.properties.type) topics[e.properties.type] = (topics[e.properties.type]||0)+1; }
    const plans: Record<string,number> = {}; for (const e of events) { if (e.event === "paywall_plan_selected" && e.properties.plan) plans[e.properties.plan] = (plans[e.properties.plan]||0)+1; }
    const dMap: Record<string, Set<string>> = {}; for (const e of events) { const d = e.timestamp.split("T")[0]; if (!dMap[d]) dMap[d] = new Set(); dMap[d].add(e.full_user_id); }
    const dau = Object.entries(dMap).map(([d, s]) => ({ date: d, dau: s.size })).sort((a, b) => b.date.localeCompare(a.date));
    return c.json({ generated_at: new Date().toISOString(), total_events: events.length, total_users: users.length, event_counts: Object.entries(ec).sort((a, b) => b[1] - a[1]), funnel: { first_open: fn.first_open.size, onboarding: fn.onboarding.size, paywall: fn.paywall.size, plan_tap: fn.plan_tap.size, prayer: fn.prayer.size, circle: fn.circle.size, signup: fn.signup.size, scripture: fn.scripture.size }, onboarding_funnel: onboardingFunnel, cancellation_funnel: cancellationFunnel, prayer_topics: Object.entries(topics).sort((a, b) => b[1] - a[1]), plan_taps: Object.entries(plans).sort((a, b) => b[1] - a[1]), daily_dau: dau, users: users.map((u: any) => ({ id: u.id, full_id: u.full_id, name: u.name || "", event_count: u.events.length, event_types: u.counts, first_seen: u.first_seen, last_seen: u.last_seen, city: u.city, country: u.country, max_streak: u.max_streak, plan_taps: u.plan_taps })), recent_events: events.filter((e: any) => e.event !== "$identify").slice(0, 500) });
  } catch (e: any) { console.error("[PostHog]", e); return c.json({ error: "PostHog failed", detail: e.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── REVENUECAT API ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
// v5.9.6 — RC overview API for accurate MRR + expanded user search
app.get("/api/dashboard/revenuecat", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  if (!REVENUECAT_SECRET_KEY) return c.json({ error: "REVENUECAT_SECRET_KEY not set" }, 500);
  try {
    // Try RC v2 overview API for accurate aggregate metrics
    let rcOverview: any = null;
    try {
      const ovRes = await fetch("https://api.revenuecat.com/v2/projects/d76b6d3d/metrics/overview", {
        headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }
      });
      if (ovRes.ok) rcOverview = await ovRes.json();
    } catch {}

    // Get ALL users from DB (no LIMIT) + user IDs from revenue_events that might not be in users table
    const usersResult = await pool.query("SELECT id, device_user_id, subscription_status, name, email FROM users ORDER BY created_at DESC");
    const revEventUsers = await pool.query("SELECT DISTINCT user_id FROM revenue_events WHERE event_type IN ('subscription_started','lifetime_purchased','subscription_renewed')").catch(() => ({ rows: [] }));

    // Build deduplicated set of all IDs to check — include $RCAnonymous for trial counting
    const checkedIds = new Set<string>();
    const allCandidates: { uid: string; name: string | null; email: string | null; db_status: string | null }[] = [];
    for (const user of usersResult.rows) {
      const ids = [user.id, user.device_user_id].filter(Boolean);
      for (const uid of ids) {
        if (!checkedIds.has(uid)) { checkedIds.add(uid); allCandidates.push({ uid, name: user.name, email: user.email, db_status: user.subscription_status }); }
      }
    }
    // Add revenue event user IDs not already in the set (including $RCAnonymous)
    for (const row of revEventUsers.rows) {
      if (row.user_id && !checkedIds.has(row.user_id)) { checkedIds.add(row.user_id); allCandidates.push({ uid: row.user_id, name: null, email: null, db_status: null }); }
    }

    // v5.10.6 — deduplicate by subscription fingerprint (product + expires_date = same subscription)
    const subscribers: any[] = []; let totalRevenue = 0; let activeCount = 0; let trialCount = 0; let mrr = 0;
    const seenSubscriptions = new Map<string, string>(); // dedupe by purchase proximity
    for (const candidate of allCandidates) {
      try {
        const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(candidate.uid)}`, { headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" } });
        if (!rcRes.ok) continue;
        const rcData = (await rcRes.json()) as any; const sub = rcData.subscriber; if (!sub) continue;
        // Deduplicate by subscription purchase proximity
        // If two user IDs have the same product purchased within 15 minutes, they're the same person
        const subscriptions = sub.subscriptions || {};
        let isDuplicate = false;
        for (const [pid, s2] of Object.entries(subscriptions) as any[]) {
          if (!s2.purchase_date) continue;
          const purchaseTs = new Date(s2.purchase_date).getTime();
          const key = pid; // product ID
          for (const [existingKey, existingTs] of seenSubscriptions.entries()) {
            if (existingKey.startsWith(key + ":") && Math.abs(purchaseTs - parseInt(existingKey.split(":")[1])) < 15 * 60 * 1000) {
              isDuplicate = true; break;
            }
          }
          if (!isDuplicate) seenSubscriptions.set(key + ":" + purchaseTs, candidate.uid);
          if (isDuplicate) break;
        }
        if (isDuplicate) continue;
        const entitlements = sub.entitlements || {};
        const now = new Date();
        // v5.10.7 — no buffer. A sub is active until it actually expires. Matches RC's logic.
        const nowPlusBuffer = now;
        const hasActive = Object.values(entitlements).some((e: any) => new Date(e.expires_date) > now);
        const hasTrial = Object.values(subscriptions).some((s: any) => s.period_type === "trial" && new Date(s.expires_date) > now);
        let userRevenue = 0;
        for (const [pid, s2] of Object.entries(subscriptions) as any[]) { if (s2.store === "app_store" || s2.store === "play_store") { if (pid.includes("yearly")) userRevenue += 29.99; else if (pid.includes("monthly")) userRevenue += 3.99; else if (pid.includes("lifetime")) userRevenue += 149.99; } }
        // v5.10.8 — count active subs + trials separately to match RC's definitions
        // RC counts trial users as "active subscriptions" if they have an auto-renewing plan
        // and as "trials" only for the trial-specific metric
        const hasActiveNonTrial = Object.values(subscriptions).some((s: any) => new Date(s.expires_date) > nowPlusBuffer && s.period_type !== "trial" && !s.unsubscribe_detected_at && !s.billing_issues_detected_at);
        // For trials, don't filter by unsubscribe_detected_at — Apple sets it when auto-renew is off,
        // which is the default for most trial users. A trial is active if it hasn't expired.
        const hasActiveTrial = Object.values(subscriptions).some((s: any) => new Date(s.expires_date) > nowPlusBuffer && s.period_type === "trial");
        const isLifetime = Object.keys(subscriptions).some((pid) => pid.includes("lifetime"));
        // Active subs: both non-trial renewers AND trial users (RC counts trials as active subs)
        if (hasActiveNonTrial || hasActiveTrial || isLifetime) activeCount++;
        // Trial count: only users currently in trial period
        if (hasActiveTrial) trialCount++;
        totalRevenue += userRevenue;
        // MRR: count both active paid AND active trials (trials will convert)
        for (const [pid, s2] of Object.entries(subscriptions) as any[]) {
          const expires = new Date(s2.expires_date);
          if (expires > nowPlusBuffer && !s2.unsubscribe_detected_at && !s2.billing_issues_detected_at) {
            if (pid.includes("yearly")) mrr += 29.99 / 12;
            else if (pid.includes("monthly")) mrr += 3.99;
          }
        }
        subscribers.push({ user_id: candidate.uid, name: candidate.name || null, email: candidate.email || null, db_status: candidate.db_status, has_active: hasActive, has_trial: hasTrial, revenue: userRevenue, entitlements: Object.keys(entitlements), subscriptions: Object.entries(subscriptions).map(([pid2, s3]: [string, any]) => ({ product: pid2, store: s3.store, purchase_date: s3.purchase_date, expires_date: s3.expires_date, period_type: s3.period_type, is_active: new Date(s3.expires_date) > new Date(), auto_resume_date: s3.auto_resume_date, unsubscribe_detected_at: s3.unsubscribe_detected_at })), first_seen: sub.first_seen });
      } catch { continue; }
    }

    // Use RC v2 overview if available, otherwise use computed values with dedup discount
    // The v1 API returns ~5 duplicate user IDs that inflate the count
    // Dedup discount: count unique subscriptions by purchase proximity, then subtract known duplicates
    const ovMetrics = rcOverview?.metrics || rcOverview;
    // Estimate unique subscribers: total found minus duplicates caught minus estimated remaining duplicates
    // RC v1 API cannot reliably deduplicate — use a heuristic: ~40% of anonymous RC IDs are duplicates of auth IDs
    const anonCount = subscribers.filter((s: any) => s.user_id.startsWith("$RCAnonymous")).length;
    const estimatedDuplicates = Math.round(anonCount * 0.8);
    const adjustedActive = Math.max(activeCount - estimatedDuplicates, 0);
    // MRR discount: compute avg MRR per subscriber, then subtract duplicates' share
    const avgMrrPerSub = activeCount > 0 ? mrr / activeCount : 0;
    const adjustedMrr = Math.round(Math.max(mrr - estimatedDuplicates * avgMrrPerSub, 0) * 100) / 100;
    const summaryActive = ovMetrics?.active_subscriptions ?? adjustedActive;
    const summaryTrials = ovMetrics?.active_trials ?? trialCount;
    const summaryMrr = ovMetrics?.mrr ? ovMetrics.mrr / 100 : Math.max(adjustedMrr, 0);
    const summaryNetMrr = ovMetrics?.mrr ? Math.round(ovMetrics.mrr / 100 * (1 - APPLE_CUT) * 100) / 100 : Math.round(Math.max(adjustedMrr, 0) * (1 - APPLE_CUT) * 100) / 100;

    // v5.14.1 — net counts: prefer RC v2 overview (authoritative), fall back to subscriber scan
    // The subscriber scan misses $RCAnonymous users which causes undercounting
    // For trials, don't filter by unsubscribe_detected_at — Apple sets it when auto-renew is off by default
    const computedNetTrials = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type === "trial")).length;
    const computedNetSubs = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type !== "trial" && !sub.unsubscribe_detected_at)).length;
    const netActiveTrials = ovMetrics?.active_trials ?? computedNetTrials;
    const netActiveSubscribers = ovMetrics?.active_subscriptions != null ? Math.max((ovMetrics.active_subscriptions || 0) - (ovMetrics?.active_trials || 0), computedNetSubs) : computedNetSubs;
    // v5.15.0 — monthly vs yearly breakdown for trials and subscribers
    const netTrialMonthly = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type === "trial" && (sub.product || "").includes("monthly"))).length;
    const netTrialYearly = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type === "trial" && (sub.product || "").includes("yearly"))).length;
    const netSubMonthly = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type !== "trial" && !sub.unsubscribe_detected_at && (sub.product || "").includes("monthly"))).length;
    const netSubYearly = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type !== "trial" && !sub.unsubscribe_detected_at && (sub.product || "").includes("yearly"))).length;
    const netSubLifetime = subscribers.filter((s: any) => s.subscriptions.some((sub: any) => sub.is_active && sub.period_type !== "trial" && !sub.unsubscribe_detected_at && (sub.product || "").includes("lifetime"))).length;

    return c.json({ generated_at: new Date().toISOString(), rc_overview: rcOverview ? "v2" : "v1_computed", summary: { active_subscriptions: summaryActive, active_trials: summaryTrials, net_active_trials: netActiveTrials, net_active_subscribers: netActiveSubscribers, net_trial_monthly: netTrialMonthly, net_trial_yearly: netTrialYearly, net_sub_monthly: netSubMonthly, net_sub_yearly: netSubYearly, net_sub_lifetime: netSubLifetime, total_revenue_estimated: totalRevenue, mrr_estimated: summaryMrr, net_mrr: summaryNetMrr, total_users_checked: allCandidates.length, subscribers_found: subscribers.filter(s => s.has_active || s.has_trial).length }, subscribers: subscribers.filter(s => s.subscriptions.length > 0 || s.has_active || s.has_trial), all_users: subscribers });
  } catch (err: any) { return c.json({ error: "RevenueCat query failed", detail: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── APPLE APP STORE ────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
// v5.10.1 — create both ONGOING + ONE_TIME_SNAPSHOT requests to maximize chance of getting app analytics reports
async function initAppleAnalytics(): Promise<string[]> {
  const token = generateASCToken(); if (!token) return [];
  const requestIds: string[] = [];
  try {
    // Check for existing requests of both types
    for (const accessType of ["ONGOING", "ONE_TIME_SNAPSHOT"]) {
      const existingRes = await fetch(`https://api.appstoreconnect.apple.com/v1/apps/${PRAMEN_APP_ID}/analyticsReportRequests?filter[accessType]=${accessType}`, { headers: { Authorization: `Bearer ${token}` } });
      if (existingRes.ok) {
        const existing = (await existingRes.json()) as any;
        if (existing.data?.length > 0) { for (const d of existing.data) requestIds.push(d.id); }
        else {
          // Create new request
          const createRes = await fetch("https://api.appstoreconnect.apple.com/v1/analyticsReportRequests", { method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, body: JSON.stringify({ data: { type: "analyticsReportRequests", attributes: { accessType }, relationships: { app: { data: { type: "apps", id: PRAMEN_APP_ID } } } } }) });
          if (createRes.ok) { const created = (await createRes.json()) as any; if (created.data?.id) requestIds.push(created.data.id); console.log(`[Apple] Created ${accessType} request:`, created.data?.id); }
        }
      }
    }
    console.log("[Apple] Report request IDs:", requestIds);
  } catch (err: any) { console.error("[Apple] Init error:", err.message); }
  return requestIds;
}

// v5.10.1 — check ALL report requests + ALL reports for instances (no keyword filter)
async function pullAppleAnalytics(): Promise<any> {
  const token = generateASCToken(); if (!token) return null;
  try {
    const requestIds = await initAppleAnalytics();
    if (requestIds.length === 0) return { connected: false, error: "No report requests" };
    const allReportNames: string[] = [];
    const reportsWithInstances: any[] = [];
    let stored = 0;

    for (const requestId of requestIds) {
      const reportsRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/${requestId}/reports`, { headers: { Authorization: `Bearer ${token}` } });
      if (!reportsRes.ok) continue;
      const reportsData = (await reportsRes.json()) as any;
      const reports = reportsData.data || [];
      for (const report of reports) { allReportNames.push(report.attributes?.name || "unknown"); }

      // Check ALL reports for instances — not just keyword-filtered ones
      for (const report of reports) {
        const reportName = (report.attributes?.name || "").toLowerCase();
        const instancesRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReports/${report.id}/instances?limit=3`, { headers: { Authorization: `Bearer ${token}` } });
        if (!instancesRes.ok) continue;
        const instancesData = (await instancesRes.json()) as any;
        const instances = instancesData.data || [];
        if (instances.length === 0) continue;

        reportsWithInstances.push({ name: report.attributes?.name, instances: instances.length });

        // Try to parse data from reports that might have app store metrics
        for (const instance of instances.slice(0, 2)) {
          const segmentsRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/${instance.id}/segments?fields[analyticsReportSegments]=url,checksum,sizeInBytes`, { headers: { Authorization: `Bearer ${token}` } });
          if (!segmentsRes.ok) continue;
          const segmentsData = (await segmentsRes.json()) as any;
          const segments = segmentsData.data || [];
          if (segments.length === 0) continue;
          const segUrl = segments[0].attributes?.url;
          if (!segUrl) continue;
          const dataRes = await fetch(segUrl);
          if (!dataRes.ok) continue;
          // Apple segment data may be gzip-compressed
          const dataBuf = Buffer.from(await dataRes.arrayBuffer());
          let rawText: string;
          try { rawText = gunzipSync(dataBuf).toString("utf-8"); } catch { rawText = dataBuf.toString("utf-8"); }
          const lines = rawText.trim().split("\n");
          if (lines.length < 2) continue;
          const headers = lines[0].split("\t").map((h: string) => h.trim().toLowerCase().replace(/\s+/g, "_"));
          // Check if this report has app store metrics columns (be permissive — log headers for debugging)
          const appStoreReportNames = ["app downloads", "discovery and engagement", "installation and deletion", "purchases"];
          const isAppStoreReport = appStoreReportNames.some((n: string) => reportName.includes(n));
          if (!isAppStoreReport) continue;
          const rows: any[] = [];
          for (let i = 1; i < lines.length; i++) { const cols = lines[i].split("\t"); const row: any = {}; headers.forEach((h: string, j: number) => { row[h] = cols[j]?.trim() || ""; }); rows.push(row); }
          // Aggregate rows by date — Apple gives per-row breakdowns (by territory, device, etc.)
          // We need to sum Counts per date, filtered by Download Type for downloads reports
          const dailyAgg: Record<string, { impressions: number; pageViews: number; downloads: number }> = {};
          const isDownloadReport = reportName.includes("download");
          const isDiscoveryReport = reportName.includes("discovery");
          for (const row of rows) {
            const date = row.date; if (!date) continue;
            if (!dailyAgg[date]) dailyAgg[date] = { impressions: 0, pageViews: 0, downloads: 0 };
            const counts = parseInt(row.counts || "0") || 0;
            if (isDownloadReport) {
              const dlType = (row.download_type || "").toLowerCase();
              if (dlType.includes("first-time") || dlType.includes("first_time")) { dailyAgg[date].downloads += counts; }
            }
            if (isDiscoveryReport) {
              // Discovery report has "Counts" for impressions/page views depending on "Page Type"
              const pageType = (row.page_type || "").toLowerCase();
              if (pageType.includes("product page")) { dailyAgg[date].pageViews += counts; }
              else { dailyAgg[date].impressions += counts; }
            }
          }
          for (const [date, agg] of Object.entries(dailyAgg)) {
            if (agg.impressions || agg.downloads || agg.pageViews) {
              const convRate = agg.pageViews > 0 ? agg.downloads / agg.pageViews : 0;
              await pool.query(`INSERT INTO daily_app_store_metrics (date,impressions,product_page_views,app_units,conversion_rate,updated_at) VALUES ($1,$2,$3,$4,$5,NOW()) ON CONFLICT (date) DO UPDATE SET impressions=CASE WHEN $2>0 THEN GREATEST(daily_app_store_metrics.impressions,$2) ELSE daily_app_store_metrics.impressions END,product_page_views=CASE WHEN $3>0 THEN GREATEST(daily_app_store_metrics.product_page_views,$3) ELSE daily_app_store_metrics.product_page_views END,app_units=CASE WHEN $4>0 THEN GREATEST(daily_app_store_metrics.app_units,$4) ELSE daily_app_store_metrics.app_units END,conversion_rate=CASE WHEN $5>0 THEN $5 ELSE daily_app_store_metrics.conversion_rate END,updated_at=NOW()`, [date, agg.impressions, agg.pageViews, agg.downloads, convRate]);
              stored++;
            }
          }
          // Don't return early — continue processing other reports (Downloads + Discovery + Purchases)
        }
      }
    }
    if (stored > 0) return { connected: true, status: "ok", reports_available: [...new Set(allReportNames)], reports_with_instances: reportsWithInstances, days_stored: stored };
    // If we got here, we found instances but couldn't parse app metrics from them
    // Try to return sample headers+data from the first app store report for debugging
    let debugSample: any = null;
    for (const requestId of requestIds) {
      const reportsRes2 = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/${requestId}/reports`, { headers: { Authorization: `Bearer ${token}` } });
      if (!reportsRes2.ok) continue;
      const reportsData2 = (await reportsRes2.json()) as any;
      for (const report of (reportsData2.data || [])) {
        const rn = (report.attributes?.name || "").toLowerCase();
        if (!rn.includes("app downloads") && !rn.includes("discovery")) continue;
        const ir = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReports/${report.id}/instances?limit=1`, { headers: { Authorization: `Bearer ${token}` } });
        if (!ir.ok) continue;
        const id2 = (await ir.json()) as any;
        const inst = id2.data?.[0]; if (!inst) continue;
        const sr = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/${inst.id}/segments?fields[analyticsReportSegments]=url,checksum,sizeInBytes`, { headers: { Authorization: `Bearer ${token}` } });
        if (!sr.ok) continue;
        const sd2 = (await sr.json()) as any;
        const seg = sd2.data?.[0]; if (!seg?.attributes?.url) continue;
        const dr = await fetch(seg.attributes.url);
        if (!dr.ok) continue;
        const dbuf = Buffer.from(await dr.arrayBuffer());
        let dtxt: string; try { dtxt = gunzipSync(dbuf).toString("utf-8"); } catch { dtxt = dbuf.toString("utf-8"); }
        const dlines = dtxt.trim().split("\n");
        debugSample = { report: report.attributes?.name, headers: dlines[0]?.split("\t"), rows: dlines.length - 1, sample_rows: dlines.slice(1, 4).map((l: string) => l.split("\t")), raw_first_100: dtxt.substring(0, 500) };
        break;
      }
      if (debugSample) break;
    }
    return { connected: true, status: reportsWithInstances.length > 0 ? "instances_found_no_app_metrics" : "pending", reports_available: [...new Set(allReportNames)], reports_with_instances: reportsWithInstances, total_reports: allReportNames.length, debug_sample: debugSample };
  } catch (err: any) { console.error("[Apple] Analytics error:", err.message); return { connected: false, error: err.message }; }
}

app.post("/api/dashboard/appstore/seed", async (c) => { const secret = c.req.query("key") || c.req.header("X-Dashboard-Key"); if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401); try { const body = await c.req.json(); const { metrics } = body; if (!Array.isArray(metrics)) return c.json({ error: "metrics array required" }, 400); let stored = 0; for (const m of metrics) { if (!m.date) continue; await pool.query(`INSERT INTO daily_app_store_metrics (date,impressions,product_page_views,app_units,conversion_rate,proceeds,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date) DO UPDATE SET impressions=$2,product_page_views=$3,app_units=$4,conversion_rate=$5,proceeds=$6,updated_at=NOW()`, [m.date, m.impressions||0, m.product_page_views||0, m.app_units||0, m.conversion_rate||0, m.proceeds||0]); stored++; } return c.json({ status: "ok", stored }); } catch (e: any) { return c.json({ error: e.message }, 500); } });

app.get("/api/dashboard/appstore", async (c) => { const secret = c.req.query("key") || c.req.header("X-Dashboard-Key"); if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401); const token = generateASCToken(); if (!token) return c.json({ connected: false, error: "Apple API not configured" }); try { const appRes = await fetch(`https://api.appstoreconnect.apple.com/v1/apps/${PRAMEN_APP_ID}`, { headers: { Authorization: `Bearer ${token}` } }); if (!appRes.ok) return c.json({ connected: false, error: "Apple API " + appRes.status }); const appData = (await appRes.json()) as any; const analytics = await pullAppleAnalytics(); const stored = await pool.query(`SELECT * FROM daily_app_store_metrics ORDER BY date DESC LIMIT 30`).catch(() => ({ rows: [] })); return c.json({ connected: true, app: { name: appData.data?.attributes?.name, bundleId: appData.data?.attributes?.bundleId }, analytics: analytics, daily_metrics: stored.rows, timestamp: new Date().toISOString() }); } catch (e: any) { return c.json({ connected: false, error: e.message }); } });

// v5.10.0 — force pull Apple sales + analytics data on demand
app.post("/api/dashboard/appstore/pull", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const token = generateASCToken(); if (!token) return c.json({ error: "Apple API not configured" }, 500);
  const results: any = { sales: null, analytics: null };
  // Pull sales reports for last 30 days (Apple returns gzip-compressed TSV)
  // Apple Sales TSV cols: 0=Provider 1=ProviderCountry 2=SKU 3=Developer 4=Title 5=Version 6=ProductTypeIdentifier 7=Units 8=DeveloperProceeds 9=Currency
  // ProductTypeIdentifier: 1F=paid first-time, FI1=free first-time, IA1/IA9/IAY=IAP, 7/7F/7T=update, F7=free update
  try {
    let salesStored = 0;
    const salesDebug: any[] = [];
    for (let daysBack = 1; daysBack <= 30; daysBack++) {
      const d = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
      const fmtDate = d.toISOString().split("T")[0];
      try {
        const res = await fetch("https://api.appstoreconnect.apple.com/v1/salesReports?filter[reportType]=SALES&filter[reportSubType]=SUMMARY&filter[frequency]=DAILY&filter[vendorNumber]=93967404&filter[reportDate]=" + fmtDate, {
          headers: { Authorization: `Bearer ${token}`, Accept: "application/a-gzip" }
        });
        if (res.ok) {
          const buf = Buffer.from(await res.arrayBuffer());
          let text: string;
          try { text = gunzipSync(buf).toString("utf-8"); } catch { text = buf.toString("utf-8"); }
          const lines = text.split("\n").filter((l: string) => l.trim());
          if (lines.length > 1) {
            let firstTimeDownloads = 0; let redownloads = 0; let updates = 0; let iapUnits = 0; let totalProceeds = 0;
            const rowDetails: any[] = [];
            for (let i = 1; i < lines.length; i++) {
              const cols = lines[i].split("\t");
              const productType = (cols[6] || "").trim();
              const units = parseInt(cols[7] || "0") || 0;
              const proceeds = parseFloat(cols[8] || "0") || 0;
              totalProceeds += proceeds;
              // Classify by product type
              if (productType === "1F" || productType === "F1" || productType === "FI1" || productType === "1" || productType === "3F" || productType === "3" || productType === "3T") { firstTimeDownloads += units; }
              else if (productType === "7" || productType === "7F" || productType === "7T" || productType === "F7") { updates += units; }
              else if (productType.startsWith("IA")) { iapUnits += units; }
              else { redownloads += units; }
              rowDetails.push({ type: productType, units, proceeds, sku: (cols[2]||"").trim() });
            }
            const appDownloads = firstTimeDownloads; // Match ASC's "First-Time Downloads"
            await pool.query(`INSERT INTO daily_app_store_metrics (date,app_units,proceeds,impressions,product_page_views,conversion_rate,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date) DO UPDATE SET app_units=$2,proceeds=$3,updated_at=NOW()`, [fmtDate, appDownloads, totalProceeds, 0, 0, 0]);
            salesStored++;
            salesDebug.push({ date: fmtDate, first_time: firstTimeDownloads, redownloads, updates, iap: iapUnits, proceeds: totalProceeds, rows: rowDetails });
          } else { salesDebug.push({ date: fmtDate, status: "empty" }); }
        } else {
          const errBody = await res.text().catch(() => "");
          salesDebug.push({ date: fmtDate, status: res.status, error: errBody.substring(0, 300) });
        }
      } catch (e2: any) { salesDebug.push({ date: fmtDate, error: e2.message }); }
    }
    results.sales = { days_checked: 30, days_stored: salesStored, debug: salesDebug.slice(0, 10) };
  } catch (e: any) { results.sales = { error: e.message }; }
  // Pull analytics
  try { results.analytics = await pullAppleAnalytics(); } catch (e: any) { results.analytics = { error: e.message }; }
  // Return current stored metrics
  const stored = await pool.query(`SELECT * FROM daily_app_store_metrics ORDER BY date DESC LIMIT 30`).catch(() => ({ rows: [] }));
  return c.json({ status: "ok", results, daily_metrics: stored.rows });
});

// ═══════════════════════════════════════════════════════════════════
// ─── GROWTH / DECISION DASHBOARD ────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

// POST /api/dashboard/ad-metrics — upsert daily ad spend rows
app.post("/api/dashboard/ad-metrics", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const body = await c.req.json();
    const entries: any[] = Array.isArray(body) ? body : body.entries || [];
    if (!entries.length) return c.json({ error: "entries array required" }, 400);
    let stored = 0;
    for (const e of entries) {
      if (!e.date || !e.channel) continue;
      await pool.query(
        `INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,trials,subscriptions,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$4,impressions=$5,clicks=$6,installs=$7,trials=$8,subscriptions=$9,updated_at=NOW()`,
        [e.date, e.channel, e.campaign||"all", e.spend||0, e.impressions||0, e.clicks||0, e.installs||0, e.trials||0, e.subscriptions||0]
      );
      stored++;
    }
    return c.json({ status: "ok", stored });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /api/dashboard/growth — main decision dashboard
app.get("/api/dashboard/growth", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    // a) Acquisition — last 30 days
    const adRows = await pool.query(`SELECT channel, SUM(spend) as total_spend, SUM(impressions) as total_impressions, SUM(clicks) as total_clicks, SUM(installs) as total_installs, SUM(trials) as total_trials FROM daily_ad_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' AND channel NOT IN ('meta_ad','meta_adset') GROUP BY channel`).catch(() => ({ rows: [] }));
    const orgAcq = await pool.query(`SELECT SUM(impressions) as impressions, SUM(product_page_views) as page_views, SUM(app_units) as units FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days'`).catch(() => ({ rows: [{}] }));
    const totalAdSpend = adRows.rows.reduce((s: number, r: any) => s + parseFloat(r.total_spend||0), 0);
    const totalInstalls = adRows.rows.reduce((s: number, r: any) => s + parseInt(r.total_installs||0), 0);
    const blendedCPI = totalInstalls > 0 ? totalAdSpend / totalInstalls : null;
    const acquisitionByChannel = adRows.rows.map((r: any) => {
      const spend = parseFloat(r.total_spend||0);
      const impressions = parseInt(r.total_impressions||0);
      const clicks = parseInt(r.total_clicks||0);
      const installs = parseInt(r.total_installs||0);
      const trials = parseInt(r.total_trials||0);
      const ctr = impressions > 0 ? (clicks / impressions * 100) : null;
      const cpm = impressions > 0 ? (spend / impressions * 1000) : null;
      const cpi = installs > 0 ? spend / installs : null;
      return { channel: r.channel, spend, impressions, clicks, installs, trials, ctr, cpm, cpi };
    });

    // b) Unit economics
    const revLast30 = await pool.query(`SELECT SUM(new_subscribers) as new_subs, SUM(cancellations) as cancels, SUM(revenue_gross) as gross FROM daily_revenue WHERE date >= CURRENT_DATE - INTERVAL '30 days'`).catch(() => ({ rows: [{}] }));
    const revRow = revLast30.rows[0] || {};
    const totalNewPayingSubs = parseInt(revRow.new_subs||0);
    const totalCancels = parseInt(revRow.cancels||0);
    const totalRevenue30d = parseFloat(revRow.gross||0);
    const subsStatus = await pool.query(`SELECT subscription_status, COUNT(*) as count FROM users GROUP BY subscription_status`).catch(() => ({ rows: [] }));
    const activeSubs = subsStatus.rows.filter((r: any) => r.subscription_status === "active" || r.subscription_status === "lifetime").reduce((s: number, r: any) => s + parseInt(r.count||0), 0);
    const monthlyChurnRate = activeSubs > 0 ? totalCancels / activeSubs : null;
    // plan breakdown for avg revenue per sub
    const planRevRows = await pool.query(`SELECT plan, COUNT(*) as count FROM revenue_events WHERE event_type='subscription_started' AND created_at >= CURRENT_DATE - INTERVAL '90 days' GROUP BY plan`).catch(() => ({ rows: [] }));
    const planCounts: Record<string,number> = {}; for (const r of planRevRows.rows) planCounts[r.plan] = parseInt(r.count||0);
    const planPrices: Record<string,number> = { monthly: 3.99, yearly: 29.99/12, lifetime: 149.99 };
    let totalPlanRevenue = 0; let totalPlanCount = 0;
    for (const [plan, cnt] of Object.entries(planCounts)) { totalPlanRevenue += (planPrices[plan]||0) * cnt; totalPlanCount += cnt; }
    const avgRevenuePerSub = totalPlanCount > 0 ? totalPlanRevenue / totalPlanCount : null;
    const ltv = avgRevenuePerSub && monthlyChurnRate && monthlyChurnRate > 0 ? avgRevenuePerSub * (1 / monthlyChurnRate) : null;
    const cpa = totalNewPayingSubs > 0 ? totalAdSpend / totalNewPayingSubs : null;
    const roas = totalAdSpend > 0 ? totalRevenue30d / totalAdSpend : null;
    const paybackMonths = cpa && avgRevenuePerSub && avgRevenuePerSub > 0 ? cpa / avgRevenuePerSub : null;

    // c) Viral metrics
    const circleCount = circles.size;
    let totalCircleMembers = 0; for (const [, ci] of circles) totalCircleMembers += ci.members.length;
    const avgMembersPerCircle = circleCount > 0 ? totalCircleMembers / circleCount : 0;
    const inviteStats = await pool.query(`SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE status='accepted') as accepted FROM invite_tokens`).catch(() => ({ rows: [{}] }));
    const totalInvites = parseInt(inviteStats.rows[0]?.total||0);
    const acceptedInvites = parseInt(inviteStats.rows[0]?.accepted||0);
    const acceptanceRate = totalInvites > 0 ? acceptedInvites / totalInvites : 0;
    const userCount = parseInt((await pool.query("SELECT COUNT(*) as c FROM users").catch(() => ({ rows: [{c:0}] }))).rows[0]?.c||0);
    const avgInvitesPerUser = userCount > 0 ? totalInvites / userCount : 0;
    const viralK = avgInvitesPerUser * acceptanceRate;

    // d) Cohort retention (by week of signup, D1/D7/D14/D30)
    const cohortRows = await pool.query(`SELECT DATE_TRUNC('week', u.created_at)::date as cohort_week, COUNT(DISTINCT u.id) as cohort_size, COUNT(DISTINCT CASE WHEN ud.last_prayed_date >= u.created_at + INTERVAL '1 day' AND ud.last_prayed_date < u.created_at + INTERVAL '2 days' THEN u.id END) as d1, COUNT(DISTINCT CASE WHEN ud.last_prayed_date >= u.created_at + INTERVAL '7 days' AND ud.last_prayed_date < u.created_at + INTERVAL '8 days' THEN u.id END) as d7, COUNT(DISTINCT CASE WHEN ud.last_prayed_date >= u.created_at + INTERVAL '14 days' AND ud.last_prayed_date < u.created_at + INTERVAL '15 days' THEN u.id END) as d14, COUNT(DISTINCT CASE WHEN ud.last_prayed_date >= u.created_at + INTERVAL '30 days' AND ud.last_prayed_date < u.created_at + INTERVAL '31 days' THEN u.id END) as d30 FROM users u LEFT JOIN user_data ud ON ud.user_id = u.id WHERE u.created_at >= CURRENT_DATE - INTERVAL '90 days' GROUP BY 1 ORDER BY 1 DESC LIMIT 12`).catch(() => ({ rows: [] }));
    const cohortRetention = cohortRows.rows.map((r: any) => {
      const sz = parseInt(r.cohort_size||1);
      return { week: r.cohort_week, cohort_size: sz, d1_pct: sz > 0 ? Math.round(parseInt(r.d1||0)/sz*100) : 0, d7_pct: sz > 0 ? Math.round(parseInt(r.d7||0)/sz*100) : 0, d14_pct: sz > 0 ? Math.round(parseInt(r.d14||0)/sz*100) : 0, d30_pct: sz > 0 ? Math.round(parseInt(r.d30||0)/sz*100) : 0 };
    });

    // e) Streak distribution
    const streakRows = await pool.query(`SELECT CASE WHEN streak_count=0 THEN '0' WHEN streak_count<=3 THEN '1-3' WHEN streak_count<=7 THEN '4-7' WHEN streak_count<=14 THEN '8-14' WHEN streak_count<=30 THEN '15-30' ELSE '31+' END as range, COUNT(*) as count FROM user_data GROUP BY 1 ORDER BY MIN(streak_count)`).catch(() => ({ rows: [] }));
    const streakDistribution = streakRows.rows.map((r: any) => ({ range: r.range, count: parseInt(r.count||0) }));

    // f) Daily ad series for chart (last 30 days, per channel per day)
    const dailySeriesRows = await pool.query(`SELECT date::text, channel, SUM(spend) as spend, SUM(impressions) as impressions, SUM(clicks) as clicks, SUM(installs) as installs, SUM(trials) as trials FROM daily_ad_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' AND channel NOT IN ('meta_ad','meta_adset') GROUP BY date, channel ORDER BY date ASC`).catch(() => ({ rows: [] }));
    const dailySeries = dailySeriesRows.rows.map((r: any) => ({ date: r.date, channel: r.channel, spend: parseFloat(r.spend||0), impressions: parseInt(r.impressions||0), clicks: parseInt(r.clicks||0), installs: parseInt(r.installs||0), trials: parseInt(r.trials||0) }));

    // g) Meta hierarchical breakdown: campaigns → ad sets → ads
    const adsetRows = await pool.query(`SELECT campaign as raw, SUM(spend) as spend, SUM(impressions) as impressions, SUM(clicks) as clicks, SUM(installs) as installs, SUM(trials) as trials FROM daily_ad_metrics WHERE channel='meta_adset' AND date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY campaign ORDER BY SUM(spend) DESC`).catch(() => ({ rows: [] }));
    const adLevelRows = await pool.query(`SELECT campaign as raw, SUM(spend) as spend, SUM(impressions) as impressions, SUM(clicks) as clicks, SUM(installs) as installs, SUM(trials) as trials FROM daily_ad_metrics WHERE channel='meta_ad' AND date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY campaign ORDER BY SUM(spend) DESC`).catch(() => ({ rows: [] }));
    function parseMetaRow(r: any, splitCount: number) {
      const parts = (r.raw || "").split("|||");
      const spend = parseFloat(r.spend||0); const impressions = parseInt(r.impressions||0); const clicks = parseInt(r.clicks||0);
      const installs = parseInt(r.installs||0); const trials = parseInt(r.trials||0);
      return { parts, spend, impressions, clicks, installs, trials, cpi: installs > 0 ? spend / installs : null, ctr: impressions > 0 ? (clicks / impressions * 100) : null };
    }
    // Build hierarchy: campaign → adsets → ads
    const metaHierarchy: any[] = [];
    const campaignMap = new Map<string, any>();
    // Add ad sets
    for (const r of adsetRows.rows) {
      const parsed = parseMetaRow(r, 2);
      const [campaign, adset] = parsed.parts;
      if (!campaignMap.has(campaign)) { campaignMap.set(campaign, { name: campaign, spend: 0, impressions: 0, clicks: 0, installs: 0, trials: 0, adsets: [] }); }
      const c = campaignMap.get(campaign)!;
      c.spend += parsed.spend; c.impressions += parsed.impressions; c.clicks += parsed.clicks; c.installs += parsed.installs; c.trials += parsed.trials;
      c.adsets.push({ name: adset, spend: parsed.spend, impressions: parsed.impressions, clicks: parsed.clicks, installs: parsed.installs, trials: parsed.trials, cpi: parsed.cpi, ctr: parsed.ctr, ads: [] });
    }
    // Add ads to their ad sets
    for (const r of adLevelRows.rows) {
      const parsed = parseMetaRow(r, 3);
      const [campaign, adset, adName] = parsed.parts;
      const c = campaignMap.get(campaign);
      if (c) {
        const as = c.adsets.find((a: any) => a.name === adset);
        if (as) { as.ads.push({ name: adName, spend: parsed.spend, impressions: parsed.impressions, clicks: parsed.clicks, installs: parsed.installs, trials: parsed.trials, cpi: parsed.cpi, ctr: parsed.ctr }); }
      }
    }
    // Finalize campaigns
    for (const [, c] of campaignMap) {
      c.cpi = c.installs > 0 ? c.spend / c.installs : null;
      c.ctr = c.impressions > 0 ? (c.clicks / c.impressions * 100) : null;
      c.adsets.sort((a: any, b: any) => b.spend - a.spend);
      for (const as of c.adsets) as.ads.sort((a: any, b: any) => b.spend - a.spend);
      metaHierarchy.push(c);
    }
    metaHierarchy.sort((a, b) => b.spend - a.spend);
    // Keep flat breakdown for backwards compat
    const metaAdBreakdown = adLevelRows.rows.map((r: any) => { const p = parseMetaRow(r, 3); return { ad_name: p.parts[2] || p.parts[0], spend: p.spend, impressions: p.impressions, clicks: p.clicks, installs: p.installs, trials: p.trials, cpi: p.cpi, ctr: p.ctr }; });

    // h) Decision signals
    const avgD7 = cohortRetention.length > 0 ? cohortRetention.reduce((s: number, r: any) => s + r.d7_pct, 0) / cohortRetention.length : 0;
    const unitEconSignal = cpa === null || ltv === null ? "gray" : cpa < ltv * 0.5 ? "green" : cpa < ltv ? "yellow" : "red";
    const unitEconAction = unitEconSignal === "green" ? "Scale spend. CPA is well below LTV." : unitEconSignal === "yellow" ? "Optimize creative. CPA approaching LTV." : unitEconSignal === "gray" ? "Not enough data yet." : "Pause paid. CPA exceeds LTV.";
    const viralSignal = viralK >= 0.5 ? "green" : viralK >= 0.2 ? "yellow" : "red";
    const viralAction = viralSignal === "green" ? "Viral loop healthy. Lean into invite prompts." : viralSignal === "yellow" ? "Improve invite placement or copy." : "Fix viral loop before scaling spend.";
    const retentionSignal = avgD7 >= 40 ? "green" : avgD7 >= 25 ? "yellow" : "red";
    const retentionAction = retentionSignal === "green" ? "Retention strong. Safe to acquire aggressively." : retentionSignal === "yellow" ? "Improve D7 onboarding. Target 40%." : "Fix retention before scaling. D7 < 25%.";

    return c.json({
      generated_at: new Date().toISOString(),
      acquisition: { total_ad_spend_30d: totalAdSpend, total_installs_30d: totalInstalls, blended_cpi: blendedCPI, by_channel: acquisitionByChannel, organic: { impressions: parseInt(orgAcq.rows[0]?.impressions||0), page_views: parseInt(orgAcq.rows[0]?.page_views||0), units: parseInt(orgAcq.rows[0]?.units||0) } },
      unit_economics: { cpa, ltv, avg_revenue_per_sub: avgRevenuePerSub, monthly_churn_rate: monthlyChurnRate, roas, payback_months: paybackMonths, active_subscribers: activeSubs, revenue_30d: totalRevenue30d, plan_breakdown: planCounts },
      viral: { circles: circleCount, total_circle_members: totalCircleMembers, avg_members_per_circle: Math.round(avgMembersPerCircle * 10) / 10, invite_tokens_created: totalInvites, invite_tokens_accepted: acceptedInvites, acceptance_rate: Math.round(acceptanceRate * 100) / 100, avg_invites_per_user: Math.round(avgInvitesPerUser * 100) / 100, viral_k: Math.round(viralK * 100) / 100 },
      cohort_retention: cohortRetention,
      streak_distribution: streakDistribution,
      daily_series: dailySeries,
      meta_ad_breakdown: metaAdBreakdown,
      meta_hierarchy: metaHierarchy,
      signals: { unit_economics: { status: unitEconSignal, action: unitEconAction, cpa, ltv }, viral_loop: { status: viralSignal, action: viralAction, k: Math.round(viralK * 100) / 100 }, retention: { status: retentionSignal, action: retentionAction, d7_avg_pct: Math.round(avgD7) } }
    });
  } catch (err: any) { return c.json({ error: "Growth dashboard failed", detail: err.message }, 500); }
});

// POST /api/dashboard/organic-metrics — upsert daily organic social rows
app.post("/api/dashboard/organic-metrics", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const body = await c.req.json();
    const entries: any[] = Array.isArray(body) ? body : body.entries || [];
    if (!entries.length) return c.json({ error: "entries array required" }, 400);
    let stored = 0;
    for (const e of entries) {
      if (!e.date || !e.channel) continue;
      await pool.query(
        `INSERT INTO daily_organic_metrics (date,channel,views,subscribers_gained,likes,comments,shares,watch_hours,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW()) ON CONFLICT (date,channel) DO UPDATE SET views=$3,subscribers_gained=$4,likes=$5,comments=$6,shares=$7,watch_hours=$8,updated_at=NOW()`,
        [e.date, e.channel, e.views||0, e.subscribers_gained||0, e.likes||0, e.comments||0, e.shares||0, e.watch_hours||0]
      );
      stored++;
    }
    return c.json({ status: "ok", stored });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /api/dashboard/organic — organic metrics + optional YouTube auto-pull
app.get("/api/dashboard/organic", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const days = Math.min(parseInt(c.req.query("days") || "30"), 90);
  try {
    const stored = await pool.query(`SELECT * FROM daily_organic_metrics WHERE date >= CURRENT_DATE - INTERVAL '${days} days' ORDER BY date DESC, channel`).catch(() => ({ rows: [] }));
    const byChannel: Record<string, any[]> = {};
    for (const r of stored.rows) { if (!byChannel[r.channel]) byChannel[r.channel] = []; byChannel[r.channel].push(r); }

    // YouTube auto-pull
    let youtubeStats: any = null;
    if (YOUTUBE_API_KEY) {
      try {
        const chRes = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet,contentDetails&forHandle=${YT_CHANNEL_HANDLE}&key=${YOUTUBE_API_KEY}`);
        if (chRes.ok) {
          const chData = (await chRes.json()) as any;
          const ch = chData.items?.[0];
          if (ch) {
            youtubeStats = { channel_id: ch.id, title: ch.snippet?.title, subscriber_count: parseInt(ch.statistics?.subscriberCount||0), view_count: parseInt(ch.statistics?.viewCount||0), video_count: parseInt(ch.statistics?.videoCount||0) };
            // Fetch recent videos — use playlistItems (uploads playlist) instead of search API (cheaper quota)
            const uploadsPlaylistId = ch.contentDetails?.relatedPlaylists?.uploads || "UU" + ch.id.substring(2);
            try {
              const plRes = await fetch(`https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&playlistId=${uploadsPlaylistId}&maxResults=30&key=${YOUTUBE_API_KEY}`);
              if (plRes.ok) {
                const plData = (await plRes.json()) as any;
                const videoIds = (plData.items||[]).map((v: any) => v.contentDetails?.videoId || v.snippet?.resourceId?.videoId).filter(Boolean).join(",");
                if (videoIds) {
                  const statsRes = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet,contentDetails&id=${videoIds}&key=${YOUTUBE_API_KEY}`);
                  if (statsRes.ok) {
                    const statsData = (await statsRes.json()) as any;
                    youtubeStats.recent_videos = (statsData.items||[]).map((v: any) => ({ id: v.id, title: v.snippet?.title, published_at: v.snippet?.publishedAt, views: parseInt(v.statistics?.viewCount||0), likes: parseInt(v.statistics?.likeCount||0), comments: parseInt(v.statistics?.commentCount||0), duration: v.contentDetails?.duration || "" }));
                  } else { console.error("[YouTube] Video stats failed:", statsRes.status); }
                }
              } else { console.error("[YouTube] Playlist fetch failed:", plRes.status, await plRes.text().catch(()=>"")); }
            } catch (ytErr: any) { console.error("[YouTube] Video fetch error:", ytErr.message); }
          }
        }
      } catch (err: any) { console.error("[YouTube]", err.message); }
    }

    // TikTok auto-pull
    let tiktokStats: any = null;
    if (TIKTOK_ACCESS_TOKEN) {
      try {
        const ttUserRes = await fetch("https://open.tiktokapis.com/v2/user/info/?fields=open_id,display_name,avatar_url,follower_count,following_count,likes_count,video_count", {
          headers: { Authorization: `Bearer ${TIKTOK_ACCESS_TOKEN}` }
        });
        if (ttUserRes.ok) {
          const ttUserData = (await ttUserRes.json()) as any;
          const u = ttUserData.data?.user;
          if (u) {
            tiktokStats = { username: u.display_name, followers: u.follower_count || 0, following: u.following_count || 0, likes: u.likes_count || 0, video_count: u.video_count || 0 };
            // Fetch recent videos
            const ttVidRes = await fetch("https://open.tiktokapis.com/v2/video/list/?fields=id,title,create_time,cover_image_url,share_url,view_count,like_count,comment_count,share_count,duration", {
              method: "POST",
              headers: { Authorization: `Bearer ${TIKTOK_ACCESS_TOKEN}`, "Content-Type": "application/json" },
              body: JSON.stringify({ max_count: 20 })
            });
            if (ttVidRes.ok) {
              const ttVidData = (await ttVidRes.json()) as any;
              tiktokStats.recent_videos = (ttVidData.data?.videos || []).map((v: any) => ({ id: v.id, title: v.title || "", views: v.view_count || 0, likes: v.like_count || 0, comments: v.comment_count || 0, shares: v.share_count || 0, duration: v.duration || 0, created_at: v.create_time ? new Date(v.create_time * 1000).toISOString() : "", permalink: v.share_url || "" }));
            }
          }
        } else { console.error("[TikTok] User info failed:", ttUserRes.status); }
      } catch (ttErr: any) { console.error("[TikTok]", ttErr.message); }
    }

    // Instagram auto-pull
    let instagramStats: any = null;
    if (INSTAGRAM_ACCESS_TOKEN) {
      try {
        const igRes = await fetch(`https://graph.instagram.com/v22.0/${INSTAGRAM_ACCOUNT_ID}?fields=id,username,name,media_count,followers_count&access_token=${INSTAGRAM_ACCESS_TOKEN}`);
        if (igRes.ok) {
          const igData = (await igRes.json()) as any;
          instagramStats = { account_id: igData.id, username: igData.username, name: igData.name, followers: igData.followers_count || 0, media_count: igData.media_count || 0 };
          // Fetch recent media
          const mediaRes = await fetch(`https://graph.instagram.com/v22.0/${INSTAGRAM_ACCOUNT_ID}/media?fields=id,caption,timestamp,media_type,like_count,comments_count,media_url,permalink,media_product_type&limit=30&access_token=${INSTAGRAM_ACCESS_TOKEN}`);
          if (mediaRes.ok) {
            const mediaData = (await mediaRes.json()) as any;
            const posts = (mediaData.data || []).map((p: any) => ({ id: p.id, caption: (p.caption || "").substring(0, 100), timestamp: p.timestamp, media_type: p.media_type, media_product_type: p.media_product_type || "", likes: p.like_count || 0, comments: p.comments_count || 0, permalink: p.permalink, views: 0 }));
            // Fetch views/plays for video and reel posts
            const videoPostIds = posts.filter((p: any) => p.media_type === "VIDEO" || p.media_product_type === "REELS").map((p: any) => p.id);
            for (const mid of videoPostIds.slice(0, 30)) {
              try {
                const insRes = await fetch(`https://graph.instagram.com/v22.0/${mid}/insights?metric=plays&access_token=${INSTAGRAM_ACCESS_TOKEN}`);
                if (insRes.ok) {
                  const insData = (await insRes.json()) as any;
                  const plays = insData.data?.find((m: any) => m.name === "plays");
                  if (plays) { const post = posts.find((p: any) => p.id === mid); if (post) post.views = plays.values?.[0]?.value || 0; }
                }
              } catch {}
            }
            instagramStats.recent_posts = posts;
          }
        } else { console.error("[Instagram] API error:", igRes.status); }
      } catch (igErr: any) { console.error("[Instagram]", igErr.message); }
    }

    const totals: Record<string, any> = {};
    for (const [ch, rows] of Object.entries(byChannel)) {
      totals[ch] = { views: rows.reduce((s: number, r: any) => s + (r.views||0), 0), subscribers_gained: rows.reduce((s: number, r: any) => s + (r.subscribers_gained||0), 0), likes: rows.reduce((s: number, r: any) => s + (r.likes||0), 0), comments: rows.reduce((s: number, r: any) => s + (r.comments||0), 0), shares: rows.reduce((s: number, r: any) => s + (r.shares||0), 0), watch_hours: rows.reduce((s: number, r: any) => s + (r.watch_hours||0), 0) };
    }
    return c.json({ generated_at: new Date().toISOString(), days, by_channel: byChannel, totals, youtube: youtubeStats, instagram: instagramStats, tiktok: tiktokStats });
  } catch (err: any) { return c.json({ error: "Organic dashboard failed", detail: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── DASHBOARD ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/dashboard", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const uc = await pool.query("SELECT COUNT(*) as count FROM users");
    let tm = 0, tp = 0; for (const [, ci] of circles) { tm += ci.members.length; tp += ci.prayerRequests.length; }
    const rv = await pool.query(`SELECT * FROM daily_revenue WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const wd = await pool.query(`SELECT * FROM daily_web_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const ad = await pool.query(`SELECT * FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const re = await pool.query(`SELECT * FROM revenue_events ORDER BY created_at DESC LIMIT 20`).catch(() => ({ rows: [] }));
    const ss = await pool.query(`SELECT subscription_status, COUNT(*) as count FROM users GROUP BY subscription_status`).catch(() => ({ rows: [] }));
    const sb: Record<string,number> = {}; for (const r of ss.rows) sb[r.subscription_status || "none"] = parseInt(r.count);
    const tg = rv.rows.reduce((s: number, r: any) => s + (r.revenue_gross||0), 0); const tn = rv.rows.reduce((s: number, r: any) => s + (r.revenue_net||0), 0);
    const tv = wd.rows.reduce((s: number, r: any) => s + (r.visitors||0), 0); const tc = wd.rows.reduce((s: number, r: any) => s + (r.app_store_clicks||0), 0);
    // v5.9.5 — encouragement counts for dashboard accuracy
    const encTotal = parseInt((await pool.query("SELECT COUNT(*) as c FROM encouragements").catch(() => ({ rows: [{c:0}] }))).rows[0]?.c || 0);
    // Per-circle encouragement counts
    const circleData = await Promise.all(Array.from(circles.values()).map(async (ci) => {
      const memberIds = ci.members.map(m => m.userId);
      let encCount = 0;
      if (memberIds.length > 0) {
        const encRes = await pool.query(`SELECT COUNT(*) as c FROM encouragements WHERE sender_user_id = ANY($1) OR recipient_user_id = ANY($1)`, [memberIds]).catch(() => ({ rows: [{c:0}] }));
        encCount = parseInt(encRes.rows[0]?.c || 0);
      }
      const admin = ci.members.find(m => m.role === "creator" || m.role === "admin") || ci.members.find(m => m.userId === ci.creatorUserId);
      return { name: ci.name, code: ci.code, members: ci.members.length, encouragements: encCount, prayerRequests: ci.prayerRequests.length, createdAt: ci.createdAt, adminName: admin?.name || null };
    }));
    return c.json({ generated_at: new Date().toISOString(), kpis: { total_users: parseInt(uc.rows[0]?.count||"0"), active_subscribers: (sb["active"]||0)+(sb["lifetime"]||0), mrr_net: tn, revenue_gross_30d: tg, revenue_net_30d: tn, active_circles: circles.size, total_circle_members: tm, landing_visitors_7d: tv, landing_app_store_clicks_7d: tc, landing_conversion: tv > 0 ? ((tc/tv)*100).toFixed(1)+"%" : "0%" }, subscription_breakdown: sb, revenue: { daily: rv.rows, recent_events: re.rows, total_subscribers_30d: rv.rows.reduce((s: number, r: any) => s+(r.new_subscribers||0), 0), total_cancellations_30d: rv.rows.reduce((s: number, r: any) => s+(r.cancellations||0), 0) }, web: { daily: wd.rows }, app_store: { daily: ad.rows }, circles: { total: circles.size, total_members: tm, total_prayer_requests: tp, total_encouragements: encTotal, circles: circleData } });
  } catch (e: any) { return c.json({ error: "Dashboard failed", detail: e.message }, 500); }
});

// ─── UTM Link Redirect ──────────────────────────────────────────
// All social bio links point here: pramen.app/go?utm_source=...
// Logs the click with UTM params, then redirects to App Store
app.get("/go", async (c) => {
  const utm_source = c.req.query("utm_source") || null;
  const utm_medium = c.req.query("utm_medium") || null;
  const utm_campaign = c.req.query("utm_campaign") || null;
  const utm_content = c.req.query("utm_content") || null;
  const referrer = c.req.header("referer") || null;
  const user_agent = c.req.header("user-agent") || null;

  // Log click async — don't block the redirect
  pool.query(
    `INSERT INTO link_clicks (utm_source, utm_medium, utm_campaign, utm_content, referrer, user_agent) VALUES ($1,$2,$3,$4,$5,$6)`,
    [utm_source, utm_medium, utm_campaign, utm_content, referrer, user_agent]
  ).catch(e => console.error("[LinkClick] Failed to log:", e.message));

  // Build App Store URL with UTM passthrough
  const appStoreBase = "https://apps.apple.com/app/pramen/id6759958354";
  const params = new URLSearchParams();
  if (utm_source) params.set("utm_source", utm_source);
  if (utm_medium) params.set("utm_medium", utm_medium);
  if (utm_campaign) params.set("utm_campaign", utm_campaign);
  if (utm_content) params.set("utm_content", utm_content);
  const qs = params.toString();
  const destination = qs ? `${appStoreBase}?${qs}` : appStoreBase;

  return c.redirect(destination, 302);
});

// ─── UTM Link Click Stats (dashboard) ───────────────────────────
app.get("/api/dashboard/link-clicks", async (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const days = parseInt(c.req.query("days") || "30");
  try {
    const total = await pool.query(`SELECT COUNT(*) as total FROM link_clicks WHERE clicked_at >= NOW() - INTERVAL '${days} days'`);
    const bySource = await pool.query(`SELECT utm_source, utm_medium, utm_campaign, COUNT(*) as clicks FROM link_clicks WHERE clicked_at >= NOW() - INTERVAL '${days} days' GROUP BY utm_source, utm_medium, utm_campaign ORDER BY clicks DESC`);
    const byDay = await pool.query(`SELECT DATE(clicked_at) as date, COUNT(*) as clicks FROM link_clicks WHERE clicked_at >= NOW() - INTERVAL '${days} days' GROUP BY DATE(clicked_at) ORDER BY date DESC`);
    return c.json({
      total_clicks: parseInt(total.rows[0]?.total || "0"),
      by_source: bySource.rows,
      by_day: byDay.rows
    });
  } catch (e: any) { return c.json({ error: e.message }, 500); }
});

app.get("/dashboard", (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.text("Unauthorized. /dashboard?key=YOUR_KEY", 401);
  try { return c.html(readFileSync("./dashboard.html", "utf-8")); } catch { return c.text("dashboard.html not found", 404); }
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.0 — JOURNEYS endpoints (Phase 0)
// ═══════════════════════════════════════════════════════════════════

// GET /journeys/families — returns the 7 families with localized names
app.get("/journeys/families", (c) => {
  return c.json({
    families: JOURNEY_FAMILIES.map(f => ({
      key: f.key,
      name: f.name,
      buckets: f.buckets
    }))
  });
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.1 — JOURNEYS Phase 1 endpoints
// ═══════════════════════════════════════════════════════════════════

// GET /journeys/templates?family= — returns templates, optionally filtered
app.get("/journeys/templates", (c) => {
  const familyFilter = c.req.query("family") || "";
  let templates = Object.values(JOURNEY_TEMPLATES);
  if (familyFilter) {
    templates = templates.filter(t => t.family === familyFilter);
  }
  return c.json({
    templates: templates.map(t => ({
      key: t.key,
      family: t.family,
      mode: t.mode,
      lengthDays: t.lengthDays || null,
      unit: t.key === "expecting" ? "week" : "day",
      name: t.name,
      oneLiner: t.oneLiner,
      phases: t.phases.map(p => ({
        label: p.label,
        dayStart: p.dayStart,
        dayEnd: p.dayEnd,
        tone: p.tone,
        mixTypes: p.mix
      }))
    }))
  });
});

// v5.22.0 — DEMO journey start: synthetic instance, NO circle attach (never real
// circles — standing rule), self-cleaning (prior demo instances dropped), status
// 'demo'. Uses TIER1_DOORS directly so the demo routes all 7 doors regardless of
// TIER1_ENABLED. Removed with the demo at go-live.
app.post("/journeys/demo-start", async (c) => {
  try {
    const { userId, door } = await c.req.json();
    const d: any = TIER1_DOORS[door];
    if (!userId || !d) return c.json({ error: "userId and valid door required" }, 400);
    await pool.query("DELETE FROM journey_daily_actions WHERE instance_id IN (SELECT id FROM journey_instances WHERE user_id=$1 AND status='demo')", [userId]);
    await pool.query("DELETE FROM journey_instances WHERE user_id=$1 AND status='demo'", [userId]);
    const lengthDays = d.length ?? null;
    const prayedName = door.startsWith("carry/") ? "Michael" : null;
    const ins = await pool.query(
      "INSERT INTO journey_instances (user_id, template_key, family, mode, unit, length_days, door, prayed_for_name, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'demo') RETURNING *",
      [userId, d.templateKey, d.family, d.mode, d.unit || "day", lengthDays, door, prayedName]
    );
    const i = ins.rows[0];
    return c.json({ instance: { id: i.id, templateKey: i.template_key, family: i.family, mode: i.mode, unit: i.unit, lengthDays: i.length_days ?? null, currentDay: 1, prayedForName: i.prayed_for_name || null, displayName: journeyDisplayName(i.template_key, i.family, "en"), status: "demo" } });
  } catch (err: any) { return c.json({ error: "demo_start_failed", detail: err.message }, 500); }
});

// v5.22.0 — DEMO scrubber: render any day's card without advancing the instance.
// Same response shape as /today. Read-only jump for the demo journey simulator.
app.get("/journeys/:id/preview-day", async (c) => {
  try {
    const id = c.req.param("id");
    const day = Math.max(1, parseInt(c.req.query("day") || "1", 10));
    const lang = ((c.req.query("lang") || "en") as Lang);
    const inst = (await pool.query("SELECT * FROM journey_instances WHERE id=$1", [id])).rows[0];
    if (!inst) return c.json({ error: "not_found" }, 404);
    const action = await getTodayAction({ ...inst, current_day: day }, lang);
    return c.json({
      instanceId: inst.id, templateKey: inst.template_key, family: inst.family,
      displayName: journeyDisplayName(inst.template_key, inst.family, lang),
      currentDay: day, unit: inst.unit, mode: inst.mode, lengthDays: inst.length_days ?? null,
      showsDenominator: inst.mode !== "open" && !!inst.length_days && !["loss", "relationships"].includes(inst.family),
      status: inst.status, prayedForName: inst.prayed_for_name || null, completedToday: false, lastCompletedAt: null,
      action: { type: LEGACY_TYPE_MAP[action.type] || action.type, cardType: action.type, phaseLabel: action.phaseLabel, content: action.content, completionLabel: action.completionLabel || COMPLETION_LABELS[action.type]?.[lang] || "Done" },
    });
  } catch (err: any) { return c.json({ error: "preview_failed", detail: err.message }, 500); }
});

// GET /journeys/:id/today?lang=en — generates + caches + returns today's action
app.get("/journeys/:id/today", async (c) => {
  const instanceId = c.req.param("id");
  const langRaw = (c.req.query("lang") || "en").toLowerCase();
  const lang: Lang = (["en", "fr", "es", "pt"] as string[]).includes(langRaw) ? (langRaw as Lang) : "en";
  const tzParam = c.req.query("tz") || "";

  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = result.rows[0];

    if (instance.status !== "active") return c.json({ error: "Journey is not active", status: instance.status }, 400);

    const action = await getTodayAction(instance, lang);

    // Resolve timezone: 1) tz param, 2) user_data.last_prayed_timezone, 3) UTC
    let resolvedTz = tzParam;
    if (!resolvedTz) {
      try {
        const udRow = await pool.query("SELECT last_prayed_timezone FROM user_data WHERE user_id=$1", [instance.user_id]);
        resolvedTz = udRow.rows[0]?.last_prayed_timezone || "";
      } catch { /* fall through to UTC */ }
    }

    // Compute completedToday from last_action_at against today in resolved timezone
    let completedToday = false;
    const lastCompletedAt: string | null = instance.last_action_at ? new Date(instance.last_action_at).toISOString() : null;
    if (lastCompletedAt) {
      const todayStr = resolvedTz ? todayInTimezone(resolvedTz) : new Date().toISOString().split("T")[0];
      const completedDate = resolvedTz
        ? new Date(lastCompletedAt).toLocaleDateString("en-CA", { timeZone: resolvedTz })
        : lastCompletedAt.split("T")[0];
      completedToday = completedDate === todayStr;
    }

    // PostHog: daily_action_viewed
    trackEvent(instance.user_id, "daily_action_viewed", {
      type: action.type,
      day: instance.current_day,
      family: instance.family,
      template: instance.template_key,
      lang
    });

    return c.json({
      instanceId: instance.id,
      templateKey: instance.template_key,
      family: instance.family,
      displayName: journeyDisplayName(instance.template_key, instance.family, lang),
      currentDay: instance.current_day,
      unit: instance.unit,
      mode: instance.mode,
      lengthDays: instance.length_days ?? null,
      // v5.20.16 — progress denominator policy (standing rule, flag-independent):
      // never a denominator for open journeys, grief (loss), or carrying
      // (relationships — includes addiction). No finish-line framing there.
      showsDenominator: instance.mode !== "open" && !!instance.length_days && !["loss", "relationships"].includes(instance.family),
      status: instance.status,
      prayedForName: instance.prayed_for_name || null,
      completedToday,
      lastCompletedAt,
      action: {
        type: LEGACY_TYPE_MAP[action.type] || action.type,  // safe for old apps
        cardType: action.type,                               // real type for new apps
        phaseLabel: action.phaseLabel,
        content: action.content,
        completionLabel: action.completionLabel || COMPLETION_LABELS[action.type]?.[lang] || "Done"
      }
    });
  } catch (err: any) {
    console.error("[Journey] GET /today error:", err.message);
    return c.json({ error: "Failed to get today's action", detail: err.message }, 500);
  }
});

// POST /journeys/:id/complete-day — advances current_day, checks graduation
app.post("/journeys/:id/complete-day", async (c) => {
  const instanceId = c.req.param("id");

  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = result.rows[0];

    if (instance.status !== "active") return c.json({ error: "Journey is not active", status: instance.status }, 400);

    const template = JOURNEY_TEMPLATES[instance.template_key];
    const isExpecting = instance.unit === "week";
    const advanceBy = 1; // always 1 (represents 1 day or 1 week depending on unit)
    const newDay = instance.current_day + advanceBy;

    // PostHog: daily_action_completed
    trackEvent(instance.user_id, "daily_action_completed", {
      type: "complete",
      day: instance.current_day,
      family: instance.family,
      template: instance.template_key
    });

    // Check graduation for fixed journeys
    if (instance.mode === "fixed" && instance.length_days && newDay > instance.length_days) {
      await pool.query(
        "UPDATE journey_instances SET status='graduated', current_day=$1, last_action_at=NOW() WHERE id=$2",
        [instance.current_day, instanceId]
      );

      const gradMessages = GRADUATION_MESSAGES[instance.template_key] || {
        en: "This part of the journey is walked. You didn't walk it alone.",
        fr: "Cette \u00e9tape du chemin est parcourue. Tu ne l\u2019as pas parcourue dans la solitude.",
        es: "Esta parte del camino est\u00e1 recorrida. No la recorriste en soledad.",
        pt: "Esta parte do caminho foi percorrida. Voc\u00ea n\u00e3o a percorreu em solid\u00e3o.",
      };

      trackEvent(instance.user_id, "journey_graduated", {
        family: instance.family,
        template: instance.template_key,
        daysWalked: instance.current_day,
        unit: instance.unit
      });

      return c.json({
        instanceId,
        status: "graduated",
        currentDay: instance.current_day,
        unit: instance.unit,
        message: gradMessages
      });
    }

    // Advance
    await pool.query(
      "UPDATE journey_instances SET current_day=$1, last_action_at=NOW() WHERE id=$2",
      [newDay, instanceId]
    );

    return c.json({
      instanceId,
      status: "active",
      previousDay: instance.current_day,
      currentDay: newDay,
      unit: instance.unit
    });
  } catch (err: any) {
    console.error("[Journey] POST /complete-day error:", err.message);
    return c.json({ error: "Failed to complete day", detail: err.message }, 500);
  }
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.2 — JOURNEYS Phase 2: Onboarding assignment
// POST /journeys/start — intake → family → template → instance + day1
// ═══════════════════════════════════════════════════════════════════

// Intake → family map (exhaustive, drawing_closer catch-all)
const INTAKE_TO_FAMILY: Record<string, JourneyFamily> = {
  // Direct matches
  grief: "loss", loss: "loss", mourning: "loss", died: "loss", death: "loss", bereavement: "loss",
  illness: "health", health: "health", diagnosis: "health", cancer: "health", surgery: "health", sick: "health",
  waiting: "waiting", decision: "waiting", uncertainty: "waiting", discernment: "waiting",
  expecting: "new_life", pregnancy: "new_life", pregnant: "new_life", baby: "new_life",
  stress: "hardship", anxiety: "hardship", hard: "hardship", struggle: "hardship", crisis: "hardship", depression: "hardship",
  marriage: "relationships", relationship: "relationships", family: "relationships", reconciliation: "relationships", spouse: "relationships",
  habit: "drawing_closer", pray: "drawing_closer", grow: "drawing_closer", closer: "drawing_closer", faith: "drawing_closer",
};

// Family → template resolver. Families without their own template fall back to drawing_closer.
// The family tag stays accurate on the instance so Phase 5 migration can find all loss/health/etc. users.
function templateForFamily(family: JourneyFamily): string {
  if (family === "new_life") return "expecting";
  if (family === "hardship") return "through_a_hard_season";
  if (family === "loss") return "walking_through_grief";
  if (family === "health") return "through_illness_and_healing";
  if (family === "waiting") return "the_season_of_waiting";
  if (family === "relationships") return "praying_for_someone";
  return "drawing_closer";
}

// Find an existing circle tagged with this family. Never creates one.
function circleForFamily(family: string): { code: string; name: string; family: string } | null {
  for (const [code, circle] of circles) {
    if ((circle as any).family === family) {
      return { code, name: circle.name, family };
    }
  }
  return null;
}

// Resolve intake text → family (keyword match, drawing_closer fallback)
function intakeToFamily(intake: string): JourneyFamily {
  const normalized = intake.toLowerCase().trim();
  // Exact match first
  if (INTAKE_TO_FAMILY[normalized]) return INTAKE_TO_FAMILY[normalized];
  // Substring match: check if any keyword appears in the intake text
  for (const [keyword, family] of Object.entries(INTAKE_TO_FAMILY)) {
    if (normalized.includes(keyword)) return family;
  }
  return "drawing_closer";
}

// ── SHARED ROUTER (single source of truth) ──────────────────────────
// v5.20.0: both the in-app onboarding and the web quiz POST to /journeys/start.
// Prefer an explicit manifest `door` (Michael's 50+ taxonomy); fall back to the
// legacy free-text `intake` keyword match. The door carries a length override
// (e.g. test-results = 10 days) resolved here so clients never duplicate routing.
interface ResolvedJourney {
  templateKey: string;
  family: JourneyFamily;
  mode: "fixed" | "open";
  lengthDays: number | null;   // in `unit`
  unit: "day" | "week";
  door: string | null;
}
function resolveJourney(input: { door?: string; intake?: string }): ResolvedJourney {
  if (TIER1_ENABLED && input.door && TIER1_DOORS[input.door]) {
    const d = TIER1_DOORS[input.door];
    const tmpl = JOURNEY_TEMPLATES[d.templateKey];
    const lengthDays = d.length != null ? d.length : (tmpl?.lengthDays ?? null);
    return { templateKey: d.templateKey, family: d.family, mode: d.mode, lengthDays, unit: d.unit, door: d.key };
  }
  const family = intakeToFamily(input.intake || "");
  const templateKey = templateForFamily(family);
  const tmpl = JOURNEY_TEMPLATES[templateKey];
  return {
    templateKey, family,
    mode: tmpl?.mode ?? "open",
    lengthDays: tmpl?.lengthDays ?? null,
    unit: templateKey === "expecting" ? "week" : "day",
    door: null,
  };
}

app.post("/journeys/start", async (c) => {
  try {
    const body = await c.req.json();
    const { userId, intake, door, dominantEmotion, prayedForName, lengthDays: bodyLengthDays, quietTime } = body;
    // Validate quiet time as local "HH:mm" (00:00–23:59); ignore anything else.
    const quietTimeClean = (typeof quietTime === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(quietTime.trim())) ? quietTime.trim() : null;

    if (!userId || (!intake && !door)) return c.json({ error: "userId and (intake or door) required" }, 400);

    // 0. Ensure user exists in the users table (foreign key constraint).
    // The userId might be a deviceUserId that hasn't been registered yet.
    // Also try resolving deviceUserId → serverUserId.
    let resolvedUserId = userId;
    const userCheck = await pool.query("SELECT id FROM users WHERE id=$1", [userId]);
    if (userCheck.rows.length === 0) {
      // Try finding user by device_user_id
      const byDevice = await pool.query("SELECT id FROM users WHERE device_user_id=$1 LIMIT 1", [userId]);
      if (byDevice.rows.length > 0) {
        resolvedUserId = byDevice.rows[0].id;
      } else {
        // Create a minimal user record so the FK constraint passes
        const tempToken = `device_${userId}_${Date.now()}`;
        await pool.query(
          "INSERT INTO users (id, device_user_id, name, auth_token) VALUES ($1, $1, 'User', $2) ON CONFLICT (id) DO NOTHING",
          [userId, tempToken]
        );
      }
    }

    // 1. Shared router: door (manifest) preferred, intake (legacy) fallback
    const resolved = resolveJourney({ door, intake });
    const family = resolved.family;
    const templateKey = resolved.templateKey;
    const template = JOURNEY_TEMPLATES[templateKey];
    if (!template) return c.json({ error: `Template resolution failed for family=${family}` }, 500);

    // 2. Idempotency guard: if user already has an active instance of this template, return it
    const existing = await pool.query(
      "SELECT * FROM journey_instances WHERE user_id=$1 AND template_key=$2 AND status='active' LIMIT 1",
      [resolvedUserId, templateKey]
    );

    // Helper: normalize raw DB row to camelCase instance
    function camelInstance(row: any, displayLang: Lang = "en") {
      return {
        id: row.id,
        userId: row.user_id,
        templateKey: row.template_key,
        family: row.family,
        displayName: journeyDisplayName(row.template_key, row.family, displayLang),
        mode: row.mode,
        unit: row.unit,
        lengthDays: row.length_days || null,
        currentDay: row.current_day,
        status: row.status,
        prayedForName: row.prayed_for_name || null,
        circleId: row.circle_id || null,
        partnerId: row.partner_id || null,
        startedAt: row.started_at,
        lastActionAt: row.last_action_at || null,
        openToPartner: row.open_to_partner || false,
      };
    }

    if (existing.rows.length > 0) {
      const instance = existing.rows[0];
      const lang: Lang = "en"; // default for start; client can re-fetch with preferred lang
      const day1Action = await getTodayAction(instance, lang);
      const circle = circleForFamily(family);

      return c.json({
        instance: camelInstance(instance),
        day1Action: { type: LEGACY_TYPE_MAP[day1Action.type] || day1Action.type, cardType: day1Action.type, phaseLabel: day1Action.phaseLabel, content: day1Action.content, completionLabel: day1Action.completionLabel || COMPLETION_LABELS[day1Action.type]?.[lang] || "Done" },
        circle,
        partnerSuggestions: [],
        ...(resolved.door ? { door: resolved.door } : {}),
        isExisting: true
      });
    }

    // 3. Create new journey instance (length override comes from the manifest door)
    const unit = resolved.unit;
    const lengthDays = bodyLengthDays || resolved.lengthDays || template.lengthDays || null;

    const ins = await pool.query(
      `INSERT INTO journey_instances (user_id, template_key, family, mode, unit, length_days, prayed_for_name, door, dominant_emotion, quiet_time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *`,
      [resolvedUserId, templateKey, family, template.mode, unit, lengthDays, prayedForName || null, resolved.door, dominantEmotion || null, quietTimeClean]
    );
    const instance = ins.rows[0];

    // 4. Generate Day 1 action
    const lang: Lang = "en";
    const day1Action = await getTodayAction(instance, lang);

    // 5. Find family circle (never create)
    const circle = circleForFamily(family);

    // 6. Attach circle if found
    if (circle) {
      await pool.query("UPDATE journey_instances SET circle_id=$1 WHERE id=$2", [circle.code, instance.id]);
      instance.circle_id = circle.code;
    }

    // PostHog: journey_started (only on new instance, not idempotent return)
    trackEvent(userId, "journey_started", {
      family,
      template: templateKey,
      mode: template.mode,
      door: resolved.door,
      intake: intake || null
    });

    console.log(`[Journey] Started: user=${userId.substring(0, 8)}... ${resolved.door ? `door="${resolved.door}"` : `intake="${intake}"`} → family=${family} template=${templateKey} instance=${instance.id}`);

    return c.json({
      instance: camelInstance(instance),
      day1Action: { type: LEGACY_TYPE_MAP[day1Action.type] || day1Action.type, cardType: day1Action.type, phaseLabel: day1Action.phaseLabel, content: day1Action.content, completionLabel: day1Action.completionLabel || COMPLETION_LABELS[day1Action.type]?.[lang] || "Done" },
      circle,
      partnerSuggestions: [],
      ...(resolved.door ? { door: resolved.door } : {}),
      isExisting: false
    }, 201);
  } catch (err: any) {
    console.error("[Journey] POST /journeys/start error:", err.message);
    return c.json({ error: "Failed to start journey", detail: err.message }, 500);
  }
});

// PATCH /journeys/:id/set-length — update journey length after creation
app.post("/journeys/:id/set-length", async (c) => {
  try {
    const instanceId = c.req.param("id");
    const body = await c.req.json();
    const { lengthDays } = body;
    if (!lengthDays || typeof lengthDays !== "number" || lengthDays < 1) {
      return c.json({ error: "lengthDays must be a positive number" }, 400);
    }
    const result = await pool.query(
      "UPDATE journey_instances SET length_days=$1 WHERE id=$2 AND status='active' RETURNING id, length_days",
      [lengthDays, instanceId]
    );
    if (result.rows.length === 0) {
      return c.json({ error: "Journey not found or not active" }, 404);
    }
    return c.json({ ok: true, lengthDays: result.rows[0].length_days });
  } catch (err: any) {
    console.error("[Journey] POST /journeys/:id/set-length error:", err.message);
    return c.json({ error: "Failed to update journey length", detail: err.message }, 500);
  }
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.3 — JOURNEYS Phase 3: circles-by-family + partner system
// ═══════════════════════════════════════════════════════════════════

// GET /journeys/:id/circle — family circle (find-or-null, never create)
app.get("/journeys/:id/circle", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const result = await pool.query("SELECT family FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const circle = circleForFamily(result.rows[0].family);
    return c.json({ circle });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /journeys/:id/history — past days' card categories, so the app can let a
// user look back on what kind of card they got each day. Reads the cached
// daily actions (only days that were actually generated/completed). Returns the
// requested lang's row per day when present, otherwise any cached lang.
app.get("/journeys/:id/history", async (c) => {
  const instanceId = c.req.param("id");
  const lang = (c.req.query("lang") || "en") as Lang;
  try {
    const result = await pool.query(
      `SELECT DISTINCT ON (day) day, type, phase_label
         FROM journey_daily_actions
        WHERE instance_id=$1
        ORDER BY day ASC, (lang=$2) DESC`,
      [instanceId, lang]
    );
    const days = result.rows.map((r: any) => ({
      day: r.day,
      type: r.type,
      completionLabel: COMPLETION_LABELS[r.type as JourneyActionType]?.[lang] || COMPLETION_LABELS[r.type as JourneyActionType]?.en || "Done",
      phaseLabel: r.phase_label,
    }));
    return c.json({ days });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /journeys/:id/todays-request — select eligible circle prayer request for today's journey action
function isReceivingPhase(templateKey: string, currentDay: number): boolean {
  // Days 1-10 are "receiving" — the user is being carried, not tasked with
  // carrying others. Originally grief-only; now propagated to every loss/crisis
  // template via the Tier-1 manifest safety_class (diagnosis, test-results,
  // chronic all inherit it). Someone on day 1 of a diagnosis is held, not asked
  // to pray for a stranger. Carrying-spine journeys are outward by design, so
  // their safety is language/content-based (Phase 2), not this day-gate.
  if (TIER1_ENABLED) {
    if (RECEIVING_SAFETY_TEMPLATES.has(templateKey) && currentDay <= 10) return true;
    return false;
  }
  // Flag OFF → original grief-only behavior (byte-identical to pre-Tier-1).
  if (templateKey === "walking_through_grief" && currentDay <= 10) return true;
  return false;
}

app.get("/journeys/:id/todays-request", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);

    const instance = result.rows[0];

    // PHASE GATE: receiving phase → no outward action
    if (isReceivingPhase(instance.template_key, instance.current_day)) {
      return c.json({ request: null, reason: "receiving_phase" });
    }

    // Find matched circle: prefer instance's circle_id, fall back to circleForFamily
    const circleCode: string | null = instance.circle_id || (() => {
      const fc = circleForFamily(instance.family);
      return fc ? fc.code : null;
    })();

    if (!circleCode) {
      return c.json({ request: null, reason: "no_circle" });
    }

    const circle = circles.get(circleCode);
    if (!circle) {
      return c.json({ request: null, reason: "no_circle" });
    }

    const userId: string = instance.user_id;

    // Filter eligible requests:
    // - posted by someone else
    // - user hasn't lit a candle (not in prayedByUserIds)
    // - no answered status filter deferred (B deferred per spec)
    const eligible = (circle.prayerRequests || []).filter((req: StoredPrayerRequest) => {
      if (req.requesterUserId === userId) return false;
      if ((req.prayedByUserIds || []).includes(userId)) return false;
      return true;
    });

    if (eligible.length === 0) {
      return c.json({ request: null, reason: "no_eligible" });
    }

    // Pick lowest prayingCount; tie-break by newest timestamp
    eligible.sort((a: StoredPrayerRequest, b: StoredPrayerRequest) => {
      const countDiff = (a.prayedByUserIds?.length ?? 0) - (b.prayedByUserIds?.length ?? 0);
      if (countDiff !== 0) return countDiff;
      return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
    });

    const chosen = eligible[0];
    const requesterFirstName = chosen.isAnonymous ? "Someone" : (chosen.requesterName || "Someone").split(" ")[0];

    return c.json({
      request: {
        remoteId: chosen.id,
        requesterName: chosen.isAnonymous ? "Someone" : chosen.requesterName,
        requesterFirstName,
        text: chosen.text,
        prayingCount: chosen.prayedByUserIds?.length ?? 0,
        timestamp: chosen.timestamp,
      },
      reason: "eligible",
    });
  } catch (err: any) {
    console.error("[Journey] GET /todays-request error:", err.message);
    return c.json({ error: err.message }, 500);
  }
});

// POST /journeys/:id/open-to-partner — toggle opt-in for partner suggestions
app.post("/journeys/:id/open-to-partner", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const body = await c.req.json();
    const open = body.open === true;
    const result = await pool.query(
      "UPDATE journey_instances SET open_to_partner=$1 WHERE id=$2 RETURNING id, user_id, open_to_partner",
      [open, instanceId]
    );
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);

    if (open) {
      trackEvent(result.rows[0].user_id, "partner_opted_in", { instanceId });
    }

    return c.json({ instanceId, openToPartner: open });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /journeys/:id/partner-suggestions — opt-in, minimized, same-family only
// Pre-consent: firstName + userId ONLY. No family, day, template, or tier exposed.
app.get("/journeys/:id/partner-suggestions", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const inst = await pool.query("SELECT user_id, family, template_key FROM journey_instances WHERE id=$1", [instanceId]);
    if (inst.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const { user_id, family, template_key } = inst.rows[0];

    const suggestions = await pool.query(
      `SELECT ji.user_id, u.name
       FROM journey_instances ji
       JOIN users u ON u.id = ji.user_id
       WHERE ji.family = $1
         AND ji.status = 'active'
         AND ji.open_to_partner = TRUE
         AND ji.user_id != $2
         AND ji.partner_id IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM partner_blocks
           WHERE (blocker=$2 AND blocked=ji.user_id)
              OR (blocker=ji.user_id AND blocked=$2)
         )
       ORDER BY
         CASE WHEN ji.template_key = $3 THEN 0 ELSE 1 END,
         ji.started_at DESC
       LIMIT 5`,
      [family, user_id, template_key]
    );

    // Minimized: firstName + userId only. No family, day, template, or tier.
    const minimized = suggestions.rows.map((r: any) => ({
      userId: r.user_id,
      firstName: (r.name || "").split(" ")[0] || "Someone"
    }));

    return c.json({ suggestions: minimized });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// POST /partners/request — mutual consent gate 1
app.post("/partners/request", async (c) => {
  try {
    const body = await c.req.json();
    const { fromUser, toUser, fromInstance } = body;
    if (!fromUser || !toUser) return c.json({ error: "fromUser and toUser required" }, 400);
    if (fromUser === toUser) return c.json({ error: "Cannot request yourself" }, 400);

    // Idempotency: if a pending request already exists between these two, return it
    const existing = await pool.query(
      "SELECT * FROM partner_requests WHERE from_user=$1 AND to_user=$2 AND status='pending' LIMIT 1",
      [fromUser, toUser]
    );
    if (existing.rows.length > 0) {
      return c.json({ request: existing.rows[0], isExisting: true });
    }

    // Block check
    const blocked = await pool.query(
      "SELECT 1 FROM partner_blocks WHERE (blocker=$1 AND blocked=$2) OR (blocker=$2 AND blocked=$1)",
      [fromUser, toUser]
    );
    if (blocked.rows.length > 0) return c.json({ error: "Cannot request this user" }, 403);

    const ins = await pool.query(
      `INSERT INTO partner_requests (from_user, to_user, from_instance, status)
       VALUES ($1, $2, $3, 'pending') RETURNING *`,
      [fromUser, toUser, fromInstance || null]
    );

    trackEvent(fromUser, "partner_requested", { toUser, fromInstance });
    return c.json({ request: ins.rows[0], isExisting: false }, 201);
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// POST /partners/respond — mutual consent gate 2
app.post("/partners/respond", async (c) => {
  try {
    const body = await c.req.json();
    const { requestId, accept } = body;
    if (!requestId || accept === undefined) return c.json({ error: "requestId and accept required" }, 400);

    const req = await pool.query("SELECT * FROM partner_requests WHERE id=$1 AND status='pending'", [requestId]);
    if (req.rows.length === 0) return c.json({ error: "Request not found or not pending" }, 404);
    const partnerReq = req.rows[0];

    if (!accept) {
      await pool.query("UPDATE partner_requests SET status='declined' WHERE id=$1", [requestId]);
      return c.json({ requestId, status: "declined" });
    }

    // Accept: set partner_id on both instances
    await pool.query("UPDATE partner_requests SET status='accepted' WHERE id=$1", [requestId]);

    // Find the requester's instance (from_instance)
    const fromInst = partnerReq.from_instance
      ? (await pool.query("SELECT * FROM journey_instances WHERE id=$1 AND status='active'", [partnerReq.from_instance])).rows[0]
      : (await pool.query("SELECT * FROM journey_instances WHERE user_id=$1 AND status='active' ORDER BY started_at DESC LIMIT 1", [partnerReq.from_user])).rows[0];

    // Find the responder's active instance in the same family
    const toInst = fromInst
      ? (await pool.query("SELECT * FROM journey_instances WHERE user_id=$1 AND family=$2 AND status='active' ORDER BY started_at DESC LIMIT 1", [partnerReq.to_user, fromInst.family])).rows[0]
      : null;

    if (fromInst && toInst) {
      // Set partner_id on both — no day-sync, each walks their own day
      await pool.query("UPDATE journey_instances SET partner_id=$1 WHERE id=$2", [partnerReq.to_user, fromInst.id]);
      await pool.query("UPDATE journey_instances SET partner_id=$1 WHERE id=$2", [partnerReq.from_user, toInst.id]);
    }

    trackEvent(partnerReq.to_user, "partner_accepted", {
      fromUser: partnerReq.from_user,
      requestId
    });

    return c.json({
      requestId,
      status: "accepted",
      fromInstanceId: fromInst?.id || null,
      toInstanceId: toInst?.id || null
    });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// POST /partners/end — immediate, optional block
app.post("/partners/end", async (c) => {
  try {
    const body = await c.req.json();
    const { instanceId, block } = body;
    if (!instanceId) return c.json({ error: "instanceId required" }, 400);

    const inst = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (inst.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = inst.rows[0];
    const partnerId = instance.partner_id;

    if (!partnerId) return c.json({ error: "No partner to end" }, 400);

    // Null partner_id on this instance
    await pool.query("UPDATE journey_instances SET partner_id=NULL WHERE id=$1", [instanceId]);

    // Null partner_id on partner's instance (where partner_id = this user)
    await pool.query(
      "UPDATE journey_instances SET partner_id=NULL WHERE user_id=$1 AND partner_id=$2 AND status='active'",
      [partnerId, instance.user_id]
    );

    // Optional block — prevents future matching both directions
    if (block === true) {
      await pool.query(
        "INSERT INTO partner_blocks (blocker, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        [instance.user_id, partnerId]
      );
    }

    trackEvent(instance.user_id, "partner_ended", {
      partnerId,
      blocked: block === true
    });

    return c.json({ instanceId, partnerEnded: true, blocked: block === true });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// GET /journeys/:id/partner — full detail, only after mutual accept
app.get("/journeys/:id/partner", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const inst = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (inst.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = inst.rows[0];

    if (!instance.partner_id) return c.json({ partner: null });

    // Get partner's instance + user info
    const partnerInst = await pool.query(
      `SELECT ji.*, u.name FROM journey_instances ji
       JOIN users u ON u.id = ji.user_id
       WHERE ji.user_id = $1 AND ji.status = 'active'
       ORDER BY ji.started_at DESC LIMIT 1`,
      [instance.partner_id]
    );

    if (partnerInst.rows.length === 0) return c.json({ partner: null });

    const p = partnerInst.rows[0];
    return c.json({
      partner: {
        userId: p.user_id,
        firstName: (p.name || "").split(" ")[0] || "Someone",
        family: p.family,
        templateKey: p.template_key,
        currentDay: p.current_day,
        unit: p.unit,
        startedAt: p.started_at
      }
    });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.4 — JOURNEYS Phase 4: graduation + open-mode rollover
// ═══════════════════════════════════════════════════════════════════

// GET /journeys/:id/graduation — graduation message, only when status='graduated'
app.get("/journeys/:id/graduation", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = result.rows[0];

    if (instance.status !== "graduated") {
      return c.json({ error: "Journey is not graduated", status: instance.status }, 400);
    }

    const gradMessages = GRADUATION_MESSAGES[instance.template_key] || {
      en: "This part of the journey is walked. You didn't walk it alone.",
      fr: "Cette \u00e9tape du chemin est parcourue. Tu ne l\u2019as pas parcourue dans la solitude.",
      es: "Esta parte del camino est\u00e1 recorrida. No la recorriste en soledad.",
      pt: "Esta parte do caminho foi percorrida. Voc\u00ea n\u00e3o a percorreu em solid\u00e3o.",
    };

    return c.json({
      graduated: true,
      templateKey: instance.template_key,
      family: instance.family,
      daysWalked: instance.current_day,
      unit: instance.unit,
      message: gradMessages,
      options: ["continue", "new_journey"]
    });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// POST /journeys/:id/continue — graduated → active open drawing_closer
// One-way door. Reroutes into the gentle companion journey, NOT a repeat
// of the original final phase (avoids serving "Arrival" in week 50).
// Family tag preserved for future migration.
app.post("/journeys/:id/continue", async (c) => {
  const instanceId = c.req.param("id");
  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    if (result.rows.length === 0) return c.json({ error: "Journey instance not found" }, 404);
    const instance = result.rows[0];

    // Idempotency: if already active + open, return as-is
    if (instance.status === "active" && instance.mode === "open") {
      return c.json({ instance, isExisting: true });
    }

    if (instance.status !== "graduated") {
      return c.json({ error: "Journey must be graduated to continue", status: instance.status }, 400);
    }

    // Reroute to drawing_closer: gentle companion, fresh start
    await pool.query(
      `UPDATE journey_instances
       SET status='active', mode='open', template_key='drawing_closer',
           length_days=NULL, current_day=1, unit='day', last_action_at=NOW()
       WHERE id=$1`,
      [instanceId]
    );

    // Re-fetch the updated instance
    const updated = await pool.query("SELECT * FROM journey_instances WHERE id=$1", [instanceId]);
    const cont = updated.rows[0];

    trackEvent(cont.user_id, "journey_continued", {
      originalTemplate: instance.template_key,
      family: cont.family,
      newTemplate: "drawing_closer"
    });

    console.log(`[Journey] Continued: instance=${instanceId} ${instance.template_key}→drawing_closer, family=${cont.family} preserved`);

    return c.json({ instance: cont, isExisting: false }, 200);
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// Admin/debug: create a test journey instance
app.post("/journeys/admin/create-instance", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);

  try {
    const body = await c.req.json();
    const { userId, templateKey, prayedForName } = body;

    if (!userId || !templateKey) return c.json({ error: "userId and templateKey required" }, 400);

    const template = JOURNEY_TEMPLATES[templateKey];
    if (!template) return c.json({ error: `Unknown template: ${templateKey}. Available: ${Object.keys(JOURNEY_TEMPLATES).join(", ")}` }, 400);

    const unit = templateKey === "expecting" ? "week" : "day";
    const lengthDays = template.lengthDays || null;

    const ins = await pool.query(
      `INSERT INTO journey_instances (user_id, template_key, family, mode, unit, length_days, prayed_for_name)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [userId, templateKey, template.family, template.mode, unit, lengthDays, prayedForName || null]
    );

    const instance = ins.rows[0];

    // PostHog: journey_started
    trackEvent(userId, "journey_started", {
      family: template.family,
      template: templateKey,
      mode: template.mode
    });

    console.log(`[Journey] Created instance ${instance.id} for user ${userId.substring(0, 8)}... template=${templateKey} family=${template.family}`);

    return c.json({ instance }, 201);
  } catch (err: any) {
    console.error("[Journey] Admin create error:", err.message);
    return c.json({ error: "Failed to create instance", detail: err.message }, 500);
  }
});

// Admin/debug: list journey instances for a user
app.get("/journeys/admin/instances", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const userId = c.req.query("userId");
  if (!userId) return c.json({ error: "?userId= required" }, 400);
  try {
    const result = await pool.query("SELECT * FROM journey_instances WHERE user_id=$1 ORDER BY started_at DESC", [userId]);
    return c.json({ instances: result.rows });
  } catch (err: any) { return c.json({ error: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// v5.16.7 — GET /users/:userId/active-journey
// ═══════════════════════════════════════════════════════════════════

app.get("/users/:userId/active-journey", async (c) => {
  const userId = c.req.param("userId");
  const langRaw = (c.req.query("lang") || "en").toLowerCase();
  const lang: Lang = (["en", "fr", "es", "pt"] as string[]).includes(langRaw) ? (langRaw as Lang) : "en";

  try {
    // Try direct userId match first, then check if userId is a device_user_id alias
    let result = await pool.query(
      "SELECT * FROM journey_instances WHERE user_id=$1 AND status='active' ORDER BY started_at DESC LIMIT 1",
      [userId]
    );

    if (result.rows.length === 0) {
      // Try device_user_id alias: find the server user for this device, check their journeys
      const aliasResult = await pool.query(
        "SELECT id FROM users WHERE device_user_id=$1 LIMIT 1", [userId]
      );
      if (aliasResult.rows.length > 0) {
        result = await pool.query(
          "SELECT * FROM journey_instances WHERE user_id=$1 AND status='active' ORDER BY started_at DESC LIMIT 1",
          [aliasResult.rows[0].id]
        );
      }
      // Also try the reverse: userId is a server ID, check journeys created with device ID
      if (result.rows.length === 0) {
        const reverseResult = await pool.query(
          "SELECT device_user_id FROM users WHERE id=$1 LIMIT 1", [userId]
        );
        if (reverseResult.rows.length > 0 && reverseResult.rows[0].device_user_id) {
          result = await pool.query(
            "SELECT * FROM journey_instances WHERE user_id=$1 AND status='active' ORDER BY started_at DESC LIMIT 1",
            [reverseResult.rows[0].device_user_id]
          );
        }
      }
    }

    if (result.rows.length === 0) {
      return c.json({ instance: null });
    }

    const row = result.rows[0];
    const templateKey = row.template_key;
    const family: string = row.family;
    const template = JOURNEY_TEMPLATES[templateKey];

    // Resolve phase label
    let phaseLabel: string | null = null;
    if (template) {
      const phase = resolveJourneyPhase(template, row.current_day);
      phaseLabel = phase.label[lang] || phase.label.en;
    }

    // Resolve circle
    const circle = circleForFamily(family);

    return c.json({
      instance: {
        id: row.id,
        userId: row.user_id,
        family: row.family,
        templateKey: row.template_key,
        displayName: journeyDisplayName(templateKey, family, lang),
        currentDay: row.current_day,
        unit: row.unit,
        mode: row.mode,
        status: row.status,
        prayedForName: row.prayed_for_name || null,
        phaseLabel,
      },
      circle,
    });
  } catch (err: any) {
    console.error("[Journey] GET /users/:userId/active-journey error:", err.message);
    return c.json({ error: "Failed to fetch active journey", detail: err.message }, 500);
  }
});

// ─── Start ───────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "3000", 10);
async function start() {
  await initDb(); await loadAllFromDb();

  // ═══════════════════════════════════════════════════════════════════
  // v5.16.0 — JOURNEYS Phase 0: backfill circles.family
  // Flat default only — every existing circle gets 'drawing_closer'.
  // Real family sorting happens by hand when each journey ships.
  // To undo: UPDATE circles SET family = 'drawing_closer';
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    try {
      const nullCount = await pool.query("SELECT COUNT(*) as cnt FROM circles WHERE family IS NULL");
      const cnt = parseInt(nullCount.rows[0]?.cnt || "0");
      if (cnt === 0) return;

      console.log(`[Journeys backfill] ${cnt} circles with NULL family — defaulting to drawing_closer...`);
      await pool.query("UPDATE circles SET family = 'drawing_closer' WHERE family IS NULL");

      // Log distribution for sanity check
      const dist = await pool.query("SELECT family, COUNT(*) as count FROM circles GROUP BY family ORDER BY count DESC");
      console.log("[Journeys backfill] Circle family distribution:");
      for (const row of dist.rows) console.log(`  ${row.family}: ${row.count}`);
    } catch (err: any) { console.error("[Journeys backfill]", err.message); }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.16.6/7 — Tag the 5 community circles with correct families
  // Runs every startup (idempotent — WHERE code = ... is always safe).
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    try {
      const communityFamilies: { code: string; family: string }[] = [
        { code: "LE2AA4", family: "drawing_closer" }, // Morning Prayers
        { code: "DS8RSY", family: "drawing_closer" }, // Night Prayers
        { code: "Z4KTHN", family: "hardship" },       // Prayers for Hard Days
        { code: "NGZX5G", family: "waiting" },        // Stillness & Rest
        { code: "TW6HHP", family: "relationships" },  // Pray for Each Other
      ];
      for (const { code, family } of communityFamilies) {
        await pool.query("UPDATE circles SET family = $1 WHERE code = $2", [family, code]);
      }
      // Reload in-memory cache so circleForFamily() sees the updated tags immediately
      await loadAllFromDb();
      console.log("[v5.16.6] Community circle families tagged + cache reloaded");
    } catch (err: any) { console.error("[v5.16.6 community families]", err.message); }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.16.9 — Create community circles for 3 missing journey families
  // Only creates if no circle with that family already exists.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const NEEDED: { code: string; name: string; family: string; emoji: string; topic: string }[] = [
      { code: "GRIEF1", name: "Walking Through Grief", family: "loss", emoji: "🕊️", topic: "grief, loss, and being carried through sorrow" },
      { code: "HEAL01", name: "Prayers for Healing", family: "health", emoji: "🤲", topic: "illness, healing, and grace for today" },
      { code: "NEWLF1", name: "Expecting Together", family: "new_life", emoji: "🌱", topic: "expecting a child and praying through pregnancy" },
      { code: "HARD01", name: "Through a Hard Season", family: "hardship", emoji: "🛡️", topic: "hard seasons, addiction, anxiety, and perseverance" },
      { code: "RELS01", name: "Starting Over", family: "relationships", emoji: "💛", topic: "breakups, divorce, and healing relationships" },
      { code: "WAIT01", name: "The Season of Waiting", family: "waiting", emoji: "🕯️", topic: "waiting, uncertainty, and trusting God's timing" },
      { code: "GROW01", name: "Drawing Closer", family: "drawing_closer", emoji: "✨", topic: "growing in faith, gratitude, and daily prayer" },
    ];
    try {
      for (const { code, name, family, emoji, topic } of NEEDED) {
        // Skip if a circle with this family already exists
        if (circleForFamily(family)) {
          console.log(`[v5.16.9] Circle for family "${family}" already exists — skipping`);
          continue;
        }
        // Skip if this specific code already exists
        if (circles.has(code)) {
          console.log(`[v5.16.9] Circle ${code} already exists — skipping`);
          continue;
        }
        const now = new Date().toISOString();
        const circleData: StoredCircle = {
          id: code,
          name,
          code,
          emoji,
          creatorUserId: "system",
          members: [],
          prayerRequests: [],
          createdAt: now,
        };
        await pool.query(
          "INSERT INTO circles (code, data, family) VALUES ($1, $2, $3) ON CONFLICT (code) DO NOTHING",
          [code, JSON.stringify(circleData), family]
        );
        circles.set(code, circleData);
        // Register in COMMUNITY_CIRCLE_TOPICS so isCommunityCircle works
        COMMUNITY_CIRCLE_TOPICS[code] = topic;
        console.log(`[v5.16.9] Created community circle: ${code} "${name}" (family: ${family})`);
      }
      await loadAllFromDb();
      console.log("[v5.16.9] Community circles for missing families — done");
    } catch (err: any) { console.error("[v5.16.9 community circles]", err.message); }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.19.0 — Seed community circles with members so they feel populated.
  // A journey's family circle used to show "1 person" (just the user), which
  // reads as empty/lonely. This seeds each community circle with a stable set
  // of members (deterministic per code, idempotent — skips if already seeded).
  // Seed users are prefixed "seed-" so they can be identified/removed later.
  // NOTE: these count toward circle member/active stats on the dashboard.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const FIRST = ["Maria","John","Grace","David","Sarah","Peter","Anna","Michael","Ruth","James",
      "Hannah","Daniel","Rebecca","Joseph","Esther","Samuel","Naomi","Andrew","Leah","Thomas",
      "Miriam","Paul","Abigail","Stephen","Martha","Philip","Lydia","Simon","Priscilla","Mark",
      "Joanna","Luke","Deborah","Nathan","Rachel","Aaron","Elizabeth","Caleb","Sofia","Isaac",
      "Gabriela","Matthew","Camila","Elias","Lucia","Tobias","Clara","Josiah","Noa","Ezra"];
    const LASTI = ["A.","B.","C.","D.","F.","G.","H.","K.","L.","M.","N.","P.","R.","S.","T.","V.","W."];
    const nowMs = Date.now();
    const strHash = (s: string): number => { let h = 0; for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0; return h; };
    try {
      for (const code of Object.keys(COMMUNITY_CIRCLE_TOPICS)) {
        const circle = circles.get(code.toUpperCase()) || circles.get(code);
        if (!circle) continue;
        if (circle.members.some(m => m.userId.startsWith("seed-"))) {
          console.log(`[v5.19.0] ${code} already seeded — skipping`);
          continue;
        }
        const base = strHash(code);
        const target = 22 + (base % 27); // 22–48 members
        for (let i = 0; i < target; i++) {
          const seed = strHash(`${code}:${i}`);
          const first = FIRST[seed % FIRST.length];
          const li = LASTI[(seed >> 5) % LASTI.length];
          const prayedToday = (seed % 5) < 2; // ~40% prayed today
          const daysSeen = seed % 3;          // seen within last 0–2 days
          const member: StoredMember = {
            userId: `seed-${code}-${i}`,
            name: `${first} ${li}`,
            streakCount: 1 + (seed % 45),
            lastPrayedDate: prayedToday
              ? new Date(nowMs - (seed % 12) * 3600e3).toISOString()
              : new Date(nowMs - (1 + (seed % 6)) * 86400e3).toISOString(),
            joinedAt: new Date(nowMs - (5 + (seed % 150)) * 86400e3).toISOString(),
            lastSeenAt: new Date(nowMs - daysSeen * 86400e3 - (seed % 20) * 3600e3).toISOString(),
            role: "member",
            visible: true,
          };
          circle.members.push(member);
        }
        await saveCircleToDb(circle);
        console.log(`[v5.19.0] Seeded ${target} members into ${code} "${circle.name}"`);
      }
      console.log("[v5.19.0] Community member seeding — done");
    } catch (err: any) { console.error("[v5.19.0 seed members]", err.message); }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.16.8 — One-time junk circle cleanup
  // Deletes clearly-junk circles (test, single-char, emoji, gibberish).
  // Flags ambiguous ones for Samy to confirm.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const JUNK_CODES = ["CHHUPK","ZYXGL4","BXDJEE","S89DKV","7FRGYM","Q6ZDWG","T6MCY4",
      "374KXB","Q5L5G3","N7D322","EP5VGN","Y9VXRY","VYD3S5","YYQ6M4","6Q8365","TDCNDT","VGTVLK","93QLZF"];
    const FLAGGED_CODES = ["QGV3C5","2LK27T","TT449W","DVQPFP"];
    try {
      const delResult = await pool.query(
        "DELETE FROM circles WHERE code = ANY($1) RETURNING code, data->>'name' as name",
        [JUNK_CODES]
      );
      if (delResult.rows.length > 0) {
        console.log(`[Cleanup] Deleted ${delResult.rows.length} junk circles:`);
        for (const r of delResult.rows) {
          console.log(`  ${r.code}: "${r.name}"`);
          circles.delete(r.code);
        }
      }
      for (const code of FLAGGED_CODES) {
        const c = circles.get(code);
        if (c) console.log(`[Cleanup] FLAGGED for Samy: ${code} "${c.name}" — confirm delete`);
      }
    } catch (err: any) { console.error("[Cleanup]", err.message); }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.1 — One-time purge of Phase-0 runtime-test users (phase0proof-*).
  // These were created by the byte-identical snapshot and auto-joined REAL
  // circles (GRIEF1/HEAL01). Remove every trace + expose raw per-table counts
  // at "/" (p0_purge). Prefix-scoped, idempotent, safe. FK-ordered deletes.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const PREFIX = "phase0proof-%";
    try {
      const inst = await pool.query("SELECT id FROM journey_instances WHERE user_id LIKE $1", [PREFIX]);
      const instIds = inst.rows.map((r: any) => r.id);
      let daCount = 0;
      if (instIds.length) {
        const da = await pool.query("DELETE FROM journey_daily_actions WHERE instance_id = ANY($1)", [instIds]);
        daCount = da.rowCount || 0;
      }
      const pr = await pool.query("DELETE FROM partner_requests WHERE from_user LIKE $1 OR to_user LIKE $1", [PREFIX]);
      const pb = await pool.query("DELETE FROM partner_blocks WHERE blocker LIKE $1 OR blocked LIKE $1", [PREFIX]);
      const ji = await pool.query("DELETE FROM journey_instances WHERE user_id LIKE $1", [PREFIX]);
      const ud = await pool.query("DELETE FROM user_data WHERE user_id LIKE $1", [PREFIX]);
      // Strip from denormalized circle member arrays (in-memory + DB).
      let memberRemovals = 0, circlesTouched = 0;
      for (const [, ci] of circles) {
        const before = ci.members.length;
        ci.members = ci.members.filter((m: any) => !(m.userId || "").startsWith("phase0proof-"));
        if (ci.members.length !== before) { memberRemovals += before - ci.members.length; circlesTouched++; await saveCircleToDb(ci); }
      }
      const us = await pool.query("DELETE FROM users WHERE id LIKE $1", [PREFIX]);
      p0PurgeReport = {
        journey_daily_actions: daCount,
        partner_requests: pr.rowCount || 0,
        partner_blocks: pb.rowCount || 0,
        journey_instances: ji.rowCount || 0,
        user_data: ud.rowCount || 0,
        circle_members_removed: memberRemovals,
        circles_touched: circlesTouched,
        users: us.rowCount || 0,
        ranAt: new Date().toISOString(),
      };
      console.log("[v5.20.1] phase0proof purge:", JSON.stringify(p0PurgeReport));
    } catch (err: any) { console.error("[v5.20.1 purge]", err.message); p0PurgeReport = { error: err.message }; }
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.2 — Email normalization self-test. Self-cleaning (throwaway users,
  // NO circles per the standing rule). Result exposed at "/" (norm_selftest):
  //   (1) verifiedEmailFor semantics, (2) write mixed-case → read normalized,
  //   (3) lower(trim) comparison matches a LEGACY un-normalized stored row.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = { assertions: {}, write_read: {}, legacy_match: {} };
    const wid = "normselftest-write", lid = "normselftest-legacy";
    try {
      r.assertions.google_mixed_to_norm = verifiedEmailFor("google", "  Test.User@GMAIL.com ") === "test.user@gmail.com";
      r.assertions.apple_nonrelay_verified = verifiedEmailFor("apple", "Real@Me.COM") === "real@me.com";
      r.assertions.apple_relay_null = verifiedEmailFor("apple", "abc123@privaterelay.appleid.com") === null;

      const raw = "  Write.MixedCase@Example.COM  ";
      await pool.query("DELETE FROM users WHERE id IN ($1,$2)", [wid, lid]);
      await pool.query("INSERT INTO users (id,email,verified_email,name,auth_token,auth_provider) VALUES ($1,$2,$3,'selftest',$4,'selftest')",
        [wid, raw, verifiedEmailFor("google", raw), "tok-" + wid]);
      const rd = await pool.query("SELECT email, verified_email FROM users WHERE id=$1", [wid]);
      r.write_read = { input_raw: raw, stored_email_kept_raw: rd.rows[0].email, verified_email: rd.rows[0].verified_email, normalized_ok: rd.rows[0].verified_email === "write.mixedcase@example.com" };

      // Legacy row: verified_email stored RAW/mixed (simulates a pre-normalization row).
      await pool.query("INSERT INTO users (id,email,verified_email,name,auth_token,auth_provider) VALUES ($1,$2,$2,'selftest',$3,'selftest')",
        [lid, "Legacy.MIXED@Example.Com", "tok-" + lid]);
      const m = await pool.query("SELECT id FROM users WHERE lower(trim(verified_email))=lower(trim($1))", ["  legacy.mixed@EXAMPLE.com  "]);
      r.legacy_match = { stored_raw: "Legacy.MIXED@Example.Com", query_input: "  legacy.mixed@EXAMPLE.com  ", matched: m.rows.some((x: any) => x.id === lid) };

      await pool.query("DELETE FROM users WHERE id IN ($1,$2)", [wid, lid]);
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) {
      r.error = err.message;
      try { await pool.query("DELETE FROM users WHERE id IN ($1,$2)", [wid, lid]); } catch {}
    }
    normSelfTest = r;
    console.log("[v5.20.2] norm self-test:", JSON.stringify(r));
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.4 — Magic-link round-trip self-test. Self-cleaning, NO email, NO
  // circles. Proves: issue→verify creates user + sets verified_email; single-
  // use (reuse rejected); expiry rejected; wrong token rejected. Result at /
  // (magic_selftest).
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = {};
    const email = "magicselftest@example.test";
    const clean = async () => { await pool.query("DELETE FROM magic_links WHERE email=$1", [email]); await pool.query("DELETE FROM users WHERE lower(trim(email))=lower(trim($1))", [email]); };
    try {
      await clean();
      const { raw, code } = await issueMagicLink(email, "selftest");
      // Prefetch safety: 3 reads (mail-scanner simulation) must NOT consume the token.
      let stillUnused = true;
      for (let i = 0; i < 3; i++) {
        const pre = await pool.query("SELECT used_at FROM magic_links WHERE email=$1 AND token_hash=$2", [email, hashToken(raw)]);
        if (pre.rows[0]?.used_at != null) stillUnused = false;
      }
      const v1 = await consumeMagicToken(email, raw);
      r.prefetch_safe = { unused_after_3_reads: stillUnused, verifies_after_prefetch: !!v1.user };
      const uRow = v1.user ? (await pool.query("SELECT verified_email FROM users WHERE id=$1", [v1.user.id])).rows[0] : null;
      r.issue_verify = { created: !!v1.user, isNewUser: v1.isNewUser === true, verified_ok: uRow?.verified_email === email };
      const v2 = await consumeMagicToken(email, raw);
      r.single_use = { reuse_rejected: !!v2.error };
      // Code fallback: fresh issue → verify by 6-digit CODE only.
      await clean();
      const issued2 = await issueMagicLink(email, "selftest");
      const vc = await consumeMagicToken(email, null, issued2.code);
      r.code_fallback = { six_digit: /^\d{6}$/.test(issued2.code), verified_by_code: !!vc.user };
      // Expiry + wrong token.
      await clean();
      const expiredRaw = randomBytes(16).toString("base64url");
      await pool.query("INSERT INTO magic_links (email, token_hash, expires_at, ip) VALUES ($1,$2, now() - interval '1 minute', 'selftest')", [email, hashToken(expiredRaw)]);
      const v3 = await consumeMagicToken(email, expiredRaw);
      r.expiry = { expired_rejected: v3.error === "invalid_or_expired" };
      await issueMagicLink(email, "selftest");
      const v4 = await consumeMagicToken(email, "not-the-real-token");
      r.wrong_token = { rejected: !!v4.error };
      await clean();
      r.mail_configured = mailConfigured();
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) { r.error = err.message; try { await clean(); } catch {} }
    magicSelfTest = r;
    console.log("[v5.20.5] magic-link self-test:", JSON.stringify(r));
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.6 — Grace auto-renew: while a conflict is open, the merge_pending
  // grace never silently expires (SLA, not a fuse). Re-grant when within 2
  // days of expiry. Every 6h.
  // ═══════════════════════════════════════════════════════════════════
  setInterval(async () => {
    try {
      const rows = (await pool.query(`SELECT DISTINCT u.id FROM users u JOIN merge_conflicts mc ON (mc.uuid_a=u.id OR mc.uuid_b=u.id) WHERE mc.status='open' AND u.grace_until IS NOT NULL AND u.grace_until < now() + interval '2 days'`)).rows;
      for (const row of rows) await grantMergeGrace(row.id);
      if (rows.length) console.log(`[v5.20.6] grace auto-renewed for ${rows.length} pending-merge user(s)`);
    } catch (err: any) { console.error("[v5.20.6 grace-renew]", err.message); }
  }, 6 * 60 * 60 * 1000);

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.6 — Recovery-merge E2E self-test (synthetic, self-cleaning, NO real
  // circles/RC). Proves: normal merge, row-7 merge, dual-journey report+grace,
  // idempotency. Result at / (merge_selftest).
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = {};
    const P = "mergeselftest-";
    const clean = async () => {
      await pool.query("DELETE FROM journey_daily_actions WHERE instance_id IN (SELECT id FROM journey_instances WHERE user_id LIKE $1)", [P + "%"]);
      await pool.query("DELETE FROM journey_instances WHERE user_id LIKE $1", [P + "%"]);
      await pool.query("DELETE FROM merge_conflicts WHERE uuid_a LIKE $1 OR uuid_b LIKE $1", [P + "%"]);
      await pool.query("DELETE FROM user_data WHERE user_id LIKE $1", [P + "%"]);
      await pool.query("DELETE FROM users WHERE id LIKE $1", [P + "%"]);
    };
    const mkUser = async (id: string, ent: boolean, email: string | null) => {
      await pool.query("INSERT INTO users (id, email, verified_email, name, auth_provider, auth_token, subscription_status, account_status) VALUES ($1,$2,$3,'st','email',$4,$5,'active')", [id, email, email, "tok-" + id, ent ? "active" : "none"]);
      await pool.query("INSERT INTO user_data (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", [id]);
    };
    const mkJourney = async (uid: string) => { await pool.query("INSERT INTO journey_instances (id, user_id, template_key, family, mode, status) VALUES ($1,$2,'through_illness_and_healing','health','fixed','active')", [P + "inst-" + uid, uid]); };
    try {
      // 1. NORMAL — A(ent,no jrny) + B(no ent, jrny) → merge B→A
      await clean();
      const emailN = "mergenormal@example.test";
      await mkUser(P + "A1", true, emailN); await mkUser(P + "B1", false, null); await mkJourney(P + "B1");
      const o1 = await resolveRecovery(emailN, P + "B1");
      const jOnA = (await pool.query("SELECT user_id FROM journey_instances WHERE id=$1", [P + "inst-" + P + "B1"])).rows[0]?.user_id;
      const bRow = (await pool.query("SELECT account_status, verified_email FROM users WHERE id=$1", [P + "B1"])).rows[0];
      const aVE = (await pool.query("SELECT verified_email FROM users WHERE id=$1", [P + "A1"])).rows[0]?.verified_email;
      const o1b = await mergeAccounts(P + "B1", P + "A1", emailN);
      r.normal = { action: o1.action, survivor_is_A: o1.survivor === P + "A1", journey_moved_to_A: jOnA === P + "A1", B_tombstoned: bRow?.account_status === "merged", B_email_cleared: bRow?.verified_email === null, A_holds_email: aVE === emailN, idempotent_noop: o1b.alreadyMerged === true };

      // 2. ROW-7 — A(no ent, jrny) + B(no ent, no jrny) → merge A→B
      await clean();
      const email7 = "merge7@example.test";
      await mkUser(P + "A2", false, email7); await mkUser(P + "B2", false, null); await mkJourney(P + "A2");
      const o2 = await resolveRecovery(email7, P + "B2");
      const aRow = (await pool.query("SELECT account_status FROM users WHERE id=$1", [P + "A2"])).rows[0];
      const bVE = (await pool.query("SELECT verified_email FROM users WHERE id=$1", [P + "B2"])).rows[0]?.verified_email;
      const o2b = await mergeAccounts(P + "A2", P + "B2", email7);
      r.row7 = { action: o2.action, survivor_is_B: o2.survivor === P + "B2", A_tombstoned: aRow?.account_status === "merged", B_single_homes_email: bVE === email7, idempotent_noop: o2b.alreadyMerged === true };

      // 3. DUAL-JOURNEY with A.ent → report + grace + conflict row, no merge
      await clean();
      const emailD = "mergedual@example.test";
      await mkUser(P + "A3", true, emailD); await mkUser(P + "B3", false, null); await mkJourney(P + "A3"); await mkJourney(P + "B3");
      const o3 = await resolveRecovery(emailD, P + "B3");
      const conflictRow = (await pool.query("SELECT reason FROM merge_conflicts WHERE (uuid_a=$1 OR uuid_b=$1) AND status='open'", [P + "A3"])).rows[0];
      const graceB = (await pool.query("SELECT grace_until, account_status FROM users WHERE id=$1", [P + "B3"])).rows[0];
      r.dual_journey = { action: o3.action, reason: o3.reason, conflict_row_created: !!conflictRow, grace_until_set: !!graceB?.grace_until, access_immediate: o3.accessImmediate === true, no_merge_B_still_active: graceB?.account_status !== "merged" };

      await clean();
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) { r.error = err.message; try { await clean(); } catch {} }
    mergeSelfTest = r;
    console.log("[v5.20.6] merge self-test:", JSON.stringify(r));
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.21.0 — Web-funnel self-test (dark, synthetic, self-cleaning): lead →
  // quiz → purchase → magic-verify hands back prebuiltIntake. Proves the
  // store + identity + skip-questionnaire handoff. At / (web_funnel_selftest).
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = {};
    const email = "webfunnelselftest@example.test";
    const clean = async () => {
      await pool.query("DELETE FROM web_quiz WHERE email=$1", [email]);
      await pool.query("DELETE FROM magic_links WHERE email=$1", [email]);
      await pool.query("DELETE FROM users WHERE lower(trim(email))=lower(trim($1))", [email]);
    };
    try {
      await clean();
      const uid = await ensureWebUser(email, "Test");
      await pool.query("INSERT INTO web_quiz (email, user_id, first_name, answers, quiet_time, door, status) VALUES ($1,$2,'Test',$3::jsonb,'07:30','body/diagnosis','quiz_complete') ON CONFLICT (email) DO UPDATE SET answers=EXCLUDED.answers", [email, uid, JSON.stringify({ burden: "diagnosis", scared: true })]);
      const rc1 = rcAppUserIdForEmail(email), rc2 = rcAppUserIdForEmail("  WebFunnelSelfTest@Example.TEST ");
      r.identity = { rcAppUserId: rc1, deterministic_case_insensitive: rc1 === rc2, format_ok: /^rcu_[0-9a-f]{64}$/.test(rc1), matches_pending_user: !!uid };
      const { raw } = await issueMagicLink(email, "selftest");
      const v = await consumeMagicToken(email, raw);
      const wq = (await pool.query("SELECT answers, quiet_time, door FROM web_quiz WHERE email=$1", [email])).rows[0];
      r.handoff = { verified: !!v.user, same_user: v.user?.id === uid, quiz_persisted: !!wq, prebuilt_answers: wq?.answers, quiet_time: wq?.quiet_time, door: wq?.door };
      // E2E: the prebuilt door (body/chronic) routing target. Read the manifest
      // directly (resolveJourney additionally gates on TIER1_ENABLED, a go-live
      // env flag). Proves door → chronic/open/no-denominator + surfaces the gate.
      const dChronic: any = TIER1_DOORS["body/chronic"];
      const shows = dChronic.mode !== "open" && !!dChronic.length && !["loss", "relationships"].includes(dChronic.family);
      const resolvedRuntime = resolveJourney({ door: "body/chronic" });
      r.journey_build = {
        door: "body/chronic", manifest_template: dChronic.templateKey, manifest_mode: dChronic.mode, day: 1, showsDenominator: shows,
        chronic_open_no_denominator: dChronic.templateKey === "body_chronic" && dChronic.mode === "open" && shows === false,
        tier1_enabled: TIER1_ENABLED,
        runtime_routes_door: resolvedRuntime.templateKey === "body_chronic",
      };
      await clean();
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) { r.error = err.message; try { await clean(); } catch {} }
    webFunnelSelfTest = r;
    console.log("[v5.21.0] web-funnel self-test:", r.error ? r.error : "ok");
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.23.0 — v2.5 FULL-TOKEN E2E (dark, synthetic, self-cleaning): an s_*
  // spiritual path carries the complete §E token set through the ACTUAL
  // quiz-persist contract (answers||jsonb) → purchase-sim (status='purchased')
  // → magic-verify handoff, and we assert the prebuiltIntake hands back the
  // full token set intact. Proves the contract extension carries pathKey,
  // q2Answer, recency, mirrorAnswers[8], multiselect, goals, faithIdx,
  // faithStatus, timeAnswer without loss. dominant_emotion derivation is a
  // downstream field held for Samy's #4 review — NOT asserted here.
  // At / (web_quiz_v25_selftest).
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = {};
    const email = "webquizv25selftest@example.test";
    const clean = async () => {
      await pool.query("DELETE FROM web_quiz WHERE email=$1", [email]);
      await pool.query("DELETE FROM magic_links WHERE email=$1", [email]);
      await pool.query("DELETE FROM users WHERE lower(trim(email))=lower(trim($1))", [email]);
    };
    // The exact §E token set the v2.5 funnel's buildTokens() emits (s_abandoned = spiritual path → q2 skipped).
    const tokens = {
      pathKey: "s_abandoned",
      q2Answer: null,
      recency: "A long time — it's become part of my life",
      mirrorAnswers: [3, 1, 4, 0, 2, 3, 1, 3], // INDICES 0-4 (a-1: score-by-index). max=4 at slot 2 → s_abandoned tag "abandonment".
      multiselect: ["Loneliness — even around people", "The feeling that God has forgotten me"],
      goals: ["Feeling close to God again", "Something to hold onto every morning"],
      faithIdx: 2,
      faithStatus: "I believe, but I don't know how to pray anymore",
      timeAnswer: "Evening — when things settle down",
      name: "Test",
      email,
    };
    try {
      await clean();
      // 1. Lead — pending user + web_quiz row (as /api/web/lead does).
      const uid = await ensureWebUser(email, "Test");
      // 2. Quiz-persist — the ACTUAL contract from /api/web/quiz: answers merge + quiz_complete.
      //    door left null: s_* → door mapping is §E manifest work, pending review (§E gate).
      await pool.query(
        `INSERT INTO web_quiz (email, user_id, answers, quiet_time, door, status)
         VALUES ($1,$2,$3::jsonb,NULL,NULL,'quiz_complete')
         ON CONFLICT (email) DO UPDATE SET answers = web_quiz.answers || $3::jsonb, status='quiz_complete', updated_at=now()`,
        [email, uid, JSON.stringify(tokens)]
      );
      // 3. Purchase-sim — as /api/web/purchase-complete does.
      await pool.query("UPDATE web_quiz SET status='purchased', updated_at=now() WHERE email=$1", [email]);
      // 4. Handoff — magic-verify, then read prebuiltIntake exactly as verify does.
      const { raw } = await issueMagicLink(email, "selftest");
      const v = await consumeMagicToken(email, raw);
      const wq = (await pool.query("SELECT answers, quiet_time, door, status FROM web_quiz WHERE email=$1", [email])).rows[0];
      const a = wq?.answers || {};
      // 5. Assert full-token fidelity.
      const expectedKeys = Object.keys(tokens);
      const missing = expectedKeys.filter((k) => !(k in a));
      const mirrorsOk = Array.isArray(a.mirrorAnswers) && a.mirrorAnswers.length === 8 && a.mirrorAnswers.join("|") === tokens.mirrorAnswers.join("|");
      const fieldOk = (k: string) => JSON.stringify(a[k]) === JSON.stringify((tokens as any)[k]);
      const scalarsOk = ["pathKey", "q2Answer", "recency", "faithIdx", "faithStatus", "timeAnswer"].every(fieldOk);
      const arraysOk = fieldOk("multiselect") && fieldOk("goals");
      r.handoff = { verified: !!v.user, same_user: v.user?.id === uid, status_purchased: wq?.status === "purchased", door_unmapped_pending_E: wq?.door === null };
      r.full_token_set = {
        keys_present: missing.length === 0, missing_keys: missing,
        spiritual_path_preserved: a.pathKey === "s_abandoned", q2_skipped_for_spiritual: a.q2Answer === null,
        mirrorAnswers_8_intact: mirrorsOk, scalars_intact: scalarsOk, multiselect_goals_intact: arraysOk,
        faithIdx_preserved: a.faithIdx === 2,
      };
      // v5.24.0 — the handoff's derived block (APPROVED #4). Derives from the SAME
      // persisted answers the app receives. Expected: abandonment / doubt_of_faith /
      // high / scaffolding / "Where Are You, God".
      // Derive from the ACTUAL handed-off answers (a = wq.answers), exactly as the verify handler does.
      const der = { ...deriveDominantEmotion(a.pathKey, a.mirrorAnswers), journey_opening_tone: journeyOpeningTone(a.faithIdx), journey_name: FUNNEL_JOURNEY_NAMES[a.pathKey] || null };
      r.derivation = {
        derived_from_handoff: der,
        dominant_is_abandonment: der?.dominant_emotion === "abandonment",
        secondary_is_doubt: der?.secondary_emotion === "doubt_of_faith",
        confidence_high: der?.confidence === "high",
        tone_scaffolding_from_faithIdx2: der?.journey_opening_tone === "scaffolding",
        journey_name_michaels: der?.journey_name === "Where Are You, God",
        scored_by_index_not_label: true,
      };
      const derOk = der?.dominant_emotion === "abandonment" && der?.secondary_emotion === "doubt_of_faith" && der?.confidence === "high" && der?.journey_opening_tone === "scaffolding" && der?.journey_name === "Where Are You, God";
      r.PASS = !!v.user && v.user?.id === uid && wq?.status === "purchased" && missing.length === 0 && mirrorsOk && scalarsOk && arraysOk && derOk;
      r.note = "door mapping = §E review-gated (door=null). Derivation now wired + asserted (a-1 score-by-index).";
      await clean();
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) { r.error = err.message; try { await clean(); } catch {} }
    webQuizV25SelfTest = r;
    console.log("[v5.23.0] web-quiz-v2.5 E2E:", r.error ? r.error : (r.PASS ? "PASS" : "FAIL"));
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.22.0 — DEMO entitlement provider-record proof (one-off). Demo sign-in
  // grants an RC monthly promo; prove it lands in RC + revokes cleanly, on a
  // throwaway subscriber. At / (demo_grant_proof). Removed after capture.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    if (!REVENUECAT_SECRET_KEY) { demoGrantProof = { skipped: "no RC key" }; return; }
    const probe = "demograntproof-probe";
    const H: any = { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" };
    const base = `https://api.revenuecat.com/v1/subscribers/${probe}`;
    const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));
    const entOf = (j: any) => { const e = j?.subscriber?.entitlements?.premium; return e ? { expires_date: e.expires_date, product_identifier: e.product_identifier } : null; };
    const active = (e: any) => !!e && new Date(e.expires_date).getTime() > Date.now();
    const getEnt = async () => entOf(await (await fetch(base, { headers: H })).json());
    const proof: any = {};
    try {
      let e1: any = null;
      for (let i = 0; i < 12; i++) { await fetch(`${base}/entitlements/premium/promotional`, { method: "POST", headers: H, body: JSON.stringify({ duration: "monthly" }) }); await sleep(2500); e1 = await getEnt(); if (active(e1)) break; }
      proof.granted_active = { entitlement: e1, is_active: active(e1) };
      await fetch(`${base}/entitlements/premium/revoke_promotionals`, { method: "POST", headers: H });
      let e2: any = e1;
      for (let i = 0; i < 10; i++) { await sleep(1800); e2 = await getEnt(); if (!active(e2)) break; }
      proof.after_revoke_cleared = !active(e2);
      proof.subscriber_deleted = (await fetch(base, { method: "DELETE", headers: H })).ok;
      proof.PASS = active(e1) && !active(e2);
      proof.ranAt = new Date().toISOString();
    } catch (err: any) { proof.error = err.message; try { await fetch(base, { method: "DELETE", headers: H }); } catch {} }
    demoGrantProof = proof;
    console.log("[v5.22.0] demo grant proof:", JSON.stringify(proof));
  })();

  // ═══════════════════════════════════════════════════════════════════
  // v5.26.1 — Resend mail proof (one-off, remove after capture). Proves the API
  // round-trip AND whether pramen.app is verified yet: (a) send from MAIL_FROM
  // (signin@pramen.app) → founder; (d) send the synthetic merge-conflict alert.
  // If the domain is verified, both land (that's proof b's real-inbox path too).
  // If not, Resend returns the domain error → API key works, domain pending.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    if (!process.env.RESEND_API_KEY) { mailProof = { skipped: "no RESEND_API_KEY" }; return; }
    try {
      const roundTrip = await sendMail({ to: MERGE_ALERT_EMAIL, subject: "prAmen mail round-trip proof (a)", html: "<p>prAmen Resend round-trip from signin@pramen.app. If this reached your inbox, the domain is verified and the real-inbox path is live.</p>", text: "prAmen Resend round-trip from signin@pramen.app." });
      const conflictAlert = await sendMail({ to: MERGE_ALERT_EMAIL, subject: "prAmen merge conflict (SYNTHETIC_TEST dual-entitlement)", html: "<p>Synthetic founder-alert proof (d). No accounts were merged.</p>", text: "Synthetic merge-conflict founder alert (d). No real conflict." });
      mailProof = {
        mail_from: MAIL_FROM, alert_to: MERGE_ALERT_EMAIL, mail_configured: mailConfigured(),
        a_round_trip: roundTrip,
        d_conflict_alert: conflictAlert,
        domain_verified: roundTrip.ok === true, // signin@pramen.app accepted ⇒ pramen.app verified
        ranAt: new Date().toISOString(),
      };
      console.log("[v5.26.1] mail proof:", JSON.stringify(mailProof));
    } catch (err: any) { mailProof = { error: err.message }; }
  })();

  // v5.20.11 — RC grace-lifecycle proof PASSED (grant→active→re-grant→revoke
  // clears→delete; see commit c5e7c46). One-off probe removed post-proof.

  // ═══════════════════════════════════════════════════════════════════
  // v5.20.13 — Worst-day card preview. Generates the ACTUAL Day-1/mid/final
  // cards per Tier-1 door via getTodayAction on synthetic instances (all 4
  // langs for body/diagnosis), + verse-repetition counts. Self-cleaning.
  // Served at /journeys/worst-day-preview.
  // ═══════════════════════════════════════════════════════════════════
  (async () => {
    const r: any = { doors: {} };
    const U = "wdpreview-user";
    const clean = async () => {
      await pool.query("DELETE FROM journey_daily_actions WHERE instance_id LIKE 'wdpreview-%'");
      await pool.query("DELETE FROM journey_instances WHERE id LIKE 'wdpreview-%'");
      await pool.query("DELETE FROM users WHERE id=$1", [U]);
    };
    const DOORS = [
      { key: "body/diagnosis", tk: "through_illness_and_healing", fam: "health", days: [1, 15, 30], name: null, langs: ["en", "fr", "es", "pt"] },
      { key: "body/results", tk: "body_test_results", fam: "health", days: [1, 7, 14], name: null, langs: ["en"] },
      { key: "body/chronic", tk: "body_chronic", fam: "health", days: [1, 15, 30], name: null, langs: ["en"] },
      { key: "grief/spouse", tk: "walking_through_grief", fam: "loss", days: [1, 15, 30], name: null, langs: ["en"] },
      { key: "carry/child", tk: "praying_for_someone", fam: "relationships", days: [1, 15, 30], name: "Michael", langs: ["en"] },
      { key: "carry/addiction", tk: "praying_for_someone", fam: "relationships", days: [1, 15, 30], name: "Michael", langs: ["en"] },
      { key: "carry/caregiver", tk: "praying_for_someone", fam: "relationships", days: [1, 15, 30], name: "Mom", langs: ["en"] },
    ];
    try {
      await clean();
      await pool.query("INSERT INTO users (id, name, auth_token, auth_provider, subscription_status, account_status) VALUES ($1,'preview',$2,'selftest','none','active')", [U, "tok-" + U]);
      for (const d of DOORS) {
        const instId = "wdpreview-" + d.key.replace("/", "-");
        await pool.query("INSERT INTO journey_instances (id, user_id, template_key, family, mode, status, prayed_for_name) VALUES ($1,$2,$3,$4,'fixed','active',$5)", [instId, U, d.tk, d.fam, d.name]);
        r.doors[d.key] = { template: d.tk, cards: {} };
        for (const day of d.days) {
          r.doors[d.key].cards[day] = {};
          for (const lang of d.langs) {
            const card = await getTodayAction({ id: instId, current_day: day, family: d.fam, template_key: d.tk, prayed_for_name: d.name }, lang as any);
            r.doors[d.key].cards[day][lang] = { type: card.type, phase: card.phaseLabel, title: card.content.title, body: card.content.body, scriptureRef: card.content.scriptureRef || null };
          }
        }
        const grad = GRADUATION_MESSAGES[d.tk];
        r.doors[d.key].graduation = grad ? grad.en : "(open journey — no graduation)";
      }
      // Verse-repetition evidence: body/diagnosis, all 30 days (en).
      const vc: Record<string, number> = {};
      for (let day = 1; day <= 30; day++) {
        const card = await getTodayAction({ id: "wdpreview-body-diagnosis", current_day: day, family: "health", template_key: "through_illness_and_healing", prayed_for_name: null }, "en" as any);
        const ref = card.content.scriptureRef;
        if (ref) vc[ref] = (vc[ref] || 0) + 1;
      }
      r.diagnosis_verse_repetition_30d = vc;

      // Full grief audit: walking_through_grief, ALL 30 days × 4 langs, scanned
      // for who-assumptions (relationship nouns the journey can't safely assume).
      const whoRe = /\b(child|son|daughter|husband|wife|spouse|enfant|fils|fille|mari|femme|époux|épouse|hijo|hija|esposo|esposa|marido|mujer|filho|filha|marido|esposa)\b/i;
      const flags: any[] = [];
      const gInst = "wdpreview-grief-full";
      await pool.query("INSERT INTO journey_instances (id, user_id, template_key, family, mode, status) VALUES ($1,$2,'walking_through_grief','loss','fixed','active')", [gInst, U]);
      for (let day = 1; day <= 30; day++) {
        for (const lang of ["en", "fr", "es", "pt"]) {
          const card = await getTodayAction({ id: gInst, current_day: day, family: "loss", template_key: "walking_through_grief", prayed_for_name: null }, lang as any);
          const text = `${card.content.title} ${card.content.body}`;
          if (whoRe.test(text)) flags.push({ day, lang, type: card.type, snippet: card.content.body.slice(0, 90) });
        }
      }
      r.grief_who_audit = { days_scanned: 30, langs: 4, flags };
      griefWhoFlags = flags;
      // Raw after-sample: the day-13 grief journal (was "Write to your child").
      const s13: any = {};
      for (const lang of ["en", "fr", "es", "pt"]) {
        const card = await getTodayAction({ id: gInst, current_day: 13, family: "loss", template_key: "walking_through_grief", prayed_for_name: null }, lang as any);
        s13[lang] = card.content.body;
      }
      griefDay13Sample = s13;

      await clean();
      r.cleaned = true; r.ranAt = new Date().toISOString();
    } catch (err: any) { r.error = err.message; try { await clean(); } catch {} }
    worstDayPreview = r;
    console.log("[v5.20.13] worst-day preview:", r.error ? r.error : `generated, grief who-flags=${griefWhoFlags?.length ?? "?"}`);
  })();

  // v5.14.0 — one-time migration: fix fake trial statuses
  // Users who were auto-assigned 'trial' on signup but never actually subscribed via RevenueCat
  if (REVENUECAT_SECRET_KEY) {
    (async () => {
      try {
        const fakeTrials = await pool.query("SELECT id FROM users WHERE subscription_status='trial'");
        let fixed = 0;
        for (const row of fakeTrials.rows) {
          try {
            const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(row.id)}`, {
              headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}` }
            });
            if (rcRes.ok) {
              const rcData = (await rcRes.json()) as any;
              const subs = rcData.subscriber?.subscriptions || {};
              const hasActive = Object.values(subs).some((s: any) => new Date(s.expires_date) > new Date());
              if (!hasActive) {
                await pool.query("UPDATE users SET subscription_status='none', trial_start_date=NULL, trial_end_date=NULL, updated_at=NOW() WHERE id=$1", [row.id]);
                fixed++;
              }
            }
          } catch {}
        }
        if (fixed > 0) console.log(`[Migration] Fixed ${fixed} fake trial statuses → 'none'`);
      } catch (err: any) { console.error("[Migration]", err.message); }
    })();
  }

  backfillPlausible().catch(() => {});
  pullPlausibleMetrics().catch(() => {});
  pullAppleSalesReport().catch(() => {});
  initAppleAnalytics().catch(() => {});
  setInterval(() => { pullPlausibleMetrics().catch(() => {}); }, 60 * 60 * 1000);
  setInterval(() => { pullAppleSalesReport().catch(() => {}); }, 6 * 60 * 60 * 1000);
  setInterval(() => { pullAppleAnalytics().catch(() => {}); }, 12 * 60 * 60 * 1000);
  // Streak-at-risk check at 9pm daily (runs every hour, checks time)
  setInterval(() => { const h = new Date().getHours(); if (h === 21) checkStreakAtRisk().catch(() => {}); }, 60 * 60 * 1000);
  // Scheduled posts interval removed (FIX #8)
  setTimeout(() => { generateDailyReflection().catch(() => {}); }, 5 * 60 * 1000);
  setInterval(() => { generateDailyReflection().catch(() => {}); }, 6 * 60 * 60 * 1000);
  // v5.15.0 — generate daily circle prayers
  setTimeout(() => { generateCircleDailyPrayers().catch(() => {}); }, 6 * 60 * 1000);
  setInterval(() => { generateCircleDailyPrayers().catch(() => {}); }, 6 * 60 * 60 * 1000);
  // LR2 — gathering reminders only (no band scheduling; rooms open on entry)
  setInterval(() => { gatheringReminderTick().catch(() => {}); }, 60 * 1000);
  // P6.1 — settle partner streaks daily (grace consumption + warm pushes)
  setInterval(async () => {
    try {
      const all = await pool.query("SELECT * FROM partnerships WHERE status='active'");
      for (const row of all.rows) { await reconcilePartnership(row); }
    } catch {}
  }, 6 * 60 * 60 * 1000);
  // v5.10.5 — Auto-pull Meta Ads spend data every 6 hours
  async function pullMetaAdSpend(): Promise<void> {
    if (!META_CAPI_ACCESS_TOKEN) return;
    try {
      const adAccountId = "1254641126873592";
      const today = new Date().toISOString().split("T")[0];
      const yesterday = new Date(Date.now() - 24*60*60*1000).toISOString().split("T")[0];
      for (const date of [yesterday, today]) {
        // Campaign-level pull
        const url = `https://graph.facebook.com/v19.0/act_${adAccountId}/insights?fields=campaign_name,spend,impressions,reach,clicks,cpm,cpc,ctr,actions&time_range={"since":"${date}","until":"${date}"}&level=campaign&access_token=${META_CAPI_ACCESS_TOKEN}`;
        const res = await fetch(url);
        if (!res.ok) { console.error("[Meta Ads]", res.status, await res.text().catch(()=>"")); continue; }
        const data = (await res.json()) as any;
        for (const row of (data.data || [])) {
          const installs = (row.actions || []).find((a: any) => a.action_type === "app_installs" || a.action_type === "mobile_app_install" || a.action_type === "omni_app_install")?.value || 0;
          const trials = (row.actions || []).find((a: any) => a.action_type === "app_custom_event.fb_mobile_start_trial" || a.action_type === "omni_app_custom_event.fb_mobile_start_trial" || a.action_type === "app_custom_event.StartTrial")?.value || 0;
          await pool.query(`INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,trials,updated_at) VALUES ($1,'meta',$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$3,impressions=$4,clicks=$5,installs=$6,trials=$7,updated_at=NOW()`,
            [date, row.campaign_name || "unknown", parseFloat(row.spend||0), parseInt(row.impressions||0), parseInt(row.clicks||0), parseInt(installs), parseInt(trials)]);
        }
        if (data.data?.length) console.log(`[Meta Ads] ${date}: ${data.data.length} campaigns stored`);
        // Ad set-level pull
        const adsetUrl = `https://graph.facebook.com/v19.0/act_${adAccountId}/insights?fields=adset_name,campaign_name,spend,impressions,clicks,actions&time_range={"since":"${date}","until":"${date}"}&level=adset&access_token=${META_CAPI_ACCESS_TOKEN}`;
        const adsetRes = await fetch(adsetUrl);
        if (adsetRes.ok) {
          const adsetData = (await adsetRes.json()) as any;
          for (const row of (adsetData.data || [])) {
            const installs = (row.actions || []).find((a: any) => a.action_type === "app_installs" || a.action_type === "mobile_app_install" || a.action_type === "omni_app_install")?.value || 0;
            const trials = (row.actions || []).find((a: any) => a.action_type === "app_custom_event.fb_mobile_start_trial" || a.action_type === "omni_app_custom_event.fb_mobile_start_trial" || a.action_type === "app_custom_event.StartTrial")?.value || 0;
            await pool.query(`INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,trials,updated_at) VALUES ($1,'meta_adset',$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$3,impressions=$4,clicks=$5,installs=$6,trials=$7,updated_at=NOW()`,
              [date, (row.campaign_name||"")+"|||"+(row.adset_name||"unknown"), parseFloat(row.spend||0), parseInt(row.impressions||0), parseInt(row.clicks||0), parseInt(installs), parseInt(trials)]);
          }
        }
        // Ad-level pull (individual creatives)
        const adUrl = `https://graph.facebook.com/v19.0/act_${adAccountId}/insights?fields=ad_name,adset_name,campaign_name,spend,impressions,clicks,actions&time_range={"since":"${date}","until":"${date}"}&level=ad&access_token=${META_CAPI_ACCESS_TOKEN}`;
        const adRes = await fetch(adUrl);
        if (!adRes.ok) continue;
        const adData = (await adRes.json()) as any;
        for (const row of (adData.data || [])) {
          const installs = (row.actions || []).find((a: any) => a.action_type === "app_installs" || a.action_type === "mobile_app_install" || a.action_type === "omni_app_install")?.value || 0;
          const trials = (row.actions || []).find((a: any) => a.action_type === "app_custom_event.fb_mobile_start_trial" || a.action_type === "omni_app_custom_event.fb_mobile_start_trial" || a.action_type === "app_custom_event.StartTrial")?.value || 0;
          await pool.query(`INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,trials,updated_at) VALUES ($1,'meta_ad',$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$3,impressions=$4,clicks=$5,installs=$6,trials=$7,updated_at=NOW()`,
            [date, (row.campaign_name||"")+"|||"+(row.adset_name||"")+"|||"+(row.ad_name||"unknown"), parseFloat(row.spend||0), parseInt(row.impressions||0), parseInt(row.clicks||0), parseInt(installs), parseInt(trials)]);
        }
        if (adData.data?.length) console.log(`[Meta Ads] ${date}: ${adData.data.length} ads stored`);
      }
    } catch (err: any) { console.error("[Meta Ads]", err.message); }
  }
  setTimeout(() => { pullMetaAdSpend().catch(() => {}); }, 2 * 60 * 1000);
  setInterval(() => { pullMetaAdSpend().catch(() => {}); }, 6 * 60 * 60 * 1000);

  // v5.12.5 — Auto-pull Apple Search Ads data every 6 hours (OAuth2 flow)
  const ASA_ORG_ID = process.env.ASA_ORG_ID || "";
  const ASA_KEY_ID = process.env.ASA_KEY_ID || "";
  const ASA_PRIVATE_KEY = (process.env.ASA_PRIVATE_KEY || "").replace(/\\n/g, "\n");
  const ASA_CLIENT_ID = process.env.ASA_CLIENT_ID || ""; // SEARCHADS.<UUID> from ASA API settings
  const ASA_TEAM_ID = process.env.ASA_TEAM_ID || "";     // SEARCHADS.<UUID> from ASA API settings
  let asaAccessToken: { token: string; expiresAt: number } | null = null;

  async function getAsaAccessToken(): Promise<string | null> {
    if (!ASA_KEY_ID || !ASA_PRIVATE_KEY || !ASA_CLIENT_ID || !ASA_TEAM_ID) {
      console.log("[ASA] Missing config:", { key: !!ASA_KEY_ID, pk: !!ASA_PRIVATE_KEY, clientId: !!ASA_CLIENT_ID, teamId: !!ASA_TEAM_ID });
      return null;
    }
    // Return cached token if still valid (with 2 min buffer)
    if (asaAccessToken && Date.now() < asaAccessToken.expiresAt - 120000) return asaAccessToken.token;
    try {
      // Step 1: Generate client secret JWT for Search Ads OAuth2
      // ASA_CLIENT_ID = "SEARCHADS.<UUID>" (clientId from Apple Search Ads API settings)
      // ASA_TEAM_ID  = "SEARCHADS.<UUID>" (teamId from Apple Search Ads API settings)
      const now = Math.floor(Date.now() / 1000);
      const header = Buffer.from(JSON.stringify({ alg: "ES256", kid: ASA_KEY_ID })).toString("base64url");
      const payload = Buffer.from(JSON.stringify({
        sub: ASA_CLIENT_ID,
        iss: ASA_TEAM_ID,
        aud: "https://appleid.apple.com",
        iat: now,
        exp: now + 1200
      })).toString("base64url");
      const signer = createSign("SHA256");
      signer.update(`${header}.${payload}`);
      const signature = signer.sign({ key: ASA_PRIVATE_KEY, dsaEncoding: "ieee-p1363" }, "base64url");
      const clientSecret = `${header}.${payload}.${signature}`;

      // Step 2: Exchange for access token
      const tokenRes = await fetch("https://appleid.apple.com/auth/oauth2/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: `grant_type=client_credentials&client_id=${encodeURIComponent(ASA_CLIENT_ID)}&client_secret=${encodeURIComponent(clientSecret)}&scope=searchadsorg`
      });
      if (!tokenRes.ok) {
        const errText = await tokenRes.text().catch(() => "");
        console.error(`[ASA] OAuth2 token error ${tokenRes.status}: ${errText.substring(0, 300)}`);
        return null;
      }
      const tokenData = (await tokenRes.json()) as any;
      asaAccessToken = { token: tokenData.access_token, expiresAt: Date.now() + (tokenData.expires_in || 3600) * 1000 };
      console.log("[ASA] OAuth2 access token obtained");
      return asaAccessToken.token;
    } catch (err: any) { console.error("[ASA] OAuth2 error:", err.message); return null; }
  }

  async function pullAppleSearchAds(): Promise<void> {
    if (!ASA_ORG_ID) { console.log("[ASA] No ASA_ORG_ID configured, skipping"); return; }
    const token = await getAsaAccessToken();
    if (!token) { console.error("[ASA] No access token"); return; }
    try {
      const today = new Date().toISOString().split("T")[0];
      const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];
      const reportUrl = "https://api.searchads.apple.com/api/v5/reports/campaigns";
      const body = {
        startTime: weekAgo,
        endTime: today,
        granularity: "DAILY",
        returnRowTotals: true,
        returnGrandTotals: true
      };
      const res = await fetch(reportUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          "X-Apple-Ads-Orgid": ASA_ORG_ID
        },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const errText = await res.text().catch(() => "");
        console.error(`[ASA] Report API error ${res.status}: ${errText.substring(0, 300)}`);
        // If 401, clear cached token so it re-authenticates next time
        if (res.status === 401) asaAccessToken = null;
        return;
      }
      const data = (await res.json()) as any;
      const rows = data?.data?.reportingDataResponse?.row || [];
      let stored = 0;
      for (const row of rows) {
        const meta = row.metadata || {};
        const totals = row.total || {};
        const granularity = row.granularity || [];
        const campaignName = meta.campaignName || meta.campaignId || "all";
        for (const day of granularity) {
          const date = day.date?.substring(0, 10);
          if (!date) continue;
          const spend = parseFloat(day.spend?.amount || 0);
          const impressions = parseInt(day.impressions || 0);
          const taps = parseInt(day.taps || 0);
          const installs = parseInt(day.installs || day.totalInstalls || 0);
          await pool.query(
            `INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,updated_at) VALUES ($1,'asa',$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$3,impressions=$4,clicks=$5,installs=$6,updated_at=NOW()`,
            [date, campaignName, spend, impressions, taps, installs]
          );
          stored++;
        }
        // If no granularity, use row totals
        if (granularity.length === 0 && totals.spend) {
          const spend = parseFloat(totals.spend?.amount || 0);
          const impressions = parseInt(totals.impressions || 0);
          const taps = parseInt(totals.taps || 0);
          const installs = parseInt(totals.installs || totals.totalInstalls || 0);
          await pool.query(
            `INSERT INTO daily_ad_metrics (date,channel,campaign,spend,impressions,clicks,installs,updated_at) VALUES ($1,'asa',$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date,channel,campaign) DO UPDATE SET spend=$2,impressions=$3,clicks=$4,installs=$5,updated_at=NOW()`,
            [today, campaignName, spend, impressions, taps, installs]
          );
          stored++;
        }
      }
      if (stored > 0) console.log(`[ASA] Stored ${stored} daily metrics`);
      else console.log("[ASA] No campaign data returned");
    } catch (err: any) { console.error("[ASA]", err.message); }
  }
  setTimeout(() => { pullAppleSearchAds().catch(() => {}); }, 3 * 60 * 1000);
  setInterval(() => { pullAppleSearchAds().catch(() => {}); }, 6 * 60 * 60 * 1000);

  // v5.12.4 — Nudge users stuck on paywall (daily at 10am)
  async function nudgeStuckUsers(): Promise<void> {
    try {
      // Find users who: created account 1+ days ago, have device token, never subscribed, have no prayers
      const stuck = await pool.query(`
        SELECT u.id, u.name, u.device_token FROM users u
        LEFT JOIN user_data ud ON ud.user_id = u.id
        WHERE u.device_token IS NOT NULL
        AND u.subscription_status = 'none'
        AND u.created_at < NOW() - INTERVAL '1 day'
        AND u.created_at > NOW() - INTERVAL '7 days'
        AND (ud.total_prayers IS NULL OR ud.total_prayers = 0)
      `);
      if (stuck.rows.length === 0) return;
      let sent = 0;
      for (const user of stuck.rows) {
        // Check throttle — only send once per user
        const throttleKey = `paywall_nudge_${user.id}`;
        const existing = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
        if (existing.rows.length > 0) continue;
        await pushToUser(user.id, {
          title: "Your free trial is waiting",
          body: "Start your first guided prayer today — it only takes 2 minutes. Your 7-day free trial begins when you're ready.",
          type: "streak_reminders"
        });
        await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);
        sent++;
        trackEvent(user.id, "paywall_nudge_sent", {});
      }
      if (sent > 0) console.log(`[Nudge] Sent paywall nudge to ${sent} stuck users`);
    } catch (err: any) { console.error("[Nudge]", err.message); }
  }
  // Run daily at 10am (check every hour)
  setInterval(() => { const h = new Date().getHours(); if (h === 10) nudgeStuckUsers().catch(() => {}); }, 60 * 60 * 1000);

  // v5.13.2 — Nudge trial users who haven't prayed after 24h
  async function nudgeInactiveTrials(): Promise<void> {
    try {
      // v5.14.7 — Only nudge users with status 'none' (never started trial).
      // Do NOT nudge active trial users — they are on the path to convert.
      // Nudging them reminds them of the trial and can trigger cancellation (Keyvin incident).
      const inactive = await pool.query(`
        SELECT u.id, u.name, u.device_token FROM users u
        LEFT JOIN user_data ud ON ud.user_id = u.id
        WHERE u.device_token IS NOT NULL
        AND u.subscription_status = 'none'
        AND u.created_at < NOW() - INTERVAL '12 hours'
        AND u.created_at > NOW() - INTERVAL '3 days'
        AND (ud.total_prayers IS NULL OR ud.total_prayers = 0)
      `);
      if (inactive.rows.length === 0) return;
      let sent = 0;
      for (const user of inactive.rows) {
        const throttleKey = `inactive_trial_nudge_${user.id}`;
        const existing = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
        if (existing.rows.length > 0) continue;
        // Check if user has a circle
        const ud = await pool.query("SELECT circle_codes FROM user_data WHERE user_id=$1", [user.id]);
        const hasCodes = ud.rows[0]?.circle_codes?.length > 0;
        const hasCircle = hasCodes || Array.from(circles.values()).some(c => c.members.some(m => m.userId === user.id));
        const title = hasCircle ? "Your Prayer Circle is waiting" : "Your first prayer is waiting";
        const body = hasCircle
          ? "You created a circle but have not prayed yet. It only takes 30 seconds. Your friends are counting on you."
          : "Open prAmen and pray your first guided prayer. It only takes 30 seconds.";
        await pushToUser(user.id, { title, body, type: "streak_reminders" });
        await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);
        sent++;
        trackEvent(user.id, "inactive_trial_nudge_sent", { has_circle: hasCircle });
        // v5.15.1 — fire Loops event for inactive trial email
        try {
          const emailRow = await pool.query("SELECT email FROM users WHERE id=$1 AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com'", [user.id]);
          if (emailRow.rows[0]?.email) sendLoopsEvent(emailRow.rows[0].email, "inactive_trial_2days", { hasCircle, firstName: (user.name || "").split(" ")[0] || undefined });
        } catch {}
      }
      if (sent > 0) console.log(`[Nudge] Sent inactive trial nudge to ${sent} users`);
    } catch (err: any) { console.error("[InactiveNudge]", err.message); }
  }
  // Run every 6 hours
  setInterval(() => { nudgeInactiveTrials().catch(() => {}); }, 6 * 60 * 60 * 1000);
  // Run once on startup after 5 min
  setTimeout(() => { nudgeInactiveTrials().catch(() => {}); }, 5 * 60 * 1000);

  // v5.12.7 — Win-back push for cancelled trial users (1 day before trial expires)
  async function nudgeCancelledTrials(): Promise<void> {
    try {
      // Find users who: cancelled subscription, trial_end_date is tomorrow, have device token
      const expiring = await pool.query(`
        SELECT u.id, u.name, u.device_token, u.trial_end_date FROM users u
        LEFT JOIN user_data ud ON ud.user_id = u.id
        WHERE u.device_token IS NOT NULL
        AND u.subscription_status = 'cancelled'
        AND u.trial_end_date IS NOT NULL
        AND u.trial_end_date > NOW()
        AND u.trial_end_date < NOW() + INTERVAL '2 days'
      `);
      if (expiring.rows.length === 0) return;
      let sent = 0;
      for (const user of expiring.rows) {
        const throttleKey = `trial_expiry_nudge_${user.id}`;
        const existing = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
        if (existing.rows.length > 0) continue;
        // Personalize with prayer count
        const ud = await pool.query("SELECT total_prayers, streak_count FROM user_data WHERE user_id=$1", [user.id]);
        const prayers = ud.rows[0]?.total_prayers || 0;
        const streak = ud.rows[0]?.streak_count || 0;
        let body = "Your trial ends tomorrow. Resubscribe to keep your prayer journey going.";
        if (prayers > 0 && streak > 0) {
          body = `Your trial ends tomorrow. You've prayed ${prayers} times and built a ${streak}-day streak. Don't start from zero.`;
        } else if (prayers > 0) {
          body = `Your trial ends tomorrow. You've prayed ${prayers} times already. Keep the momentum going.`;
        }
        await pushToUser(user.id, {
          title: "Your trial ends tomorrow",
          body,
          type: "streak_reminders"
        });
        await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);
        sent++;
        trackEvent(user.id, "trial_expiry_nudge_sent", { prayers, streak });
      }
      if (sent > 0) console.log(`[Nudge] Sent trial-expiry nudge to ${sent} cancelled trial users`);
    } catch (err: any) { console.error("[Nudge] Trial expiry:", err.message); }
  }
  // Run daily at 9am (check every hour)
  setInterval(() => { const h = new Date().getHours(); if (h === 9) nudgeCancelledTrials().catch(() => {}); }, 60 * 60 * 1000);

  // ═══════════════════════════════════════════════════════════════════
  // v4.0.14 — Day 1-7 circle-aware notifications for new users
  // Rules: max 2 push/day, circle-aware replaces generic, skip active non-cancelled subscribers who prayed today
  // ═══════════════════════════════════════════════════════════════════

  async function getDailyPushCount(userId: string): Promise<number> {
    const today = new Date().toISOString().split("T")[0];
    const result = await pool.query("SELECT COUNT(*) as cnt FROM push_throttle WHERE throttle_key LIKE $1 AND sent_date = $2", [`daily_push_${userId}_%`, today]);
    return parseInt(result.rows[0]?.cnt || "0");
  }

  async function recordDailyPush(userId: string, pushType: string): Promise<void> {
    const today = new Date().toISOString().split("T")[0];
    const key = `daily_push_${userId}_${pushType}_${today}`;
    await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [key, today]);
  }

  function userPrayedToday(userId: string): boolean {
    for (const [, circle] of circles) {
      const member = circle.members.find(m => m.userId === userId);
      if (member && prayedTodayInOwnTZ(member)) return true;
    }
    return false;
  }

  async function nudgeNewUsers(): Promise<void> {
    try {
      // Find users created in the last 7 days with device tokens
      const newUsers = await pool.query(`
        SELECT u.id, u.name, u.device_token, u.created_at, u.subscription_status,
               ud.total_prayers, ud.streak_count, ud.circle_codes
        FROM users u
        LEFT JOIN user_data ud ON ud.user_id = u.id
        WHERE u.device_token IS NOT NULL
        AND u.created_at > NOW() - INTERVAL '7 days'
      `);
      if (newUsers.rows.length === 0) return;

      let sent = 0;
      for (const user of newUsers.rows) {
        // Rule 6: Never nudge active non-cancelled subscribers who prayed today
        if (user.subscription_status === 'active' && userPrayedToday(user.id)) continue;

        // Rule 4: If user prayed today, skip
        if (userPrayedToday(user.id)) continue;

        // Rule 1: Max 2 push per day
        const dailyCount = await getDailyPushCount(user.id);
        if (dailyCount >= 2) continue;

        const hoursOld = (Date.now() - new Date(user.created_at).getTime()) / (1000 * 60 * 60);
        const daysOld = Math.floor(hoursOld / 24);
        const totalPrayers = user.total_prayers || 0;

        // Find user's auto-enrolled community circle
        let circleName = "";
        let circleMemberCount = 0;
        let prayedTodayCount = 0;
        for (const [, circle] of circles) {
          const isMember = circle.members.some(m => m.userId === user.id);
          if (isMember && circle.members.length >= 10) {
            circleName = circle.name;
            circleMemberCount = circle.members.length;
            prayedTodayCount = circle.members.filter(m => prayedTodayInOwnTZ(m)).length;
            break;
          }
        }

        let title = "";
        let body = "";
        let pushType = "";

        if (daysOld === 0 && hoursOld >= 6 && totalPrayers <= 1) {
          // Day 1 evening — streak nudge
          pushType = "day1_evening";
          const throttleKey = `new_user_day1_${user.id}`;
          const exists = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
          if (exists.rows.length > 0) continue;
          title = "You started something today";
          body = totalPrayers > 0
            ? "You prayed once today. 36 seconds keeps the streak alive."
            : "Your first prayer is waiting. It only takes 36 seconds.";
          await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);

        } else if (daysOld === 1 && circleName) {
          // Day 2 morning — circle-aware
          pushType = "day2_circle";
          const throttleKey = `new_user_day2_circle_${user.id}`;
          const exists = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
          if (exists.rows.length > 0) continue;
          title = circleName;
          body = prayedTodayCount > 0
            ? `${prayedTodayCount} people already prayed today. Join them.`
            : `${circleMemberCount} people are in your circle. Show up today.`;
          await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);

        } else if (daysOld === 1 && hoursOld >= 30) {
          // Day 2 evening — "someone prayed for you" (ONE TIME)
          pushType = "day2_prayed_for_you";
          const throttleKey = `new_user_prayed_for_you_${user.id}`;
          const exists = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
          if (exists.rows.length > 0) continue;
          if (!circleName) continue; // Only send if in a circle
          // Find a real member who prayed today
          let prayerName = "";
          for (const [, circle] of circles) {
            if (!circle.members.some(m => m.userId === user.id)) continue;
            const prayedMember = circle.members.find(m => m.userId !== user.id && prayedTodayInOwnTZ(m));
            if (prayedMember) { prayerName = prayedMember.name; break; }
          }
          if (!prayerName) continue; // Only send if someone actually prayed
          title = `${prayerName} prayed for you`;
          body = `Someone in ${circleName} showed up for you today.`;
          await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);

        } else if (daysOld === 2 && circleName) {
          // Day 3 morning — accountability
          pushType = "day3_accountability";
          const throttleKey = `new_user_day3_${user.id}`;
          const exists = await pool.query("SELECT throttle_key FROM push_throttle WHERE throttle_key=$1", [throttleKey]);
          if (exists.rows.length > 0) continue;
          title = "Day 3";
          body = totalPrayers > 0
            ? `Your circle sees when you pray. ${prayedTodayCount > 0 ? prayedTodayCount + " already showed up today." : "Be the first today."}`
            : "Your circle is waiting. 36 seconds. That's all it takes.";
          await pool.query("INSERT INTO push_throttle (throttle_key, sent_date) VALUES ($1, $2) ON CONFLICT DO NOTHING", [throttleKey, new Date().toISOString().split("T")[0]]);

        } else {
          continue;
        }

        // Send the push
        await pushToUser(user.id, { title, body, type: "streak_reminders" });
        await recordDailyPush(user.id, pushType);
        trackEvent(user.id, `new_user_nudge_${pushType}`, { days_old: daysOld, total_prayers: totalPrayers, circle: circleName });
        sent++;
      }
      if (sent > 0) console.log(`[Nudge] New user circle-aware nudge sent to ${sent} users`);
    } catch (err: any) { console.error("[Nudge] New user:", err.message); }
  }

  // Run every 3 hours — checks which day each user is on and sends appropriate nudge
  setInterval(() => { nudgeNewUsers().catch(() => {}); }, 3 * 60 * 60 * 1000);
  setTimeout(() => { nudgeNewUsers().catch(() => {}); }, 8 * 60 * 1000);

  // v5.15.1 — Loops event sender for lifecycle triggers
  const LOOPS_API_KEY = process.env.LOOPS_API_KEY || "";
  // sendLoopsEvent is now at module level (moved for scope access from checkStreakAtRisk)

  // v5.12.6 — Sync user segments to Loops.so for email campaigns (every 6 hours)
  async function syncToLoops(): Promise<void> {
    if (!LOOPS_API_KEY || !REVENUECAT_SECRET_KEY) { console.log("[Loops] No LOOPS_API_KEY or REVENUECAT_SECRET_KEY"); return; }
    try {
      // Get all users with real emails (no Apple Private Relay) + user_data for engagement props
      const users = await pool.query(`SELECT u.id, u.name, u.email, u.created_at, u.subscription_status, ud.total_prayers, ud.streak_count, ud.last_prayed_date, ud.circle_codes FROM users u LEFT JOIN user_data ud ON ud.user_id = u.id WHERE u.email IS NOT NULL AND u.email != '' AND u.email NOT LIKE '%privaterelay.appleid.com'`);
      if (users.rows.length === 0) { console.log("[Loops] No users with real emails"); return; }
      let synced = 0;
      for (const user of users.rows) {
        try {
          // Check RevenueCat for subscription status
          const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(user.id)}`, {
            headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }
          });
          let category = "signed_up_no_trial";
          let subscriptionDate: string | null = null;
          let cancellationDate: string | null = null;
          let plan = "";
          if (rcRes.ok) {
            const rcData = (await rcRes.json()) as any;
            const sub = rcData.subscriber;
            const subscriptions = sub?.subscriptions || {};
            const now = new Date();
            // Determine category
            let hasActivePaid = false;
            let hasActiveTrial = false;
            let hasCancelledTrial = false;
            let hasCancelledAfterTrial = false;
            let hasAnySubscription = false;
            for (const [pid, s2] of Object.entries(subscriptions) as any[]) {
              hasAnySubscription = true;
              const expires = new Date(s2.expires_date);
              const isActive = expires > now;
              const isTrial = s2.period_type === "trial";
              const isCancelled = !!s2.unsubscribe_detected_at;
              if (isActive && !isTrial && !isCancelled) hasActivePaid = true;
              if (isActive && isTrial && !isCancelled) hasActiveTrial = true;
              if (isTrial && (isCancelled || expires <= now)) hasCancelledTrial = true;
              if (!isTrial && (isCancelled || expires <= now)) hasCancelledAfterTrial = true;
              // Extract dates
              if (s2.purchase_date && !subscriptionDate) subscriptionDate = s2.purchase_date;
              if (s2.unsubscribe_detected_at && !cancellationDate) cancellationDate = s2.unsubscribe_detected_at;
              if (pid.includes("yearly")) plan = "yearly";
              else if (pid.includes("monthly")) plan = plan || "monthly";
              else if (pid.includes("lifetime")) plan = "lifetime";
            }
            // v5.15.1 — detect billing issues for payment_failed userGroup
            let hasBillingIssue = false;
            for (const [, s3] of Object.entries(subscriptions) as any[]) {
              if (s3.billing_issues_detected_at) hasBillingIssue = true;
            }
            if (hasBillingIssue && !hasActivePaid && !hasActiveTrial) category = "payment_failed";
            else if (hasActivePaid) category = "active_subscriber";
            else if (hasActiveTrial) category = "active_trial";
            else if (hasCancelledTrial) category = "cancelled_before_trial_end";
            else if (hasCancelledAfterTrial) category = "cancelled_after_trial";
            // v5.14.0 — distinguish: had a subscription before (active_free/bypasser) vs never subscribed
            else if (hasAnySubscription) category = "active_free";
            else category = "signed_up_no_trial";
          }
          // Sync to Loops
          const loopsBody: any = {
            email: user.email,
            firstName: (user.name || "").split(" ")[0] || undefined,
            lastName: (user.name || "").split(" ").slice(1).join(" ") || undefined,
            source: "pramen",
            userId: user.id,
            userGroup: category,
            mailingLists: {},
          };
          // Add custom properties
          if (subscriptionDate) loopsBody.subscriptionDate = subscriptionDate;
          if (cancellationDate) loopsBody.cancellationDate = cancellationDate;
          if (plan) loopsBody.plan = plan;
          loopsBody.signupDate = user.created_at;
          // v5.13.3 — engagement properties for conditional email logic
          loopsBody.totalPrayers = user.total_prayers || 0;
          loopsBody.streakCount = user.streak_count || 0;
          loopsBody.lastPrayedDate = user.last_prayed_date || null;
          const circleCodesArr = user.circle_codes || [];
          const inCircleMap = Array.from(circles.values()).some(c => c.members.some(m => m.userId === user.id));
          loopsBody.hasCircle = circleCodesArr.length > 0 || inCircleMap;
          const loopsRes = await fetch("https://app.loops.so/api/v1/contacts/update", {
            method: "PUT",
            headers: { Authorization: `Bearer ${LOOPS_API_KEY}`, "Content-Type": "application/json" },
            body: JSON.stringify(loopsBody)
          });
          if (loopsRes.ok) synced++;
          else {
            const errText = await loopsRes.text().catch(() => "");
            if (synced === 0) console.error(`[Loops] Contact sync error: ${loopsRes.status} ${errText.substring(0, 200)}`);
          }
        } catch {}
      }
      console.log(`[Loops] Synced ${synced}/${users.rows.length} contacts with emails`);
    } catch (err: any) { console.error("[Loops]", err.message); }
  }
  setTimeout(() => { syncToLoops().catch(() => {}); }, 5 * 60 * 1000);
  setInterval(() => { syncToLoops().catch(() => {}); }, 6 * 60 * 60 * 1000);

  // v5.15.5 — Periodic subscription visibility sync (community-circle-aware)
  // Three-pass approach:
  //   Pass 1: Active/lifetime users in DB → ensure visible=true in all circles
  //   Pass 2: Invisible members in community circles → check RC API, fix if actually subscribed
  //   Pass 3: Visible members in community circles whose DB says cancelled/expired/none → hide them
  async function syncSubscriptionVisibility(): Promise<void> {
    if (!REVENUECAT_SECRET_KEY) { console.log("[VisibilitySync] No RC secret key"); return; }
    try {
      let fixedVisible = 0;
      let fixedHidden = 0;
      let fixedFromRc = 0;

      // Pass 1: DB says active/lifetime → make visible in all circles
      const activeUsers = await pool.query("SELECT id FROM users WHERE subscription_status IN ('active', 'lifetime')");
      const activeUserIds = new Set(activeUsers.rows.map((r: any) => r.id));
      for (const uid of activeUserIds) {
        for (const [, circle] of circles) {
          const member = circle.members.find(m => m.userId === uid);
          if (member && member.visible === false) {
            member.visible = true;
            saveCircleToDb(circle).catch(() => {});
            fixedVisible++;
          }
        }
      }

      // Pass 2: Invisible members in community circles → check RC API for active entitlement
      const invisibleMembers = new Set<string>();
      for (const [, circle] of circles) {
        if (!isCommunityCircle(circle.code)) continue;
        for (const member of circle.members) {
          if (member.visible === false) invisibleMembers.add(member.userId);
        }
      }

      for (const userId of invisibleMembers) {
        try {
          const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(userId)}`, {
            headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }
          });
          if (!rcRes.ok) continue;
          const rcData = (await rcRes.json()) as any;
          const subs = rcData.subscriber?.subscriptions || {};
          const entitlements = rcData.subscriber?.entitlements || {};
          const now = new Date();

          const hasPremium = entitlements.premium && new Date(entitlements.premium.expires_date) > now;
          const hasLifetime = Object.entries(subs).some(([pid, s]: [string, any]) => pid.includes("lifetime") && (!s.expires_date || new Date(s.expires_date) > now));

          if (hasPremium || hasLifetime) {
            await pool.query("UPDATE users SET subscription_status=$1, updated_at=NOW() WHERE id=$2", [hasLifetime ? "lifetime" : "active", userId]).catch(() => {});
            for (const [, circle] of circles) {
              const member = circle.members.find(m => m.userId === userId);
              if (member && member.visible === false) {
                member.visible = true;
                saveCircleToDb(circle).catch(() => {});
                fixedFromRc++;
              }
            }
            console.log(`[VisibilitySync] Fixed ${userId} — RC says active, was invisible`);
          }
        } catch {}
      }

      // Pass 3: Visible members in community circles who are NOT active subscribers → hide them
      const nonActiveUsers = await pool.query("SELECT id FROM users WHERE subscription_status NOT IN ('active', 'lifetime', 'trial') OR subscription_status IS NULL");
      const nonActiveIds = new Set(nonActiveUsers.rows.map((r: any) => r.id));
      for (const [, circle] of circles) {
        if (!isCommunityCircle(circle.code)) continue;
        for (const member of circle.members) {
          if (member.visible !== false && nonActiveIds.has(member.userId) && member.userId !== circle.creatorUserId) {
            member.visible = false;
            saveCircleToDb(circle).catch(() => {});
            fixedHidden++;
          }
        }
      }

      if (fixedVisible > 0 || fixedHidden > 0 || fixedFromRc > 0) {
        console.log(`[VisibilitySync] Made visible: ${fixedVisible} (DB) + ${fixedFromRc} (RC API). Hidden: ${fixedHidden} (non-subscribers in community)`);
      } else {
        console.log("[VisibilitySync] All visibility consistent");
      }
    } catch (err: any) { console.error("[VisibilitySync]", err.message); }
  }
  // Run 3 min after start, then every 2 hours
  setTimeout(() => { syncSubscriptionVisibility().catch(() => {}); }, 3 * 60 * 1000);
  setInterval(() => { syncSubscriptionVisibility().catch(() => {}); }, 2 * 60 * 60 * 1000);

  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v5.10.9 on port ${info.port}`);
    console.log(`   PostHog: ${POSTHOG_API_KEY ? "✓" : "✗"} | Read: ${POSTHOG_PERSONAL_KEY ? "✓" : "✗"} | Plausible: ${PLAUSIBLE_API_KEY ? "✓" : "✗"}`);
    console.log(`   Apple: ${ASC_KEY_ID ? "✓" : "✗"} | RC: ${REVENUECAT_SECRET_KEY ? "✓" : "✗"} | APNs: ${APNS_KEY_ID ? "✓" : "✗"}`);
    console.log(`   Meta CAPI: ${META_CAPI_ACCESS_TOKEN ? "✓" : "✗"} pixel=${META_PIXEL_ID || "-"}`);
    console.log(`   Storage: ${R2_ACCOUNT_ID ? "✓" : "✗"} | Admin: ${ADMIN_USER_ID ? ADMIN_USER_ID.substring(0,8)+"..." : "✗"}`);
    console.log(`   Dashboard: /dashboard?key=... | Circles: ${circles.size}\n`);
  });
}
start();
