import { Hono } from "hono";
import { cors } from "hono/cors";
import { streamSSE } from "hono/streaming";
import { serve } from "@hono/node-server";
import { randomUUID, createHash, createSign } from "crypto";
import { gunzipSync } from "zlib";
import { readFileSync } from "fs";
import http2 from "http2";
import pg from "pg";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────
interface StoredMember { userId: string; name: string; streakCount: number; lastPrayedDate: string | null; lastPrayedLocalDate?: string | null; lastPrayedTimezone?: string | null; joinedAt: string; canPost?: boolean; notificationsMuted?: boolean; role?: string; avatarUrl?: string; }
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
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY || "AIzaSyD0WUdkjl_HBAoaojLM073AK0CuFgb5rro";
const YT_CHANNEL_HANDLE = "fatherjohnprays";

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

function sendPush(deviceToken: string, payload: PushPayload): void {
  const jwt = generateAPNsJWT();
  if (!jwt || !deviceToken) return;
  const apnsPayload = JSON.stringify({ aps: { alert: { title: payload.title, body: payload.body }, sound: "default", badge: 1, "mutable-content": 1 }, type: payload.type, circleCode: payload.circleCode || "", circleName: payload.circleName || "", ...(payload.extra || {}) });
  try {
    const client = http2.connect(`https://${APNS_HOST}`);
    client.on("error", (err) => { console.error("[APNs] Connection error:", err.message); client.close(); });
    const req = client.request({ ":method": "POST", ":path": `/3/device/${deviceToken}`, authorization: `bearer ${jwt}`, "apns-topic": APNS_BUNDLE_ID, "apns-push-type": "alert", "apns-priority": "10", "apns-expiration": "0", "content-type": "application/json" });
    req.on("response", (headers) => { const status = headers[":status"]; if (status !== 200) { let body = ""; req.on("data", (chunk: Buffer) => { body += chunk.toString(); }); req.on("end", () => { console.log(`[APNs] Push failed status=${status} token=${deviceToken.substring(0, 8)}... body=${body}`); client.close(); }); return; } });
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
      sendPush(result.rows[0].device_token, payload);
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

const PUSH_STRINGS: Record<string, Record<Lang, string>> = {
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
    await client.query(`CREATE TABLE IF NOT EXISTS user_data (user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, streak_count INTEGER DEFAULT 0, highest_streak INTEGER DEFAULT 0, total_prayers INTEGER DEFAULT 0, total_minutes INTEGER DEFAULT 0, last_prayed_date TIMESTAMPTZ, sessions JSONB DEFAULT '[]'::jsonb, preferences JSONB DEFAULT '{}'::jsonb, circle_codes TEXT[] DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_apple_user_id ON users(apple_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_google_user_id ON users(google_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_auth_token ON users(auth_token)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_device_user_id ON users(device_user_id)`);
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
    await client.query(`CREATE TABLE IF NOT EXISTS seasonal_verses (date DATE NOT NULL, lang TEXT NOT NULL DEFAULT 'en', verse TEXT NOT NULL, reference TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (date, lang))`);
    await client.query(`CREATE TABLE IF NOT EXISTS favorites (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, title TEXT, source TEXT NOT NULL DEFAULT 'app', prayer_text TEXT, prayer_id TEXT, media_url TEXT, media_type TEXT, media_filename TEXT, transcript TEXT, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id, is_deleted, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, type TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, data JSONB DEFAULT '{}', is_read BOOLEAN DEFAULT false, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_deleted, created_at DESC)`);
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
    console.log("DB initialized (v5.9.4 — referral system: /api/referrals/validate/:code, /ref/ deep-link path, link format updated)");
  } catch (err) { console.error("DB init failed:", err); } finally { client.release(); }
}

// ─── Circle Cache ────────────────────────────────────────────────────
const circles = new Map<string, StoredCircle>();
async function loadAllFromDb(): Promise<void> { try { const r = await pool.query("SELECT code, data FROM circles"); for (const row of r.rows) circles.set(row.code, row.data as StoredCircle); console.log(`Loaded ${circles.size} circles`); } catch (err) { console.error("Load circles:", err); } }
async function saveCircleToDb(circle: StoredCircle): Promise<void> { const k = circle.code.toUpperCase(); circles.set(k, circle); try { await pool.query(`INSERT INTO circles (code,data,updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (code) DO UPDATE SET data=$2,updated_at=NOW()`, [k, JSON.stringify(circle)]); } catch (err) { console.error("Save circle:", err); } }
async function deleteCircleFromDb(code: string): Promise<boolean> { const k = code.toUpperCase(); const e = circles.delete(k); try { await pool.query("DELETE FROM circles WHERE code=$1", [k]); } catch {} return e; }
function getCircle(code: string): StoredCircle | undefined { return circles.get(code.toUpperCase()); }
function generateCircleCode(): string { const ch = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; let c = ""; for (let i = 0; i < 6; i++) c += ch[Math.floor(Math.random() * ch.length)]; if (circles.has(c)) return generateCircleCode(); return c; }

// ─── Auth Helpers ────────────────────────────────────────────────────
function generateAuthToken(): string { return randomUUID() + "-" + randomUUID(); }
async function getUserByToken(token: string) { if (!token) return null; try { const r = await pool.query("SELECT * FROM users WHERE auth_token=$1", [token]); return r.rows[0] || null; } catch { return null; } }
async function getUserByAppleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE apple_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByGoogleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE google_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByEmail(email: string) { try { const r = await pool.query("SELECT * FROM users WHERE email=$1", [email]); return r.rows[0] || null; } catch { return null; } }
async function getUserData(userId: string) { try { const r = await pool.query("SELECT * FROM user_data WHERE user_id=$1", [userId]); if (r.rows[0]) { const d = r.rows[0]; let streakCount = d.streak_count || 0; if (streakCount > 0 && d.last_prayed_date) { const now = new Date(); const lastPrayed = new Date(d.last_prayed_date); const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const yesterday = new Date(today.getTime() - 86400000); const lastPrayedDay = new Date(lastPrayed.getFullYear(), lastPrayed.getMonth(), lastPrayed.getDate()); if (lastPrayedDay < yesterday) { streakCount = 0; await pool.query("UPDATE user_data SET streak_count=0, updated_at=NOW() WHERE user_id=$1", [userId]); } } return { streakCount, highestStreak: d.highest_streak, totalPrayers: d.total_prayers, totalMinutes: d.total_minutes, lastPrayedDate: d.last_prayed_date, sessions: d.sessions || [], preferences: d.preferences || {}, circleCodes: d.circle_codes || [] }; } return null; } catch { return null; } }
function getUserCircleCodes(...userIds: string[]): string[] { const ids = new Set(userIds.filter(Boolean)); const codes: string[] = []; for (const [code, circle] of circles) { if (circle.members.some(m => ids.has(m.userId))) codes.push(code); } return codes; }
async function migrateCircleMembership(oldId: string, newId: string, name: string) { for (const [, c] of circles) { const m = c.members.find(m => m.userId === oldId); if (m) { m.userId = newId; if (name) m.name = name; await saveCircleToDb(c); } if (c.creatorUserId === oldId) { c.creatorUserId = newId; await saveCircleToDb(c); } } }
async function requireAuth(c: any): Promise<any | null> { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return null; return getUserByToken(ah.replace("Bearer ", "")); }

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

app.get("/", (c) => c.json({ status: "ok", service: "prAmen API", version: "5.10.1", circles: circles.size, posthog: !!POSTHOG_API_KEY, posthog_read: !!POSTHOG_PERSONAL_KEY, plausible: !!PLAUSIBLE_API_KEY, apple: !!ASC_KEY_ID, revenuecat_api: !!REVENUECAT_SECRET_KEY, apns: !!APNS_KEY_ID, storage: !!R2_ACCOUNT_ID, admin: !!ADMIN_USER_ID, dashboard: "/dashboard?key=..." }));

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
    const circleStates = result.rows.map((r: any) => {
      const ci = getCircle(r.code);
      const prayedToday = ci ? ci.members.filter(m => prayedTodayInOwnTZ(m)).length : 0;
      return { code: r.code, updatedAt: r.updated_at, prayedToday, totalMembers: ci?.members.length || 0 };
    });
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
      if (email && !user.email) { await pool.query("UPDATE users SET email=$1,updated_at=NOW() WHERE id=$2", [email, user.id]); user.email = email; }
      oldDeviceUserId = user.device_user_id;
      if (deviceUserId && deviceUserId !== user.device_user_id) {
        if (user.device_user_id) await migrateCircleMembership(user.device_user_id, user.id, user.name || "");
        await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
      }
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || "";
      const ts = new Date(); const te = new Date(ts.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,apple_user_id,email,name,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'trial')`, [userId, appleUserId, email||null, userName, authToken, deviceUserId||null, ts, te]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, apple_user_id: appleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: ts, trial_end_date: te, subscription_status: "trial" };
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
    if (!user) { const ex = await getUserByEmail(email); if (ex) { await pool.query("UPDATE users SET google_user_id=$1,auth_provider=CASE WHEN auth_provider='apple' THEN 'apple+google' ELSE 'google' END,updated_at=NOW() WHERE id=$2", [googleUserId, ex.id]); user = ex; } }
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
      const ts = new Date(); const te = new Date(ts.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,google_user_id,email,name,auth_provider,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,'google',$5,$6,$7,$8,'trial')`, [userId, googleUserId, email, userName, authToken, deviceUserId||null, ts, te]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, google_user_id: googleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: ts, trial_end_date: te, subscription_status: "trial" };
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
app.get("/api/admin/email-list", async (c) => { if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const r = await pool.query("SELECT email,name,auth_provider,created_at FROM users WHERE email_opt_in=true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"); return c.json({ count: r.rows.length, emails: r.rows }); });
app.post("/api/auth/verify", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ valid: false }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ valid: false }, 401); return c.json({ valid: true, user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id) }); });
app.post("/api/auth/logout", async (c) => { const ah = c.req.header("Authorization"); if (!ah) return c.json({ success: true }); await pool.query("UPDATE users SET auth_token=$1,updated_at=NOW() WHERE auth_token=$2", [generateAuthToken(), ah.replace("Bearer ", "")]); return c.json({ success: true }); });
app.delete("/api/auth/account", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Not found" }, 404); await pool.query("DELETE FROM user_data WHERE user_id=$1", [u.id]); await pool.query("DELETE FROM users WHERE id=$1", [u.id]); trackEvent(u.id, "account_deleted", {}); return c.json({ success: true }); });

// ═══════════════════════════════════════════════════════════════════
// ─── DATA SYNC + DEVICE TOKEN ───────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); return c.json({ user: { id: u.id, name: u.name, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.id, u.device_user_id) }); });
app.put("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const b = await c.req.json(); try { await pool.query(`INSERT INTO user_data (user_id,streak_count,highest_streak,total_prayers,total_minutes,last_prayed_date,sessions,preferences,circle_codes,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT (user_id) DO UPDATE SET streak_count=$2,highest_streak=GREATEST(user_data.highest_streak,$3),total_prayers=$4,total_minutes=$5,last_prayed_date=$6,sessions=$7,preferences=$8,circle_codes=$9,updated_at=NOW()`, [u.id, b.streakCount||0, b.highestStreak||0, b.totalPrayers||0, b.totalMinutes||0, b.lastPrayedDate||null, JSON.stringify(b.sessions||[]), JSON.stringify(b.preferences||{}), b.circleCodes||[]]); if (b.userName && b.userName !== u.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [b.userName, u.id]); return c.json({ status: "ok", synced: true }); } catch (e: any) { return c.json({ error: "Sync failed", detail: e.message }, 500); } });
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
    await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, resolvedUid]).catch(() => {});
    if (resolvedUid !== rcUid) await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, rcUid]).catch(() => {});
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
    console.log(`[RC] ${ev.type} → ${name} | rc:${rcUid.substring(0,12)} → resolved:${resolvedUid.substring(0,12)} ${plan}`); return c.json({ status: "ok" });
  } catch (e) { console.error("[RC]", e); return c.json({ error: "Error" }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLES ────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/circles", async (c) => { const b = await c.req.json(); if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const code = generateCircleCode(); const ci: StoredCircle = { id: randomUUID(), name: b.name || "Prayer Circle", code, emoji: b.emoji || "cross.fill", creatorUserId: b.userId, members: [{ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString(), role: "creator" }], prayerRequests: [], createdAt: new Date().toISOString() }; await saveCircleToDb(ci); trackEvent(b.userId, "circle_created", { circle_id: ci.id, circle_code: code, circle_name: ci.name }); return c.json({ circle: ci }, 201); });
app.get("/api/circles/:code", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); try { const memberIds = ci.members.map(m => m.userId).filter(Boolean); if (memberIds.length > 0) { const avatars = await pool.query("SELECT id, device_user_id, avatar_url, name FROM users WHERE id = ANY($1) OR device_user_id = ANY($1)", [memberIds]); const avatarMap: Record<string, { avatar_url: string | null; name: string }> = {}; for (const row of avatars.rows) { avatarMap[row.id] = { avatar_url: row.avatar_url, name: row.name }; if (row.device_user_id) avatarMap[row.device_user_id] = { avatar_url: row.avatar_url, name: row.name }; } const enriched = { ...ci, members: ci.members.map(m => ({ ...m, avatarUrl: avatarMap[m.userId]?.avatar_url || m.avatarUrl || null, name: avatarMap[m.userId]?.name || m.name })) }; return c.json({ circle: enriched }); } } catch (err: any) { console.error("[Circle] Avatar enrich error:", err.message); } return c.json({ circle: ci }); });
app.post("/api/circles/:code/join", async (c) => { const code = c.req.param("code").toUpperCase(); let b; try { b = await c.req.json(); } catch { return c.json({ error: "Invalid body" }, 400); } if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); if (ci.members.find(m => m.userId === b.userId)) return c.json({ circle: ci }); ci.members.push({ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString() }); await saveCircleToDb(ci); trackEvent(b.userId, "circle_invite_accepted", { circle_code: code, circle_size: ci.members.length }); trackEvent(ci.creatorUserId, "circle_member_joined", { circle_code: code, circle_size: ci.members.length, new_member_name: b.userName }); pushToUserLocalized(ci.creatorUserId, { titleKey: "member_joined_title", titleParams: { name: b.userName || "Someone", circle: ci.name }, bodyKey: "member_joined_body", bodyParams: { count: ci.members.length }, type: "member_joined", circleCode: code, circleName: ci.name }); return c.json({ circle: ci }); });
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

  // v5.8.3 — only check last-one-standing on real prayer transitions, not idempotent re-PUTs
  if (prayedStateChanged) { checkLastOneStanding(ci, c.req.param("userId")).catch(() => {}); }
  if (b.streakCount !== undefined && b.streakCount > old && [3,7,14,30,60,90,180,365].includes(b.streakCount)) {
    trackEvent(c.req.param("userId"), "streak_milestone", { streak_count: b.streakCount, circle_code: c.req.param("code").toUpperCase() });
    pushToCircleMembersLocalized(ci, c.req.param("userId"), { titleKey: "streak_milestone_title", titleParams: { name: m.name, count: b.streakCount }, bodyKey: "streak_milestone_body", bodyParams: { circle: ci.name }, type: "streak_milestone", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name, extra: { memberName: m.name, streakCount: b.streakCount } });
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
  const members = ci.members.map(m => {
    const prayedToday = prayedTodayInOwnTZ(m);
    return { userId: m.userId, name: m.name, avatarUrl: avatarMap[m.userId] || m.avatarUrl || null, prayedToday, prayedAt: prayedToday ? m.lastPrayedDate : null, streakCount: m.streakCount || 0, role: m.role || "member" };
  });
  return c.json({ members, totalMembers: ci.members.length, prayedToday: members.filter(m => m.prayedToday).length });
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
  // Enforce one ask per day per (asker, target) within this circle
  const codeUpper = c.req.param("code").toUpperCase();
  const today = new Date().toISOString().split("T")[0];
  if (b.userId) {
    try {
      const existing = await pool.query("SELECT 1 FROM prayer_ask_log WHERE circle_code=$1 AND asker_user_id=$2 AND COALESCE(target_user_id,'')=$3 AND day=$4 LIMIT 1", [codeUpper, b.userId, targetUserId || "", today]);
      if (existing.rows.length > 0) return c.json({ error: "already_asked_today", targetType, targetUserId: targetUserId || null }, 409);
    } catch (err: any) { console.error("[AskLog] Check error:", err.message); }
  }
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
    if (req.requesterUserId !== b.userId) {
      const prayerName = ci.members.find(m => m.userId === b.userId)?.name || "Someone";
      // Notify the requester with read receipt
      // v5.9.1 — translate title to requester's language; keep body as their own request text
      (async () => {
        try {
          const lang = await getUserLanguage(req.requesterUserId);
          const title = t(lang, "prayer_request_prayed_title", { name: prayerName });
          const body = req.text.length > 60 ? req.text.substring(0, 60) + "..." : req.text;
          await pushToUser(req.requesterUserId, { title, body, type: "prayer_request_prayed", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name, extra: { requestId: c.req.param("rid"), prayerName, actedOn: true } });
        } catch {}
      })();
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
    ci.members.push({ userId: u.id, name: u.name || "", streakCount: 0, lastPrayedDate: null, joinedAt: new Date().toISOString() });
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
  if (!/^[A-Z0-9]{5,12}$/.test(code)) return c.json({ valid: false, reason: "bad_format" }, 400);
  const result = await pool.query("SELECT rc.user_id, u.name FROM referral_codes rc LEFT JOIN users u ON u.id = rc.user_id WHERE rc.code=$1", [code]);
  if (!result.rows[0]) return c.json({ valid: false, reason: "not_found" });
  return c.json({ valid: true, referrerName: result.rows[0].name || null });
});

app.get("/api/referrals/me", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]); const code = codeResult.rows[0]?.code || null; const referrals = await pool.query("SELECT id, referred_user_id, status, created_at, confirmed_at FROM referrals WHERE referrer_user_id=$1 ORDER BY created_at DESC", [u.id]); const enriched = []; for (const ref of referrals.rows) { let name = null; if (ref.referred_user_id) { const usr = await pool.query("SELECT name FROM users WHERE id=$1", [ref.referred_user_id]); name = usr.rows[0]?.name || null; } enriched.push({ ...ref, referred_name: name }); } const confirmedCount = referrals.rows.filter((r: any) => r.status === "confirmed").length; return c.json({ code, link: code ? `https://pramen.app/ref/${code}` : null, referrals: enriched, confirmedCount }); });
app.post("/api/referrals/generate", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const existing = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]); if (existing.rows[0]) return c.json({ code: existing.rows[0].code, link: `https://pramen.app/ref/${existing.rows[0].code}` }); let code = ""; for (let attempt = 0; attempt < 10; attempt++) { code = generateReferralCode(u.name || "PRAY"); const collision = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [code]); if (collision.rows.length === 0) break; } await pool.query("INSERT INTO referral_codes (user_id, code) VALUES ($1, $2)", [u.id, code]); trackEvent(u.id, "referral_code_generated", { code }); return c.json({ code, link: `https://pramen.app/ref/${code}` }); });
app.post("/api/referrals/track", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const { referralCode, newUserEmail } = await c.req.json(); if (!referralCode) return c.json({ error: "referralCode required" }, 400); const referrer = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [referralCode.toUpperCase()]); if (!referrer.rows[0]) return c.json({ error: "Invalid referral code" }, 404); const referrerId = referrer.rows[0].user_id; if (u.id === referrerId) return c.json({ error: "You cannot refer yourself." }, 422); const dup = await pool.query("SELECT id FROM referrals WHERE referrer_user_id=$1 AND referred_user_id=$2", [referrerId, u.id]); if (dup.rows.length > 0) return c.json({ error: "This referral has already been tracked." }, 409); const r = await pool.query("INSERT INTO referrals (referrer_user_id, referred_user_id, referred_email) VALUES ($1,$2,$3) RETURNING id", [referrerId, u.id, newUserEmail || null]); trackEvent(referrerId, "referral_tracked", { referred_user_id: u.id, code: referralCode }); const now = new Date(); const te30 = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000); await pool.query("UPDATE users SET trial_end_date=$1, updated_at=NOW() WHERE id=$2 AND subscription_status='trial'", [te30, u.id]); console.log(`[Referral] Extended trial to 30d for ${u.id.substring(0, 8)}… (code: ${referralCode})`); return c.json({ referralId: r.rows[0].id, trialDays: 30, discountApplied: true }); });
app.post("/api/referrals/confirm", async (c) => { const sec = c.req.header("X-Admin-Secret"); if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const { referredUserId } = await c.req.json(); if (!referredUserId) return c.json({ error: "referredUserId required" }, 400); const ref = await pool.query("UPDATE referrals SET status='confirmed', confirmed_at=NOW() WHERE referred_user_id=$1 AND status='pending' RETURNING referrer_user_id", [referredUserId]); if (ref.rows[0]) { pushToUserLocalized(ref.rows[0].referrer_user_id, { titleKey: "referral_confirmed_title", bodyKey: "referral_confirmed_body", type: "referral_confirmed" }); } return c.json({ confirmed: ref.rows.length }); });
app.post("/api/referrals/reverse", async (c) => { const sec = c.req.header("X-Admin-Secret"); if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const { referredUserId } = await c.req.json(); if (!referredUserId) return c.json({ error: "referredUserId required" }, 400); await pool.query("UPDATE referrals SET status='reversed', reversed_at=NOW() WHERE referred_user_id=$1 AND status='confirmed'", [referredUserId]); return c.json({ reversed: true }); });
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
    const existing = await pool.query("SELECT id FROM users WHERE email=$1", [email]);
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
  // Recent prayers (from member status updates)
  for (const m of ci.members) {
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
    // "Prayed for" activity
    for (const uid of req.prayedByUserIds) {
      const prayerMember = ci.members.find(m => m.userId === uid);
      if (prayerMember) {
        activities.push({ id: `prayed-for-${req.id}-${uid}`, memberName: prayerMember.name, memberAvatarUrl: avatarMap[uid] || null, action: "prayed_for_request", detail: req.requesterName, timestamp: req.timestamp });
      }
    }
  }
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
// ─── STREAK AT RISK PUSH (scheduled) ────────────────────────────
// ═══════════════════════════════════════════════════════════════════
async function checkStreakAtRisk(): Promise<void> {
  try {
    const today = new Date().toISOString().split("T")[0];
    // Find users who have a streak > 0 but haven't prayed today
    const result = await pool.query(
      "SELECT ud.user_id, ud.streak_count, ud.last_prayed_date FROM user_data ud WHERE ud.streak_count > 0 AND (ud.last_prayed_date IS NULL OR ud.last_prayed_date::date < $1::date)",
      [today]
    );
    for (const row of result.rows) {
      if (row.streak_count >= 3) { // Only nudge if streak is worth protecting
        pushToUserLocalized(row.user_id, {
          titleKey: "streak_at_risk_title",
          titleParams: { count: row.streak_count },
          bodyKey: "streak_at_risk_body",
          type: "streak_at_risk"
        });
        trackEvent(row.user_id, "streak_at_risk_push", { streak_count: row.streak_count });
      }
    }
    console.log(`[Streak] Checked ${result.rows.length} users at risk`);
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
    const events = raw.filter((e: any) => !exclude.has(e.distinct_id)).map((e: any) => ({ event: e.event, timestamp: e.timestamp, user: e.distinct_id?.substring(0, 8) || "?", full_user_id: e.distinct_id, properties: { type: e.properties?.type, plan: e.properties?.plan, price: e.properties?.price, trigger: e.properties?.trigger, duration: e.properties?.duration_seconds, streak: e.properties?.streak_day, is_first_open: e.properties?.is_first_open, circle_code: e.properties?.circle_code, circle_name: e.properties?.circle_name, content_type: e.properties?.content_type, city: e.properties?.$geoip_city_name || e.properties?.$set?.$geoip_city_name, country: e.properties?.$geoip_country_name || e.properties?.$set?.$geoip_country_name } }));
    const uMap: Record<string, any> = {};
    for (const e of events) { if (!uMap[e.full_user_id]) uMap[e.full_user_id] = { id: e.user, full_id: e.full_user_id, events: [], counts: {} as Record<string,number>, first_seen: e.timestamp, last_seen: e.timestamp, city: "", country: "", max_streak: 0, plan_taps: 0 }; const u = uMap[e.full_user_id]; u.events.push(e); u.counts[e.event] = (u.counts[e.event]||0)+1; if (e.timestamp < u.first_seen) u.first_seen = e.timestamp; if (e.timestamp > u.last_seen) u.last_seen = e.timestamp; if (e.properties.city) u.city = e.properties.city; if (e.properties.country) u.country = e.properties.country; if (e.properties.streak) { const s = parseInt(e.properties.streak); if (s > u.max_streak) u.max_streak = s; } if (e.event === "paywall_plan_selected") u.plan_taps++; }
    const users = Object.values(uMap).sort((a: any, b: any) => b.events.length - a.events.length);
    const ec: Record<string,number> = {}; for (const e of events) { if (e.event !== "$identify") ec[e.event] = (ec[e.event]||0)+1; }
    const fn = { first_open: new Set<string>(), onboarding: new Set<string>(), paywall: new Set<string>(), plan_tap: new Set<string>(), prayer: new Set<string>(), circle: new Set<string>(), signup: new Set<string>(), scripture: new Set<string>() };
    for (const e of events) { const u = e.full_user_id; if (e.properties.is_first_open === true || e.properties.is_first_open === "True") fn.first_open.add(u); if (e.event === "onboarding_completed") fn.onboarding.add(u); if (e.event === "paywall_viewed") fn.paywall.add(u); if (e.event === "paywall_plan_selected") fn.plan_tap.add(u); if (e.event === "prayer_logged") fn.prayer.add(u); if (e.event === "circle_created") fn.circle.add(u); if (e.event === "user_signed_up") fn.signup.add(u); if (e.event === "scripture_viewed") fn.scripture.add(u); }
    const topics: Record<string,number> = {}; for (const e of events) { if (e.event === "prayer_logged" && e.properties.type) topics[e.properties.type] = (topics[e.properties.type]||0)+1; }
    const plans: Record<string,number> = {}; for (const e of events) { if (e.event === "paywall_plan_selected" && e.properties.plan) plans[e.properties.plan] = (plans[e.properties.plan]||0)+1; }
    const dMap: Record<string, Set<string>> = {}; for (const e of events) { const d = e.timestamp.split("T")[0]; if (!dMap[d]) dMap[d] = new Set(); dMap[d].add(e.full_user_id); }
    const dau = Object.entries(dMap).map(([d, s]) => ({ date: d, dau: s.size })).sort((a, b) => b.date.localeCompare(a.date));
    return c.json({ generated_at: new Date().toISOString(), total_events: events.length, total_users: users.length, event_counts: Object.entries(ec).sort((a, b) => b[1] - a[1]), funnel: { first_open: fn.first_open.size, onboarding: fn.onboarding.size, paywall: fn.paywall.size, plan_tap: fn.plan_tap.size, prayer: fn.prayer.size, circle: fn.circle.size, signup: fn.signup.size, scripture: fn.scripture.size }, prayer_topics: Object.entries(topics).sort((a, b) => b[1] - a[1]), plan_taps: Object.entries(plans).sort((a, b) => b[1] - a[1]), daily_dau: dau, users: users.map((u: any) => ({ id: u.id, full_id: u.full_id, event_count: u.events.length, event_types: u.counts, first_seen: u.first_seen, last_seen: u.last_seen, city: u.city, country: u.country, max_streak: u.max_streak, plan_taps: u.plan_taps })), recent_events: events.filter((e: any) => e.event !== "$identify").slice(0, 500) });
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

    // Build deduplicated set of all IDs to check
    const checkedIds = new Set<string>();
    const allCandidates: { uid: string; name: string | null; email: string | null; db_status: string | null }[] = [];
    for (const user of usersResult.rows) {
      const ids = [user.id, user.device_user_id].filter(Boolean);
      for (const uid of ids) {
        if (!checkedIds.has(uid)) { checkedIds.add(uid); allCandidates.push({ uid, name: user.name, email: user.email, db_status: user.subscription_status }); }
      }
    }
    // Add revenue event user IDs not already in the set
    for (const row of revEventUsers.rows) {
      if (row.user_id && !checkedIds.has(row.user_id)) { checkedIds.add(row.user_id); allCandidates.push({ uid: row.user_id, name: null, email: null, db_status: null }); }
    }

    const subscribers: any[] = []; let totalRevenue = 0; let activeCount = 0; let trialCount = 0; let mrr = 0;
    for (const candidate of allCandidates) {
      try {
        const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(candidate.uid)}`, { headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" } });
        if (!rcRes.ok) continue;
        const rcData = (await rcRes.json()) as any; const sub = rcData.subscriber; if (!sub) continue;
        const entitlements = sub.entitlements || {}; const subscriptions = sub.subscriptions || {};
        // v5.9.9 — match RC's MRR exactly: exclude cancelled, expiring-within-24h, billing issues
        const now = new Date();
        // Sub must expire more than 24h from now to count toward MRR (RC doesn't count subs in their final day)
        const nowPlusBuffer = new Date(now.getTime() + 24 * 60 * 60 * 1000);
        const hasActive = Object.values(entitlements).some((e: any) => new Date(e.expires_date) > now);
        const hasTrial = Object.values(subscriptions).some((s: any) => s.period_type === "trial" && new Date(s.expires_date) > now);
        let userRevenue = 0;
        for (const [pid, s2] of Object.entries(subscriptions) as any[]) { if (s2.store === "app_store" || s2.store === "play_store") { if (pid.includes("yearly")) userRevenue += 29.99; else if (pid.includes("monthly")) userRevenue += 3.99; else if (pid.includes("lifetime")) userRevenue += 149.99; } }
        // Active sub = will renew: expires well in future, not trial, not cancelled, no billing issues
        const willRenew = Object.values(subscriptions).some((s: any) => new Date(s.expires_date) > nowPlusBuffer && s.period_type !== "trial" && !s.unsubscribe_detected_at && !s.billing_issues_detected_at);
        const isLifetime = Object.keys(subscriptions).some((pid) => pid.includes("lifetime"));
        if (willRenew || isLifetime) activeCount++;
        if (hasTrial) trialCount++;
        totalRevenue += userRevenue;
        // MRR: only subs that will actually renew
        for (const [pid, s2] of Object.entries(subscriptions) as any[]) {
          const expires = new Date(s2.expires_date);
          if (expires > nowPlusBuffer && s2.period_type !== "trial" && !s2.unsubscribe_detected_at && !s2.billing_issues_detected_at) {
            if (pid.includes("yearly")) mrr += 29.99 / 12;
            else if (pid.includes("monthly")) mrr += 3.99;
          }
        }
        subscribers.push({ user_id: candidate.uid, name: candidate.name || null, email: candidate.email || null, db_status: candidate.db_status, has_active: hasActive, has_trial: hasTrial, revenue: userRevenue, entitlements: Object.keys(entitlements), subscriptions: Object.entries(subscriptions).map(([pid2, s3]: [string, any]) => ({ product: pid2, store: s3.store, purchase_date: s3.purchase_date, expires_date: s3.expires_date, period_type: s3.period_type, is_active: new Date(s3.expires_date) > new Date(), auto_resume_date: s3.auto_resume_date, unsubscribe_detected_at: s3.unsubscribe_detected_at })), first_seen: sub.first_seen });
      } catch { continue; }
    }

    // Use RC v2 overview if available (most accurate), otherwise use our computed values
    const ovMetrics = rcOverview?.metrics || rcOverview;
    const summaryActive = ovMetrics?.active_subscriptions ?? activeCount;
    const summaryTrials = ovMetrics?.active_trials ?? trialCount;
    const summaryMrr = ovMetrics?.mrr ? ovMetrics.mrr / 100 : Math.round(mrr * 100) / 100; // RC v2 returns cents
    const summaryNetMrr = ovMetrics?.mrr ? Math.round(ovMetrics.mrr / 100 * (1 - APPLE_CUT) * 100) / 100 : Math.round(mrr * (1 - APPLE_CUT) * 100) / 100;

    return c.json({ generated_at: new Date().toISOString(), rc_overview: rcOverview ? "v2" : "v1_computed", summary: { active_subscriptions: summaryActive, active_trials: summaryTrials, total_revenue_estimated: totalRevenue, mrr_estimated: summaryMrr, net_mrr: summaryNetMrr, total_users_checked: allCandidates.length, subscribers_found: subscribers.filter(s => s.has_active || s.has_trial).length }, subscribers: subscribers.filter(s => s.subscriptions.length > 0 || s.has_active || s.has_trial), all_users: subscribers });
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
    const adRows = await pool.query(`SELECT channel, SUM(spend) as total_spend, SUM(installs) as total_installs FROM daily_ad_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY channel`).catch(() => ({ rows: [] }));
    const orgAcq = await pool.query(`SELECT SUM(impressions) as impressions, SUM(product_page_views) as page_views, SUM(app_units) as units FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days'`).catch(() => ({ rows: [{}] }));
    const totalAdSpend = adRows.rows.reduce((s: number, r: any) => s + parseFloat(r.total_spend||0), 0);
    const totalInstalls = adRows.rows.reduce((s: number, r: any) => s + parseInt(r.total_installs||0), 0);
    const blendedCPI = totalInstalls > 0 ? totalAdSpend / totalInstalls : null;
    const acquisitionByChannel = adRows.rows.map((r: any) => ({ channel: r.channel, spend: parseFloat(r.total_spend||0), installs: parseInt(r.total_installs||0), cpi: parseInt(r.total_installs||0) > 0 ? parseFloat(r.total_spend||0) / parseInt(r.total_installs||0) : null }));

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

    // f) Decision signals
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
        const chRes = await fetch(`https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&forHandle=${YT_CHANNEL_HANDLE}&key=${YOUTUBE_API_KEY}`);
        if (chRes.ok) {
          const chData = (await chRes.json()) as any;
          const ch = chData.items?.[0];
          if (ch) {
            youtubeStats = { channel_id: ch.id, title: ch.snippet?.title, subscriber_count: parseInt(ch.statistics?.subscriberCount||0), view_count: parseInt(ch.statistics?.viewCount||0), video_count: parseInt(ch.statistics?.videoCount||0) };
            // Fetch recent videos
            const searchRes = await fetch(`https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${ch.id}&order=date&type=video&maxResults=10&key=${YOUTUBE_API_KEY}`);
            if (searchRes.ok) {
              const searchData = (await searchRes.json()) as any;
              const videoIds = (searchData.items||[]).map((v: any) => v.id?.videoId).filter(Boolean).join(",");
              if (videoIds) {
                const statsRes = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id=${videoIds}&key=${YOUTUBE_API_KEY}`);
                if (statsRes.ok) {
                  const statsData = (await statsRes.json()) as any;
                  youtubeStats.recent_videos = (statsData.items||[]).map((v: any) => ({ id: v.id, title: v.snippet?.title, published_at: v.snippet?.publishedAt, views: parseInt(v.statistics?.viewCount||0), likes: parseInt(v.statistics?.likeCount||0), comments: parseInt(v.statistics?.commentCount||0) }));
                }
              }
            }
          }
        }
      } catch (err: any) { console.error("[YouTube]", err.message); }
    }

    const totals: Record<string, any> = {};
    for (const [ch, rows] of Object.entries(byChannel)) {
      totals[ch] = { views: rows.reduce((s: number, r: any) => s + (r.views||0), 0), subscribers_gained: rows.reduce((s: number, r: any) => s + (r.subscribers_gained||0), 0), likes: rows.reduce((s: number, r: any) => s + (r.likes||0), 0), comments: rows.reduce((s: number, r: any) => s + (r.comments||0), 0), shares: rows.reduce((s: number, r: any) => s + (r.shares||0), 0), watch_hours: rows.reduce((s: number, r: any) => s + (r.watch_hours||0), 0) };
    }
    return c.json({ generated_at: new Date().toISOString(), days, by_channel: byChannel, totals, youtube: youtubeStats });
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
      return { name: ci.name, code: ci.code, members: ci.members.length, encouragements: encCount, prayerRequests: ci.prayerRequests.length, createdAt: ci.createdAt };
    }));
    return c.json({ generated_at: new Date().toISOString(), kpis: { total_users: parseInt(uc.rows[0]?.count||"0"), active_subscribers: (sb["active"]||0)+(sb["lifetime"]||0), mrr_net: tn, revenue_gross_30d: tg, revenue_net_30d: tn, active_circles: circles.size, total_circle_members: tm, landing_visitors_7d: tv, landing_app_store_clicks_7d: tc, landing_conversion: tv > 0 ? ((tc/tv)*100).toFixed(1)+"%" : "0%" }, subscription_breakdown: sb, revenue: { daily: rv.rows, recent_events: re.rows, total_subscribers_30d: rv.rows.reduce((s: number, r: any) => s+(r.new_subscribers||0), 0), total_cancellations_30d: rv.rows.reduce((s: number, r: any) => s+(r.cancellations||0), 0) }, web: { daily: wd.rows }, app_store: { daily: ad.rows }, circles: { total: circles.size, total_members: tm, total_prayer_requests: tp, total_encouragements: encTotal, circles: circleData } });
  } catch (e: any) { return c.json({ error: "Dashboard failed", detail: e.message }, 500); }
});

app.get("/dashboard", (c) => {
  if (c.req.query("key") !== DASHBOARD_SECRET) return c.text("Unauthorized. /dashboard?key=YOUR_KEY", 401);
  try { return c.html(readFileSync("./dashboard.html", "utf-8")); } catch { return c.text("dashboard.html not found", 404); }
});

// ─── Start ───────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "3000", 10);
async function start() {
  await initDb(); await loadAllFromDb();
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
  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v5.10.1 on port ${info.port}`);
    console.log(`   PostHog: ${POSTHOG_API_KEY ? "✓" : "✗"} | Read: ${POSTHOG_PERSONAL_KEY ? "✓" : "✗"} | Plausible: ${PLAUSIBLE_API_KEY ? "✓" : "✗"}`);
    console.log(`   Apple: ${ASC_KEY_ID ? "✓" : "✗"} | RC: ${REVENUECAT_SECRET_KEY ? "✓" : "✗"} | APNs: ${APNS_KEY_ID ? "✓" : "✗"}`);
    console.log(`   Meta CAPI: ${META_CAPI_ACCESS_TOKEN ? "✓" : "✗"} pixel=${META_PIXEL_ID || "-"}`);
    console.log(`   Storage: ${R2_ACCOUNT_ID ? "✓" : "✗"} | Admin: ${ADMIN_USER_ID ? ADMIN_USER_ID.substring(0,8)+"..." : "✗"}`);
    console.log(`   Dashboard: /dashboard?key=... | Circles: ${circles.size}\n`);
  });
}
start();
