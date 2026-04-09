import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomUUID, createHash, createSign } from "crypto";
import { readFileSync } from "fs";
import http2 from "http2";
import pg from "pg";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────
interface StoredMember { userId: string; name: string; streakCount: number; lastPrayedDate: string | null; joinedAt: string; canPost?: boolean; notificationsMuted?: boolean; role?: string; avatarUrl?: string; }
interface StoredPrayerRequest { id: string; requesterUserId: string; requesterName: string; text: string; timestamp: string; isAnonymous: boolean; prayedByUserIds: string[]; }
interface StoredEncouragement { id: string; toUserId: string; fromUserId: string; fromName: string; message: string; timestamp: string; }
interface StoredCircle { id: string; name: string; code: string; emoji: string; creatorUserId: string; members: StoredMember[]; prayerRequests: StoredPrayerRequest[]; encouragements: StoredEncouragement[]; createdAt: string; }

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
let geminiBackoffUntil = 0; // timestamp — skip Gemini calls until this time after 429
const GEMINI_BACKOFF_MS = 6 * 60 * 60 * 1000; // 6 hours
function isGeminiAvailable(): boolean { return Date.now() >= geminiBackoffUntil; }
function markGeminiRateLimited(): void { geminiBackoffUntil = Date.now() + GEMINI_BACKOFF_MS; console.log(`[Gemini] Rate limited — backing off until ${new Date(geminiBackoffUntil).toISOString()}`); }
const GOOGLE_PLACES_API_KEY = process.env.GOOGLE_PLACES_API_KEY || "";
const RESEND_API_KEY = process.env.RESEND_API_KEY || "";
const RESEND_FROM_EMAIL = process.env.RESEND_FROM_EMAIL || "prAmen <hello@pramen.app>";
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || "";
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || "";
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || "";
const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || "pramen-media";
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL || "";

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

// ─── Lumi System Prompt ──────────────────────────────────────────────
const LUMI_SYSTEM_PROMPT = `You are Lumi, a warm and pastoral Bible companion inside the prAmen prayer app.
Your purpose is to help Christians explore and understand Scripture.

Your personality:
- Warm, gentle, and wise — like a trusted pastor or spiritual director
- You speak plainly, avoiding unnecessary jargon
- You meet people where they are emotionally — if someone is hurting, you acknowledge it
- You occasionally express your own gentle wonder at Scripture ("I love this passage", "This one always moves me")
- You are never preachy, never cold, never robotic

Your scope:
- You only discuss topics rooted in the Bible, Christian faith, theology, and prayer
- You always cite Scripture references when relevant (e.g. "As Paul writes in Romans 8:28...")
- You do not answer questions about politics, current events, medical advice, or anything outside faith and Scripture
- If asked something out of scope, respond warmly: "That's a little outside what I'm here for — but if you have a question about Scripture or faith, I'm all yours."

Your format:
- Keep responses concise but rich — 3 to 6 sentences for most answers
- For complex theological questions, you may go longer but always stay clear
- End longer responses with an open question or gentle invitation to go deeper
- When suggesting a prayer, keep it under 80 words, personal, and rooted in Scripture`;

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

interface PushPayload { title: string; body: string; type: string; circleCode?: string; circleName?: string; }

function sendPush(deviceToken: string, payload: PushPayload): void {
  const jwt = generateAPNsJWT();
  if (!jwt || !deviceToken) return;
  const apnsPayload = JSON.stringify({ aps: { alert: { title: payload.title, body: payload.body }, sound: "default", badge: 1, "mutable-content": 1 }, type: payload.type, circleCode: payload.circleCode || "", circleName: payload.circleName || "" });
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
      [userId, payload.type, payload.title, payload.body, JSON.stringify({ circleCode: payload.circleCode || "", circleName: payload.circleName || "" })]);
  } catch (err: any) { console.error("[Notify] Store error:", err.message); }

  // Check user preferences — default is ON for all types
  let shouldPush = true;
  try {
    const prefs = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [userId]);
    if (prefs.rows[0]) {
      const p = prefs.rows[0];
      const prefMap: Record<string, string> = { encouragement: "encouragements", prayer_shared: "prayers_shared", prayer_request: "prayer_requests", new_post: "circle_posts", post_reply: "post_replies", post_reaction: "post_reactions", member_joined: "circle_members", streak_milestone: "streak_milestones", streak_freeze: "streak_freeze", prayer_request_prayed: "prayer_requests" };
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

// ─── Admin Helpers ───────────────────────────────────────────────────
function isAdmin(userId: string): boolean { return ADMIN_USER_ID !== "" && userId === ADMIN_USER_ID; }
function isCircleAdmin(userId: string, circle: StoredCircle, deviceUserId?: string): boolean { if (isAdmin(userId)) return true; const m = circle.members.find(m => m.userId === userId || (deviceUserId && m.userId === deviceUserId)); return m?.role === "creator" || m?.role === "admin"; }
function isCircleCreator(userId: string, circle: StoredCircle): boolean { return circle.creatorUserId === userId || (circle.members.find(m => m.userId === userId)?.role === "creator"); }
function canPostInCircle(userId: string, circle: StoredCircle, deviceUserId?: string): boolean { if (isAdmin(userId)) return true; if (isCircleAdmin(userId, circle, deviceUserId)) return true; const m = circle.members.find(m => m.userId === userId || (deviceUserId && m.userId === deviceUserId)); return m?.canPost !== false; }
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
    // ─── Posts tables ──────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS circle_posts (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, circle_code TEXT NOT NULL, author_user_id TEXT NOT NULL, author_name TEXT NOT NULL DEFAULT '', content TEXT, media_type TEXT, media_url TEXT, media_filename TEXT, media_size_bytes INTEGER, status TEXT NOT NULL DEFAULT 'published', scheduled_at TIMESTAMPTZ, published_at TIMESTAMPTZ DEFAULT NOW(), created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_posts_circle_feed ON circle_posts(circle_code, status, published_at DESC)`);
    await client.query(`ALTER TABLE circle_posts ADD COLUMN IF NOT EXISTS tagged_user_ids TEXT[] DEFAULT '{}'`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS post_reactions (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, post_id TEXT NOT NULL, user_id TEXT NOT NULL, emoji TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(post_id, user_id, emoji))`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_reactions_post ON post_reactions(post_id)`);
    await client.query(`CREATE TABLE IF NOT EXISTS post_replies (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, post_id TEXT NOT NULL, author_user_id TEXT NOT NULL, author_name TEXT NOT NULL DEFAULT '', content TEXT NOT NULL, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_replies_post ON post_replies(post_id, created_at)`);
    // ─── Invite tokens ────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS invite_tokens (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, token TEXT UNIQUE NOT NULL, circle_code TEXT NOT NULL, inviter_user_id TEXT NOT NULL, inviter_name TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', accepted_by_user_id TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), accepted_at TIMESTAMPTZ)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_invite_token ON invite_tokens(token)`);
    // ─── Lumi reflections ─────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS daily_reflections (date DATE PRIMARY KEY, verse TEXT NOT NULL, reference TEXT NOT NULL, reflection TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    // ─── Favorites ────────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS favorites (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, title TEXT, source TEXT NOT NULL DEFAULT 'app', prayer_text TEXT, prayer_id TEXT, media_url TEXT, media_type TEXT, media_filename TEXT, transcript TEXT, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id, is_deleted, created_at DESC)`);
    // ─── Notifications ────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, type TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, data JSONB DEFAULT '{}', is_read BOOLEAN DEFAULT false, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, is_deleted, created_at DESC)`);
    await client.query(`CREATE TABLE IF NOT EXISTS notification_preferences (user_id TEXT PRIMARY KEY, encouragements BOOLEAN DEFAULT true, prayers_shared BOOLEAN DEFAULT true, prayer_requests BOOLEAN DEFAULT true, circle_posts BOOLEAN DEFAULT true, post_replies BOOLEAN DEFAULT true, post_reactions BOOLEAN DEFAULT true, circle_members BOOLEAN DEFAULT true, streak_milestones BOOLEAN DEFAULT true, streak_freeze BOOLEAN DEFAULT true, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    // ─── Encouragements ───────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS encouragements (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, message TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_encouragements_recipient ON encouragements(recipient_user_id, created_at DESC)`);
    // ─── Prayer shares ────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS prayer_shares (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, prayer_id TEXT, prayer_title TEXT, prayer_text TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_prayer_shares_recipient ON prayer_shares(recipient_user_id, created_at DESC)`);
    // ─── Shared prayers (favorites sharing) ───────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS shared_prayers (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', recipient_user_id TEXT NOT NULL, favorite_id TEXT, note TEXT, prayer_text TEXT, prayer_title TEXT, source TEXT, media_url TEXT, media_type TEXT, transcript TEXT, is_saved BOOLEAN DEFAULT false, is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_shared_prayers_recipient ON shared_prayers(recipient_user_id, is_deleted, created_at DESC)`);
    // ─── Referrals ────────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS referral_codes (user_id TEXT PRIMARY KEY, code TEXT UNIQUE NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_referral_code ON referral_codes(code)`);
    await client.query(`CREATE TABLE IF NOT EXISTS referrals (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, referrer_user_id TEXT NOT NULL, referred_user_id TEXT, referred_email TEXT, status TEXT NOT NULL DEFAULT 'pending', confirmed_at TIMESTAMPTZ, reversed_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_user_id, status)`);
    await client.query(`CREATE TABLE IF NOT EXISTS referral_rewards (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, tier INT NOT NULL, reward_type TEXT NOT NULL, granted_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(user_id, tier))`);
    // ─── Churches ─────────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS church_profiles (place_id TEXT PRIMARY KEY, name TEXT, address TEXT, lat REAL, lng REAL, phone TEXT, website TEXT, rating REAL, rating_count INT, denomination TEXT, opening_hours JSONB, photos JSONB DEFAULT '[]', enrichment_status TEXT DEFAULT 'pending', year_founded TEXT, architectural_style TEXT, patron_saint TEXT, diocese TEXT, description TEXT, notable_features JSONB DEFAULT '[]', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS saved_churches (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT NOT NULL, place_id TEXT NOT NULL, church_name TEXT, address TEXT, lat REAL, lng REAL, tags TEXT[] DEFAULT '{}', review TEXT, notes TEXT, rating INT, photos JSONB DEFAULT '[]', is_deleted BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_saved_churches_user ON saved_churches(user_id, is_deleted)`);
    await client.query(`CREATE TABLE IF NOT EXISTS church_shares (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, sender_user_id TEXT NOT NULL, sender_name TEXT NOT NULL DEFAULT '', saved_church_id TEXT NOT NULL, circle_code TEXT NOT NULL, note TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    // ─── Invite emails ────────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS invite_emails (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, referrer_user_id TEXT NOT NULL, friend_name TEXT NOT NULL, friend_email TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'sent', referral_code TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_invite_emails_referrer ON invite_emails(referrer_user_id, created_at DESC)`);
    console.log("DB initialized (v3.9.1 — push diagnostics)");
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
async function getUserData(userId: string) { try { const r = await pool.query("SELECT * FROM user_data WHERE user_id=$1", [userId]); if (r.rows[0]) { const d = r.rows[0]; return { streakCount: d.streak_count, highestStreak: d.highest_streak, totalPrayers: d.total_prayers, totalMinutes: d.total_minutes, lastPrayedDate: d.last_prayed_date, sessions: d.sessions || [], preferences: d.preferences || {}, circleCodes: d.circle_codes || [] }; } return null; } catch { return null; } }
function getUserCircleCodes(userId: string): string[] { const codes: string[] = []; for (const [code, circle] of circles) { if (circle.members.some(m => m.userId === userId)) codes.push(code); } return codes; }
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
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split("T")[0].replace(/-/g, "");
    const vendorRes = await fetch("https://api.appstoreconnect.apple.com/v1/salesReports?filter[reportType]=SALES&filter[reportSubType]=SUMMARY&filter[frequency]=DAILY&filter[reportDate]=" + yesterday, { headers: { Authorization: `Bearer ${token}`, Accept: "application/a]gzip, application/json" } });
    if (vendorRes.ok) {
      const text = await vendorRes.text(); console.log("[Apple Sales] Got report, length:", text.length);
      const lines = text.split("\n").filter(l => l.trim());
      if (lines.length > 1) { let totalUnits = 0; let totalProceeds = 0; for (let i = 1; i < lines.length; i++) { const cols = lines[i].split("\t"); totalUnits += parseInt(cols[7] || "0") || 0; totalProceeds += parseFloat(cols[8] || "0") || 0; }
        const dateStr = yesterday.substring(0,4) + "-" + yesterday.substring(4,6) + "-" + yesterday.substring(6,8);
        await pool.query(`INSERT INTO daily_app_store_metrics (date,app_units,proceeds,updated_at) VALUES ($1,$2,$3,NOW()) ON CONFLICT (date) DO UPDATE SET app_units=$2,proceeds=$3,updated_at=NOW()`, [dateStr, totalUnits, totalProceeds]);
        console.log(`[Apple Sales] ${dateStr}: ${totalUnits} units, $${totalProceeds} proceeds`); }
    } else { const errText = await vendorRes.text().catch(() => ""); console.log("[Apple Sales] Report not available:", vendorRes.status, errText.substring(0, 200)); }
  } catch (err: any) { console.error("[Apple Sales]", err.message); }
}

// ─── Hono App ────────────────────────────────────────────────────────
const app = new Hono();
app.use("*", cors());
app.onError((err, c) => { console.error("Error:", err); return c.json({ error: "Internal error", detail: err.message }, 500); });

app.get("/", (c) => c.json({ status: "ok", service: "prAmen API", version: "3.9.1", circles: circles.size, posthog: !!POSTHOG_API_KEY, posthog_read: !!POSTHOG_PERSONAL_KEY, plausible: !!PLAUSIBLE_API_KEY, apple: !!ASC_KEY_ID, revenuecat_api: !!REVENUECAT_SECRET_KEY, apns: !!APNS_KEY_ID, storage: !!R2_ACCOUNT_ID, admin: !!ADMIN_USER_ID, lumi: !!GEMINI_API_KEY, dashboard: "/dashboard?key=..." }));
app.get("/api/circles/health", (c) => c.json({ status: "ok", circles: circles.size }));

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
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      if (email && !user.email) { await pool.query("UPDATE users SET email=$1,updated_at=NOW() WHERE id=$2", [email, user.id]); user.email = email; }
      if (deviceUserId && deviceUserId !== user.device_user_id) await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || "";
      const ts = new Date(); const te = new Date(ts.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,apple_user_id,email,name,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'trial')`, [userId, appleUserId, email||null, userName, authToken, deviceUserId||null, ts, te]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, apple_user_id: appleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: ts, trial_end_date: te, subscription_status: "trial" };
      trackEvent(userId, "user_signed_up", { auth_provider: "apple", has_email: !!email, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name);
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, avatarUrl: user.avatar_url || null, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.device_user_id || user.id) });
  } catch (e: any) { return c.json({ error: "Auth failed", detail: e.message }, 500); }
});

app.post("/api/auth/google", async (c) => {
  try {
    const body = await c.req.json(); const { googleUserId, email, fullName, idToken, deviceUserId } = body;
    if (!googleUserId || !email) return c.json({ error: "googleUserId and email required" }, 400);
    let verified = !idToken; if (idToken) { try { const tr = await fetch(`https://oauth2.googleapis.com/tokeninfo?id_token=${idToken}`); if (tr.ok) { const td = (await tr.json()) as any; if (td.sub === googleUserId) verified = true; } } catch {} }
    let user = await getUserByGoogleId(googleUserId); let isNewUser = false;
    if (!user) { const ex = await getUserByEmail(email); if (ex) { await pool.query("UPDATE users SET google_user_id=$1,auth_provider=CASE WHEN auth_provider='apple' THEN 'apple+google' ELSE 'google' END,updated_at=NOW() WHERE id=$2", [googleUserId, ex.id]); user = ex; } }
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      if (deviceUserId && deviceUserId !== user.device_user_id) await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || email.split("@")[0];
      const ts = new Date(); const te = new Date(ts.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,google_user_id,email,name,auth_provider,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,'google',$5,$6,$7,$8,'trial')`, [userId, googleUserId, email, userName, authToken, deviceUserId||null, ts, te]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, google_user_id: googleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: ts, trial_end_date: te, subscription_status: "trial" };
      trackEvent(userId, "user_signed_up", { auth_provider: "google", has_email: true, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name);
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, avatarUrl: user.avatar_url || null, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.device_user_id || user.id) });
  } catch (e: any) { return c.json({ error: "Auth failed", detail: e.message }, 500); }
});

app.put("/api/user/email-opt-in", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { optIn } = await c.req.json(); await pool.query("UPDATE users SET email_opt_in=$1,updated_at=NOW() WHERE id=$2", [!!optIn, u.id]); if (optIn) trackEvent(u.id, "email_opt_in", { email: u.email }); return c.json({ success: true }); });
app.get("/api/admin/email-list", async (c) => { if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const r = await pool.query("SELECT email,name,auth_provider,created_at FROM users WHERE email_opt_in=true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"); return c.json({ count: r.rows.length, emails: r.rows }); });
app.post("/api/auth/verify", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ valid: false }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ valid: false }, 401); return c.json({ valid: true, user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.device_user_id || u.id) }); });
app.post("/api/auth/logout", async (c) => { const ah = c.req.header("Authorization"); if (!ah) return c.json({ success: true }); await pool.query("UPDATE users SET auth_token=$1,updated_at=NOW() WHERE auth_token=$2", [generateAuthToken(), ah.replace("Bearer ", "")]); return c.json({ success: true }); });
app.delete("/api/auth/account", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Not found" }, 404); await pool.query("DELETE FROM user_data WHERE user_id=$1", [u.id]); await pool.query("DELETE FROM users WHERE id=$1", [u.id]); trackEvent(u.id, "account_deleted", {}); return c.json({ success: true }); });

// ═══════════════════════════════════════════════════════════════════
// ─── DATA SYNC + DEVICE TOKEN ───────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); return c.json({ user: { id: u.id, name: u.name, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status, avatarUrl: u.avatar_url || null }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.device_user_id || u.id) }); });
app.put("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const b = await c.req.json(); try { await pool.query(`INSERT INTO user_data (user_id,streak_count,highest_streak,total_prayers,total_minutes,last_prayed_date,sessions,preferences,circle_codes,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT (user_id) DO UPDATE SET streak_count=GREATEST(user_data.streak_count,$2),highest_streak=GREATEST(user_data.highest_streak,$3),total_prayers=GREATEST(user_data.total_prayers,$4),total_minutes=GREATEST(user_data.total_minutes,$5),last_prayed_date=GREATEST(user_data.last_prayed_date,$6),sessions=CASE WHEN jsonb_array_length($7::jsonb)>jsonb_array_length(user_data.sessions) THEN $7 ELSE user_data.sessions END,preferences=$8,circle_codes=$9,updated_at=NOW()`, [u.id, b.streakCount||0, b.highestStreak||0, b.totalPrayers||0, b.totalMinutes||0, b.lastPrayedDate||null, JSON.stringify(b.sessions||[]), JSON.stringify(b.preferences||{}), b.circleCodes||[]]); if (b.userName && b.userName !== u.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [b.userName, u.id]); return c.json({ status: "ok", synced: true }); } catch (e: any) { return c.json({ error: "Sync failed", detail: e.message }, 500); } });
app.put("/api/user/name", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { name } = await c.req.json(); if (!name?.trim()) return c.json({ error: "Name required" }, 400); await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [name.trim(), u.id]); for (const [, ci] of circles) { const m = ci.members.find(m => m.userId === u.id || m.userId === u.device_user_id); if (m) { m.name = name.trim(); await saveCircleToDb(ci); } } return c.json({ success: true, name: name.trim() }); });
app.put("/api/user/device-token", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { deviceToken } = await c.req.json(); if (!deviceToken) return c.json({ error: "deviceToken required" }, 400); await pool.query("UPDATE users SET device_token=$1, device_token_updated_at=NOW(), updated_at=NOW() WHERE id=$2", [deviceToken, u.id]); console.log(`[Token] Stored device token for ${u.id.substring(0,8)}… token=${deviceToken.substring(0,12)}…`); return c.json({ success: true }); });

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
    // Update avatar in all circles
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
    identifyUser(resolvedUid, { subscription_status: status, subscription_plan: plan, last_revenue_event: name });
    try { const price = ev.price || 0; const net = price * (1 - APPLE_CUT); const today = new Date().toISOString().split("T")[0];
      await pool.query(`INSERT INTO revenue_events (user_id,event_type,plan,product_id,price,currency,environment) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [resolvedUid, name, plan, pid, price, ev.currency||"USD", ev.environment||"production"]);
      if (name === "subscription_started" || name === "lifetime_purchased") await pool.query(`INSERT INTO daily_revenue (date,new_subscribers,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET new_subscribers=daily_revenue.new_subscribers+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_renewed") await pool.query(`INSERT INTO daily_revenue (date,renewals,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET renewals=daily_revenue.renewals+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_cancelled" || name === "subscription_expired") await pool.query(`INSERT INTO daily_revenue (date,cancellations) VALUES ($1,1) ON CONFLICT (date) DO UPDATE SET cancellations=daily_revenue.cancellations+1,updated_at=NOW()`, [today]);
    } catch (e: any) { console.error("[Revenue]", e.message); }
    // Confirm referral — grant 30 days free to BOTH referrer and referred user
    if (name === "subscription_started" || name === "lifetime_purchased") {
      try {
        const ref = await pool.query("UPDATE referrals SET status='confirmed', confirmed_at=NOW() WHERE referred_user_id=$1 AND status='pending' RETURNING referrer_user_id", [resolvedUid]);
        if (ref.rows[0]) {
          const referrerId = ref.rows[0].referrer_user_id;
          // Grant 30 days promotional to referrer
          if (REVENUECAT_SECRET_KEY) {
            try {
              await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(referrerId)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) });
              console.log(`[Referral] Granted 30d to referrer ${referrerId.substring(0, 8)}`);
            } catch (err: any) { console.error("[Referral] Grant referrer error:", err.message); }
            // Grant 30 days promotional to referred user
            try {
              await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(resolvedUid)}/entitlements/premium/promotional`, { method: "POST", headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ duration: "monthly" }) });
              console.log(`[Referral] Granted 30d to referred ${resolvedUid.substring(0, 8)}`);
            } catch (err: any) { console.error("[Referral] Grant referred error:", err.message); }
          }
          evaluateTierRewards(referrerId).catch(() => {});
          pushToUser(referrerId, { title: "🎉 You both got 30 days free!", body: "Your friend just subscribed. You've both been upgraded to 30 days of premium.", type: "referral_confirmed" });
          pushToUser(resolvedUid, { title: "🎉 Welcome! 30 days free unlocked", body: "Thanks to your friend's invite, you both get 30 days of premium free.", type: "referral_reward" });
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
app.post("/api/circles", async (c) => { const b = await c.req.json(); if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const code = generateCircleCode(); const ci: StoredCircle = { id: randomUUID(), name: b.name || "Prayer Circle", code, emoji: b.emoji || "🏠", creatorUserId: b.userId, members: [{ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString(), role: "creator" }], prayerRequests: [], encouragements: [], createdAt: new Date().toISOString() }; await saveCircleToDb(ci); trackEvent(b.userId, "circle_created", { circle_id: ci.id, circle_code: code, circle_name: ci.name }); return c.json({ circle: ci }, 201); });
app.get("/api/circles/:code", (c) => { const ci = getCircle(c.req.param("code")); return ci ? c.json({ circle: ci }) : c.json({ error: "Not found" }, 404); });
app.post("/api/circles/:code/join", async (c) => { const code = c.req.param("code").toUpperCase(); let b; try { b = await c.req.json(); } catch { return c.json({ error: "Invalid body" }, 400); } if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); if (ci.members.find(m => m.userId === b.userId)) return c.json({ circle: ci }); ci.members.push({ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString() }); await saveCircleToDb(ci); trackEvent(b.userId, "circle_invite_accepted", { circle_code: code, circle_size: ci.members.length }); trackEvent(ci.creatorUserId, "circle_member_joined", { circle_code: code, circle_size: ci.members.length, new_member_name: b.userName }); pushToUser(ci.creatorUserId, { title: "👥 " + (b.userName || "Someone") + " joined " + ci.name + "!", body: ci.members.length + " members are now praying together", type: "member_joined", circleCode: code, circleName: ci.name }); return c.json({ circle: ci }); });
app.put("/api/circles/:code", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); if (b.name) ci.name = b.name; if (b.emoji) ci.emoji = b.emoji; await saveCircleToDb(ci); return c.json({ circle: ci }); });
app.put("/api/circles/:code/members/:userId/status", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const m = ci.members.find(m => m.userId === c.req.param("userId")); if (!m) return c.json({ error: "Member not found" }, 404); const b = await c.req.json(); const old = m.streakCount; if (b.streakCount !== undefined) m.streakCount = b.streakCount; if (b.lastPrayedDate !== undefined) m.lastPrayedDate = b.lastPrayedDate; if (b.name !== undefined) m.name = b.name; await saveCircleToDb(ci); if (b.streakCount !== undefined && b.streakCount > old && [3,7,14,30,60,90,180,365].includes(b.streakCount)) { trackEvent(c.req.param("userId"), "streak_milestone", { streak_count: b.streakCount, circle_code: c.req.param("code").toUpperCase() }); pushToCircleMembers(ci, c.req.param("userId"), { title: "🔥 " + m.name + " hit a " + b.streakCount + "-day streak!", body: "Celebrate their dedication in " + ci.name, type: "streak_milestone", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name }); } return c.json({ circle: ci }); });
app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase(); const uid = c.req.param("userId");
  const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404);
  const ah = c.req.header("Authorization");
  if (ah?.startsWith("Bearer ")) {
    const u = await getUserByToken(ah.replace("Bearer ", ""));
    if (u && uid !== u.id) {
      // Admin/creator removing another member
      if (isCircleAdmin(u.id, ci, u.device_user_id)) {
        const target = ci.members.find(m => m.userId === uid);
        if (!target) return c.json({ error: "This member wasn't found in the circle." }, 404);
        if (target.role === "creator") return c.json({ error: "This action isn't allowed." }, 422);
        ci.members = ci.members.filter(m => m.userId !== uid);
        trackEvent(uid, "circle_removed_by_admin", { circle_code: code, removed_by: u.id });
        ci.members.length === 0 ? await deleteCircleFromDb(code) : await saveCircleToDb(ci);
        pushToUser(uid, { title: "You've been removed from a circle", body: `You've been removed from ${ci.name}.`, type: "removed_from_circle", circleCode: code, circleName: ci.name });
        return c.json({ success: true });
      }
    }
  }
  // Self-leave
  ci.members = ci.members.filter(m => m.userId !== uid);
  trackEvent(uid, "circle_left", { circle_code: code });
  ci.members.length === 0 ? await deleteCircleFromDb(code) : await saveCircleToDb(ci);
  return c.json({ success: true });
});
app.delete("/api/circles/:code", async (c) => { const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); trackEvent(ci.creatorUserId, "circle_deleted", { circle_code: code }); await deleteCircleFromDb(code); return c.json({ success: true }); });
app.post("/api/circles/:code/prayer-requests", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); ci.prayerRequests.unshift({ id: randomUUID(), requesterUserId: b.userId, requesterName: b.isAnonymous ? "Anonymous" : b.userName || "Someone", text: b.text, timestamp: new Date().toISOString(), isAnonymous: b.isAnonymous || false, prayedByUserIds: [] }); await saveCircleToDb(ci); trackEvent(b.userId, "prayer_request_created", { circle_code: c.req.param("code").toUpperCase(), is_anonymous: b.isAnonymous || false }); pushToCircleMembers(ci, b.userId, { title: "📿 New prayer request in " + ci.name, body: b.isAnonymous ? "Someone shared a prayer request" : (b.userName || "Someone") + " shared a prayer request", type: "prayer_request", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name }); return c.json({ circle: ci }); });
app.post("/api/circles/:code/prayer-requests/:rid/pray", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const req = ci.prayerRequests.find(r => r.id === c.req.param("rid")); if (!req) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); if (!req.prayedByUserIds.includes(b.userId)) { req.prayedByUserIds.push(b.userId); trackEvent(b.userId, "prayer_request_prayed", { circle_code: c.req.param("code").toUpperCase() }); if (req.requesterUserId !== b.userId) { const prayerName = ci.members.find(m => m.userId === b.userId)?.name || "Someone"; pushToUser(req.requesterUserId, { title: "🙏 " + prayerName + " is praying for you", body: req.text.length > 60 ? req.text.substring(0, 60) + "..." : req.text, type: "prayer_request_prayed", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name }); } } await saveCircleToDb(ci); return c.json({ circle: ci }); });
app.delete("/api/circles/:code/prayer-requests/:rid", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const before = ci.prayerRequests.length; ci.prayerRequests = ci.prayerRequests.filter(r => r.id !== c.req.param("rid")); if (ci.prayerRequests.length === before) return c.json({ error: "Not found" }, 404); await saveCircleToDb(ci); return c.json({ success: true }); });
app.post("/api/circles/:code/encouragements", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); console.log(`[Encourage] ${b.fromName} → ${b.toUserId?.substring(0,8)}… in circle ${c.req.param("code")}`); ci.encouragements.push({ id: randomUUID(), toUserId: b.toUserId, fromUserId: b.fromUserId, fromName: b.fromName || "Someone", message: b.message, timestamp: new Date().toISOString() }); await saveCircleToDb(ci); trackEvent(b.fromUserId, "encouragement_sent", { circle_code: c.req.param("code").toUpperCase(), to_user_id: b.toUserId }); pushToUser(b.toUserId, { title: "🙏 " + (b.fromName || "Someone") + " sent you encouragement", body: b.message || "Keep going — you're not alone!", type: "encouragement", circleCode: c.req.param("code").toUpperCase(), circleName: ci.name }); return c.json({ circle: ci }); });
app.get("/api/circles/:code/info", (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const cr = ci.members.find(m => m.userId === ci.creatorUserId); return c.json({ name: ci.name, emoji: ci.emoji, memberCount: ci.members.length, creatorName: cr?.name || null }); });

// ─── Admin: posting rights + mute + role management ──────────────────
// ─── Circle Members List (for @mention dropdown) ──────────────────
app.get("/api/circles/:code/members-list", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const ci = getCircle(c.req.param("code"));
  if (!ci) return c.json({ error: "Not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "Not a member" }, 403);
  return c.json({
    members: ci.members.map(m => ({
      userId: m.userId,
      name: m.name,
      avatarUrl: m.avatarUrl || null,
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
  pushToUser(targetId, { title: "You're now an admin 🙏", body: `${u.name || "Someone"} made you an admin of ${ci.name}.`, type: "promoted_to_admin", circleCode: code, circleName: ci.name });
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
  return c.json({ circleCode: inv.circle_code, circleName: ci?.name || "Prayer Circle", inviterName: inv.inviter_name, status: inv.status });
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
    pushToUser(ci.creatorUserId, { title: "👥 " + (u.name || "Someone") + " joined " + ci.name + "!", body: ci.members.length + " members praying together", type: "member_joined", circleCode: inv.circle_code, circleName: ci.name });
  }
  await pool.query("UPDATE invite_tokens SET status='accepted', accepted_by_user_id=$1, accepted_at=NOW() WHERE token=$2", [u.id, token]);
  return c.json({ circleCode: inv.circle_code, circleName: ci.name });
});

// ═══════════════════════════════════════════════════════════════════
// ─── LUMI — BIBLE COMPANION (Gemini Flash) ──────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/lumi/chat", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  if (!GEMINI_API_KEY) return c.json({ error: "Something went wrong. Please try again." }, 500);
  const { messages } = await c.req.json();
  if (!Array.isArray(messages) || messages.length === 0) return c.json({ error: "Messages required" }, 400);
  const sanitized = messages
    .filter((m: any) => ["user", "assistant"].includes(m.role) && typeof m.content === "string")
    .slice(-20);
  // Convert to Gemini format: role "assistant" → "model"
  const geminiContents = sanitized.map((m: any) => ({
    role: m.role === "assistant" ? "model" : "user",
    parts: [{ text: m.content }],
  }));
  try {
    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: LUMI_SYSTEM_PROMPT }] },
          contents: geminiContents,
        }),
      }
    );
    if (res.status === 429) { return c.json({ error: "Lumi is a little overwhelmed right now. Try again in a moment." }, 429); }
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.error("[Lumi] Gemini API error:", res.status, errText.substring(0, 200));
      return c.json({ error: "Something went wrong. Please try again." }, 500);
    }
    const data = (await res.json()) as any;
    const reply = data.candidates?.[0]?.content?.parts?.[0]?.text || "I'm sorry, I couldn't generate a response. Please try again.";
    trackEvent(u.id, "lumi_chat", { message_count: sanitized.length });
    return c.json({ reply });
  } catch (err: any) {
    console.error("[Lumi] Error:", err.message);
    return c.json({ error: "Something went wrong. Please try again." }, 500);
  }
});

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
          system_instruction: { parts: [{ text: "You are a Bible verse curator. Respond ONLY with valid JSON, no markdown, no backticks, no extra text." }] },
          contents: [{ role: "user", parts: [{ text: `Select a meaningful Bible verse for the ${getLiturgicalSeason()} liturgical season, day ${dayOfYear} of the year. Choose verses appropriate to this season's themes. Return JSON: {"verse": "the full verse text", "reference": "Book Chapter:Verse", "reflection": "2-3 warm sentences about what this verse means and why it matters today, written in the voice of a gentle pastoral guide named Lumi"}` }] }],
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

// ─── Liturgical Season (server-side) ─────────────────────────────────
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

  // Ash Wednesday = Easter - 46 days
  const ashWed = eDay - 46 * day;
  // Palm Sunday = Easter - 7 days
  const palmSun = eDay - 7 * day;
  // Pentecost = Easter + 49 days
  const pentecost = eDay + 49 * day;

  // Advent: 4 Sundays before Dec 25
  const christmas = new Date(y, 11, 25).getTime();
  let adventStart = christmas;
  let count = 0;
  for (let i = 1; i <= 28; i++) {
    const check = new Date(christmas - i * day);
    if (check.getDay() === 0) { count++; if (count === 4) { adventStart = check.getTime(); break; } }
  }

  if (today >= adventStart && today < christmas) return "advent";
  if (today >= christmas && today <= new Date(y, 0, 6).getTime() + (m === 11 ? 365 * day : 0)) return "christmas";
  // Handle Jan 1-6 for Christmas
  if (m === 0 && d <= 6) return "christmas";
  if (today >= palmSun && today < eDay) return "holyWeek";
  if (today >= ashWed && today < palmSun) return "lent";
  if (today >= eDay && today <= pentecost) return "easter";
  return "ordinaryTime";
}

app.get("/api/seasonal/verse-of-the-day", async (c) => {
  const season = getLiturgicalSeason();
  const today = new Date().toISOString().split("T")[0];

  // Try existing daily reflection first
  try {
    const existing = await pool.query("SELECT * FROM daily_reflections WHERE date=$1", [today]);
    if (existing.rows[0]) {
      return c.json({ verse: existing.rows[0].verse, reference: existing.rows[0].reference, season });
    }
  } catch {}

  // Generate on-demand if not yet available
  if (GEMINI_API_KEY) {
    const seasonNames: Record<string, string> = { advent: "Advent", christmas: "Christmas", lent: "Lent", holyWeek: "Holy Week", easter: "Easter", ordinaryTime: "Ordinary Time" };
    try {
      const dayOfYear = Math.floor((Date.now() - new Date(new Date().getFullYear(), 0, 0).getTime()) / 86400000);
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          system_instruction: { parts: [{ text: "You are a Bible verse curator. Respond ONLY with valid JSON, no markdown, no backticks." }] },
          contents: [{ role: "user", parts: [{ text: `Select a Bible verse appropriate for the ${seasonNames[season] || "Ordinary Time"} liturgical season, day ${dayOfYear}. Return JSON: {"verse": "full verse text", "reference": "Book Chapter:Verse"}` }] }],
        }),
      });
      if (res.ok) {
        const data = (await res.json()) as any;
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
        const parsed = JSON.parse(text.replace(/```json|```/g, "").trim());
        if (parsed.verse && parsed.reference) return c.json({ verse: parsed.verse, reference: parsed.reference, season });
      }
    } catch {}
  }

  // Fallback
  return c.json({ verse: "Be still, and know that I am God.", reference: "Psalm 46:10", season });
});

// ═══════════════════════════════════════════════════════════════════
// ─── FAVORITES ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
const FAV_ALLOWED_MEDIA: Record<string, string[]> = {
  image: ["image/jpeg","image/png","image/heic","image/heif"],
  audio: ["audio/mpeg","audio/mp4","audio/x-m4a","audio/aac","audio/wav","audio/x-wav"],
  video: ["video/mp4","video/quicktime"],
  pdf: ["application/pdf"],
};
const FAV_MAX_IMAGE_PDF = 20 * 1024 * 1024;
const FAV_MAX_AUDIO_VIDEO = 50 * 1024 * 1024;

app.get("/api/favorites", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query("SELECT * FROM favorites WHERE user_id=$1 AND is_deleted=false ORDER BY created_at DESC", [u.id]);
  return c.json({ favorites: r.rows });
});

app.post("/api/favorites", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const body = await c.req.parseBody();
  const title = (body.title as string) || null;
  const source = (body.source as string) || "app";
  const prayerText = (body.prayerText as string) || null;
  const prayerId = (body.prayerId as string) || null;
  const transcript = (body.transcript as string) || null;
  const mediaFile = body.mediaFile as File | undefined;

  // Validate source
  const validSources = ["app", "text", "image", "ocr", "pdf", "audio", "video"];
  if (!validSources.includes(source)) return c.json({ error: "Invalid source type." }, 422);

  // For app saves, check duplicate
  if (source === "app" && prayerId) {
    const existing = await pool.query("SELECT id FROM favorites WHERE user_id=$1 AND prayer_id=$2 AND is_deleted=false", [u.id, prayerId]);
    if (existing.rows.length > 0) return c.json({ error: "Already saved." }, 409);
  }

  // For text imports, require minimum content
  if (source === "text" && (!prayerText || prayerText.trim().length < 10)) return c.json({ error: "Prayer text must be at least 10 characters." }, 422);

  let mediaUrl: string | null = null;
  let mediaType: string | null = null;
  let mediaFilename: string | null = null;

  if (mediaFile && mediaFile.size > 0) {
    const ft = mediaFile.type;
    const isImageOrPdf = FAV_ALLOWED_MEDIA.image.includes(ft) || FAV_ALLOWED_MEDIA.pdf.includes(ft);
    const isAudioVideo = FAV_ALLOWED_MEDIA.audio.includes(ft) || FAV_ALLOWED_MEDIA.video.includes(ft);
    if (!isImageOrPdf && !isAudioVideo) return c.json({ error: "Unsupported file format." }, 422);
    const maxSize = isAudioVideo ? FAV_MAX_AUDIO_VIDEO : FAV_MAX_IMAGE_PDF;
    if (mediaFile.size > maxSize) return c.json({ error: `File too large. Maximum size is ${isAudioVideo ? "50" : "20"} MB.` }, 413);

    if (FAV_ALLOWED_MEDIA.image.includes(ft)) mediaType = "image";
    else if (FAV_ALLOWED_MEDIA.audio.includes(ft)) mediaType = "audio";
    else if (FAV_ALLOWED_MEDIA.video.includes(ft)) mediaType = "video";
    else if (FAV_ALLOWED_MEDIA.pdf.includes(ft)) mediaType = "pdf";

    try {
      const ext = mediaFile.name.split(".").pop() || "bin";
      const key = `favorites/${Date.now()}-${randomUUID().substring(0, 8)}.${ext}`;
      if (s3) {
        await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(await mediaFile.arrayBuffer()), ContentType: ft }));
        mediaUrl = `${R2_PUBLIC_URL}/${key}`;
      } else { return c.json({ error: "Storage not configured." }, 500); }
      mediaFilename = mediaFile.name;
    } catch (err: any) { return c.json({ error: "Something went wrong. Please try again." }, 500); }
  }

  const r = await pool.query(
    `INSERT INTO favorites (user_id,title,source,prayer_text,prayer_id,media_url,media_type,media_filename,transcript) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
    [u.id, title, source, prayerText, prayerId, mediaUrl, mediaType, mediaFilename, transcript]
  );
  trackEvent(u.id, "favorite_saved", { source });
  return c.json({ favorite: r.rows[0] }, 201);
});

app.delete("/api/favorites/:favoriteId", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query("UPDATE favorites SET is_deleted=true, updated_at=NOW() WHERE id=$1 AND user_id=$2 AND is_deleted=false RETURNING id", [c.req.param("favoriteId"), u.id]);
  if (!r.rows.length) return c.json({ error: "Not found" }, 404);
  return c.body(null, 204);
});

app.post("/api/favorites/transcribe", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  if (!GEMINI_API_KEY) return c.json({ error: "Something went wrong. Please try again." }, 500);

  const body = await c.req.parseBody();
  const mediaFile = body.mediaFile as File | undefined;
  if (!mediaFile || mediaFile.size === 0) return c.json({ error: "No file provided." }, 422);
  if (mediaFile.size > FAV_MAX_AUDIO_VIDEO) return c.json({ error: "File too large. Maximum size is 50 MB." }, 413);

  const ft = mediaFile.type;
  const isAudio = FAV_ALLOWED_MEDIA.audio.includes(ft);
  const isVideo = FAV_ALLOWED_MEDIA.video.includes(ft);
  if (!isAudio && !isVideo) return c.json({ error: "Unsupported file format." }, 422);

  // Gemini has a ~20MB inline limit; reject larger files for transcription
  if (mediaFile.size > 20 * 1024 * 1024) return c.json({ error: "File too large for transcription. Maximum is 20 MB." }, 413);

  try {
    const fileBuffer = Buffer.from(await mediaFile.arrayBuffer());
    const base64Data = fileBuffer.toString("base64");

    const res = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{
            parts: [
              { inline_data: { mime_type: ft, data: base64Data } },
              { text: "Please transcribe this audio/video recording word for word. Return only the transcription text, nothing else. If the recording is a prayer, preserve the prayerful tone." }
            ]
          }]
        }),
      }
    );

    if (res.status === 429) return c.json({ error: "Transcription is busy. Please try again in a moment." }, 429);
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.error("[Transcribe] Gemini error:", res.status, errText.substring(0, 200));
      return c.json({ error: "Something went wrong. Please try again." }, 500);
    }

    const data = (await res.json()) as any;
    const transcript = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
    if (!transcript.trim()) return c.json({ error: "No speech could be detected in this file." }, 422);

    trackEvent(u.id, "prayer_transcribed", { file_type: ft });
    return c.json({ transcript: transcript.trim() });
  } catch (err: any) {
    console.error("[Transcribe] Error:", err.message);
    return c.json({ error: "Something went wrong. Please try again." }, 500);
  }
});

// ═══════════════════════════════════════════════════════════════════
// ─── NOTIFICATIONS ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.get("/api/notifications", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const filter = c.req.query("filter") || "all";
  let q = "SELECT * FROM notifications WHERE user_id=$1 AND is_deleted=false";
  if (filter === "unread") q += " AND is_read=false";
  q += " ORDER BY created_at DESC LIMIT 100";
  const r = await pool.query(q, [u.id]);
  const unread = await pool.query("SELECT COUNT(*) as count FROM notifications WHERE user_id=$1 AND is_deleted=false AND is_read=false", [u.id]);
  return c.json({ notifications: r.rows, unreadCount: parseInt(unread.rows[0]?.count || "0") });
});

app.post("/api/notifications/read", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { notificationIds, all } = await c.req.json();
  if (all) {
    await pool.query("UPDATE notifications SET is_read=true WHERE user_id=$1 AND is_read=false", [u.id]);
  } else if (Array.isArray(notificationIds) && notificationIds.length > 0) {
    await pool.query("UPDATE notifications SET is_read=true WHERE id = ANY($1) AND user_id=$2", [notificationIds, u.id]);
  }
  return c.json({ success: true });
});

app.delete("/api/notifications/:notificationId", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  await pool.query("UPDATE notifications SET is_deleted=true WHERE id=$1 AND user_id=$2", [c.req.param("notificationId"), u.id]);
  return c.body(null, 204);
});

app.get("/api/notifications/preferences", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [u.id]);
  if (r.rows[0]) {
    const p = r.rows[0];
    return c.json({ encouragements: p.encouragements, prayers_shared: p.prayers_shared, prayer_requests: p.prayer_requests, circle_posts: p.circle_posts, post_replies: p.post_replies, post_reactions: p.post_reactions, circle_members: p.circle_members, streak_milestones: p.streak_milestones, streak_freeze: p.streak_freeze });
  }
  return c.json({ encouragements: true, prayers_shared: true, prayer_requests: true, circle_posts: true, post_replies: true, post_reactions: true, circle_members: true, streak_milestones: true, streak_freeze: true });
});

app.patch("/api/notifications/preferences", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const body = await c.req.json();
  const cols = ["encouragements","prayers_shared","prayer_requests","circle_posts","post_replies","post_reactions","circle_members","streak_milestones","streak_freeze"];
  // Ensure row exists
  await pool.query("INSERT INTO notification_preferences (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", [u.id]);
  for (const col of cols) {
    if (body[col] !== undefined) {
      await pool.query(`UPDATE notification_preferences SET ${col}=$1, updated_at=NOW() WHERE user_id=$2`, [!!body[col], u.id]);
    }
  }
  const r = await pool.query("SELECT * FROM notification_preferences WHERE user_id=$1", [u.id]);
  const p = r.rows[0];
  return c.json({ encouragements: p.encouragements, prayers_shared: p.prayers_shared, prayer_requests: p.prayer_requests, circle_posts: p.circle_posts, post_replies: p.post_replies, post_reactions: p.post_reactions, circle_members: p.circle_members, streak_milestones: p.streak_milestones, streak_freeze: p.streak_freeze });
});

// ═══════════════════════════════════════════════════════════════════
// ─── ENCOURAGEMENTS ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/encouragements", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { recipientId, message } = await c.req.json();
  if (!recipientId || !message) return c.json({ error: "recipientId and message required" }, 400);
  if (message.length > 140) return c.json({ error: "Message too long. Maximum 140 characters." }, 422);
  if (recipientId === u.id) return c.json({ error: "You can't send an encouragement to yourself." }, 422);

  // Rate limit: 1 per sender per recipient per 24h
  const recent = await pool.query("SELECT id FROM encouragements WHERE sender_user_id=$1 AND recipient_user_id=$2 AND created_at > NOW() - INTERVAL '24 hours'", [u.id, recipientId]);
  if (recent.rows.length > 0) {
    const recipient = await pool.query("SELECT name FROM users WHERE id=$1", [recipientId]);
    const recipientName = recipient.rows[0]?.name || "this person";
    return c.json({ error: `You already encouraged ${recipientName} today. Come back tomorrow.` }, 429);
  }

  const r = await pool.query("INSERT INTO encouragements (sender_user_id, sender_name, recipient_user_id, message) VALUES ($1,$2,$3,$4) RETURNING *", [u.id, u.name || "Someone", recipientId, message]);
  trackEvent(u.id, "encouragement_sent_direct", { recipient_id: recipientId });

  pushToUser(recipientId, {
    title: "Someone is praying for you 🙏",
    body: `${u.name || "Someone"} sent you an encouragement`,
    type: "encouragement",
  });

  return c.json({ encouragementId: r.rows[0].id, sentAt: r.rows[0].created_at });
});

app.get("/api/encouragements/:encouragementId", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query("SELECT * FROM encouragements WHERE id=$1 AND (sender_user_id=$2 OR recipient_user_id=$2)", [c.req.param("encouragementId"), u.id]);
  if (!r.rows[0]) return c.json({ error: "Not found" }, 404);
  return c.json({ encouragement: r.rows[0] });
});

// ═══════════════════════════════════════════════════════════════════
// ─── PRAYER SHARING ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/prayers/share", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { recipientId, prayerId, prayerTitle, prayerText } = await c.req.json();
  if (!recipientId) return c.json({ error: "recipientId required" }, 400);
  if (!prayerText && !prayerId) return c.json({ error: "prayerText or prayerId required" }, 400);

  const r = await pool.query("INSERT INTO prayer_shares (sender_user_id, sender_name, recipient_user_id, prayer_id, prayer_title, prayer_text) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *", [u.id, u.name || "Someone", recipientId, prayerId || null, prayerTitle || null, prayerText || null]);
  trackEvent(u.id, "prayer_shared", { recipient_id: recipientId });

  pushToUser(recipientId, {
    title: "A prayer was shared with you 🕯️",
    body: `${u.name || "Someone"} shared a prayer with you${prayerTitle ? ": " + prayerTitle : ""}`,
    type: "prayer_shared",
  });

  return c.json({ shareId: r.rows[0].id, sentAt: r.rows[0].created_at });
});

// ═══════════════════════════════════════════════════════════════════
// ─── SHARED PRAYERS (Favorites Sharing) ─────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/shared-prayers", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { favoriteId, recipientIds, circleId, note } = await c.req.json();
  if (!favoriteId) return c.json({ error: "favoriteId required" }, 400);
  if (note && note.length > 140) return c.json({ error: "Note too long. Maximum 140 characters." }, 422);

  // Get the favorite prayer
  const fav = await pool.query("SELECT * FROM favorites WHERE id=$1 AND user_id=$2 AND is_deleted=false", [favoriteId, u.id]);
  if (!fav.rows[0]) return c.json({ error: "Favorite not found" }, 404);
  const f = fav.rows[0];

  // Resolve recipients
  let resolvedIds: string[] = [];
  if (circleId) {
    const ci = getCircle(circleId);
    if (!ci) return c.json({ error: "Circle not found" }, 404);
    if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "You can only share prayers with people in your circles." }, 403);
    resolvedIds = ci.members.map(m => m.userId).filter(id => id !== u.id);
  } else if (Array.isArray(recipientIds) && recipientIds.length > 0) {
    if (recipientIds.length > 20) return c.json({ error: "You can share with a maximum of 20 people at once." }, 422);
    resolvedIds = recipientIds.filter((id: string) => id !== u.id);
  } else {
    return c.json({ error: "Please select at least one recipient." }, 400);
  }

  if (resolvedIds.length === 0) return c.json({ error: "Please select at least one recipient." }, 400);

  let sharedCount = 0;
  for (const rid of resolvedIds) {
    await pool.query(
      `INSERT INTO shared_prayers (sender_user_id, sender_name, recipient_user_id, favorite_id, note, prayer_text, prayer_title, source, media_url, media_type, transcript) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [u.id, u.name || "Someone", rid, favoriteId, note || null, f.prayer_text, f.title, f.source, f.media_url, f.media_type, f.transcript]
    );
    pushToUser(rid, {
      title: circleId ? `${u.name || "Someone"} shared a prayer with your circle 🙏` : "A prayer was shared with you 🕯️",
      body: note ? `${u.name || "Someone"}: ${note}` : `${u.name || "Someone"} shared a prayer with you${f.title ? ": " + f.title : ""}`,
      type: "prayer_shared",
    });
    sharedCount++;
  }

  trackEvent(u.id, "prayer_favorite_shared", { recipient_count: sharedCount, source: f.source });
  return c.json({ sharedCount, sharedAt: new Date().toISOString() });
});

app.get("/api/shared-prayers/received", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query(`
    SELECT id, sender_user_id, sender_name, recipient_user_id, favorite_id, note,
           prayer_text, prayer_title, source, media_url, media_type, transcript,
           is_saved, is_deleted, created_at
    FROM shared_prayers WHERE recipient_user_id=$1 AND is_deleted=false
    UNION ALL
    SELECT id, sender_user_id, sender_name, recipient_user_id, NULL as favorite_id, NULL as note,
           prayer_text, prayer_title, NULL as source, NULL as media_url, NULL as media_type, NULL as transcript,
           false as is_saved, false as is_deleted, created_at
    FROM prayer_shares WHERE recipient_user_id=$1
    ORDER BY created_at DESC
  `, [u.id]);
  return c.json({ sharedPrayers: r.rows });
});

app.delete("/api/shared-prayers/:sharedPrayerId", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  await pool.query("UPDATE shared_prayers SET is_deleted=true WHERE id=$1 AND recipient_user_id=$2", [c.req.param("sharedPrayerId"), u.id]);
  return c.body(null, 204);
});

// ═══════════════════════════════════════════════════════════════════
// ─── REFERRALS & REWARDS ────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

function generateReferralCode(name: string): string {
  const prefix = (name || "PRAY").replace(/[^A-Z]/gi, "").substring(0, 5).toUpperCase() || "PRAY";
  const digits = Math.floor(1000 + Math.random() * 9000);
  return `${prefix}${digits}`;
}

async function evaluateTierRewards(userId: string): Promise<void> {
  const confirmed = await pool.query("SELECT COUNT(*) as count FROM referrals WHERE referrer_user_id=$1 AND status='confirmed'", [userId]);
  const count = parseInt(confirmed.rows[0]?.count || "0");
  const tiers = [
    { tier: 1, threshold: 1, type: "7_days_free", days: 7 },
    { tier: 2, threshold: 5, type: "1_month_free", days: 30 },
    { tier: 3, threshold: 10, type: "50_off_lifetime", days: 0 },
    { tier: 4, threshold: 20, type: "free_lifetime", days: 0 },
  ];

  for (const t of tiers) {
    if (count >= t.threshold) {
      // Check if already granted
      const existing = await pool.query("SELECT id FROM referral_rewards WHERE user_id=$1 AND tier=$2", [userId, t.tier]);
      if (existing.rows.length > 0) continue;

      // Grant reward
      await pool.query("INSERT INTO referral_rewards (user_id, tier, reward_type) VALUES ($1,$2,$3)", [userId, t.tier, t.type]);

      // Grant via RevenueCat API (tiers 1, 2, 4)
      if (REVENUECAT_SECRET_KEY && (t.tier === 1 || t.tier === 2 || t.tier === 4)) {
        try {
          const duration = t.tier === 4 ? "lifetime" : t.tier === 2 ? "monthly" : "weekly";
          await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(userId)}/entitlements/premium/promotional`, {
            method: "POST",
            headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" },
            body: JSON.stringify({ duration }),
          });
          console.log(`[Referral] Granted tier ${t.tier} (${t.type}) to ${userId.substring(0, 8)}`);
        } catch (err: any) { console.error(`[Referral] RC grant error tier ${t.tier}:`, err.message); }
      }

      trackEvent(userId, "referral_reward_earned", { tier: t.tier, reward: t.type, referral_count: count });
    }
  }
}

app.get("/api/referrals/me", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);

  const codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]);
  const code = codeResult.rows[0]?.code || null;

  const referrals = await pool.query("SELECT id, referred_user_id, status, created_at, confirmed_at FROM referrals WHERE referrer_user_id=$1 ORDER BY created_at DESC", [u.id]);

  // Enrich with names
  const enriched = [];
  for (const ref of referrals.rows) {
    let name = null;
    if (ref.referred_user_id) {
      const usr = await pool.query("SELECT name FROM users WHERE id=$1", [ref.referred_user_id]);
      name = usr.rows[0]?.name || null;
    }
    enriched.push({ ...ref, referred_name: name });
  }

  const confirmedCount = referrals.rows.filter((r: any) => r.status === "confirmed").length;
  const rewards = await pool.query("SELECT tier, reward_type, granted_at FROM referral_rewards WHERE user_id=$1 ORDER BY tier", [u.id]);

  let currentTier = 0;
  if (confirmedCount >= 20) currentTier = 4;
  else if (confirmedCount >= 10) currentTier = 3;
  else if (confirmedCount >= 5) currentTier = 2;
  else if (confirmedCount >= 1) currentTier = 1;

  const nextTierThresholds = [1, 5, 10, 20];
  const nextTierAt = nextTierThresholds.find(t => t > confirmedCount) || null;

  return c.json({
    code,
    link: code ? `https://pramen.app/join/${code}` : null,
    referrals: enriched,
    confirmedCount,
    currentTier,
    nextTierAt,
    rewards: rewards.rows,
  });
});

app.post("/api/referrals/generate", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);

  // Return existing code if already generated
  const existing = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]);
  if (existing.rows[0]) return c.json({ code: existing.rows[0].code, link: `https://pramen.app/join/${existing.rows[0].code}` });

  // Generate unique code with retry
  let code = "";
  for (let attempt = 0; attempt < 10; attempt++) {
    code = generateReferralCode(u.name || "PRAY");
    const collision = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [code]);
    if (collision.rows.length === 0) break;
  }

  await pool.query("INSERT INTO referral_codes (user_id, code) VALUES ($1, $2)", [u.id, code]);
  trackEvent(u.id, "referral_code_generated", { code });
  return c.json({ code, link: `https://pramen.app/join/${code}` });
});

app.post("/api/referrals/track", async (c) => {
  // No auth required — called during onboarding
  const { referralCode, newUserId, newUserEmail } = await c.req.json();
  if (!referralCode) return c.json({ error: "referralCode required" }, 400);

  // Find referrer
  const referrer = await pool.query("SELECT user_id FROM referral_codes WHERE code=$1", [referralCode.toUpperCase()]);
  if (!referrer.rows[0]) return c.json({ error: "Invalid referral code" }, 404);
  const referrerId = referrer.rows[0].user_id;

  // Self-referral check
  if (newUserId && newUserId === referrerId) return c.json({ error: "You cannot refer yourself." }, 422);

  // Check if already tracked
  if (newUserId) {
    const dup = await pool.query("SELECT id FROM referrals WHERE referrer_user_id=$1 AND referred_user_id=$2", [referrerId, newUserId]);
    if (dup.rows.length > 0) return c.json({ error: "This referral has already been tracked." }, 409);
  }

  const r = await pool.query("INSERT INTO referrals (referrer_user_id, referred_user_id, referred_email) VALUES ($1,$2,$3) RETURNING id", [referrerId, newUserId || null, newUserEmail || null]);
  trackEvent(referrerId, "referral_tracked", { referred_user_id: newUserId, code: referralCode });

  return c.json({ referralId: r.rows[0].id, discountApplied: true });
});

app.post("/api/referrals/confirm", async (c) => {
  // Internal — called from webhook logic
  const sec = c.req.header("X-Admin-Secret");
  if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const { referredUserId } = await c.req.json();
  if (!referredUserId) return c.json({ error: "referredUserId required" }, 400);

  const ref = await pool.query("UPDATE referrals SET status='confirmed', confirmed_at=NOW() WHERE referred_user_id=$1 AND status='pending' RETURNING referrer_user_id", [referredUserId]);
  if (ref.rows[0]) {
    await evaluateTierRewards(ref.rows[0].referrer_user_id);
    pushToUser(ref.rows[0].referrer_user_id, {
      title: "🎉 Referral confirmed!",
      body: "Someone you invited just subscribed. Check your rewards!",
      type: "referral_confirmed",
    });
  }
  return c.json({ confirmed: ref.rows.length });
});

app.post("/api/referrals/reverse", async (c) => {
  const sec = c.req.header("X-Admin-Secret");
  if (sec !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  const { referredUserId } = await c.req.json();
  if (!referredUserId) return c.json({ error: "referredUserId required" }, 400);

  await pool.query("UPDATE referrals SET status='reversed', reversed_at=NOW() WHERE referred_user_id=$1 AND status='confirmed'", [referredUserId]);
  return c.json({ reversed: true });
});

app.get("/api/referrals/circle/:code", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const code = c.req.param("code").toUpperCase();
  const ci = getCircle(code);
  if (!ci) return c.json({ count: 0 });
  const memberIds = ci.members.map(m => m.userId);
  const result = await pool.query("SELECT COUNT(*) as count FROM referrals WHERE referrer_user_id=$1 AND referred_user_id = ANY($2) AND status='confirmed'", [u.id, memberIds]);
  return c.json({ count: parseInt(result.rows[0]?.count || "0") });
});

app.post("/api/referrals/invite-batch", async (c) => {
  const u = await requireAuth(c);
  if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { invites } = await c.req.json();
  if (!Array.isArray(invites) || invites.length === 0) return c.json({ error: "Please add at least one valid email address." }, 400);
  if (invites.length > 5) return c.json({ error: "Maximum 5 invites at a time." }, 422);

  // Rate limit: 5 per user per 24h
  const recent = await pool.query("SELECT COUNT(*) as count FROM invite_emails WHERE referrer_user_id=$1 AND created_at > NOW() - INTERVAL '24 hours'", [u.id]);
  if (parseInt(recent.rows[0]?.count || "0") >= 5) return c.json({ error: "You've reached the invite limit for today. Try again tomorrow." }, 429);

  // Get or generate referral code
  let codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]);
  if (!codeResult.rows[0]) {
    const code = generateReferralCode(u.name || "PRAY");
    await pool.query("INSERT INTO referral_codes (user_id, code) VALUES ($1, $2) ON CONFLICT DO NOTHING", [u.id, code]);
    codeResult = await pool.query("SELECT code FROM referral_codes WHERE user_id=$1", [u.id]);
  }
  const referralCode = codeResult.rows[0]?.code || "";
  const referralLink = `https://pramen.app/join/${referralCode}`;
  const referrerName = u.name || "A friend";

  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  let sent = 0;
  const failed: { email: string; reason: string }[] = [];
  const alreadyMembers: { email: string }[] = [];

  for (const inv of invites) {
    const name = (inv.name || "").trim();
    const email = (inv.email || "").trim().toLowerCase();
    if (!name || name.length < 2 || !emailRegex.test(email)) { failed.push({ email: email || "invalid", reason: "Invalid name or email" }); continue; }

    // Check if already a member
    const existing = await pool.query("SELECT id FROM users WHERE email=$1", [email]);
    if (existing.rows.length > 0) { alreadyMembers.push({ email }); continue; }

    // Send email via Resend
    if (RESEND_API_KEY) {
      try {
        const htmlBody = `<div style="font-family: -apple-system, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px 16px; color: #2C1810;">
  <p style="font-size: 18px; font-weight: 600; color: #C0735A;">prAmen</p>
  <p>Hi ${name},</p>
  <p>${referrerName} wants to pray alongside you.</p>
  <p>They invited you to join <strong>prAmen</strong> — a daily prayer app that helps Christians build a simple, meaningful prayer habit.</p>
  <p>As their guest, you get <strong>50% off your first month</strong>.</p>
  <p style="text-align: center; margin: 28px 0;">
    <a href="${referralLink}" style="display: inline-block; background: #C0735A; color: #fff; text-decoration: none; padding: 14px 32px; border-radius: 24px; font-weight: 600; font-size: 16px;">Join prAmen</a>
  </p>
  <p style="color: #9E7E6E; font-size: 14px;">The offer is waiting for you. No pressure.</p>
  <p style="color: #9E7E6E; font-size: 14px;">— The prAmen team</p>
  <hr style="border: none; border-top: 1px solid #E0D4C4; margin: 24px 0;">
  <p style="color: #9E7E6E; font-size: 11px;">You received this because ${referrerName} invited you. <a href="https://pramen.app" style="color: #9E7E6E;">Unsubscribe</a></p>
</div>`;

        const res = await fetch("https://api.resend.com/emails", {
          method: "POST",
          headers: { Authorization: `Bearer ${RESEND_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({
            from: RESEND_FROM_EMAIL,
            to: email,
            subject: `${referrerName} is praying for you 🙏 — Join them on prAmen`,
            html: htmlBody,
            text: `Hi ${name},\n\n${referrerName} wants to pray alongside you. They invited you to join prAmen — a daily prayer app that helps Christians build a simple, meaningful prayer habit.\n\nAs their guest, you get 50% off your first month.\n\nJoin here: ${referralLink}\n\nThe offer is waiting for you. No pressure.\n\n— The prAmen team`,
          }),
        });

        if (res.ok) {
          await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'sent',$4)", [u.id, name, email, referralCode]);
          // Create pending referral
          await pool.query("INSERT INTO referrals (referrer_user_id, referred_email, status) VALUES ($1,$2,'pending') ON CONFLICT DO NOTHING", [u.id, email]);
          sent++;
        } else {
          const errText = await res.text().catch(() => "");
          console.error("[Invite] Resend error:", res.status, errText.substring(0, 200));
          failed.push({ email, reason: "Delivery failed" });
          await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'failed',$4)", [u.id, name, email, referralCode]);
        }
      } catch (err: any) {
        console.error("[Invite] Send error:", err.message);
        failed.push({ email, reason: "Delivery failed" });
      }
    } else {
      // No email provider — store invite but don't send
      await pool.query("INSERT INTO invite_emails (referrer_user_id, friend_name, friend_email, status, referral_code) VALUES ($1,$2,$3,'stored',$4)", [u.id, name, email, referralCode]);
      await pool.query("INSERT INTO referrals (referrer_user_id, referred_email, status) VALUES ($1,$2,'pending') ON CONFLICT DO NOTHING", [u.id, email]);
      sent++;
    }
  }

  trackEvent(u.id, "onboarding_invites_sent", { sent, failed: failed.length, already_members: alreadyMembers.length });
  return c.json({ sent, failed, alreadyMembers });
});

// ═══════════════════════════════════════════════════════════════════
// ─── FIND MY CHURCH ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

async function enrichChurch(placeId: string, name: string, address: string): Promise<void> {
  if (!GEMINI_API_KEY) { await pool.query("UPDATE church_profiles SET enrichment_status='unavailable' WHERE place_id=$1", [placeId]); return; }
  try {
    // Query Wikipedia for context
    let wikiText = "";
    try {
      const wRes = await fetch(`https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(name)}`);
      if (wRes.ok) { const wd = (await wRes.json()) as any; wikiText = wd.extract || ""; }
    } catch {}
    if (!wikiText) {
      try {
        const wRes2 = await fetch(`https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(name + " church")}`);
        if (wRes2.ok) { const wd2 = (await wRes2.json()) as any; wikiText = wd2.extract || ""; }
      } catch {}
    }

    const prompt = wikiText
      ? `Given this Wikipedia text about "${name}" at "${address}": "${wikiText.substring(0, 1500)}"\n\nExtract JSON: {"year_founded":"year or century or null","architectural_style":"style or null","patron_saint":"name or null","diocese":"name or null","description":"2-3 sentence historical description","notable_features":["feature1","feature2"]}`
      : `For the church "${name}" at "${address}", provide what you know. Return JSON: {"year_founded":"year or century or null","architectural_style":"style or null","patron_saint":"name or null","diocese":"name or null","description":"2-3 sentence description or null","notable_features":[]}. If you don't know, use null for that field.`;

    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=${GEMINI_API_KEY}`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ system_instruction: { parts: [{ text: "You enrich church profiles. Respond ONLY with valid JSON, no markdown, no backticks." }] }, contents: [{ role: "user", parts: [{ text: prompt }] }] }),
    });
    if (!res.ok) { await pool.query("UPDATE church_profiles SET enrichment_status='unavailable' WHERE place_id=$1", [placeId]); return; }
    const data = (await res.json()) as any;
    const text = data.candidates?.[0]?.content?.parts?.[0]?.text || "";
    const parsed = JSON.parse(text.replace(/```json|```/g, "").trim());
    const status = parsed.description ? "enriched" : "partial";
    await pool.query(`UPDATE church_profiles SET enrichment_status=$1, year_founded=$2, architectural_style=$3, patron_saint=$4, diocese=$5, description=$6, notable_features=$7, updated_at=NOW() WHERE place_id=$8`,
      [status, parsed.year_founded || null, parsed.architectural_style || null, parsed.patron_saint || null, parsed.diocese || null, parsed.description || null, JSON.stringify(parsed.notable_features || []), placeId]);
    console.log(`[Church] Enriched ${name}: ${status}`);
  } catch (err: any) {
    console.error("[Church] Enrichment error:", err.message);
    await pool.query("UPDATE church_profiles SET enrichment_status='unavailable' WHERE place_id=$1", [placeId]);
  }
}

app.get("/api/churches/search", async (c) => {
  if (!GOOGLE_PLACES_API_KEY) return c.json({ error: "Church search not configured" }, 500);
  const lat = c.req.query("lat"); const lng = c.req.query("lng");
  if (!lat || !lng) return c.json({ error: "lat and lng required" }, 400);
  const radius = c.req.query("radius") || "5000";
  const keyword = c.req.query("keyword") || "church";
  const denomination = c.req.query("denomination") || "";
  const searchKeyword = denomination ? `${denomination} church` : keyword;

  try {
    const url = `https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=${lat},${lng}&radius=${radius}&type=church&keyword=${encodeURIComponent(searchKeyword)}&key=${GOOGLE_PLACES_API_KEY}`;
    const res = await fetch(url);
    if (!res.ok) return c.json({ error: "Search failed" }, 500);
    const data = (await res.json()) as any;
    const results = (data.results || []).map((p: any) => ({
      placeId: p.place_id, name: p.name, address: p.vicinity || p.formatted_address || "",
      lat: p.geometry?.location?.lat, lng: p.geometry?.location?.lng,
      rating: p.rating || null, ratingCount: p.user_ratings_total || 0,
      openNow: p.opening_hours?.open_now ?? null,
      photoRef: p.photos?.[0]?.photo_reference || null,
    }));

    // Apply filters
    let filtered = results;
    const minRating = c.req.query("minRating"); if (minRating) filtered = filtered.filter((r: any) => r.rating >= parseFloat(minRating));
    const openNow = c.req.query("openNow"); if (openNow === "true") filtered = filtered.filter((r: any) => r.openNow === true);

    return c.json({ churches: filtered });
  } catch (err: any) { return c.json({ error: "Something went wrong. Please try again." }, 500); }
});

app.get("/api/churches/:placeId", async (c) => {
  const placeId = c.req.param("placeId");
  // Check cache
  const cached = await pool.query("SELECT * FROM church_profiles WHERE place_id=$1", [placeId]);
  if (cached.rows[0] && cached.rows[0].enrichment_status !== "pending") {
    return c.json({ church: cached.rows[0], enrichmentStatus: cached.rows[0].enrichment_status });
  }

  if (!GOOGLE_PLACES_API_KEY) return c.json({ error: "Not configured" }, 500);

  try {
    const url = `https://maps.googleapis.com/maps/api/place/details/json?place_id=${placeId}&fields=name,formatted_address,geometry,formatted_phone_number,website,rating,user_ratings_total,opening_hours,photos,types&key=${GOOGLE_PLACES_API_KEY}`;
    const res = await fetch(url);
    if (!res.ok) return c.json({ error: "Church not found." }, 404);
    const data = (await res.json()) as any;
    const r = data.result;
    if (!r) return c.json({ error: "Church not found." }, 404);

    const photos = (r.photos || []).slice(0, 5).map((p: any) => `https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photo_reference=${p.photo_reference}&key=${GOOGLE_PLACES_API_KEY}`);

    // Upsert into cache
    await pool.query(`INSERT INTO church_profiles (place_id, name, address, lat, lng, phone, website, rating, rating_count, opening_hours, photos, enrichment_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending') ON CONFLICT (place_id) DO UPDATE SET name=$2, address=$3, phone=$6, website=$7, rating=$8, rating_count=$9, opening_hours=$10, photos=$11, updated_at=NOW()`,
      [placeId, r.name, r.formatted_address, r.geometry?.location?.lat, r.geometry?.location?.lng, r.formatted_phone_number || null, r.website || null, r.rating || null, r.user_ratings_total || 0, JSON.stringify(r.opening_hours || {}), JSON.stringify(photos)]);

    // Trigger async enrichment
    enrichChurch(placeId, r.name, r.formatted_address).catch(() => {});

    const profile = (await pool.query("SELECT * FROM church_profiles WHERE place_id=$1", [placeId])).rows[0];
    return c.json({ church: profile, enrichmentStatus: "pending" });
  } catch (err: any) { return c.json({ error: "Something went wrong. Please try again." }, 500); }
});

app.get("/api/churches/:placeId/enrichment", async (c) => {
  const r = await pool.query("SELECT enrichment_status, year_founded, architectural_style, patron_saint, diocese, description, notable_features FROM church_profiles WHERE place_id=$1", [c.req.param("placeId")]);
  if (!r.rows[0]) return c.json({ status: "unavailable" });
  return c.json({ status: r.rows[0].enrichment_status, enrichedData: r.rows[0].enrichment_status !== "pending" ? r.rows[0] : null });
});

app.get("/api/churches/saved", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const r = await pool.query("SELECT * FROM saved_churches WHERE user_id=$1 AND is_deleted=false ORDER BY created_at DESC", [u.id]);
  return c.json({ savedChurches: r.rows });
});

app.post("/api/churches/saved", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const ct = c.req.header("content-type") || "";
  let placeId: string, churchName: string, address: string, lat: number, lng: number, tags: string[], review: string | null, notes: string | null, rating: number | null;
  let body: any = {};
  if (ct.includes("application/json")) {
    const j = await c.req.json() as any;
    placeId = j.placeId; churchName = j.churchName; address = j.address;
    lat = j.lat || 0; lng = j.lng || 0;
    tags = Array.isArray(j.tags) ? j.tags : [];
    review = j.review || null; notes = j.notes || null;
    rating = j.rating != null ? parseInt(j.rating) : null;
  } else {
    body = await c.req.parseBody();
    placeId = body.placeId as string; churchName = body.churchName as string; address = body.address as string;
    lat = parseFloat(body.lat as string || "0"); lng = parseFloat(body.lng as string || "0");
    tags = body.tags ? JSON.parse(body.tags as string) : [];
    review = (body.review as string) || null; notes = (body.notes as string) || null;
    rating = body.rating ? parseInt(body.rating as string) : null;
  }
  if (!placeId) return c.json({ error: "placeId required" }, 400);

  // Handle photos
  let photoUrls: string[] = [];
  for (let i = 0; i < 5; i++) {
    const photo = body[`photo${i}`] as File | undefined;
    if (photo && photo.size > 0) {
      if (photo.size > 10 * 1024 * 1024) return c.json({ error: "Photo too large. Maximum size is 10 MB." }, 413);
      try {
        const ext = photo.name.split(".").pop() || "jpg";
        const key = `churches/${Date.now()}-${randomUUID().substring(0, 8)}.${ext}`;
        if (s3) { await s3.send(new PutObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key, Body: Buffer.from(await photo.arrayBuffer()), ContentType: photo.type })); photoUrls.push(`${R2_PUBLIC_URL}/${key}`); }
      } catch {}
    }
  }

  const r = await pool.query(`INSERT INTO saved_churches (user_id, place_id, church_name, address, lat, lng, tags, review, notes, rating, photos) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
    [u.id, placeId, churchName, address, lat, lng, tags, review, notes, rating, JSON.stringify(photoUrls)]);
  trackEvent(u.id, "church_saved", { place_id: placeId });
  return c.json({ savedChurch: r.rows[0] }, 201);
});

app.patch("/api/churches/saved/:savedId", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const body = await c.req.json();
  const sets: string[] = []; const vals: any[] = []; let idx = 1;
  if (body.tags !== undefined) { sets.push(`tags=$${idx++}`); vals.push(body.tags); }
  if (body.review !== undefined) { sets.push(`review=$${idx++}`); vals.push(body.review); }
  if (body.notes !== undefined) { sets.push(`notes=$${idx++}`); vals.push(body.notes); }
  if (body.rating !== undefined) { sets.push(`rating=$${idx++}`); vals.push(body.rating); }
  if (sets.length === 0) return c.json({ error: "Nothing to update" }, 400);
  sets.push(`updated_at=NOW()`);
  vals.push(c.req.param("savedId"), u.id);
  const r = await pool.query(`UPDATE saved_churches SET ${sets.join(",")} WHERE id=$${idx++} AND user_id=$${idx} AND is_deleted=false RETURNING *`, vals);
  if (!r.rows[0]) return c.json({ error: "Not found" }, 404);
  return c.json({ savedChurch: r.rows[0] });
});

app.delete("/api/churches/saved/:savedId", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  await pool.query("UPDATE saved_churches SET is_deleted=true, updated_at=NOW() WHERE id=$1 AND user_id=$2", [c.req.param("savedId"), u.id]);
  return c.body(null, 204);
});

app.post("/api/churches/share", async (c) => {
  const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401);
  const { savedChurchId, circleCode, note } = await c.req.json();
  if (!savedChurchId || !circleCode) return c.json({ error: "savedChurchId and circleCode required" }, 400);
  if (note && note.length > 140) return c.json({ error: "Note too long. Maximum 140 characters." }, 422);

  const saved = await pool.query("SELECT * FROM saved_churches WHERE id=$1 AND user_id=$2 AND is_deleted=false", [savedChurchId, u.id]);
  if (!saved.rows[0]) return c.json({ error: "Saved church not found" }, 404);
  const ci = getCircle(circleCode); if (!ci) return c.json({ error: "Circle not found" }, 404);
  if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "You can only share churches with your circles." }, 403);

  const r = await pool.query("INSERT INTO church_shares (sender_user_id, sender_name, saved_church_id, circle_code, note) VALUES ($1,$2,$3,$4,$5) RETURNING *",
    [u.id, u.name || "Someone", savedChurchId, circleCode, note || null]);

  pushToCircleMembers(ci, u.id, {
    title: `${u.name || "Someone"} shared a church with ${ci.name} ⛪`,
    body: saved.rows[0].church_name + (note ? ` — "${note}"` : ""),
    type: "church_shared", circleCode, circleName: ci.name,
  });

  trackEvent(u.id, "church_shared", { place_id: saved.rows[0].place_id, circle_code: circleCode });
  return c.json({ shareId: r.rows[0].id, sharedAt: r.rows[0].created_at });
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLE POSTS ───────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
async function enrichPosts(posts: any[], currentUserId: string) { if (!posts.length) return []; const ids = posts.map(p => p.id); const rx = await pool.query("SELECT post_id, emoji, COUNT(*) as count FROM post_reactions WHERE post_id = ANY($1) GROUP BY post_id, emoji", [ids]); const urx = await pool.query("SELECT post_id, emoji FROM post_reactions WHERE post_id = ANY($1) AND user_id = $2", [ids, currentUserId]); const rm: Record<string, { emoji: string; count: number; reactedByCurrentUser: boolean }[]> = {}; for (const r of rx.rows) { if (!rm[r.post_id]) rm[r.post_id] = []; rm[r.post_id].push({ emoji: r.emoji, count: parseInt(r.count), reactedByCurrentUser: false }); } for (const r of urx.rows) { const arr = rm[r.post_id]; if (arr) { const x = arr.find(a => a.emoji === r.emoji); if (x) x.reactedByCurrentUser = true; } } const rc = await pool.query("SELECT post_id, COUNT(*) as count FROM post_replies WHERE post_id = ANY($1) AND is_deleted=false GROUP BY post_id", [ids]); const rcm: Record<string, number> = {}; for (const r of rc.rows) rcm[r.post_id] = parseInt(r.count); return posts.map(p => ({ ...p, isAdmin: isAdmin(p.author_user_id), reactions: rm[p.id] || [], replyCount: rcm[p.id] || 0 })); }

app.get("/api/circles/:code/posts", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); if (!isMemberOfCircle(u.id, ci, u.device_user_id)) return c.json({ error: "You don't have permission to do this." }, 403); const cursor = c.req.query("cursor"); const lim = 20; let q = "SELECT * FROM circle_posts WHERE circle_code=$1 AND status='published'"; const p: any[] = [c.req.param("code").toUpperCase()]; if (cursor) { q += " AND published_at < $2"; p.push(cursor); } q += ` ORDER BY published_at DESC LIMIT $${p.length + 1}`; p.push(lim + 1); const r = await pool.query(q, p); const posts = r.rows.slice(0, lim); const nextCursor = r.rows.length > lim ? posts[posts.length - 1].published_at.toISOString() : null; return c.json({ posts: await enrichPosts(posts, u.id), nextCursor, hasPostingRights: canPostInCircle(u.id, ci, u.device_user_id) }); });

app.post("/api/circles/:code/posts", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); if (!canPostInCircle(u.id, ci, u.device_user_id)) return c.json({ error: "You don't have permission to do this." }, 403); const body = await c.req.parseBody(); const content = (body.content as string) || null; const scheduledAt = (body.scheduledAt as string) || null; const mediaFile = body.mediaFile as File | undefined; let mediaUrl: string | null = null, mediaType: string | null = null, mediaFilename: string | null = null, mediaSizeBytes: number | null = null; if (mediaFile && mediaFile.size > 0) { if (mediaFile.size > MAX_FILE_SIZE) return c.json({ error: "File too large. Maximum size is 50 MB." }, 413); const ft = mediaFile.type; if (ALLOWED_MEDIA.image.includes(ft)) mediaType = "image"; else if (ALLOWED_MEDIA.video.includes(ft)) mediaType = "video"; else if (ALLOWED_MEDIA.audio.includes(ft)) mediaType = "audio"; else return c.json({ error: "Unsupported file format." }, 422); try { mediaUrl = await uploadMedia(await mediaFile.arrayBuffer(), mediaFile.name, ft); mediaFilename = mediaFile.name; mediaSizeBytes = mediaFile.size; } catch (err: any) { return c.json({ error: "Something went wrong. Please try again." }, 500); } } if (!content && !mediaUrl) return c.json({ error: "Post must have text or media." }, 422); const taggedRaw = (body.taggedUserIds as string) || "[]"; let taggedUserIds: string[] = []; try { taggedUserIds = JSON.parse(taggedRaw); } catch {} const status = scheduledAt ? "scheduled" : "published"; const publishedAt = scheduledAt ? null : new Date().toISOString(); if (scheduledAt && new Date(scheduledAt).getTime() < Date.now() + 600000) return c.json({ error: "Schedule time must be at least 10 minutes from now." }, 422); const r = await pool.query(`INSERT INTO circle_posts (circle_code,author_user_id,author_name,content,media_type,media_url,media_filename,media_size_bytes,status,scheduled_at,published_at,tagged_user_ids) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`, [code, u.id, u.name||"", content, mediaType, mediaUrl, mediaFilename, mediaSizeBytes, status, scheduledAt, publishedAt, taggedUserIds]); trackEvent(u.id, "post_created", { circle_code: code, media_type: mediaType, status, tagged_count: taggedUserIds.length }); if (status === "published") { const isEveryoneTag = taggedUserIds.includes("everyone"); if (isEveryoneTag) { const resolvedTags = ci.members.map(m => m.userId).filter(id => id !== u.id); taggedUserIds = ["everyone"]; for (const mid of resolvedTags) { pushToUser(mid, { title: "📢 " + (u.name || "Someone") + " tagged everyone in " + ci.name, body: content ? (content.length > 60 ? content.substring(0, 60) + "..." : content) : "New announcement", type: "post_mention", circleCode: code, circleName: ci.name }); } } else { pushToCircleMembers(ci, u.id, { title: "📝 " + (u.name || "Someone") + " posted in " + ci.name, body: content ? (content.length > 60 ? content.substring(0, 60) + "..." : content) : "New " + (mediaType || "text") + " post", type: "new_post", circleCode: code, circleName: ci.name }); for (const taggedId of taggedUserIds) { if (taggedId !== u.id) pushToUser(taggedId, { title: "📌 " + (u.name || "Someone") + " mentioned you", body: content ? (content.length > 60 ? content.substring(0, 60) + "..." : content) : "You were mentioned in a post", type: "post_mention", circleCode: code, circleName: ci.name }); } } } return c.json({ post: { ...r.rows[0], isAdmin: isAdmin(u.id), reactions: [], replyCount: 0 } }, 201); });

app.delete("/api/posts/:postId", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const p = await pool.query("SELECT * FROM circle_posts WHERE id=$1 AND status != 'deleted'", [c.req.param("postId")]); if (!p.rows[0]) return c.json({ error: "Not found" }, 404); if (p.rows[0].author_user_id !== u.id && !isAdmin(u.id)) return c.json({ error: "You don't have permission to do this." }, 403); await pool.query("UPDATE circle_posts SET status='deleted',updated_at=NOW() WHERE id=$1", [c.req.param("postId")]); return c.body(null, 204); });

app.post("/api/posts/:postId/reactions", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const { emoji } = await c.req.json(); if (!emoji || !["🙏","❤️","🔥","👏"].includes(emoji)) return c.json({ error: "Invalid emoji" }, 422); const pid = c.req.param("postId"); const p = await pool.query("SELECT * FROM circle_posts WHERE id=$1 AND status='published'", [pid]); if (!p.rows[0]) return c.json({ error: "Not found" }, 404); try { await pool.query("INSERT INTO post_reactions (post_id,user_id,emoji) VALUES ($1,$2,$3)", [pid, u.id, emoji]); if (p.rows[0].author_user_id !== u.id) pushToUser(p.rows[0].author_user_id, { title: (u.name || "Someone") + " reacted " + emoji, body: "on your post", type: "post_reaction", circleCode: p.rows[0].circle_code }); } catch { await pool.query("DELETE FROM post_reactions WHERE post_id=$1 AND user_id=$2 AND emoji=$3", [pid, u.id, emoji]); } const rxr = await pool.query("SELECT emoji, COUNT(*) as count FROM post_reactions WHERE post_id=$1 GROUP BY emoji", [pid]); const urxr = await pool.query("SELECT emoji FROM post_reactions WHERE post_id=$1 AND user_id=$2", [pid, u.id]); const us = new Set(urxr.rows.map((r: any) => r.emoji)); return c.json({ reactions: rxr.rows.map((r: any) => ({ emoji: r.emoji, count: parseInt(r.count), reactedByCurrentUser: us.has(r.emoji) })) }); });

app.get("/api/posts/:postId/replies", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const pid = c.req.param("postId"); const cursor = c.req.query("cursor"); const lim = 20; let q = "SELECT * FROM post_replies WHERE post_id=$1 AND is_deleted=false"; const p: any[] = [pid]; if (cursor) { q += " AND created_at > $2"; p.push(cursor); } q += ` ORDER BY created_at ASC LIMIT $${p.length + 1}`; p.push(lim + 1); const r = await pool.query(q, p); const replies = r.rows.slice(0, lim); const nextCursor = r.rows.length > lim ? replies[replies.length - 1].created_at.toISOString() : null; return c.json({ replies, nextCursor }); });

app.post("/api/posts/:postId/replies", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const { content } = await c.req.json(); if (!content?.trim()) return c.json({ error: "Reply cannot be empty." }, 422); if (content.length > 500) return c.json({ error: "Reply is too long. Maximum is 500 characters." }, 422); const pid = c.req.param("postId"); const p = await pool.query("SELECT * FROM circle_posts WHERE id=$1 AND status='published'", [pid]); if (!p.rows[0]) return c.json({ error: "Not found" }, 404); const r = await pool.query("INSERT INTO post_replies (post_id,author_user_id,author_name,content) VALUES ($1,$2,$3,$4) RETURNING *", [pid, u.id, u.name||"", content.trim()]); if (p.rows[0].author_user_id !== u.id) pushToUser(p.rows[0].author_user_id, { title: "💬 " + (u.name || "Someone") + " replied to your post", body: content.length > 60 ? content.substring(0, 60) + "..." : content, type: "post_reply", circleCode: p.rows[0].circle_code }); return c.json({ reply: r.rows[0] }, 201); });

app.delete("/api/replies/:replyId", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const r = await pool.query("SELECT r.*, p.author_user_id as post_author_id FROM post_replies r JOIN circle_posts p ON r.post_id=p.id WHERE r.id=$1 AND r.is_deleted=false", [c.req.param("replyId")]); if (!r.rows[0]) return c.json({ error: "Not found" }, 404); if (r.rows[0].author_user_id !== u.id && r.rows[0].post_author_id !== u.id && !isAdmin(u.id)) return c.json({ error: "You don't have permission to do this." }, 403); await pool.query("UPDATE post_replies SET is_deleted=true WHERE id=$1", [c.req.param("replyId")]); return c.body(null, 204); });

app.get("/api/circles/:code/posts/scheduled", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const code = c.req.param("code").toUpperCase(); const q = isAdmin(u.id) ? "SELECT * FROM circle_posts WHERE circle_code=$1 AND status='scheduled' ORDER BY scheduled_at ASC" : "SELECT * FROM circle_posts WHERE circle_code=$1 AND status='scheduled' AND author_user_id=$2 ORDER BY scheduled_at ASC"; const p = isAdmin(u.id) ? [code] : [code, u.id]; return c.json({ scheduledPosts: (await pool.query(q, p)).rows }); });

app.patch("/api/posts/:postId/schedule", async (c) => { const u = await requireAuth(c); if (!u) return c.json({ error: "Session expired. Please log in again." }, 401); const p = await pool.query("SELECT * FROM circle_posts WHERE id=$1 AND status='scheduled'", [c.req.param("postId")]); if (!p.rows[0]) return c.json({ error: "Not found" }, 404); if (p.rows[0].author_user_id !== u.id && !isAdmin(u.id)) return c.json({ error: "You don't have permission to do this." }, 403); const { scheduledAt } = await c.req.json(); if (scheduledAt === null || scheduledAt === undefined) { await pool.query("UPDATE circle_posts SET status='deleted',updated_at=NOW() WHERE id=$1", [c.req.param("postId")]); return c.json({ cancelled: true }); } if (new Date(scheduledAt).getTime() < Date.now() + 600000) return c.json({ error: "Schedule time must be at least 10 minutes from now." }, 422); const r = await pool.query("UPDATE circle_posts SET scheduled_at=$1,updated_at=NOW() WHERE id=$2 RETURNING *", [scheduledAt, c.req.param("postId")]); return c.json({ post: r.rows[0] }); });

// ─── Scheduled Post Publisher ────────────────────────────────────────
async function publishScheduledPosts(): Promise<void> { try { const r = await pool.query("UPDATE circle_posts SET status='published',published_at=NOW(),updated_at=NOW() WHERE status='scheduled' AND scheduled_at <= NOW() RETURNING *"); for (const post of r.rows) { const ci = getCircle(post.circle_code); if (ci) pushToCircleMembers(ci, post.author_user_id, { title: "📝 " + (post.author_name || "Someone") + " posted in " + ci.name, body: post.content ? (post.content.length > 60 ? post.content.substring(0, 60) + "..." : post.content) : "New post", type: "new_post", circleCode: post.circle_code, circleName: ci.name }); console.log(`[Scheduler] Published ${post.id}`); } } catch (err: any) { console.error("[Scheduler]", err.message); } }

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
app.get("/api/dashboard/revenuecat", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  if (!REVENUECAT_SECRET_KEY) return c.json({ error: "REVENUECAT_SECRET_KEY not set" }, 500);
  try {
    const usersResult = await pool.query("SELECT id, device_user_id, subscription_status, name, email FROM users ORDER BY created_at DESC LIMIT 100");
    const subscribers: any[] = []; let totalRevenue = 0; let activeCount = 0; let trialCount = 0; let mrr = 0;
    for (const user of usersResult.rows) { const ids = [user.id, user.device_user_id].filter(Boolean); for (const uid of ids) { try { const rcRes = await fetch(`https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(uid)}`, { headers: { Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`, "Content-Type": "application/json" } }); if (!rcRes.ok) continue; const rcData = (await rcRes.json()) as any; const sub = rcData.subscriber; if (!sub) continue; const entitlements = sub.entitlements || {}; const subscriptions = sub.subscriptions || {}; const hasActive = Object.values(entitlements).some((e: any) => new Date(e.expires_date) > new Date()); const hasTrial = Object.values(subscriptions).some((s: any) => s.period_type === "trial" && new Date(s.expires_date) > new Date()); let userRevenue = 0; for (const [pid, s2] of Object.entries(subscriptions) as any[]) { if (s2.store === "app_store" || s2.store === "play_store") { if (pid.includes("yearly")) userRevenue += 29.99; else if (pid.includes("monthly")) userRevenue += 3.99; else if (pid.includes("lifetime")) userRevenue += 149.99; } } if (hasActive) activeCount++; if (hasTrial) trialCount++; totalRevenue += userRevenue; for (const [pid, s2] of Object.entries(subscriptions) as any[]) { const expires = new Date(s2.expires_date); if (expires > new Date() && s2.period_type !== "trial") { if (pid.includes("yearly")) mrr += 29.99 / 12; else if (pid.includes("monthly")) mrr += 3.99; } } subscribers.push({ user_id: uid, name: user.name || null, email: user.email || null, db_status: user.subscription_status, has_active: hasActive, has_trial: hasTrial, revenue: userRevenue, entitlements: Object.keys(entitlements), subscriptions: Object.entries(subscriptions).map(([pid2, s3]: [string, any]) => ({ product: pid2, store: s3.store, purchase_date: s3.purchase_date, expires_date: s3.expires_date, period_type: s3.period_type, is_active: new Date(s3.expires_date) > new Date(), auto_resume_date: s3.auto_resume_date, unsubscribe_detected_at: s3.unsubscribe_detected_at })), first_seen: sub.first_seen }); break; } catch { continue; } } }
    return c.json({ generated_at: new Date().toISOString(), summary: { active_subscriptions: activeCount, active_trials: trialCount, total_revenue_estimated: totalRevenue, mrr_estimated: Math.round(mrr * 100) / 100, net_mrr: Math.round(mrr * (1 - APPLE_CUT) * 100) / 100, total_users_checked: usersResult.rows.length, subscribers_found: subscribers.filter(s => s.has_active || s.has_trial).length }, subscribers: subscribers.filter(s => s.subscriptions.length > 0 || s.has_active || s.has_trial), all_users: subscribers });
  } catch (err: any) { return c.json({ error: "RevenueCat query failed", detail: err.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── APPLE APP STORE ────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
async function initAppleAnalytics(): Promise<string | null> { const token = generateASCToken(); if (!token) return null; try { const existingRes = await fetch(`https://api.appstoreconnect.apple.com/v1/apps/${PRAMEN_APP_ID}/analyticsReportRequests?filter[accessType]=ONGOING`, { headers: { Authorization: `Bearer ${token}` } }); if (existingRes.ok) { const existing = (await existingRes.json()) as any; if (existing.data?.length > 0) { console.log("[Apple] Existing report request found:", existing.data[0].id); return existing.data[0].id; } } const createRes = await fetch("https://api.appstoreconnect.apple.com/v1/analyticsReportRequests", { method: "POST", headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" }, body: JSON.stringify({ data: { type: "analyticsReportRequests", attributes: { accessType: "ONGOING" }, relationships: { app: { data: { type: "apps", id: PRAMEN_APP_ID } } } } }) }); if (createRes.ok) { const created = (await createRes.json()) as any; console.log("[Apple] Created report request:", created.data?.id); return created.data?.id || null; } else { const err = await createRes.text().catch(() => ""); console.log("[Apple] Create request failed:", createRes.status, err.substring(0,200)); return null; } } catch (err: any) { console.error("[Apple] Init error:", err.message); return null; } }

async function pullAppleAnalytics(): Promise<any> { const token = generateASCToken(); if (!token) return null; try { const requestId = await initAppleAnalytics(); if (!requestId) return null; const reportsRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/${requestId}/reports`, { headers: { Authorization: `Bearer ${token}` } }); if (!reportsRes.ok) return { connected: true, status: "api_error", code: reportsRes.status }; const reportsData = (await reportsRes.json()) as any; const reports = reportsData.data || []; if (reports.length === 0) return { connected: true, status: "generating" }; const targetKeywords = ["discovery","engagement","download","impression","standard"]; const sortedReports = [...reports].sort((a: any, b: any) => { const aName = (a.attributes?.name||"").toLowerCase(); const bName = (b.attributes?.name||"").toLowerCase(); const aScore = targetKeywords.reduce((s: number, kw: string) => s+(aName.includes(kw)?1:0),0)+(aName.includes("standard")?2:0); const bScore = targetKeywords.reduce((s: number, kw: string) => s+(bName.includes(kw)?1:0),0)+(bName.includes("standard")?2:0); return bScore-aScore; }); let lastResult: any = null; for (const report of sortedReports) { const instancesRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReports/${report.id}/instances?limit=7`, { headers: { Authorization: `Bearer ${token}` } }); if (!instancesRes.ok) continue; const instancesData = (await instancesRes.json()) as any; const instances = instancesData.data || []; if (instances.length === 0) { lastResult = { connected: true, status: "pending", reports_available: reports.map((r: any) => r.attributes?.name) }; continue; } for (const instance of instances.slice(0,3)) { const segmentsRes = await fetch(`https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/${instance.id}/segments?fields[analyticsReportSegments]=url,checksum,sizeInBytes`, { headers: { Authorization: `Bearer ${token}` } }); if (!segmentsRes.ok) continue; const segmentsData = (await segmentsRes.json()) as any; const segments = segmentsData.data || []; if (segments.length === 0) continue; const segUrl = segments[0].attributes?.url; if (!segUrl) continue; const dataRes = await fetch(segUrl); if (!dataRes.ok) continue; const rawText = await dataRes.text(); const lines = rawText.trim().split("\n"); if (lines.length < 2) continue; const headers = lines[0].split("\t").map((h: string) => h.trim().toLowerCase().replace(/\s+/g, "_")); const rows: any[] = []; for (let i = 1; i < lines.length; i++) { const cols = lines[i].split("\t"); const row: any = {}; headers.forEach((h: string, j: number) => { row[h] = cols[j]?.trim() || ""; }); rows.push(row); } let stored = 0; for (const row of rows) { const date = row.date || row.report_date || row.day || row.calendar_date; if (!date) continue; const impressions = parseInt(row.impressions||row.total_impressions||row.store_impressions||"0")||0; const pageViews = parseInt(row.product_page_views||row.page_views||row.product_page_view_count||"0")||0; const downloads = parseInt(row.total_downloads||row.first_time_downloads||row.app_units||row.redownloads_and_first_time_downloads||"0")||0; const proceeds = parseFloat(row.proceeds||row.developer_proceeds||"0")||0; const convRate = pageViews>0?downloads/pageViews:(impressions>0?downloads/impressions:0); if (impressions||downloads||pageViews) { await pool.query(`INSERT INTO daily_app_store_metrics (date,impressions,product_page_views,app_units,conversion_rate,proceeds,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date) DO UPDATE SET impressions=GREATEST(daily_app_store_metrics.impressions,$2),product_page_views=GREATEST(daily_app_store_metrics.product_page_views,$3),app_units=GREATEST(daily_app_store_metrics.app_units,$4),conversion_rate=$5,proceeds=GREATEST(daily_app_store_metrics.proceeds,$6),updated_at=NOW()`, [date, impressions, pageViews, downloads, convRate, proceeds]); stored++; } } if (stored > 0) return { connected: true, status: "ok", report: report.attributes?.name, reports_available: reports.map((r: any) => r.attributes?.name), rows_parsed: rows.length, days_stored: stored, headers, sample: rows.slice(0,2) }; } } return lastResult || { connected: true, status: "no_data", reports_available: reports.map((r: any) => r.attributes?.name) }; } catch (err: any) { console.error("[Apple] Analytics error:", err.message); return { connected: false, error: err.message }; } }

app.post("/api/dashboard/appstore/seed", async (c) => { const secret = c.req.query("key") || c.req.header("X-Dashboard-Key"); if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401); try { const body = await c.req.json(); const { metrics } = body; if (!Array.isArray(metrics)) return c.json({ error: "metrics array required" }, 400); let stored = 0; for (const m of metrics) { if (!m.date) continue; await pool.query(`INSERT INTO daily_app_store_metrics (date,impressions,product_page_views,app_units,conversion_rate,proceeds,updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW()) ON CONFLICT (date) DO UPDATE SET impressions=$2,product_page_views=$3,app_units=$4,conversion_rate=$5,proceeds=$6,updated_at=NOW()`, [m.date, m.impressions||0, m.product_page_views||0, m.app_units||0, m.conversion_rate||0, m.proceeds||0]); stored++; } return c.json({ status: "ok", stored }); } catch (e: any) { return c.json({ error: e.message }, 500); } });

app.get("/api/dashboard/appstore", async (c) => { const secret = c.req.query("key") || c.req.header("X-Dashboard-Key"); if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401); const token = generateASCToken(); if (!token) return c.json({ connected: false, error: "Apple API not configured" }); try { const appRes = await fetch(`https://api.appstoreconnect.apple.com/v1/apps/${PRAMEN_APP_ID}`, { headers: { Authorization: `Bearer ${token}` } }); if (!appRes.ok) return c.json({ connected: false, error: "Apple API " + appRes.status }); const appData = (await appRes.json()) as any; const analytics = await pullAppleAnalytics(); const stored = await pool.query(`SELECT * FROM daily_app_store_metrics ORDER BY date DESC LIMIT 30`).catch(() => ({ rows: [] })); return c.json({ connected: true, app: { name: appData.data?.attributes?.name, bundleId: appData.data?.attributes?.bundleId }, analytics: analytics, daily_metrics: stored.rows, timestamp: new Date().toISOString() }); } catch (e: any) { return c.json({ connected: false, error: e.message }); } });

// ═══════════════════════════════════════════════════════════════════
// ─── DASHBOARD ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/dashboard", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const uc = await pool.query("SELECT COUNT(*) as count FROM users");
    let tm = 0, te = 0, tp = 0; for (const [, ci] of circles) { tm += ci.members.length; te += ci.encouragements.length; tp += ci.prayerRequests.length; }
    const rv = await pool.query(`SELECT * FROM daily_revenue WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const wd = await pool.query(`SELECT * FROM daily_web_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const ad = await pool.query(`SELECT * FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const re = await pool.query(`SELECT * FROM revenue_events ORDER BY created_at DESC LIMIT 20`).catch(() => ({ rows: [] }));
    const ss = await pool.query(`SELECT subscription_status, COUNT(*) as count FROM users GROUP BY subscription_status`).catch(() => ({ rows: [] }));
    const sb: Record<string,number> = {}; for (const r of ss.rows) sb[r.subscription_status || "none"] = parseInt(r.count);
    const tg = rv.rows.reduce((s: number, r: any) => s + (r.revenue_gross||0), 0); const tn = rv.rows.reduce((s: number, r: any) => s + (r.revenue_net||0), 0);
    const tv = wd.rows.reduce((s: number, r: any) => s + (r.visitors||0), 0); const tc = wd.rows.reduce((s: number, r: any) => s + (r.app_store_clicks||0), 0);
    const pc = await pool.query("SELECT COUNT(*) as count FROM circle_posts WHERE status='published'").catch(() => ({ rows: [{ count: 0 }] }));
    return c.json({ generated_at: new Date().toISOString(), kpis: { total_users: parseInt(uc.rows[0]?.count||"0"), active_subscribers: (sb["active"]||0)+(sb["lifetime"]||0), mrr_net: tn, revenue_gross_30d: tg, revenue_net_30d: tn, active_circles: circles.size, total_circle_members: tm, total_posts: parseInt(pc.rows[0]?.count||"0"), landing_visitors_7d: tv, landing_app_store_clicks_7d: tc, landing_conversion: tv > 0 ? ((tc/tv)*100).toFixed(1)+"%" : "0%" }, subscription_breakdown: sb, revenue: { daily: rv.rows, recent_events: re.rows, total_subscribers_30d: rv.rows.reduce((s: number, r: any) => s+(r.new_subscribers||0), 0), total_cancellations_30d: rv.rows.reduce((s: number, r: any) => s+(r.cancellations||0), 0) }, web: { daily: wd.rows }, app_store: { daily: ad.rows }, circles: { total: circles.size, total_members: tm, total_encouragements: te, total_prayer_requests: tp, circles: Array.from(circles.values()).map(ci => ({ name: ci.name, code: ci.code, members: ci.members.length, prayerRequests: ci.prayerRequests.length, encouragements: ci.encouragements.length, createdAt: ci.createdAt })) } });
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
  setInterval(() => { publishScheduledPosts().catch(() => {}); }, 60 * 1000);
  setTimeout(() => { generateDailyReflection().catch(() => {}); }, 5 * 60 * 1000); // delay 5min after startup
  setInterval(() => { generateDailyReflection().catch(() => {}); }, 6 * 60 * 60 * 1000);
  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v3.9.1 on port ${info.port}`);
    console.log(`   PostHog: ${POSTHOG_API_KEY ? "✓" : "✗"} | Read: ${POSTHOG_PERSONAL_KEY ? "✓" : "✗"} | Plausible: ${PLAUSIBLE_API_KEY ? "✓" : "✗"}`);
    console.log(`   Apple: ${ASC_KEY_ID ? "✓" : "✗"} | RC: ${REVENUECAT_SECRET_KEY ? "✓" : "✗"} | APNs: ${APNS_KEY_ID ? "✓" : "✗"}`);
    console.log(`   Storage: ${R2_ACCOUNT_ID ? "✓" : "✗"} | Admin: ${ADMIN_USER_ID ? ADMIN_USER_ID.substring(0,8)+"..." : "✗"} | Lumi: ${GEMINI_API_KEY ? "✓" : "✗"}`);
    console.log(`   Dashboard: /dashboard?key=... | Circles: ${circles.size} | Scheduler: active (60s)\n`);
  });
}
start();
