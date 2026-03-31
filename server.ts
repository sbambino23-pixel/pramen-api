import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomUUID, createHash } from "crypto";
import { readFileSync } from "fs";
import pg from "pg";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────

interface StoredMember { userId: string; name: string; streakCount: number; lastPrayedDate: string | null; joinedAt: string; }
interface StoredPrayerRequest { id: string; requesterUserId: string; requesterName: string; text: string; timestamp: string; isAnonymous: boolean; prayedByUserIds: string[]; }
interface StoredEncouragement { id: string; toUserId: string; fromUserId: string; fromName: string; message: string; timestamp: string; }
interface StoredCircle { id: string; name: string; code: string; emoji: string; creatorUserId: string; members: StoredMember[]; prayerRequests: StoredPrayerRequest[]; encouragements: StoredEncouragement[]; createdAt: string; }

// ─── PostHog Analytics ───────────────────────────────────────────────

const POSTHOG_API_KEY = process.env.POSTHOG_API_KEY || "";
const POSTHOG_HOST = process.env.POSTHOG_HOST || "https://us.i.posthog.com";

// ─── Dashboard & Plausible Config ────────────────────────────────────

const PLAUSIBLE_API_KEY = process.env.PLAUSIBLE_API_KEY || "";
const PLAUSIBLE_SITE_ID = "pramen.app";
const DASHBOARD_SECRET = process.env.DASHBOARD_SECRET || "pramen_dash_2026";
const APPLE_CUT = 0.15;

function trackEvent(distinctId: string, event: string, properties?: Record<string, any>) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ api_key: POSTHOG_API_KEY, event, distinct_id: distinctId, properties: { ...properties, $lib: "pramen-backend", platform: "ios" }, timestamp: new Date().toISOString() }),
  }).catch((err) => console.error("[PostHog] Track error:", err.message));
}

function identifyUser(distinctId: string, userProperties: Record<string, any>) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ api_key: POSTHOG_API_KEY, event: "$identify", distinct_id: distinctId, properties: { $set: userProperties }, timestamp: new Date().toISOString() }),
  }).catch((err) => console.error("[PostHog] Identify error:", err.message));
}

// ─── Postgres ────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost") ? false : { rejectUnauthorized: false },
});

async function initDb(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query(`CREATE TABLE IF NOT EXISTS circles (code TEXT PRIMARY KEY, data JSONB NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, apple_user_id TEXT UNIQUE, google_user_id TEXT UNIQUE, email TEXT, name TEXT NOT NULL DEFAULT '', auth_provider TEXT DEFAULT 'apple', auth_token TEXT UNIQUE NOT NULL, device_user_id TEXT, trial_start_date TIMESTAMPTZ, trial_end_date TIMESTAMPTZ, subscription_status TEXT DEFAULT 'none', email_opt_in BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS google_user_id TEXT UNIQUE`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider TEXT DEFAULT 'apple'`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_in BOOLEAN DEFAULT false`).catch(() => {});
    await client.query(`CREATE TABLE IF NOT EXISTS user_data (user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE, streak_count INTEGER DEFAULT 0, highest_streak INTEGER DEFAULT 0, total_prayers INTEGER DEFAULT 0, total_minutes INTEGER DEFAULT 0, last_prayed_date TIMESTAMPTZ, sessions JSONB DEFAULT '[]'::jsonb, preferences JSONB DEFAULT '{}'::jsonb, circle_codes TEXT[] DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_apple_user_id ON users(apple_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_google_user_id ON users(google_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_auth_token ON users(auth_token)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_users_device_user_id ON users(device_user_id)`);

    // ─── Analytics Tables ────────────────────────────────────────
    await client.query(`CREATE TABLE IF NOT EXISTS daily_web_metrics (date DATE PRIMARY KEY, visitors INT DEFAULT 0, pageviews INT DEFAULT 0, bounce_rate REAL DEFAULT 0, visit_duration_avg REAL DEFAULT 0, app_store_clicks INT DEFAULT 0, top_sources JSONB DEFAULT '[]', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_revenue (date DATE PRIMARY KEY, new_subscribers INT DEFAULT 0, renewals INT DEFAULT 0, cancellations INT DEFAULT 0, revenue_gross REAL DEFAULT 0, revenue_net REAL DEFAULT 0, mrr REAL DEFAULT 0, plan_breakdown JSONB DEFAULT '{}', updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_product_metrics (date DATE PRIMARY KEY, dau INT DEFAULT 0, new_users INT DEFAULT 0, prayers_logged INT DEFAULT 0, circles_created INT DEFAULT 0, invites_accepted INT DEFAULT 0, encouragements_sent INT DEFAULT 0, paywall_views INT DEFAULT 0, plan_taps INT DEFAULT 0, scripture_views INT DEFAULT 0, signups INT DEFAULT 0, account_deletions INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS daily_app_store_metrics (date DATE PRIMARY KEY, impressions INT DEFAULT 0, product_page_views INT DEFAULT 0, app_units INT DEFAULT 0, conversion_rate REAL DEFAULT 0, proceeds REAL DEFAULT 0, active_devices INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())`);
    await client.query(`CREATE TABLE IF NOT EXISTS revenue_events (id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text, user_id TEXT, event_type TEXT NOT NULL, plan TEXT, product_id TEXT, price REAL DEFAULT 0, currency TEXT DEFAULT 'USD', environment TEXT DEFAULT 'production', created_at TIMESTAMPTZ DEFAULT NOW())`);

    console.log("Database initialized (circles + users + user_data + analytics)");
  } catch (err) { console.error("Failed to initialize database:", err); } finally { client.release(); }
}

// ─── Circle Cache ────────────────────────────────────────────────────

const circles = new Map<string, StoredCircle>();

async function loadAllFromDb(): Promise<void> {
  try { const result = await pool.query("SELECT code, data FROM circles"); for (const row of result.rows) circles.set(row.code, row.data as StoredCircle); console.log(`Loaded ${circles.size} circles from database`); } catch (err) { console.error("Failed to load circles:", err); }
}

async function saveCircleToDb(circle: StoredCircle): Promise<void> {
  const key = circle.code.toUpperCase(); circles.set(key, circle);
  try { await pool.query(`INSERT INTO circles (code, data, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (code) DO UPDATE SET data = $2, updated_at = NOW()`, [key, JSON.stringify(circle)]); } catch (err) { console.error("Failed to save circle:", err); }
}

async function deleteCircleFromDb(code: string): Promise<boolean> {
  const key = code.toUpperCase(); const existed = circles.delete(key);
  try { await pool.query("DELETE FROM circles WHERE code = $1", [key]); } catch (err) { console.error("Failed to delete circle:", err); } return existed;
}

function getCircle(code: string): StoredCircle | undefined { return circles.get(code.toUpperCase()); }

function generateCode(): string {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; let code = "";
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  if (circles.has(code)) return generateCode(); return code;
}

// ─── Auth Helpers ────────────────────────────────────────────────────

function generateAuthToken(): string { return randomUUID() + "-" + randomUUID(); }

async function getUserByToken(token: string) { if (!token) return null; try { const r = await pool.query("SELECT * FROM users WHERE auth_token = $1", [token]); return r.rows[0] || null; } catch { return null; } }
async function getUserByAppleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE apple_user_id = $1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByGoogleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE google_user_id = $1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByEmail(email: string) { try { const r = await pool.query("SELECT * FROM users WHERE email = $1", [email]); return r.rows[0] || null; } catch { return null; } }

async function getUserData(userId: string) {
  try { const r = await pool.query("SELECT * FROM user_data WHERE user_id = $1", [userId]); if (r.rows[0]) { const d = r.rows[0]; return { streakCount: d.streak_count, highestStreak: d.highest_streak, totalPrayers: d.total_prayers, totalMinutes: d.total_minutes, lastPrayedDate: d.last_prayed_date, sessions: d.sessions || [], preferences: d.preferences || {}, circleCodes: d.circle_codes || [] }; } return null; } catch { return null; }
}

function getUserCircleCodes(userId: string): string[] {
  const codes: string[] = []; for (const [code, circle] of circles) { if (circle.members.some((m) => m.userId === userId)) codes.push(code); } return codes;
}

async function migrateCircleMembership(oldDeviceId: string, newUserId: string, userName: string) {
  for (const [, circle] of circles) {
    const member = circle.members.find((m) => m.userId === oldDeviceId);
    if (member) { member.userId = newUserId; if (userName) member.name = userName; await saveCircleToDb(circle); }
    if (circle.creatorUserId === oldDeviceId) { circle.creatorUserId = newUserId; await saveCircleToDb(circle); }
  }
}

// ─── Plausible Pull ──────────────────────────────────────────────────

async function pullPlausibleMetrics(): Promise<void> {
  if (!PLAUSIBLE_API_KEY) { console.log("[Plausible] No API key, skipping"); return; }
  try {
    const headers = { Authorization: `Bearer ${PLAUSIBLE_API_KEY}` };
    const base = "https://plausible.io/api/v1/stats";
    const aggRes = await fetch(`${base}/aggregate?site_id=${PLAUSIBLE_SITE_ID}&period=day&metrics=visitors,pageviews,bounce_rate,visit_duration`, { headers });
    const aggData = (await aggRes.json()) as any;
    let appStoreClicks = 0;
    try { const cr = await fetch(`${base}/aggregate?site_id=${PLAUSIBLE_SITE_ID}&period=day&metrics=events&filters=event:name==App%20Store%20Click`, { headers }); const cd = (await cr.json()) as any; appStoreClicks = cd?.results?.events?.value || 0; } catch {}
    let topSources: any[] = [];
    try { const sr = await fetch(`${base}/breakdown?site_id=${PLAUSIBLE_SITE_ID}&period=day&property=visit:source&limit=5`, { headers }); const sd = (await sr.json()) as any; topSources = sd?.results || []; } catch {}
    const today = new Date().toISOString().split("T")[0];
    const m = aggData?.results || {};
    await pool.query(`INSERT INTO daily_web_metrics (date,visitors,pageviews,bounce_rate,visit_duration_avg,app_store_clicks,top_sources,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (date) DO UPDATE SET visitors=$2,pageviews=$3,bounce_rate=$4,visit_duration_avg=$5,app_store_clicks=$6,top_sources=$7,updated_at=NOW()`,
      [today, m.visitors?.value||0, m.pageviews?.value||0, m.bounce_rate?.value||0, m.visit_duration?.value||0, appStoreClicks, JSON.stringify(topSources)]);
    console.log(`[Plausible] ${today}: ${m.visitors?.value||0} visitors, ${appStoreClicks} clicks`);
  } catch (err: any) { console.error("[Plausible] Error:", err.message); }
}

// ─── Hono App ────────────────────────────────────────────────────────

const app = new Hono();
app.use("*", cors());
app.onError((err, c) => { console.error("Unhandled error:", err); return c.json({ error: "Internal server error", detail: err.message }, 500); });

// ─── Health ──────────────────────────────────────────────────────────

app.get("/", (c) => c.json({ status: "ok", service: "prAmen API", version: "2.1.0", storage: "postgres", circles: circles.size, auth: "sign-in-with-apple+google", analytics: POSTHOG_API_KEY ? "posthog" : "disabled", plausible: PLAUSIBLE_API_KEY ? "configured" : "disabled", dashboard: "/dashboard?key=...", timestamp: new Date().toISOString() }));
app.get("/api/circles/health", (c) => c.json({ status: "ok", storage: "postgres", circles: circles.size, timestamp: new Date().toISOString() }));

// ═══════════════════════════════════════════════════════════════════
// ─── AUTH ENDPOINTS ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/auth/apple", async (c) => {
  try {
    const body = await c.req.json();
    const { appleUserId, email, fullName, identityToken, deviceUserId } = body;
    if (!appleUserId) return c.json({ error: "appleUserId is required" }, 400);
    let user = await getUserByAppleId(appleUserId); let isNewUser = false;
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      if (email && !user.email) { await pool.query("UPDATE users SET email=$1,updated_at=NOW() WHERE id=$2", [email, user.id]); user.email = email; }
      if (deviceUserId && deviceUserId !== user.device_user_id) await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || "";
      const trialStart = new Date(); const trialEnd = new Date(trialStart.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,apple_user_id,email,name,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'trial')`, [userId, appleUserId, email||null, userName, authToken, deviceUserId||null, trialStart, trialEnd]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, apple_user_id: appleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: trialStart, trial_end_date: trialEnd, subscription_status: "trial" };
      trackEvent(userId, "user_signed_up", { auth_provider: "apple", has_email: !!email, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name);
    const userData = await getUserData(user.id); const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, isNewUser }, data: userData, circleCodes: userCircleCodes });
  } catch (error: any) { console.error("[Auth] Apple error:", error); return c.json({ error: "Authentication failed", detail: error.message }, 500); }
});

app.post("/api/auth/google", async (c) => {
  try {
    const body = await c.req.json();
    const { googleUserId, email, fullName, idToken, deviceUserId } = body;
    if (!googleUserId || !email) return c.json({ error: "googleUserId and email are required" }, 400);
    let verified = !idToken;
    if (idToken) { try { const tr = await fetch(`https://oauth2.googleapis.com/tokeninfo?id_token=${idToken}`); if (tr.ok) { const td = (await tr.json()) as any; if (td.sub === googleUserId) verified = true; } } catch {} }
    let user = await getUserByGoogleId(googleUserId); let isNewUser = false;
    if (!user) { const existing = await getUserByEmail(email); if (existing) { await pool.query("UPDATE users SET google_user_id=$1,auth_provider=CASE WHEN auth_provider='apple' THEN 'apple+google' ELSE 'google' END,updated_at=NOW() WHERE id=$2", [googleUserId, existing.id]); user = existing; user.google_user_id = googleUserId; } }
    if (user) {
      if (fullName && !user.name) { await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [fullName, user.id]); user.name = fullName; }
      if (deviceUserId && deviceUserId !== user.device_user_id) await pool.query("UPDATE users SET device_user_id=$1,updated_at=NOW() WHERE id=$2", [deviceUserId, user.id]);
    } else {
      isNewUser = true; const authToken = generateAuthToken(); const userId = randomUUID(); const userName = fullName || email.split("@")[0];
      const trialStart = new Date(); const trialEnd = new Date(trialStart.getTime() + 7*24*60*60*1000);
      await pool.query(`INSERT INTO users (id,google_user_id,email,name,auth_provider,auth_token,device_user_id,trial_start_date,trial_end_date,subscription_status) VALUES ($1,$2,$3,$4,'google',$5,$6,$7,$8,'trial')`, [userId, googleUserId, email, userName, authToken, deviceUserId||null, trialStart, trialEnd]);
      await pool.query(`INSERT INTO user_data (user_id) VALUES ($1)`, [userId]);
      user = { id: userId, google_user_id: googleUserId, email, name: userName, auth_token: authToken, device_user_id: deviceUserId, trial_start_date: trialStart, trial_end_date: trialEnd, subscription_status: "trial" };
      trackEvent(userId, "user_signed_up", { auth_provider: "google", has_email: true, has_device_migration: !!deviceUserId });
    }
    if (deviceUserId && isNewUser) await migrateCircleMembership(deviceUserId, user.id, user.name);
    const userData = await getUserData(user.id); const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, isNewUser }, data: userData, circleCodes: userCircleCodes });
  } catch (error: any) { console.error("[Auth] Google error:", error); return c.json({ error: "Authentication failed", detail: error.message }, 500); }
});

app.put("/api/user/email-opt-in", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ error: "Unauthorized" }, 401);
  const { optIn } = await c.req.json(); await pool.query("UPDATE users SET email_opt_in=$1,updated_at=NOW() WHERE id=$2", [!!optIn, user.id]);
  if (optIn) trackEvent(user.id, "email_opt_in", { email: user.email });
  return c.json({ success: true, emailOptIn: !!optIn });
});

app.get("/api/admin/email-list", async (c) => {
  if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403);
  try { const r = await pool.query("SELECT email,name,auth_provider,created_at FROM users WHERE email_opt_in=true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"); return c.json({ count: r.rows.length, emails: r.rows }); } catch { return c.json({ error: "Failed" }, 500); }
});

app.post("/api/auth/verify", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ valid: false }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ valid: false }, 401);
  const userData = await getUserData(user.id); const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);
  return c.json({ valid: true, user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status }, data: userData, circleCodes: userCircleCodes });
});

app.post("/api/auth/logout", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader) return c.json({ success: true });
  const newToken = generateAuthToken(); await pool.query("UPDATE users SET auth_token=$1,updated_at=NOW() WHERE auth_token=$2", [newToken, authHeader.replace("Bearer ", "")]);
  return c.json({ success: true });
});

app.delete("/api/auth/account", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ error: "User not found" }, 404);
  await pool.query("DELETE FROM user_data WHERE user_id=$1", [user.id]); await pool.query("DELETE FROM users WHERE id=$1", [user.id]);
  trackEvent(user.id, "account_deleted", {}); return c.json({ success: true });
});

// ═══════════════════════════════════════════════════════════════════
// ─── DATA SYNC ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.get("/api/user/data", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ error: "Unauthorized" }, 401);
  const userData = await getUserData(user.id); const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);
  return c.json({ user: { id: user.id, name: user.name, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status }, data: userData, circleCodes: userCircleCodes });
});

app.put("/api/user/data", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ error: "Unauthorized" }, 401);
  const body = await c.req.json();
  try {
    await pool.query(`INSERT INTO user_data (user_id,streak_count,highest_streak,total_prayers,total_minutes,last_prayed_date,sessions,preferences,circle_codes,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT (user_id) DO UPDATE SET streak_count=GREATEST(user_data.streak_count,$2),highest_streak=GREATEST(user_data.highest_streak,$3),total_prayers=GREATEST(user_data.total_prayers,$4),total_minutes=GREATEST(user_data.total_minutes,$5),last_prayed_date=GREATEST(user_data.last_prayed_date,$6),sessions=CASE WHEN jsonb_array_length($7::jsonb)>jsonb_array_length(user_data.sessions) THEN $7 ELSE user_data.sessions END,preferences=$8,circle_codes=$9,updated_at=NOW()`,
      [user.id, body.streakCount||0, body.highestStreak||0, body.totalPrayers||0, body.totalMinutes||0, body.lastPrayedDate||null, JSON.stringify(body.sessions||[]), JSON.stringify(body.preferences||{}), body.circleCodes||[]]);
    if (body.userName && body.userName !== user.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [body.userName, user.id]);
    return c.json({ status: "ok", synced: true });
  } catch (error: any) { return c.json({ error: "Sync failed", detail: error.message }, 500); }
});

app.put("/api/user/name", async (c) => {
  const authHeader = c.req.header("Authorization"); if (!authHeader?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401);
  const user = await getUserByToken(authHeader.replace("Bearer ", "")); if (!user) return c.json({ error: "Unauthorized" }, 401);
  const { name } = await c.req.json(); if (!name || name.trim().length === 0) return c.json({ error: "Name is required" }, 400);
  await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [name.trim(), user.id]);
  for (const [, circle] of circles) { const member = circle.members.find((m) => m.userId === user.id || m.userId === user.device_user_id); if (member) { member.name = name.trim(); await saveCircleToDb(circle); } }
  return c.json({ success: true, name: name.trim() });
});

// ═══════════════════════════════════════════════════════════════════
// ─── EVENT CAPTURE ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/v1/events/capture", async (c) => {
  try { const { event, distinct_id, properties } = await c.req.json(); if (!event || !distinct_id) return c.json({ error: "Missing event or distinct_id" }, 400); trackEvent(distinct_id, event, { ...properties, app_version: c.req.header("X-App-Version") || "unknown" }); return c.json({ status: "ok" }); } catch { return c.json({ error: "Internal error" }, 500); }
});

app.post("/api/v1/events/capture/batch", async (c) => {
  try { const { events: el } = await c.req.json(); if (!Array.isArray(el) || !el.length) return c.json({ error: "Empty events" }, 400); if (el.length > 50) return c.json({ error: "Max 50" }, 400); for (const e of el) trackEvent(e.distinct_id, e.event, e.properties); return c.json({ status: "ok", count: el.length }); } catch { return c.json({ error: "Internal error" }, 500); }
});

app.post("/api/v1/events/identify", async (c) => {
  try { const { distinct_id, properties } = await c.req.json(); if (!distinct_id) return c.json({ error: "Missing distinct_id" }, 400); identifyUser(distinct_id, { language: properties?.language, subscription_status: properties?.subscription_status, subscription_plan: properties?.subscription_plan, circle_count: properties?.circle_count, install_source: properties?.install_source, total_prayers: properties?.total_prayers, current_streak: properties?.current_streak }); return c.json({ status: "ok" }); } catch { return c.json({ error: "Internal error" }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── REVENUECAT WEBHOOK ─────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

const RC_EVENT_MAP: Record<string, string> = { INITIAL_PURCHASE: "subscription_started", RENEWAL: "subscription_renewed", CANCELLATION: "subscription_cancelled", UNCANCELLATION: "subscription_reactivated", EXPIRATION: "subscription_expired", BILLING_ISSUE: "billing_issue_detected", PRODUCT_CHANGE: "subscription_plan_changed", NON_RENEWING_PURCHASE: "lifetime_purchased" };
function getPlanFromProductId(pid: string): string { if (pid.includes("monthly")) return "monthly"; if (pid.includes("yearly")) return "yearly"; if (pid.includes("lifetime")) return "lifetime"; return "unknown"; }
function getStatusFromRCEvent(type: string): string { switch(type) { case "INITIAL_PURCHASE": case "RENEWAL": case "UNCANCELLATION": return "active"; case "CANCELLATION": return "cancelled"; case "EXPIRATION": return "expired"; case "BILLING_ISSUE": return "billing_issue"; case "NON_RENEWING_PURCHASE": return "lifetime"; default: return "unknown"; } }

app.post("/webhooks/revenuecat", async (c) => {
  try {
    const authHeader = c.req.header("Authorization"); const expectedSecret = process.env.REVENUECAT_WEBHOOK_SECRET;
    if (expectedSecret && authHeader !== `Bearer ${expectedSecret}`) return c.json({ error: "Unauthorized" }, 401);
    const body = await c.req.json(); const rcEvent = body.event;
    if (!rcEvent?.type) return c.json({ error: "Invalid payload" }, 400);
    const eventName = RC_EVENT_MAP[rcEvent.type]; if (!eventName) return c.json({ status: "skipped" });
    const userId = rcEvent.app_user_id; const productId = rcEvent.product_id || ""; const plan = getPlanFromProductId(productId); const status = getStatusFromRCEvent(rcEvent.type);
    await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, userId]).catch(() => {});
    trackEvent(userId, eventName, { plan, product_id: productId, price: rcEvent.price, currency: rcEvent.currency, store: rcEvent.store, period_type: rcEvent.period_type, environment: rcEvent.environment, country_code: rcEvent.country_code, $revenue: rcEvent.price || 0, $currency: rcEvent.currency || "USD" });
    identifyUser(userId, { subscription_status: status, subscription_plan: plan, last_revenue_event: eventName });

    // ─── Log to analytics tables ───
    try {
      const price = rcEvent.price || 0; const netPrice = price * (1 - APPLE_CUT); const today = new Date().toISOString().split("T")[0];
      await pool.query(`INSERT INTO revenue_events (user_id,event_type,plan,product_id,price,currency,environment) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [userId, eventName, plan, productId, price, rcEvent.currency||"USD", rcEvent.environment||"production"]);
      if (eventName === "subscription_started" || eventName === "lifetime_purchased") {
        await pool.query(`INSERT INTO daily_revenue (date,new_subscribers,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET new_subscribers=daily_revenue.new_subscribers+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, netPrice]);
      } else if (eventName === "subscription_renewed") {
        await pool.query(`INSERT INTO daily_revenue (date,renewals,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET renewals=daily_revenue.renewals+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, netPrice]);
      } else if (eventName === "subscription_cancelled" || eventName === "subscription_expired") {
        await pool.query(`INSERT INTO daily_revenue (date,cancellations) VALUES ($1,1) ON CONFLICT (date) DO UPDATE SET cancellations=daily_revenue.cancellations+1,updated_at=NOW()`, [today]);
      }
    } catch (err: any) { console.error("[Revenue] Log error:", err.message); }

    console.log(`[RC Webhook] ${rcEvent.type} → ${eventName} | user=${userId} plan=${plan}`);
    return c.json({ status: "ok" });
  } catch (error) { console.error("[RC Webhook] Error:", error); return c.json({ error: "Internal error" }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLES CRUD ───────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.post("/api/circles", async (c) => {
  const body = await c.req.json(); const { userId, userName, name, emoji, streakCount, lastPrayedDate } = body;
  if (!userId || !userName) return c.json({ error: "userId and userName are required" }, 400);
  const code = generateCode(); const circle: StoredCircle = { id: randomUUID(), name: name || "Prayer Circle", code, emoji: emoji || "🏠", creatorUserId: userId, members: [{ userId, name: userName, streakCount: streakCount||0, lastPrayedDate: lastPrayedDate||null, joinedAt: new Date().toISOString() }], prayerRequests: [], encouragements: [], createdAt: new Date().toISOString() };
  await saveCircleToDb(circle); trackEvent(userId, "circle_created", { circle_id: circle.id, circle_code: code, circle_name: name || "Prayer Circle" });
  return c.json({ circle }, 201);
});

app.get("/api/circles/:code", (c) => { const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404); return c.json({ circle }); });

app.post("/api/circles/:code/join", async (c) => {
  const code = c.req.param("code").toUpperCase(); let body; try { body = await c.req.json(); } catch { return c.json({ error: "Invalid request body" }, 400); }
  const { userId, userName, streakCount, lastPrayedDate } = body; if (!userId || !userName) return c.json({ error: "userId and userName are required" }, 400);
  const circle = getCircle(code); if (!circle) return c.json({ error: "Circle not found." }, 404);
  if (circle.members.find((m) => m.userId === userId)) return c.json({ circle });
  circle.members.push({ userId, name: userName, streakCount: streakCount||0, lastPrayedDate: lastPrayedDate||null, joinedAt: new Date().toISOString() });
  await saveCircleToDb(circle);
  trackEvent(userId, "circle_invite_accepted", { circle_id: circle.id, circle_code: code, circle_size: circle.members.length });
  trackEvent(circle.creatorUserId, "circle_member_joined", { circle_id: circle.id, circle_code: code, circle_size: circle.members.length, new_member_name: userName });
  return c.json({ circle });
});

app.put("/api/circles/:code", async (c) => { const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404); const body = await c.req.json(); if (body.name) circle.name = body.name; if (body.emoji) circle.emoji = body.emoji; await saveCircleToDb(circle); return c.json({ circle }); });

app.put("/api/circles/:code/members/:userId/status", async (c) => {
  const code = c.req.param("code").toUpperCase(); const userId = c.req.param("userId"); const body = await c.req.json();
  const circle = getCircle(code); if (!circle) return c.json({ error: "Circle not found" }, 404);
  const member = circle.members.find((m) => m.userId === userId); if (!member) return c.json({ error: "Member not found" }, 404);
  const oldStreak = member.streakCount;
  if (body.streakCount !== undefined) member.streakCount = body.streakCount; if (body.lastPrayedDate !== undefined) member.lastPrayedDate = body.lastPrayedDate; if (body.name !== undefined) member.name = body.name;
  await saveCircleToDb(circle);
  if (body.streakCount !== undefined && body.streakCount > oldStreak && [3,7,14,30,60,90,180,365].includes(body.streakCount)) trackEvent(userId, "streak_milestone", { streak_count: body.streakCount, circle_code: code });
  return c.json({ circle });
});

app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase(); const userId = c.req.param("userId");
  const circle = getCircle(code); if (!circle) return c.json({ error: "Circle not found" }, 404);
  circle.members = circle.members.filter((m) => m.userId !== userId);
  trackEvent(userId, "circle_left", { circle_code: code, remaining_members: circle.members.length });
  if (circle.members.length === 0) await deleteCircleFromDb(code); else await saveCircleToDb(circle);
  return c.json({ success: true });
});

app.delete("/api/circles/:code", async (c) => {
  const code = c.req.param("code").toUpperCase(); const circle = getCircle(code); if (!circle) return c.json({ error: "Circle not found" }, 404);
  trackEvent(circle.creatorUserId, "circle_deleted", { circle_code: code, member_count: circle.members.length });
  await deleteCircleFromDb(code); return c.json({ success: true });
});

// ─── Prayer Requests ─────────────────────────────────────────────────

app.post("/api/circles/:code/prayer-requests", async (c) => {
  const code = c.req.param("code").toUpperCase(); const body = await c.req.json(); const circle = getCircle(code); if (!circle) return c.json({ error: "Circle not found" }, 404);
  circle.prayerRequests.unshift({ id: randomUUID(), requesterUserId: body.userId, requesterName: body.isAnonymous ? "Anonymous" : body.userName || "Someone", text: body.text, timestamp: new Date().toISOString(), isAnonymous: body.isAnonymous || false, prayedByUserIds: [] });
  await saveCircleToDb(circle); trackEvent(body.userId, "prayer_request_created", { circle_code: code, is_anonymous: body.isAnonymous || false, word_count: (body.text || "").split(/\s+/).length });
  return c.json({ circle });
});

app.post("/api/circles/:code/prayer-requests/:requestId/pray", async (c) => {
  const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404);
  const request = circle.prayerRequests.find((r) => r.id === c.req.param("requestId")); if (!request) return c.json({ error: "Prayer request not found" }, 404);
  const body = await c.req.json(); if (!request.prayedByUserIds.includes(body.userId)) { request.prayedByUserIds.push(body.userId); trackEvent(body.userId, "prayer_request_prayed", { circle_code: c.req.param("code").toUpperCase(), request_id: c.req.param("requestId"), total_prayers: request.prayedByUserIds.length }); }
  await saveCircleToDb(circle); return c.json({ circle });
});

app.delete("/api/circles/:code/prayer-requests/:requestId", async (c) => {
  const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404);
  const before = circle.prayerRequests.length; circle.prayerRequests = circle.prayerRequests.filter((r) => r.id !== c.req.param("requestId"));
  if (circle.prayerRequests.length === before) return c.json({ error: "Not found" }, 404);
  await saveCircleToDb(circle); return c.json({ success: true });
});

// ─── Encouragements ──────────────────────────────────────────────────

app.post("/api/circles/:code/encouragements", async (c) => {
  const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404);
  const body = await c.req.json();
  circle.encouragements.push({ id: randomUUID(), toUserId: body.toUserId, fromUserId: body.fromUserId, fromName: body.fromName || "Someone", message: body.message, timestamp: new Date().toISOString() });
  await saveCircleToDb(circle); trackEvent(body.fromUserId, "encouragement_sent", { circle_code: c.req.param("code").toUpperCase(), to_user_id: body.toUserId });
  return c.json({ circle });
});

app.get("/api/circles/:code/info", (c) => {
  const circle = getCircle(c.req.param("code")); if (!circle) return c.json({ error: "Circle not found" }, 404);
  const creator = circle.members.find((m) => m.userId === circle.creatorUserId);
  return c.json({ name: circle.name, emoji: circle.emoji, memberCount: circle.members.length, creatorName: creator?.name || null });
});

// ═══════════════════════════════════════════════════════════════════
// ─── DASHBOARD API ──────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════

app.get("/api/dashboard", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  try {
    const userCount = await pool.query("SELECT COUNT(*) as count FROM users");
    let totalMembers = 0, totalEncouragements = 0, totalPrayerRequests = 0;
    for (const [, ci] of circles) { totalMembers += ci.members.length; totalEncouragements += ci.encouragements.length; totalPrayerRequests += ci.prayerRequests.length; }
    const revenueData = await pool.query(`SELECT * FROM daily_revenue WHERE date >= CURRENT_DATE - INTERVAL '30 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const productData = await pool.query(`SELECT * FROM daily_product_metrics WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const webData = await pool.query(`SELECT * FROM daily_web_metrics WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const appStoreData = await pool.query(`SELECT * FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const recentRevenue = await pool.query(`SELECT * FROM revenue_events ORDER BY created_at DESC LIMIT 20`).catch(() => ({ rows: [] }));
    const subStatus = await pool.query(`SELECT subscription_status, COUNT(*) as count FROM users GROUP BY subscription_status`).catch(() => ({ rows: [] }));
    const statusBreakdown: Record<string, number> = {}; for (const row of subStatus.rows) statusBreakdown[row.subscription_status || "none"] = parseInt(row.count);
    const totalRevenueGross = revenueData.rows.reduce((s: number, r: any) => s + (r.revenue_gross||0), 0);
    const totalRevenueNet = revenueData.rows.reduce((s: number, r: any) => s + (r.revenue_net||0), 0);
    const totalVisitors = webData.rows.reduce((s: number, r: any) => s + (r.visitors||0), 0);
    const totalClicks = webData.rows.reduce((s: number, r: any) => s + (r.app_store_clicks||0), 0);
    return c.json({
      generated_at: new Date().toISOString(),
      kpis: { total_users: parseInt(userCount.rows[0]?.count||"0"), active_subscribers: (statusBreakdown["active"]||0)+(statusBreakdown["lifetime"]||0), mrr_net: totalRevenueNet, revenue_gross_30d: totalRevenueGross, revenue_net_30d: totalRevenueNet, active_circles: circles.size, total_circle_members: totalMembers, landing_visitors_7d: totalVisitors, landing_app_store_clicks_7d: totalClicks, landing_conversion: totalVisitors > 0 ? ((totalClicks/totalVisitors)*100).toFixed(1)+"%" : "0%" },
      subscription_breakdown: statusBreakdown,
      revenue: { daily: revenueData.rows, recent_events: recentRevenue.rows, total_subscribers_30d: revenueData.rows.reduce((s: number, r: any) => s+(r.new_subscribers||0), 0), total_cancellations_30d: revenueData.rows.reduce((s: number, r: any) => s+(r.cancellations||0), 0) },
      product: { daily: productData.rows }, web: { daily: webData.rows }, app_store: { daily: appStoreData.rows },
      circles: { total: circles.size, total_members: totalMembers, total_encouragements: totalEncouragements, total_prayer_requests: totalPrayerRequests, circles: Array.from(circles.values()).map((ci) => ({ name: ci.name, code: ci.code, members: ci.members.length, prayerRequests: ci.prayerRequests.length, encouragements: ci.encouragements.length, createdAt: ci.createdAt })) },
    });
  } catch (err: any) { return c.json({ error: "Dashboard query failed", detail: err.message }, 500); }
});

// Serve dashboard HTML
app.get("/dashboard", (c) => {
  const secret = c.req.query("key");
  if (secret !== DASHBOARD_SECRET) return c.text("Unauthorized. Access: /dashboard?key=YOUR_KEY", 401);
  try { const html = readFileSync("./dashboard.html", "utf-8"); return c.html(html); } catch { return c.text("Dashboard HTML not found", 404); }
});

// ─── Start Server ────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3000", 10);

async function start() {
  await initDb();
  await loadAllFromDb();
  pullPlausibleMetrics().catch(() => {});
  setInterval(() => { pullPlausibleMetrics().catch(() => {}); }, 60 * 60 * 1000);
  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v2.1 running on port ${info.port}`);
    console.log(`   Storage: PostgreSQL | Auth: Apple+Google`);
    console.log(`   PostHog: ${POSTHOG_API_KEY ? "✓" : "✗"} | Plausible: ${PLAUSIBLE_API_KEY ? "✓" : "✗"}`);
    console.log(`   Dashboard: /dashboard?key=...`);
    console.log(`   Circles: ${circles.size} loaded\n`);
  });
}

start();
