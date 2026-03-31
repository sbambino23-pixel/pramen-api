import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomUUID, createHash, createSign } from "crypto";
import { readFileSync } from "fs";
import pg from "pg";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────
interface StoredMember { userId: string; name: string; streakCount: number; lastPrayedDate: string | null; joinedAt: string; }
interface StoredPrayerRequest { id: string; requesterUserId: string; requesterName: string; text: string; timestamp: string; isAnonymous: boolean; prayedByUserIds: string[]; }
interface StoredEncouragement { id: string; toUserId: string; fromUserId: string; fromName: string; message: string; timestamp: string; }
interface StoredCircle { id: string; name: string; code: string; emoji: string; creatorUserId: string; members: StoredMember[]; prayerRequests: StoredPrayerRequest[]; encouragements: StoredEncouragement[]; createdAt: string; }

// ─── Config ──────────────────────────────────────────────────────────
const POSTHOG_API_KEY = process.env.POSTHOG_API_KEY || "";
const POSTHOG_HOST = process.env.POSTHOG_HOST || "https://us.i.posthog.com";
const POSTHOG_READ_HOST = "https://us.posthog.com"; // Read API is different from ingestion!
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
    const signature = signer.sign(ASC_PRIVATE_KEY, "base64url");
    return `${header}.${payload}.${signature}`;
  } catch (err: any) { console.error("[ASC] JWT error:", err.message); return ""; }
}

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
    console.log("DB initialized (circles + users + analytics)");
  } catch (err) { console.error("DB init failed:", err); } finally { client.release(); }
}

// ─── Circle Cache ────────────────────────────────────────────────────
const circles = new Map<string, StoredCircle>();
async function loadAllFromDb(): Promise<void> { try { const r = await pool.query("SELECT code, data FROM circles"); for (const row of r.rows) circles.set(row.code, row.data as StoredCircle); console.log(`Loaded ${circles.size} circles`); } catch (err) { console.error("Load circles:", err); } }
async function saveCircleToDb(circle: StoredCircle): Promise<void> { const k = circle.code.toUpperCase(); circles.set(k, circle); try { await pool.query(`INSERT INTO circles (code,data,updated_at) VALUES ($1,$2,NOW()) ON CONFLICT (code) DO UPDATE SET data=$2,updated_at=NOW()`, [k, JSON.stringify(circle)]); } catch (err) { console.error("Save circle:", err); } }
async function deleteCircleFromDb(code: string): Promise<boolean> { const k = code.toUpperCase(); const e = circles.delete(k); try { await pool.query("DELETE FROM circles WHERE code=$1", [k]); } catch {} return e; }
function getCircle(code: string): StoredCircle | undefined { return circles.get(code.toUpperCase()); }
function generateCode(): string { const ch = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; let c = ""; for (let i = 0; i < 6; i++) c += ch[Math.floor(Math.random() * ch.length)]; if (circles.has(c)) return generateCode(); return c; }

// ─── Auth Helpers ────────────────────────────────────────────────────
function generateAuthToken(): string { return randomUUID() + "-" + randomUUID(); }
async function getUserByToken(token: string) { if (!token) return null; try { const r = await pool.query("SELECT * FROM users WHERE auth_token=$1", [token]); return r.rows[0] || null; } catch { return null; } }
async function getUserByAppleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE apple_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByGoogleId(id: string) { try { const r = await pool.query("SELECT * FROM users WHERE google_user_id=$1", [id]); return r.rows[0] || null; } catch { return null; } }
async function getUserByEmail(email: string) { try { const r = await pool.query("SELECT * FROM users WHERE email=$1", [email]); return r.rows[0] || null; } catch { return null; } }
async function getUserData(userId: string) { try { const r = await pool.query("SELECT * FROM user_data WHERE user_id=$1", [userId]); if (r.rows[0]) { const d = r.rows[0]; return { streakCount: d.streak_count, highestStreak: d.highest_streak, totalPrayers: d.total_prayers, totalMinutes: d.total_minutes, lastPrayedDate: d.last_prayed_date, sessions: d.sessions || [], preferences: d.preferences || {}, circleCodes: d.circle_codes || [] }; } return null; } catch { return null; } }
function getUserCircleCodes(userId: string): string[] { const codes: string[] = []; for (const [code, circle] of circles) { if (circle.members.some(m => m.userId === userId)) codes.push(code); } return codes; }
async function migrateCircleMembership(oldId: string, newId: string, name: string) { for (const [, c] of circles) { const m = c.members.find(m => m.userId === oldId); if (m) { m.userId = newId; if (name) m.name = name; await saveCircleToDb(c); } if (c.creatorUserId === oldId) { c.creatorUserId = newId; await saveCircleToDb(c); } } }

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

// ─── Hono App ────────────────────────────────────────────────────────
const app = new Hono();
app.use("*", cors());
app.onError((err, c) => { console.error("Error:", err); return c.json({ error: "Internal error", detail: err.message }, 500); });

app.get("/", (c) => c.json({ status: "ok", service: "prAmen API", version: "2.2.0", circles: circles.size, posthog: !!POSTHOG_API_KEY, posthog_read: !!POSTHOG_PERSONAL_KEY, plausible: !!PLAUSIBLE_API_KEY, apple: !!ASC_KEY_ID, dashboard: "/dashboard?key=..." }));
app.get("/api/circles/health", (c) => c.json({ status: "ok", circles: circles.size }));

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
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.device_user_id || user.id) });
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
    return c.json({ user: { id: user.id, name: user.name, email: user.email, authToken: user.auth_token, trialStartDate: user.trial_start_date, trialEndDate: user.trial_end_date, subscriptionStatus: user.subscription_status, isNewUser }, data: await getUserData(user.id), circleCodes: getUserCircleCodes(user.device_user_id || user.id) });
  } catch (e: any) { return c.json({ error: "Auth failed", detail: e.message }, 500); }
});

app.put("/api/user/email-opt-in", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { optIn } = await c.req.json(); await pool.query("UPDATE users SET email_opt_in=$1,updated_at=NOW() WHERE id=$2", [!!optIn, u.id]); if (optIn) trackEvent(u.id, "email_opt_in", { email: u.email }); return c.json({ success: true }); });
app.get("/api/admin/email-list", async (c) => { if (c.req.header("X-Admin-Secret") !== process.env.ADMIN_SECRET) return c.json({ error: "Forbidden" }, 403); const r = await pool.query("SELECT email,name,auth_provider,created_at FROM users WHERE email_opt_in=true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"); return c.json({ count: r.rows.length, emails: r.rows }); });
app.post("/api/auth/verify", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ valid: false }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ valid: false }, 401); return c.json({ valid: true, user: { id: u.id, name: u.name, email: u.email, authToken: u.auth_token, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.device_user_id || u.id) }); });
app.post("/api/auth/logout", async (c) => { const ah = c.req.header("Authorization"); if (!ah) return c.json({ success: true }); await pool.query("UPDATE users SET auth_token=$1,updated_at=NOW() WHERE auth_token=$2", [generateAuthToken(), ah.replace("Bearer ", "")]); return c.json({ success: true }); });
app.delete("/api/auth/account", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Not found" }, 404); await pool.query("DELETE FROM user_data WHERE user_id=$1", [u.id]); await pool.query("DELETE FROM users WHERE id=$1", [u.id]); trackEvent(u.id, "account_deleted", {}); return c.json({ success: true }); });

// ═══════════════════════════════════════════════════════════════════
// ─── DATA SYNC ──────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); return c.json({ user: { id: u.id, name: u.name, trialStartDate: u.trial_start_date, trialEndDate: u.trial_end_date, subscriptionStatus: u.subscription_status }, data: await getUserData(u.id), circleCodes: getUserCircleCodes(u.device_user_id || u.id) }); });
app.put("/api/user/data", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const b = await c.req.json(); try { await pool.query(`INSERT INTO user_data (user_id,streak_count,highest_streak,total_prayers,total_minutes,last_prayed_date,sessions,preferences,circle_codes,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW()) ON CONFLICT (user_id) DO UPDATE SET streak_count=GREATEST(user_data.streak_count,$2),highest_streak=GREATEST(user_data.highest_streak,$3),total_prayers=GREATEST(user_data.total_prayers,$4),total_minutes=GREATEST(user_data.total_minutes,$5),last_prayed_date=GREATEST(user_data.last_prayed_date,$6),sessions=CASE WHEN jsonb_array_length($7::jsonb)>jsonb_array_length(user_data.sessions) THEN $7 ELSE user_data.sessions END,preferences=$8,circle_codes=$9,updated_at=NOW()`, [u.id, b.streakCount||0, b.highestStreak||0, b.totalPrayers||0, b.totalMinutes||0, b.lastPrayedDate||null, JSON.stringify(b.sessions||[]), JSON.stringify(b.preferences||{}), b.circleCodes||[]]); if (b.userName && b.userName !== u.name) await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [b.userName, u.id]); return c.json({ status: "ok", synced: true }); } catch (e: any) { return c.json({ error: "Sync failed", detail: e.message }, 500); } });
app.put("/api/user/name", async (c) => { const ah = c.req.header("Authorization"); if (!ah?.startsWith("Bearer ")) return c.json({ error: "Unauthorized" }, 401); const u = await getUserByToken(ah.replace("Bearer ", "")); if (!u) return c.json({ error: "Unauthorized" }, 401); const { name } = await c.req.json(); if (!name?.trim()) return c.json({ error: "Name required" }, 400); await pool.query("UPDATE users SET name=$1,updated_at=NOW() WHERE id=$2", [name.trim(), u.id]); for (const [, ci] of circles) { const m = ci.members.find(m => m.userId === u.id || m.userId === u.device_user_id); if (m) { m.name = name.trim(); await saveCircleToDb(ci); } } return c.json({ success: true, name: name.trim() }); });

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
    const uid = ev.app_user_id; const pid = ev.product_id || ""; const plan = rcPlan(pid); const status = rcStatus(ev.type);
    await pool.query("UPDATE users SET subscription_status=$1,updated_at=NOW() WHERE id=$2 OR device_user_id=$2", [status, uid]).catch(() => {});
    trackEvent(uid, name, { plan, product_id: pid, price: ev.price, currency: ev.currency, store: ev.store, environment: ev.environment, $revenue: ev.price || 0, $currency: ev.currency || "USD" });
    identifyUser(uid, { subscription_status: status, subscription_plan: plan, last_revenue_event: name });
    try {
      const price = ev.price || 0; const net = price * (1 - APPLE_CUT); const today = new Date().toISOString().split("T")[0];
      await pool.query(`INSERT INTO revenue_events (user_id,event_type,plan,product_id,price,currency,environment) VALUES ($1,$2,$3,$4,$5,$6,$7)`, [uid, name, plan, pid, price, ev.currency||"USD", ev.environment||"production"]);
      if (name === "subscription_started" || name === "lifetime_purchased") await pool.query(`INSERT INTO daily_revenue (date,new_subscribers,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET new_subscribers=daily_revenue.new_subscribers+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_renewed") await pool.query(`INSERT INTO daily_revenue (date,renewals,revenue_gross,revenue_net) VALUES ($1,1,$2,$3) ON CONFLICT (date) DO UPDATE SET renewals=daily_revenue.renewals+1,revenue_gross=daily_revenue.revenue_gross+$2,revenue_net=daily_revenue.revenue_net+$3,updated_at=NOW()`, [today, price, net]);
      else if (name === "subscription_cancelled" || name === "subscription_expired") await pool.query(`INSERT INTO daily_revenue (date,cancellations) VALUES ($1,1) ON CONFLICT (date) DO UPDATE SET cancellations=daily_revenue.cancellations+1,updated_at=NOW()`, [today]);
    } catch (e: any) { console.error("[Revenue]", e.message); }
    console.log(`[RC] ${ev.type} → ${name} | ${uid} ${plan}`); return c.json({ status: "ok" });
  } catch (e) { console.error("[RC]", e); return c.json({ error: "Error" }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── CIRCLES ────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.post("/api/circles", async (c) => { const b = await c.req.json(); if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const code = generateCode(); const ci: StoredCircle = { id: randomUUID(), name: b.name || "Prayer Circle", code, emoji: b.emoji || "🏠", creatorUserId: b.userId, members: [{ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString() }], prayerRequests: [], encouragements: [], createdAt: new Date().toISOString() }; await saveCircleToDb(ci); trackEvent(b.userId, "circle_created", { circle_id: ci.id, circle_code: code, circle_name: ci.name }); return c.json({ circle: ci }, 201); });
app.get("/api/circles/:code", (c) => { const ci = getCircle(c.req.param("code")); return ci ? c.json({ circle: ci }) : c.json({ error: "Not found" }, 404); });
app.post("/api/circles/:code/join", async (c) => { const code = c.req.param("code").toUpperCase(); let b; try { b = await c.req.json(); } catch { return c.json({ error: "Invalid body" }, 400); } if (!b.userId || !b.userName) return c.json({ error: "userId and userName required" }, 400); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); if (ci.members.find(m => m.userId === b.userId)) return c.json({ circle: ci }); ci.members.push({ userId: b.userId, name: b.userName, streakCount: b.streakCount||0, lastPrayedDate: b.lastPrayedDate||null, joinedAt: new Date().toISOString() }); await saveCircleToDb(ci); trackEvent(b.userId, "circle_invite_accepted", { circle_code: code, circle_size: ci.members.length }); trackEvent(ci.creatorUserId, "circle_member_joined", { circle_code: code, circle_size: ci.members.length, new_member_name: b.userName }); return c.json({ circle: ci }); });
app.put("/api/circles/:code", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); if (b.name) ci.name = b.name; if (b.emoji) ci.emoji = b.emoji; await saveCircleToDb(ci); return c.json({ circle: ci }); });
app.put("/api/circles/:code/members/:userId/status", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const m = ci.members.find(m => m.userId === c.req.param("userId")); if (!m) return c.json({ error: "Member not found" }, 404); const b = await c.req.json(); const old = m.streakCount; if (b.streakCount !== undefined) m.streakCount = b.streakCount; if (b.lastPrayedDate !== undefined) m.lastPrayedDate = b.lastPrayedDate; if (b.name !== undefined) m.name = b.name; await saveCircleToDb(ci); if (b.streakCount !== undefined && b.streakCount > old && [3,7,14,30,60,90,180,365].includes(b.streakCount)) trackEvent(c.req.param("userId"), "streak_milestone", { streak_count: b.streakCount, circle_code: c.req.param("code").toUpperCase() }); return c.json({ circle: ci }); });
app.delete("/api/circles/:code/members/:userId", async (c) => { const code = c.req.param("code").toUpperCase(); const uid = c.req.param("userId"); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); ci.members = ci.members.filter(m => m.userId !== uid); trackEvent(uid, "circle_left", { circle_code: code }); ci.members.length === 0 ? await deleteCircleFromDb(code) : await saveCircleToDb(ci); return c.json({ success: true }); });
app.delete("/api/circles/:code", async (c) => { const code = c.req.param("code").toUpperCase(); const ci = getCircle(code); if (!ci) return c.json({ error: "Not found" }, 404); trackEvent(ci.creatorUserId, "circle_deleted", { circle_code: code }); await deleteCircleFromDb(code); return c.json({ success: true }); });
app.post("/api/circles/:code/prayer-requests", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); ci.prayerRequests.unshift({ id: randomUUID(), requesterUserId: b.userId, requesterName: b.isAnonymous ? "Anonymous" : b.userName || "Someone", text: b.text, timestamp: new Date().toISOString(), isAnonymous: b.isAnonymous || false, prayedByUserIds: [] }); await saveCircleToDb(ci); trackEvent(b.userId, "prayer_request_created", { circle_code: c.req.param("code").toUpperCase(), is_anonymous: b.isAnonymous || false }); return c.json({ circle: ci }); });
app.post("/api/circles/:code/prayer-requests/:rid/pray", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const req = ci.prayerRequests.find(r => r.id === c.req.param("rid")); if (!req) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); if (!req.prayedByUserIds.includes(b.userId)) { req.prayedByUserIds.push(b.userId); trackEvent(b.userId, "prayer_request_prayed", { circle_code: c.req.param("code").toUpperCase() }); } await saveCircleToDb(ci); return c.json({ circle: ci }); });
app.delete("/api/circles/:code/prayer-requests/:rid", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const before = ci.prayerRequests.length; ci.prayerRequests = ci.prayerRequests.filter(r => r.id !== c.req.param("rid")); if (ci.prayerRequests.length === before) return c.json({ error: "Not found" }, 404); await saveCircleToDb(ci); return c.json({ success: true }); });
app.post("/api/circles/:code/encouragements", async (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const b = await c.req.json(); ci.encouragements.push({ id: randomUUID(), toUserId: b.toUserId, fromUserId: b.fromUserId, fromName: b.fromName || "Someone", message: b.message, timestamp: new Date().toISOString() }); await saveCircleToDb(ci); trackEvent(b.fromUserId, "encouragement_sent", { circle_code: c.req.param("code").toUpperCase(), to_user_id: b.toUserId }); return c.json({ circle: ci }); });
app.get("/api/circles/:code/info", (c) => { const ci = getCircle(c.req.param("code")); if (!ci) return c.json({ error: "Not found" }, 404); const cr = ci.members.find(m => m.userId === ci.creatorUserId); return c.json({ name: ci.name, emoji: ci.emoji, memberCount: ci.members.length, creatorName: cr?.name || null }); });

// ═══════════════════════════════════════════════════════════════════
// ─── POSTHOG EVENTS (uses us.posthog.com NOT us.i.posthog.com) ──
// ═══════════════════════════════════════════════════════════════════
app.get("/api/dashboard/events", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  if (!POSTHOG_PERSONAL_KEY) return c.json({ error: "POSTHOG_PERSONAL_API_KEY not set" }, 500);
  try {
    const limit = parseInt(c.req.query("limit") || "500");
    const daysBack = parseInt(c.req.query("days") || "14");
    const afterDate = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000).toISOString();
    const res = await fetch(`${POSTHOG_READ_HOST}/api/projects/${POSTHOG_PROJECT_ID}/events/?limit=${limit}&after=${afterDate}`
    if (!res.ok) { const txt = await res.text().catch(() => ""); console.error("[PostHog Read]", res.status, txt.substring(0,200)); return c.json({ error: "PostHog " + res.status, detail: txt.substring(0,200) }, 500); }
    const raw = ((await res.json()) as any).results || [];
    const exclude = new Set(["samy_setup", "samy_test"]);
    const events = raw.filter((e: any) => !exclude.has(e.distinct_id)).map((e: any) => ({
      event: e.event, timestamp: e.timestamp, user: e.distinct_id?.substring(0, 8) || "?", full_user_id: e.distinct_id,
      properties: { type: e.properties?.type, plan: e.properties?.plan, price: e.properties?.price, trigger: e.properties?.trigger, duration: e.properties?.duration_seconds, streak: e.properties?.streak_day, is_first_open: e.properties?.is_first_open, circle_code: e.properties?.circle_code, circle_name: e.properties?.circle_name, content_type: e.properties?.content_type, city: e.properties?.$geoip_city_name || e.properties?.$set?.$geoip_city_name, country: e.properties?.$geoip_country_name || e.properties?.$set?.$geoip_country_name }
    }));
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
    return c.json({ generated_at: new Date().toISOString(), total_events: events.length, total_users: users.length, event_counts: Object.entries(ec).sort((a, b) => b[1] - a[1]), funnel: { first_open: fn.first_open.size, onboarding: fn.onboarding.size, paywall: fn.paywall.size, plan_tap: fn.plan_tap.size, prayer: fn.prayer.size, circle: fn.circle.size, signup: fn.signup.size, scripture: fn.scripture.size }, prayer_topics: Object.entries(topics).sort((a, b) => b[1] - a[1]), plan_taps: Object.entries(plans).sort((a, b) => b[1] - a[1]), daily_dau: dau, users: users.map((u: any) => ({ id: u.id, full_id: u.full_id, event_count: u.events.length, event_types: u.counts, first_seen: u.first_seen, last_seen: u.last_seen, city: u.city, country: u.country, max_streak: u.max_streak, plan_taps: u.plan_taps })), recent_events: events.filter((e: any) => e.event !== "$identify").slice(0, 200) });
  } catch (e: any) { console.error("[PostHog]", e); return c.json({ error: "PostHog failed", detail: e.message }, 500); }
});

// ═══════════════════════════════════════════════════════════════════
// ─── APPLE APP STORE ────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
app.get("/api/dashboard/appstore", async (c) => {
  const secret = c.req.query("key") || c.req.header("X-Dashboard-Key");
  if (secret !== DASHBOARD_SECRET) return c.json({ error: "Unauthorized" }, 401);
  const token = generateASCToken();
  if (!token) return c.json({ connected: false, error: "Apple API not configured — check ASC_KEY_ID, ASC_ISSUER_ID, ASC_PRIVATE_KEY env vars" });
  try {
    const r = await fetch(`https://api.appstoreconnect.apple.com/v1/apps/${PRAMEN_APP_ID}`, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) { const t = await r.text().catch(() => ""); return c.json({ connected: false, error: `Apple API ${r.status}: ${t.substring(0,200)}` }); }
    const d = (await r.json()) as any;
    return c.json({ connected: true, app: { name: d.data?.attributes?.name, bundleId: d.data?.attributes?.bundleId, sku: d.data?.attributes?.sku }, timestamp: new Date().toISOString() });
  } catch (e: any) { return c.json({ connected: false, error: e.message }); }
});

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
    const wd = await pool.query(`SELECT * FROM daily_web_metrics WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const ad = await pool.query(`SELECT * FROM daily_app_store_metrics WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC`).catch(() => ({ rows: [] }));
    const re = await pool.query(`SELECT * FROM revenue_events ORDER BY created_at DESC LIMIT 20`).catch(() => ({ rows: [] }));
    const ss = await pool.query(`SELECT subscription_status, COUNT(*) as count FROM users GROUP BY subscription_status`).catch(() => ({ rows: [] }));
    const sb: Record<string,number> = {}; for (const r of ss.rows) sb[r.subscription_status || "none"] = parseInt(r.count);
    const tg = rv.rows.reduce((s: number, r: any) => s + (r.revenue_gross||0), 0); const tn = rv.rows.reduce((s: number, r: any) => s + (r.revenue_net||0), 0);
    const tv = wd.rows.reduce((s: number, r: any) => s + (r.visitors||0), 0); const tc = wd.rows.reduce((s: number, r: any) => s + (r.app_store_clicks||0), 0);
    return c.json({ generated_at: new Date().toISOString(), kpis: { total_users: parseInt(uc.rows[0]?.count||"0"), active_subscribers: (sb["active"]||0)+(sb["lifetime"]||0), mrr_net: tn, revenue_gross_30d: tg, revenue_net_30d: tn, active_circles: circles.size, total_circle_members: tm, landing_visitors_7d: tv, landing_app_store_clicks_7d: tc, landing_conversion: tv > 0 ? ((tc/tv)*100).toFixed(1)+"%" : "0%" }, subscription_breakdown: sb, revenue: { daily: rv.rows, recent_events: re.rows, total_subscribers_30d: rv.rows.reduce((s: number, r: any) => s+(r.new_subscribers||0), 0), total_cancellations_30d: rv.rows.reduce((s: number, r: any) => s+(r.cancellations||0), 0) }, web: { daily: wd.rows }, app_store: { daily: ad.rows }, circles: { total: circles.size, total_members: tm, total_encouragements: te, total_prayer_requests: tp, circles: Array.from(circles.values()).map(ci => ({ name: ci.name, code: ci.code, members: ci.members.length, prayerRequests: ci.prayerRequests.length, encouragements: ci.encouragements.length, createdAt: ci.createdAt })) } });
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
  pullPlausibleMetrics().catch(() => {});
  setInterval(() => { pullPlausibleMetrics().catch(() => {}); }, 60 * 60 * 1000);
  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v2.2 on port ${info.port}`);
    console.log(`   PostHog write: ${POSTHOG_API_KEY ? "✓" : "✗"} | PostHog read: ${POSTHOG_PERSONAL_KEY ? "✓" : "✗"}`);
    console.log(`   Plausible: ${PLAUSIBLE_API_KEY ? "✓" : "✗"} | Apple: ${ASC_KEY_ID ? "✓" : "✗"} (pk ${ASC_PRIVATE_KEY.length} chars)`);
    console.log(`   Dashboard: /dashboard?key=...`);
    console.log(`   Circles: ${circles.size}\n`);
  });
}
start();
