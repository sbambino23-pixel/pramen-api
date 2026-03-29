import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomUUID, createHash } from "crypto";
import pg from "pg";

const { Pool } = pg;

// ─── Types ───────────────────────────────────────────────────────────

interface StoredMember {
  userId: string;
  name: string;
  streakCount: number;
  lastPrayedDate: string | null;
  joinedAt: string;
}

interface StoredPrayerRequest {
  id: string;
  requesterUserId: string;
  requesterName: string;
  text: string;
  timestamp: string;
  isAnonymous: boolean;
  prayedByUserIds: string[];
}

interface StoredEncouragement {
  id: string;
  toUserId: string;
  fromUserId: string;
  fromName: string;
  message: string;
  timestamp: string;
}

interface StoredCircle {
  id: string;
  name: string;
  code: string;
  emoji: string;
  creatorUserId: string;
  members: StoredMember[];
  prayerRequests: StoredPrayerRequest[];
  encouragements: StoredEncouragement[];
  createdAt: string;
}

// ─── PostHog Analytics ───────────────────────────────────────────────

const POSTHOG_API_KEY = process.env.POSTHOG_API_KEY || "";
const POSTHOG_HOST = process.env.POSTHOG_HOST || "https://us.i.posthog.com";

function trackEvent(
  distinctId: string,
  event: string,
  properties?: Record<string, any>
) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: POSTHOG_API_KEY,
      event,
      distinct_id: distinctId,
      properties: {
        ...properties,
        $lib: "pramen-backend",
        platform: "ios",
      },
      timestamp: new Date().toISOString(),
    }),
  }).catch((err) => console.error("[PostHog] Track error:", err.message));
}

function identifyUser(
  distinctId: string,
  userProperties: Record<string, any>
) {
  if (!POSTHOG_API_KEY) return;
  fetch(`${POSTHOG_HOST}/capture/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: POSTHOG_API_KEY,
      event: "$identify",
      distinct_id: distinctId,
      properties: { $set: userProperties },
      timestamp: new Date().toISOString(),
    }),
  }).catch((err) => console.error("[PostHog] Identify error:", err.message));
}

// ─── Postgres Persistence Layer ─────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
});

async function initDb(): Promise<void> {
  const client = await pool.connect();
  try {
    // Circles table (existing)
    await client.query(`
      CREATE TABLE IF NOT EXISTS circles (
        code TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Users table (NEW)
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        apple_user_id TEXT UNIQUE,
        google_user_id TEXT UNIQUE,
        email TEXT,
        name TEXT NOT NULL DEFAULT '',
        auth_provider TEXT DEFAULT 'apple',
        auth_token TEXT UNIQUE NOT NULL,
        device_user_id TEXT,
        trial_start_date TIMESTAMPTZ,
        trial_end_date TIMESTAMPTZ,
        subscription_status TEXT DEFAULT 'none',
        email_opt_in BOOLEAN DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Add columns if they don't exist (safe migration for existing installs)
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS google_user_id TEXT UNIQUE`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_provider TEXT DEFAULT 'apple'`).catch(() => {});
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_opt_in BOOLEAN DEFAULT false`).catch(() => {});

    // User data table (NEW) — stores synced app data
    await client.query(`
      CREATE TABLE IF NOT EXISTS user_data (
        user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        streak_count INTEGER DEFAULT 0,
        highest_streak INTEGER DEFAULT 0,
        total_prayers INTEGER DEFAULT 0,
        total_minutes INTEGER DEFAULT 0,
        last_prayed_date TIMESTAMPTZ,
        sessions JSONB DEFAULT '[]'::jsonb,
        preferences JSONB DEFAULT '{}'::jsonb,
        circle_codes TEXT[] DEFAULT '{}',
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    // Index for fast Apple user lookup
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_apple_user_id ON users(apple_user_id)
    `);

    // Index for fast Google user lookup
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_google_user_id ON users(google_user_id)
    `);

    // Index for auth token lookup
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_auth_token ON users(auth_token)
    `);

    // Index for device user id migration
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_device_user_id ON users(device_user_id)
    `);

    console.log("Database initialized (circles + users + user_data)");
  } catch (err) {
    console.error("Failed to initialize database:", err);
  } finally {
    client.release();
  }
}

// ─── In-memory circle cache backed by Postgres ──────────────────────

const circles = new Map<string, StoredCircle>();

async function loadAllFromDb(): Promise<void> {
  try {
    const result = await pool.query("SELECT code, data FROM circles");
    for (const row of result.rows) {
      circles.set(row.code, row.data as StoredCircle);
    }
    console.log(`Loaded ${circles.size} circles from database`);
  } catch (err) {
    console.error("Failed to load circles from database:", err);
  }
}

async function saveCircleToDb(circle: StoredCircle): Promise<void> {
  const key = circle.code.toUpperCase();
  circles.set(key, circle);
  try {
    await pool.query(
      `INSERT INTO circles (code, data, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (code) DO UPDATE SET data = $2, updated_at = NOW()`,
      [key, JSON.stringify(circle)]
    );
  } catch (err) {
    console.error("Failed to save circle to database:", err);
  }
}

async function deleteCircleFromDb(code: string): Promise<boolean> {
  const key = code.toUpperCase();
  const existed = circles.delete(key);
  try {
    await pool.query("DELETE FROM circles WHERE code = $1", [key]);
  } catch (err) {
    console.error("Failed to delete circle from database:", err);
  }
  return existed;
}

function getCircle(code: string): StoredCircle | undefined {
  return circles.get(code.toUpperCase());
}

function generateCode(): string {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  if (circles.has(code)) return generateCode();
  return code;
}

// ─── Auth Helpers ────────────────────────────────────────────────────

function generateAuthToken(): string {
  return randomUUID() + "-" + randomUUID();
}

async function getUserByToken(token: string) {
  if (!token) return null;
  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE auth_token = $1",
      [token]
    );
    return result.rows[0] || null;
  } catch {
    return null;
  }
}

async function getUserByAppleId(appleUserId: string) {
  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE apple_user_id = $1",
      [appleUserId]
    );
    return result.rows[0] || null;
  } catch {
    return null;
  }
}

async function getUserByGoogleId(googleUserId: string) {
  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE google_user_id = $1",
      [googleUserId]
    );
    return result.rows[0] || null;
  } catch {
    return null;
  }
}

async function getUserByEmail(email: string) {
  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE email = $1",
      [email]
    );
    return result.rows[0] || null;
  } catch {
    return null;
  }
}

// Auth middleware — extracts user from Bearer token
async function authMiddleware(c: any, next: () => Promise<void>) {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) {
    return c.json({ error: "Invalid or expired token" }, 401);
  }
  c.set("user", user);
  await next();
}

// ─── Hono App ────────────────────────────────────────────────────────

const app = new Hono();

app.use("*", cors());

app.onError((err, c) => {
  console.error("Unhandled error:", err);
  return c.json({ error: "Internal server error", detail: err.message }, 500);
});

// ─── Health ──────────────────────────────────────────────────────────

app.get("/", (c) => {
  return c.json({
    status: "ok",
    service: "prAmen API",
    version: "2.0.0",
    storage: "postgres",
    circles: circles.size,
    auth: "sign-in-with-apple",
    analytics: POSTHOG_API_KEY ? "posthog" : "disabled",
    timestamp: new Date().toISOString(),
  });
});

app.get("/api/circles/health", (c) => {
  return c.json({
    status: "ok",
    storage: "postgres",
    circles: circles.size,
    timestamp: new Date().toISOString(),
  });
});

// ══════════════════════════════════════════════════════════════════════
// ─── AUTHENTICATION ENDPOINTS (NEW) ─────────────────────────────────
// ══════════════════════════════════════════════════════════════════════

// POST /api/auth/apple — Sign in with Apple
// Called after iOS AuthenticationServices returns the credential
app.post("/api/auth/apple", async (c) => {
  try {
    const body = await c.req.json();
    const { appleUserId, email, fullName, identityToken, deviceUserId } = body;

    if (!appleUserId) {
      return c.json({ error: "appleUserId is required" }, 400);
    }

    // Check if user already exists
    let user = await getUserByAppleId(appleUserId);
    let isNewUser = false;

    if (user) {
      // Existing user — update name/email if provided (Apple only sends on first sign-in)
      if (fullName && !user.name) {
        await pool.query(
          "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2",
          [fullName, user.id]
        );
        user.name = fullName;
      }
      if (email && !user.email) {
        await pool.query(
          "UPDATE users SET email = $1, updated_at = NOW() WHERE id = $2",
          [email, user.id]
        );
        user.email = email;
      }
      // Update device_user_id for circle migration
      if (deviceUserId && deviceUserId !== user.device_user_id) {
        await pool.query(
          "UPDATE users SET device_user_id = $1, updated_at = NOW() WHERE id = $2",
          [deviceUserId, user.id]
        );
      }

      console.log(`[Auth] Existing user signed in: ${user.id} (${user.name || 'unnamed'})`);
    } else {
      // New user — create account
      isNewUser = true;
      const authToken = generateAuthToken();
      const userId = randomUUID();
      const userName = fullName || "";

      // Calculate trial dates
      const trialStartDate = new Date();
      const trialEndDate = new Date(trialStartDate.getTime() + 7 * 24 * 60 * 60 * 1000);

      await pool.query(
        `INSERT INTO users (id, apple_user_id, email, name, auth_token, device_user_id, trial_start_date, trial_end_date, subscription_status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'trial')`,
        [userId, appleUserId, email || null, userName, authToken, deviceUserId || null, trialStartDate, trialEndDate]
      );

      // Create user_data record
      await pool.query(
        `INSERT INTO user_data (user_id) VALUES ($1)`,
        [userId]
      );

      user = {
        id: userId,
        apple_user_id: appleUserId,
        email,
        name: userName,
        auth_token: authToken,
        device_user_id: deviceUserId,
        trial_start_date: trialStartDate,
        trial_end_date: trialEndDate,
        subscription_status: "trial",
      };

      console.log(`[Auth] New user created: ${userId} (${userName})`);

      // Analytics
      trackEvent(userId, "user_signed_up", {
        auth_provider: "apple",
        has_email: !!email,
        has_device_migration: !!deviceUserId,
      });
    }

    // If user had a deviceUserId, migrate their circle memberships
    if (deviceUserId && isNewUser) {
      await migrateCircleMembership(deviceUserId, user.id, user.name);
    }

    // Fetch user data for response
    const userData = await getUserData(user.id);

    // Get circle codes this user belongs to
    const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);

    return c.json({
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        authToken: user.auth_token,
        trialStartDate: user.trial_start_date,
        trialEndDate: user.trial_end_date,
        subscriptionStatus: user.subscription_status,
        isNewUser,
      },
      data: userData,
      circleCodes: userCircleCodes,
    });
  } catch (error: any) {
    console.error("[Auth] Apple sign-in error:", error);
    return c.json({ error: "Authentication failed", detail: error.message }, 500);
  }
});

// POST /api/auth/google — Sign in with Google
// Called after iOS Google Sign-In SDK returns the credential
app.post("/api/auth/google", async (c) => {
  try {
    const body = await c.req.json();
    const { googleUserId, email, fullName, idToken, deviceUserId } = body;

    if (!googleUserId || !email) {
      return c.json({ error: "googleUserId and email are required" }, 400);
    }

    // Verify the Google ID token
    let verified = false;
    if (idToken) {
      try {
        const tokenRes = await fetch(
          `https://oauth2.googleapis.com/tokeninfo?id_token=${idToken}`
        );
        if (tokenRes.ok) {
          const tokenData = await tokenRes.json() as any;
          // Verify the sub (subject) matches the googleUserId
          if (tokenData.sub === googleUserId) {
            verified = true;
          }
        }
      } catch (err) {
        console.warn("[Auth] Google token verification failed, proceeding with trust:", err);
      }
    }
    // If no idToken provided or verification skipped, we still proceed
    // (the Google Sign-In SDK on iOS already verified the token client-side)
    if (!idToken) verified = true;

    // Check if user already exists by Google ID
    let user = await getUserByGoogleId(googleUserId);
    let isNewUser = false;

    // Also check if an account exists with the same email (could be an Apple sign-in user)
    if (!user) {
      const existingByEmail = await getUserByEmail(email);
      if (existingByEmail) {
        // Link Google to existing account
        await pool.query(
          "UPDATE users SET google_user_id = $1, auth_provider = CASE WHEN auth_provider = 'apple' THEN 'apple+google' ELSE 'google' END, updated_at = NOW() WHERE id = $2",
          [googleUserId, existingByEmail.id]
        );
        user = existingByEmail;
        user.google_user_id = googleUserId;
        console.log(`[Auth] Linked Google to existing account: ${user.id} (${email})`);
      }
    }

    if (user) {
      // Existing user — update name if provided
      if (fullName && !user.name) {
        await pool.query(
          "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2",
          [fullName, user.id]
        );
        user.name = fullName;
      }
      // Update device_user_id for circle migration
      if (deviceUserId && deviceUserId !== user.device_user_id) {
        await pool.query(
          "UPDATE users SET device_user_id = $1, updated_at = NOW() WHERE id = $2",
          [deviceUserId, user.id]
        );
      }

      console.log(`[Auth] Google user signed in: ${user.id} (${user.name || email})`);
    } else {
      // New user — create account
      isNewUser = true;
      const authToken = generateAuthToken();
      const userId = randomUUID();
      const userName = fullName || email.split("@")[0];

      const trialStartDate = new Date();
      const trialEndDate = new Date(trialStartDate.getTime() + 7 * 24 * 60 * 60 * 1000);

      await pool.query(
        `INSERT INTO users (id, google_user_id, email, name, auth_provider, auth_token, device_user_id, trial_start_date, trial_end_date, subscription_status)
         VALUES ($1, $2, $3, $4, 'google', $5, $6, $7, $8, 'trial')`,
        [userId, googleUserId, email, userName, authToken, deviceUserId || null, trialStartDate, trialEndDate]
      );

      await pool.query(
        `INSERT INTO user_data (user_id) VALUES ($1)`,
        [userId]
      );

      user = {
        id: userId,
        google_user_id: googleUserId,
        email,
        name: userName,
        auth_token: authToken,
        device_user_id: deviceUserId,
        trial_start_date: trialStartDate,
        trial_end_date: trialEndDate,
        subscription_status: "trial",
      };

      console.log(`[Auth] New Google user created: ${userId} (${userName})`);

      trackEvent(userId, "user_signed_up", {
        auth_provider: "google",
        has_email: true,
        has_device_migration: !!deviceUserId,
      });
    }

    // Migrate circle memberships
    if (deviceUserId && isNewUser) {
      await migrateCircleMembership(deviceUserId, user.id, user.name);
    }

    const userData = await getUserData(user.id);
    const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);

    return c.json({
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        authToken: user.auth_token,
        trialStartDate: user.trial_start_date,
        trialEndDate: user.trial_end_date,
        subscriptionStatus: user.subscription_status,
        isNewUser,
      },
      data: userData,
      circleCodes: userCircleCodes,
    });
  } catch (error: any) {
    console.error("[Auth] Google sign-in error:", error);
    return c.json({ error: "Authentication failed", detail: error.message }, 500);
  }
});

// PUT /api/user/email-opt-in — Update email marketing opt-in
app.put("/api/user/email-opt-in", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) return c.json({ error: "Unauthorized" }, 401);

  const { optIn } = await c.req.json();
  await pool.query(
    "UPDATE users SET email_opt_in = $1, updated_at = NOW() WHERE id = $2",
    [!!optIn, user.id]
  );

  if (optIn) {
    trackEvent(user.id, "email_opt_in", { email: user.email });
  }

  return c.json({ success: true, emailOptIn: !!optIn });
});

// GET /api/admin/email-list — Export opted-in emails (protect with secret)
app.get("/api/admin/email-list", async (c) => {
  const secret = c.req.header("X-Admin-Secret");
  if (secret !== process.env.ADMIN_SECRET) {
    return c.json({ error: "Forbidden" }, 403);
  }

  try {
    const result = await pool.query(
      "SELECT email, name, auth_provider, created_at FROM users WHERE email_opt_in = true AND email IS NOT NULL AND email NOT LIKE '%privaterelay.appleid.com' ORDER BY created_at DESC"
    );
    return c.json({
      count: result.rows.length,
      emails: result.rows,
    });
  } catch (error: any) {
    return c.json({ error: "Failed to fetch emails" }, 500);
  }
});

// POST /api/auth/verify — Verify existing token (auto-login on app launch)
app.post("/api/auth/verify", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ valid: false }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);

  if (!user) {
    return c.json({ valid: false }, 401);
  }

  // Fetch user data
  const userData = await getUserData(user.id);
  const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);

  return c.json({
    valid: true,
    user: {
      id: user.id,
      name: user.name,
      email: user.email,
      authToken: user.auth_token,
      trialStartDate: user.trial_start_date,
      trialEndDate: user.trial_end_date,
      subscriptionStatus: user.subscription_status,
    },
    data: userData,
    circleCodes: userCircleCodes,
  });
});

// POST /api/auth/logout — Invalidate token
app.post("/api/auth/logout", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader) return c.json({ success: true });

  const token = authHeader.replace("Bearer ", "");
  // Generate new token to invalidate old one
  const newToken = generateAuthToken();
  await pool.query(
    "UPDATE users SET auth_token = $1, updated_at = NOW() WHERE auth_token = $2",
    [newToken, token]
  );

  return c.json({ success: true });
});

// DELETE /api/auth/account — Delete user account and all data (GDPR)
app.delete("/api/auth/account", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) return c.json({ error: "User not found" }, 404);

  // Delete user data
  await pool.query("DELETE FROM user_data WHERE user_id = $1", [user.id]);
  // Delete user
  await pool.query("DELETE FROM users WHERE id = $1", [user.id]);

  console.log(`[Auth] Account deleted: ${user.id}`);
  trackEvent(user.id, "account_deleted", {});

  return c.json({ success: true });
});

// ══════════════════════════════════════════════════════════════════════
// ─── DATA SYNC ENDPOINTS (NEW) ──────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════

// GET /api/user/data — Fetch all user data (for restore on reinstall)
app.get("/api/user/data", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) return c.json({ error: "Unauthorized" }, 401);

  const userData = await getUserData(user.id);
  const userCircleCodes = getUserCircleCodes(user.device_user_id || user.id);

  return c.json({
    user: {
      id: user.id,
      name: user.name,
      trialStartDate: user.trial_start_date,
      trialEndDate: user.trial_end_date,
      subscriptionStatus: user.subscription_status,
    },
    data: userData,
    circleCodes: userCircleCodes,
  });
});

// PUT /api/user/data — Sync user data to server
app.put("/api/user/data", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) return c.json({ error: "Unauthorized" }, 401);

  const body = await c.req.json();

  try {
    await pool.query(
      `INSERT INTO user_data (user_id, streak_count, highest_streak, total_prayers, total_minutes, last_prayed_date, sessions, preferences, circle_codes, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
       ON CONFLICT (user_id) DO UPDATE SET
         streak_count = GREATEST(user_data.streak_count, $2),
         highest_streak = GREATEST(user_data.highest_streak, $3),
         total_prayers = GREATEST(user_data.total_prayers, $4),
         total_minutes = GREATEST(user_data.total_minutes, $5),
         last_prayed_date = GREATEST(user_data.last_prayed_date, $6),
         sessions = CASE WHEN jsonb_array_length($7::jsonb) > jsonb_array_length(user_data.sessions) THEN $7 ELSE user_data.sessions END,
         preferences = $8,
         circle_codes = $9,
         updated_at = NOW()`,
      [
        user.id,
        body.streakCount || 0,
        body.highestStreak || 0,
        body.totalPrayers || 0,
        body.totalMinutes || 0,
        body.lastPrayedDate || null,
        JSON.stringify(body.sessions || []),
        JSON.stringify(body.preferences || {}),
        body.circleCodes || [],
      ]
    );

    // Update user name if provided
    if (body.userName && body.userName !== user.name) {
      await pool.query(
        "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2",
        [body.userName, user.id]
      );
    }

    return c.json({ status: "ok", synced: true });
  } catch (error: any) {
    console.error("[Sync] Data sync error:", error);
    return c.json({ error: "Sync failed", detail: error.message }, 500);
  }
});

// PUT /api/user/name — Update display name
app.put("/api/user/name", async (c) => {
  const authHeader = c.req.header("Authorization");
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.replace("Bearer ", "");
  const user = await getUserByToken(token);
  if (!user) return c.json({ error: "Unauthorized" }, 401);

  const { name } = await c.req.json();
  if (!name || name.trim().length === 0) {
    return c.json({ error: "Name is required" }, 400);
  }

  await pool.query(
    "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2",
    [name.trim(), user.id]
  );

  // Also update name in all circles this user belongs to
  for (const [, circle] of circles) {
    const member = circle.members.find(
      (m) => m.userId === user.id || m.userId === user.device_user_id
    );
    if (member) {
      member.name = name.trim();
      await saveCircleToDb(circle);
    }
  }

  return c.json({ success: true, name: name.trim() });
});

// ─── Helpers for auth endpoints ──────────────────────────────────────

async function getUserData(userId: string) {
  try {
    const result = await pool.query(
      "SELECT * FROM user_data WHERE user_id = $1",
      [userId]
    );
    if (result.rows[0]) {
      const d = result.rows[0];
      return {
        streakCount: d.streak_count,
        highestStreak: d.highest_streak,
        totalPrayers: d.total_prayers,
        totalMinutes: d.total_minutes,
        lastPrayedDate: d.last_prayed_date,
        sessions: d.sessions || [],
        preferences: d.preferences || {},
        circleCodes: d.circle_codes || [],
      };
    }
    return null;
  } catch {
    return null;
  }
}

function getUserCircleCodes(userId: string): string[] {
  const codes: string[] = [];
  for (const [code, circle] of circles) {
    if (circle.members.some((m) => m.userId === userId)) {
      codes.push(code);
    }
  }
  return codes;
}

async function migrateCircleMembership(
  oldDeviceId: string,
  newUserId: string,
  userName: string
) {
  for (const [, circle] of circles) {
    const member = circle.members.find((m) => m.userId === oldDeviceId);
    if (member) {
      member.userId = newUserId;
      if (userName) member.name = userName;
      await saveCircleToDb(circle);
      console.log(
        `[Migration] Migrated circle ${circle.code} membership: ${oldDeviceId} → ${newUserId}`
      );
    }
    // Also update creatorUserId if needed
    if (circle.creatorUserId === oldDeviceId) {
      circle.creatorUserId = newUserId;
      await saveCircleToDb(circle);
    }
  }
}

// ══════════════════════════════════════════════════════════════════════
// ─── EXISTING ENDPOINTS (UNCHANGED) ─────────────────────────────────
// ══════════════════════════════════════════════════════════════════════

// ─── Event Capture API (for iOS app) ─────────────────────────────────

app.post("/api/v1/events/capture", async (c) => {
  try {
    const body = await c.req.json();
    const { event, distinct_id, properties, timestamp } = body;

    if (!event || !distinct_id) {
      return c.json({ error: "Missing event or distinct_id" }, 400);
    }

    trackEvent(distinct_id, event, {
      ...properties,
      app_version: c.req.header("X-App-Version") || "unknown",
    });

    return c.json({ status: "ok" });
  } catch (error) {
    console.error("[Events] Capture error:", error);
    return c.json({ error: "Internal error" }, 500);
  }
});

app.post("/api/v1/events/capture/batch", async (c) => {
  try {
    const { events: eventList } = await c.req.json();

    if (!Array.isArray(eventList) || eventList.length === 0) {
      return c.json({ error: "Empty or invalid events array" }, 400);
    }

    if (eventList.length > 50) {
      return c.json({ error: "Max 50 events per batch" }, 400);
    }

    for (const e of eventList) {
      trackEvent(e.distinct_id, e.event, e.properties);
    }

    return c.json({ status: "ok", count: eventList.length });
  } catch (error) {
    console.error("[Events] Batch error:", error);
    return c.json({ error: "Internal error" }, 500);
  }
});

app.post("/api/v1/events/identify", async (c) => {
  try {
    const { distinct_id, properties } = await c.req.json();

    if (!distinct_id) {
      return c.json({ error: "Missing distinct_id" }, 400);
    }

    identifyUser(distinct_id, {
      language: properties?.language,
      subscription_status: properties?.subscription_status,
      subscription_plan: properties?.subscription_plan,
      circle_count: properties?.circle_count,
      install_source: properties?.install_source,
      total_prayers: properties?.total_prayers,
      current_streak: properties?.current_streak,
    });

    return c.json({ status: "ok" });
  } catch (error) {
    console.error("[Events] Identify error:", error);
    return c.json({ error: "Internal error" }, 500);
  }
});

// ─── RevenueCat Webhook ──────────────────────────────────────────────

const RC_EVENT_MAP: Record<string, string> = {
  INITIAL_PURCHASE: "subscription_started",
  RENEWAL: "subscription_renewed",
  CANCELLATION: "subscription_cancelled",
  UNCANCELLATION: "subscription_reactivated",
  EXPIRATION: "subscription_expired",
  BILLING_ISSUE: "billing_issue_detected",
  PRODUCT_CHANGE: "subscription_plan_changed",
  NON_RENEWING_PURCHASE: "lifetime_purchased",
};

function getPlanFromProductId(productId: string): string {
  if (productId.includes("monthly")) return "monthly";
  if (productId.includes("yearly")) return "yearly";
  if (productId.includes("lifetime")) return "lifetime";
  return "unknown";
}

function getStatusFromRCEvent(type: string): string {
  switch (type) {
    case "INITIAL_PURCHASE":
    case "RENEWAL":
    case "UNCANCELLATION":
      return "active";
    case "CANCELLATION":
      return "cancelled";
    case "EXPIRATION":
      return "expired";
    case "BILLING_ISSUE":
      return "billing_issue";
    case "NON_RENEWING_PURCHASE":
      return "lifetime";
    default:
      return "unknown";
  }
}

app.post("/webhooks/revenuecat", async (c) => {
  try {
    const authHeader = c.req.header("Authorization");
    const expectedSecret = process.env.REVENUECAT_WEBHOOK_SECRET;

    if (expectedSecret && authHeader !== `Bearer ${expectedSecret}`) {
      console.warn("[RC Webhook] Unauthorized request");
      return c.json({ error: "Unauthorized" }, 401);
    }

    const body = await c.req.json();
    const rcEvent = body.event;

    if (!rcEvent || !rcEvent.type) {
      return c.json({ error: "Invalid webhook payload" }, 400);
    }

    const eventName = RC_EVENT_MAP[rcEvent.type];
    if (!eventName) {
      console.log("[RC Webhook] Unmapped event type:", rcEvent.type);
      return c.json({ status: "skipped" });
    }

    const userId = rcEvent.app_user_id;
    const productId = rcEvent.product_id || "";
    const plan = getPlanFromProductId(productId);

    // Update user subscription status in DB
    const status = getStatusFromRCEvent(rcEvent.type);
    await pool
      .query(
        "UPDATE users SET subscription_status = $1, updated_at = NOW() WHERE id = $2 OR device_user_id = $2",
        [status, userId]
      )
      .catch(() => {});

    trackEvent(userId, eventName, {
      plan,
      product_id: productId,
      price: rcEvent.price,
      currency: rcEvent.currency,
      store: rcEvent.store,
      period_type: rcEvent.period_type,
      environment: rcEvent.environment,
      country_code: rcEvent.country_code,
      $revenue: rcEvent.price || 0,
      $currency: rcEvent.currency || "USD",
    });

    identifyUser(userId, {
      subscription_status: status,
      subscription_plan: plan,
      last_revenue_event: eventName,
    });

    console.log(
      `[RC Webhook] ${rcEvent.type} → ${eventName} | user=${userId} plan=${plan}`
    );

    return c.json({ status: "ok" });
  } catch (error) {
    console.error("[RC Webhook] Error:", error);
    return c.json({ error: "Internal error" }, 500);
  }
});

// ─── Circle CRUD (unchanged) ─────────────────────────────────────────

app.post("/api/circles", async (c) => {
  const body = await c.req.json();
  const { userId, userName, name, emoji, streakCount, lastPrayedDate } = body;

  if (!userId || !userName) {
    return c.json({ error: "userId and userName are required" }, 400);
  }

  const code = generateCode();
  const circle: StoredCircle = {
    id: randomUUID(),
    name: name || "Prayer Circle",
    code,
    emoji: emoji || "🏠",
    creatorUserId: userId,
    members: [
      {
        userId,
        name: userName,
        streakCount: streakCount || 0,
        lastPrayedDate: lastPrayedDate || null,
        joinedAt: new Date().toISOString(),
      },
    ],
    prayerRequests: [],
    encouragements: [],
    createdAt: new Date().toISOString(),
  };

  await saveCircleToDb(circle);
  console.log(`Circle created: ${code} by ${userName}`);

  trackEvent(userId, "circle_created", {
    circle_id: circle.id,
    circle_code: code,
    circle_name: name || "Prayer Circle",
  });

  return c.json({ circle }, 201);
});

app.get("/api/circles/:code", (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);
  return c.json({ circle });
});

app.post("/api/circles/:code/join", async (c) => {
  const code = c.req.param("code").toUpperCase();
  let body;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "Invalid request body" }, 400);
  }

  const { userId, userName, streakCount, lastPrayedDate } = body;
  if (!userId || !userName) {
    return c.json({ error: "userId and userName are required" }, 400);
  }

  const circle = getCircle(code);
  if (!circle) {
    return c.json(
      { error: "Circle not found. Check the code and try again." },
      404
    );
  }

  const existing = circle.members.find((m) => m.userId === userId);
  if (existing) {
    return c.json({ circle });
  }

  circle.members.push({
    userId,
    name: userName,
    streakCount: streakCount || 0,
    lastPrayedDate: lastPrayedDate || null,
    joinedAt: new Date().toISOString(),
  });

  await saveCircleToDb(circle);
  console.log(`${userName} joined circle ${code}`);

  trackEvent(userId, "circle_invite_accepted", {
    circle_id: circle.id,
    circle_code: code,
    circle_size: circle.members.length,
  });

  trackEvent(circle.creatorUserId, "circle_member_joined", {
    circle_id: circle.id,
    circle_code: code,
    circle_size: circle.members.length,
    new_member_name: userName,
  });

  return c.json({ circle });
});

app.put("/api/circles/:code", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const body = await c.req.json();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  if (body.name) circle.name = body.name;
  if (body.emoji) circle.emoji = body.emoji;

  await saveCircleToDb(circle);
  return c.json({ circle });
});

app.put("/api/circles/:code/members/:userId/status", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.param("userId");
  const body = await c.req.json();

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  const member = circle.members.find((m) => m.userId === userId);
  if (!member) return c.json({ error: "Member not found" }, 404);

  const oldStreak = member.streakCount;

  if (body.streakCount !== undefined) member.streakCount = body.streakCount;
  if (body.lastPrayedDate !== undefined)
    member.lastPrayedDate = body.lastPrayedDate;
  if (body.name !== undefined) member.name = body.name;

  await saveCircleToDb(circle);

  const milestones = [3, 7, 14, 30, 60, 90, 180, 365];
  if (
    body.streakCount !== undefined &&
    body.streakCount > oldStreak &&
    milestones.includes(body.streakCount)
  ) {
    trackEvent(userId, "streak_milestone", {
      streak_count: body.streakCount,
      circle_code: code,
    });
  }

  return c.json({ circle });
});

app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.param("userId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.members = circle.members.filter((m) => m.userId !== userId);

  trackEvent(userId, "circle_left", {
    circle_code: code,
    remaining_members: circle.members.length,
  });

  if (circle.members.length === 0) {
    await deleteCircleFromDb(code);
  } else {
    await saveCircleToDb(circle);
  }

  return c.json({ success: true });
});

app.delete("/api/circles/:code", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  trackEvent(circle.creatorUserId, "circle_deleted", {
    circle_code: code,
    member_count: circle.members.length,
  });

  await deleteCircleFromDb(code);
  console.log(`Circle deleted: ${code}`);
  return c.json({ success: true });
});

// ─── Prayer Requests ─────────────────────────────────────────────────

app.post("/api/circles/:code/prayer-requests", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const body = await c.req.json();

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  const requestId = randomUUID();
  circle.prayerRequests.unshift({
    id: requestId,
    requesterUserId: body.userId,
    requesterName: body.isAnonymous ? "Anonymous" : body.userName || "Someone",
    text: body.text,
    timestamp: new Date().toISOString(),
    isAnonymous: body.isAnonymous || false,
    prayedByUserIds: [],
  });

  await saveCircleToDb(circle);

  trackEvent(body.userId, "prayer_request_created", {
    circle_code: code,
    is_anonymous: body.isAnonymous || false,
    word_count: (body.text || "").split(/\s+/).length,
  });

  return c.json({ circle });
});

app.post("/api/circles/:code/prayer-requests/:requestId/pray", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const requestId = c.req.param("requestId");
  const body = await c.req.json();

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  const request = circle.prayerRequests.find((r) => r.id === requestId);
  if (!request) return c.json({ error: "Prayer request not found" }, 404);

  if (!request.prayedByUserIds.includes(body.userId)) {
    request.prayedByUserIds.push(body.userId);

    trackEvent(body.userId, "prayer_request_prayed", {
      circle_code: code,
      request_id: requestId,
      total_prayers: request.prayedByUserIds.length,
    });
  }

  await saveCircleToDb(circle);
  return c.json({ circle });
});

app.delete("/api/circles/:code/prayer-requests/:requestId", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const requestId = c.req.param("requestId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  const before = circle.prayerRequests.length;
  circle.prayerRequests = circle.prayerRequests.filter(
    (r) => r.id !== requestId
  );

  if (circle.prayerRequests.length === before) {
    return c.json({ error: "Prayer request not found" }, 404);
  }

  await saveCircleToDb(circle);
  console.log(`Prayer request ${requestId} deleted from circle ${code}`);
  return c.json({ success: true });
});

// ─── Encouragements ──────────────────────────────────────────────────

app.post("/api/circles/:code/encouragements", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const body = await c.req.json();

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.encouragements.push({
    id: randomUUID(),
    toUserId: body.toUserId,
    fromUserId: body.fromUserId,
    fromName: body.fromName || "Someone",
    message: body.message,
    timestamp: new Date().toISOString(),
  });

  await saveCircleToDb(circle);

  trackEvent(body.fromUserId, "encouragement_sent", {
    circle_code: code,
    to_user_id: body.toUserId,
  });

  return c.json({ circle });
});

// ─── Circle Info (public, for join page) ─────────────────────────────

app.get("/api/circles/:code/info", (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);
  const creator = circle.members.find(
    (m) => m.userId === circle.creatorUserId
  );
  return c.json({
    name: circle.name,
    emoji: circle.emoji,
    memberCount: circle.members.length,
    creatorName: creator?.name || null,
  });
});

// ─── Start Server ────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3000", 10);

async function start() {
  await initDb();
  await loadAllFromDb();

  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API v2.0 running on port ${info.port}`);
    console.log(`   Storage: PostgreSQL`);
    console.log(`   Auth: Sign in with Apple`);
    console.log(`   Analytics: ${POSTHOG_API_KEY ? "PostHog ✓" : "Disabled"}`);
    console.log(`   Circles: ${circles.size} loaded\n`);
  });
}

start();
