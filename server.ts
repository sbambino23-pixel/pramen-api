import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import { randomUUID } from "crypto";
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
    await client.query(`
      CREATE TABLE IF NOT EXISTS circles (
        code TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    console.log("Database initialized");
  } catch (err) {
    console.error("Failed to initialize database:", err);
  } finally {
    client.release();
  }
}

// In-memory cache backed by Postgres
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

// ─── Circle helpers ──────────────────────────────────────────────────

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

// ─── Hono App ────────────────────────────────────────────────────────

const app = new Hono();

// CORS for all origins (mobile app)
app.use("*", cors());

// Global error handler — always return JSON, never let Railway serve HTML
app.onError((err, c) => {
  console.error("Unhandled error:", err);
  return c.json({ error: "Internal server error", detail: err.message }, 500);
});

// ─── Health ──────────────────────────────────────────────────────────

app.get("/", (c) => {
  return c.json({
    status: "ok",
    service: "prAmen API",
    storage: "postgres",
    circles: circles.size,
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
      subscription_status: getStatusFromRCEvent(rcEvent.type),
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

// ─── Create Circle ───────────────────────────────────────────────────

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

  // 📊 Analytics: Circle created
  trackEvent(userId, "circle_created", {
    circle_id: circle.id,
    circle_code: code,
    circle_name: name || "Prayer Circle",
  });

  return c.json({ circle }, 201);
});

// ─── Get Circle ──────────────────────────────────────────────────────

app.get("/api/circles/:code", (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);
  return c.json({ circle });
});

// ─── Join Circle ─────────────────────────────────────────────────────

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
    return c.json({ error: "Circle not found. Check the code and try again." }, 404);
  }

  // Already a member — just return the circle
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

  // 📊 Analytics: Member joined circle (tracks viral loop)
  trackEvent(userId, "circle_invite_accepted", {
    circle_id: circle.id,
    circle_code: code,
    circle_size: circle.members.length,
  });

  // 📊 Analytics: Notify circle creator that someone joined
  trackEvent(circle.creatorUserId, "circle_member_joined", {
    circle_id: circle.id,
    circle_code: code,
    circle_size: circle.members.length,
    new_member_name: userName,
  });

  return c.json({ circle });
});

// ─── Update Circle ───────────────────────────────────────────────────

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

// ─── Update Member Status ────────────────────────────────────────────

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
  if (body.lastPrayedDate !== undefined) member.lastPrayedDate = body.lastPrayedDate;
  if (body.name !== undefined) member.name = body.name;

  await saveCircleToDb(circle);

  // 📊 Analytics: Streak milestone detection
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

// ─── Remove Member ───────────────────────────────────────────────────

app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.param("userId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.members = circle.members.filter((m) => m.userId !== userId);

  // 📊 Analytics: Member left circle
  trackEvent(userId, "circle_left", {
    circle_code: code,
    remaining_members: circle.members.length,
  });

  // Auto-delete empty circles
  if (circle.members.length === 0) {
    await deleteCircleFromDb(code);
  } else {
    await saveCircleToDb(circle);
  }

  return c.json({ success: true });
});

// ─── Delete Circle ───────────────────────────────────────────────────

app.delete("/api/circles/:code", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  // 📊 Analytics: Circle deleted
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

  // 📊 Analytics: Prayer request created
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

    // 📊 Analytics: User prayed for someone's request
    trackEvent(body.userId, "prayer_request_prayed", {
      circle_code: code,
      request_id: requestId,
      total_prayers: request.prayedByUserIds.length,
    });
  }

  await saveCircleToDb(circle);
  return c.json({ circle });
});

// ─── Delete Prayer Request ──────────────────────────────────────────

app.delete("/api/circles/:code/prayer-requests/:requestId", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const requestId = c.req.param("requestId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  const before = circle.prayerRequests.length;
  circle.prayerRequests = circle.prayerRequests.filter((r) => r.id !== requestId);

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

  // 📊 Analytics: Encouragement sent
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
  const creator = circle.members.find((m) => m.userId === circle.creatorUserId);
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
    console.log(`\n🙏 prAmen API running on port ${info.port}`);
    console.log(`   Storage: PostgreSQL`);
    console.log(`   Analytics: ${POSTHOG_API_KEY ? "PostHog ✓" : "Disabled"}`);
    console.log(`   Circles: ${circles.size} loaded\n`);
  });
}

start();
