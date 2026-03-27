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

  if (body.streakCount !== undefined) member.streakCount = body.streakCount;
  if (body.lastPrayedDate !== undefined) member.lastPrayedDate = body.lastPrayedDate;
  if (body.name !== undefined) member.name = body.name;

  await saveCircleToDb(circle);
  return c.json({ circle });
});

// ─── Remove Member ───────────────────────────────────────────────────

app.delete("/api/circles/:code/members/:userId", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.param("userId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.members = circle.members.filter((m) => m.userId !== userId);

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

  circle.prayerRequests.unshift({
    id: randomUUID(),
    requesterUserId: body.userId,
    requesterName: body.isAnonymous ? "Anonymous" : body.userName || "Someone",
    text: body.text,
    timestamp: new Date().toISOString(),
    isAnonymous: body.isAnonymous || false,
    prayedByUserIds: [],
  });

  await saveCircleToDb(circle);
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
  return c.json({ circle });
});

// ─── Circle Info (public, for join page) ─────────────────────────────

app.get("/api/circles/:code/info", (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);
  return c.json({
    name: circle.name,
    emoji: circle.emoji,
    memberCount: circle.members.length,
  });
});
```

Then commit and push to trigger Railway deploy. Once it's live, you can test it by hitting:
```
https://web-production-88ed0.up.railway.app/api/circles/N2QUCM/info

// ─── Start Server ────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3000", 10);

async function start() {
  await initDb();
  await loadAllFromDb();

  serve({ fetch: app.fetch, port: PORT }, (info) => {
    console.log(`\n🙏 prAmen API running on port ${info.port}`);
    console.log(`   Storage: PostgreSQL`);
    console.log(`   Circles: ${circles.size} loaded\n`);
  });
}

start();
