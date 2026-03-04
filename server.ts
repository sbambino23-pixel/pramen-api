import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import * as fs from "fs";
import * as path from "path";

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

// ─── Persistence Layer ───────────────────────────────────────────────
// In-memory store with JSON file backup for persistence across restarts

const DATA_FILE = path.join(process.cwd(), "data", "circles.json");
const circles = new Map<string, StoredCircle>();

function loadFromDisk(): void {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = fs.readFileSync(DATA_FILE, "utf-8");
      const data: Record<string, StoredCircle> = JSON.parse(raw);
      for (const [key, circle] of Object.entries(data)) {
        circles.set(key, circle);
      }
      console.log(`Loaded ${circles.size} circles from disk`);
    }
  } catch (err) {
    console.error("Failed to load data from disk:", err);
  }
}

function saveToDisk(): void {
  try {
    const dir = path.dirname(DATA_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const data: Record<string, StoredCircle> = {};
    for (const [key, circle] of circles.entries()) {
      data[key] = circle;
    }
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error("Failed to save data to disk:", err);
  }
}

// Debounced save — writes at most once per second
let saveTimer: ReturnType<typeof setTimeout> | null = null;
function debouncedSave(): void {
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(saveToDisk, 1000);
}

// ─── Circle helpers ──────────────────────────────────────────────────

function getCircle(code: string): StoredCircle | undefined {
  return circles.get(code.toUpperCase());
}

function saveCircle(circle: StoredCircle): void {
  circles.set(circle.code.toUpperCase(), circle);
  debouncedSave();
}

function deleteCircle(code: string): boolean {
  const deleted = circles.delete(code.toUpperCase());
  if (deleted) debouncedSave();
  return deleted;
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

// ─── Health ──────────────────────────────────────────────────────────

app.get("/", (c) => {
  return c.json({
    status: "ok",
    service: "prAmen API",
    circles: circles.size,
    timestamp: new Date().toISOString(),
  });
});

app.get("/api/circles/health", (c) => {
  return c.json({
    status: "ok",
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
    id: crypto.randomUUID(),
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

  saveCircle(circle);
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

  saveCircle(circle);
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

  saveCircle(circle);
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

  saveCircle(circle);
  return c.json({ circle });
});

// ─── Remove Member ───────────────────────────────────────────────────

app.delete("/api/circles/:code/members/:userId", (c) => {
  const code = c.req.param("code").toUpperCase();
  const userId = c.req.param("userId");

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.members = circle.members.filter((m) => m.userId !== userId);
  saveCircle(circle);

  // Auto-delete empty circles
  if (circle.members.length === 0) {
    deleteCircle(code);
  }

  return c.json({ success: true });
});

// ─── Delete Circle ───────────────────────────────────────────────────

app.delete("/api/circles/:code", (c) => {
  const code = c.req.param("code").toUpperCase();
  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  deleteCircle(code);
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
    id: crypto.randomUUID(),
    requesterUserId: body.userId,
    requesterName: body.isAnonymous ? "Anonymous" : body.userName || "Someone",
    text: body.text,
    timestamp: new Date().toISOString(),
    isAnonymous: body.isAnonymous || false,
    prayedByUserIds: [],
  });

  saveCircle(circle);
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

  saveCircle(circle);
  return c.json({ circle });
});

// ─── Encouragements ──────────────────────────────────────────────────

app.post("/api/circles/:code/encouragements", async (c) => {
  const code = c.req.param("code").toUpperCase();
  const body = await c.req.json();

  const circle = getCircle(code);
  if (!circle) return c.json({ error: "Circle not found" }, 404);

  circle.encouragements.push({
    id: crypto.randomUUID(),
    toUserId: body.toUserId,
    fromUserId: body.fromUserId,
    fromName: body.fromName || "Someone",
    message: body.message,
    timestamp: new Date().toISOString(),
  });

  saveCircle(circle);
  return c.json({ circle });
});

// ─── Start Server ────────────────────────────────────────────────────

const PORT = parseInt(process.env.PORT || "3000", 10);

loadFromDisk();

serve({ fetch: app.fetch, port: PORT }, (info) => {
  console.log(`\n🙏 prAmen API running on port ${info.port}`);
  console.log(`   Health: http://localhost:${info.port}/`);
  console.log(`   Circles: ${circles.size} loaded\n`);
});
