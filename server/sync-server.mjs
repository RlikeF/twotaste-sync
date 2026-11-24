#!/usr/bin/env node
/**
 * 双人食光 · 情侣同步小服务（零依赖，Node 18+）
 *
 *   node server/sync-server.mjs
 *   PORT=8787 TOKEN=我们的口令 node server/sync-server.mjs
 *
 * 接口：
 *   GET  /health          健康检查
 *   GET  /state           当前整份状态（调试用）
 *   POST /sync            {version, state} → 合并后回传，并广播给所有在线设备
 *   GET  /events          SSE 长连接，有改动就推 {"version":n}，客户端收到立刻回推自己的状态
 *   POST /blob/:id        上传一张菜品封面（JPEG 原始字节）
 *   GET  /blob/:id        取回一张封面；404 = 还没人上传
 *   GET  /blobs           已上传的封面 id 列表（调试用）
 *
 * 合并策略：按条目 updatedAt 取新（last-write-wins），列表按 id 对齐，
 * 投票按 dishId 对齐，整份状态只在真的有差异时才升版本。
 */
import http from "node:http";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, "data", "state.json");
/** 封面照片和 state.json 放一起，方便整个目录备份 */
const BLOB_DIR = path.join(path.dirname(DATA_FILE), "blobs");
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "0.0.0.0";
/** 不设就谁都能连；公网部署务必设上 */
const TOKEN = process.env.TOKEN || "";
const HEARTBEAT_MS = 15_000;
const MAX_BLOB_BYTES = 6 * 1024 * 1024;

let current = load();
/** 在线的 SSE 客户端，实时推送用 */
const subscribers = new Set();
let peakSubscribers = 0;

function load() {
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch {
    return { version: 0, state: null, serverTime: 0 };
  }
}

function persist() {
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(current, null, 2));
  } catch (err) {
    console.warn("[warn] 写入 state.json 失败：", err.message);
  }
}

function newer(a, b) {
  return (b?.updatedAt ?? 0) > (a?.updatedAt ?? 0) ? b : a;
}

function mergeList(a = [], b = []) {
  const map = new Map();
  for (const item of [...a, ...b]) {
    if (!item?.id) continue;
    const prev = map.get(item.id);
    map.set(item.id, prev ? newer(prev, item) : item);
  }
  return [...map.values()];
}

function mergeVotes(a = {}, b = {}) {
  const out = { ...a };
  for (const [key, value] of Object.entries(b)) {
    const prev = out[key];
    out[key] = prev ? newer(prev, value) : value;
  }
  return out;
}

function mergeState(local, incoming) {
  if (!local) return incoming;
  if (!incoming) return local;
  const base = newer(local, incoming);
  const merged = {
    ...base,
    dishes: mergeList(local.dishes, incoming.dishes),
    fridge: mergeList(local.fridge, incoming.fridge),
    logs: mergeList(local.logs, incoming.logs),
    shopping: mergeList(local.shopping, incoming.shopping),
    orders: mergeList(local.orders, incoming.orders),
    votes: mergeVotes(local.votes, incoming.votes),
    myTaste: newer(local.myTaste, incoming.myTaste),
    partnerTaste: newer(local.partnerTaste, incoming.partnerTaste),
    settings: { ...newer(local.settings, incoming.settings) },
  };
  // 设备本地字段不参与同步，避免两台手机互相覆盖同步地址和口令
  merged.settings.syncUrl = "";
  merged.settings.syncToken = "";
  return merged;
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    const out = {};
    for (const key of Object.keys(value).sort()) out[key] = stable(value[key]);
    return out;
  }
  return value;
}

/** 只比较内容，忽略版本号；键序无关，避免「看起来没变却升版本」。 */
function changed(a, b) {
  if (!a) return true;
  const strip = (s) => {
    const clone = { ...s };
    delete clone.version;
    return stable(clone);
  };
  return JSON.stringify(strip(a)) !== JSON.stringify(strip(b));
}

function authorized(req, url) {
  if (!TOKEN) return true;
  const given = req.headers["x-token"] || url.searchParams.get("token") || "";
  return given === TOKEN;
}

function send(res, code, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, X-Device, X-Token",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Cache-Control": "no-store",
  });
  res.end(body);
}

/** 状态变了就告诉所有在线设备：版本号变了，快来同步 */
function broadcast(version) {
  const frame = `event: update\ndata: ${JSON.stringify({ version })}\n\n`;
  for (const res of subscribers) {
    try {
      res.write(frame);
    } catch {
      subscribers.delete(res);
    }
  }
}

function openStream(req, res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
    "Access-Control-Allow-Origin": "*",
  });
  // 断线后客户端 3 秒重连（服务端重启也能自己接上）
  res.write("retry: 3000\n\n");
  res.write(`event: hello\ndata: ${JSON.stringify({ version: current.version })}\n\n`);

  subscribers.add(res);
  peakSubscribers = Math.max(peakSubscribers, subscribers.size);
  console.log(`[sse] +1 在线 ${subscribers.size} 台（${req.headers["x-device"] || "unknown"}）`);

  const heartbeat = setInterval(() => {
    try {
      res.write(": ping\n\n");
    } catch {
      clearInterval(heartbeat);
    }
  }, HEARTBEAT_MS);

  const close = () => {
    clearInterval(heartbeat);
    if (subscribers.delete(res)) {
      console.log(`[sse] -1 在线 ${subscribers.size} 台`);
    }
  };
  req.on("close", close);
  req.on("error", close);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 8 * 1024 * 1024) reject(new Error("payload too large"));
    });
    req.on("end", () => resolve(raw));
    req.on("error", reject);
  });
}

// ---------------- 封面照片 ----------------

/** id 只允许字母数字下划线短横，杜绝 ../ 之类的路径穿越 */
function blobPath(id) {
  if (!/^[A-Za-z0-9_-]{6,64}$/.test(id)) return null;
  return path.join(BLOB_DIR, `${id}.jpg`);
}

function readBlob(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BLOB_BYTES) {
        reject(new Error("图片太大了"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function handleBlobUpload(req, res, id) {
  const file = blobPath(id);
  if (!file) return send(res, 400, { error: "id 不合法" });
  try {
    const bytes = await readBlob(req);
    if (bytes.length === 0) return send(res, 400, { error: "空文件" });
    if (bytes[0] !== 0xff || bytes[1] !== 0xd8) return send(res, 400, { error: "只收 JPEG" });
    fs.mkdirSync(BLOB_DIR, { recursive: true });
    fs.writeFileSync(file, bytes);
    console.log(`[blob] ${id} 上传 ${Math.round(bytes.length / 1024)}KB`);
    return send(res, 200, { ok: true, id, bytes: bytes.length });
  } catch (err) {
    return send(res, 400, { error: err.message });
  }
}

function handleBlobDownload(req, res, id) {
  const file = blobPath(id);
  if (!file || !fs.existsSync(file)) return send(res, 404, { error: "没有这张图" });
  const stat = fs.statSync(file);
  res.writeHead(200, {
    "Content-Type": "image/jpeg",
    "Content-Length": stat.size,
    // 同一个 id 的内容永远不变，客户端可以放心缓存
    "Cache-Control": "public, max-age=31536000, immutable",
    "Access-Control-Allow-Origin": "*",
  });
  fs.createReadStream(file).pipe(res);
}

function listBlobs() {
  try {
    return fs
      .readdirSync(BLOB_DIR)
      .filter((f) => f.endsWith(".jpg"))
      .map((f) => f.slice(0, -4));
  } catch {
    return [];
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host}`);

  if (req.method === "OPTIONS") return send(res, 204, {});
  if (url.pathname === "/health") {
    return send(res, 200, {
      ok: true,
      version: current.version,
      serverTime: Date.now(),
      online: subscribers.size,
      auth: Boolean(TOKEN),
    });
  }
  if (!authorized(req, url)) {
    return send(res, 401, { error: "口令不对" });
  }
  if (url.pathname === "/state" && req.method === "GET") {
    return send(res, 200, current);
  }
  if (url.pathname === "/events" && req.method === "GET") {
    return openStream(req, res);
  }
  if (url.pathname.startsWith("/blob/")) {
    const id = decodeURIComponent(url.pathname.slice("/blob/".length));
    if (req.method === "POST" || req.method === "PUT") return handleBlobUpload(req, res, id);
    if (req.method === "GET") return handleBlobDownload(req, res, id);
  }
  if (url.pathname === "/blobs" && req.method === "GET") {
    return send(res, 200, { blobs: listBlobs() });
  }
  if (url.pathname === "/sync" && req.method === "POST") {
    try {
      const raw = await readBody(req);
      const envelope = JSON.parse(raw || "{}");
      if (!envelope.state) return send(res, 400, { error: "missing state" });

      const before = current.state;
      const merged = mergeState(before, envelope.state);
      const dirty = !before || changed(before, merged);
      if (dirty) {
        current = { version: Math.max(current.version, envelope.version || 0) + 1, state: merged, serverTime: Date.now() };
        persist();
        console.log(`[sync] v${current.version} <- ${req.headers["x-device"] || "unknown"} (${raw.length}B)`);
      } else {
        current.serverTime = Date.now();
      }
      const payload = { version: current.version, state: current.state, serverTime: current.serverTime };
      send(res, 200, payload);
      // 先回给发起方，再叫醒其他人，避免发起方写两次
      if (dirty && subscribers.size > 0) broadcast(current.version);
      return;
    } catch (err) {
      console.warn("[sync] 失败：", err.message);
      return send(res, 500, { error: err.message });
    }
  }
  return send(res, 404, { error: "not found", routes: ["/health", "/state", "/sync", "/events", "/blob/:id"] });
});

function lanAddresses() {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter((i) => i && i.family === "IPv4" && !i.internal)
    .map((i) => i.address);
}

server.listen(PORT, HOST, () => {
  console.log("双人食光 · 同步服务已开张（实时推送）");
  console.log(`  本机：   http://127.0.0.1:${PORT}`);
  for (const ip of lanAddresses()) console.log(`  局域网： http://${ip}:${PORT}    ← 手机填这个`);
  console.log(`  口令：   ${TOKEN ? "已开启，App 里要填同一串" : "未设置（公网部署请设 TOKEN）"}`);
  console.log(`  数据文件：${DATA_FILE}`);
  console.log(`  封面照片：${BLOB_DIR}（已存 ${listBlobs().length} 张）`);
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log("\n[bye] 同步服务关闭，数据已落盘");
    for (const res of subscribers) {
      try {
        res.end();
      } catch {
        /* ignore */
      }
    }
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500);
  });
}