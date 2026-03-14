// =====================================================
// 🧠 STEAL A BRAINROT SCANNER - BACKEND v4.0
// =====================================================
//
// UPGRADED FROM v3.9:
//   ✅ /api/v1/report-dead — bots report Error 2/279 → instant blacklist
//   ✅ Keep-alive ping to prevent Render free tier sleep
//   ✅ Reduced STALE_THRESHOLD (10min → 4min) for fresher servers
//   ✅ Smarter cleanup + dead server propagation
//   ✅ /alerts endpoint integrated (notifier support)
//   ✅ BOT_REQUEST_COOLDOWN tuned for 100+ bots
//
// ENDPOINTS:
//   POST /api/v1/get-job-assignment — assign servers to bot
//   POST /api/v1/report-found — check if server already reported
//   POST /api/v1/report-restricted — blacklist a dead server
//   POST /api/v1/report-dead — bot reports teleport failure (Error 2/279)
//   POST /api/v1/clear-history — reset a bot's history
//   POST /scan-complete — bot reports real scan done (hops tracking)
//   GET  /api/v1/stats — dashboard
//   GET  /alerts — notifier polling endpoint
//   GET  /health — health check
//
// =====================================================

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// =====================================================
// 🎮 CONFIGURATION
// =====================================================

const STEAL_A_BRAINROT = {
    PLACE_ID: 109983668079237,
    UNIVERSE_ID: 7709344486,
    GAME_NAME: "Steal a Brainrot"
};

// =====================================================
// 🌍 PROXIES & RÉGIONS
// =====================================================

const PROXY_POOL = [];

const REGIONAL_CONFIG = {
    'us':            { name: 'United States',  vps_range: [1, 10],  expected_bots: 250 },
    'europe':        { name: 'Europe',         vps_range: [11, 19], expected_bots: 225 },
    'asia':          { name: 'Asia Pacific',   vps_range: [20, 24], expected_bots: 125 },
    'south-america': { name: 'South America',  vps_range: [25, 27], expected_bots: 75 },
    'oceania':       { name: 'Oceania',        vps_range: [28, 30], expected_bots: 75 },
};

function initProxyPool() {
    const envMap = {
        'PROXY_US':            'US',
        'PROXY_EU':            'EU',
        'PROXY_ASIA':          'Asia',
        'PROXY_SOUTH_AMERICA': 'South America',
        'PROXY_OCEANIA':       'Oceania',
    };
    for (const [envKey, label] of Object.entries(envMap)) {
        const url = process.env[envKey];
        if (url) {
            PROXY_POOL.push({ baseUrl: url, url, label, errors: 0, lastError: 0, lastFetch: {} });
        }
    }

    for (let i = 1; i <= 20; i++) {
        const url = process.env[`PROXY_${i}`];
        if (url) {
            PROXY_POOL.push({ baseUrl: url, url, label: `P${i}`, errors: 0, lastError: 0, lastFetch: {} });
        }
    }

    console.log(`🔧 Proxy pool: ${PROXY_POOL.length} proxies`);
    for (const p of PROXY_POOL) {
        console.log(`   📡 ${p.label}: ${p.baseUrl.replace(/:[^:@]+@/, ':***@')}`);
    }
    if (PROXY_POOL.length === 0) {
        console.warn('⚠️  NO PROXIES configured!');
    }
}

// =====================================================
// 🔄 SMARTPROXY SESSION ROTATION
// =====================================================

function randomSessionId() {
    return crypto.randomBytes(6).toString('hex');
}

function rotateOneProxy(proxy) {
    const base = proxy.baseUrl;
    if (base.includes('_session-')) {
        proxy.url = base.replace(/_session-[a-zA-Z0-9]+/, `_session-${randomSessionId()}`);
    } else if (base.includes('_life-')) {
        proxy.url = base.replace(/(_life-\d+)/, `$1_session-${randomSessionId()}`);
    } else {
        proxy.url = base;
    }
}

// =====================================================
// ⚙️ PARAMÈTRES — TUNED FOR 100 BOTS + RENDER FREE
// =====================================================

const CONFIG = {
    ASSIGNMENT_DURATION: 45000,
    COOLDOWN_DURATION: 90000,
    AUTO_COOLDOWN_ON_EXPIRE: 90000,
    SERVERS_PER_BOT: 3,

    INITIAL_PAGES_PER_PROXY: 80,
    CONTINUOUS_PAGES_PER_PROXY: 20,
    FETCH_PAGE_DELAY: 1200,
    FETCH_PAGE_TIMEOUT: 12000,
    FETCH_MAX_CONSECUTIVE_ERRORS: 4,
    FETCH_RATE_LIMIT_BACKOFF: 5000,
    CONTINUOUS_FETCH_DELAY: 10000,

    // ✅ v4.0: Reduced from 600000 (10min) to 240000 (4min)
    // Dead servers get cleared faster → less Error 2/279
    STALE_THRESHOLD: 240000,

    DIRECT_PAGES: 50,
    DIRECT_PAGE_DELAY: 1000,

    // ✅ v4.0: Reduced from 5000 to 2000 for faster bot cycling
    BOT_REQUEST_COOLDOWN: 2000,
    MAX_BOT_HISTORY: 2000,

    BLACKLIST_DURATION: 600000,

    // ✅ v4.0: Quick blacklist for teleport failures (shorter than full blacklist)
    DEAD_SERVER_BLACKLIST_DURATION: 180000, // 3 min blacklist for Error 2/279

    // ✅ v4.0: After N reports of same server dead, extend blacklist
    DEAD_REPORT_THRESHOLD: 2,
    DEAD_EXTENDED_BLACKLIST: 600000, // 10 min if multiple bots report same server dead

    CLEANUP_INTERVAL: 10000,

    CYCLE_TIMEOUT: 240000,
    WATCHDOG_TIMEOUT: 300000,
    MAX_ROTATIONS: 10,
    ROTATION_NORESET_AFTER: 7,

    // ✅ v4.0: Keep-alive for Render free tier
    KEEP_ALIVE_INTERVAL: 840000, // Ping every 14 min (Render sleeps after 15)

    // ✅ v4.0: Alerts retention for notifier
    ALERTS_MAX_AGE: 300000,       // 5 min max age for alerts
    ALERTS_MAX_COUNT: 500,        // Max alerts in memory
};

// =====================================================
// 💾 HTTP CLIENT
// =====================================================

const { HttpsProxyAgent } = require('https-proxy-agent');

function httpGet(url, proxyUrl = null, timeout = 12000) {
    return new Promise((resolve, reject) => {
        let settled = false;
        const done = (fn, val) => { if (!settled) { settled = true; fn(val); } };

        const options = {
            method: 'GET',
            timeout,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
        };

        if (proxyUrl) {
            options.agent = new HttpsProxyAgent(proxyUrl, { timeout });
        }

        const req = https.request(url, options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 429) return done(reject, new Error('RATE_LIMITED'));
                if (res.statusCode !== 200) return done(reject, new Error(`HTTP_${res.statusCode}`));
                try { done(resolve, JSON.parse(data)); }
                catch (_) { done(reject, new Error('JSON_PARSE_FAIL')); }
            });
        });

        req.on('error', (err) => done(reject, new Error(`REQ_ERROR: ${err.message}`)));
        req.on('timeout', () => { req.destroy(); done(reject, new Error('TIMEOUT')); });

        const hardTimer = setTimeout(() => {
            req.destroy();
            done(reject, new Error('HARD_TIMEOUT'));
        }, timeout + 3000);

        req.on('close', () => clearTimeout(hardTimer));
        req.end();
    });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =====================================================
// 🔐 SHA-256 DISTRIBUTION
// =====================================================

function deterministicHashNum(serverId, botId) {
    const buf = crypto.createHash('sha256')
        .update(`${serverId}::${botId}::sab-v33-salt`)
        .digest();
    return buf.readUIntBE(0, 6);
}

// =====================================================
// 🔒 GLOBAL LOCK
// =====================================================

class AsyncLock {
    constructor() {
        this.locked = false;
        this.queue = [];
    }

    acquire() {
        return new Promise(resolve => {
            if (!this.locked) {
                this.locked = true;
                resolve();
            } else {
                this.queue.push(resolve);
            }
        });
    }

    release() {
        if (this.queue.length > 0) {
            this.queue.shift()();
        } else {
            this.locked = false;
        }
    }

    get queueLength() {
        return this.queue.length;
    }
}

const globalAssignmentLock = new AsyncLock();

// =====================================================
// 💾 GLOBAL STATE
// =====================================================

const globalCache = {
    jobs: [],
    lastUpdate: 0,
    fetchInProgress: false,
    fetchStartedAt: 0,
    lastFetchStats: {}
};

let globalCancelFlag = { cancelled: false };
let proxyRotationIndex = 0;

const serverAssignments = new Map();
const serverCooldowns = new Map();
const serverBlacklist = new Map();
const reportedServers = new Map();
const REPORT_DEDUP_DURATION = 300000;
const botHistory = new Map();
const botLastRequest = new Map();

// ✅ v4.0: Dead server tracking — counts how many bots reported each server dead
const deadServerReports = new Map();

const stats = {
    total_requests: 0,
    total_assignments: 0,
    total_duplicates_skipped: 0,
    total_blacklist_filtered: 0,
    total_rate_limited: 0,
    total_fetch_cycles: 0,
    total_collisions_detected: 0,
    total_collisions_resolved: 0,
    total_cycle_timeouts: 0,
    total_watchdog_resets: 0,
    total_stream_cancels: 0,
    total_dead_reports: 0,        // ✅ v4.0
    total_dead_blacklisted: 0,    // ✅ v4.0
    lock_max_queue: 0,
    uptime_start: Date.now()
};

// =====================================================
// 📊 REAL-TIME HOPS TRACKING + DISCORD STATS
// =====================================================

const DISCORD_STATS_WEBHOOK = process.env.DISCORD_WEBHOOK || '';

const scanTimestamps = [];
let totalScans = 0;
let discordMessageId = null;

function getHopsPerMinute() {
    const now = Date.now();
    const oneMinAgo = now - 60000;
    while (scanTimestamps.length > 0 && scanTimestamps[0] < oneMinAgo) {
        scanTimestamps.shift();
    }
    return scanTimestamps.length;
}

function getHopsPerMinute5m() {
    const now = Date.now();
    const fiveMinAgo = now - 300000;
    let count = 0;
    for (let i = scanTimestamps.length - 1; i >= 0; i--) {
        if (scanTimestamps[i] >= fiveMinAgo) count++;
        else break;
    }
    return Math.round(count / 5);
}

async function updateDiscordStats() {
    if (!DISCORD_STATS_WEBHOOK) return;

    const hopsMin = getHopsPerMinute();
    const hops5m = getHopsPerMinute5m();
    const uptimeS = Math.floor((Date.now() - stats.uptime_start) / 1000);
    const uptimeM = Math.floor(uptimeS / 60);
    const uptimeH = Math.floor(uptimeM / 60);

    const embed = {
        title: '⚡ Scanner Stats — Real-Time',
        color: 0x00FFB3,
        fields: [
            { name: '⚡ Combined Rate', value: `**${hopsMin}** hops/min`, inline: true },
            { name: '📊 Avg (5min)', value: `**${hops5m}** hops/min`, inline: true },
            { name: '✅ Total Scans', value: `**${totalScans.toLocaleString()}**`, inline: true },
            { name: '⏱️ Uptime', value: `${uptimeH}h ${uptimeM % 60}m`, inline: true },
            { name: '💀 Dead Reports', value: `**${stats.total_dead_reports}** (${stats.total_dead_blacklisted} blacklisted)`, inline: true },
            { name: '🗄️ Cache', value: `**${globalCache.jobs.length}** servers`, inline: true },
        ],
        footer: { text: `Updated • Backend v4.0` },
    };

    const payload = JSON.stringify({ username: 'Scanner Stats', embeds: [embed] });

    try {
        if (discordMessageId) {
            const res = await fetch(`${DISCORD_STATS_WEBHOOK}/messages/${discordMessageId}`, {
                method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: payload,
            });
            if (!res.ok) discordMessageId = null;
        }
        if (!discordMessageId) {
            const res = await fetch(`${DISCORD_STATS_WEBHOOK}?wait=true`, {
                method: 'POST', headers: { 'Content-Type': 'application/json' }, body: payload,
            });
            if (res.ok) {
                const data = await res.json();
                discordMessageId = data.id;
            }
        }
    } catch (err) {
        console.log(`⚠️ Discord stats error: ${err.message}`);
    }
}

setInterval(updateDiscordStats, 30000);
setTimeout(updateDiscordStats, 10000);

// =====================================================
// 📢 ALERTS SYSTEM (for Uchiwa notifier)
// =====================================================

const alertsStore = [];

function addAlert(alertData) {
    const alert = {
        ...alertData,
        timestamp: alertData.timestamp || Date.now(),
        id: crypto.randomBytes(8).toString('hex'),
    };
    alertsStore.push(alert);

    // Trim old alerts
    const cutoff = Date.now() - CONFIG.ALERTS_MAX_AGE;
    while (alertsStore.length > 0 && alertsStore[0].timestamp < cutoff) {
        alertsStore.shift();
    }
    while (alertsStore.length > CONFIG.ALERTS_MAX_COUNT) {
        alertsStore.shift();
    }

    return alert;
}

// =====================================================
// 🚫 BLACKLIST
// =====================================================

function blacklistServer(id, reason = 'unknown', duration = null) {
    const dur = duration || CONFIG.BLACKLIST_DURATION;
    serverBlacklist.set(id, { reason, expires_at: Date.now() + dur });
    if (['restricted', 'not_found', 'timeout', 'dead_teleport'].includes(reason)) {
        const before = globalCache.jobs.length;
        globalCache.jobs = globalCache.jobs.filter(j => j.id !== id);
        if (globalCache.jobs.length < before) {
            console.log(`   🗑️ Removed dead server ${id.slice(0,8)} (${reason}) → Cache: ${globalCache.jobs.length}`);
        }
    }
}

function isBlacklisted(id) {
    const e = serverBlacklist.get(id);
    if (!e) return false;
    if (Date.now() > e.expires_at) { serverBlacklist.delete(id); return false; }
    return true;
}

// =====================================================
// 💀 DEAD SERVER REPORTING (v4.0)
// =====================================================

function reportDeadServer(serverId, botId, errorCode) {
    stats.total_dead_reports++;

    const key = serverId;
    const existing = deadServerReports.get(key);
    const now = Date.now();

    if (existing) {
        // Don't count same bot twice
        if (!existing.bots.has(botId)) {
            existing.bots.add(botId);
            existing.count++;
            existing.lastReport = now;
        }
    } else {
        deadServerReports.set(key, {
            count: 1,
            bots: new Set([botId]),
            firstReport: now,
            lastReport: now,
            errorCode,
        });
    }

    const report = deadServerReports.get(key);

    // Always remove from assignment so other bots don't get it
    if (serverAssignments.has(serverId)) {
        serverAssignments.delete(serverId);
    }

    if (report.count >= CONFIG.DEAD_REPORT_THRESHOLD) {
        // Multiple bots confirmed dead → extended blacklist
        blacklistServer(serverId, 'dead_teleport', CONFIG.DEAD_EXTENDED_BLACKLIST);
        stats.total_dead_blacklisted++;
        console.log(`💀 Server ${serverId.slice(0,8)} CONFIRMED DEAD (${report.count} reports, Error ${errorCode}) → blacklisted 10min`);
        deadServerReports.delete(key);
        return { action: 'blacklisted_extended', reports: report.count };
    } else {
        // First report → quick blacklist
        blacklistServer(serverId, 'dead_teleport', CONFIG.DEAD_SERVER_BLACKLIST_DURATION);
        console.log(`⚠️ Server ${serverId.slice(0,8)} reported dead by ${botId} (Error ${errorCode}) → blacklisted 3min`);
        return { action: 'blacklisted_short', reports: report.count };
    }
}

// =====================================================
// 🔒 SERVER MANAGEMENT
// =====================================================

function isServerAvailable(id) {
    if (isBlacklisted(id)) return false;
    const a = serverAssignments.get(id);
    if (a) {
        if (Date.now() < a.expires_at) return false;
        serverAssignments.delete(id);
        serverCooldowns.set(id, Date.now() + CONFIG.AUTO_COOLDOWN_ON_EXPIRE);
    }
    const cd = serverCooldowns.get(id);
    if (cd) {
        if (Date.now() < cd) return false;
        serverCooldowns.delete(id);
    }
    return true;
}

function assignServer(id, botId) {
    serverAssignments.set(id, {
        bot_id: botId,
        assigned_at: Date.now(),
        expires_at: Date.now() + CONFIG.ASSIGNMENT_DURATION
    });
    serverCooldowns.set(id, Date.now() + CONFIG.COOLDOWN_DURATION);
}

// =====================================================
// 📋 BOT HISTORY
// =====================================================

function getBotHistory(botId) {
    if (!botHistory.has(botId)) botHistory.set(botId, new Set());
    return botHistory.get(botId);
}

function addToBotHistory(botId, ids) {
    const h = getBotHistory(botId);
    for (const id of ids) h.add(id);
    if (h.size > CONFIG.MAX_BOT_HISTORY) {
        const arr = Array.from(h);
        botHistory.set(botId, new Set(arr.slice(arr.length - Math.floor(CONFIG.MAX_BOT_HISTORY / 2))));
    }
}

function botAlreadyScanned(botId, id) {
    const h = botHistory.get(botId);
    return h ? h.has(id) : false;
}

// =====================================================
// 🚦 RATE LIMITING
// =====================================================

function checkBotRateLimit(botId) {
    const last = botLastRequest.get(botId);
    const now = Date.now();
    if (last && (now - last) < CONFIG.BOT_REQUEST_COOLDOWN) {
        return { allowed: false, wait_ms: CONFIG.BOT_REQUEST_COOLDOWN - (now - last) };
    }
    botLastRequest.set(botId, now);
    return { allowed: true };
}

// =====================================================
// 🌐 FETCH — Continuous per-proxy loops
// =====================================================

async function fetchChainWithProxy(proxy, maxPages, pageDelay, sortOrder, cancelFlag) {
    const baseLabel = proxy ? proxy.label : 'DIRECT';
    const label = `${baseLabel}-${sortOrder}`;
    const servers = [];
    let cursor = null;
    let pageCount = 0;
    let consecutiveErrors = 0;
    let rotations = 0;

    while (pageCount < maxPages) {
        if (cancelFlag.cancelled) {
            console.log(`   🛑 [${label}] Cancelled at page ${pageCount}`);
            stats.total_stream_cancels++;
            break;
        }

        try {
            let url = `https://games.roblox.com/v1/games/${STEAL_A_BRAINROT.PLACE_ID}/servers/Public?sortOrder=${sortOrder}&limit=100`;
            if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;

            const response = await httpGet(url, proxy?.url || null, CONFIG.FETCH_PAGE_TIMEOUT);
            consecutiveErrors = 0;

            if (response && response.data) {
                const page = response.data
                    .filter(s => s.id && s.playing > 0)
                    .map(s => ({
                        id: s.id,
                        playing: s.playing,
                        maxPlayers: s.maxPlayers,
                        ping: s.ping || 0,
                        fetched_at: Date.now(),
                        source: label
                    }));

                servers.push(...page);
                cursor = response.nextPageCursor;
                pageCount++;

                if (pageCount % 10 === 0 || !cursor) {
                    console.log(`   📄 [${label}] Page ${pageCount}: ${servers.length} servers`);
                }

                if (!cursor) break;
                await sleep(pageDelay);
            } else {
                break;
            }
        } catch (err) {
            if (cancelFlag.cancelled) break;

            consecutiveErrors++;
            if (err.message === 'RATE_LIMITED') {
                console.warn(`   🚦 [${label}] Rate limited at page ${pageCount + 1}`);
                if (proxy) {
                    proxy.errors++;
                    proxy.lastError = Date.now();
                    if (rotations < CONFIG.MAX_ROTATIONS) {
                        rotateOneProxy(proxy);
                        rotations++;
                        console.warn(`   🔄 [${label}] Rotated to new IP (${rotations}/${CONFIG.MAX_ROTATIONS})`);
                        if (rotations <= CONFIG.ROTATION_NORESET_AFTER) {
                            consecutiveErrors = 0;
                        }
                    }
                }
                await sleep(CONFIG.FETCH_RATE_LIMIT_BACKOFF);
            } else {
                console.warn(`   ⚠️ [${label}] Page ${pageCount + 1}: ${err.message}`);
                if (proxy) proxy.errors++;
                await sleep(1500);
            }
            if (consecutiveErrors >= CONFIG.FETCH_MAX_CONSECUTIVE_ERRORS) {
                console.error(`   ❌ [${label}] ${consecutiveErrors} consecutive errors, stopping`);
                break;
            }
        }
    }

    return { label, servers, pages: pageCount };
}

function mergeIntoCache(newServers, label) {
    if (newServers.length === 0) return 0;
    const now = Date.now();
    const mergedMap = new Map();
    for (const s of globalCache.jobs) {
        if (now - s.fetched_at < CONFIG.STALE_THRESHOLD) {
            mergedMap.set(s.id, s);
        }
    }
    let newCount = 0;
    for (const s of newServers) {
        if (!mergedMap.has(s.id)) newCount++;
        mergedMap.set(s.id, s);
    }
    globalCache.jobs = Array.from(mergedMap.values());
    globalCache.lastUpdate = now;
    return newCount;
}

async function proxyFetchLoop(proxy) {
    let cycleNum = 0;
    while (true) {
        cycleNum++;
        const sortOrder = cycleNum % 2 === 0 ? 'Asc' : 'Desc';
        const halfPages = Math.ceil(CONFIG.CONTINUOUS_PAGES_PER_PROXY / 2);
        rotateOneProxy(proxy);
        const cancelFlag = { cancelled: false };
        const startTime = Date.now();
        try {
            const result = await fetchChainWithProxy(proxy, halfPages, CONFIG.FETCH_PAGE_DELAY, sortOrder, cancelFlag);
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            if (result.servers.length > 0) {
                const newCount = mergeIntoCache(result.servers, `${proxy.label}/${sortOrder}`);
                console.log(`   ✅ [${proxy.label}/${sortOrder}] ${result.servers.length} fetched, +${newCount} new → Cache: ${globalCache.jobs.length} (${elapsed}s)`);
                stats.total_fetch_cycles++;
            } else {
                console.log(`   ⚪ [${proxy.label}/${sortOrder}] 0 servers (${elapsed}s)`);
            }
        } catch (e) {
            console.error(`   ❌ [${proxy.label}/${sortOrder}] Error: ${e.message}`);
        }
        await sleep(CONFIG.CONTINUOUS_FETCH_DELAY);
    }
}

async function initialBigFetch() {
    console.log('═'.repeat(60));
    console.log('🚀 INITIAL BIG FETCH — all proxies in parallel');
    console.log('═'.repeat(60));
    const startTime = Date.now();
    const halfPages = Math.ceil(CONFIG.INITIAL_PAGES_PER_PROXY / 2);
    const promises = [];
    for (const proxy of PROXY_POOL) {
        rotateOneProxy(proxy);
        console.log(`   🚀 ${proxy.label} ↓Desc (${halfPages} pages)`);
        promises.push(fetchChainWithProxy(proxy, halfPages, CONFIG.FETCH_PAGE_DELAY, 'Desc', { cancelled: false }));
        const proxyClone = { ...proxy, url: proxy.baseUrl, errors: 0 };
        rotateOneProxy(proxyClone);
        console.log(`   🚀 ${proxy.label} ↑Asc  (${halfPages} pages)`);
        promises.push(fetchChainWithProxy(proxyClone, halfPages, CONFIG.FETCH_PAGE_DELAY, 'Asc', { cancelled: false }));
    }
    console.log(`   ⏳ ${promises.length} streams...\n`);
    const results = await Promise.allSettled(promises);
    let totalRaw = 0, totalNew = 0;
    for (const r of results) {
        if (r.status === 'fulfilled' && r.value.servers.length > 0) {
            const newCount = mergeIntoCache(r.value.servers, r.value.label);
            totalRaw += r.value.servers.length;
            totalNew += newCount;
            console.log(`   ✅ [${r.value.label}] ${r.value.servers.length} servers (${r.value.pages} pages)`);
        }
    }
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n' + '═'.repeat(60));
    console.log(`✅ Initial fetch: ${totalRaw} raw → ${totalNew} unique → Cache: ${globalCache.jobs.length} (${elapsed}s)`);
    console.log('═'.repeat(60) + '\n');
}

async function startContinuousFetching() {
    await initialBigFetch();
    console.log('✅ Backend v4.0 ready!\n');
    console.log('🔄 Starting continuous fetch loops...\n');
    for (let i = 0; i < PROXY_POOL.length; i++) {
        const proxy = PROXY_POOL[i];
        setTimeout(() => {
            console.log(`   🔁 [${proxy.label}] Continuous loop started`);
            proxyFetchLoop(proxy).catch(e => {
                console.error(`❌ [${proxy.label}] Loop crashed: ${e.message}`);
            });
        }, i * 2000);
    }

    // Purge stale servers every minute
    setInterval(() => {
        const now = Date.now();
        const before = globalCache.jobs.length;
        globalCache.jobs = globalCache.jobs.filter(s => now - s.fetched_at < CONFIG.STALE_THRESHOLD);
        const purged = before - globalCache.jobs.length;
        if (purged > 0) console.log(`   🧹 Purged ${purged} stale → Cache: ${globalCache.jobs.length}`);
    }, 60000);
}

// =====================================================
// 🔑 API KEY
// =====================================================

function verifyApiKey(req, res, next) {
    const validKey = process.env.API_KEY;
    if (!validKey) return res.status(500).json({ error: 'API_KEY not set' });
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== validKey) return res.status(401).json({ error: 'Invalid API key' });
    next();
}

// =====================================================
// 🎯 JOB ASSIGNMENT — ZERO COLLISION
// =====================================================

async function handleJobAssignment(bot_id, vps_id, res) {
    let botRegion = null;
    for (const [region, config] of Object.entries(REGIONAL_CONFIG)) {
        if (vps_id >= config.vps_range[0] && vps_id <= config.vps_range[1]) {
            botRegion = region;
            break;
        }
    }
    if (!botRegion) return res.status(400).json({ error: `Invalid VPS ID: ${vps_id}` });

    const rl = checkBotRateLimit(bot_id);
    if (!rl.allowed) {
        stats.total_rate_limited++;
        return res.status(429).json({ error: 'Too fast', retry_in_ms: rl.wait_ms });
    }

    if (globalCache.jobs.length === 0) {
        return res.status(503).json({ error: 'Cache empty', retry_in: 10 });
    }

    await globalAssignmentLock.acquire();
    if (globalAssignmentLock.queueLength > stats.lock_max_queue) {
        stats.lock_max_queue = globalAssignmentLock.queueLength;
    }

    try {
        let skipBL = 0, skipAssign = 0, skipHist = 0;
        const available = [];
        for (const job of globalCache.jobs) {
            if (isBlacklisted(job.id)) { skipBL++; continue; }
            if (!isServerAvailable(job.id)) { skipAssign++; continue; }
            if (botAlreadyScanned(bot_id, job.id)) { skipHist++; continue; }
            available.push(job);
        }
        stats.total_blacklist_filtered += skipBL;
        stats.total_duplicates_skipped += skipHist;

        if (available.length === 0) {
            const h = botHistory.get(bot_id);
            if (h && h.size > 0) {
                console.log(`🔄 ${bot_id}: History full (${h.size}), resetting`);
                h.clear();
                const retry = globalCache.jobs.filter(j => !isBlacklisted(j.id) && isServerAvailable(j.id));
                if (retry.length > 0) return doAssign(retry, bot_id, botRegion, res);
            }
            return res.status(503).json({
                error: 'All servers busy/scanned',
                cached: globalCache.jobs.length,
                skipped: { assigned: skipAssign, blacklisted: skipBL, history: skipHist },
                retry_in: 5
            });
        }

        return doAssign(available, bot_id, botRegion, res);
    } finally {
        globalAssignmentLock.release();
    }
}

function doAssign(available, bot_id, botRegion, res) {
    const sorted = available
        .map(j => ({ ...j, _h: deterministicHashNum(j.id, bot_id) }))
        .sort((a, b) => a._h - b._h);

    const count = Math.min(CONFIG.SERVERS_PER_BOT, sorted.length);
    const candidates = sorted.slice(0, count);
    const finalIds = [];
    let collisionsDetected = 0;

    for (const job of candidates) {
        const existing = serverAssignments.get(job.id);
        if (existing && existing.bot_id !== bot_id && Date.now() < existing.expires_at) {
            collisionsDetected++;
            stats.total_collisions_detected++;
            continue;
        }
        finalIds.push(job.id);
    }

    if (collisionsDetected > 0 && sorted.length > count) {
        const extra = sorted.slice(count, count + collisionsDetected);
        for (const job of extra) {
            const existing = serverAssignments.get(job.id);
            if (!existing || existing.bot_id === bot_id || Date.now() >= existing.expires_at) {
                finalIds.push(job.id);
                stats.total_collisions_resolved++;
            }
            if (finalIds.length >= count) break;
        }
    }

    for (const id of finalIds) {
        assignServer(id, bot_id);
    }

    addToBotHistory(bot_id, finalIds);
    stats.total_assignments++;

    const histSize = getBotHistory(bot_id).size;
    const lockQ = globalAssignmentLock.queueLength;

    console.log(`✅ [${botRegion}] ${bot_id}: ${finalIds.length} servers | Pool: ${available.length}/${globalCache.jobs.length} | Hist: ${histSize}${lockQ > 0 ? ` | Queue: ${lockQ}` : ''}${collisionsDetected > 0 ? ` | ⚠️ ${collisionsDetected} collisions!` : ''}`);

    res.json({
        success: true,
        job_ids: finalIds,
        region: botRegion,
        count: finalIds.length,
        available_servers: available.length,
        total_cached: globalCache.jobs.length,
        place_id: STEAL_A_BRAINROT.PLACE_ID,
        history_size: histSize,
        cache_age_s: Math.floor((Date.now() - globalCache.lastUpdate) / 1000),
        collisions_detected: collisionsDetected
    });
}

// =====================================================
// 🎯 ENDPOINTS
// =====================================================

app.post('/api/v1/get-job-assignment', verifyApiKey, async (req, res) => {
    try {
        stats.total_requests++;
        const { bot_id, vps_id } = req.body;
        if (!bot_id || vps_id === undefined) return res.status(400).json({ error: 'Missing: bot_id, vps_id' });
        await handleJobAssignment(bot_id, parseInt(vps_id), res);
    } catch (e) { console.error('❌', e); res.status(500).json({ error: 'Internal error' }); }
});

app.get('/api/v1/get-job-assignment', verifyApiKey, async (req, res) => {
    try {
        stats.total_requests++;
        const { bot_id, vps_id } = req.query;
        if (!bot_id || !vps_id) return res.status(400).json({ error: 'Missing: bot_id, vps_id' });
        await handleJobAssignment(bot_id, parseInt(vps_id), res);
    } catch (e) { console.error('❌', e); res.status(500).json({ error: 'Internal error' }); }
});

app.post('/api/v1/report-found', verifyApiKey, (req, res) => {
    const { bot_id, job_id, brainrotName, value, serverId } = req.body;
    if (!bot_id || !job_id) return res.status(400).json({ error: 'Missing: bot_id, job_id' });
    const existing = reportedServers.get(job_id);
    const now = Date.now();
    if (existing && (now - existing.reported_at) < REPORT_DEDUP_DURATION) {
        return res.json({ success: true, already_reported: true, reported_by: existing.bot_id });
    }
    reportedServers.set(job_id, { bot_id, reported_at: now });

    // ✅ v4.0: Also push to alerts store for the notifier
    if (brainrotName) {
        addAlert({
            brainrotName: brainrotName,
            value: value || 'Unknown',
            botId: bot_id,
            serverId: serverId || job_id,
            timestamp: now,
        });
        console.log(`📢 ${bot_id}: found ${brainrotName} [${value}] in ${(serverId || job_id).slice(0, 8)} → alert pushed`);
    } else {
        console.log(`📢 ${bot_id}: reported server ${job_id.substring(0, 8)}… (first report)`);
    }

    return res.json({ success: true, already_reported: false });
});

// ✅ v4.0: NEW ENDPOINT — Bot reports teleport failure
app.post('/api/v1/report-dead', verifyApiKey, (req, res) => {
    const { bot_id, job_id, error_code } = req.body;
    if (!bot_id || !job_id) return res.status(400).json({ error: 'Missing: bot_id, job_id' });

    const result = reportDeadServer(job_id, bot_id, error_code || 0);

    res.json({
        success: true,
        server_id: job_id,
        ...result,
        total_blacklisted: serverBlacklist.size,
    });
});

app.post('/api/v1/report-restricted', verifyApiKey, (req, res) => {
    const { bot_id, job_id, reason } = req.body;
    if (!bot_id || !job_id) return res.status(400).json({ error: 'Missing: bot_id, job_id' });
    blacklistServer(job_id, reason || 'restricted');
    res.json({ success: true, total_blacklisted: serverBlacklist.size });
});

app.post('/api/v1/clear-history', verifyApiKey, (req, res) => {
    const { bot_id } = req.body;
    if (!bot_id) return res.status(400).json({ error: 'Missing: bot_id' });
    const h = botHistory.get(bot_id);
    const sz = h ? h.size : 0;
    if (h) h.clear();
    res.json({ success: true, cleared: sz });
});

// ── 📊 Scan complete — bot confirms real hop done ──
app.post('/scan-complete', (req, res) => {
    scanTimestamps.push(Date.now());
    totalScans++;
    res.json({ ok: true, hops_min: getHopsPerMinute(), total: totalScans });
});

// ✅ v4.0: Alerts endpoint for Uchiwa notifier
app.get('/alerts', verifyApiKey, (req, res) => {
    const since = parseInt(req.query.since) || 0;
    const filtered = alertsStore.filter(a => a.timestamp > since);
    res.json({
        alerts: filtered,
        count: filtered.length,
        total: alertsStore.length,
        server_time: Date.now(),
    });
});

app.get('/api/v1/stats', (req, res) => {
    const avail = globalCache.jobs.filter(j => isServerAvailable(j.id)).length;
    res.json({
        game: STEAL_A_BRAINROT,
        version: '4.0',
        cache: {
            total: globalCache.jobs.length,
            available: avail,
            assigned: serverAssignments.size,
            cooldowns: serverCooldowns.size,
            blacklisted: serverBlacklist.size,
            reported_dedup: reportedServers.size,
            age_s: globalCache.lastUpdate ? Math.floor((Date.now() - globalCache.lastUpdate) / 1000) : -1,
        },
        bots: {
            tracked: botHistory.size,
            ...stats,
            uptime_s: Math.floor((Date.now() - stats.uptime_start) / 1000)
        },
        hops: {
            real_hops_per_min: getHopsPerMinute(),
            real_hops_5m_avg: getHopsPerMinute5m(),
            total_scans: totalScans,
        },
        dead_servers: {
            total_reports: stats.total_dead_reports,
            total_blacklisted: stats.total_dead_blacklisted,
            pending_reports: deadServerReports.size,
        },
        alerts: {
            stored: alertsStore.length,
        },
        lock: {
            queue_now: globalAssignmentLock.queueLength,
            queue_max_ever: stats.lock_max_queue,
            collisions_detected: stats.total_collisions_detected,
            collisions_resolved: stats.total_collisions_resolved
        },
        proxies: PROXY_POOL.map(p => ({
            label: p.label,
            errors: p.errors,
            status: p.errors >= 10 ? 'degraded' : 'active'
        })),
        regions: Object.fromEntries(
            Object.entries(REGIONAL_CONFIG).map(([r, c]) => [r, c])
        ),
        config: {
            cooldown_s: CONFIG.COOLDOWN_DURATION / 1000,
            servers_per_bot: CONFIG.SERVERS_PER_BOT,
            stale_threshold_s: CONFIG.STALE_THRESHOLD / 1000,
            dead_blacklist_s: CONFIG.DEAD_SERVER_BLACKLIST_DURATION / 1000,
            dead_extended_s: CONFIG.DEAD_EXTENDED_BLACKLIST / 1000,
            proxy_pool_size: PROXY_POOL.length
        }
    });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok', version: '4.0',
        servers: globalCache.jobs.length,
        hops_min: getHopsPerMinute(),
        total_scans: totalScans,
        uptime: Math.floor(process.uptime()),
        dead_reports: stats.total_dead_reports,
        memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024)
    });
});

// =====================================================
// 🧹 CLEANUP — every 10s
// =====================================================

setInterval(() => {
    const now = Date.now();
    let cA = 0, cC = 0, cB = 0, cR = 0, cD = 0;
    for (const [id, a] of serverAssignments.entries()) {
        if (now > a.expires_at) {
            serverAssignments.delete(id);
            serverCooldowns.set(id, now + CONFIG.AUTO_COOLDOWN_ON_EXPIRE);
            cA++;
        }
    }
    for (const [id, exp] of serverCooldowns.entries()) {
        if (now > exp) { serverCooldowns.delete(id); cC++; }
    }
    for (const [id, e] of serverBlacklist.entries()) {
        if (now > e.expires_at) { serverBlacklist.delete(id); cB++; }
    }
    for (const [id, ts] of botLastRequest.entries()) {
        if (now - ts > 60000) botLastRequest.delete(id);
    }
    for (const [id, r] of reportedServers.entries()) {
        if (now - r.reported_at > REPORT_DEDUP_DURATION) { reportedServers.delete(id); cR++; }
    }
    // ✅ v4.0: Clean old dead reports
    for (const [id, r] of deadServerReports.entries()) {
        if (now - r.lastReport > CONFIG.DEAD_EXTENDED_BLACKLIST) {
            deadServerReports.delete(id);
            cD++;
        }
    }
    if (cA + cC + cB + cR + cD > 0) {
        console.log(`🧹 ${cA} assign→cd, ${cC} cd expired, ${cB} bl expired${cR > 0 ? `, ${cR} reports` : ''}${cD > 0 ? `, ${cD} dead-reports` : ''}`);
    }
}, CONFIG.CLEANUP_INTERVAL);

// Trim bot history every 30min
setInterval(() => {
    let trimmed = 0;
    for (const [botId, h] of botHistory.entries()) {
        if (h.size > CONFIG.MAX_BOT_HISTORY) {
            const arr = Array.from(h);
            const keep = new Set(arr.slice(arr.length - Math.floor(CONFIG.MAX_BOT_HISTORY / 2)));
            trimmed += h.size - keep.size;
            botHistory.set(botId, keep);
        }
    }
    if (trimmed > 0) console.log(`🧹 Trimmed ${trimmed} history entries`);
}, 1800000);

// Trim alerts store every minute
setInterval(() => {
    const cutoff = Date.now() - CONFIG.ALERTS_MAX_AGE;
    const before = alertsStore.length;
    while (alertsStore.length > 0 && alertsStore[0].timestamp < cutoff) {
        alertsStore.shift();
    }
    const removed = before - alertsStore.length;
    if (removed > 0) console.log(`🧹 Trimmed ${removed} old alerts → ${alertsStore.length} remaining`);
}, 60000);

// =====================================================
// 💓 KEEP-ALIVE — Prevent Render free tier sleep
// =====================================================

function startKeepAlive() {
    const appUrl = process.env.RENDER_EXTERNAL_URL || process.env.APP_URL;
    if (!appUrl) {
        console.log('⚠️ No RENDER_EXTERNAL_URL or APP_URL set — keep-alive disabled');
        console.log('   Set APP_URL env var to your Render URL to prevent sleep');
        return;
    }

    console.log(`💓 Keep-alive: pinging ${appUrl}/health every ${CONFIG.KEEP_ALIVE_INTERVAL / 1000}s`);

    setInterval(async () => {
        try {
            const res = await fetch(`${appUrl}/health`);
            if (res.ok) {
                const data = await res.json();
                console.log(`💓 Keep-alive OK — ${data.servers} servers, ${data.hops_min} hops/min`);
            } else {
                console.warn(`💓 Keep-alive responded ${res.status}`);
            }
        } catch (err) {
            console.warn(`💓 Keep-alive failed: ${err.message}`);
        }
    }, CONFIG.KEEP_ALIVE_INTERVAL);
}

// =====================================================
// 🚀 STARTUP
// =====================================================

app.listen(PORT, () => {
    console.clear();
    console.log('\n' + '═'.repeat(60));
    console.log('🧠 STEAL A BRAINROT SCANNER - BACKEND v4.0');
    console.log('   ⚡ INSTANT COOLDOWN + ZERO COLLISION');
    console.log('   💀 DEAD SERVER FEEDBACK LOOP');
    console.log('   📢 INTEGRATED ALERTS (notifier support)');
    console.log('   💓 RENDER KEEP-ALIVE');
    console.log('═'.repeat(60));
    console.log(`🎮 ${STEAL_A_BRAINROT.GAME_NAME}`);
    console.log(`📍 Place ID: ${STEAL_A_BRAINROT.PLACE_ID}`);
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`🔑 API Key: ${process.env.API_KEY ? '✅' : '❌ NOT SET!'}`);
    console.log('');

    initProxyPool();
    startKeepAlive();

    const totalBots = Object.values(REGIONAL_CONFIG).reduce((s, c) => s + c.expected_bots, 0);

    console.log('\n🔒 ANTI-COLLISION:');
    console.log('   Global lock + SHA-256 + history');

    console.log('\n💀 DEAD SERVER FEEDBACK (v4.0):');
    console.log(`   1 report → blacklist ${CONFIG.DEAD_SERVER_BLACKLIST_DURATION / 1000}s`);
    console.log(`   ${CONFIG.DEAD_REPORT_THRESHOLD}+ reports → blacklist ${CONFIG.DEAD_EXTENDED_BLACKLIST / 1000}s`);
    console.log(`   Stale threshold: ${CONFIG.STALE_THRESHOLD / 1000}s (was 600s in v3.9)`);

    console.log('\n⚡ FETCH:');
    console.log(`   🌐 ${PROXY_POOL.length} proxies — ALL running continuously`);
    console.log(`   🚀 Phase 1: Initial — ${CONFIG.INITIAL_PAGES_PER_PROXY} pages/proxy`);
    console.log(`   🔁 Phase 2: Continuous — ${CONFIG.CONTINUOUS_PAGES_PER_PROXY} pages, ${CONFIG.CONTINUOUS_FETCH_DELAY / 1000}s gap`);

    console.log('\n📢 ALERTS:');
    console.log(`   Max age: ${CONFIG.ALERTS_MAX_AGE / 1000}s | Max count: ${CONFIG.ALERTS_MAX_COUNT}`);
    console.log('   GET /alerts?since=<timestamp> (for Uchiwa notifier)');

    console.log('\n📊 CAPACITY:');
    console.log(`   🤖 ${totalBots} bots × ${CONFIG.SERVERS_PER_BOT} servers = ${(totalBots * CONFIG.SERVERS_PER_BOT).toLocaleString()}/cycle`);

    console.log('');
    for (const [, c] of Object.entries(REGIONAL_CONFIG)) {
        console.log(`   🌍 ${c.name.padEnd(16)} VPS ${c.vps_range[0]}-${c.vps_range[1]}  (${c.expected_bots} bots)`);
    }
    console.log('═'.repeat(60) + '\n');

    startContinuousFetching().catch(e => {
        console.error('❌ Startup failed:', e.message);
    });
});

process.on('unhandledRejection', (e) => console.error('❌ Unhandled rejection:', e?.message || e));
process.on('uncaughtException', (e) => console.error('❌ Uncaught exception:', e?.message || e));
