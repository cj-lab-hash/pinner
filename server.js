const path = require('path');
const { WebcastPushConnection } = require('tiktok-live-connector');
const express = require('express');

const app = express();
const PORT = 3000;

let clients = [];

// ========================================================
// CONFIGURATION
// ========================================================
const TIKTOK_USERNAME = "sorellaph13";
const USE_REAL_PRINTER = false;

console.log(`[Printer Engine] Starting monitor for @${TIKTOK_USERNAME}...`);

// ========================================================
// MIDDLEWARE
// ========================================================
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

// ========================================================
// SSE STREAM
// ========================================================
app.get('/stream-pins', (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
    });

    clients.push(res);
    console.log(`[Dashboard Connected] Total: ${clients.length}`);

    const interval = setInterval(() => {
        res.write(': keep-alive\n\n');
    }, 15000);

    req.on('close', () => {
        clearInterval(interval);
        clients = clients.filter(client => client !== res);
        console.log(`[Dashboard Disconnected] Total: ${clients.length}`);
    });
});

// ========================================================
// ROUTES
// ========================================================
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', (req, res) => {
    res.json({
        status: "ok",
        clients: clients.length,
        queue: queue.length,
        isPrinting
    });
});

app.listen(PORT, () => {
    console.log(`[Data Server Online] Listening on port ${PORT}`);
});

// ========================================================
// SEND DATA TO DASHBOARD
// ========================================================
function sendPinToHtmlDashboard(username, commentText) {
    const dataPayload = JSON.stringify({ username, commentText });

    clients = clients.filter(client => {
        try {
            client.write(`data: ${dataPayload}\n\n`);
            return true;
        } catch {
            return false;
        }
    });
}

// ========================================================
// PRINT QUEUE SYSTEM
// ========================================================
let isPrinting = false;
let queue = [];
const MAX_QUEUE = 50;

function enqueueJob(user, text) {
    if (queue.length >= MAX_QUEUE) {
        console.log("⚠️ Queue full, skipping...");
        return;
    }

    queue.push({ user, text });
    sendPinToHtmlDashboard(user, text);
    processQueue();
}

async function processQueue() {
    if (isPrinting || queue.length === 0) return;

    isPrinting = true;
    const job = queue.shift();

    try {
        if (USE_REAL_PRINTER) {
            await realPrint(job.user, job.text);
        } else {
            await simulateTerminalPrint(job.user, job.text);
        }
    } catch (err) {
        console.error("Print failed:", err);
    }

    isPrinting = false;
    processQueue();
}

// ========================================================
// TIKTOK CONNECTION
// ========================================================
const recentPins = new Set();

function handlePinnedEvent(data) {
    const user =
        data.uniqueId ||
        data.user?.uniqueId ||
        data.user?.displayId ||
        data.user?.nickname ||
        "Anonymous";

    const text =
        data.comment ||
        data.content ||
        data.title ||
        "No text available";

    const id = `${user}:${text}`;

    if (recentPins.has(id)) return;
    recentPins.add(id);

    setTimeout(() => recentPins.delete(id), 5000);

    console.log("📌 PIN DETECTED:", user, text);
    enqueueJob(user, text);
}

const tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData: false,
    enableWebsocketUpgrade: true,
    requestOptions: {
        timeout: 15000,
        headers: {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.tiktok.com/'
        }
    }
});

let isConnecting = false;

function connectTikTok() {
    if (isConnecting) return;
    isConnecting = true;

    tiktokConnection.connect()
        .then((state) => {
            isConnecting = false;

            console.log(`\n====================================`);
            console.log(`[Connected] Watching @${TIKTOK_USERNAME}`);
            console.log(`Room ID: ${state.roomId || "N/A"}`);
            console.log(`====================================\n`);
        })
        .catch(err => {
            isConnecting = false;

            console.error(`[Error] ${err.message || err}`);
            console.log("[Retrying in 10s...]");

            setTimeout(connectTikTok, 10000);
        });
}

connectTikTok();

let reconnectTimeout;

tiktokConnection.on('disconnected', () => {
    console.log('[Reconnect] Lost connection');

    if (reconnectTimeout) return;

    reconnectTimeout = setTimeout(() => {
        reconnectTimeout = null;
        connectTikTok();
    }, 5000);
});

// ========================================================
// EVENT LISTENERS
// ========================================================
tiktokConnection.on('roomPin', handlePinnedEvent);

tiktokConnection.on('roomUser', (data) => {
    if (data.pinnedMessage) {
        handlePinnedEvent(data.pinnedMessage);
    }
});

tiktokConnection.on('chat', (data) => {
    if (
        data.isPinned ||
        data.eventAttributes?.isPinned ||
        data.eventAttributes?.pinnedToTop
    ) {
        console.log("📌 PIN CHAT:", data.uniqueId, data.comment);
        handlePinnedEvent(data);
    }
});

tiktokConnection.on('error', (err) => {
    console.error('[TikTok Error]', err);
});

// ========================================================
// REAL PRINTER (SAFE PLACEHOLDER)
// ========================================================
async function realPrint(username, commentText) {
    console.log("🖨️ Real printer not configured yet");
}

// ========================================================
// SIMULATED PRINTER
// ========================================================
function simulateTerminalPrint(username, commentText) {
    return new Promise((resolve) => {
        const timestamp = new Date().toLocaleTimeString();

        console.log(`\n🖨️ Printing for @${username}`);
        console.log(`Time: ${timestamp}`);
        console.log(`"${commentText}"`);

        setTimeout(() => {
            console.log("[Done Printing]");
            resolve();
        }, 1500);
    });
}

// ========================================================
// TEST PIN
// ========================================================
setTimeout(() => {
    console.log("\n🧪 Test pin firing...");
    enqueueJob("test_user", "This is a test pin ✅");
}, 5000);