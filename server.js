const path = require('path');
const { WebcastPushConnection } = require('tiktok-live-connector');
const express = require('express');
const fs = require('fs');
const { print } = require('pdf-to-printer');
const PDFDocument = require('pdfkit');

const app = express();
const PORT = 3000;

let clients = [];

// ========================================================
// CONFIGURATION
// ========================================================
const TIKTOK_USERNAME = "jsjdenimcollection";
const USE_REAL_PRINTER = false;

console.log(`[Printer Engine] Starting monitor for @${TIKTOK_USERNAME}...`);

let reconnectAttempts = 0;
let reconnectTimeout = null;

let lastEventTime= Date.now();
const MAX_RECONNECT_ATTEMPTS = 8;

// setInterval(() => {
//     const now = Date.now();

//     if (now - lastEventTime > 90000) {
//         console.log("⚠️ No activity detected. Forcing reconnect...");

//         try {
//             tiktokConnection.disconnect();
//         } catch {}
//         connectTikTok();
//     }
// }, 10000);
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
     lastEventTime = Date.now();
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
    enableWebsocketUpgrade: false,
    requestOptions: {
        timeout: 20000,
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'https://www.tiktok.com/',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive'
        }
    }
});

let isConnecting = false;

function connectTikTok() {
    
    if (isConnecting) return;

    if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
        console.log("❌ Max reconnect attempts reached. Cooling down 60s...");
        
        setTimeout(() => {
            reconnectAttempts = 0;
            connectTikTok();
        }, 60000);

        return;
    }

    isConnecting = true;
    console.log(`🔄 Connecting... (Attempt ${reconnectAttempts + 1})`);

    tiktokConnection.connect()
        .then((state) => {
            isConnecting = false;
            reconnectAttempts = 0; // ✅ reset on success
            lastEventTime = Date.now();

            console.log(`\n==============================`);
            console.log(`[✅ CONNECTED] @${TIKTOK_USERNAME}`);
            console.log(`Room ID: ${state.roomId || "N/A"}`);
            console.log(`==============================\n`);
        })
        .catch(err => {
            isConnecting = false;
            reconnectAttempts++;

            console.log(`❌ Connection failed (${reconnectAttempts})`);

            const delay = Math.min(10000 + reconnectAttempts * 2000, 30000);
            console.log(`⏳ Retry in ${delay / 1000}s`);

            setTimeout(connectTikTok, delay);
        });

}

connectTikTok();

// let reconnectTimeout;

tiktokConnection.on('disconnected', () => {
    console.log('⚠️ Disconnected from TikTok');

    if (reconnectTimeout) return;

    reconnectAttempts++;
    const delay = Math.min(500 + reconnectAttempts * 2000, 30000);

    reconnectTimeout = setTimeout(() => {
        reconnectTimeout = null;
        console.log(`🔁 Reconnecting in ${delay / 1000}s...`);
        connectTikTok();
    }, delay);
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
    lastEventTime = Date.now();

    const user = data.uniqueId;
    const text = data.comment?.trim();
    const isShortMessage = text.length <= 6;
    const containsNumber = /\d/.test(text);

    if (!text) return;

    // ✅ STRICT PIN DETECTION ONLY
    const isPinned =
        data.isPinned === true ||
        data.eventAttributes?.isPinned === true ||
        data.eventAttributes?.pinnedToTop === true;

    if (isPinned || (isShortMessage && containsNumber)) {
        console.log("📌 REAL/LIKELY PIN:", user, "→", text);
        enqueueJob(user, text);
    }
});

tiktokConnection.on('error', (err) => {
    console.error('[TikTok Error]', err);
});

// ========================================================
// REAL PRINTER (SAFE PLACEHOLDER)
// ========================================================
async function realPrint(username, commentText) {
    return new Promise((resolve, reject) => {
        const filePath = path.join(__dirname, `receipt-${Date.now()}.pdf`);
        const doc = new PDFDocument({ size: [220, 300], margin: 10 });

        const stream = fs.createWriteStream(filePath);
        doc.pipe(stream);

        const timestamp = new Date().toLocaleTimeString();

        // Title
        doc.fontSize(14).text("🧾 ORDER TICKET", { align: "center" });
        doc.moveDown();

        // Divider
        doc.fontSize(10).text("----------------------------", { align: "center" });
        doc.moveDown();

        // Content
        doc.fontSize(10).text(`USER : @${username}`);
        doc.text(`ORDER: ${commentText}`);
        doc.text(`TIME : ${timestamp}`);

        doc.moveDown();
        doc.text("----------------------------", { align: "center" });

        doc.moveDown();
        doc.fontSize(9).text("TikTok Live Order Feed", { align: "center" });

        doc.end();

        stream.on('finish', async () => {
            try {
                await print(filePath); // ✅ sends to default printer
                fs.unlinkSync(filePath); // cleanup
                console.log("🖨️ Printed receipt");
                resolve();
            } catch (err) {
                reject(err);
            }
        });
    });
}
// ========================================================
// SIMULATED PRINTER
// ========================================================
function simulateTerminalPrint(username, commentText) {
    return new Promise((resolve) => {
        const timestamp = new Date().toLocaleTimeString();

        const lineWidth = 28;

        function padLine(label, value) {
            const text = `${label} ${value}`;
            return text.padEnd(lineWidth, ' ');
        }

        console.log("\n🖨️  Printing Receipt...\n");

        console.log("┌" + "─".repeat(lineWidth) + "┐");
        console.log("│" + "🧾 ORDER TICKET".padStart(19).padEnd(lineWidth) + "│");
        console.log("├" + "─".repeat(lineWidth) + "┤");
        console.log("│" + padLine("USER :", "@" + username).slice(0, lineWidth) + "│");
        console.log("│" + padLine("ORDER:", commentText).slice(0, lineWidth) + "│");
        console.log("│" + padLine("TIME :", timestamp).slice(0, lineWidth) + "│");
        console.log("├" + "─".repeat(lineWidth) + "┤");
        console.log("│" + "TikTok Live Order Feed".padEnd(lineWidth) + "│");
        console.log("└" + "─".repeat(lineWidth) + "┘\n");

        setTimeout(() => {
            console.log("[✅ Receipt Printed]");
            resolve();
        }, 1000);
    });
}

// ========================================================
// TEST PIN
// ========================================================
setTimeout(() => {
    console.log("\n🧪 Test pin firing...");
    enqueueJob("test_user", "This is a test pin ✅");
}, 5000);