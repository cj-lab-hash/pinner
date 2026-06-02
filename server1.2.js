require("dotenv").config();
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
// const TIKTOK_USERNAME = "d.vellaclothing1";
// const USE_REAL_PRINTER = false;

const TIKTOK_USERNAME = process.env.SELLER || "default_username";
const USE_REAL_PRINTER = process.env.PRINTER === "true";


console.log(`[Printer Engine] Starting monitor for @${TIKTOK_USERNAME}...`);
// console.log("Loaded API Key:", process.env.EULERSTREAM_API_KEY ? "✅ Found" : "❌ Missing");
console.log("Printer mode:", USE_REAL_PRINTER ? "🖨️ Real printer" : "🖥️ Simulated printer");

let reconnectAttempts = 0;
let lastEventTime = Date.now();
let rateLimitResetTime = null;
let isRateLimited = false;

const MAX_RECONNECT_ATTEMPTS = 5; // Reduced from 8
const MIN_RETRY_DELAY = 30000; // Start with 30 seconds
const MAX_RETRY_DELAY = 300000; // Max 5 minutes
// const RATE_LIMIT_COOLDOWN = 900000; // 15 minutes cooldown
const RATE_LIMIT_COOLDOWN = 3600000; // 1 hour instead of 15 min
const DEDUP_TIMEOUT = 5000;

const userAgents = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36'
];

function getRandomUserAgent() {
    return userAgents[Math.floor(Math.random() * userAgents.length)];
}

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
        isPrinting,
        tiktokStatus: isRateLimited ? "rate_limited" : (isConnecting ? "connecting" : "connected"),
        rateLimitReset: rateLimitResetTime ? new Date(rateLimitResetTime).toLocaleTimeString() : null
    });
});

app.listen(PORT, () => {
    console.log(`[Data Server Online] Listening on port ${PORT}`);
});

// ========================================================
// SEND DATA TO DASHBOARD
// ========================================================
function sendPinToHtmlDashboard(username, commentText, isPinned = true) {
    const dataPayload = JSON.stringify({ 
        username, 
        commentText, 
        isPinned, 
        timestamp: new Date().toISOString() 
    });

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

function enqueueJob(user, text, isPinned = true) {
    if (queue.length >= MAX_QUEUE) {
        console.log("⚠️ Queue full, skipping...");
        return;
    }

    queue.push({ user, text, isPinned, timestamp: Date.now() });
    sendPinToHtmlDashboard(user, text, isPinned);
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


function calculateRetryDelay(attempt, isRateLimit = false) {
    if (isRateLimit) {
        return RATE_LIMIT_COOLDOWN;
    }
    const delay = Math.min(MIN_RETRY_DELAY + (attempt * 30000), MAX_RETRY_DELAY);
    return delay;
}

function formatTime(ms) {
    const minutes = Math.floor(ms / 60000);
    const seconds = Math.floor((ms % 60000) / 1000);
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`;
    }
    return `${seconds}s`;
}

// ========================================================
// TIKTOK CONNECTION
// ========================================================
const recentPins = new Map();

const tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData: false,
    enableWebsocketUpgrade: false,
    requestOptions: {
        timeout: 20000,
        headers: {
            'User-Agent': getRandomUserAgent(),
            'Referer': 'https://www.tiktok.com/',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'X-Requested-With': 'XMLHttpRequest'
        }
    },
    eulerStreamApiKey: process.env.EULERSTREAM_API_KEY

});

let isConnecting = false;

function connectTikTok() {
    if (isConnecting) return;

    // Check if we're rate limited
    if (isRateLimited) {
        const now = Date.now();
        const timeUntilReset = rateLimitResetTime - now;
        
        if (timeUntilReset > 0) {
            console.log(`\n⚠️  RATE LIMITED - Waiting until ${new Date(rateLimitResetTime).toLocaleTimeString()}`);
            console.log(`⏳ Time remaining: ${formatTime(timeUntilReset)}\n`);
            
            setTimeout(() => {
                isRateLimited = false;
                rateLimitResetTime = null;
                reconnectAttempts = 0;
                console.log("✅ Rate limit cooldown complete. Attempting to reconnect...\n");
                connectTikTok();
            }, timeUntilReset + 5000);
            
            return;
        } else {
            isRateLimited = false;
            rateLimitResetTime = null;
            reconnectAttempts = 0;
        }
    }

    // Check if we've exceeded max attempts
    if (reconnectAttempts > MAX_RECONNECT_ATTEMPTS) {
        console.log(`\n❌ Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached.`);
        console.log("⚠️  Please restart the application or check your connection.\n");
        return;
    }

    isConnecting = true;
    console.log(`\n🔄 Connecting... (Attempt ${reconnectAttempts + 1}/${MAX_RECONNECT_ATTEMPTS})`);

    tiktokConnection.connect()
        .then((state) => {
            isConnecting = false;
            reconnectAttempts = 0;
            lastEventTime = Date.now();
            isRateLimited = false;

            console.log(`\n==============================`);
            console.log(`[✅ CONNECTED] @${TIKTOK_USERNAME}`);
            console.log(`Room ID: ${state.roomId || "N/A"}`);
            console.log(`==============================\n`);
        })
        .catch(err => {
            isConnecting = false;
            
            // Check for rate limit error
            if (err.reason === 'Rate Limited' || (err.message && err.message.includes('Rate Limited'))) {
                isRateLimited = true;
                rateLimitResetTime = err.resetTime || (Date.now() + RATE_LIMIT_COOLDOWN);
                
                console.log(`\n🚨 RATE LIMITED BY TIKTOK!`);
                console.log(`⚠️  Too many connection attempts in a short time.`);
                console.log(`📍 Reset time: ${new Date(rateLimitResetTime).toLocaleTimeString()}`);
                console.log(`⏳ Cooldown: ${formatTime(RATE_LIMIT_COOLDOWN)}\n`);
                
                setTimeout(connectTikTok, 5000);
                return;
            }

            // Normal connection error
            reconnectAttempts++;
            const delay = calculateRetryDelay(reconnectAttempts - 1);
            
            console.log(`❌ Connection failed (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
            console.log(`📊 Error: ${err.message || err}`);
            console.log(`⏳ Retry in ${formatTime(delay)}...\n`);

            setTimeout(connectTikTok, delay);
        });
}

connectTikTok();

tiktokConnection.on('disconnected', () => {
    console.log('\n⚠️ Disconnected from TikTok');
    isConnecting = false;
    
    const delay = calculateRetryDelay(reconnectAttempts);
    console.log(`🔁 Will retry in ${formatTime(delay)}...\n`);

    setTimeout(() => {
        if (!isRateLimited) {
            reconnectAttempts++;
            connectTikTok();
        }
    }, delay);
});

// ========================================================
// INTERCEPT MESSAGE PARSING - DEEP HOOK
// ========================================================

// Store original message handlers
const originalEmit = tiktokConnection.emit.bind(tiktokConnection);
let capturedPinData = null;


tiktokConnection.emit = function(eventName, ...args) {
    // Only intercept if WebcastRoomPinMessage is detected
    if (args.length > 0) {
        const fullDataStr = JSON.stringify(args);
        
        if (fullDataStr.includes('WebcastRoomPinMessage')) {
            // Try to extract data from all arguments
            for (let i = 0; i < args.length; i++) {
                const arg = args[i];
                const argStr = JSON.stringify(arg);
                
                if (argStr.includes('uniqueId') || argStr.includes('comment') || argStr.includes('nickname')) {
                    handleRoomPin(arg);
                    break;
                }
            }
        }
    }

    return originalEmit(eventName, ...args);
};

// ========================================================
// EVENT LISTENERS
// ========================================================

tiktokConnection.on('chat', (data) => {
    lastEventTime = Date.now();
    const user = data.uniqueId || "Unknown";
    const comment = data.comment || "No text";
    console.log(`💬 CHAT: @${user}: ${comment}`);
});

tiktokConnection.on('roomPin', (data) => {
    console.log("\n🎯 ROOMPIP EVENT FIRED!\n");
    handleRoomPin(data);
});

// Listen to all events
tiktokConnection.on('member', () => {});
tiktokConnection.on('like', () => {});
tiktokConnection.on('gift', () => {});
tiktokConnection.on('follow', () => {});
tiktokConnection.on('share', () => {});

tiktokConnection.on('error', (err) => {
    console.error('[TikTok Error]', err.message || err);
});

// ========================================================
// ROOM PIN HANDLER
// ========================================================

function handleRoomPin(data) {
    try {
        let user = "Unknown";
        let text = "Unknown";

        // THE ACTUAL STRUCTURE - Extract from chatMessage object
        if (data.chatMessage) {
            const msg = data.chatMessage;
            
            // Get user nickname
            if (msg.user?.nickname) {
                user = msg.user.nickname;
            }
            
            // Get comment text
            if (msg.comment) {
                text = msg.comment;
            }
        }

        // Fallback extraction methods
        if (user === "Unknown") {
            if (data.user?.nickname) user = data.user.nickname;
            if (data.nickname) user = data.nickname;
        }

        if (text === "Unknown") {
            const dataStr = JSON.stringify(data);
            const commentMatch = dataStr.match(/"comment"\s*:\s*"([^"]{1,100})"/);
            if (commentMatch && commentMatch[1]) text = commentMatch[1];
        }

        console.log(`\n📌 EXTRACTED: User="${user}", Text="${text}"\n`);

        if (text === "Unknown" || text.length === 0) {
            console.log("⚠️ Could not extract meaningful text");
            return;
        }

        const id = `${user}:${text}`;

        if (recentPins.has(id)) {
            console.log("⏭️  Duplicate pin skipped");
            return;
        }

        recentPins.set(id, Date.now());
        setTimeout(() => recentPins.delete(id), DEDUP_TIMEOUT);

        console.log("╔════════════════════════════════════════╗");
        console.log("║     🔖 PINNED COMMENT DETECTED! 🔖    ║");
        console.log("╠════════════════════════════════════════╣");
        console.log(`║ User: ${String(user).substring(0, 32).padEnd(33)}║`);
        console.log(`║ Text: ${String(text).substring(0, 32).padEnd(33)}║`);
        console.log("╚════════════════════════════════════════╝\n");

        enqueueJob(user, text, true);
    } catch (err) {
        console.error("❌ Error handling room pin:", err.message);
    }
}

// ========================================================
// REAL PRINTER
// ========================================================
async function realPrint(username, commentText) {
    return new Promise((resolve, reject) => {
        const filePath = path.join(__dirname, `receipt-${Date.now()}.pdf`);
        const doc = new PDFDocument({ size: [220, 300], margin: 10 });

        const stream = fs.createWriteStream(filePath);
        doc.pipe(stream);

        const timestamp = new Date().toLocaleTimeString();

        doc.fontSize(14).text("🧾 ORDER TICKET", { align: "center" });
        doc.moveDown();
        doc.fontSize(10).text("----------------------------", { align: "center" });
        doc.moveDown();
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
                await print(filePath);
                fs.unlinkSync(filePath);
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
    enqueueJob("test_user", "This is a test pin ✅", true);
}, 5000);