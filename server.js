const fs = require('fs');
const path = require('path');
const { WebcastPushConnection } = require('tiktok-live-connector');
const html_to_pdf = require('html-pdf-node');
const ptp = require('pdf-to-printer');
const express = require('express');

const app = express();
const PORT = 3000;

// Track active HTML dashboard connections
let clients = [];

// ========================================================
// CONFIGURATION
// ========================================================
const TIKTOK_USERNAME = "thefittingshop2025.ii"; // Change to your clean text handle
// const PRINTER_NAME = "Rollo Printer";

console.log(`[Printer Engine] Starting monitor for @${TIKTOK_USERNAME}...`);

// Allow your separate HTML file to connect securely from your computer
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

// Create the data stream pathway for your HTML file to listen to
app.get('/stream-pins', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    
    clients.push(res);
    
    req.on('close', () => {
        clients = clients.filter(client => client !== res);
    });
});

app.listen(PORT, () => {
    console.log(`[Data Server Online] Listening for dashboard connections on port ${PORT}`);
});

// Helper to broadcast live pins to the HTML file
function sendPinToHtmlDashboard(username, commentText) {
    const dataPayload = JSON.stringify({ username, commentText });
    clients.forEach(client => client.write(`data: ${dataPayload}\n\n`));
}

// ========================================================
// UTILITIES & QUEUE SYSTEM
// ========================================================
function escapeHtml(text = "") {
    return text
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

let isPrinting = false;
let queue = [];

function enqueueJob(user, text) {
    // 1. Send immediately to your physical printer queue
    queue.push({ user, text });
    processQueue();

    // 2. Broadcast immediately to your visual HTML file
    sendPinToHtmlDashboard(user, text);
}

async function processQueue() {
    if (isPrinting || queue.length === 0) return;
    isPrinting = true;
    const job = queue.shift();
    await generateAndPrintLabel(job.user, job.text);
    isPrinting = false;
    processQueue();
}

// ========================================================
// TIKTOK CONNECTION
// ========================================================
const tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData: true,
    enableWebsocketUpgrade: false,
    requestOptions: {
        timeout: 10000,
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
    }
});

function connectTikTok() {
    tiktokConnection.connect()
        .then(() => console.log(`[Connected] Watching for pinned items to print!`))
        .catch(err => {
            console.error(`[Error] Connection Failed: ${err.message || err}`);
            console.log(`[Retry] Attempting to reconnect in 10 seconds...`);
            setTimeout(connectTikTok, 10000);
        });
}
connectTikTok();


// ========================================================
// EVENT LISTENERS
// ========================================================
tiktokConnection.on(`disconnected`,() => {
    console.log(`[Reconnect] Connection lost. Reconnecting in 5 seconds`);
    setTimeout(connectTikTok, 5000);
});

tiktokConnection.on('roomUser', (data) => {
    if (data.pinnedMessage) {
        const user = data.pinnedMessage.user?.displayId || data.pinnedMessage.user?.nickname || "Anonymous";
        const text = data.pinnedMessage.content;
        enqueueJob(user, text);
    }
});

tiktokConnection.on('chat', (data) => {
    const isPinned = data.isPinned || data.eventAttributes?.isPinned || data.eventAttributes?.pinnedToTop;
    if (isPinned) {
        enqueueJob(data.uniqueId, data.comment);
    }
});

// ========================================================
// PRINT FUNCTION
// ========================================================
async function generateAndPrintLabel(username, commentText) {
    const safeUsername = escapeHtml(username);
    const safeComment = escapeHtml(commentText);
    console.log(`[Processing Print Job] Preparing label for @${safeUsername}`);
    const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            @page { size: 2in 3in; margin: 0; }
            body { font-family: Arial, sans-serif; margin: 0; padding: 12px; background: #fff; color: #000; display: flex; flex-direction: column; justify-content: space-between; height: 2.7in; box-sizing: border-box; }
            .header { border-bottom: 2px dashed #000; padding-bottom: 4px; display: flex; justify-content: space-between; }
            .title { font-size: 11px; font-weight: bold; }
            .time { font-size: 9px; color: #555; }
            .user-tag { font-size: 14px; font-weight: bold; margin-top: 8px; word-break: break-all; }
            .msg-body { font-size: 13px; margin-top: 6px; flex-grow: 1; word-wrap: break-word; }
            .footer { font-size: 8px; text-align: center; border-top: 1px solid #000; padding-top: 4px; font-style: italic; }
        </style>
    </head>
    <body>
        <div>
            <div class="header">
                <span class="title">📌 TIKTOK LIVE PIN</span>
                <span class="time">${timestamp}</span>
            </div>
            <div class="user-tag">@${safeUsername}</div>
            <div class="msg-body">"${safeComment}"</div>
        </div>
        <div class="footer">Live Stream Order/Interaction Ticket</div>
    </body>
    </html>
    `;

    const options = { width: '2in', height: '3in', printBackground: true };
    const tempPdfPath = path.join(__dirname, `label_${Date.now()}_${Math.floor(Math.random() * 1000)}.pdf`);

    try {
        const pdfBuffer = await html_to_pdf.generatePdf({ content: htmlContent }, options);
        await fs.promises.writeFile(tempPdfPath, pdfBuffer);
        await ptp.print(tempPdfPath, {});
        console.log(`[Printer Success] Printed for @${safeUsername}`);
        await fs.promises.unlink(tempPdfPath);
    } catch (err) {
        console.error(`[ERROR] Print pipeline failed:`, err);
        if (fs.existsSync(tempPdfPath)) {
            try { await fs.promises.unlink(tempPdfPath); } catch {}
        }
    }
}