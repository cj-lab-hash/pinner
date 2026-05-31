const fs = require('fs');
const path = require('path');
const { WebcastPushConnection } = require('tiktok-live-connector');
const express = require('express');

const app = express();
const PORT = 3000;

let clients = [];

// ========================================================
// CONFIGURATION (Set to a creator who is LIVE right now)
// ========================================================
const TIKTOK_USERNAME = "sorellaph13"; 

console.log(`[Printer Engine] Starting monitor for @${TIKTOK_USERNAME}...`);

app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    next();
});

// Create the data stream pathway for your HTML file to listen to
app.get('/stream-pins', (req, res) => {
    // 1. Send the official 200 OK handshake immediately
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    });
    
    clients.push(res);
    console.log(`[Dashboard Connected] A monitoring window just opened! (Total: ${clients.length})`);
    
    // 2. Send a tiny invisible "ping" so the browser knows the line is officially open
    res.write(': ping\n\n');
    
    req.on('close', () => {
        clients = clients.filter(client => client !== res);
        console.log(`[Dashboard Disconnected] Window closed. (Total: ${clients.length})`);
    });
});
app.listen(PORT, () => {
    console.log(`[Data Server Online] Listening for dashboard connections on port ${PORT}`);
});
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});
function sendPinToHtmlDashboard(username, commentText) {
    const dataPayload = JSON.stringify({ username, commentText });
    clients.forEach(client => client.write(`data: ${dataPayload}\n\n`));
}

// ========================================================
// PRINT QUEUE SYSTEM (Simulated for testing)
// ========================================================
let isPrinting = false;
let queue = [];

function enqueueJob(user, text) {
    queue.push({ user, text });
    processQueue();
    sendPinToHtmlDashboard(user, text);
}

async function processQueue() {
    if (isPrinting || queue.length === 0) return;
    isPrinting = true;
    const job = queue.shift();
    
    // Simulate the time it takes for a printer to physically roll out paper (1.5 seconds)
    await simulateTerminalPrint(job.user, job.text);
    
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
        .then(() => {
            console.log(`\n==================================================`);
            console.log(`[Connected] Successfully watching @${TIKTOK_USERNAME}!`);
            console.log(`Waiting for a comment to be pinned on TikTok...`);
            console.log(`==================================================\n`);
        })
        .catch(err => {
            console.error(`[Error] Connection Failed: ${err.message || err}`);
            console.log(`[Retry] Attempting to reconnect in 10 seconds...`);
            setTimeout(connectTikTok, 10000);
        });
}
connectTikTok();

tiktokConnection.on('disconnected', () => {
    console.log('[Reconnect] Connection lost. Reconnecting in 5s...');
    setTimeout(connectTikTok, 5000);
});

// ========================================================
// EVENT LISTENERS
// ========================================================
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
// THE DEDICATED PIN CATCHER
// ========================================================
tiktokConnection.on('roomPin', (data) => {
    // 1. Alert the terminal that a pin was caught!
    console.log(`\n🚨 [DEBUG] PIN EVENT DETECTED BY TIKTOK API!`);
    
    // 2. Extract the data (TikTok's data structure changes sometimes, so we check a few spots)
    const pinnedData = data.pinMessage || data.pinnedMessage || data;
    
    // 3. Dig out the username and text
    const user = pinnedData.user?.displayId || pinnedData.user?.nickname || "Anonymous";
    const text = pinnedData.content || pinnedData.comment || pinnedData.title || "No text available";
    
    // 4. Send it to the printer and dashboard!
    enqueueJob(user, text);
});
// ========================================================
// VISUAL TERMINAL LOGGING (Your Simulated Printer)
// ========================================================
function simulateTerminalPrint(username, commentText) {
    return new Promise((resolve) => {
        const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        
        console.log(`\n🖨️  [VIRTUAL PRINTER] Spooling ticket for @${username}...`);
        console.log(`┌────────────────────────────────────────────────┐`);
        console.log(`│ 📌 TIKTOK LIVE PIN TICKET         ⏰ ${timestamp} │`);
        console.log(`├────────────────────────────────────────────────┤`);
        console.log(`│ USER: @${username.padEnd(39)} │`);
        
        // Wrap text cleanly if the comment is long
        const maxLineLength = 45;
        let text = commentText;
        while (text.length > 0) {
            let chunk = text.slice(0, maxLineLength);
            text = text.slice(maxLineLength);
            console.log(`│ "${chunk.padEnd(44)}" │`);
        }
        
        console.log(`├────────────────────────────────────────────────┤`);
        console.log(`│    Live Stream Order / Interaction Ticket      │`);
        console.log(`└────────────────────────────────────────────────┘\n`);
        
        setTimeout(() => {
            console.log(`[Printer Success] Ticket printed smoothly.`);
            resolve();
        }, 1500); 
    });
}
setTimeout(() => {
    console.log(`\n🧪 [TEST] Simulating a fake pin in 5 seconds...`);
    enqueueJob("test_user_99", "Hello! This is a test to make sure the dashboard works!");
}, 5000);