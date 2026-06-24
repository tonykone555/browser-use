import express from 'express';
import cors from 'cors';
import { WebSocketServer, WebSocket } from 'ws';
import { v4 as uuidv4 } from 'uuid';
import multer from 'multer';
import axios from 'axios';
import http from 'http';
import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import path from 'path';
import fs from 'fs';

if (getApps().length === 0) {
  if (process.env.FIREBASE_PRIVATE_KEY) {
    initializeApp({
      credential: cert({
        projectId: process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      }),
    });
  } else {
    initializeApp();
  }
}
const db = getFirestore();

const GROQ_API_KEY = process.env.GROQ_API_KEY || '';
const GROQ_MODEL = 'qwen/qwen3-27b';
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const GITHUB_REPO = 'tonykone555/Assix';

// Trigger GitHub Actions instantly
const triggerGitHubActions = async () => {
  if (!GITHUB_TOKEN) return;
  try {
    await axios.post(
      `https://api.github.com/repos/${GITHUB_REPO}/actions/workflows/browser-agent.yml/dispatches`,
      { ref: 'main' },
      { headers: { Authorization: `Bearer ${GITHUB_TOKEN}`, Accept: 'application/vnd.github.v3+json' } }
    );
    console.log('GitHub Actions triggered');
  } catch (e: any) {
    console.log('GitHub trigger failed:', e.message);
  }
};

const callGroq = async (messages: { role: string; content: string }[], retries = 3): Promise => {
  if (!GROQ_API_KEY) return 'GROQ_API_KEY not configured.';
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await axios.post(
        'https://api.groq.com/openai/v1/chat/completions',
        { model: GROQ_MODEL, messages, max_tokens: 500, temperature: 0.3 },
        { headers: { Authorization: `Bearer ${GROQ_API_KEY}`, 'Content-Type': 'application/json' } }
      );
      return res.data.choices[0]?.message?.content || '';
    } catch (e: any) {
      const msg = e.response?.data?.error?.message || e.message || '';
      if (msg.includes('Rate limit') && attempt < retries - 1) {
        await new Promise(r => setTimeout(r, (attempt + 1) * 8000));
        continue;
      }
      return 'Groq error: ' + msg;
    }
  }
  return 'Groq error: max retries reached';
};

const callLLM = async (systemPrompt: string, userPrompt: string): Promise =>
  callGroq([{ role: 'system', content: systemPrompt }, { role: 'user', content: userPrompt }]);

const callLLMChat = async (systemPrompt: string, messages: any[]): Promise =>
  callGroq([
    { role: 'system', content: systemPrompt },
    ...messages.map(m => ({ role: m.role === 'agent' ? 'assistant' : 'user', content: m.msg || m.content || '' })),
  ]);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

const wsClients = new Map();
const latestScreenshots = new Map();
const taskLogs = new Map();

app.use(cors());
app.use(express.json({ limit: '50mb' }));

wss.on('connection', (ws: WebSocket & { taskId?: string }) => {
  ws.on('message', (data: string) => {
    try {
      const { type, taskId } = JSON.parse(data);
      if (type === 'subscribe') { wsClients.set(taskId, ws); ws.taskId = taskId; }
      if (type === 'unsubscribe') wsClients.delete(taskId);
    } catch (e) {}
  });
  ws.on('close', () => { if (ws.taskId) wsClients.delete(ws.taskId); });
});

const sendWS = (taskId: string, data: any) => {
  const client = wsClients.get(taskId);
  if (client && client.readyState === 1) client.send(JSON.stringify(data));
};

setInterval(async () => {
  for (const [taskId, ws] of wsClients.entries()) {
    if (ws.readyState !== 1) continue;
    try {
      const doc = await db.collection('assix_tasks').doc(taskId).get();
      const data = doc.data();
      if (!data) continue;
      if (data.latestScreenshot && data.latestScreenshot !== latestScreenshots.get(taskId)) {
        latestScreenshots.set(taskId, data.latestScreenshot);
        ws.send(JSON.stringify({ type: 'screenshot', taskId, imageBase64: data.latestScreenshot }));
      }
      ws.send(JSON.stringify({ type: 'status', taskId, progress: data.progress || 0, total: data.total || 0, progressPct: data.progressPct || 0, status: data.status }));
      const existingLogs = taskLogs.get(taskId) || [];
      const recentLogs = data.recentLogs || [];
      if (recentLogs.length > existingLogs.length) {
        const newLogs = recentLogs.slice(existingLogs.length);
        taskLogs.set(taskId, recentLogs);
        for (const log of newLogs) ws.send(JSON.stringify({ type: 'log', taskId, ...log }));
      }
      if (data.status === 'complete' || data.status === 'error') {
        ws.send(JSON.stringify({ type: 'complete', taskId, results: { results: data.results || [] } }));
      }
    } catch (e) {}
  }
}, 2000);

const logAction = async (taskId: string, msg: string, type = 'info') => {
  const entry = { time: new Date().toLocaleTimeString('en-GB'), msg, type, timestamp: Date.now() };
  try { await db.collection('assix_tasks').doc(taskId).collection('logs').add(entry); } catch (e) {}
  sendWS(taskId, { type: 'log', taskId, ...entry });
  const logs = taskLogs.get(taskId) || [];
  logs.push(entry);
  if (logs.length > 50) logs.shift();
  taskLogs.set(taskId, logs);
};

const toCSV = (data: any[]) => {
  if (!data || !data.length) return 'No data';
  const headers = Object.keys(data[0]);
  const rows = data.map(row => headers.map(h => { const val = row[h] ?? ''; return typeof val === 'string' && (val.includes(',') || val.includes('"')) ? `"${val.replace(/"/g, '""')}"` : val; }).join(','));
  return [headers.join(','), ...rows].join('\n');
};

const pushToClose = async (lead: any) => {
  if (!process.env.CLOSE_API_KEY) return { error: 'No Close API key' };
  try {
    const res = await axios.post('https://api.close.com/api/v1/lead/', {
      name: lead.businessName || lead.name || 'Business',
      contacts: [{ name: lead.businessName || lead.name || 'Business', phones: lead.phone ? [{ phone: lead.phone, type: 'office' }] : [], emails: lead.email ? [{ email: lead.email, type: 'office' }] : [] }],
      custom: { city: lead.city, sector: lead.sector, lead_type: lead.leadType, market: lead.market || 'english_ca' }
    }, { auth: { username: process.env.CLOSE_API_KEY, password: '' } });
    return { success: true, closeId: res.data.id };
  } catch (e: any) { return { error: e.message }; }
};

app.get('/health', (req, res) => res.json({ status: 'ok', mode: 'github-actions', timestamp: Date.now() }));

app.get('/api/task/:taskId/screenshot', async (req, res) => {
  const { taskId } = req.params;
  const memImg = latestScreenshots.get(taskId);
  if (memImg) return res.json({ screenshot: memImg, timestamp: Date.now() });
  try {
    const doc = await db.collection('assix_tasks').doc(taskId).get();
    const data = doc.data();
    if (data?.latestScreenshot) { latestScreenshots.set(taskId, data.latestScreenshot); return res.json({ screenshot: data.latestScreenshot, timestamp: data.screenshotAt || Date.now() }); }
  } catch (e) {}
  res.json({ screenshot: null });
});

app.get('/api/task/:taskId/logs/live', async (req, res) => {
  const { taskId } = req.params;
  const memLogs = taskLogs.get(taskId);
  if (memLogs && memLogs.length > 0) return res.json({ logs: memLogs });
  try { const doc = await db.collection('assix_tasks').doc(taskId).get(); return res.json({ logs: doc.data()?.recentLogs || [] }); } catch (e) {}
  res.json({ logs: [] });
});

app.get('/api/task/:taskId/live', async (req, res) => {
  try {
    const { taskId } = req.params;
    const doc = await db.collection('assix_tasks').doc(taskId).get();
    const task = doc.exists ? doc.data() : null;
    res.json({ task, screenshot: latestScreenshots.get(taskId) || task?.latestScreenshot || null, logs: taskLogs.get(taskId) || task?.recentLogs || [] });
  } catch (e: any) { res.json({ task: null, screenshot: null, logs: [] }); }
});

app.get('/debug/test', async (req, res) => {
  try { await db.collection('assix_tasks').limit(1).get(); res.json({ success: true, mode: 'github-actions', groq: !!GROQ_API_KEY, github: !!GITHUB_TOKEN }); }
  catch (e: any) { res.json({ success: false, error: e.message }); }
});

app.post('/api/task/start', async (req, res) => {
  try {
    const { taskType, config = {}, label } = req.body;
    const taskId = uuidv4();
    await db.collection('assix_tasks').doc(taskId).set({
      taskId, taskType, label: label || taskType, config,
      status: 'queued', progress: 0,
      total: config.maxLeads || config.targets?.length || 10,
      createdAt: new Date().toISOString(), runner: 'github-actions',
    });
    await logAction(taskId, 'Task queued — triggering GitHub runner...');
    await triggerGitHubActions(); // INSTANT TRIGGER
    res.json({ taskId });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.post('/api/task/dynamic', async (req, res) => {
  try {
    const { goal, context, url } = req.body;
    const taskId = uuidv4();
    await db.collection('assix_tasks').doc(taskId).set({
      taskId, taskType: 'dynamic', label: `AI: ${goal.slice(0, 40)}`,
      config: { goal, context, url }, status: 'queued', progress: 0, total: 10,
      createdAt: new Date().toISOString(), runner: 'github-actions',
    });
    await logAction(taskId, 'Task queued — triggering GitHub runner...');
    await triggerGitHubActions(); // INSTANT TRIGGER
    res.json({ taskId });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/task/:taskId/status', async (req, res) => {
  try {
    const doc = await db.collection('assix_tasks').doc(req.params.taskId).get();
    if (!doc.exists) return res.status(404).json({ error: 'Not found' });
    const logs = await db.collection('assix_tasks').doc(req.params.taskId).collection('logs').orderBy('timestamp').limit(100).get();
    res.json({ task: doc.data(), logs: logs.docs.map(d => d.data()) });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks/all', async (req, res) => {
  try { const s = await db.collection('assix_tasks').orderBy('createdAt', 'desc').limit(50).get(); res.json(s.docs.map(d => d.data())); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks/completed', async (req, res) => {
  try { const s = await db.collection('assix_tasks').where('status', '==', 'complete').orderBy('createdAt', 'desc').get(); res.json(s.docs.map(d => d.data())); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/tasks/active', async (req, res) => {
  try { const s = await db.collection('assix_tasks').where('status', 'in', ['running', 'queued']).get(); res.json(s.docs.map(d => d.data())); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.post('/api/task/:taskId/resolve', async (req, res) => {
  try { await db.collection('assix_tasks').doc(req.params.taskId).update({ resolved: true, status: 'running' }); res.json({ success: true }); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/task/:taskId', async (req, res) => {
  try { await db.collection('assix_tasks').doc(req.params.taskId).delete(); res.json({ success: true }); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/task/:taskId/export/csv', async (req, res) => {
  try {
    const snap = await db.collection('leads').where('taskId', '==', req.params.taskId).get();
    let data = snap.docs.map(d => d.data());
    if (data.length === 0) { const t = await db.collection('assix_tasks').doc(req.params.taskId).get(); if (t.exists && t.data()?.results) data = t.data()?.results; }
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="assix-${req.params.taskId}.csv"`);
    res.send(toCSV(data));
  } catch (err: any) { res.status(500).send(err.message); }
});

app.get('/api/task/:taskId/report', async (req, res) => {
  try {
    const doc = await db.collection('assix_tasks').doc(req.params.taskId).get();
    if (!doc.exists) return res.status(404).json({ error: 'Not found' });
    const task = doc.data() || {};
    if (task.report) return res.json({ report: task.report });
    const leads = await db.collection('leads').where('taskId', '==', req.params.taskId).get();
    const report = await callLLM('You are a market intelligence analyst.', `Task: ${task.taskType}\nCity: ${task.config?.city}\nNiche: ${task.config?.niche}\nLeads: ${leads.size}\n\n## Executive Summary\n## Lead Analysis\n## Recommended Pitch\n## Next Steps`);
    await db.collection('assix_tasks').doc(req.params.taskId).update({ report });
    res.json({ report });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.post('/api/console/message', upload.array('files'), async (req, res) => {
  try {
    const { message, taskId = 'general' } = req.body;
    const histSnap = await db.collection('assix_tasks').doc(taskId).collection('messages').orderBy('timestamp').limit(20).get();
    const messages = histSnap.docs.map(d => d.data());
    const userEntry = { role: 'user', msg: message, timestamp: Date.now() };
    await db.collection('assix_tasks').doc(taskId).collection('messages').add(userEntry);
    messages.push(userEntry);
    const response = await callLLMChat('You are Assix Agent — an intelligent browser automation assistant. Be concise and direct.', messages);
    await db.collection('assix_tasks').doc(taskId).collection('messages').add({ role: 'agent', msg: response, timestamp: Date.now() });
    res.json({ response });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/leads/all', async (req, res) => {
  try { const s = await db.collection('leads').orderBy('createdAt', 'desc').limit(200).get(); res.json(s.docs.map(d => ({ leadId: d.id, ...d.data() }))); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.post('/api/leads/push-close-batch', async (req, res) => {
  try {
    const snap = await db.collection('leads').where('sentToClose', '==', false).limit(50).get();
    let pushed = 0; let failed = 0;
    for (const doc of snap.docs) {
      const r = await pushToClose(doc.data());
      if ('success' in r) { await doc.ref.update({ sentToClose: true }); pushed++; } else failed++;
      await new Promise(r => setTimeout(r, 600));
    }
    res.json({ pushed, failed });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.get('/api/sessions/all', async (req, res) => {
  try { const s = await db.collection('assix_sessions').get(); res.json(s.docs.map(d => ({ platform: d.id, savedAt: d.data()?.savedAt }))); }
  catch (err: any) { res.status(500).json({ error: err.message }); }
});

app.post('/api/scrape/universal', async (req, res) => {
  try {
    const { url, extract } = req.body;
    const taskId = uuidv4();
    await db.collection('assix_tasks').doc(taskId).set({
      taskId, taskType: 'universal_scrape', label: `Scrape: ${url.slice(0, 40)}`,
      config: { url, extract }, status: 'queued', progress: 0, total: 10,
      createdAt: new Date().toISOString(),
    });
    await logAction(taskId, 'Task queued — triggering GitHub runner...');
    await triggerGitHubActions();
    res.json({ taskId });
  } catch (err: any) { res.status(500).json({ error: err.message }); }
});

const publicDir = path.join(process.cwd(), 'public');
app.use(express.static(publicDir));
app.get('*', (req, res) => {
  const indexPath = path.join(publicDir, 'index.html');
  if (fs.existsSync(indexPath)) res.sendFile(indexPath);
  else res.json({ status: 'Assix API running', version: '3.0.0', mode: 'github-actions' });
});

const PORT = parseInt(process.env.PORT || '8080');
server.listen(PORT, '0.0.0.0', () => console.log(`Assix v3 running on port ${PORT}`));
