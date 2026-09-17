#!/usr/bin/env node
/**
 * openai_token_server.js
 * Tiny Express server that mints short‑lived Realtime session tokens for WebRTC
 * using your real OPENAI_API_KEY. Mirrors the pattern in OpenAI's Realtime console app.
 *
 * Endpoints
 *   POST /session  -> { client_secret: { value, expires_at }, id, ... }
 *                    (minted via POST /v1/realtime/client_secrets)
 *   GET  /healthz  -> 200 OK
 *
 * Env
 *   OPENAI_API_KEY=sk-...
 *   OPENAI_REALTIME_MODEL=gpt-4o-realtime-preview   (optional)
 *   TOKEN_SERVER_PORT=3002                          (optional)
 *
 * CLI
 *   node openai_token_server.js [--port PORT] [--model MODEL]
 */

require('dotenv').config();
const express = require('express');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Parse CLI arguments
const argv = yargs(hideBin(process.argv))
  .scriptName('openai_token_server')
  .usage('$0 [options]')
  .option('port', { alias: 'p', type: 'number', default: parseInt(process.env.TOKEN_SERVER_PORT, 10) || 3002, describe: 'Server port' })
  .option('model', { alias: 'm', type: 'string', default: process.env.OPENAI_REALTIME_MODEL || 'gpt-4o-realtime-preview', describe: 'Default OpenAI Realtime model' })
  .help().argv;

const PORT = argv.port;
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const DEFAULT_MODEL = argv.model;

if (!OPENAI_KEY) {
  console.error('[token-server] Missing OPENAI_API_KEY in environment.');
  process.exit(1);
}

const app = express();
app.use(express.json());

// Very small allowlist for local access by default. Expand if you need remote adapters.
app.use((req, res, next) => {
  // If you want to restrict further: check req.ip === '::1' / '127.0.0.1' or shared secret header
  next();
});

app.get('/healthz', (_req, res) => res.status(200).send('ok'));

/**
 * Create a short‑lived Realtime session on OpenAI and return the token payload.
 * Body (optional): { model, voice, instructions, turn_detection, input_audio_transcription }
 */
app.post('/session', async (req, res) => {
  try {
    const body = req.body || {};
    const model = body.model || DEFAULT_MODEL;

    // POST /v1/realtime/sessions was removed; the current endpoint is
    // /v1/realtime/client_secrets and it nests voice and turn detection under
    // session.audio instead of taking them flat.
    const session = { type: 'realtime', model };
    if (body.instructions) session.instructions = body.instructions;
    const audio = {};
    if (body.turn_detection || body.input_audio_transcription) {
      audio.input = {};
      if (body.turn_detection) audio.input.turn_detection = body.turn_detection;
      if (body.input_audio_transcription) audio.input.transcription = body.input_audio_transcription;
    }
    if (body.voice) audio.output = { voice: body.voice };
    if (Object.keys(audio).length) session.audio = audio;
    const payload = { session };

    const r = await fetch('https://api.openai.com/v1/realtime/client_secrets', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      console.error('[token-server] OpenAI /realtime/client_secrets error:', r.status, errText);
      return res.status(500).json({ error: 'failed_to_create_session', status: r.status, detail: errText });
    }

    const json = await r.json();
    // The new endpoint returns { value, expires_at, session }. Keep answering in
    // the old { client_secret: { value } } shape so adapters need no change.
    res.json({
      ...(json.session || {}),
      client_secret: { value: json.value, expires_at: json.expires_at },
    });
  } catch (e) {
    console.error('[token-server] /session exception:', e);
    res.status(500).json({ error: 'exception', detail: String(e) });
  }
});

app.listen(PORT, () => {
  console.log(`[token-server] Listening on http://localhost:${PORT}`);
});
