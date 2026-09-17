#!/usr/bin/env node
/**
 * eesi_adapter.js — Role B (examinee) adapter for EESI Nur Live.
 *
 * Bridges the orchestrator's WebRTC leg (48 kHz PCM16, strict 10 ms cadence)
 * to a Nur Live realtime WebSocket session (16 kHz PCM16 up, 24 kHz down,
 * base64 inside JSON frames).
 *
 * Three things make this more than a format converter:
 *
 *  - Nur emits its reply faster than real time, so the downlink is a playout
 *    queue drained on a 10 ms clock. Pushing deltas straight into the
 *    RTCAudioSource would compress the reply in time and corrupt both the
 *    recording and what the examiner hears.
 *  - The gateway decides barge-in from the client's playback state, so this
 *    reports `eesi.output.status` four times a second and flushes the queue on
 *    `eesi.nur.interrupted` — the same contract the console and LiveProbe keep.
 *  - Downsampling to 16 kHz runs through a windowed-sinc low-pass first.
 *    Decimating by 3 without one aliases speech into the band Nur's STT reads.
 *
 * Launched by single_conversation.sh as:
 *   node adapters/eesi_adapter.js --role B --signalUrl ws://localhost:PORT/signal
 * with EESI_API_KEY in the environment. EESI_BASE_URL defaults to dev.
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const wrtc = require('wrtc');
const { RTCAudioSource, RTCAudioSink } = wrtc.nonstandard;
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const ORCH_SR = parseInt(process.env.WIRE_SAMPLE_RATE, 10) || 48000;
const NUR_IN_SR = 16000;
const NUR_OUT_SR = 24000;
const UPLINK_MS = 40; // what the gateway expects per append
const ORCH_10MS = Math.floor(ORCH_SR * 0.01);
const STATUS_MS = 250;

const argv = yargs(hideBin(process.argv))
  .scriptName('eesi_adapter')
  .usage('$0 --role <A|B> --signalUrl <ws://host:port/signal> [opts]')
  .option('role', { type: 'string', choices: ['A', 'B'], demandOption: true })
  .option('signalUrl', { type: 'string', demandOption: true })
  .option('baseUrl', { type: 'string', default: process.env.EESI_BASE_URL || 'https://api.dev.eesi.ai' })
  .option('versionId', { type: 'string', default: process.env.EESI_NUR_VERSION || '' })
  .option('systemPrompt', { type: 'string', default: '', describe: 'Override Nur\'s serving persona; empty keeps it' })
  .option('log', { type: 'boolean', default: true })
  // Accepted and ignored: the launcher passes these to every non-GPT adapter.
  .option('moshiUrl', { type: 'string' })
  .option('tokenServer', { type: 'string' })
  .help().argv;

const ROLE = argv.role;
const LOG = argv.log;
const API_KEY = process.env.EESI_API_KEY || '';
if (!API_KEY) {
  console.error('[EESI] EESI_API_KEY is not set');
  process.exit(1);
}
const host = new URL(argv.baseUrl);
if (!['localhost', '127.0.0.1', 'api.dev.eesi.ai'].includes(host.hostname)) {
  console.error('[EESI] Use the local gateway or api.dev.eesi.ai');
  process.exit(1);
}

function log(...args) { if (LOG) console.log(`[EESI-${ROLE}]`, ...args); }

// ── Resampling ───────────────────────────────────────────────────────────────

/** Hamming-windowed sinc low-pass, normalised cutoff in cycles/sample. */
function lowpass(taps, cutoff) {
  const h = new Float32Array(taps);
  const mid = (taps - 1) / 2;
  let sum = 0;
  for (let i = 0; i < taps; i++) {
    const x = i - mid;
    const sinc = x === 0 ? 2 * cutoff : Math.sin(2 * Math.PI * cutoff * x) / (Math.PI * x);
    const window = 0.54 - 0.46 * Math.cos((2 * Math.PI * i) / (taps - 1));
    h[i] = sinc * window;
    sum += h[i];
  }
  for (let i = 0; i < taps; i++) h[i] /= sum;
  return h;
}

/** Streaming FIR + decimate by `factor`; keeps its tail across calls. */
class Decimator {
  constructor(factor, taps = 31) {
    this.factor = factor;
    this.h = lowpass(taps, 0.5 / factor * 0.9);
    this.tail = new Float32Array(taps - 1);
  }
  process(input) {
    const h = this.h;
    const padded = new Float32Array(this.tail.length + input.length);
    padded.set(this.tail, 0);
    padded.set(input, this.tail.length);
    const count = Math.floor((padded.length - h.length + 1) / this.factor);
    const out = new Float32Array(Math.max(0, count));
    for (let n = 0; n < count; n++) {
      let acc = 0;
      const base = n * this.factor;
      for (let k = 0; k < h.length; k++) acc += h[k] * padded[base + k];
      out[n] = acc;
    }
    const consumed = count * this.factor;
    this.tail = padded.slice(consumed, consumed + h.length - 1);
    return out;
  }
}

/** Linear interpolation up by 2; the source is already band-limited. */
function upsample2(f, carry) {
  const out = new Float32Array(f.length * 2);
  let prev = carry;
  for (let i = 0; i < f.length; i++) {
    out[i * 2] = 0.5 * (prev + f[i]);
    out[i * 2 + 1] = f[i];
    prev = f[i];
  }
  return { out, carry: prev };
}

function i16ToF32(buf) {
  const out = new Float32Array(buf.length / 2);
  for (let i = 0, j = 0; i < buf.length; i += 2, j++) out[j] = buf.readInt16LE(i) / 32768;
  return out;
}
function f32ToI16(f) {
  const out = Buffer.alloc(f.length * 2);
  for (let i = 0; i < f.length; i++) {
    out.writeInt16LE(Math.max(-32768, Math.min(32767, Math.round(f[i] * 32768))), i * 2);
  }
  return out;
}

// ── State ────────────────────────────────────────────────────────────────────

let orchPC, orchSource, orchSink, orchWS, nurWS;
let uplink = Buffer.alloc(0);          // 48 kHz PCM16 from the examiner
let playout = [];                      // 24 kHz PCM16 chunks Nur has sent
let playoutBytes = 0;
let upCarry = null, downCarry = 0;
let timers = [];
const decimator = new Decimator(3);    // 48 kHz → 16 kHz
const events = [];
const transcript = [];
let currentResponse = null;
let received = 0, rendered = 0, discarded = 0;
let started = Date.now();
const nowMs = () => Date.now() - started;

function record(type, extra = {}) {
  if (events.length < 8000) events.push({ type, at_ms: nowMs(), ...extra });
}

// ── Orchestrator leg ─────────────────────────────────────────────────────────

async function connectOrchestrator() {
  orchPC = new wrtc.RTCPeerConnection();
  orchSource = new RTCAudioSource({ sampleRate: ORCH_SR, channelCount: 1 });
  orchPC.addTrack(orchSource.createTrack());

  orchPC.ontrack = ({ track }) => {
    if (track.kind !== 'audio') return;
    if (orchSink) { try { orchSink.stop(); } catch { /* already stopped */ } }
    orchSink = new RTCAudioSink(track, { sampleRate: ORCH_SR, channelCount: 1, bitDepth: 16 });
    orchSink.ondata = ({ samples }) => {
      uplink = Buffer.concat([uplink, Buffer.from(samples.buffer, samples.byteOffset, samples.byteLength)]);
    };
  };

  orchWS = new WebSocket(argv.signalUrl);
  await new Promise((res) => orchWS.once('open', res));

  orchPC.onicecandidate = ({ candidate }) => {
    if (!candidate) return;
    orchWS.send(JSON.stringify({
      role: ROLE,
      type: 'candidate',
      candidate: { candidate: candidate.candidate, sdpMid: candidate.sdpMid, sdpMLineIndex: candidate.sdpMLineIndex },
    }));
  };

  const offer = await orchPC.createOffer();
  await orchPC.setLocalDescription(offer);
  orchWS.send(JSON.stringify({ role: ROLE, type: 'offer', sdp: offer.sdp }));

  orchWS.on('message', async (msg) => {
    const data = JSON.parse(msg);
    if (data.role !== ROLE) return;
    if (data.type === 'answer') {
      await orchPC.setRemoteDescription({ type: 'answer', sdp: data.sdp });
      log('orchestrator answer applied');
    } else if (data.type === 'candidate') {
      try { await orchPC.addIceCandidate(data.candidate); } catch { /* late candidate */ }
    }
  });
}

// ── Nur Live leg ─────────────────────────────────────────────────────────────

function realtimeUrl() {
  const scheme = host.protocol === 'https:' ? 'wss' : 'ws';
  const params = new URLSearchParams({ model: 'nur-realtime-v1', source: 'live' });
  if (argv.versionId) params.set('nur_version', argv.versionId);
  return `${scheme}://${host.host}/v1/realtime?${params.toString()}`;
}

async function connectNur() {
  nurWS = new WebSocket(realtimeUrl(), ['realtime'], {
    headers: { Authorization: `Bearer ${API_KEY}` },
    maxPayload: 16 * 1024 * 1024,
  });
  await new Promise((res, rej) => {
    nurWS.once('open', res);
    nurWS.once('error', rej);
  });
  log('connected to Nur Live');

  nurWS.on('message', (raw) => {
    let event;
    try { event = JSON.parse(raw.toString()); } catch { return; }
    if (!event || typeof event !== 'object') return;
    handleNurEvent(event);
  });
  nurWS.on('close', (code) => log('Nur socket closed', code));
  nurWS.on('error', (err) => log('Nur socket error', err.message));

  await new Promise((res) => {
    const onReady = (raw) => {
      let event;
      try { event = JSON.parse(raw.toString()); } catch { return; }
      if (event.type === 'session.updated') { nurWS.off('message', onReady); res(); }
    };
    nurWS.on('message', onReady);
  });
  log('session configured');
}

function configureSession() {
  // The console's own policy, minus tools: turn control and gateway barge-in
  // are what this benchmark is measuring, while offering web search would let
  // the gateway hold first audio behind a speculative search.
  nurWS.send(JSON.stringify({
    type: 'eesi.nur.configure',
    server_tasks: false,
    turn_control: true,
    barge_in: 'gateway',
    time_zone: 'UTC',
  }));
  nurWS.send(JSON.stringify({
    type: 'session.update',
    session: {
      type: 'realtime',
      instructions: argv.systemPrompt,
      tools: [],
      audio: {
        input: { format: { type: 'audio/pcm' } },
        output: { format: { type: 'audio/pcm', rate: NUR_OUT_SR } },
      },
    },
  }));
}

function flushPlayout() {
  for (const chunk of playout) discarded += chunk.length / 2;
  playout = [];
  playoutBytes = 0;
}

function handleNurEvent(event) {
  const kind = event.type;
  if (kind === 'session.created') { configureSession(); return; }
  if (kind === 'response.created') { currentResponse = event.response?.id || 'unknown'; return; }
  if (kind === 'response.output_audio.delta' || kind === 'response.audio.delta' || kind === 'eesi.backchannel') {
    const rate = event.sample_rate || NUR_OUT_SR;
    const pcm = Buffer.from(event.delta || event.audio || '', 'base64');
    if (!pcm.length || pcm.length % 2) return;
    if (rate !== NUR_OUT_SR) { log('unexpected output rate', rate); return; }
    if (received === 0) record('audio.first', { response_id: currentResponse });
    received += pcm.length / 2;
    playout.push(pcm);
    playoutBytes += pcm.length;
    if (kind === 'eesi.backchannel') record(kind, { text: event.text });
    return;
  }
  if (kind === 'eesi.nur.interrupted') {
    flushPlayout();
    record(kind, { reason: event.reason, after_ms: event.after_ms });
    return;
  }
  if (kind === 'response.done') {
    record(kind, { status: event.response?.status });
    return;
  }
  if (kind === 'input_audio_buffer.speech_started' || kind === 'input_audio_buffer.speech_stopped') {
    record(kind);
    return;
  }
  if (kind === 'error') {
    record('realtime.error', { code: event.error?.code || event.error?.type });
    log('realtime error', event.error?.code || event.error?.type);
    return;
  }
  if (kind === 'conversation.item.input_audio_transcription.completed') {
    transcript.push({ speaker: 'examiner', at_ms: nowMs(), text: event.transcript || '' });
  } else if (kind === 'response.output_audio_transcript.done' || kind === 'response.audio_transcript.done') {
    transcript.push({ speaker: 'nur', at_ms: nowMs(), text: event.transcript || '' });
  }
}

// ── Pacers ───────────────────────────────────────────────────────────────────

function startBridge() {
  started = Date.now();

  const uplinkBytes = Math.floor(ORCH_SR * (UPLINK_MS / 1000)) * 2;
  timers.push(setInterval(() => {
    if (nurWS.readyState !== WebSocket.OPEN) return;
    let chunk;
    if (uplink.length >= uplinkBytes) {
      chunk = uplink.subarray(0, uplinkBytes);
      uplink = uplink.subarray(uplinkBytes);
    } else {
      chunk = Buffer.alloc(uplinkBytes);
      uplink.copy(chunk, 0);
      uplink = Buffer.alloc(0);
    }
    const pcm16k = f32ToI16(decimator.process(i16ToF32(chunk)));
    if (!pcm16k.length) return;
    nurWS.send(JSON.stringify({
      type: 'input_audio_buffer.append',
      audio: pcm16k.toString('base64'),
    }));
  }, UPLINK_MS));

  // The examinee's side of the wire has to tick at 10 ms whether Nur is
  // speaking or not; silence is what the examiner hears while it thinks.
  const frame24 = Math.floor(NUR_OUT_SR * 0.01);
  timers.push(setInterval(() => {
    const wanted = frame24 * 2;
    let taken = Buffer.alloc(0);
    while (taken.length < wanted && playout.length) {
      const head = playout[0];
      const need = wanted - taken.length;
      if (head.length <= need) {
        taken = Buffer.concat([taken, head]);
        playout.shift();
        playoutBytes -= head.length;
      } else {
        taken = Buffer.concat([taken, head.subarray(0, need)]);
        playout[0] = head.subarray(need);
        playoutBytes -= need;
      }
    }
    rendered += taken.length / 2;
    const padded = Buffer.alloc(wanted);
    taken.copy(padded, 0);
    const up = upsample2(i16ToF32(padded), downCarry);
    downCarry = up.carry;
    const pcm = f32ToI16(up.out);
    for (let off = 0; off + ORCH_10MS * 2 <= pcm.length; off += ORCH_10MS * 2) {
      const ab = new ArrayBuffer(ORCH_10MS * 2);
      new Uint8Array(ab).set(pcm.subarray(off, off + ORCH_10MS * 2));
      orchSource.onData({ samples: new Int16Array(ab), sampleRate: ORCH_SR, bitsPerSample: 16, channelCount: 1 });
    }
  }, 10));

  timers.push(setInterval(() => {
    if (nurWS.readyState !== WebSocket.OPEN) return;
    nurWS.send(JSON.stringify({
      type: 'eesi.output.status',
      active: playoutBytes > 0,
      available: true,
      buffered_ms: Math.round((playoutBytes / 2 / NUR_OUT_SR) * 1000),
      playback: {
        response_id: currentResponse || 'unknown',
        sample_rate: NUR_OUT_SR,
        received_samples: received,
        rendered_samples: rendered,
        discarded_samples: discarded,
        clock_running: true,
        muted: false,
        flushed: discarded > 0,
      },
    }));
  }, STATUS_MS));
}

// ── Lifecycle ────────────────────────────────────────────────────────────────

function writeEvidence() {
  const dir = process.env.RECORD_DIR;
  if (!dir) return;
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'B.eesi.json'), JSON.stringify({
      base_url: argv.baseUrl,
      version_id: argv.versionId || 'serving',
      instructions_overridden: Boolean(argv.systemPrompt),
      received_samples: received,
      rendered_samples: rendered,
      discarded_samples: discarded,
      events,
      transcript,
    }, null, 2));
  } catch (err) {
    log('could not write evidence:', err.message);
  }
}

let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  log('shutting down');
  timers.forEach(clearInterval);
  writeEvidence();
  try { if (orchSink) orchSink.stop(); } catch { /* already stopped */ }
  try { if (orchPC) orchPC.close(); } catch { /* already closed */ }
  for (const socket of [nurWS, orchWS]) {
    try { if (socket && socket.readyState === WebSocket.OPEN) socket.close(); } catch { /* already closed */ }
  }
  setTimeout(() => process.exit(0), 200);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

(async function main() {
  log('starting; signal =', argv.signalUrl, 'nur =', argv.baseUrl);
  await connectOrchestrator();
  await connectNur();
  startBridge();
  log('bridging');
})().catch((err) => {
  console.error(`[EESI-${ROLE}] fatal:`, err.message);
  process.exit(1);
});
