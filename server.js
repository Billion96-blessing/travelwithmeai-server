import http from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { SimpleWebSocket, acceptWebSocketUpgrade, connectRealtimeWebSocket } from "./simple-ws.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const publicDir = join(__dirname, "public");

async function loadLocalEnv() {
  try {
    const envFile = await readFile(join(__dirname, ".env"), "utf8");
    for (const line of envFile.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;

      const separatorIndex = trimmed.indexOf("=");
      if (separatorIndex === -1) continue;

      const key = trimmed.slice(0, separatorIndex).trim();
      const value = trimmed
        .slice(separatorIndex + 1)
        .trim()
        .replace(/^["']|["']$/g, "");

      if (key && process.env[key] === undefined) {
        process.env[key] = value;
      }
    }
  } catch {
    // A .env file is optional. Shell environment variables still work.
  }
}

await loadLocalEnv();

const port = Number(process.env.PORT || 3000);
const host = process.env.HOST || "0.0.0.0";
const model = process.env.OPENAI_MODEL || "gpt-5-mini";
const realtimeModel = process.env.OPENAI_REALTIME_MODEL || "gpt-realtime";
const transcribeModel = process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";
const ttsModel = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const ttsVoice = process.env.OPENAI_TTS_VOICE || "marin";
const ttsFormat = "mp3";
const requestTimeoutMs = Number(process.env.REQUEST_TIMEOUT_MS || 30000);
const maxRequestBytes = Number(process.env.MAX_REQUEST_BYTES || 8 * 1024 * 1024);
const allowedOrigins = (process.env.ALLOWED_ORIGINS || "*")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml"
};

function sendJson(res, status, payload, extraHeaders = {}) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    ...securityHeaders(),
    ...corsHeaders(),
    ...extraHeaders
  });
  res.end(JSON.stringify(payload));
}

function corsHeaders(origin = "") {
  const allowOrigin =
    allowedOrigins.includes("*") || !origin || allowedOrigins.includes(origin)
      ? (allowedOrigins.includes("*") ? "*" : origin)
      : allowedOrigins[0] || "";

  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Vary": "Origin"
  };
}

function securityHeaders() {
  return {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Permissions-Policy": "microphone=(self)",
    "Cross-Origin-Opener-Policy": "same-origin"
  };
}

function publicError(error, fallback = "Service temporarily unavailable.") {
  if (error.name === "AbortError" || error.code === "TIMEOUT") {
    return "The request timed out. Please try again.";
  }
  return error.publicMessage || fallback;
}

function voiceLog(stage, details = {}) {
  console.log(JSON.stringify({
    service: "travelwithmeai-server",
    area: "voice",
    stage,
    at: new Date().toISOString(),
    ...details
  }));
}

function bufferHeaderHex(buffer, bytes = 16) {
  return Buffer.from(buffer)
    .subarray(0, bytes)
    .toString("hex")
    .match(/.{1,2}/g)
    ?.join(" ") || "";
}

function mimeTypeForAudioFormat(format) {
  switch (String(format || "").toLowerCase()) {
    case "wav":
      return "audio/wav";
    case "aac":
      return "audio/aac";
    case "flac":
      return "audio/flac";
    case "opus":
      return "audio/opus";
    case "pcm":
      return "audio/pcm";
    default:
      return "audio/mpeg";
  }
}

function shouldRetry(status) {
  return status === 408 || status === 409 || status === 425 || status === 429 || status >= 500;
}

async function wait(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, options, { retries = 2, timeoutMs = requestTimeoutMs } = {}) {
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal
      });
      const data = await response.json().catch(() => ({}));

      if (!response.ok && attempt < retries && shouldRetry(response.status)) {
        await wait(300 * (attempt + 1));
        continue;
      }

      return { response, data };
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await wait(300 * (attempt + 1));
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError || new Error("Request failed.");
}

async function fetchBinaryWithRetry(url, options, { retries = 2, timeoutMs = requestTimeoutMs } = {}) {
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal
      });

      if (!response.ok && attempt < retries && shouldRetry(response.status)) {
        await wait(300 * (attempt + 1));
        continue;
      }

      const contentType = response.headers.get("content-type") || "";
      const body = Buffer.from(await response.arrayBuffer());
      return { response, body, contentType };
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await wait(300 * (attempt + 1));
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError || new Error("Request failed.");
}

function writeSse(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function getResponseText(data) {
  if (data.output_text) return data.output_text;

  const chunks = [];
  for (const item of data.output || []) {
    for (const content of item.content || []) {
      if (content.text) chunks.push(content.text);
      if (content.output_text) chunks.push(content.output_text);
    }
  }

  return chunks.join("\n").trim();
}

function parseNegotiatorResponse(text) {
  const jsonMatch = text.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    return {
      replyThai: text,
      userSummary: "The AI negotiator replied, but could not create a structured summary.",
      proposedDeal: "",
      needsUserApproval: false,
      finalNote: "",
      status: "negotiating"
    };
  }

  return JSON.parse(jsonMatch[0]);
}

function parseJsonObject(text, fallback) {
  const jsonMatch = String(text || "").match(/\{[\s\S]*\}/);
  if (!jsonMatch) return fallback;

  try {
    return { ...fallback, ...JSON.parse(jsonMatch[0]) };
  } catch {
    return fallback;
  }
}

function audioExtensionFromMime(mimeType = "") {
  const normalized = mimeType.toLowerCase();
  if (normalized.includes("webm")) return "webm";
  if (normalized.includes("wav")) return "wav";
  if (normalized.includes("mpeg") || normalized.includes("mp3")) return "mp3";
  if (normalized.includes("aac")) return "aac";
  return "m4a";
}

function providerLanguageCode(language = "") {
  const normalized = language.toLowerCase();
  if (normalized.includes("thai")) return "th";
  if (normalized.includes("english")) return "en";
  if (normalized.includes("burmese") || normalized.includes("myanmar")) return "my";
  if (normalized.includes("chinese") || normalized.includes("mandarin")) return "zh";
  if (normalized.includes("japanese")) return "ja";
  if (normalized.includes("korean")) return "ko";
  return "";
}

async function readBody(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;
    if (size > maxRequestBytes) {
      const error = new Error("Request body is too large.");
      error.status = 413;
      error.publicMessage = "Request body is too large.";
      throw error;
    }
    chunks.push(chunk);
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    const error = new Error("Invalid JSON body.");
    error.status = 400;
    error.publicMessage = "Invalid JSON body.";
    throw error;
  }
}

async function serveStatic(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
  const safePath = normalize(decodeURIComponent(requestedPath)).replace(/^(\.\.[/\\])+/, "");
  const filePath = join(publicDir, safePath);

  if (!filePath.startsWith(publicDir)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  try {
    const file = await readFile(filePath);
    res.writeHead(200, {
      "Content-Type": mimeTypes[extname(filePath)] || "application/octet-stream",
      ...securityHeaders()
    });
    res.end(file);
  } catch {
    res.writeHead(404);
    res.end("Not found");
  }
}

async function handleAssistant(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to your shell environment before starting the app."
    });
    return;
  }

  try {
    const { messages, tone } = await readBody(req);
    if (!Array.isArray(messages) || messages.length === 0) {
      sendJson(res, 400, { error: "A non-empty messages array is required." });
      return;
    }

    const latestUserMessage = messages.at(-1)?.content?.trim();
    if (!latestUserMessage) {
      sendJson(res, 400, { error: "Message text is required." });
      return;
    }

    const recentConversation = messages.slice(-12).map((message) => ({
      role: message.role === "assistant" ? "assistant" : "user",
      content: message.content
    }));

    const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model,
        instructions: [
          "You are a helpful AI assistant inside a compact desktop web app.",
          "Be clear, practical, and warm. Ask one focused follow-up question only when needed.",
          `Preferred style: ${tone || "balanced"}.`
        ].join(" "),
        input: recentConversation,
        max_output_tokens: 900
      })
    });

    if (!response.ok) {
      sendJson(res, response.status, {
        error: data.error?.message || "OpenAI request failed."
      });
      return;
    }

    sendJson(res, 200, {
      reply: getResponseText(data) || "I did not receive text output from the model.",
      model
    });
  } catch (error) {
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

async function handleAssistantStream(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to your .env file before starting the app."
    });
    return;
  }

  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no"
  });

  try {
    const { messages, tone } = await readBody(req);
    if (!Array.isArray(messages) || messages.length === 0) {
      writeSse(res, "error", { error: "A non-empty messages array is required." });
      res.end();
      return;
    }

    const latestUserMessage = messages.at(-1)?.content?.trim();
    if (!latestUserMessage) {
      writeSse(res, "error", { error: "Message text is required." });
      res.end();
      return;
    }

    const recentConversation = messages.slice(-16).map((message) => ({
      role: message.role === "assistant" ? "assistant" : "user",
      content: message.content
    }));

    const streamController = new AbortController();
    const streamTimeout = setTimeout(() => streamController.abort(), requestTimeoutMs * 2);
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal: streamController.signal,
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model,
        stream: true,
        instructions: [
          "You are a modern AI assistant inside a futuristic chat app.",
          "Answer with useful structure, clear wording, and a calm confident tone.",
          "When the user asks for code, be practical and concise.",
          `Preferred style: ${tone || "balanced"}.`
        ].join(" "),
        input: recentConversation,
        max_output_tokens: 1200
      })
    });

    clearTimeout(streamTimeout);

    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      writeSse(res, "error", {
        error: data.error?.message || "OpenAI streaming request failed."
      });
      res.end();
      return;
    }

    const decoder = new TextDecoder();
    let buffer = "";
    let completed = false;

    for await (const chunk of response.body) {
      buffer += decoder.decode(chunk, { stream: true });
      const parts = buffer.split("\n\n");
      buffer = parts.pop() || "";

      for (const part of parts) {
        const dataLine = part
          .split("\n")
          .find((line) => line.startsWith("data: "));

        if (!dataLine) continue;
        const raw = dataLine.slice(6);
        if (raw === "[DONE]") continue;

        const event = JSON.parse(raw);
        if (event.type === "response.output_text.delta" && event.delta) {
          writeSse(res, "delta", { delta: event.delta });
        }

        if (event.type === "response.completed") {
          completed = true;
          writeSse(res, "done", { ok: true });
        }

        if (event.type === "error") {
          writeSse(res, "error", { error: event.error?.message || "Streaming error." });
        }
      }
    }

    if (!completed) {
      writeSse(res, "done", { ok: true });
    }
    res.end();
  } catch (error) {
    writeSse(res, "error", { error: publicError(error) });
    res.end();
  }
}

async function handleTranslate(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to your .env file before starting the app."
    });
    return;
  }

  try {
    const { text, targetLanguage } = await readBody(req);
    const cleanText = text?.trim();
    const language = targetLanguage?.trim() || "English";

    if (!cleanText) {
      sendJson(res, 400, { error: "Text is required for translation." });
      return;
    }

    const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model,
        instructions: [
          "You are a precise translation assistant.",
          "Translate the user's text into the requested language.",
          "Return only the translated text unless a phrase is impossible to translate naturally."
        ].join(" "),
        input: `Target language: ${language}\n\nText:\n${cleanText}`,
        max_output_tokens: 700
      })
    });

    if (!response.ok) {
      sendJson(res, response.status, {
        error: data.error?.message || "Translation request failed."
      });
      return;
    }

    sendJson(res, 200, {
      translatedText: getResponseText(data) || "No translation was returned.",
      model
    });
  } catch (error) {
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

async function handleNegotiator(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to your .env file before starting the app."
    });
    return;
  }

  try {
    const { goal, transcript, phase, userApproved } = await readBody(req);
    const cleanGoal = goal?.trim();

    if (!cleanGoal) {
      sendJson(res, 400, { error: "Negotiation goal is required." });
      return;
    }

    const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model,
        instructions: [
          "You are AI Negotiator Mode for a travel assistant app.",
          "The user privately gives you a negotiation goal. You speak to the Thai service provider in Thai.",
          "Be polite, natural, respectful, calm, and culturally appropriate in Thai.",
          "Ask smart questions about price, time, safety, pickup or location, inclusions, exclusions, cancellation, and discount.",
          "Negotiate for a fair price, not an exploitative price.",
          "Critical rule: never accept, confirm, or finalize a deal with the provider until userApproved is true.",
          "If the provider offers a deal that seems ready, pause and ask the user for approval in the app summary.",
          "If userApproved is true, confirm the agreed deal clearly in Thai and create a final trip note.",
          "Return only valid JSON with these exact keys: replyThai, userSummary, proposedDeal, needsUserApproval, finalNote, status.",
          "status must be one of: negotiating, needs_approval, approved, saved."
        ].join(" "),
        input: JSON.stringify({
          privateUserGoal: cleanGoal,
          phase: phase || "negotiate",
          userApproved: Boolean(userApproved),
          conversationTranscript: transcript || []
        }),
        max_output_tokens: 900
      })
    });

    if (!response.ok) {
      sendJson(res, response.status, {
        error: data.error?.message || "Negotiator request failed."
      });
      return;
    }

    sendJson(res, 200, parseNegotiatorResponse(getResponseText(data)));
  } catch (error) {
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

async function handleRealtimeToken(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to your .env file before starting the app."
    });
    return;
  }

  try {
    const tokenInput = req.method === "GET"
      ? readRealtimeTokenQuery(req)
      : await readBody(req);
    const { goal, mode } = tokenInput;
    const cleanGoal = goal?.trim() || "Help the user negotiate politely and fairly.";
    const providerLanguage = extractGoalField(cleanGoal, "Provider language") || "Thai";
    const userLanguage = extractGoalField(cleanGoal, "User translation language") || "English";
    const isNegotiator = mode === "negotiator";
    const instructions = isNegotiator
      ? [
          "You are AI Negotiator Mode for a travel assistant app.",
          "The user privately gives you a goal. The service provider must never hear the private goal directly.",
          `Speak only polite, natural, respectful ${providerLanguage} to the service provider.`,
          `Do not switch to Thai unless the selected provider language is Thai. The selected provider language is ${providerLanguage}.`,
          `Your first provider-facing message must be short: greet the provider, say you are helping your friend communicate, briefly say the customer wants ${buildUserRequestSummary(cleanGoal)}, then ask the price.`,
          "Example meaning: Hi, I am helping my friend communicate. How much is it?",
          "After the first message, negotiate on behalf of the user.",
          "Talk like a normal helpful person, not like a robot or formal assistant.",
          "Close the deal faster. Keep each turn under one short sentence when possible.",
          "Use natural pacing and adapt emotional tone to the provider: warm, calm, and confident.",
          "Handle interruptions naturally. If the provider interrupts, stop and listen.",
          "Use short questions: Can you reduce a little? Is that the final price? What is included? Pickup included? Any extra fee?",
          "Use direct counteroffers like: Can you do 1500?",
          "If the deal sounds ready, say: Okay, let me ask my friend first.",
          "Be context-aware. For taxi or rental car, ask about toll fee, waiting time, pickup/drop-off, luggage, route, and extra charge; do not ask about fuel unless relevant.",
          "For boat, ask about life jacket, round trip, island fee, pickup point, safety, and duration.",
          "For hotel, ask about tax, breakfast, deposit, and late checkout.",
          "For shopping, ask about discount, warranty, original/fake, and delivery.",
          "Negotiate for the best fair price, not an unfair price.",
          "Keep voice turns short and natural.",
          "Critical rule: do not accept, finalize, or confirm the final deal until the user approves.",
          `When a deal seems ready, explain the proposed deal in ${userLanguage} and ask the user: Do you approve this deal?`,
          `Only after user approval may you confirm the agreement in ${providerLanguage}.`,
          `Private user goal: ${cleanGoal}`
        ].join(" ")
      : [
          "You are a refined real-time voice assistant inside a futuristic desktop app.",
          "Speak naturally, keep answers concise, and help with chat, translation, planning, and everyday tasks."
        ].join(" ");

    const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/realtime/client_secrets", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        expires_after: {
          anchor: "created_at",
          seconds: 600
        },
        session: {
          type: "realtime",
          model: realtimeModel,
          instructions,
          audio: {
            input: {
              transcription: {
                model: "gpt-4o-mini-transcribe"
              }
            },
            output: {
              voice: "marin"
            }
          }
        }
      })
    });

    if (!response.ok) {
      sendJson(res, response.status, {
        error: data.error?.message || "Realtime token request failed."
      });
      return;
    }

    sendJson(res, 200, data, { "Cache-Control": "no-store" });
  } catch (error) {
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

function readRealtimeTokenQuery(req) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  return {
    goal: url.searchParams.get("goal") || "",
    mode: url.searchParams.get("mode") || "negotiator"
  };
}

async function transcribeVoiceTurn({ audioBase64, audioMimeType, providerLanguage }) {
  const buffer = Buffer.from(String(audioBase64 || ""), "base64");
  if (buffer.length < 1200) {
    const error = new Error("Audio recording is too short.");
    error.status = 400;
    error.publicMessage = "I could not hear enough audio. Please try again.";
    throw error;
  }

  const mimeType = audioMimeType || "audio/mp4";
  const fileName = `voice-turn.${audioExtensionFromMime(mimeType)}`;
  const form = new FormData();
  form.append("model", transcribeModel);
  form.append("response_format", "json");

  const languageCode = providerLanguageCode(providerLanguage);
  if (languageCode) {
    form.append("language", languageCode);
  }

  form.append("file", new Blob([buffer], { type: mimeType }), fileName);

  voiceLog("audio_sent_to_openai_stt", {
    bytes: buffer.length,
    mimeType,
    audioHeaderHex: bufferHeaderHex(buffer),
    providerLanguage,
    model: transcribeModel
  });

  const { response, data } = await fetchJsonWithRetry(
    "https://api.openai.com/v1/audio/transcriptions",
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: form
    },
    { retries: 1, timeoutMs: requestTimeoutMs }
  );

  if (!response.ok) {
    const error = new Error(data.error?.message || "Transcription failed.");
    error.status = response.status;
    error.publicMessage = "Speech transcription failed. Please try again.";
    throw error;
  }

  const transcript = (data.text || data.transcript || "").trim();
  voiceLog("stt_transcript_generated", {
    transcriptLength: transcript.length,
    openAIContentType: response.headers.get("content-type") || "",
    model: transcribeModel
  });

  if (!transcript) {
    const error = new Error("No speech detected.");
    error.status = 422;
    error.publicMessage = "I could not hear clear speech. Please try again.";
    throw error;
  }

  return transcript;
}

async function generateVoiceStartReply(goal) {
  const providerLanguage = extractGoalField(goal, "Provider language") || "Thai";
  const userLanguage = extractGoalField(goal, "User translation language") || "English";

  const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model,
      instructions: [
        "You write the first spoken line for TravelBuddy AI negotiation mode.",
        `The provider-facing line must be in ${providerLanguage}.`,
        `Also provide a ${userLanguage} translation for the user.`,
        "The provider-facing line must be short, polite, direct, and human-like.",
        "Say you are helping your friend/customer communicate, briefly state the service wanted, then ask the price.",
        "Do not mention the private target price or maximum budget.",
        "Return only valid JSON with keys: aiReply, aiTranslation, needsUserApproval, status."
      ].join(" "),
      input: JSON.stringify({
        privateUserGoal: goal,
        firstTurn: true,
        requestSummary: buildUserRequestSummary(goal)
      }),
      max_output_tokens: 450
    })
  });

  if (!response.ok) {
    const error = new Error(data.error?.message || "AI reply failed.");
    error.status = response.status;
    error.publicMessage = "AI could not prepare the first voice reply.";
    throw error;
  }

  const fallback = {
    aiReply: providerLanguage.toLowerCase().includes("thai")
      ? `สวัสดีครับ ผมช่วยเพื่อนสื่อสารนะครับ ขอสอบถามราคาสำหรับ ${buildUserRequestSummary(goal)} หน่อยครับ`
      : `Hi, I am helping my friend communicate. How much is ${buildUserRequestSummary(goal)}?`,
    aiTranslation: `Hi, I am helping my friend communicate. May I ask the price for ${buildUserRequestSummary(goal)}?`,
    needsUserApproval: false,
    status: "negotiating"
  };
  const parsed = parseJsonObject(getResponseText(data), fallback);
  voiceLog("ai_start_text_generated", {
    replyLength: String(parsed.aiReply || "").length,
    model
  });
  return parsed;
}

async function generateVoiceNegotiatorReply({ goal, providerTranscript, conversation }) {
  const providerLanguage = extractGoalField(goal, "Provider language") || "Thai";
  const userLanguage = extractGoalField(goal, "User translation language") || "English";

  const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model,
      instructions: [
        "You are TravelBuddy AI voice negotiator for a traveler.",
        `Reply to the service provider only in ${providerLanguage}.`,
        `Translate the provider message and your reply for the user in ${userLanguage}.`,
        "Speak like a helpful human assistant, not a robot.",
        "Keep the provider-facing reply short, polite, direct, and natural.",
        "Ask price first, then discount/final price, then only details relevant to the service type.",
        "Taxi: ask toll fee, waiting fee, pickup/drop-off, luggage, route, extra charge.",
        "Boat: ask life jacket, island fee, round trip, duration, pickup point, safety.",
        "Hotel: ask breakfast, tax, deposit, late checkout.",
        "Shopping: ask discount, warranty, original/fake, delivery.",
        "Do not confirm or finalize the deal until the user approves in the app.",
        "If the provider offer seems ready, say a short version of: okay, let me ask my friend first.",
        "Return only valid JSON with keys: providerTranslation, aiReply, aiTranslation, needsUserApproval, finalNote, status."
      ].join(" "),
      input: JSON.stringify({
        privateUserGoal: goal,
        providerTranscript,
        recentConversation: Array.isArray(conversation) ? conversation.slice(-12) : []
      }),
      max_output_tokens: 650
    })
  });

  if (!response.ok) {
    const error = new Error(data.error?.message || "AI reply failed.");
    error.status = response.status;
    error.publicMessage = "AI could not generate a voice reply.";
    throw error;
  }

  const fallback = {
    providerTranslation: providerTranscript,
    aiReply: providerLanguage.toLowerCase().includes("thai")
      ? "ลดได้อีกหน่อยไหมครับ ราคาสุดท้ายเท่าไหร่ครับ"
      : "Can you reduce a little? What is the final price?",
    aiTranslation: "Can you reduce a little? What is the final price?",
    needsUserApproval: false,
    finalNote: "",
    status: "negotiating"
  };
  const parsed = parseJsonObject(getResponseText(data), fallback);
  voiceLog("ai_text_generated", {
    providerTranscriptLength: providerTranscript.length,
    replyLength: String(parsed.aiReply || "").length,
    model
  });
  return parsed;
}

async function synthesizeVoiceAudio(text) {
  const cleanText = String(text || "").trim();
  if (!cleanText) {
    const error = new Error("No text for speech.");
    error.status = 400;
    error.publicMessage = "AI voice had no text to speak.";
    throw error;
  }

  voiceLog("tts_audio_requested", {
    textLength: cleanText.length,
    model: ttsModel,
    voice: ttsVoice,
    format: ttsFormat
  });

  const { response, body, contentType } = await fetchBinaryWithRetry(
    "https://api.openai.com/v1/audio/speech",
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: ttsModel,
        voice: ttsVoice,
        input: cleanText,
        response_format: ttsFormat
      })
    },
    { retries: 1, timeoutMs: requestTimeoutMs }
  );

  if (!response.ok) {
    let message = "Text-to-speech failed.";
    try {
      const data = JSON.parse(body.toString("utf8"));
      message = data.error?.message || message;
    } catch {
      message = body.toString("utf8").slice(0, 180) || message;
    }
    const error = new Error(message);
    error.status = response.status;
    error.publicMessage = "AI voice generation failed. Please try again.";
    throw error;
  }

  voiceLog("tts_audio_generated", {
    bytes: body.length,
    contentType,
    audioHeaderHex: bufferHeaderHex(body),
    model: ttsModel
  });

  return {
    audioBase64: body.toString("base64"),
    audioMimeType: contentType.includes("audio/")
      ? contentType
      : mimeTypeForAudioFormat(ttsFormat),
    audioByteLength: body.length,
    audioHeaderHex: bufferHeaderHex(body),
    audioContentType: contentType || mimeTypeForAudioFormat(ttsFormat)
  };
}

async function handleVoiceStart(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to the backend environment variables."
    });
    return;
  }

  try {
    const { goal } = await readBody(req);
    const cleanGoal = String(goal || "").trim();
    if (!cleanGoal) {
      sendJson(res, 400, { error: "Negotiation goal is required." });
      return;
    }

    voiceLog("voice_session_start", {
      providerLanguage: extractGoalField(cleanGoal, "Provider language") || "Thai"
    });
    const reply = await generateVoiceStartReply(cleanGoal);
    const audio = await synthesizeVoiceAudio(reply.aiReply);

    sendJson(res, 200, {
      mode: "voice",
      providerTranscript: "",
      providerTranslation: "",
      aiReply: reply.aiReply,
      aiTranslation: reply.aiTranslation,
      needsUserApproval: Boolean(reply.needsUserApproval),
      status: reply.status || "negotiating",
      ...audio
    });
  } catch (error) {
    voiceLog("voice_start_error", {
      status: error.status || 500,
      message: error.message
    });
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

async function handleVoiceTurn(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to the backend environment variables."
    });
    return;
  }

  try {
    const { goal, audioBase64, audioMimeType, conversation } = await readBody(req);
    const cleanGoal = String(goal || "").trim();
    if (!cleanGoal) {
      sendJson(res, 400, { error: "Negotiation goal is required." });
      return;
    }
    if (!audioBase64) {
      sendJson(res, 400, { error: "Audio is required." });
      return;
    }

    const providerLanguage = extractGoalField(cleanGoal, "Provider language") || "Thai";
    const decodedAudio = Buffer.from(String(audioBase64), "base64");
    voiceLog("backend_received_audio", {
      base64Length: String(audioBase64).length,
      decodedBytes: decodedAudio.length,
      audioMimeType: audioMimeType || "audio/mp4",
      audioHeaderHex: bufferHeaderHex(decodedAudio),
      providerLanguage
    });

    const providerTranscript = await transcribeVoiceTurn({
      audioBase64,
      audioMimeType,
      providerLanguage
    });
    const reply = await generateVoiceNegotiatorReply({
      goal: cleanGoal,
      providerTranscript,
      conversation
    });
    const audio = await synthesizeVoiceAudio(reply.aiReply);

    sendJson(res, 200, {
      mode: "voice",
      providerTranscript,
      providerTranslation: reply.providerTranslation || providerTranscript,
      aiReply: reply.aiReply,
      aiTranslation: reply.aiTranslation || reply.aiReply,
      needsUserApproval: Boolean(reply.needsUserApproval),
      finalNote: reply.finalNote || "",
      status: reply.status || "negotiating",
      ...audio
    });
  } catch (error) {
    voiceLog("voice_turn_error", {
      status: error.status || 500,
      message: error.message
    });
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

async function handleVoiceGoal(req, res) {
  if (!process.env.OPENAI_API_KEY) {
    sendJson(res, 500, {
      error: "Missing OPENAI_API_KEY. Add it to the backend environment variables."
    });
    return;
  }

  try {
    const { audioBase64, audioMimeType, userLanguage } = await readBody(req);
    if (!audioBase64) {
      sendJson(res, 400, { error: "Audio is required." });
      return;
    }

    const transcript = await transcribeVoiceTurn({
      audioBase64,
      audioMimeType,
      providerLanguage: userLanguage || "English"
    });

    const { response, data } = await fetchJsonWithRetry("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model,
        instructions: [
          "Extract a travel negotiation goal from a user's spoken request.",
          "Return only valid JSON with keys: destination, activity, people, budget, notes.",
          "Leave unknown fields as empty strings."
        ].join(" "),
        input: transcript,
        max_output_tokens: 300
      })
    });

    if (!response.ok) {
      sendJson(res, response.status, {
        error: data.error?.message || "Goal extraction failed."
      });
      return;
    }

    const parsed = parseJsonObject(getResponseText(data), {
      destination: "",
      activity: transcript,
      people: "",
      budget: "",
      notes: ""
    });

    sendJson(res, 200, {
      transcript,
      destination: parsed.destination || "",
      activity: parsed.activity || transcript,
      people: parsed.people || "",
      budget: parsed.budget || "",
      notes: parsed.notes || ""
    });
  } catch (error) {
    voiceLog("voice_goal_error", {
      status: error.status || 500,
      message: error.message
    });
    sendJson(res, error.status || 500, { error: publicError(error) });
  }
}

function extractGoalField(goal, fieldName) {
  const escaped = fieldName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = String(goal || "").match(new RegExp(`${escaped}:\\s*([^\\n]+)`, "i"));
  return match?.[1]?.trim() || "";
}

function buildUserRequestSummary(goal) {
  const activity = extractGoalField(goal, "Activity") || "this service";
  const people = extractGoalField(goal, "People");
  return people ? `${activity} for ${people} people` : activity;
}

const server = http.createServer((req, res) => {
  const requestUrl = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      ...securityHeaders(),
      ...corsHeaders(req.headers.origin)
    });
    res.end();
    return;
  }

  if (req.method === "GET" && requestUrl.pathname === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      service: "travelwithmeai-server",
      model,
      realtimeModel,
      transcribeModel,
      ttsModel,
      ttsVoice,
      ttsFormat,
      voiceReady: Boolean(process.env.OPENAI_API_KEY),
      hasOpenAIKey: Boolean(process.env.OPENAI_API_KEY)
    });
    return;
  }

  if (req.method === "POST" && req.url === "/api/assistant") {
    handleAssistant(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/assistant-stream") {
    handleAssistantStream(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/translate") {
    handleTranslate(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/negotiator") {
    handleNegotiator(req, res);
    return;
  }

  if (
    (req.method === "GET" || req.method === "POST") &&
    requestUrl.pathname === "/api/realtime-token"
  ) {
    handleRealtimeToken(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/voice-start") {
    handleVoiceStart(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/voice-turn") {
    handleVoiceTurn(req, res);
    return;
  }

  if (req.method === "POST" && req.url === "/api/voice-goal") {
    handleVoiceGoal(req, res);
    return;
  }

  if (req.method === "GET") {
    serveStatic(req, res);
    return;
  }

  res.writeHead(405);
  res.end("Method not allowed");
});

server.requestTimeout = requestTimeoutMs + 5000;
server.headersTimeout = requestTimeoutMs + 10000;
server.keepAliveTimeout = 65000;

function buildNegotiatorInstructions(providerLanguage, userLanguage, goal = "") {
  return [
    "You are AI Negotiator Mode for a travel assistant app.",
    "The user privately tells you their goal. The service provider never sees that private goal.",
    `Speak only polite, natural, respectful ${providerLanguage} to the service provider.`,
    `Do not switch to Thai unless the selected provider language is Thai. The selected provider language is ${providerLanguage}.`,
    `Your first provider-facing message must be short: greet the provider, say you are helping your friend communicate, briefly say the customer wants ${buildUserRequestSummary(goal)}, then ask the price.`,
    "After the first message, negotiate on behalf of the user.",
    "Talk like a normal helpful person, not like a robot or formal assistant.",
    "Close the deal faster. Keep each turn under one short sentence when possible.",
    "Use natural pacing and adapt emotional tone to the provider: warm, calm, and confident.",
    "Handle interruptions naturally. If the provider interrupts, stop and listen.",
    "Use short questions: Can you reduce a little? Is that the final price? What is included? Pickup included? Any extra fee?",
    "Use direct counteroffers like: Can you do 1500?",
    "If the deal sounds ready, say: Okay, let me ask my friend first.",
    "Be context-aware. For taxi or rental car, ask about toll fee, waiting time, pickup/drop-off, luggage, route, and extra charge; do not ask about fuel unless relevant.",
    "For boat, ask about life jacket, round trip, island fee, pickup point, safety, and duration.",
    "For hotel, ask about tax, breakfast, deposit, and late checkout.",
    "For shopping, ask about discount, warranty, original/fake, and delivery.",
    "Negotiate for a fair price, not an unfair or exploitative price.",
    "Keep turns short and conversational because this is real-time voice.",
    "Critical safety rule: do not accept, finalize, or confirm a final deal until the user explicitly approves.",
    `When a deal looks ready, stop negotiating and tell the user in ${userLanguage} summary that approval is needed.`,
    `If user approval is received, confirm the final agreement politely in ${providerLanguage}.`,
    `After final confirmation, produce a concise ${userLanguage} Trip Notes summary.`
  ].join(" ");
}

function sendClient(socket, payload) {
  if (socket.readyState === SimpleWebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  }
}

function buildNegotiatorSession(goal) {
  const providerLanguage = extractGoalField(goal, "Provider language") || "Thai";
  const userLanguage = extractGoalField(goal, "User translation language") || "English";
  return {
    type: "session.update",
    session: {
      type: "realtime",
      model: realtimeModel,
      instructions: `${buildNegotiatorInstructions(providerLanguage, userLanguage, goal)}\n\nPrivate user goal: ${goal}`,
      output_modalities: ["audio"],
      audio: {
        input: {
          format: {
            type: "audio/pcm",
            rate: 24000
          },
          transcription: {
            model: "gpt-4o-mini-transcribe"
          },
          turn_detection: {
            type: "server_vad",
            threshold: 0.55,
            prefix_padding_ms: 300,
            silence_duration_ms: 420,
            create_response: true,
            interrupt_response: true
          }
        },
        output: {
          format: {
            type: "audio/pcm",
            rate: 24000
          },
          voice: "marin"
        }
      }
    }
  };
}

function buildApprovalResponse(goal) {
  const providerLanguage = extractGoalField(goal, "Provider language") || "Thai";
  const userLanguage = extractGoalField(goal, "User translation language") || "English";
  return {
    type: "response.create",
    response: {
      output_modalities: ["audio"],
      instructions: [
        "The user approved the proposed deal.",
        `Now confirm the final agreement politely in ${providerLanguage} with the provider.`,
        `Then create a short ${userLanguage} Trip Notes summary containing price, duration, people, pickup/drop-off, safety, inclusions, and any conditions.`
      ].join(" ")
    }
  };
}

function buildStartResponse(goal) {
  const providerLanguage = extractGoalField(goal, "Provider language") || "Thai";
  const request = buildUserRequestSummary(goal);
  return {
    type: "response.create",
    response: {
      output_modalities: ["audio"],
      instructions: [
        `Start the negotiation in ${providerLanguage}.`,
        `Speak only ${providerLanguage} to the provider.`,
        "Your first message must be short and natural.",
        `First greet the provider politely, say you are helping your friend communicate, briefly say the customer wants ${request}, then ask the price.`,
        "Ask price first. Do not ask duration, pickup, safety, or inclusions in the first message unless the provider asks.",
        "Do not mention the user's private target price or budget."
      ].join(" ")
    }
  };
}

function handleNegotiatorSocket(clientSocket) {
  let openAiSocket = null;
  let privateGoal = "";
  let userApproved = false;
  let lastClientMessageAt = Date.now();
  let clientAudioChunks = 0;
  let aiAudioChunks = 0;
  let currentResponseHadAudio = false;
  let sessionReady = false;
  const transcript = [];
  const heartbeat = setInterval(() => {
    if (Date.now() - lastClientMessageAt > 45000) {
      sendClient(clientSocket, {
        type: "status",
        message: "Connection alive. Waiting for audio..."
      });
    }
  }, 30000);

  function connectOpenAI(goal) {
    if (!process.env.OPENAI_API_KEY) {
      sendClient(clientSocket, {
        type: "error",
        message: "Missing OPENAI_API_KEY. Add it to your backend .env file."
      });
      clientSocket.close();
      return;
    }

    privateGoal = goal;
    openAiSocket = connectRealtimeWebSocket({
      apiKey: process.env.OPENAI_API_KEY,
      model: realtimeModel
    });

    openAiSocket.on("open", () => {
      voiceLog("realtime_session_connected", {
        model: realtimeModel
      });
      openAiSocket.send(JSON.stringify(buildNegotiatorSession(privateGoal)));
      sendClient(clientSocket, { type: "status", message: "Preparing live voice session..." });
    });

    openAiSocket.on("message", (raw) => {
      let event;
      try {
        event = JSON.parse(raw.toString());
      } catch {
        sendClient(clientSocket, { type: "error", message: "Realtime event parse failed." });
        return;
      }

      if (
        event.type === "input_audio_buffer.speech_started"
        || event.type === "input_audio_buffer.speech_stopped"
      ) {
        const speaking = event.type.endsWith("speech_started");
        voiceLog(speaking ? "provider_speech_started" : "provider_speech_stopped");
        sendClient(clientSocket, {
          type: speaking ? "provider_speaking" : "status",
          message: speaking ? "Provider speaking..." : "AI Thinking"
        });
      }

      if (event.type === "session.updated" && !sessionReady) {
        sessionReady = true;
        voiceLog("realtime_session_ready");
        sendClient(clientSocket, {
          type: "realtime_ready",
          message: "Connected",
          listen: false
        });
        openAiSocket.send(JSON.stringify(buildStartResponse(privateGoal)));
      }

      if (event.type === "response.created") {
        currentResponseHadAudio = false;
        aiAudioChunks = 0;
      }

      if (event.type === "response.audio.delta" || event.type === "response.output_audio.delta") {
        currentResponseHadAudio = true;
        aiAudioChunks += 1;
        if (aiAudioChunks === 1 || aiAudioChunks % 50 === 0) {
          voiceLog("ai_audio_chunk_received", {
            chunks: aiAudioChunks
          });
        }
        sendClient(clientSocket, { type: "audio_delta", audio: event.delta });
      }

      if (
        event.type === "response.audio.done"
        || event.type === "response.output_audio.done"
      ) {
        voiceLog("ai_audio_done", {
          chunks: aiAudioChunks
        });
        sendClient(clientSocket, { type: "ai_audio_done" });
        aiAudioChunks = 0;
      }

      if (
        event.type === "conversation.item.input_audio_transcription.completed"
        || event.type === "input_audio_transcription.completed"
      ) {
        transcript.push({ speaker: "provider", text: event.transcript });
        sendClient(clientSocket, { type: "provider_transcript", text: event.transcript });
      }

      if (
        event.type === "response.audio_transcript.delta"
        || event.type === "response.output_audio_transcript.delta"
        || event.type === "response.output_text.delta"
      ) {
        sendClient(clientSocket, { type: "ai_transcript_delta", text: event.delta || "" });
      }

      if (event.type === "response.done") {
        sendClient(clientSocket, { type: "response_done", hadAudio: currentResponseHadAudio });
        sendClient(clientSocket, {
          type: "summary",
          summary: userApproved
            ? "Final agreement confirmed. Save the Trip Notes below."
            : "Review the current offer. The AI will not confirm a final deal until you approve."
        });
        currentResponseHadAudio = false;
      }

      if (event.type === "error") {
        sendClient(clientSocket, { type: "error", message: event.error?.message || "OpenAI realtime error." });
      }
    });

    openAiSocket.on("close", (closeInfo = {}) => {
      voiceLog("realtime_websocket_closed", {
        code: closeInfo.code || "",
        reason: closeInfo.reason || ""
      });
      sendClient(clientSocket, {
        type: "status",
        message: "Negotiator disconnected.",
        code: closeInfo.code || "",
        reason: closeInfo.reason || ""
      });
    });

    openAiSocket.on("error", (error) => {
      voiceLog("realtime_websocket_error", {
        message: error.message
      });
      sendClient(clientSocket, { type: "error", message: error.message });
    });
  }

  clientSocket.on("message", (raw) => {
    lastClientMessageAt = Date.now();
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      sendClient(clientSocket, { type: "error", message: "Invalid realtime message." });
      return;
    }

    if (message.type === "start") {
      clientAudioChunks = 0;
      aiAudioChunks = 0;
      currentResponseHadAudio = false;
      sessionReady = false;
      connectOpenAI(message.goal || "");
      return;
    }

    if (message.type === "audio" && openAiSocket?.readyState === SimpleWebSocket.OPEN) {
      clientAudioChunks += 1;
      if (clientAudioChunks === 1 || clientAudioChunks % 50 === 0) {
        voiceLog("backend_received_realtime_audio", {
          chunks: clientAudioChunks,
          base64Length: String(message.audio || "").length
        });
      }
      openAiSocket.send(JSON.stringify({
        type: "input_audio_buffer.append",
        audio: message.audio
      }));
      return;
    }

    if (message.type === "approve" && openAiSocket?.readyState === SimpleWebSocket.OPEN) {
      userApproved = true;
      openAiSocket.send(JSON.stringify(buildApprovalResponse(privateGoal)));
      sendClient(clientSocket, {
        type: "trip_note",
        note: `Goal: ${privateGoal}\nStatus: Approved by user\nTranscript turns: ${transcript.length}`
      });
      return;
    }

    if (message.type === "stop") {
      openAiSocket?.close();
      clientSocket.close();
    }
  });

  clientSocket.on("close", () => {
    clearInterval(heartbeat);
    openAiSocket?.close();
  });

  clientSocket.on("error", () => {
    clearInterval(heartbeat);
    openAiSocket?.close();
  });
}

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/ws/negotiator") {
    acceptWebSocketUpgrade(req, socket, head, handleNegotiatorSocket);
    return;
  }

  socket.destroy();
});

server.listen(port, host);
