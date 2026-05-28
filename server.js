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
const requestTimeoutMs = Number(process.env.REQUEST_TIMEOUT_MS || 30000);
const maxRequestBytes = Number(process.env.MAX_REQUEST_BYTES || 1024 * 1024);
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

function sendJson(res, status, payload) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    ...securityHeaders(),
    ...corsHeaders()
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
    const { goal, mode } = await readBody(req);
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

    sendJson(res, 200, data);
  } catch (error) {
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
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      ...securityHeaders(),
      ...corsHeaders(req.headers.origin)
    });
    res.end();
    return;
  }

  if (req.method === "GET" && req.url === "/api/health") {
    sendJson(res, 200, {
      ok: true,
      service: "travelwithmeai-server",
      model,
      realtimeModel,
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

  if (req.method === "POST" && req.url === "/api/realtime-token") {
    handleRealtimeToken(req, res);
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
      output_modalities: ["audio", "text"],
      audio: {
        input: {
          format: {
            type: "audio/pcm",
            rate: 24000
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
      modalities: ["audio", "text"],
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
      modalities: ["audio", "text"],
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
      openAiSocket.send(JSON.stringify(buildNegotiatorSession(privateGoal)));
      openAiSocket.send(JSON.stringify(buildStartResponse(privateGoal)));
      const providerLanguage = extractGoalField(privateGoal, "Provider language") || "Thai";
      sendClient(clientSocket, { type: "status", message: `Negotiator connected. Listening in ${providerLanguage}.` });
    });

    openAiSocket.on("message", (raw) => {
      let event;
      try {
        event = JSON.parse(raw.toString());
      } catch {
        sendClient(clientSocket, { type: "error", message: "Realtime event parse failed." });
        return;
      }

      if (event.type === "response.audio.delta" || event.type === "response.output_audio.delta") {
        sendClient(clientSocket, { type: "audio_delta", audio: event.delta });
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
        sendClient(clientSocket, {
          type: "summary",
          summary: userApproved
            ? "Final agreement confirmed. Save the Trip Notes below."
            : "Review the current offer. The AI will not confirm a final deal until you approve."
        });
      }

      if (event.type === "error") {
        sendClient(clientSocket, { type: "error", message: event.error?.message || "OpenAI realtime error." });
      }
    });

    openAiSocket.on("close", () => {
      sendClient(clientSocket, { type: "status", message: "Negotiator disconnected." });
    });

    openAiSocket.on("error", (error) => {
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
      connectOpenAI(message.goal || "");
      return;
    }

    if (message.type === "audio" && openAiSocket?.readyState === SimpleWebSocket.OPEN) {
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
