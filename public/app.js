const form = document.querySelector("#chatForm");
const input = document.querySelector("#messageInput");
const messagesEl = document.querySelector("#messages");
const toneEl = document.querySelector("#tone");
const sendButton = document.querySelector("#sendButton");
const voiceButton = document.querySelector("#voiceButton");
const voiceStatus = document.querySelector("#voiceStatus");
const quickPromptButtons = document.querySelectorAll("[data-prompt]");
const newChatButton = document.querySelector("#newChatButton");
const chatHistoryEl = document.querySelector("#chatHistory");
const sidebarToggle = document.querySelector("#sidebarToggle");
const appShell = document.querySelector(".app-shell");
const negotiationGoal = document.querySelector("#negotiationGoal");
const startRealtimeButton = document.querySelector("#startRealtime");
const stopRealtimeButton = document.querySelector("#stopRealtime");
const approveDealButton = document.querySelector("#approveDeal");
const realtimeStatus = document.querySelector("#realtimeStatus");
const realtimeTranscript = document.querySelector("#realtimeTranscript");
const tripNotes = document.querySelector("#tripNotes");
const realtimeAudio = document.querySelector("#realtimeAudio");

const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
const recognition = SpeechRecognition ? new SpeechRecognition() : null;
const storageKey = "modern-ai-assistant-chats";

let chats = loadChats();
let activeChatId = chats[0]?.id || createChat().id;
let isStreaming = false;
let realtimeConnection = null;
let realtimeChannel = null;
let realtimeStream = null;
let transcriptBuffer = "";

if (recognition) {
  recognition.continuous = false;
  recognition.interimResults = true;
  recognition.lang = navigator.language || "en-US";
} else {
  voiceButton.disabled = true;
  voiceStatus.textContent = "Voice input is not supported in this browser.";
}

function createId() {
  return crypto.randomUUID ? crypto.randomUUID() : String(Date.now());
}

function starterMessage() {
  return {
    id: createId(),
    role: "assistant",
    content: "Welcome back. I can chat, draft, translate, explain code, and listen through voice input.",
    streaming: false
  };
}

function createChat() {
  const chat = {
    id: createId(),
    title: "New chat",
    createdAt: Date.now(),
    messages: [starterMessage()]
  };
  chats.unshift(chat);
  saveChats();
  return chat;
}

function loadChats() {
  try {
    const saved = JSON.parse(localStorage.getItem(storageKey) || "[]");
    return Array.isArray(saved) ? saved : [];
  } catch {
    return [];
  }
}

function saveChats() {
  localStorage.setItem(storageKey, JSON.stringify(chats.slice(0, 20)));
}

function activeChat() {
  return chats.find((chat) => chat.id === activeChatId);
}

function titleFromMessage(content) {
  return content.replace(/\s+/g, " ").trim().slice(0, 38) || "New chat";
}

function renderHistory() {
  chatHistoryEl.innerHTML = "";

  for (const chat of chats) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `history-item${chat.id === activeChatId ? " is-active" : ""}`;
    button.innerHTML = `
      <span>${chat.title}</span>
      <small>${new Date(chat.createdAt).toLocaleDateString()}</small>
    `;
    button.addEventListener("click", () => {
      activeChatId = chat.id;
      appShell.classList.remove("history-open");
      render();
    });
    chatHistoryEl.appendChild(button);
  }
}

function renderMessages() {
  const chat = activeChat();
  messagesEl.innerHTML = "";

  for (const message of chat.messages) {
    const bubble = document.createElement("article");
    bubble.className = `message ${message.role}${message.streaming ? " is-streaming" : ""}`;

    const avatar = document.createElement("span");
    avatar.className = "avatar";
    avatar.textContent = message.role === "user" ? "You" : "AI";

    const content = document.createElement("div");
    content.className = "message-content";
    content.textContent = message.content;

    if (message.streaming && !message.content) {
      content.innerHTML = '<span class="typing-dot"></span><span class="typing-dot"></span><span class="typing-dot"></span>';
    }

    bubble.append(avatar, content);
    messagesEl.appendChild(bubble);
  }

  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function render() {
  renderHistory();
  renderMessages();
}

function setLoading(loading) {
  isStreaming = loading;
  sendButton.disabled = loading;
  voiceButton.disabled = loading || !recognition;
  sendButton.querySelector("span").textContent = loading ? "Streaming" : "Send";
}

function autosizeInput() {
  input.style.height = "auto";
  input.style.height = `${Math.min(input.scrollHeight, 150)}px`;
}

function setRealtimeState(isLive, status) {
  startRealtimeButton.disabled = isLive;
  stopRealtimeButton.disabled = !isLive;
  approveDealButton.disabled = !isLive;
  startRealtimeButton.classList.toggle("is-live", isLive);
  realtimeStatus.textContent = status;
}

function addTripNote(note) {
  const time = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  tripNotes.textContent = `[${time}] ${note}`;
}

function stopRealtimeNegotiator() {
  if (realtimeChannel) {
    realtimeChannel.close();
    realtimeChannel = null;
  }

  if (realtimeConnection) {
    realtimeConnection.close();
    realtimeConnection = null;
  }

  if (realtimeStream) {
    realtimeStream.getTracks().forEach((track) => track.stop());
    realtimeStream = null;
  }

  realtimeAudio.srcObject = null;
  setRealtimeState(false, "Stopped. Press Start voice to test again.");
}

async function startRealtimeNegotiator() {
  if (!navigator.mediaDevices?.getUserMedia || !window.RTCPeerConnection) {
    realtimeStatus.textContent = "Realtime voice is not supported in this browser.";
    return;
  }

  const goal = negotiationGoal.value.trim();
  if (!goal) {
    realtimeStatus.textContent = "Add your private negotiation goal first.";
    negotiationGoal.focus();
    return;
  }

  transcriptBuffer = "";
  realtimeTranscript.textContent = "Connecting...";
  setRealtimeState(true, "Requesting secure realtime voice session...");

  try {
    const tokenResponse = await fetch("/api/realtime-token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode: "negotiator", goal })
    });
    const tokenData = await tokenResponse.json();
    if (!tokenResponse.ok) {
      throw new Error(tokenData.error || "Could not create realtime session.");
    }

    const ephemeralKey = tokenData.value || tokenData.client_secret?.value;
    if (!ephemeralKey) {
      throw new Error("Realtime token was missing from the server response.");
    }

    realtimeConnection = new RTCPeerConnection();
    realtimeConnection.addEventListener("connectionstatechange", () => {
      if (realtimeConnection?.connectionState === "connected") {
        setRealtimeState(true, "Live. Speak Thai near the microphone.");
      }

      if (["failed", "disconnected", "closed"].includes(realtimeConnection?.connectionState)) {
        setRealtimeState(false, "Realtime voice disconnected.");
      }
    });

    realtimeConnection.addEventListener("track", (event) => {
      realtimeAudio.srcObject = event.streams[0];
    });

    realtimeStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true
      }
    });
    realtimeStream.getTracks().forEach((track) => realtimeConnection.addTrack(track, realtimeStream));

    realtimeChannel = realtimeConnection.createDataChannel("oai-events");
    realtimeChannel.addEventListener("open", () => {
      realtimeChannel.send(JSON.stringify({
        type: "response.create",
        response: {
          modalities: ["audio", "text"],
          instructions: "Start in polite Thai. Greet the provider and ask the price, duration, safety, pickup/drop-off, inclusions, and discount. Do not finalize any deal yet."
        }
      }));
    });

    realtimeChannel.addEventListener("message", (event) => {
      const data = JSON.parse(event.data);
      const transcriptEvents = [
        "response.audio_transcript.delta",
        "response.output_audio_transcript.delta",
        "response.output_text.delta"
      ];

      if (transcriptEvents.includes(data.type) && data.delta) {
        transcriptBuffer += data.delta;
        realtimeTranscript.textContent = transcriptBuffer;
      }

      if (data.type === "response.done") {
        realtimeStatus.textContent = "Listening. Use Approve deal only when you accept the offer.";
      }

      if (data.type === "error") {
        realtimeStatus.textContent = data.error?.message || "Realtime voice error.";
      }
    });

    const offer = await realtimeConnection.createOffer();
    await realtimeConnection.setLocalDescription(offer);

    const sdpResponse = await fetch("https://api.openai.com/v1/realtime/calls", {
      method: "POST",
      body: offer.sdp,
      headers: {
        "Authorization": `Bearer ${ephemeralKey}`,
        "Content-Type": "application/sdp"
      }
    });

    if (!sdpResponse.ok) {
      throw new Error(await sdpResponse.text());
    }

    await realtimeConnection.setRemoteDescription({
      type: "answer",
      sdp: await sdpResponse.text()
    });
  } catch (error) {
    stopRealtimeNegotiator();
    realtimeStatus.textContent = error.message || "Could not start realtime voice.";
  }
}

function approveRealtimeDeal() {
  if (!realtimeChannel || realtimeChannel.readyState !== "open") {
    realtimeStatus.textContent = "Start voice before approving a deal.";
    return;
  }

  realtimeChannel.send(JSON.stringify({
    type: "response.create",
    response: {
      modalities: ["audio", "text"],
      instructions: "The user approved the deal. Confirm the final agreement politely in Thai. Then summarize the final agreement in English for Trip Notes."
    }
  }));

  addTripNote(`Approved goal: ${negotiationGoal.value.trim()}`);
  realtimeStatus.textContent = "Approved. AI is confirming in Thai.";
}

async function readStream(response, assistantMessage) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const events = buffer.split("\n\n");
    buffer = events.pop() || "";

    for (const eventText of events) {
      const eventName = eventText.match(/^event: (.+)$/m)?.[1];
      const dataText = eventText.match(/^data: (.+)$/m)?.[1];
      if (!dataText) continue;

      const payload = JSON.parse(dataText);
      if (eventName === "delta") {
        assistantMessage.content += payload.delta;
        renderMessages();
      }

      if (eventName === "error") {
        throw new Error(payload.error || "Streaming failed.");
      }
    }
  }
}

async function sendMessage(content) {
  if (isStreaming) return;

  const chat = activeChat();
  chat.messages.push({ id: createId(), role: "user", content });
  if (chat.title === "New chat") chat.title = titleFromMessage(content);

  const assistantMessage = {
    id: createId(),
    role: "assistant",
    content: "",
    streaming: true
  };
  chat.messages.push(assistantMessage);
  saveChats();
  render();
  setLoading(true);

  try {
    const response = await fetch("/api/assistant-stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages: chat.messages
          .filter((message) => !message.streaming)
          .map(({ role, content }) => ({ role, content })),
        tone: toneEl.value
      })
    });

    if (!response.ok || !response.body) {
      const data = await response.json().catch(() => ({}));
      throw new Error(data.error || "The assistant request failed.");
    }

    await readStream(response, assistantMessage);
  } catch (error) {
    assistantMessage.role = "system";
    assistantMessage.content = error.message;
  } finally {
    assistantMessage.streaming = false;
    setLoading(false);
    saveChats();
    render();
    input.focus();
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const content = input.value.trim();
  if (!content) return;
  input.value = "";
  autosizeInput();
  sendMessage(content);
});

input.addEventListener("input", autosizeInput);

input.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    form.requestSubmit();
  }
});

voiceButton.addEventListener("click", () => {
  if (!recognition) return;
  voiceStatus.textContent = "Listening...";
  voiceButton.classList.add("is-listening");
  recognition.start();
});

if (recognition) {
  recognition.addEventListener("result", (event) => {
    const transcript = Array.from(event.results)
      .map((result) => result[0].transcript)
      .join("");
    input.value = transcript;
    autosizeInput();
  });

  recognition.addEventListener("end", () => {
    voiceButton.classList.remove("is-listening");
    voiceStatus.textContent = input.value.trim() ? "Voice captured. Press Send." : "Voice input is ready.";
  });

  recognition.addEventListener("error", (event) => {
    voiceButton.classList.remove("is-listening");
    voiceStatus.textContent = `Voice input error: ${event.error}`;
  });
}

quickPromptButtons.forEach((button) => {
  button.addEventListener("click", () => {
    input.value = button.dataset.prompt;
    autosizeInput();
    input.focus();
  });
});

newChatButton.addEventListener("click", () => {
  activeChatId = createChat().id;
  appShell.classList.remove("history-open");
  render();
  input.focus();
});

sidebarToggle.addEventListener("click", () => {
  appShell.classList.toggle("history-open");
});

startRealtimeButton.addEventListener("click", startRealtimeNegotiator);
stopRealtimeButton.addEventListener("click", stopRealtimeNegotiator);
approveDealButton.addEventListener("click", approveRealtimeDeal);

render();
autosizeInput();
