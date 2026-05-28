import crypto from "node:crypto";
import tls from "node:tls";

export class SimpleWebSocket {
  static OPEN = 1;
  static CLOSED = 3;

  constructor(socket, { maskOutgoing = false } = {}) {
    this.socket = socket;
    this.maskOutgoing = maskOutgoing;
    this.readyState = SimpleWebSocket.OPEN;
    this.listeners = new Map();
    this.buffer = Buffer.alloc(0);

    socket.on("data", (chunk) => this.handleData(chunk));
    socket.setKeepAlive?.(true, 30000);
    socket.on("close", () => {
      this.readyState = SimpleWebSocket.CLOSED;
      this.emit("close");
    });
    socket.on("end", () => {
      this.readyState = SimpleWebSocket.CLOSED;
      this.emit("close");
    });
    socket.on("error", (error) => this.emit("error", error));
  }

  on(event, callback) {
    const callbacks = this.listeners.get(event) || [];
    callbacks.push(callback);
    this.listeners.set(event, callbacks);
  }

  emit(event, payload) {
    for (const callback of this.listeners.get(event) || []) {
      callback(payload);
    }
  }

  send(payload) {
    if (this.readyState !== SimpleWebSocket.OPEN) return;
    this.socket.write(encodeFrame(Buffer.from(String(payload)), {
      mask: this.maskOutgoing,
      opcode: 1
    }));
  }

  close() {
    if (this.readyState !== SimpleWebSocket.OPEN) return;
    this.readyState = SimpleWebSocket.CLOSED;
    this.socket.end(encodeFrame(Buffer.alloc(0), {
      mask: this.maskOutgoing,
      opcode: 8
    }));
  }

  handleData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);

    while (true) {
      const decoded = decodeFrame(this.buffer);
      if (!decoded) break;
      this.buffer = this.buffer.subarray(decoded.nextOffset);

      if (decoded.opcode === 8) {
        this.close();
        this.emit("close");
        break;
      }

      if (decoded.opcode === 9) {
        this.socket.write(encodeFrame(decoded.payload, {
          mask: this.maskOutgoing,
          opcode: 10
        }));
      }

      if (decoded.opcode === 10) {
        this.emit("pong");
      }

      if (decoded.opcode === 1) {
        this.emit("message", decoded.payload.toString("utf8"));
      }
    }
  }
}

export function acceptWebSocketUpgrade(req, socket, head, onConnection) {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return;
  }

  const accept = crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");

  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "\r\n"
  ].join("\r\n"));

  const peer = new SimpleWebSocket(socket);
  if (head?.length) peer.handleData(head);
  onConnection(peer);
}

export function connectRealtimeWebSocket({ apiKey, model }) {
  const key = crypto.randomBytes(16).toString("base64");
  const socket = tls.connect(443, "api.openai.com", { servername: "api.openai.com" });
  const peer = new SimpleWebSocket(socket, { maskOutgoing: true });
  let handshake = Buffer.alloc(0);
  let opened = false;

  socket.removeAllListeners("data");
  socket.on("data", (chunk) => {
    if (opened) {
      peer.handleData(chunk);
      return;
    }

    handshake = Buffer.concat([handshake, chunk]);
    const headerEnd = handshake.indexOf("\r\n\r\n");
    if (headerEnd === -1) return;

    const header = handshake.subarray(0, headerEnd).toString("utf8");
    const remaining = handshake.subarray(headerEnd + 4);

    if (!header.startsWith("HTTP/1.1 101")) {
      peer.emit("error", new Error(header.split("\r\n")[0] || "OpenAI WebSocket upgrade failed."));
      socket.end();
      return;
    }

    opened = true;
    peer.emit("open");
    if (remaining.length) peer.handleData(remaining);
  });

  socket.on("secureConnect", () => {
    socket.write([
      `GET /v1/realtime?model=${encodeURIComponent(model)} HTTP/1.1`,
      "Host: api.openai.com",
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Key: ${key}`,
      "Sec-WebSocket-Version: 13",
      `Authorization: Bearer ${apiKey}`,
      "\r\n"
    ].join("\r\n"));
  });

  socket.on("close", () => peer.emit("close"));
  socket.on("error", (error) => peer.emit("error", error));
  return peer;
}

function encodeFrame(payload, { mask, opcode }) {
  const length = payload.length;
  const lengthBytes = length < 126 ? 0 : length <= 65535 ? 2 : 8;
  const header = Buffer.alloc(2 + lengthBytes + (mask ? 4 : 0));
  header[0] = 0x80 | opcode;

  if (length < 126) {
    header[1] = (mask ? 0x80 : 0) | length;
  } else if (length <= 65535) {
    header[1] = (mask ? 0x80 : 0) | 126;
    header.writeUInt16BE(length, 2);
  } else {
    header[1] = (mask ? 0x80 : 0) | 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }

  if (!mask) return Buffer.concat([header, payload]);

  const maskKey = crypto.randomBytes(4);
  const maskOffset = 2 + lengthBytes;
  maskKey.copy(header, maskOffset);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i += 1) {
    masked[i] = payload[i] ^ maskKey[i % 4];
  }
  return Buffer.concat([header, masked]);
}

function decodeFrame(buffer) {
  if (buffer.length < 2) return null;

  const opcode = buffer[0] & 0x0f;
  const masked = Boolean(buffer[1] & 0x80);
  let length = buffer[1] & 0x7f;
  let offset = 2;

  if (length === 126) {
    if (buffer.length < offset + 2) return null;
    length = buffer.readUInt16BE(offset);
    offset += 2;
  } else if (length === 127) {
    if (buffer.length < offset + 8) return null;
    length = Number(buffer.readBigUInt64BE(offset));
    offset += 8;
  }

  const maskOffset = offset;
  if (masked) offset += 4;
  if (buffer.length < offset + length) return null;

  const payload = Buffer.from(buffer.subarray(offset, offset + length));
  if (masked) {
    const maskKey = buffer.subarray(maskOffset, maskOffset + 4);
    for (let i = 0; i < payload.length; i += 1) {
      payload[i] ^= maskKey[i % 4];
    }
  }

  return {
    nextOffset: offset + length,
    opcode,
    payload
  };
}
