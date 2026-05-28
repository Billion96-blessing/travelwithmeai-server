# TravelWithMe AI Server

Node.js backend for TravelBuddy AI / TravelWithMe AI. It protects the OpenAI API key, serves the web assistant prototype, supports streaming responses, and bridges realtime voice negotiation sessions.

## Local Run

```bash
cd travelwithmeai-server
npm install
export OPENAI_API_KEY="your_api_key_here"
npm start
```

Or create a local `.env` file:

```bash
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=gpt-5-mini
OPENAI_REALTIME_MODEL=gpt-realtime
PORT=3000
HOST=127.0.0.1
```

Then open:

```text
http://127.0.0.1:3000
```

## Render Deployment

Use these settings in Render:

- Service type: Web Service
- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Environment variable: `OPENAI_API_KEY`
- Environment variable: `NODE_ENV=production`

Optional environment variables:

```text
OPENAI_MODEL=gpt-5-mini
OPENAI_REALTIME_MODEL=gpt-realtime
REQUEST_TIMEOUT_MS=30000
ALLOWED_ORIGINS=https://travelwithmeai-server.onrender.com,https://api.travelwithmeai.com
```

Do not commit `.env`. Render should store secrets in its environment settings.

Production notes:

- The backend has request timeout handling, bounded JSON request bodies, retry logic for retryable OpenAI API failures, and security headers.
- Keep `OPENAI_API_KEY` only in Render environment variables.
- Set `ALLOWED_ORIGINS` to the deployed frontend/app origins before public launch.
- The app reads backend readiness from `/api/health`.

Production health check:

```text
GET /api/health
```

The Flutter app should use `https://travelwithmeai-server.onrender.com` by default. When the custom domain is ready, rebuild the app with:

```bash
flutter build apk --release --dart-define=TRAVELWITHMEAI_API_BASE_URL=https://api.travelwithmeai.com
```

## Features

- Chat with an AI assistant.
- Use the microphone button for voice input in browsers that support the Web Speech API.
- Turn voice replies on or off with the Voice replies checkbox.
- Start realtime voice chat for a low-latency spoken conversation.
- Translate text into common languages from the Translate panel.
- Realtime voice negotiation bridge for the Flutter app.
