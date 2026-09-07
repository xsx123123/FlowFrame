import http from 'node:http';
import { Readable } from 'node:stream';
import { convertToModelMessages, type UIMessage } from 'ai';
import { loadConfig } from './config';
import { runFlowAgent } from './agent';

const cfg = loadConfig();

function setCors(res: http.ServerResponse): void {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

function json(res: http.ServerResponse, data: unknown, status = 200): void {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  setCors(res);
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  const url = new URL(req.url ?? '/', `http://localhost:${cfg.port}`);

  if (req.method === 'GET' && url.pathname === '/api/health') {
    json(res, {
      ok: true,
      model: cfg.model,
      workflow_root: cfg.workflowRoot,
      submit_enabled: cfg.allowSubmit,
      api_key_configured: Boolean(cfg.apiKey),
    });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/chat') {
    if (!cfg.apiKey) {
      json(res, { ok: false, error: 'FLOWAGENT_API_KEY 未配置' }, 500);
      return;
    }
    try {
      const body = JSON.parse(await readBody(req)) as { messages: UIMessage[] };
      const modelMessages = await convertToModelMessages(body.messages ?? []);
      const result = runFlowAgent(cfg, modelMessages);
      const response = result.toUIMessageStreamResponse({
        onError: (e) => (e instanceof Error ? e.message : String(e)),
      });
      res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
      if (response.body) {
        Readable.fromWeb(response.body as Parameters<typeof Readable.fromWeb>[0]).pipe(res);
      } else {
        res.end();
      }
    } catch (e) {
      json(res, { ok: false, error: String(e) }, 500);
    }
    return;
  }

  json(res, { ok: false, error: 'not found', routes: ['GET /api/health', 'POST /api/chat'] }, 404);
});

server.listen(cfg.port, () => {
  console.log(`FlowAgent server  http://localhost:${cfg.port}`);
  console.log(`  POST /api/chat   Vercel AI SDK useChat 直接对接(UI message stream)`);
  console.log(`  GET  /api/health 健康检查`);
  console.log(`  workflow root    ${cfg.workflowRoot}`);
  if (!cfg.apiKey) console.warn('  [警告] FLOWAGENT_API_KEY 未配置,/api/chat 将返回 500');
});
