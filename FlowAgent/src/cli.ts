import readline from 'node:readline/promises';
import type { ModelMessage } from 'ai';
import { loadConfig } from './config';
import { runFlowAgent } from './agent';

function preview(value: unknown, max = 600): string {
  const s = typeof value === 'string' ? value : JSON.stringify(value);
  return s.length > max ? s.slice(0, max) + ' …(截断)' : s;
}

async function main() {
  const cfg = loadConfig();
  if (!cfg.apiKey) {
    console.error('缺少 API Key:复制 .env.example 为 .env 并填入 FLOWAGENT_API_KEY(或设置环境变量 ARK_API_KEY)。');
    process.exit(1);
  }
  console.log(`FlowAgent CLI  model=${cfg.model}`);
  console.log(`workflow root  ${cfg.workflowRoot}`);
  console.log(`submit 开关    ${cfg.allowSubmit ? 'ON' : 'OFF(仅 dry-run 可达,submit 一律拒绝)'}\n`);

  const messages: ModelMessage[] = [];

  async function ask(input: string): Promise<void> {
    messages.push({ role: 'user', content: input });
    const result = runFlowAgent(cfg, messages);
    for await (const part of result.fullStream) {
      switch (part.type) {
        case 'text-delta':
          process.stdout.write(part.text);
          break;
        case 'tool-call':
          console.log(`\n\x1b[36m[调用工具]\x1b[0m ${part.toolName} ${preview(part.input, 200)}`);
          break;
        case 'tool-result':
          console.log(`\x1b[90m[工具返回]\x1b[0m ${part.toolName}: ${preview(part.output)}`);
          break;
        case 'error':
          console.error('\n[错误]', part.error);
          break;
      }
    }
    process.stdout.write('\n');
    messages.push(...(await result.response).messages);
  }

  const oneShot = process.argv.slice(2).join(' ').trim();
  if (oneShot) {
    await ask(oneShot);
    return;
  }

  console.log('进入交互模式,输入需求后回车;输入 exit 退出。\n');
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  for (;;) {
    const line = (await rl.question('\x1b[32m需求>\x1b[0m ')).trim();
    if (!line) continue;
    if (line === 'exit' || line === 'quit') break;
    try {
      await ask(line);
    } catch (e) {
      console.error('[会话错误]', e);
    }
    console.log();
  }
  rl.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
