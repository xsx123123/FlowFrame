import { streamText, stepCountIs, type ModelMessage } from 'ai';
import { createOpenAICompatible } from '@ai-sdk/openai-compatible';
import type { FlowAgentConfig } from './config';
import { buildSystemPrompt } from './system-prompt';
import { createTools } from './tools';

/**
 * 一次 agent 会话:CLI 与 HTTP server 共用。
 * 循环底座用 Vercel AI SDK streamText(不手写 tool-calling 循环),
 * SOP 在 system prompt,工具层在 createTools。
 */
export function runFlowAgent(cfg: FlowAgentConfig, messages: ModelMessage[]) {
  const provider = createOpenAICompatible({
    name: 'flowagent',
    baseURL: cfg.baseURL,
    apiKey: cfg.apiKey,
  });
  return streamText({
    model: provider.chatModel(cfg.model),
    system: buildSystemPrompt(cfg),
    messages,
    tools: createTools(cfg),
    stopWhen: stepCountIs(30),
  });
}
