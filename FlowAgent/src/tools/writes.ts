import fs from 'node:fs';
import path from 'node:path';
import { tool } from 'ai';
import { z } from 'zod';
import YAML from 'yaml';
import { safeJoin, type FlowAgentConfig } from '../config';
import { appendLedger } from '../ledger';
import { readYaml } from './registry';

export function createWriteTools(cfg: FlowAgentConfig) {
  const root = cfg.workflowRoot;

  const write_gap_log = tool({
    description:
      '记录缺口事件到 agent/gap_log.yaml(端口断链/缺 rule/缺 deliverable)。同一缺口重复出现自动累计次数。高频缺口是专家下一个该策展的 rule。',
    inputSchema: z.object({
      task_id: z.string(),
      need: z.string().describe('用户需求描述'),
      missing: z.string().describe('缺的端口、rule 或 deliverable,如 port:align.bam / deliverable:diff_expr'),
    }),
    execute: async ({ task_id, need, missing }) => {
      const file = path.join(root, 'agent', 'gap_log.yaml');
      fs.mkdirSync(path.dirname(file), { recursive: true });
      const doc = fs.existsSync(file)
        ? ((readYaml(file) as any) ?? {})
        : {};
      const entries: any[] = doc.entries ?? [];
      const hit = entries.find((e) => e.missing === missing);
      if (hit) {
        hit.count = (hit.count ?? 1) + 1;
        hit.last_seen = new Date().toISOString();
        hit.need = need;
      } else {
        entries.push({ time: new Date().toISOString(), last_seen: new Date().toISOString(), need, missing, count: 1 });
      }
      fs.writeFileSync(file, YAML.stringify({ entries }));
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'error',
        object: 'agent/gap_log.yaml',
        result: 'gap_recorded',
        detail: missing,
      });
      return { ok: true, missing, count: hit ? hit.count : 1, file: 'agent/gap_log.yaml' };
    },
  });

  const write_composed = tool({
    description:
      '在 composed/ 下新建组合 Snakefile 目录(每次组合一个新目录,已存在则拒绝,永不覆盖)。内容必须用 module + use rule 引用库中 rule,禁止复制粘贴 rule 本体;仓库存在 rules/common.smk 时 include 必须为第一行。',
    inputSchema: z.object({
      task_id: z.string(),
      name: z.string().describe('需求简述目录名,如 qc_only;工具会自动加日期前缀'),
      snakefile: z.string().describe('组合 Snakefile 完整内容'),
      config_yaml: z.string().optional().describe('可选:本组合专用 config 副本内容,写入同目录 config.yaml'),
    }),
    execute: async ({ task_id, name, snakefile, config_yaml }) => {
      if (!/^[A-Za-z0-9_\-一-鿿]+$/.test(name)) {
        return { ok: false, error: `目录名含非法字符: ${name}` };
      }
      const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      const dirName = name.startsWith(date) ? name : `${date}_${name}`;
      let dir: string;
      try {
        dir = safeJoin(root, path.join('composed', dirName));
      } catch (e) {
        return { ok: false, error: String(e) };
      }
      if (fs.existsSync(dir)) {
        return { ok: false, error: `composed/${dirName} 已存在。组合目录永不覆盖(可回溯),请换一个 name。` };
      }
      if (/^\s*rule\s+(?!all\b)/m.test(snakefile) && !snakefile.includes('use rule')) {
        return {
          ok: false,
          error: '检测到疑似复制粘贴的 rule 本体且未使用 use rule 引用,违反组合约定(规范 §6.2)。请改用 module + use rule。',
        };
      }
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, 'Snakefile'), snakefile);
      const files = ['Snakefile'];
      if (config_yaml) {
        fs.writeFileSync(path.join(dir, 'config.yaml'), config_yaml);
        files.push('config.yaml');
      }
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'fix',
        object: `composed/${dirName}`,
        result: 'composed_written',
        detail: files.join(', '),
      });
      return {
        ok: true,
        dir: `composed/${dirName}`,
        files,
        next: `先调 validate_manifests,再 dry_run(targets, workdir="composed/${dirName}")`,
      };
    },
  });

  return { write_gap_log, write_composed };
}
