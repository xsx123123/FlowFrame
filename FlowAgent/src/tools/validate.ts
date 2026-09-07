import fs from 'node:fs';
import path from 'node:path';
import { tool } from 'ai';
import { z } from 'zod';
import { safeJoin, type FlowAgentConfig } from '../config';
import { appendLedger } from '../ledger';
import { runShell, tail } from '../shell';
import { loadManifests, readYaml } from './registry';

/** manifest 中的路径 pattern → 正则:{sample} / {md5dir} 等占位符一律按非贪婪段匹配 */
function patternToRegex(pattern: string): RegExp {
  const escaped = pattern.replace(/[.+^$()|[\]\\]/g, '\\$&').replace(/\{[^}]*\}/g, '[^/]+');
  return new RegExp(`^${escaped}$`);
}

/** 内置一致性检查(规范 §5.1 的轻量实现;仓库自带 agent/validate_manifests.py 时优先调用它) */
function builtinValidate(root: string): { issues: string[]; warnings: string[] } {
  const issues: string[] = [];
  const warnings: string[] = [];
  const manifests = loadManifests(root);
  if (manifests.length === 0) {
    issues.push('rules/manifests/ 下没有任何 *.manifest.yaml');
    return { issues, warnings };
  }

  // 1. rule 名与 snakefile 存在性
  const usedPorts = new Set<string>();
  const outputPatterns: { pattern: string; ref: string }[] = [];
  for (const m of manifests) {
    const smkRel = m.snakefile;
    const smkAbs = smkRel ? path.join(root, smkRel) : '';
    const smkText = smkRel && fs.existsSync(smkAbs) ? fs.readFileSync(smkAbs, 'utf8') : null;
    if (!smkText) issues.push(`${m.sourceRef}: module.snakefile 不存在: ${smkRel || '(未声明)'}`);

    for (const r of m.rules) {
      const ref = `${m.sourceRef}#rules[name=${r.name}]`;
      if (smkText && !new RegExp(`rule\\s+${r.name}\\s*[:]`).test(smkText)) {
        issues.push(`${ref}: snakefile ${smkRel} 中找不到 rule ${r.name}`);
      }
      for (const io of [...(r.inputs ?? []), ...(r.outputs ?? [])]) {
        if (!io.port) issues.push(`${ref}: input/output ${io.name} 缺少 port 声明`);
        else usedPorts.add(io.port);
      }
      for (const o of r.outputs ?? []) {
        if (typeof (o as any).pattern === 'string') {
          outputPatterns.push({ pattern: (o as any).pattern, ref });
        }
      }
      // 2. config_requires 覆盖检查(启发式:snakefile 全文 config['x'] 顶层 key)
      if (smkText) {
        const declared = new Set((r.config_requires ?? []).map((k) => k.split('.')[0]));
        for (const match of smkText.matchAll(/config\[['"]([A-Za-z0-9_]+)['"]\]/g)) {
          if (!declared.has(match[1])) {
            warnings.push(
              `${m.sourceRef}: snakefile 读取 config['${match[1]}'],但 rule ${r.name} 的 config_requires 未声明(若由其他 rule 声明请忽略)`,
            );
          }
        }
      }
    }
  }

  // 3. 端口登记 + 5. 生产者闭合
  const portsFile = path.join(root, 'agent', 'ports.yaml');
  const registered = fs.existsSync(portsFile)
    ? (((readYaml(portsFile) as any)?.ports ?? {}) as Record<string, any>)
    : {};
  for (const p of usedPorts) {
    if (!registered[p]) issues.push(`端口未登记进 agent/ports.yaml: ${p}`);
  }
  const producerPorts = new Set<string>();
  for (const m of manifests)
    for (const r of m.rules) for (const o of r.outputs ?? []) if (o.port) producerPorts.add(o.port);
  for (const m of manifests)
    for (const r of m.rules)
      for (const i of r.inputs ?? []) {
        if (!i.port || producerPorts.has(i.port)) continue;
        const entry = registered[i.port];
        if (entry && entry.external === true) continue;
        warnings.push(`端口 ${i.port}(被 ${r.name} 消费)没有库内生产者;若为外部输入请在 ports.yaml 标 external: true`);
      }

  // 4. catalog target 可被某 rule output pattern 匹配
  const catalogFile = path.join(root, 'agent', 'catalog.yaml');
  if (fs.existsSync(catalogFile)) {
    const deliverables = ((readYaml(catalogFile) as any)?.deliverables ?? []) as any[];
    for (const d of deliverables) {
      for (const t of d.targets ?? []) {
        if (!outputPatterns.some(({ pattern }) => patternToRegex(pattern).test(t))) {
          issues.push(`catalog deliverable ${d.id} 的 target 无任何 rule output pattern 可匹配: ${t}`);
        }
      }
    }
  } else {
    issues.push('agent/catalog.yaml 不存在');
  }

  return { issues, warnings };
}

export function createValidateTool(cfg: FlowAgentConfig) {
  const root = cfg.workflowRoot;

  const validate_manifests = tool({
    description:
      '一致性校验(规范 §5.1):manifest 与 .smk 漂移、端口登记、catalog target 可匹配、config_requires 覆盖。仓库自带 agent/validate_manifests.py 时优先调用它,否则用内置检查。每次改动 manifest/组合文件后必须先跑。',
    inputSchema: z.object({
      task_id: z.string().describe('任务 ID,写入台账'),
    }),
    execute: async ({ task_id }) => {
      const pyScript = path.join(root, 'agent', 'validate_manifests.py');
      if (fs.existsSync(pyScript)) {
        const { code, stdout, stderr } = await runShell('python3', [pyScript], { cwd: root });
        const ok = code === 0;
        appendLedger(root, {
          ts: new Date().toISOString(),
          task_id,
          op: 'validate',
          object: 'agent/validate_manifests.py',
          result: ok ? 'success' : 'failed',
          detail: tail(stdout + '\n' + stderr, 20),
        });
        return { ok, runner: 'python', exit_code: code, output: tail(stdout + '\n' + stderr, 80) };
      }
      const { issues, warnings } = builtinValidate(root);
      const ok = issues.length === 0;
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'validate',
        object: 'builtin',
        result: ok ? 'success' : 'failed',
        detail: [...issues, ...warnings].slice(0, 20).join('\n'),
      });
      return { ok, runner: 'builtin', issues, warnings };
    },
  });

  return { validate_manifests };
}
