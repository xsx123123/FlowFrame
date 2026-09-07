import fs from 'node:fs';
import path from 'node:path';
import { tool } from 'ai';
import { z } from 'zod';
import { safeJoin, type FlowAgentConfig } from '../config';
import { appendLedger, hasSuccessfulDryRun } from '../ledger';
import { runShell, spawnDetached, tail } from '../shell';

const taskFields = {
  task_id: z.string().describe('任务 ID(如 20260907_qc_only),贯穿 dry_run/summary/submit,硬约束校验依赖它'),
  targets: z.array(z.string()).min(1).describe('snakemake target 文件路径列表'),
  workdir: z
    .string()
    .optional()
    .describe('相对仓库根的工作目录(组合流程填 composed/<日期>_<需求>/),默认仓库根'),
};

export function createSnakemakeTools(cfg: FlowAgentConfig) {
  const root = cfg.workflowRoot;

  const condaArgs = cfg.useConda ? ['--use-conda', '--conda-frontend', cfg.condaFrontend] : [];

  const dry_run = tool({
    description:
      '封装 snakemake -n:验证流程组合定义正确性,不真正执行。结果自动写入运行台账,是 submit_cluster 的前置硬约束。同一报错连续失败 2 次必须停止并报告用户。create_envs_only=true 时用 --conda-create-envs-only 预建全部 conda 环境(不跑任务),用于正式运行前排掉环境构建失败。',
    inputSchema: z.object({
      ...taskFields,
      create_envs_only: z.boolean().optional().describe('true 时预建 conda 环境(--conda-create-envs-only),不执行任务'),
    }),
    execute: async ({ task_id, targets, workdir, create_envs_only }) => {
      let cwd: string;
      try {
        cwd = safeJoin(root, workdir ?? '.');
      } catch (e) {
        return { ok: false, error: String(e) };
      }
      const args = ['-n', '--cores', '1', ...condaArgs];
      if (create_envs_only && cfg.useConda) args.push('--conda-create-envs-only');
      args.push(...targets);
      const { code, stdout, stderr } = await runShell('snakemake', args, {
        cwd,
        timeoutMs: create_envs_only ? 1_800_000 : 180_000,
      });
      const ok = code === 0;
      const output = tail(stdout + '\n' + stderr, 100);
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'dry_run',
        target: targets.join(' '),
        object: workdir ?? '.',
        result: ok ? 'success' : 'failed',
        detail: tail(output, 20),
      });
      return {
        ok,
        exit_code: code,
        output_tail: output,
        next: ok
          ? 'dry-run 通过:调用 detailed_summary 生成计算规模,交用户确认后才可 submit_cluster'
          : '按报错对策表修复后重跑本步;同一报错连续失败 2 次则停止并报告用户',
      };
    },
  });

  const detailed_summary = tool({
    description:
      '封装 snakemake --detailed-summary:列出每个目标文件的状态/计划/输入输出,供用户确认计算规模。dry_run 通过后、submit_cluster 之前调用。',
    inputSchema: z.object(taskFields),
    execute: async ({ task_id, targets, workdir }) => {
      let cwd: string;
      try {
        cwd = safeJoin(root, workdir ?? '.');
      } catch (e) {
        return { ok: false, error: String(e) };
      }
      const { code, stdout, stderr } = await runShell('snakemake', ['--detailed-summary', ...targets], { cwd });
      const ok = code === 0;
      const output = tail(stdout + '\n' + stderr, 120);
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'summary',
        target: targets.join(' '),
        object: workdir ?? '.',
        result: ok ? 'success' : 'failed',
        detail: tail(output, 20),
      });
      return { ok, exit_code: code, summary: output };
    },
  });

  const read_log = tool({
    description: '读取仓库内日志文件尾部(如 logs/03.short_read_qc/xxx.log),用于运行期报错定位。只读,路径不允许越出仓库根。',
    inputSchema: z.object({
      path: z.string().describe('相对仓库根的日志路径'),
      tail_lines: z.number().int().positive().max(500).default(100),
    }),
    execute: async ({ path: rel, tail_lines }) => {
      let abs: string;
      try {
        abs = safeJoin(root, rel);
      } catch (e) {
        return { ok: false, error: String(e) };
      }
      if (!fs.existsSync(abs)) return { ok: false, error: `日志不存在: ${rel}` };
      return { ok: true, path: rel, content: tail(fs.readFileSync(abs, 'utf8'), tail_lines) };
    },
  });

  const submit_cluster = tool({
    description:
      '正式提交运行(硬约束工具)。两道闸:① 台账中必须存在本 task_id(或同 targets)的 dry_run 成功记录,否则直接拒绝;② 服务端需开启 FLOWAGENT_ALLOW_SUBMIT。调用前必须已用 detailed_summary 向用户确认计算规模并得到明确同意。提交为后台进程,固定带 --rerun-incomplete。',
    inputSchema: z.object({
      ...taskFields,
      confirmed: z.literal(true).describe('必须为 true,表示用户已确认 detailed_summary 的计算规模'),
    }),
    execute: async ({ task_id, targets, workdir, confirmed }) => {
      if (!hasSuccessfulDryRun(root, task_id, targets)) {
        return {
          ok: false,
          refused: true,
          reason: `台账(agent/run_ledger.jsonl)中不存在 task_id=${task_id} 或相同 targets 的 dry_run 成功记录。铁律 2:dry-run 未通过禁止提交。请先调用 dry_run。`,
        };
      }
      if (!cfg.allowSubmit) {
        return {
          ok: false,
          refused: true,
          reason: '服务端未开启正式提交(FLOWAGENT_ALLOW_SUBMIT!=true)。dry-run 记录齐全,请用户决定是否在服务端开启后重试。',
        };
      }
      let cwd: string;
      try {
        cwd = safeJoin(root, workdir ?? '.');
      } catch (e) {
        return { ok: false, error: String(e) };
      }
      const logFile = path.join(root, 'logs', 'flowagent', `submit-${task_id}-${Date.now()}.log`);
      const pid = spawnDetached(
        'snakemake',
        ['--cores', String(cfg.cores), '--rerun-incomplete', ...condaArgs, ...cfg.extraArgs, ...targets],
        { cwd, logFile },
      );
      appendLedger(root, {
        ts: new Date().toISOString(),
        task_id,
        op: 'submit',
        target: targets.join(' '),
        object: workdir ?? '.',
        result: 'started',
        detail: `pid=${pid} log=${path.relative(root, logFile)}`,
        confirmed,
      });
      return {
        ok: true,
        pid,
        log_file: path.relative(root, logFile),
        note: '已后台提交(带 --rerun-incomplete)。用 read_log 跟踪进度;失败时读对应 rule 日志修复后重新 submit(仍为断点续跑)。',
      };
    },
  });

  return { dry_run, detailed_summary, read_log, submit_cluster };
}
