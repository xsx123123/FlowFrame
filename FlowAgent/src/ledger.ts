import fs from 'node:fs';
import path from 'node:path';

export type LedgerOp = 'dry_run' | 'summary' | 'submit' | 'validate' | 'error' | 'fix' | 'human_decision';

export interface LedgerEntry {
  ts: string;
  task_id: string;
  op: LedgerOp;
  target?: string;
  object?: string;
  result?: string;
  detail?: string;
  confirmed?: boolean;
}

export function ledgerPath(root: string): string {
  return path.join(root, 'agent', 'run_ledger.jsonl');
}

export function appendLedger(root: string, entry: LedgerEntry): void {
  const p = ledgerPath(root);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.appendFileSync(p, JSON.stringify(entry) + '\n');
}

export function readLedger(root: string): LedgerEntry[] {
  const p = ledgerPath(root);
  if (!fs.existsSync(p)) return [];
  return fs
    .readFileSync(p, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line) as LedgerEntry;
      } catch {
        return null;
      }
    })
    .filter((e): e is LedgerEntry => e !== null);
}

function normTargets(targets: string[]): string {
  return [...targets].sort().join(' ');
}

/** 硬约束依据:台账中是否存在本任务(同 task_id 或同 target 集合)的 dry_run 成功记录 */
export function hasSuccessfulDryRun(root: string, taskId: string, targets: string[]): boolean {
  const want = normTargets(targets);
  return readLedger(root).some(
    (e) =>
      e.op === 'dry_run' &&
      e.result === 'success' &&
      (e.task_id === taskId || (e.target !== undefined && normTargets(e.target.split(' ')) === want)),
  );
}
