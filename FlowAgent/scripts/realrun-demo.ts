/**
 * demo 真实运行:走后端真实工具路径 dry_run → submit_cluster(后台 snakemake --use-conda)。
 * 首次会构建 fastqc/multiqc conda 环境,耗时几分钟。
 * 用法:FLOWAGENT_ALLOW_SUBMIT=true npm run demo:run
 */
import { loadConfig } from '../src/config';
import { createTools } from '../src/tools';
import { hasSuccessfulDryRun } from '../src/ledger';

const target = process.argv[2] ?? '01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html';
const taskId = 'demo_realrun';
const ctx = { toolCallId: 'realrun', messages: [] } as any;

async function main() {
  const cfg = loadConfig();
  const tools = createTools(cfg);
  console.log(`root: ${cfg.workflowRoot}\ntarget: ${target}\n`);

  if (!hasSuccessfulDryRun(cfg.workflowRoot, taskId, [target])) {
    console.log('[1/2] dry_run(含 --use-conda)...');
    const d: any = await tools.dry_run.execute!({ task_id: taskId, targets: [target] }, ctx);
    console.log(`dry_run ok = ${d.ok}`);
    if (!d.ok) {
      console.log(d.output_tail);
      process.exit(1);
    }
  } else {
    console.log('[1/2] 台账已有本任务 dry-run 成功记录,跳过');
  }

  console.log('[2/2] submit_cluster...');
  const r: any = await tools.submit_cluster.execute!(
    { task_id: taskId, targets: [target], confirmed: true },
    ctx,
  );
  console.log(JSON.stringify(r, null, 2));
  if (r.ok) {
    console.log(`\n跟踪进度: tail -f ${cfg.workflowRoot}/${r.log_file}`);
  }
}

main();
