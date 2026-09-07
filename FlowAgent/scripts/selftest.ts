/**
 * 自测脚本:不调用 LLM,直接逐个执行工具,验证后端各环节可用。
 * 把 demo 仓库复制到临时目录,避免污染 demo(台账/缺口日志/组合目录都写在副本里)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { loadConfig, PACKAGE_ROOT } from '../src/config';
import { createTools } from '../src/tools';

type ToolMap = ReturnType<typeof createTools>;

let passed = 0;
let failed = 0;

async function check(name: string, fn: () => Promise<boolean>): Promise<void> {
  try {
    const ok = await fn();
    if (ok) {
      passed++;
      console.log(`PASS  ${name}`);
    } else {
      failed++;
      console.log(`FAIL  ${name}`);
    }
  } catch (e) {
    failed++;
    console.log(`FAIL  ${name}  异常: ${e}`);
  }
}

// 工具 execute 的第二个参数(AI SDK ToolCallOptions),自测中用桩代替
const stubCtx = { toolCallId: 'selftest', messages: [] } as any;

async function exec(tool: any, input: unknown): Promise<any> {
  return tool.execute(input, stubCtx);
}

async function main() {
  // demo → 临时副本
  const srcDemo = path.join(PACKAGE_ROOT, 'demo');
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flowagent-selftest-'));
  // 排除运行产物(conda 环境缓存可达数百 MB、日志、已生成输出、历史组合)
  const skip = /(\.snakemake|\/logs|\/01\.qc|\/00\.raw_data\/link_dir|run_ledger\.jsonl|\/composed\/20)/;
  fs.cpSync(srcDemo, tmp, { recursive: true, filter: (src) => !skip.test(src) });
  console.log(`selftest root: ${tmp}\n`);

  const cfg = { ...loadConfig(), workflowRoot: tmp };
  const tools: ToolMap = createTools(cfg);
  const task = 'selftest';
  const target = '01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html';

  await check('query_catalog(关键词:质控)', async () => {
    const r = await exec(tools.query_catalog, { keyword: '质控' });
    return r.ok && r.count >= 2;
  });

  await check('query_ports(全部摘要)', async () => {
    const r = await exec(tools.query_ports, {});
    return r.ok && r.count >= 5;
  });

  await check('query_ports(单个详情)', async () => {
    const r = await exec(tools.query_ports, { port: 'qc.fastqc.zip' });
    return r.ok && r.producers.length === 2;
  });

  await check('query_rules(produces_port=qc.fastqc.zip)', async () => {
    const r = await exec(tools.query_rules, { produces_port: 'qc.fastqc.zip' });
    return r.ok && r.count === 2;
  });

  await check('get_rule(short_read_multiqc_r1)', async () => {
    const r = await exec(tools.get_rule, { rule_name: 'short_read_multiqc_r1' });
    return r.ok && r.rule.config_requires.includes('parameter.threads.multiqc');
  });

  await check('validate_manifests(内置检查)', async () => {
    const r = await exec(tools.validate_manifests, { task_id: task });
    if (!r.ok) console.log('  issues:', r.issues);
    return r.ok === true;
  });

  await check('submit_cluster 无 dry-run 记录→拒绝(硬约束)', async () => {
    const r = await exec(tools.submit_cluster, { task_id: 'never-dry-run', targets: [target], confirmed: true });
    return r.ok === false && r.refused === true;
  });

  await check('dry_run(catalog target)', async () => {
    const r = await exec(tools.dry_run, { task_id: task, targets: [target] });
    if (!r.ok) console.log('  output:', r.output_tail);
    return r.ok === true;
  });

  await check('detailed_summary', async () => {
    const r = await exec(tools.detailed_summary, { task_id: task, targets: [target] });
    return r.ok === true;
  });

  await check('submit_cluster dry-run 已过但未开启开关→拒绝', async () => {
    const r = await exec(tools.submit_cluster, { task_id: task, targets: [target], confirmed: true });
    return r.ok === false && r.refused === true && !r.reason.includes('台账');
  });

  await check('write_composed + 组合 dry-run', async () => {
    const snakefile = [
      'configfile: "../../config/config.yaml"',
      'configfile: "../../envs/general_software.yaml"   # 软件路径(Rust 二进制)',
      '',
      'workdir: "../.."   # 目标/路径均相对仓库根',
      '',
      'module qc:',
      '    snakefile: "../../rules/03.short_read_qc.smk"',
      '    config: config',
      '',
      'module md5:',
      '    snakefile: "../../rules/02.file_convert_md5.smk"',
      '    config: config',
      '',
      'use rule * from md5 as md5_*',
      'use rule short_read_qc_r1 from qc as qc_short_read_qc_r1',
      'use rule short_read_multiqc_r1 from qc as qc_short_read_multiqc_r1',
      '',
      'rule all:',
      `    input: "${target}"`,
      '',
    ].join('\n');
    const w = await exec(tools.write_composed, { task_id: task, name: 'selftest_qc_only', snakefile });
    if (!w.ok) {
      console.log('  write_composed:', w.error);
      return false;
    }
    const r = await exec(tools.dry_run, { task_id: task, targets: [target], workdir: w.dir });
    if (!r.ok) console.log('  composed dry-run:', r.output_tail);
    return r.ok === true;
  });

  await check('write_composed 同名目录→拒绝覆盖', async () => {
    const r = await exec(tools.write_composed, { task_id: task, name: 'selftest_qc_only', snakefile: 'rule all:\n    input: "x"\n' });
    return r.ok === false;
  });

  await check('write_gap_log(缺口记录+计数)', async () => {
    const r1 = await exec(tools.write_gap_log, { task_id: task, need: '差异表达分析', missing: 'deliverable:diff_expr' });
    const r2 = await exec(tools.write_gap_log, { task_id: task, need: '差异表达分析', missing: 'deliverable:diff_expr' });
    return r1.ok && r2.ok && r2.count === 2;
  });

  await check('read_log(路径越界→拒绝)', async () => {
    const r = await exec(tools.read_log, { path: '../../etc/passwd', tail_lines: 5 });
    return r.ok === false;
  });

  await check('台账已记录 dry_run 成功', async () => {
    const ledger = fs.readFileSync(path.join(tmp, 'agent', 'run_ledger.jsonl'), 'utf8');
    return ledger.includes('"op":"dry_run"') && ledger.includes('"result":"success"');
  });

  console.log(`\n结果: ${passed} 通过, ${failed} 失败`);
  process.exit(failed === 0 ? 0 : 1);
}

main();
