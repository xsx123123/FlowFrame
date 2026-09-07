# FlowAgent —— Flow(Snakemake) × AI Agent 编排后端

按《Flow × AI Agent:Rule 级自动编排与自修复技术规范》(`../Flow-AI_Rule_Orchestration.md`)§8.2 / §9.6 实现的 Agent 后端:

- **循环底座**:Vercel AI SDK `streamText` + tools(不手写 tool-calling 循环),模型走 OpenAI 兼容接口(默认火山方舟,可换);
- **大脑**:SOP 蒸馏 system prompt(决策流程 §6.1 + 禁区六条 §6.4 + 报错对策表 §6.3);
- **手**:11 个工具,全部只读仓库,写操作仅限 `composed/`、`agent/gap_log.yaml`、`agent/run_ledger.jsonl`;
- **界面**:`CLI 优先`(规范 §8.4),同时内置 HTTP server,暴露 `POST /api/chat`(Vercel AI SDK UI message stream),前端 `useChat` 直接对接。

## 快速开始

```bash
cd FlowAgent
npm install
cp .env.example .env   # 填入 FLOWAGENT_API_KEY 等
```

### 命令行测试

```bash
# 自测(不调 LLM,验证工具层全部可用;在 demo 副本上跑,不污染 demo)
npm run selftest

# 一句话模式
npm run cli -- "对这批样本出原始数据 R1 的 MultiQC 质控报告"

# 交互模式(多轮)
npm run cli
```

不配置 `FLOW_WORKFLOW_ROOT` 时默认指向内置 `demo/` 迷你流程仓库(2 个模块、6 条 rule、2 个样本),`snakemake -n` 可真实跑通。指向真实流程仓库:

```bash
FLOW_WORKFLOW_ROOT=/path/to/RNAFlow npm run cli
```

### 接入 Vercel AI SDK UI 前端

```bash
npm run server   # 默认 http://localhost:3090
```

前端(AI SDK v5):

```ts
import { useChat } from '@ai-sdk/react';
import { DefaultChatTransport } from 'ai';

const { messages, sendMessage, status } = useChat({
  transport: new DefaultChatTransport({ api: 'http://localhost:3090/api/chat' }),
});
```

AI SDK v4 前端:`useChat({ api: 'http://localhost:3090/api/chat' })`。
已开 CORS,前后端可分端口开发;生产可反代同域。

## 工具清单(规范 §9.6 的 7 个 + 4 个必要补充)

| 工具 | 作用 | 规范依据 |
|---|---|---|
| `query_catalog` | 需求 → 现成 deliverable/target | §6.1 ① |
| `query_ports` | 端口注册表查询(生产者/消费者) | §6.1 ② |
| `query_rules` / `get_rule` | rule 摘要检索 / 按需取详情(渐进式披露) | §6.5 |
| `validate_manifests` | manifest/端口/catalog 一致性校验;仓库自带 `agent/validate_manifests.py` 时优先调用 | §5.1 |
| `dry_run` | `snakemake -n` 封装,结果写台账 | §9.6 硬约束 2 |
| `detailed_summary` | `snakemake --detailed-summary`,交用户确认规模 | §6.1 ⑥ |
| `read_log` | 读 rule 日志(路径限制在仓库内) | §6.1 ⑦ |
| `submit_cluster` | **硬约束**:无本任务 dry-run 通过记录 → 拒绝;未开 `FLOWAGENT_ALLOW_SUBMIT` → 拒绝;通过则后台提交(固定 `--rerun-incomplete`) | §9.6 硬约束 1 |
| `write_gap_log` | 缺口日志(自动累计次数) | §4.5 |
| `write_composed` | `composed/<日期>_<需求>/` 新建组合文件(已存在拒绝覆盖;检测复制粘贴 rule 本体则拒绝) | §6.2 |

## 运行台账 run ledger

`agent/run_ledger.jsonl`(在目标流程仓库内),每次 dry_run / summary / validate / submit / gap 全量记录:

```json
{"ts":"2026-09-07T...","task_id":"20260907_qc_only","op":"dry_run","target":"01.qc/...","result":"success"}
```

`submit_cluster` 的硬约束直接查询该台账。断网/重启后可根据台账恢复任务上下文(规范 §9.6 验收 3)。

## 目录结构

```text
FlowAgent/
├── src/
│   ├── config.ts          # env/.env 加载、路径越界防护(safeJoin)
│   ├── system-prompt.ts   # SOP 蒸馏(铁律六条/决策流程/对策表)
│   ├── agent.ts           # streamText 循环(CLI 与 server 共用)
│   ├── ledger.ts          # run ledger JSONL
│   ├── cli.ts             # 命令行入口(单句/交互)
│   ├── server.ts          # HTTP:POST /api/chat + GET /api/health
│   └── tools/             # registry / validate / snakemake / writes
├── scripts/selftest.ts    # 无 LLM 自测(15 项)
└── demo/                  # 内置迷你 Flow 仓库(smk + manifests + ports + catalog)
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `FLOWAGENT_BASE_URL` | `https://ark.cn-beijing.volces.com/api/v3` | OpenAI 兼容接口地址 |
| `FLOWAGENT_API_KEY` | (回退 `ARK_API_KEY`) | 必填 |
| `FLOWAGENT_MODEL` | `doubao-seed-1-6-250615` | 模型/接入点 ID |
| `FLOW_WORKFLOW_ROOT` | `./demo` | 目标 Flow 流程仓库根 |
| `FLOWAGENT_PORT` | `3090` | server 端口 |
| `FLOWAGENT_ALLOW_SUBMIT` | `false` | 正式提交总开关(硬约束第二道闸) |
| `FLOWAGENT_CORES` | `8` | submit 时的 --cores |
| `FLOWAGENT_USE_CONDA` | `true` | dry_run/submit 带 `--use-conda`,rule 的 `conda: envs/*.yaml` 自动建环境 |
| `FLOWAGENT_CONDA_FRONTEND` | `conda` | 装了 mamba/micromamba 可改 `mamba` 提速 |
| `FLOWAGENT_EXTRA_ARGS` | 空 | submit 追加参数,如 `--logger rich-loguru`(真实 Flow 仓库强制) |

## 环境与软件如何工作(真实运行)

- **conda 环境**:rule 在 `.smk` 里声明 `conda: envs/xxx.yaml`(manifest 的 `conda` 字段同步登记),`dry_run` / `submit_cluster` 已带 `--use-conda`,snakemake 自动建环境;`dry_run(create_envs_only=true)` 可在正式跑之前预建全部环境(`--conda-create-envs-only`),把网络/源问题提前暴露;
- **Rust 二进制**(seq_preprocessor / json_md5_verifier):生产 Flow 仓库中路径走 `config.software.*`(规范 §3.3),Agent 通过 manifest `config_requires` 知道该查哪些 key;日志报 command not found 时向用户要路径,不改 rule;
- **demo 仓库已真实可跑**:vendor 原始 fastq(真实 gzip 数据 + md5.txt)→ **真实 Rust 二进制** `seq_preprocessor` / `json_md5_verifier`(路径走 `envs/general_software.yaml` 的 `general_software` 段,生产仓库对应 `config.software`)→ 真实 fastqc/multiqc(conda 环境)。一条命令实测:

```bash
FLOWAGENT_ALLOW_SUBMIT=true npm run demo:run
# 指定 target: npm run demo:run -- "01.qc/short_read_r2_multiqc/multiqc_r2_raw-data_report.html"
# 跟踪: tail -f demo/logs/flowagent/submit-demo_realrun-*.log
```

> demo 的 vendor 数据带有 `md5.txt`(厂商校验文件);若缺失,seq_preprocessor 的 JSON 报告不含 md5 字段,json_md5_verifier 会「无可校验记录」不产出门控文件——真实项目请确认厂商 md5 就位(SRA 数据除外,规范 §4.1)。

## 与规范的对应关系 / 后续路线

- 本目录即规范的「第 3 级:自建 Agent 应用」骨架(§8.1);资产(manifest/catalog/ports/SOP)仍在目标流程仓库,本后端零绑定、换模型只换 env;
- 真实仓库侧的阶段 0~3 资产(全库 manifest、AGENTS.md、validate_manifests.py)按规范第九章另行建设,建成后把 `FLOW_WORKFLOW_ROOT` 指过去即可,后端无需改动;
- `submit_cluster` 当前为本地后台 `snakemake --rerun-incomplete`;接 HPC 时将其替换为集群提交命令(台账硬约束逻辑不变)。
