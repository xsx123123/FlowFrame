# Flow × AI Agent：Rule 级自动编排与自修复技术规范

> 版本：v1.0（2026-09-03）
> 状态：规范定稿。阶段 0 审计报告回填后，更新「现状审计」章节。
> 适用范围：Flow（Snakemake）流程；思想可推广到 Nextflow / WDL / Makefile / CI 等声明式流程。

---

## 一、范围与目标

### 1.1 适用范围

本规范定义 Flow（Snakemake）流程与 AI Agent 协作时的接口契约、数据模型、编排规则与验证方式。适用对象包括：

- 流程开发者：编写并维护 Snakemake rule 及其 manifest；
- Agent 开发者：实现或配置按本规范执行编排与修复的 Agent；
- 实施人员：按阶段实施本规范，并逐项验收。

### 1.2 目标能力

用户用自然语言提出分析需求后，Agent 按本规范自动完成以下步骤：

1. **选择 rule**：从 rule 库中选出能完成该需求的 rule 组合；若发现缺口，提议新增 rule，不猜测；
2. **配置样本与参数**：生成或修正 sample sheet 与 `config.yaml`，使配对信息在配置层完成，rule 本身不做改动；
3. **验证流程**：使用 `snakemake -n` 对整条流程组合做 dry-run 验证，读取报错并修复，直到通过；
4. **运行与修复**：给出计算量摘要，经用户确认后正式运行；运行失败时读取 log 自修复，并使用 `--rerun-incomplete` 断点续跑。

### 1.3 核心设计判断

1. **Snakemake 已实现「拼接」算法**：当目标文件已包含在 workflow 的 target 中时，装入全部 rule 后，Snakemake 会按 input/output 模式自动 backward-chaining 反推所需 rule 子集。Agent 不需要自己拼 DAG，只需要选对 target。

2. **纯 LLM 生成的流程无法直接执行**：业界 benchmark 显示 LLM 直接生成的 workflow 结构准确率可达 93%，但没有一份能不经修正直接执行。因此本方案的核心不是「更强的描述」，而是 **结构化契约 + dry-run 验证回路**。[^1]

3. **描述补充的是契约，不是散文**：现有 rule docstring 对人友好，但对 Agent 精确匹配不够用。需要机器可读的三件套：**rule manifest（接口契约）+ 端口注册表（谁产谁消）+ 产物目录（需求→target 映射）**。

### 1.4 总体架构

```
用户需求（自然语言）
      │
      ▼
┌─────────────────────────────────────────────┐
│  Agent 决策层（SOP 驱动）                     │
│   1. 查 catalog.yaml：需求→现成 target？      │
│   2. 查 ports.yaml + manifests：端口可成链？  │
│   3. 断链 → 提议新 rule（创建后测试，人工审核后入库）│
└─────────────────────────────────────────────┘
      │  产出：target 列表 + sample sheet + config（+ 可选组合 Snakefile）
      ▼
┌─────────────────────────────────────────────┐
│  Snakemake 执行层（本方案不改动其机制）        │
│   backward-chaining 自动选定 rule 子集        │
│   -n dry-run → --detailed-summary → 正式运行  │
└─────────────────────────────────────────────┘
      │  报错回灌：MissingInput / AmbiguousRule / KeyError …
      ▼
  Agent 读报错 + 读 log → 修复（优先级：样本表 > config > 组合文件 > 新 rule > 已有 rule）
```

`catalog.yaml` 与 `ports.yaml` 可以只保存每个 rule 的描述和详情位置链接，实现渐进式披露，避免一次性加载全部 rule 导致模型注意力稀释。

---

## 二、术语与约定

| 术语 | 含义 |
|---|---|
| **rule** | Snakemake 中的基本执行单元，定义 input/output/params/shell 等。 |
| **module** | 一个 `.smk` 文件，包含一组相关 rule。 |
| **manifest** | 描述模块与 rule 接口契约的 YAML 文件，供 Agent 读取。 |
| **port（端口）** | 语义化的 input/output 类型，例如 `reads.r1.fastq`。 |
| **deliverable** | 用户会直接开口要的最终产物，对应 `catalog.yaml` 条目。 |
| **流程组合** | 为满足某一需求，由 Agent 选择 rule 并生成的可执行 Snakemake 流程。 |
| **组合 Snakefile** | Agent 生成的、引用库中 rule 的 Snakefile，位于 `composed/` 目录。 |
| **动态 rule** | 包含 checkpoint、input 函数、`unpack()` 等运行期才能确定 I/O 的 rule。 |
| **gate** | 只作为依赖闸门、不进入 shell 命令的 input。 |
| **SOP** | Agent 必须遵守的标准操作流程，写入 `AGENTS.md`。 |
| **run ledger** | 记录每次 dry-run、提交、报错、修复与人工决策的 JSONL 审计日志。 |

本规范使用以下助动词：

- **必须**：强制性要求，违反将导致方案无法正常工作或产生严重风险；
- **禁止**：绝对不允许的行为；
- **应该**：强烈推荐，允许有正当理由的例外；
- **可以**：可选做法，按实际情况决定。

---

## 三、关键约束与设计原则

这四个约束是整套方案的地基，阶段 0 审计和后续所有建设都围绕它们展开。

### 3.1 接口契约化：类型化端口

现状：rule 之间靠硬编码路径字符串衔接（如下面的 `01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip` 同时出现在上游 output 和下游 input 的 `expand` 里）。人能看懂，但 Agent 靠字符串匹配猜上下游关系既脆弱又不可靠。

```snakemake
rule short_read_qc_r1:
    input:
        md5_check = "01.qc/md5_check.tsv",
        link_r1_dir = os.path.join("00.raw_data",
                                      config['convert_md5'],
                                      "{sample}/{sample}_R1.fq.gz"),
    output:
        r1_html = "01.qc/short_read_qc_r1/{sample}_R1_fastqc.html",
        r1_zip = "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip",
    ...

rule short_read_multiqc_r1:
    input:
        fastqc_files_r1 = expand("01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip", sample=samples.keys()),
    output:
        report_dir = "01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html",
    ...
```

**要求**：给每个 input/output 声明一个语义端口类型。Agent 组链时，在端口注册表 `ports.yaml` 里做「生产者—消费者」匹配，而不是猜路径。路径约定（编号目录 + `{sample}` 通配符）保持不变，端口是叠加在现有路径之上的语义层，不要求重排目录结构。

| port 类型 | 语义 | 生产者（例） | 消费者（例） |
|---|---|---|---|
| `reads.r1.fastq` | R1 原始/处理后 reads | 数据链接 / trim 模块 | `short_read_qc_r1`、比对模块 |
| `qc.fastqc.zip` | FastQC 原始数据包 | `short_read_qc_r1/r2` | `short_read_multiqc_r1/r2` |
| `qc.md5_check.tsv` | MD5 校验闸门 | md5 校验 rule | 所有读原始数据的 rule（gate） |
| `align.bam` | 比对结果 BAM | 比对模块 | 定量 / 质控模块 |

### 3.2 解开全局 config 耦合

现状：rule 体直接读全局 config（如 `config['convert_md5']`、`config['parameter']['threads']['fastqc']`）。单条固定流程没问题；跨模块自由组合时，被选中 rule 依赖的 config key 缺一不可，缺了就是运行期 `KeyError`。

**要求**：每个 rule 在 manifest 中声明 `config_requires`（key 路径列表）。组合时由校验脚本自动合并所选 rule 的 `config_requires`，对照当前 config 文件逐项检查，缺 key 在 dry-run 之前就必须报出，并说明应补什么。

### 3.3 隐性依赖显式化

现状：`rule_resource(config, 'low_resource', skip_queue_on_local=True, logger=logger)` 这类自定义 Python helper 是 rule 的隐性运行时依赖——把 rule 摘到别的上下文就 `NameError`。

**要求**：

1. 把所有自定义 helper 固定收拢进一个公共 `rules/common.smk`（或已存在则盘点登记）；
2. 每个模块 manifest 声明 `requires_helpers`；
3. Agent 生成的任何组合 Snakefile，第一行必须 `include: "rules/common.smk"`——写进 SOP 禁区条款。

### 3.4 动态 rule 打标，禁止静态推断

现状与风险：若流程中存在 checkpoint、input 函数（`input: lambda wc: ...`）、`unpack()` 等动态 I/O，Agent 读代码「脑补」上下游必然出错。

**要求**：manifest 中标注 `dynamic: true`；SOP 规定 Agent 对动态 rule **只许 dry-run 实测**（`snakemake -n` 会真实展开 DAG），禁止凭读代码断言其 I/O。

### 3.5 使用 Snakemake module 系统

Snakemake（≥6.0）原生支持跨流程复用 rule：

```python
module qc:
    snakefile: "rules/03.short_read_qc.smk"
    config: config

use rule * from qc as qc_*
```

**要求**：Agent 组合新流程时**必须生成一个新的组合 Snakefile**（如 `composed/20260903_xxx需求/Snakefile`），用 `module` + `use rule` 从模块库挑选所需 rule，加命名空间前缀避免 `AmbiguousRuleException`，库中原始文件一行不改。这比复制粘贴 rule 干净、可回溯、可随时整体废弃。

### 3.6 Agent 可直接调用的命令

下表命令由 Snakemake 原生提供，Agent 可以直接调用，无需额外开发。

| 命令 | 用途 |
|---|---|
| `snakemake -n <targets>` | dry-run：验证流程组合定义正确性，不真正执行 |
| `snakemake --detailed-summary <targets>` | 每个文件的状态/计划/输入输出，供用户确认规模 |
| `snakemake --dag <targets> \| dot -Tsvg` | 导出实际 DAG 图 |
| `snakemake --rulegraph` | 导出 rule 级依赖图（不看样本展开） |
| `snakemake --lint` | 流程质量检查 |
| `snakemake --list-target-rules` | 列出全部目标 rule |
| `snakemake --rerun-incomplete` | 失败后断点续跑，不重跑已完成部分 |
| `python agent/validate_manifests.py` | manifest / 端口 / catalog 一致性校验 |

---

## 四、数据模型

### 4.1 Rule Manifest

每个 `.smk` 模块配一个同名 manifest：`rules/manifests/<模块名>.manifest.yaml`。

```yaml
# rules/manifests/03.short_read_qc.manifest.yaml
module:
  id: "03.short_read_qc"
  title: "原始短读长质控（FastQC + MultiQC）"
  snakefile: "rules/03.short_read_qc.smk"
  stage: "01.qc"
  summary: >
    对原始 R1/R2 reads 分别运行 FastQC，并用 MultiQC 分方向聚合全部样本报告，
    为下游去接头/质控决策提供依据。
  requires_helpers: ["rule_resource", "logger"]

rules:
  - name: short_read_qc_r1
    summary: "对单个样本的 R1 reads 运行 FastQC"
    wildcards: ["sample"]
    dynamic: false
    inputs:
      - name: link_r1_dir
        port: "reads.r1.fastq"
        pattern: "00.raw_data/{md5dir}/{sample}/{sample}_R1.fq.gz"
        config_keys: ["convert_md5"]
      - name: md5_check
        port: "qc.md5_check.tsv"
        pattern: "01.qc/md5_check.tsv"
        role: gate
    outputs:
      - name: r1_html
        port: "qc.fastqc.html"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.html"
        kind: report
      - name: r1_zip
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip"
        kind: intermediate
    config_requires:
      - "convert_md5"
      - "parameter.threads.fastqc"
    conda: "envs/fastqc.yaml"
    resources_tier: "low_resource"

  - name: short_read_qc_r2
    summary: "对单个样本的 R2 reads 运行 FastQC"
    wildcards: ["sample"]
    dynamic: false
    inputs:
      - name: link_r2_dir
        port: "reads.r2.fastq"
        pattern: "00.raw_data/{md5dir}/{sample}/{sample}_R2.fq.gz"
        config_keys: ["convert_md5"]
      - name: md5_check
        port: "qc.md5_check.tsv"
        pattern: "01.qc/md5_check.tsv"
        role: gate
    outputs:
      - name: r2_html
        port: "qc.fastqc.html"
        pattern: "01.qc/short_read_qc_r2/{sample}_R2_fastqc.html"
        kind: report
      - name: r2_zip
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r2/{sample}_R2_fastqc.zip"
        kind: intermediate
    config_requires:
      - "convert_md5"
      - "parameter.threads.fastqc"
    conda: "envs/fastqc.yaml"
    resources_tier: "low_resource"

  - name: short_read_multiqc_r1
    summary: "聚合全部样本 R1 FastQC 结果为一份 MultiQC 报告"
    wildcards: []
    dynamic: false
    per_sample: false
    inputs:
      - name: fastqc_files_r1
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip"
        expand_over: samples
    outputs:
      - name: report_dir
        port: "qc.multiqc.report"
        pattern: "01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html"
        kind: report
    config_requires:
      - "parameter.threads.multiqc"
    conda: "envs/multiqc.yaml"
    resources_tier: "low_resource"

  - name: short_read_multiqc_r2
    summary: "聚合全部样本 R2 FastQC 结果为一份 MultiQC 报告"
    wildcards: []
    dynamic: false
    per_sample: false
    inputs:
      - name: fastqc_files_r2
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r2/{sample}_R2_fastqc.zip"
        expand_over: samples
    outputs:
      - name: report
        port: "qc.multiqc.report"
        pattern: "01.qc/short_read_r2_multiqc/multiqc_r2_raw-data_report.html"
        kind: report
    config_requires:
      - "parameter.threads.multiqc"
    conda: "envs/multiqc.yaml"
    resources_tier: "low_resource"
```

#### Schema 字段约定

| 字段 | 必填 | 含义 |
|---|---|---|
| `module.requires_helpers` | 是 | 该模块用到的自定义 Python helper 名列表 |
| `rules[].port` | 是 | 语义端口类型；新类型必须先登记进 `ports.yaml` |
| `rules[].role: gate` | 否 | 该 input 只是依赖闸门、不出现在命令里 |
| `rules[].kind` | 是 | `report`（最终交付）/ `intermediate`（中间产物） |
| `rules[].dynamic` | 是 | 是否动态 I/O；`true` 时 Agent 禁止静态推断 |
| `rules[].expand_over: samples` | 否 | 该 input 对样本表全体展开（聚合 rule 标志） |
| `rules[].config_requires` | 是 | 该 rule 读取的全部 config key 路径 |
| `rules[].per_sample` | 是 | 是否按样本通配（决定组合时如何展开） |

### 4.2 端口注册表 `agent/ports.yaml`

```yaml
# agent/ports.yaml —— 全部端口类型的中央注册表；新增端口类型必须先登记
ports:
  reads.r1.fastq:
    description: "R1 reads（fastq.gz），可能为原始或去接头后"
    producers: ["00.link_dir / trim 模块 rule 名"]
    consumers: ["03.short_read_qc.short_read_qc_r1", "..."]
  qc.fastqc.zip:
    description: "FastQC 原始数据包（单样本单方向）"
    producers: ["03.short_read_qc.short_read_qc_r1", "03.short_read_qc.short_read_qc_r2"]
    consumers: ["03.short_read_qc.short_read_multiqc_r1", "03.short_read_qc.short_read_multiqc_r2"]
  qc.md5_check.tsv:
    description: "MD5 校验通过闸门文件"
    producers: ["<md5 校验 rule 名>"]
    consumers: ["所有直接读原始数据的 rule（role: gate）"]
```

`producers` / `consumers` 列由校验脚本从各 manifest 自动生成更新，人工只维护 `description`。

### 4.3 产物目录 `agent/catalog.yaml`

```yaml
# agent/catalog.yaml —— Agent 回答「我要 X 结果」时的查询入口
deliverables:
  - id: raw_multiqc_r1
    name: "原始数据 R1 MultiQC 汇总报告"
    description: "全部样本 R1 FastQC 聚合报告，评估原始数据质量、接头污染、批次异常"
    targets:
      - "01.qc/short_read_r1_multiqc/multiqc_r1_raw-data_report.html"
    keywords: ["质控", "QC", "fastqc", "multiqc", "原始数据", "R1"]
  - id: raw_multiqc_r2
    name: "原始数据 R2 MultiQC 汇总报告"
    targets:
      - "01.qc/short_read_r2_multiqc/multiqc_r2_raw-data_report.html"
    keywords: ["质控", "QC", "multiqc", "原始数据", "R2"]
```

每个「用户会开口要的最终结果」一条；中间产物不进 `catalog.yaml`。

### 4.4 注册表索引 `agent/registry.db`

**不用 MySQL**：注册表内容以 git 内 YAML 文件为唯一事实源；SQLite（含 FTS5 全文检索）只是从 YAML 构建出来的派生索引；Agent 通过 MCP 查询工具访问，不直接碰存储。

| 层级 | 内容 | 说明 |
|---|---|---|
| 事实源（git 管理） | `rules/manifests/*.manifest.yaml` + `agent/ports.yaml` + `agent/catalog.yaml` | 人评审，可 diff |
| 派生索引（`.gitignore`） | `agent/registry.db`（SQLite） | 由 `agent/build_registry.py` 自动重建 |
| 访问层（MCP 工具） | `query_rules` / `search_rules` / `query_catalog` | Agent 唯一入口 |

`registry.db` 结构：

- 普通表：`rules` / `ports` / `deliverables`（结构化精确查询）
- FTS5 虚拟表：`rule_search`（summary / docstring / keywords 全文检索）

**语义搜索现阶段不引入向量数据库**：几百条规模的注册表，LLM 自身即可完成语义匹配。`query_catalog` 返回全部 deliverable 的紧凑摘要（id + name + description + keywords，约几百 token），由模型做「我要差异表达 → 哪个 deliverable」的判断。rule 级语义匹配同理。

真到需要向量检索时（库超数千条，或 token 成本敏感），升级路径是 `sqlite-vec`（同一文件内加向量列，无新服务）；平台化、多用户共享注册表时，再考虑 PostgreSQL + pgvector，或把 `registry.db` 同步进平台既有 MySQL 做浏览界面——但那只是同步目标，唯一事实源永远是 git 里的 YAML。

### 4.5 缺口日志 `agent/gap_log.yaml`

每次「端口断链 / 缺 rule / 缺 deliverable」事件必须记录进 `agent/gap_log.yaml`，字段包括：

- 时间
- 需求描述
- 缺的端口或 deliverable
- 出现次数

高频缺口是专家下一个该策展的 rule，避免凭感觉决定库的增长方向。

### 4.6 运行台账 `run ledger`

每次 dry-run、提交、报错、修复、人工决策必须全量记录为 JSONL。字段至少包括：

- 时间戳
- 任务 ID
- 操作类型（dry_run / submit / error / fix / human_decision）
- 操作对象（target、组合 Snakefile 路径、rule 名）
- 结果或报错摘要
- 用户确认标记

`run ledger` 是审计轨迹，也是未来发文章/报项目的素材。

---

## 五、一致性校验

### 5.1 校验脚本 `agent/validate_manifests.py`

必须实现的检查（任一失败即退出码非 0，输出具体到文件与字段）：

1. 每个 manifest 的 rule 名、input/output pattern 与对应 `.smk` 实际内容一致（解析 Snakefile 比对字符串）；
2. `config_requires` 覆盖 `.smk` 中全部 `config[...]` 读取；
3. manifest 用到的每个 port 类型已登记在 `ports.yaml`；
4. `catalog.yaml` 中每个 target 能被某个 rule 的 output pattern 匹配；
5. 每个端口的 producers/consumers 双向闭合（消费者声明的端口必有生产者，或标注为「外部输入」如原始数据）。

接入方式：作为 git pre-commit 钩子 + CI 步骤；Agent 每次改完流程也必须先跑它（写进 SOP）。

### 5.2 校验与索引构建的关系

`build_registry.py` 与 `validate_manifests.py` 共用 YAML 解析。构建 `registry.db` 前必须先跑校验，校验不过不建库——索引永远不会比事实源「更正确」。

`registry.db` 加入 `.gitignore`；README 注明「删掉重建：`python agent/build_registry.py`」。

---

## 六、Agent 编排规范

### 6.1 决策流程

```
收到分析需求
  │
  ├─ ① 查 agent/catalog.yaml —— 有现成 deliverable？
  │      └─ 有 → 取其 targets → 跳到 ④
  │
  ├─ ② 查 agent/ports.yaml + rules/manifests/ —— 端口能串成完整链？
  │      └─ 能 → 生成组合 Snakefile（见 6.2），定义新 target → 跳到 ④
  │
  ├─ ③ 端口断链
  │      ├─ 缺中间 rule → 写新 rule 草案（遵守 manifest_spec 与目录命名约定）
  │      │              → 交人工确认 → 入库（.smk + manifest 同步新增）→ 回 ②
  │      └─ 缺数据源 → 报告缺什么外部输入，停止，不猜测
  │
  ├─ ④ 生成/修正 sample sheet 与 config
  │      └─ 用 validate_manifests.py + config_requires 合并校验，缺 key 先补齐
  │
  ├─ ⑤ snakemake -n <targets>（dry-run）
  │      └─ 报错 → 按 6.3 对策表修复 → 重跑本步（同一报错连续失败 2 次则停止并报告）
  │
  ├─ ⑥ snakemake --detailed-summary <targets> → 交用户确认规模
  │
  └─ ⑦ 正式运行；失败 → 读对应 rule 的 log → 修复 → --rerun-incomplete 续跑
```

### 6.2 组合 Snakefile 约定

- **位置**：`composed/<日期>_<需求简述>/Snakefile`，每次组合一个新目录，永不覆盖旧目录（可回溯）。
- **骨架固定**：

```python
include: "../../rules/common.smk"          # 隐性 helper 唯一来源，必须第一行
configfile: "../../config/config.yaml"     # 或本目录内生成的副本

module qc:
    snakefile: "../../rules/03.short_read_qc.smk"
    config: config

use rule short_read_qc_r1 from qc as qc_short_read_qc_r1
use rule short_read_multiqc_r1 from qc as qc_short_read_multiqc_r1
# …按需 use rule，命名空间前缀避免 AmbiguousRuleException

rule all:
    input: "<本组合的最终 target>"
```

- **禁止**复制粘贴库中 rule 本体进组合文件；一律 `use rule` 引用。

### 6.3 常见报错对策

| 报错 | 含义 | 对策（按优先级） |
|---|---|---|
| `MissingInputException` | 某输入没有 rule 能产出 | 查 `ports.yaml` 找该端口生产者 → 检查是否漏 `use rule` → 检查样本表路径 |
| `AmbiguousRuleException` | 两个 rule 产出同一 pattern | 给组合文件加 `ruleorder`，或改用带前缀的 `use rule ... as ns_*` |
| `WildcardError` | 通配符值不在样本表 | 修样本表，不改 rule |
| `KeyError`（config） | config 缺 key | 对照 manifest 的 `config_requires` 补 config |
| `CyclicGraphException` | 依赖成环 | 检查组合选择，去掉造成环的 rule |
| 运行期报错（log 中非零退出） | rule 内部命令失败 | 读该 rule 的 log 定位 → 修参数/环境 → `--rerun-incomplete` 续跑 |

### 6.4 禁区铁律

1. **修改优先级**：样本表 > config > 组合 Snakefile > 新增 rule > 已有 rule。已有 rule 的任何改动必须人工确认。
2. dry-run 未通过前，禁止提交正式任务到集群。
3. `dynamic: true` 的 rule，禁止凭读代码断言其 I/O，只许 dry-run 实测。
4. 正式运行失败后的续跑一律 `--rerun-incomplete`，禁止全量重跑。
5. 每次改动后先跑 `python agent/validate_manifests.py`，通过后再继续。
6. 同一报错连续修复 2 次仍失败，停止尝试，把报错、已试方案、log 摘要报告给用户。

### 6.5 渐进式披露与检索层

#### 维护分工

| 文件 | 维护粒度 | 何时要碰它 | 量级 |
|---|---|---|---|
| `rules/manifests/*.manifest.yaml` | 按 rule | 新增/修改 rule 时（与代码同提交，pre-commit 强制校验） | 每模块一份，几十~几百行 |
| `agent/ports.yaml` | 按端口类型 | 仅当引入新端口类型时；producers/consumers 由脚本自动刷新 | 全库几十条 |
| `agent/catalog.yaml` | 按终端 deliverable | 新增最终产物时 | 全库十几~几十条 |

日常维护成本主要在 manifest，它就放在模块旁边、随代码一起评审。`ports.yaml` 和 `catalog.yaml` 是小而稀疏的索引层，rule 的 I/O 细节只写在 manifest 一处，另两个文件的内容由 `build_registry.py` 自动聚合派生。

#### 三层加载

```
L0（常驻上下文，~500 token）
  system prompt：SOP + "有 catalog/ports/manifests 可查"的一句话索引

L1（工具查询返回紧凑摘要，每条 30~60 token）
  query_catalog(keyword) → [{id, name, 一句话描述}, ...]
  query_ports()          → [{port, 一句话描述, scope}, ...]
  query_rules(produces_port=X) → [{rule, module, summary}, ...]
  ※ 每条记录附带 source_ref："rules/manifests/03.short_read_qc.manifest.yaml#rules[name=short_read_qc_r1]"

L2（按需取详情，仅组链真正用到时）
  get_rule(rule_name)    → 该 rule 的完整 manifest 条目
                           （patterns / config_requires / dynamic 标记等）
  read_manifest(module)  → 整个模块 manifest（极少用）
```

token 预算估算（200 rule / 30 deliverable / 40 端口的中型库）：

| 加载方式 | token 成本 |
|---|---|
| 全量塞上下文（反模式） | 数万~十万+ |
| L1 摘要检索（catalog + 命中端口 + 候选 rule） | ~1,000~2,000 |
| L2 按需取 3~5 个 rule 详情 | ~500~1,000 |
| **实际每次任务总开销** | **约 2,000~3,000，且与库规模基本无关** |

#### 实现要点

1. `registry.db` 同时存紧凑摘要字段与 `source_ref` 指针（文件路径 + YAML 定位），摘要与详情同源构建，不会互相矛盾；
2. MCP 工具默认只返回摘要，详情必须显式调用 `get_*` 工具——把「按需加载」做在工具层而不是指望 Agent 自觉；
3. **唯一需要认真写的地方是 description/keywords**：检索命中率完全取决于摘要质量（中英文常用说法都收进 keywords）；详情写得再好，摘要不行就永远检索不到。

---

## 七、Rule 库增长与治理

### 7.1 增长机制

```
新分析需求 → Agent 组链 → 端口断链（记录进 gap log）
    → Agent 起草新 rule + manifest → 生成「评审包」→ 生信专家评审
    → 通过：入库（.smk + manifest + 测试）→ 库覆盖增长 → 下次同类需求直接命中
    → 驳回：专家修正意见回填 → 修正后复审；修正记录保存为 Agent 的 few-shot 范例
```

双增长效应：库在长大的同时，**专家修正记录持续变成 Agent 起草新 rule 的参考范例**，草案质量随轮次上升，专家评审工作量随之下降——HITL 文献证实「跟踪专家评审后的 AI 输出可用于指导模型重训、提示词精炼或微调」。

### 7.2 入库门禁

nf-core 模式的核心不是「模块多」，而是每个模块带 CI 与评审。一条坏 rule 入库会污染之后所有组合（Agent 无条件信任库），因此入库必须过五关：

1. manifest 齐全且 `validate_manifests.py` 通过；
2. 附带最小测试数据 + 预期输出；
3. dry-run 通过；
4. 小规模真实运行通过且输出与预期一致；
5. 专家评审 checklist 签字（对应 WorkflowHub 生命周期的 Test & Review 阶段）。

入库后由 CI 定期复测（LifeMonitor 模式：自动化持续测试 + 通过/失败徽章），工具版本升级导致失效的 rule 自动降级为「待修复」，Agent 组链时禁用。

### 7.3 评审包标准

专家瓶颈是增长机制的最大风险，评审材料必须标准化为一个「评审包」：

- rule diff 与 manifest yaml；
- 一句话用途 + 上下游端口说明；
- dry-run 输出摘要（DAG 片段）；
- 最小测试运行结果与预期输出 diff；
- Agent 起草时的参考来源（改自哪个已有 rule / 官方文档链接）。

### 7.4 缺口日志

见 4.5。

### 7.5 预期管理

- 常用分析步骤（QC、trim、比对、定量、差异表达）收敛很快，「覆盖大部分分析流程」对这 80% 成立；
- 长尾（新工具、非常规设计）永远存在——库不会「转完」，它是活资产，需要持续维护与 CI 复测，不是建一次就一劳永逸；
- 失败案例与「专家修正 vs Agent 草案」的分歧案例要回收入回归测试集（golden set），作为 Agent 能力是否进步的守门员。

---

## 八、实现形态

### 8.1 三级演进

最终体验确实是「一个 Agent 在干活」，但实现上「Agent」不是自造的程序，而是**通用编码 Agent + 本仓库资产**组合出来的效果。按三级演进，每层复用上层资产。

#### 第 1 级：仓库即 Agent（现在，阶段 0~3 完成即达成）

- 形态：`AGENTS.md`（SOP）+ `manifests/ports/catalog`（知识）+ `validate/dry-run` 脚本（工具）。
- 依据：`AGENTS.md` 已是跨工具开放标准（60,000+ 仓库采用，Codex/Cursor/Copilot/Gemini CLI/Aider 等 20+ 工具原生读取），研究证实它把 Agent 引导从一次性提示词变成「版本可控、可审查、可协作维护的配置资产」。任何通用编码 Agent 进入仓库即「变成」领域 Agent，零新增开发。
- 注意：业界抽样显示 68% 的 `AGENTS.md` 缺安全/审批条款——本方案的「禁区六条」恰好补这个洞，必须保留在文档顶部。

#### 第 2 级：薄 MCP server（禁区需要硬化时）

- 触发条件：提示词级软约束不够，需要机制级硬约束。
- 形态：把少量高价值操作封装为 MCP 工具（6~8 个为宜）：
  `query_catalog` / `query_ports` / `validate_manifests` / `dry_run` / `read_log` / `write_gap_log` / `submit_cluster`。
- 关键收益：**硬约束**——`submit_cluster` 在无通过的 dry-run 记录时直接拒绝，把「dry-run 未过禁止提交」从「请求 Agent 遵守」变成「工具层不可能违反」。MCP 的进程隔离与权限作用域是其相对 skill 的核心价值。
- 成本控制：MCP 工具 schema 全量加载吃 token（50 个工具约 8,000 token，同等 skill 约 400）——工具保持少而精，不要把每个 shell 命令都包一层。

#### 第 3 级：自建 Agent 应用（无人值守 / 平台化时）

- 触发条件（任一）：定时/无人值守自动化；接入自有平台对外提供服务；需要独立的权限、记忆、审计表面。
- 判断标准：流程不一致 → 写 skill；缺一个有自己的访问权限、记忆与运行表面的 worker → 才造 custom agent。成熟系统两者兼有：custom agent 管运行时与权限，skill 管可复用流程。
- 即便到这一级，`AGENTS.md` / manifests / SOP / MCP 工具全部复用，无返工。

**心智模型**：skill / `AGENTS.md` 是菜谱（知识层：告诉 Agent 怎么做），MCP 是厨房（连接层：让 Agent 真能做）。本方案第 1 级先发菜谱，第 2 级再配厨房，第 3 级才开餐厅。

### 8.2 自建 Agent 架构

若用户明确要求做成独立的 Agent 程序，则遵循以下架构：

```
用户（自然语言需求）
   │
   ▼
┌──────────────────────────┐
│ RNAFlow Agent（CLI 优先） │
│  ├ 循环底座：Agent SDK    │ ← 不手写 LLM 循环
│  ├ 大脑：SOP 蒸馏 system  │
│  │  prompt + 按需检索     │
│  └ 手：本地 MCP server    │
└──────────┬───────────────┘
           │ 工具调用
   ┌───────┴────────────────────────────┐
   │ query_catalog / query_ports        │  ← 查产物目录与端口注册表
   │ validate_manifests                 │  ← 一致性校验
   │ dry_run / summary                  │  ← snakemake -n 封装（带记录）
   │ read_log                           │  ← 读 rule 日志
   │ write_gap_log                      │  ← 缺口日志
   │ submit_cluster（硬约束）            │  ← 无 dry-run 通过记录则拒绝
   └───────┬────────────────────────────┘
           ▼
   RNAFlow 仓库（.smk 库 + manifests + config/样本表）
           │
           ▼
   run ledger（运行台账：每次 dry-run/提交/修复全记录）
```

**原则**：资产是大脑，Agent 是外壳——manifest/catalog/ports/校验器/SOP 仍是核心决策依据，Agent 程序只是把「通用 Agent 读文件干活」变成一个专门的、可分发的产品。阶段 0~2 的资产建设仍是关键路径，Agent 外壳可在阶段 1 完成后并行开工。

### 8.3 SDK 选型

经验法则：决策点超过 3~4 个或需要持久状态就用框架，不要手写 tool-calling 循环（本方案的 SOP 决策树有 7 步，远超阈值）。

| 候选 | 适配度 | 理由 |
|---|---|---|
| **Claude Agent SDK（首选）** | ★★★★★ | 把 Claude Code 的生产验证循环当库用：内建文件/shell 工具、hooks、权限系统、会话、AskUserQuestion 人工闸门、in-process MCP 工具——「给一个 agent 一台电脑并约束它能做什么」正是本场景。代价：锁定 Claude 模型。 |
| LangGraph（备选） | ★★★★ | 需要模型自由、显式状态机、崩溃可恢复的持久检查点时选它；代价：文件/shell/MCP 都要自己接，模板代码量大（50+ 行起步）。 |
| OpenAI Agents SDK | ★★★ | 轻量 handoff 编排，适合「多 Agent 对话路由」型产品，与本场景（单 Agent 深度操作环境）匹配度一般。 |

选型结论：**Claude Agent SDK + 本地 MCP server**。版本钉死（0.x 线几乎日更），若未来必须换模型底座，资产（大脑与工具）零改动，只换循环层。

### 8.4 界面与人工闸门

- **界面决策：CLI 优先**：生信主战场是服务器/HPC（SSH 会话），CLI 天然契合，也便于接 cron 与平台后端；Web UI / 平台集成是外壳的第二次迭代，不影响内核。
- **交互模式**：`rnaflow-agent "对这批新样本做质控和比对"` → Agent 按 SOP 走完决策树 → 关键节点（提交集群前、新 rule 入库前）停下来问人。
- **两个不可省的人工闸门**：
  1. `submit_cluster` 前：必须存在本任务的 dry-run 通过记录（工具层硬约束）+ 用户确认计算规模（summary 输出）。
  2. 新 rule 入库前：生成评审包，专家评审签字（见 7.3）。
- **一个差异化资产**：`run ledger` 记录每次 dry-run、提交、报错、修复、人工决策——审计轨迹是差异化卖点，也是未来发文章/报项目的素材。

### 8.5 诚实预期

demo → production 的可靠性鸿沟（95% → 99%）在所有框架上都需要 5~10 倍工程量；本方案把大头（契约、校验、SOP、门禁）前置为资产建设，是把这 5~10x 花在刀刃上的方式，而不是绕开它。

---

## 九、阶段化实施

以下阶段把规范转化为可实施、可验收的任务。每阶段完成并验收后，再进入下一阶段。

### 9.1 阶段 0：流程现状审计（只查不改）

**目标**：产出一份证据化的现状报告，回答：库里有多少模块/rule、I/O 路径约定是否一致、config key 全集、隐性 helper 依赖、动态 rule 分布、当前入口 Snakefile 如何组织。它是阶段 1~3 的事实基础，也是拿回报告后逐项核对的 checklist 基准。

**交付物**：

1. 「模块 × rule」清单表（含 docstring 一句话摘要、文件：行号）；
2. 跨模块衔接文件表（pattern → 生产者 → 消费者）；
3. config key 全集表（key → 使用方 rule 列表）；
4. 动态 rule / 隐性依赖清单（按「组合时高风险/中风险/低风险」分级）；
5. 样本配对信息现状描述（换配对的操作步骤现状）。

**验收标准**：

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 抽查报告中 3 条「生产者→消费者」衔接记录 | 文件路径与行号能对上实际代码 | 修正并全量复核该表 |
| 抽查 config key 表中的 2 个 key | 列出的使用方与实际 grep 结果一致 | 同上 |
| 检查动态 rule 清单 | 与 `grep -n "checkpoint\|lambda\|unpack" rules/` 结果一致 | 补全漏项 |
| 通读「换配对操作步骤现状」 | 与实际操作经验一致 | 口头纠正后修订该节 |

### 9.2 阶段 1：Manifest 规范定义 + 单模块试点

**目标**：定稿 rule manifest 的 YAML schema（这是全方案的「宪法」，先小范围验证再推广），并用现有模块 `rules/03.short_read_qc.smk` 做第一个试点。

**交付物**：

1. `rules/manifests/03.short_read_qc.manifest.yaml`；
2. `docs/manifest_spec.md`（中文字段说明 + 一个最小示例 + 常见错误示例 3 条）。

**验收标准**：

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 逐条 diff manifest pattern 与 `.smk` 字符串 | 完全一致 | 退回修正，要求附 diff 证据 |
| `grep -n "config\[" rules/03.short_read_qc.smk` 对照 `config_requires` | 无遗漏 | 退回补全 |
| 拿着 spec 文档让一个不懂流程的人（或另一个 Agent）复述写法 | 能复述且不出错 | 补充 spec 的示例与错误案例 |

### 9.3 阶段 2：全库 Manifest + 端口注册表 + 产物目录 + 漂移校验

**目标**：把试点验证过的 schema 推广到全部模块，并建成 Agent 组链所需的另外两件套：**端口注册表（ports.yaml）** 与 **需求→target 产物目录（catalog.yaml）**，外加一个防止 manifest 与代码漂移的一致性校验脚本。

**交付物**：

1. `rules/manifests/` 下其余每个 `.smk` 模块的 manifest；
2. `agent/ports.yaml`（producers/consumers 自动从 manifest 提取，description 人工维护）；
3. `agent/catalog.yaml`（从各模块 `kind: report` 的 output 中挑选用户会直接开口要的最终交付物）；
4. `agent/validate_manifests.py`（包含 5.1 中的 5 项检查；`--fix` 模式下自动刷新 `ports.yaml` 的 producers/consumers）；
5. git pre-commit 钩子接入 `validate_manifests.py`。

**验收标准**：

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 运行 `python agent/validate_manifests.py` | 退出码 0，全库通过 | 按报错逐条修复后重跑 |
| 故意把某 manifest 的 pattern 改错一个字符再跑校验 | 脚本报错并指出文件与字段 | 修脚本 |
| 任选 catalog 中 3 个 target 跑 `snakemake -n` | 均能解析出合理 DAG | 查 target 或样本表 |
| 通读「待人工确认清单」 | 每条都能当场拍板 | 拍板后回填 |

### 9.4 阶段 3：Agent 组合 SOP 与组合运行环境

**目标**：产出一份给 AI Agent 读的操作手册（`AGENTS.md`，等价于给 Agent 的 skill 文件），让它面对新需求时按固定决策树行动；同时把「组合 Snakefile」的运行环境约定固化下来。

**交付物**：

1. 仓库根目录 `AGENTS.md`，内容包含：项目一句话简介、目录结构图、决策流程（6.1）、组合 Snakefile 约定与骨架（6.2）、报错对策表（6.3）、禁区六条（6.4，置于文档顶部「铁律」一节）、感知器官命令速查（3.6）；
2. `composed/` 目录与 `composed/README.md`（说明该目录用途与命名规范）；
3. 示例组合 `composed/example_qc_only/Snakefile`（只跑 `03.short_read_qc` 的 multiqc target），并留存 `dry_run.txt`。

**验收标准**：

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 通读 `AGENTS.md`，逐条点击其中的文件路径 | 全部真实存在 | 修正文档 |
| 查看 `composed/example_qc_only/dry_run.txt` | 含合理的 Job 统计与 DAG 计划 | 重跑 dry-run 排查 |
| 把 `AGENTS.md` 喂给一个没参与建设的 Agent，口述一个新需求 | 它能按决策树走到 dry-run 步 | 记录卡在哪步，补 `AGENTS.md` |

### 9.5 阶段 4：端到端验收（三个场景）

本阶段是「人考 Agent」：你扮演用户提需求，观察 Agent 是否按 SOP 完成。每个场景独立验收，全过才算整套能力实施完成。

#### 场景 A：换配对信息（低难度，验证 config 层）

- **操作**：准备一份新样本表（新样本名 + 新 R1/R2 路径），对 Agent 说「用这批新样本重新出原始数据质控报告」。
- **期望现象**：Agent 只改样本表/config → dry-run 通过 → summary 交你确认 → 产出 MultiQC 报告；全程不改任何 `.smk`。
- **不通过的处理**：若 Agent 动了 rule，对照禁区铁律第 1 条退回；若 dry-run 报样本相关错，检查样本表格式并把它加入 `AGENTS.md` 的样本表规范一节。

#### 场景 B：新流程组合（中难度，验证端口层）

- **操作**：提一个库里 rule 都具备、但从未这样连过的需求（例如「只对这些样本做质控 + 比对，不做定量」，或「把 trim 后的 reads 也过一遍 FastQC 对比」）。
- **期望现象**：Agent 查 catalog 无果 → 查 ports/manifests 组链 → 生成 `composed/<日期>_<需求>/Snakefile`（use rule 引用，无复制粘贴）→ dry-run 通过 → 产出目标文件。
- **不通过的处理**：端口匹配选错说明 `ports.yaml` 描述不够，补 description；组合文件违反骨架约定，把反例加进 `AGENTS.md`。

#### 场景 C：注入故障自修复（高难度，验证回路层）

- **操作**：故意制造一个故障（如删掉一个中间文件、在 config 里删掉一个 key、给样本表填一个不存在的路径），让 Agent 继续推进流程。
- **期望现象**：Agent 通过 dry-run 报错或 log 定位到根因 → 按对策表修复（优先级正确）→ `--rerun-incomplete` 续跑成功；同一报错不反复撞墙超过 2 次。
- **不通过的处理**：把该故障现象与正确对策增补进 6.3 对策表；若 Agent 试图全量重跑，强化禁区第 4 条措辞。

### 9.6 阶段 5：Agent 外壳脚手架（可选，与阶段 1 后并行）

若决定自建独立 Agent 程序，在阶段 1 完成后即可并行开工。

**交付物**：

1. Claude Agent SDK 脚手架 + 本地 MCP server；
2. 7 个 MCP 工具：`query_catalog` / `query_ports` / `validate_manifests` / `dry_run` / `read_log` / `write_gap_log` / `submit_cluster`；
3. CLI 命令 `rnaflow-agent`；
4. `run ledger` JSONL 记录。

**硬约束**：

1. `submit_cluster`：调用时检查 `run ledger` 中本任务是否存在通过的 dry_run 记录，不存在则拒绝执行并返回原因。
2. `dry_run` 工具内部封装 `snakemake -n <targets>`，返回结构化结果（成功/失败 + 报错文本 + job 统计），并把结果写入 `run ledger`。
3. 所有工具只读仓库，唯一的写操作是：`composed/` 下新建组合文件、config/样本表副本、`run ledger`、gap log——其余路径只读。

**验收标准**：

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 跑最小端到端命令 | 查目录→dry-run→等确认，全流程无人工提示词补丁 | 看 ledger 定位卡在哪一步，补工具或 system prompt |
| 诱导直接提交集群 | 工具层拒绝并说明缺 dry-run 记录 | 硬约束没落实，退回重做 |
| 断网/拔 API key 后重启 | 已在跑的流程状态可从 ledger 恢复上下文 | 检查 ledger 记录粒度 |

---

## 十、风险与回退

| 风险 | 影响 | 应对 |
|---|---|---|
| manifest 与代码漂移（改了 `.smk` 忘改 manifest） | Agent 拿错误契约组链，越跑越偏 | `validate_manifests.py` 进 pre-commit + CI；漂移即构建失败 |
| 全库 manifest 编写工作量大 | 阶段 2 延期 | 按模块使用频率分批：高频模块先建，低频模块标「未建档」，Agent 对未建档模块禁用自动组链 |
| 动态 rule（checkpoint 等）行为不可静态预测 | 组链错误 | manifest 打标 + 只许 dry-run 实测（禁区第 3 条） |
| Agent 生成的组合 Snakefile 质量不稳 | dry-run 撞错频繁 | 骨架模板固定 + 报错对策表 + 同一报错 2 次熔断 |
| 算力误消耗（Agent 直接提交大任务） | 集群资源浪费 | 禁区第 2 条：dry-run 未过禁止提交；summary 必须人工确认 |
| 端口类型设计不当（太粗/太细） | 组链匹配率低或误配 | 阶段 1 试点时定 3~5 个端口验证手感，再推广 |

**回退原则**：任何阶段出问题，已建成的 manifest/catalog/校验脚本仍是纯文档与工具，不影响现有流程正常运行——本方案对 `.smk` 库是**增量叠加，不是改造**。

---

## 十一、泛化路线

本方案的本质范式 = **声明式 workflow + 结构化接口契约 + Agent 编排 + 可执行验证回路**，可平移到：

| 领域 | 对应物 | 验证回路 |
|---|---|---|
| Nextflow | process 的 input/output channel 声明（天然类型化） | `nextflow run -preview` / stub-run |
| CWL/WDL | 本身即强类型接口定义（契约最完备） | `cwltool --validate` / miniwdl check |
| Makefile | target + 依赖文件 | `make -n`（dry-run） |
| CI/CD（GitHub Actions 等） | job 的 needs/outputs | act 本地演练 / workflow lint |
| Airflow | task + DAG 依赖 | `airflow dags test` |

平移时不变的三件套：接口契约 manifest、产物目录 catalog、验证回路 SOP。Snakemake 是最佳起点：backward-chaining 与「从结果反推流程」天然同构，Python 生态对 Agent 最友好。

---

## 十二、附录

### 12.1 Agent 感知器官命令速查

```bash
snakemake -n <targets>                      # dry-run：验证流程组合，不执行
snakemake --detailed-summary <targets>      # 每个文件的状态/计划/输入输出
snakemake --dag <targets> | dot -Tsvg > dag.svg   # 实际 DAG 图
snakemake --rulegraph | dot -Tsvg > rg.svg  # rule 级依赖图（不展开样本）
snakemake --lint                            # 流程质量检查
snakemake --list-target-rules               # 列出全部目标 rule
snakemake --rerun-incomplete                # 失败后断点续跑
python agent/validate_manifests.py          # manifest/端口/catalog 一致性校验
```

### 12.2 文献记录的失败模式 → 本方案的对症机制

| 文献记录的失败模式 | 本方案的对症机制 | 证据 |
|---|---|---|
| 参数错误（非法语法/不支持的参数）、文件路径错误（I/O 路径用错） | manifest 端口契约 + validate_manifests.py + dry-run 回路 | 单命令运行期错误被分为参数错误、路径错误、其他三类（metaviral workflow 研究，PMC12782108） |
| 从零生成的 workflow 无一能直接执行（结构 93% 准确但跑不通） | 把「开放式生成」降维为「检索选择已验证 rule + 薄生成层 + 验证回路」 | Prompt-to-Pipeline（arXiv 2507.20122）按 GTN/nf-core 基线评估正确性/完整性/可执行性 |
| 生成结果需要迭代修复 | dry-run → 报错回灌 → 修复回路；Snakemake 同款机制实测「1~2 轮迭代可修复大多数错误」，边缘案例用 step-back prompting | Snakemaker（arXiv 2505.02841） |
| 单 Agent 修复能力有限 | SOP 决策树 + 报错对策表 + 熔断（同一报错 2 次即止） | 系统化综述（26 项研究，2019–2026）：多智能体系统（BioMaster、MARWA）在 18 种组学、102 个工具上错误恢复一致优于单智能体 |

### 12.3 三个残余缺口（本方案不声称解决的）

1. **语义错误 / 静默灰错**：dry-run 只验证「接线正确」，不验证「科学上正确」。多智能体失败中 75.17% 是「通过编译与表面检查但违背预期逻辑」的静默灰错（MAST，NeurIPS 2025）；显式验证阶段可带来 +15.6% 成功率提升。→ 对应 v1.1 增补方向：在 dry-run 之后加「结果校验层」（输出检查点、与参考结果比对）。
2. **运行期错误**：dry-run 通过不代表实际执行通过（命令内部 typo、工具版本、资源不足）；本方案由 log 读取回路覆盖，但迭代修复成功率有上限（实测阶梯约 60% → 85% → 95%），因此禁区保留「2 次熔断 + 人工确认」设计。
3. **迭代提示的天花板**：部分失败模式靠反复提示无法自愈（模型会坚持错误解释）；新 rule 生成仍是生成问题——因此 SOP 规定新 rule 草案必须人工确认后才入库。

### 12.4 结论

本方案把开放式生成问题降维成「检索 + 选择 + 薄生成 + 可执行验证」问题，与 2025–2026 文献收敛的方向一致，可消除绝大部分接线类与配置类报错；但「完全解决」不成立——语义正确性需要额外的结果校验层，这是 v1.1 的明确增补点。另注意：PRISMA 系统化综述指出目前尚无研究做出「监控-检测-修复-审计」端到端自愈流程，本方案实施本身即构成差异化贡献。

---

## 文档状态

- v1.0（2026-09-03）：规范定稿。将原计划的阶段性提示词块改为规范条文与实施验收；把原附录第 11~16 章（文献映射、增长机制、实现形态、注册表检索、维护分工）整合进正文章节；保留阶段化实施作为符合性验证路径。

[^1]: Masera M, Leone A, Köster J, et al. Snakemaker: Seamlessly transforming ad-hoc analyses into sustainable Snakemake workflows with generative AI[J]. arXiv preprint arXiv:2505.02841, 2025.
