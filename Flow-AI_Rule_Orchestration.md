# RNAFlow × AI Agent：Rule 级自动编排与自修复能力实现计划

> 版本：v1.0（2026-09-03）
> 适用范围：RNAFlow（Snakemake）流程；思想可推广到 Nextflow / WDL / Makefile / CI 等声明式流程。
> 使用方式：按阶段施工，每阶段把对应「提示词块」整段粘贴给编码 Agent（kimi code / Claude Code / CODEX 等），按验收 checklist 人工验收后再进入下一阶段。

---

## 一、背景与目标

### 1.1 现状痛点

- RNAFlow 的分析流程已经写好，但**配对信息（样本 ↔ 数据文件）规定死**，换配对要手工改配置。
- 更核心的诉求：**面对一个新的分析需求时，希望 AI Agent 能从已有 rule 库中挑选合适的 rule、拼接成一条新链路，自动 dry-run 验证、自动读报错修复**，而不是由人手工规划"这次要跑哪些 rule、按什么顺序"。

### 1.2 目标能力（最终态）

用户用自然语言说"我要对这批新样本做 X 分析"，Agent 自动完成：

1. **选**：从 rule 库中选出能完成该需求的 rule 组合（或发现缺口、提议新 rule）；
2. **配**：生成/修正 sample sheet 与 config（配对信息在这里改，rule 零改动）；
3. **验**：`snakemake -n` dry-run 验证整条链路，读报错定位修复，直到通过；
4. **跑**：给出计算量摘要，经用户确认后正式运行；运行失败读 log 自修复并断点续跑。

### 1.3 核心设计判断（为什么这样设计）

1. **"拼接"的算法本体 Snakemake 已经实现**：workflow 作用域内装入全部 rule 后，给定目标文件，Snakemake 自动按 input/output 模式做 backward-chaining 反推出所需 rule 子集。Agent 不需要自己拼 DAG，只需要**选对 target**。
2. **纯 LLM 生成的流程跑不通，闭环才可靠**：业界 benchmark 显示 LLM 直接生成的 workflow 结构准确率可达 93%，但没有一个能不经修正直接执行。因此本方案的核心不是"更强的描述"，而是 **结构化契约 + dry-run 验证回路**。
3. **描述要补，但补的是契约不是散文**：现有 rule docstring（如 `03.short_read_qc.smk`）对人友好，对 Agent 精确匹配不够用。需要机器可读的三件套：**rule manifest（接口契约）+ 端口注册表（谁产谁消）+ 产物目录（需求→target 映射）**。

### 1.4 总体架构

```
用户需求（自然语言）
      │
      ▼
┌─────────────────────────────────────────────┐
│  Agent 决策层（SOP 驱动，见阶段 3）           │
│   1. 查 catalog.yaml：需求→现成 target？      │
│   2. 查 ports.yaml + manifests：端口可成链？  │
│   3. 断链 → 提议新 rule（人工确认后入库）     │
└─────────────────────────────────────────────┘
      │  产出：target 列表 + sample sheet + config（+可选组合 Snakefile）
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

### 1.5 分阶段总览

| 阶段 | 内容 | 性质 | 依赖 |
|---|---|---|---|
| 0 | 流程现状审计（只查不改） | 调查 | 无 |
| 1 | Manifest 规范定义 + 单模块试点（03.short_read_qc） | 建设 | 阶段 0 报告 |
| 2 | 全库 manifest 生成 + 端口注册表 + 产物目录 + 漂移校验脚本 | 建设 | 阶段 1 定稿的 schema |
| 3 | Agent 组合 SOP（决策树 + 报错对策 + 禁区）+ 组合运行环境 | 建设 | 阶段 2 产物 |
| 4 | 端到端验收（三个场景）+ 文档固化 | 验收 | 阶段 3 |

---

## 二、关键技术前提（决定成败的四个约束）

这四个约束是整套方案的地基，阶段 0 审计和后续所有建设都围绕它们。

### 2.1 接口契约化：类型化端口（port）

现状：rule 之间靠**硬编码路径字符串**衔接（如 `01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip` 同时出现在上游 output 和下游 input 的 expand 里）。人能看懂，Agent 靠字符串匹配猜上下游关系既脆弱又不可靠。

做法：给每个 input/output 声明一个**语义端口类型**（port），例如：

| port 类型 | 语义 | 生产者（例） | 消费者（例） |
|---|---|---|---|
| `reads.r1.fastq` | R1 原始/处理后 reads | 数据链接 / trim 模块 | short_read_qc_r1、比对模块 |
| `qc.fastqc.zip` | FastQC 原始数据包 | short_read_qc_r1/r2 | short_read_multiqc_r1/r2 |
| `qc.md5_check.tsv` | MD5 校验闸门 | md5 校验 rule | 所有读原始数据的 rule（gate） |
| `align.bam` | 比对结果 BAM | 比对模块 | 定量 / 质控模块 |

Agent 组链 = 在端口注册表（`ports.yaml`）里做"生产者-消费者"匹配，而不是猜路径。路径约定（编号目录 + `{sample}` 通配符）保持不变，端口是叠加在现有路径之上的语义层，**不要求重排目录结构**。

### 2.2 解开全局 config 耦合

现状：rule 体直接读全局 config（如 `config['convert_md5']`、`config['parameter']['threads']['fastqc']`）。单条固定流程没问题；跨模块自由组合时，被选中 rule 依赖的 config key 缺一不可，缺了就是运行期 KeyError。

做法：每个 rule 在 manifest 中声明 `config_requires`（key 路径列表）；组合时由校验脚本自动合并所选 rule 的 `config_requires`，对照当前 config 文件逐项检查，缺 key 在 dry-run **之前**就报出来，并给出应该补什么。

### 2.3 隐性依赖显式化

现状：`rule_resource(config, 'low_resource', skip_queue_on_local=True, logger=logger)` 这类自定义 Python helper 是 rule 的隐性运行时依赖——把 rule 摘到别的上下文就 NameError。

做法：

1. 把所有自定义 helper 固定收拢进一个公共 `rules/common.smk`（或已存在则盘点登记）；
2. 每个模块 manifest 声明 `requires_helpers`；
3. Agent 生成的任何组合 Snakefile，第一行必须 `include: "rules/common.smk"`——写进 SOP 禁区条款。

### 2.4 动态 rule 打标，禁止静态推断

现状与风险：若流程中存在 checkpoint、input 函数（`input: lambda wc: ...`）、`unpack()` 等动态 I/O，Agent 读代码"脑补"上下游必然出错。

做法：manifest 中标注 `dynamic: true`；SOP 规定 Agent 对动态 rule **只许 dry-run 实测**（`snakemake -n` 会真实展开 DAG），禁止凭读代码断言其 I/O。

### 2.5 现成武器：Snakemake module 系统

Snakemake（≥6.0）原生支持跨流程复用 rule：

```python
module qc:
    snakefile: "rules/03.short_read_qc.smk"
    config: config

use rule * from qc as qc_*
```

Agent 组合新链路时**生成一个新的组合 Snakefile**（如 `composed/20260903_xxx需求/Snakefile`），用 module + `use rule` 从模块库挑选所需 rule，加命名空间前缀避免 `AmbiguousRuleException`，库中原始文件一行不改。这比复制粘贴 rule 干净、可回溯、可随时整体废弃。

### 2.6 Agent 可直接调用的"感知器官"（Snakemake 自带，零开发）

| 命令 | 用途 |
|---|---|
| `snakemake -n <targets>` | dry-run：验证链路定义正确性、不真正执行 |
| `snakemake --detailed-summary <targets>` | 每个文件的状态/计划/输入输出，给用户确认规模 |
| `snakemake --dag <targets> \| dot -Tsvg` | 导出实际 DAG 图 |
| `snakemake --rulegraph` | 导出 rule 级依赖图（不看样本展开） |
| `snakemake --lint` | 流程质量检查 |
| `snakemake --list-target-rules` | 列出全部目标 rule |
| `snakemake --rerun-incomplete` | 失败后断点续跑，不重跑已完成部分 |

---

## 三、阶段 0：流程现状审计（只查不改）

### 目标

产出一份证据化的现状报告，回答：库里有多少模块/rule、I/O 路径约定是否一致、config key 全集、隐性 helper 依赖、动态 rule 分布、当前入口 Snakefile 如何组织。它是阶段 1~3 的事实基础，也是拿回报告后逐项核对的 checklist 基准。

### 提示词块（整段粘贴给编码 Agent）

```text
你是一名资深生信流程工程师。任务：对当前仓库的 RNAFlow Snakemake 流程做一次
只读审计。本次只做代码级调研，禁止修改任何文件；先读代码再下结论，所有结论
必须标注对应文件路径与行号。

## 审计背景

我计划让 AI Agent 能够根据新分析需求，从本流程的 rule 库中挑选 rule 组成新
链路并自动 dry-run 验证。为此需要先摸清流程现状。已知一个示例模块是
rules/03.short_read_qc.smk（FastQC/MultiQC 质控），rule 使用自定义 helper
rule_resource()，样本信息来自 config 与 samples 字典。

## 审计任务

### A. 模块与 rule 清单
1. 列出全部 .smk 模块文件（路径、docstring 摘要、包含的 rule 名）。
2. 列出主入口 Snakefile（或等效入口）：它如何 include 各模块、ruleorder/
   wildcard_constraints/config 加载方式、samples 字典从哪个文件读入。

### B. I/O 约定一致性
1. 归纳目录编号约定（如 00.raw_data → 01.qc → …），各模块是否一致遵守。
2. 找出跨模块衔接的"关键文件"（一个模块的 output 是另一个模块的 input），
   列成表：文件 pattern → 生产者 rule（文件：行号）→ 消费者 rule（文件：行号）。
3. 标出所有破坏约定的特例（硬编码绝对路径、绕过约定的临时文件等）。

### C. config 与样本表
1. 列出 config 被读取的全部 key（含嵌套路径，如 parameter.threads.fastqc），
   标注每个 key 被哪些 rule 使用（文件：行号）。
2. 配对信息（样本→R1/R2 文件路径→分组）当前定义在哪个文件、什么格式，
   换配对需要改哪几处。

### D. 隐性依赖与动态 rule
1. 列出全部自定义 Python helper（如 rule_resource）的定义位置与被使用位置。
2. 找出全部动态 I/O：checkpoint、input 函数、unpack()、expand() 里依赖
   运行期信息的写法，逐一标注（文件：行号）。
3. 找出全部 conda/container 声明与环境 yaml 文件清单。

## 交付格式

1. 「模块 × rule」清单表（含 docstring 一句话摘要、文件：行号）。
2. 跨模块衔接文件表（pattern → 生产者 → 消费者）。
3. config key 全集表（key → 使用方 rule 列表）。
4. 动态 rule / 隐性依赖清单（按"组合时高风险/中风险/低风险"分级）。
5. 样本配对信息现状描述（换配对的操作步骤现状）。
6. 不做任何代码修改，只输出上述报告。
```

### 验收 checklist

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 抽查报告中 3 条"生产者→消费者"衔接记录 | 文件路径与行号能对上实际代码 | 让 Agent 修正并全量复核该表 |
| 抽查 config key 表中的 2 个 key | 列出的使用方与实际 grep 结果一致 | 同上 |
| 检查动态 rule 清单 | 与 `grep -n "checkpoint\|lambda\|unpack" rules/` 结果一致 | 让 Agent 补全漏项 |
| 通读"换配对操作步骤现状" | 与你实际经验一致 | 口头纠正，让 Agent 修订该节 |

> 拿回报告后：把它发回本对话，我会逐项核对并把确认结论追加进本文档（阶段 0 报告摘要 + 对后续阶段的修正）。

## 四、阶段 1：Manifest 规范定义 + 单模块试点

### 目标

定稿 rule manifest 的 YAML schema（这是全方案的"宪法"，先小范围验证再推广），并用现有模块 `rules/03.short_read_qc.smk` 做第一个试点。

### 4.1 Manifest Schema（定稿基准）

每个 .smk 模块配一个同名 manifest：`rules/manifests/<模块名>.manifest.yaml`。

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
  requires_helpers: ["rule_resource", "logger"]   # 隐性 Python 依赖（见 §2.3）

rules:
  - name: short_read_qc_r1
    summary: "对单个样本的 R1 reads 运行 FastQC"
    wildcards: ["sample"]
    dynamic: false                                 # 动态 I/O 打 true（见 §2.4）
    inputs:
      - name: link_r1_dir
        port: "reads.r1.fastq"                     # 类型化端口（见 §2.1）
        pattern: "00.raw_data/{md5dir}/{sample}/{sample}_R1.fq.gz"
        config_keys: ["convert_md5"]
      - name: md5_check
        port: "qc.md5_check.tsv"
        pattern: "01.qc/md5_check.tsv"
        role: gate                                 # 纯依赖闸门，不进 shell 命令
    outputs:
      - name: r1_html
        port: "qc.fastqc.html"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.html"
        kind: report                               # report=最终给人看 / intermediate=供下游
      - name: r1_zip
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip"
        kind: intermediate
    config_requires:                               # 见 §2.2
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
    per_sample: false                              # 聚合 rule：输入随样本数展开
    inputs:
      - name: fastqc_files_r1
        port: "qc.fastqc.zip"
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip"
        expand_over: samples                       # 对样本表全部样本展开
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

### 4.2 Schema 字段约定（写进规范，Agent 与人共同遵守）

| 字段 | 必填 | 含义 |
|---|---|---|
| `module.requires_helpers` | 是 | 该模块用到的自定义 Python helper 名列表 |
| `rules[].port` | 是 | 语义端口类型；新类型必须先登记进 ports.yaml（阶段 2） |
| `rules[].role: gate` | 否 | 该 input 只是依赖闸门、不出现在命令里 |
| `rules[].kind` | 是 | `report`（最终交付）/ `intermediate`（中间产物） |
| `rules[].dynamic` | 是 | 是否动态 I/O；true 时 Agent 禁止静态推断 |
| `rules[].expand_over: samples` | 否 | 该 input 对样本表全体展开（聚合 rule 标志） |
| `rules[].config_requires` | 是 | 该 rule 读取的全部 config key 路径 |
| `rules[].per_sample` | 是 | 是否按样本通配（决定组合时如何展开） |

### 提示词块（整段粘贴给编码 Agent）

```text
你是一名资深 Snakemake 工程师。任务：为本仓库的 rules/03.short_read_qc.smk
模块编写 rule manifest 试点文件，并固化 manifest 编写规范。

## 背景

我要让 AI Agent 能读懂流程中每个 rule 的能力与输入输出契约，以便按新分析
需求自动挑选 rule 组链 + dry-run 验证。manifest 是给 Agent 看的"工具卡"，
与人读的 docstring 互补，不替代 docstring。

## 要求

1. 先阅读 rules/03.short_read_qc.smk 全文与主入口 Snakefile 中 samples、
   config 的加载方式，再动笔。
2. 按我提供的 schema 定稿版（见下方）生成
   rules/manifests/03.short_read_qc.manifest.yaml，四个 rule 全覆盖，
   pattern 必须与 .smk 中的实际字符串逐一一致（含目录名、文件名、
   通配符位置），config_requires 必须覆盖 rule 体中真实读取的所有
   config key（用 grep 核对，不许凭印象）。
3. 把 schema 字段约定整理成 docs/manifest_spec.md（中文字段说明 +
   一个最小示例 + 常见错误示例 3 条），作为后续全库编写的规范文档。
4. 禁止修改任何 .smk 文件；本阶段只新增 manifest yaml 与 spec 文档。

[此处粘贴本文档 §4.1 的 YAML 示例与 §4.2 的字段约定表]

## 验收标准

- [ ] manifest 中每个 pattern 与 .smk 实际 output/input 字符串一致（逐个 diff）
- [ ] config_requires 与 grep config\[ 结果一致
- [ ] docs/manifest_spec.md 可被不熟悉本流程的工程师照着写出合格 manifest
```

### 验收 checklist

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 逐条 diff manifest pattern 与 .smk 字符串 | 完全一致 | 退回修正，要求附 diff 证据 |
| `grep -n "config\[" rules/03.short_read_qc.smk` 对照 config_requires | 无遗漏 | 退回补全 |
| 拿着 spec 文档让一个不懂流程的人（或另一个 Agent）复述写法 | 能复述且不出错 | 补充 spec 的示例与错误案例 |

## 五、阶段 2：全库 Manifest + 端口注册表 + 产物目录 + 漂移校验

### 目标

把试点验证过的 schema 推广到全部模块，并建成 Agent 组链所需的另外两件套：**端口注册表（ports.yaml）** 与 **需求→target 产物目录（catalog.yaml）**，外加一个防止 manifest 与代码漂移的一致性校验脚本。

### 5.1 端口注册表 `agent/ports.yaml`

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
  # …阶段 0 报告中的每个跨模块衔接文件都对应一个端口条目
```

> producers/consumers 列由校验脚本从各 manifest 自动生成更新，人工只维护 description。

### 5.2 产物目录 `agent/catalog.yaml`（需求→target 映射）

```yaml
# agent/catalog.yaml —— Agent 回答"我要 X 结果"时的查询入口
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
  # …每个"用户会开口要的最终结果"一条；中间产物不进 catalog
```

### 5.3 一致性校验脚本 `agent/validate_manifests.py`

必须实现的检查（任一失败即退出码非 0，输出具体到文件与字段）：

1. 每个 manifest 的 rule 名、input/output pattern 与对应 .smk 实际内容一致（解析 Snakefile 比对字符串）；
2. `config_requires` 覆盖 .smk 中全部 `config[...]` 读取；
3. manifest 用到的每个 port 类型已登记在 ports.yaml；
4. catalog.yaml 中每个 target 能被某个 rule 的 output pattern 匹配；
5. 每个端口的 producers/consumers 双向闭合（消费者声明的端口必有生产者，或标注为"外部输入"如原始数据）。

接入方式：作为 git pre-commit 钩子 + CI 步骤；Agent 每次改完流程也必须先跑它（写进阶段 3 的 SOP）。

### 提示词块（整段粘贴给编码 Agent）

```text
你是一名资深 Snakemake 工程师。任务：把 rule manifest 从试点推广到全库，
并建成端口注册表、产物目录与一致性校验脚本。前置产物：docs/manifest_spec.md
（schema 规范）与 rules/manifests/03.short_read_qc.manifest.yaml（试点样板），
先读它们再动手。

## 要求

1. 为 rules/ 下其余每个 .smk 模块生成 manifest（rules/manifests/<模块名>.manifest.yaml），
   严格遵循 manifest_spec.md；pattern 与 config_requires 必须逐个与代码核对，
   不确定处列成"待人工确认清单"单独交付，禁止猜测填写。
2. 汇总全部 manifest 生成 agent/ports.yaml：每种 port 的 producers/consumers
   自动从 manifest 提取；跨模块衔接端口对照阶段 0 审计报告逐一核对。
3. 编写 agent/catalog.yaml：从各模块 kind: report 的 output 中挑出"用户会
   直接开口要"的最终交付物，每条配 keywords（中英文常用说法都收）。
4. 实现 agent/validate_manifests.py，包含规范中的 5 项检查；失败信息必须
   定位到文件与字段；--fix 模式下自动刷新 ports.yaml 的 producers/consumers。
5. 把 validate_manifests.py 接入 git pre-commit；本阶段禁止修改任何 .smk
   的 rule 逻辑（发现 manifest 与代码不一致时，以代码为准修 manifest，
   并在交付说明中列出所有不一致点）。

## 验收标准

- [ ] python agent/validate_manifests.py 全库通过
- [ ] 人为改坏一个 manifest pattern，脚本能报出并定位到字段
- [ ] catalog.yaml 中每个 target 都能通过 snakemake -n <target> 的 DAG 解析
- [ ] 待人工确认清单一并交付
```

### 验收 checklist

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 运行 `python agent/validate_manifests.py` | 退出码 0，全库通过 | 按报错逐条修复后重跑 |
| 故意把某 manifest 的 pattern 改错一个字符再跑校验 | 脚本报错并指出文件与字段 | 修脚本 |
| 任选 catalog 中 3 个 target 跑 `snakemake -n` | 均能解析出合理 DAG | 查 target 或样本表 |
| 通读"待人工确认清单" | 每条你都能当场拍板 | 拍板后让 Agent 回填 |

## 六、阶段 3：Agent 组合 SOP 与组合运行环境

### 目标

产出一份给 AI Agent 读的操作手册（`AGENTS.md`，等价于给 Agent 的 skill 文件），让它面对新需求时按固定决策树行动；同时把"组合 Snakefile"的运行环境约定固化下来。

### 6.1 Agent 决策树（SOP 核心，写进 AGENTS.md）

```
收到分析需求
  │
  ├─ ① 查 agent/catalog.yaml ── 有现成 deliverable？
  │      └─ 有 → 取其 targets → 跳到 ④
  │
  ├─ ② 查 agent/ports.yaml + rules/manifests/ ── 端口能串成完整链？
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

- 位置：`composed/<日期>_<需求简述>/Snakefile`，每次组合一个新目录，永不覆盖旧目录（可回溯）。
- 骨架固定：

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

- 禁止复制粘贴库中 rule 本体进组合文件；一律 `use rule` 引用。

### 6.3 常见报错 → 对策表（Agent 修复时的对照依据）

| 报错 | 含义 | 对策（按优先级） |
|---|---|---|
| `MissingInputException` | 某输入没有 rule 能产出 | 查 ports.yaml 找该端口生产者 → 检查是否漏 `use rule` → 检查样本表路径 |
| `AmbiguousRuleException` | 两个 rule 产出同一 pattern | 给组合文件加 ruleorder，或改用带前缀的 `use rule ... as ns_*` |
| `WildcardError` | 通配符值不在样本表 | 修样本表，不改 rule |
| `KeyError`（config） | config 缺 key | 对照 manifest 的 config_requires 补 config |
| `CyclicGraphException` | 依赖成环 | 检查组合选择，去掉造成环的 rule |
| 运行期报错（log 中非零退出） | rule 内部命令失败 | 读该 rule 的 log 定位 → 修参数/环境 → `--rerun-incomplete` 续跑 |

### 6.4 禁区（写进 AGENTS.md 顶部）

1. 修改优先级铁律：**样本表 > config > 组合 Snakefile > 新增 rule > 已有 rule**。已有 rule 的任何改动必须人工确认。
2. dry-run 未通过前，禁止提交正式任务到集群。
3. `dynamic: true` 的 rule，禁止凭读代码断言其 I/O，只许 dry-run 实测。
4. 正式运行失败后的续跑一律 `--rerun-incomplete`，禁止全量重跑。
5. 每次改动后先跑 `python agent/validate_manifests.py`，绿了再继续。
6. 同一报错连续修复 2 次仍失败，停止尝试，把报错、已试方案、log 摘要报告给用户。

### 提示词块（整段粘贴给编码 Agent）

```text
你是一名资深 Snakemake 工程师。任务：把 Agent 操作手册与组合运行环境固化到
本仓库。前置产物：agent/ports.yaml、agent/catalog.yaml、
rules/manifests/、agent/validate_manifests.py、docs/manifest_spec.md，
先读它们再动手。

## 要求

1. 在仓库根目录创建 AGENTS.md，内容包含：项目一句话简介、目录结构图、
   我提供的决策树（6.1）、组合 Snakefile 约定与骨架（6.2）、报错对策表
   （6.3）、禁区六条（6.4，置于文档顶部"铁律"一节）、感知器官命令速查
   （snakemake -n / --detailed-summary / --dag / --lint / --rerun-incomplete）。
   全文用中文，命令保持原样不翻译。
2. 创建 composed/ 目录与 composed/README.md（说明该目录用途与命名规范），
   并用现有模块手工写一个示例组合 composed/example_qc_only/Snakefile
   （只跑 03.short_read_qc 的 multiqc target），实际执行
   snakemake -n 验证能解析出 DAG，把 dry-run 输出存入该目录 dry_run.txt。
3. 把 AGENTS.md 中每条禁区与决策树步骤，与仓库实际文件路径核对一遍
   （路径必须真实存在），不一致的改文档不改代码。

[此处粘贴本文档 §6.1~§6.4 全文]

## 验收标准

- [ ] AGENTS.md 中提到的每个文件路径真实存在
- [ ] composed/example_qc_only 下 snakemake -n 实际通过且 dry_run.txt 留存
- [ ] 禁区六条位于文档最前的"铁律"一节
```

### 验收 checklist

| 操作 | 期望现象 | 不通过的处理 |
|---|---|---|
| 通读 AGENTS.md，逐条点击其中的文件路径 | 全部真实存在 | 让 Agent 修正文档 |
| 查看 composed/example_qc_only/dry_run.txt | 含合理的 Job 统计与 DAG 计划 | 重跑 dry-run 排查 |
| 把 AGENTS.md 喂给一个没参与建设的 Agent，口述一个新需求 | 它能按决策树走到 dry-run 步 | 记录卡在哪步，补 AGENTS.md |

## 七、阶段 4：端到端验收（三个场景）

> 本阶段是"人考 Agent"：你扮演用户提需求，观察 Agent 是否按 SOP 完成。每个场景独立验收，全过才算整套能力落地。

### 场景 A：换配对信息（低难度，验证 config 层）

- **操作**：准备一份新样本表（新样本名 + 新 R1/R2 路径），对 Agent 说"用这批新样本重新出原始数据质控报告"。
- **期望现象**：Agent 只改样本表/config → dry-run 通过 → summary 交你确认 → 产出 MultiQC 报告；全程不改任何 .smk。
- **不通过的处理**：若 Agent 动了 rule，对照禁区铁律第 1 条退回；若 dry-run 报样本相关错，检查样本表格式并把它加入 AGENTS.md 的样本表规范一节。

### 场景 B：新链路组合（中难度，验证端口层）

- **操作**：提一个库里 rule 都具备、但从未这样连过的需求（例如"只对这些样本做质控 + 比对，不做定量"，或"把 trim 后的 reads 也过一遍 FastQC 对比"）。
- **期望现象**：Agent 查 catalog 无果 → 查 ports/manifests 组链 → 生成 composed/<日期>_<需求>/Snakefile（use rule 引用，无复制粘贴）→ dry-run 通过 → 产出目标文件。
- **不通过的处理**：端口匹配选错说明 ports.yaml 描述不够，补 description；组合文件违反骨架约定，把反例加进 AGENTS.md。

### 场景 C：注入故障自修复（高难度，验证回路层）

- **操作**：故意制造一个故障（如删掉一个中间文件、在 config 里删掉一个 key、给样本表填一个不存在的路径），让 Agent 继续推进流程。
- **期望现象**：Agent 通过 dry-run 报错或 log 定位到根因 → 按对策表修复（优先级正确）→ `--rerun-incomplete` 续跑成功；同一报错不反复撞墙超过 2 次。
- **不通过的处理**：把该故障现象与正确对策增补进 6.3 对策表；若 Agent 试图全量重跑，强化禁区第 4 条措辞。

---

## 八、风险与回退

| 风险 | 影响 | 应对 |
|---|---|---|
| manifest 与代码漂移（改了 .smk 忘改 manifest） | Agent 拿错误契约组链，越跑越偏 | validate_manifests.py 进 pre-commit + CI；漂移即构建失败 |
| 全库 manifest 编写工作量大 | 阶段 2 延期 | 按模块使用频率分批：高频模块先建，低频模块标"未建档"，Agent 对未建档模块禁用自动组链 |
| 动态 rule（checkpoint 等）行为不可静态预测 | 组链错误 | manifest 打标 + 只许 dry-run 实测（禁区第 3 条） |
| Agent 生成的组合 Snakefile 质量不稳 | dry-run 撞错频繁 | 骨架模板固定 + 报错对策表 + 同一报错 2 次熔断 |
| 算力误消耗（Agent 直接提交大任务） | 集群资源浪费 | 禁区第 2 条：dry-run 未过禁止提交；summary 必须人工确认 |
| 端口类型设计不当（太粗/太细） | 组链匹配率低或误配 | 阶段 1 试点时定 3~5 个端口验证手感，再推广 |

回退原则：任何阶段出问题，已建成的 manifest/catalog/校验脚本仍是纯文档与工具，不影响现有流程正常运行——本方案对 .smk 库是**增量叠加，不是改造**。

---

## 九、泛化路线（非生信领域）

本方案的本质范式 = **声明式 workflow ＋ 结构化接口契约 ＋ Agent 编排 ＋ 可执行验证回路**，可平移到：

| 领域 | 对应物 | 验证回路 |
|---|---|---|
| Nextflow | process 的 input/output channel 声明（天然类型化） | `nextflow run -preview` / stub-run |
| CWL/WDL | 本身即强类型接口定义（契约最完备） | `cwltool --validate` / miniwdl check |
| Makefile | target + 依赖文件 | `make -n`（dry-run） |
| CI/CD（GitHub Actions 等） | job 的 needs/outputs | act 本地演练 / workflow lint |
| Airflow | task + DAG 依赖 | `airflow dags test` |

平移时不变的三件套：接口契约 manifest、产物目录 catalog、验证回路 SOP。Snakemake 是最佳起点：backward-chaining 与"从结果反推流程"天然同构，Python 生态对 Agent 最友好。

---

## 十、附录：Agent 感知器官命令速查

```bash
snakemake -n <targets>                      # dry-run：验证链路，不执行
snakemake --detailed-summary <targets>      # 每个文件的状态/计划/输入输出
snakemake --dag <targets> | dot -Tsvg > dag.svg   # 实际 DAG 图
snakemake --rulegraph | dot -Tsvg > rg.svg  # rule 级依赖图（不展开样本）
snakemake --lint                            # 流程质量检查
snakemake --list-target-rules               # 列出全部目标 rule
snakemake --rerun-incomplete                # 失败后断点续跑
python agent/validate_manifests.py          # manifest/端口/catalog 一致性校验
```

---

## 文档状态

- v1.0（2026-09-03）：初版，覆盖想法评估修正后的完整方案：端口契约化、config 解耦、组合 SOP 与 dry-run 验证回路、三场景端到端验收。阶段 0 审计报告拿回后将在此追加核对结论。

---

## 十一、补充评估：与文献记录的 Agent 生成失败模式的对应关系（2026-09-03）

> 本节回答"这套方案是否解决了业界探索的 Agent 生成 workflow 报错问题"，作为方案有效性的证据链与边界声明。

### 11.1 文献记录的失败模式 → 本方案的对症机制

| 文献记录的失败模式 | 本方案的对症机制 | 证据 |
|---|---|---|
| 参数错误（非法语法/不支持的参数）、文件路径错误（I/O 路径用错） | manifest 端口契约 + validate_manifests.py + dry-run 回路 | 单命令运行期错误被分为参数错误、路径错误、其他三类（metaviral workflow 研究，PMC12782108） |
| 从零生成的 workflow 无一能直接执行（结构 93% 准确但跑不通） | 把"开放式生成"降维为"检索选择已验证 rule ＋ 薄生成层 ＋ 验证回路" | Prompt-to-Pipeline（arXiv 2507.20122）按 GTN/nf-core 基线评估正确性/完整性/可执行性 |
| 生成结果需要迭代修复 | dry-run → 报错回灌 → 修复回路；Snakemaker 同款机制实测"1~2 轮迭代可修复大多数错误"，边缘案例用 step-back prompting | Snakemaker（arXiv 2505.02841） |
| 单 Agent 修复能力有限 | SOP 决策树 ＋ 报错对策表 ＋ 熔断（同一报错 2 次即止） | 系统化综述（26 项研究，2019–2026）：多智能体系统（BioMaster、MARWA）在 18 种组学、102 个工具上错误恢复一致优于单智能体 |

### 11.2 三个残余缺口（本方案不声称解决的）

1. **语义错误 / 静默灰错**：dry-run 只验证"接线正确"，不验证"科学上正确"。多智能体失败中 75.17% 是"通过编译与表面检查但违背预期逻辑"的静默灰错（MAST，NeurIPS 2025）；显式验证阶段可带来 +15.6% 成功率提升。→ 对应 v1.1 增补方向：在 dry-run 之后加"结果校验层"（输出检查点、与参考结果比对）。
2. **运行期错误**：dry-run 通过不代表实际执行通过（命令内部 typo、工具版本、资源不足）；本方案由 log 读取回路覆盖，但迭代修复成功率有上限（实测阶梯约 60% → 85% → 95%），因此禁区保留"2 次熔断 ＋ 人工确认"设计。
3. **迭代提示的天花板**：部分失败模式靠反复提示无法自愈（模型会坚持错误解释）；新 rule 生成仍是生成问题——因此 SOP 规定新 rule 草案必须人工确认后才入库。

### 11.3 结论

本方案把开放式生成问题降维成"检索 ＋ 选择 ＋ 薄生成 ＋ 可执行验证"问题，与 2025–2026 文献收敛的方向一致，可消除绝大部分接线类与配置类报错；但"完全解决"不成立——语义正确性需要额外的结果校验层，这是 v1.1 的明确增补点。另注意：PRISMA 系统化综述指出目前尚无研究做出"监控-检测-修复-审计"端到端自愈流程，本方案落地本身即构成差异化贡献。

---

## 十二、规则库增长飞轮与专家监督机制（v1.1 增补方向，2026-09-03）

> 来源：用户提出"在生信专家监控下反复迭代，rule 库越来越大，最终覆盖大部分分析流程"。
> 评估结论：模式成立，且被业界双重验证——社区策展库（nf-core / Snakemake workflow catalog /
> WorkflowHub，后者已注册 764 个 workflow、覆盖 35 国 236 个组织）证明"策展库增长"可行；
> HITL（human-in-the-loop）文献证明"专家反馈环"可行。本节把该飞轮固化为机制设计。

### 12.1 飞轮闭环

```
新分析需求 → Agent 组链 → 端口断链（记录进 gap log）
    → Agent 起草新 rule + manifest → 生成「评审包」→ 生信专家评审
    → 通过：入库（.smk + manifest + 测试）→ 库覆盖增长 → 下次同类需求直接命中
    → 驳回：专家修正意见回填 → 修正后复审；修正记录沉淀为 Agent 的 few-shot 范例
```

双增长效应：库在长大的同时，**专家修正记录持续变成 Agent 起草新 rule 的参考范例**，
草案质量随轮次上升，专家评审工作量随之下降——HITL 文献证实"跟踪专家评审后的 AI 输出
可用于指导模型重训、提示词精炼或微调"。

### 12.2 入库门禁（admission control）——飞轮不自毁的前提

nf-core 模式的核心不是"模块多"，而是每个模块带 CI 与评审。一条坏 rule 入库会污染
之后所有组合（Agent 无条件信任库），因此入库必须过五关：

1. manifest 齐全且 validate_manifests.py 通过；
2. 附带最小测试数据 + 预期输出；
3. dry-run 通过；
4. 小规模真实运行通过且输出与预期一致；
5. 专家评审 checklist 签字（对应 WorkflowHub 生命周期的 Test & Review 阶段）。

入库后由 CI 定期复测（LifeMonitor 模式：自动化持续测试 + 通过/失败徽章），
工具版本升级导致失效的 rule 自动降级为"待修复"，Agent 组链时禁用。

### 12.3 评审包标准（让专家评审以分钟计，而不是小时）

专家瓶颈是飞轮的最大风险，评审材料必须标准化为一个「评审包」：

- rule diff 与 manifest yaml；
- 一句话用途 + 上下游端口说明；
- dry-run 输出摘要（DAG 片段）；
- 最小测试运行结果与预期输出 diff；
- Agent 起草时的参考来源（改自哪个已有 rule / 官方文档链接）。

### 12.4 缺口日志（gap log）——让库的增长数据驱动

每次"端口断链/缺 rule/缺 deliverable"事件记录进 `agent/gap_log.yaml`
（时间、需求描述、缺的端口、出现次数）。高频缺口 = 专家下一个该策展的 rule，
避免凭感觉决定库的增长方向。

### 12.5 预期管理

- 常用分析步骤（QC、trim、比对、定量、差异表达）收敛很快，"覆盖大部分分析流程"
  对这 80% 成立；
- 长尾（新工具、非常规设计）永远存在——飞轮不会"转完"，库是活资产，需要持续
  维护与 CI 复测，不是建一次就一劳永逸；
- 失败案例与"专家修正 vs Agent 草案"的分歧案例要回收入回归测试集（golden set），
  作为 Agent 能力是否进步的守门员。
