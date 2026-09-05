# Flow × AI Agent：Rule 级编排、验证与版本治理技术规范

> 版本：v1.1（2026-09-06）
> 状态：讨论结论已纳入；待现状审计、Schema 试点和端到端验收后冻结实现契约。
> 适用范围：采用统一规范构建的 Flow 系列 Snakemake 流程。
> 本文定义目标能力、接口和验收要求，不代表所述工具已经实现。文中的拟议路径、命令和 YAML 示例均需在实施阶段落地验证。

---

## 一、范围与目标

### 1.1 核心定位

**Agent 基于已有、经过严格测试和专家审核的已发布 rule，生成候选分析方案与 pipeline 实现；通过自动验证和专家审核后，发布 pipeline 并按授权策略运行。**

必须区分两种产出：

1. **复用已有 rule 的候选 pipeline**：审核对象是科学方案、组合关系、参数、依赖版本和验证证据；审核通过后发布 pipeline，不重复创建相同 rule。
2. **新增或修改的候选 rule**：仅在现有能力不足或存在缺陷时进入 Build 开发流程；连同 manifest、相关文件、环境和测试一并验证，经专家审核后作为新版本入库，再供 pipeline 引用。

已审核 rule 的可信范围受其输入条件、科学适用范围和版本约束限定。单条 rule 审核通过，不等于任意组合都科学正确。

Build 是辅助流程开发：Agent 承担检索、生成、测试、诊断和整理证据，专家负责科学方案与发布决策。Run 是分析执行与运行维护：在批准的方案、版本和权限范围内推进任务。

### 1.2 目标能力

- 自然语言需求转为可审核的分析方案和机器可验证的流程计划。
- 优先复用现有入口和已审核 rule；通过确定性生成器产出配置或组合文件。
- 分层验证契约、实际 DAG、最小真实运行和结果质量。
- 提供自主、监督、逐项确认三种运行模式，支持专家观察、暂停、批准和拒绝。
- 从首个试点开始实现 rule 级版本管理，完整锁定相关文件及传递依赖。
- 支持客户使用历史 pipeline、版本升级比较和隔离回退。
- 参考数据通过对象存储及版本化 YAML 描述，运行时校验身份与内容。
- 全程保留可查询的计划、审批、运行、修复和产物证据。

### 1.3 能力边界

本方案提高可追溯性、可重复执行性和流程自动化程度，不承诺所有科学判断正确或所有错误自动修复。专家审核分析设计，可执行校验器检查真实数据与产物，Agent 结合组学知识解释异常；三者互补。

`snakemake -n` 是必要的验证步骤，但不能证明工具实际运行成功、checkpoint 下游全部展开或结果科学正确。环境和数据完全锁定也不自动保证结果逐字节一致；非确定性工具需另外定义随机种子、运行条件及允许偏差。

### 1.4 总体架构

```text
需求 + 数据条件
       ↓
Build：检索已审核 rule → AnalysisPlan → 专家审核科学方案
       ↓
PipelinePlan → 版本解析与锁定 → 确定性编译
       ↓
静态/契约验证 → dry-run → 最小真实运行 → 结果校验
       ↓
候选实现审核 → 不可变 pipeline 发布
       ↓
Run：选择发布版本 → 项目预检 → 运行授权 → 提交
       ↓
监控 → 结果校验 → 授权内修复 / 专家核对 / 转 Build
       ↓
交付 + run.lock.yaml + ledger + 产物清单

历史复跑/回退：选择旧发布版本 → 隔离目录 → 新 run_id → 同样的预检与授权
```

---

## 二、术语与约定

| 术语 | 含义 |
|---|---|
| rule | Snakemake 执行单元；用稳定的 `rule_id` 标识逻辑能力 |
| module | 包含相关 rule 的源码模块；不能假设任意旧 `.smk` 都可独立导入 |
| manifest | rule 的接口、适用条件、文件依赖、环境和校验契约 |
| port | 输入输出的语义类型及兼容属性，不是简单的文件扩展名 |
| catalog / deliverable | 用户需求到可交付产物、入口和所需能力的映射 |
| AnalysisPlan | 面向专家的科学分析方案 |
| PipelinePlan | 面向编译器的结构化实现计划 |
| rule release | 某个 `rule_id@version` 对应的不可变依赖闭包和审核记录 |
| pipeline release | 组合逻辑、参数策略、编译产物及依赖锁的不可变发布单元 |
| release manifest | 发布身份、源码定位、依赖与证据摘要清单 |
| run lock | 单次运行使用的确切发布版本、数据、配置、环境和策略快照 |
| ledger | 运行服务追加写入的事件台账；不是 Agent 自行宣称审批的文本 |
| validation policy | 版本化的检查项、适用范围、阈值来源、严重性和处置规则 |
| approval | 绑定具体对象摘要、范围和审批人的授权记录 |
| Build / Run | 同一个 Agent 的两类能力角色，不要求两个独立 Agent |
| execution policy | 运行模式、动作白名单、资源预算、重试和审批规则 |
| skill | 指导方案设计和异常解释的知识资源，不替代实际校验或权限控制 |

本规范使用「必须」「禁止」「应该」「可以」分别表示强制要求、禁止行为、推荐做法和可选能力。规范中的审批条款约束未来系统，不代表本次编辑文档需要额外审批。

---

## 三、职责与资产边界

### 3.1 三类版本化资产

| 资产 | 内容 | 发布与管理 |
|---|---|---|
| Rule 库与 pipeline | `.smk`、运行所需 helper/scripts、manifest、配置 Schema、环境 YAML/锁文件、rule 专用校验器、测试和组合适配器 | rule 独立版本 + pipeline 发布；Git 管理源码，制品存储保存不可变打包结果 |
| Agent 系统 | Plan Schema、编译器、通用校验框架、工具服务、审批与执行策略、CLI/UI、SDK 依赖、SOP 和组学 Skills | 与 Agent 一起管理和发布；每次构建与运行记录实际版本 |
| 参考数据与运行记录 | FASTA/GTF/索引、输入和输出数据、项目配置快照、审批与运行证据 | 数据存储/对象存储管理；版本化 YAML 和摘要关联到运行 |

**按实际依赖分类**：被 rule 执行时调用的 Python/R/shell 脚本属于 rule 的依赖；帮助 Agent 检索、编译、提交和解释结果的脚本属于 Agent 系统。通用校验引擎可随 Agent 发布，但每个 pipeline 必须锁定实际使用的引擎、策略及 rule 专用检查版本。

源码与运行资料可以位于不同仓库或同一仓库的不同目录。物理布局不改变上述独立发布与锁定责任。项目敏感配置、运行日志和大型数据不要求提交到公共源码 Git。

### 3.2 Agent 与确定性代码的分工

- Agent：理解需求、查询库、提出计划、解释失败、生成候选修订、整理评审材料。
- 版本解析器：解析已发布版本、传递依赖和兼容条件，生成精确锁，不由模型猜 commit 或摘要。
- 编译器：从合法计划生成固定结构的文件；不让模型自由生成每一个生产文件。
- 校验器：解析真实文件、计算指标、判断批准的约束、产生结构化证据。
- 执行服务：核对版本与授权、提交任务、跟踪集群状态、限制修改范围、记录台账。
- 专家：批准科学方案、候选 rule/pipeline 发布，以及授权之外的科学调整或异常处置。

### 3.3 Build 与 Run 的写入范围

Build 可以修改隔离开发目录中的候选代码和依赖，创建分支及评审包。已发布制品只读；修复必须创建新版本。

Run 只读发布制品和参考源；可写单次任务的配置快照、输出、日志、缓存及 Snakemake 状态目录。不得直接编辑已发布 rule、环境锁或参考描述。发现需要代码或科学调整时转交 Build，不能以「自修复」绕过发布审核。

---

## 四、结构化契约与数据模型

### 4.1 Rule Manifest

现有 `.smk` 模块可继续配一份 `rules/manifests/<module>.manifest.yaml`，内部每条 rule 从首个试点起具备独立身份和版本。以下为单条 rule 的字段示意，不是当前模板的已验证完整依赖清单：

```yaml
schema_version: "1"
module:
  id: rna.short_read_qc
  snakefile: rules/03.short_read_qc.smk
  requires_helpers: [rule_resource, logger]
  requires_context: [config, samples]
  bootstrap_ref: flow.bootstrap@1.0.0

rules:
  - rule_id: rna.qc.short_read_qc_r1
    version: 1.0.0
    name: short_read_qc_r1
    summary: 对单个样本的原始 R1 reads 运行 FastQC
    wildcards: [sample]
    per_sample: true
    io_resolution: static
    inputs:
      - name: link_r1_dir
        port: reads.fastq
        attributes: {mate: R1, processing: raw, compression: gz}
        scope: sample
        path_binding: raw_r1
        source: upstream
      - name: md5_check
        port: qc.md5_check.tsv
        path_binding: md5_gate
        role: gate
        source: upstream
    outputs:
      - name: r1_zip
        port: qc.fastqc.zip
        attributes: {mate: R1, processing: raw}
        scope: sample
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.zip"
        kind: intermediate
      - name: r1_html
        port: qc.fastqc.html
        attributes: {mate: R1, processing: raw}
        scope: sample
        pattern: "01.qc/short_read_qc_r1/{sample}_R1_fastqc.html"
        kind: report
    config_requires: [convert_md5, parameter.threads.fastqc]
    config_schema_ref: rna.qc.config@1.0.0
    dependencies:
      helpers: [flow.resource_manager@1.0.0]
      environments: [env.fastqc@1.0.0]
      validators: [validation.fastqc@1.0.0]
    tests_ref: tests/fixtures/short_read_qc
```

必需语义约定：

- `rule_id` 不依赖目录编号，重命名文件不改变逻辑身份；`version` 对应精确发布记录。
- `inputs[].port`、`outputs[].port` 必须在注册表定义；生产/消费连接检查属性、样本范围、基数、参考身份和数据状态。
- `source` 区分 `upstream` 与 `external`。原始数据、参考文件等可来自外部，不强求库内生产者。
- `pattern` 用于可直接表达的路径；`path_binding` 引用版本化适配器。必须明确定义通配符与 config 插值，禁止把 `{md5dir}` 这类占位符同时当成两种含义。
- `config_requires` 包含必填项；可选项、默认值、类型、枚举与条件必填由 Schema 定义。helper 和 bootstrap 的配置依赖参与合并。
- `io_resolution` 为 `static`、`input_function` 或 `checkpoint`。旧 `dynamic` 字段需迁移映射；普通 input 函数不等于必须等待作业执行。
- manifest 声明运行所需环境、脚本、helper、bootstrap、校验策略和测试；实际发布清单展开全部传递文件依赖。
- `kind: report` 表示输出用途，不表示该输出自动成为用户 deliverable。

### 4.2 端口注册表与路径绑定

端口匹配只是选择候选 rule 的条件，Snakemake 仍通过真实 input/output 路径构建作业 DAG。必须同时满足语义兼容与路径绑定。

例如 `reads.fastq` 的 `processing: raw` 与 `processing: trimmed` 不可默认为可互换；`align.bam` 应根据用途约束坐标体系、排序状态、索引和参考版本。聚合步骤必须明确使用哪一个样本集合，不能悄悄扫描目录纳入其他运行的数据。

`ports.yaml` 人工维护类型定义和兼容约束，生产者/消费者索引从发布 manifest 派生，并带 `rule_id@version`。索引中的「有生产者」不等于任何候选版本都兼容。

### 4.3 产物目录与派生索引

`catalog.yaml` 保存 deliverable 的 ID、名称、用途、keywords、入口或已发布 pipeline、target 模板、样本范围、前置条件和验证策略引用。

Git 中的版本化 manifest、端口定义与 catalog 是相应发布的事实源；SQLite/FTS 等只作为可重建索引。发布状态与撤销记录由服务维护。查询工具默认返回紧凑摘要及版本/来源指针，详情按需读取；不把全库上下文成本承诺为固定 token 数。

派生索引必须记录来源发布摘要。Run 按 lock 查确切版本，禁止索引刷新后自动替换成 latest。第一版可使用本地查询库或 CLI，再通过 MCP 暴露同一实现。

### 4.4 AnalysisPlan：专家审核对象

分析方案必须记录：

- 研究目标、组学类型、实验方法、预期 deliverable；
- 样本范围、分组与对照、配对关系、批次及协变量；
- 方法、关键参数、参考数据版本、适用条件与选择依据；
- 校验策略、阈值来源、可接受差异和异常处置；
- 未知信息、方法假设、需要专家决定的问题；
- 方案 ID、修订号、所用 Skill/资料版本及审批引用。

缺少决定科学方案的信息时提出明确问题，不能为通过校验而编造分组、参考版本或阈值。专家审核绑定方案内容摘要；科学含义改变时重新审核。

### 4.5 PipelinePlan：编译器输入

Agent 提交结构化计划，不提交任意可执行代码。以下为字段示意；`<...>` 为待系统解析的占位符，正式 lock 中禁止保留：

```yaml
schema_version: "1"
pipeline_id: customer_raw_qc
revision: 1
analysis_plan_ref: analysis-001@1
mode: existing_entry
base_pipeline_ref: RNAFlow@20250601
steps:
  - instance_id: qc_r1
    rule_ref: rna.qc.short_read_qc_r1@1.0.0
bindings:
  qc_r1.link_r1_dir: input.raw_r1
  qc_r1.md5_check: upstream.md5_pass
targets: [raw_multiqc_r1]
config_ref: config/project.yaml
sample_sheet_ref: config/samples.csv
reference_ref: references/project.yaml
validation_policy_ref: validation.raw_qc@1.0.0
execution_policy_ref: supervised@1
```

计划中的 steps 可以是能力选择，解析器必须补全所需上游规则闭包，遇到多个不等价生产者要求明确选择。校验器解析完整连接和实际 target，不能因为一个示意步骤合法就视整条链完整。样本级作业展开和调度 DAG 由 Snakemake 决定，不自造调度引擎。

支持 `existing_entry` 与 `compiled_modules` 两种编译模式。计划中的路径和参数由 Schema 约束，不允许用任意 Python 表达式或 shell 文本充当绑定。

### 4.6 发布、运行、审批与事件记录

以下记录为独立对象，通过 ID 和内容摘要关联：

| 对象 | 必需记录 |
|---|---|
| rule release | rule 身份/版本、源码 commit、模块、文件与依赖摘要、环境锁、验证证据和发布审核 |
| pipeline release | 组合定义、rule 精确依赖锁、编译器/模板、编译产物、参数约束、验证与审核 |
| run.lock.yaml | 发布身份、实际 rule/环境/参考锁、有效配置、样本表和输入清单、执行环境、策略与授权 |
| approval | 审批人、时间、被审批对象摘要、决定、授权范围、限制和有效条件 |
| ledger | 事件 ID/顺序、run/attempt ID、动作、对象摘要、证据引用、结果、审批引用和集群 job ID |
| gap log | 缺口类型、需求、缺失能力/数据、来源、重复次数和后续评审关联 |

run lock 必须保存合并默认值和命令行覆盖后的有效配置，不能只保存项目 YAML 路径。日志与摘要应去除凭据；身份记录不保存访问密钥。

事件只追加，按 run 分区并处理并发。审批由身份可信的服务写入，Agent 无权自行设置 `approved: true`。模型输出、工具 stdout 和日志中的指令一律不能当作批准。

---


## 五、版本治理

### 5.1 Rule 版本

每个 rule 从首次进入受管库开始使用稳定 `rule_id` 和语义版本：

- MAJOR：输入输出端口、语义、科学假设或不兼容行为改变；
- MINOR：兼容地增加可选能力或输出；
- PATCH：修复实现、日志、资源或兼容性问题，不改变接口和科学含义。

rule 的不可歧义身份是 `rule_id + source_commit`，版本号是可读标签。发布后禁止移动 tag 或复用版本号。更改 `.smk`、manifest、helper、脚本、环境定义、锁文件、Schema、适配器或 rule 专用校验器时，必须评估是否需要新 rule 版本；有关联变更必须一个原子提交或具备明确依赖链。

每个 Rule Release Bundle 必须包含：

```text
rules/*.smk
rules/manifests/*.yaml
rules/utils 与 rule 使用的 scripts
envs/*.yaml 与平台 lock
schema/ 与 rule 配置约束
validators/
tests/fixtures 与预期结果
release.yaml
```

“同一个文件版本”不能替代依赖闭包版本。发布清单必须列出所有传递依赖的 `id@version` 和内容 SHA-256。

### 5.2 环境版本

Conda YAML 必须纳入 rule 或 pipeline 的发布依赖。推荐：

```text
envs/fastqc.yaml              # 人维护的直接依赖声明
envs/locks/fastqc.linux-64.lock  # 完整解析锁
```

环境版本独立编号，但在 release 中被精确引用：

```yaml
environment_id: env.fastqc
version: 1.0.0
spec_file: envs/fastqc.yaml
lock_files: [envs/locks/fastqc.linux-64.lock]
spec_sha256: "..."
lock_sha256: "..."
```

必须记录 Snakemake、插件、容器/Conda 前端和目标平台版本。YAML 修改、锁文件重建、通道优先级或平台改变，都要重新测试；影响 rule 结果或可运行性的环境变化不能静默覆盖旧环境。重要发布应缓存镜像或包制品，避免多年后源不可用。

### 5.3 Pipeline 版本

Pipeline Release 是组合逻辑和依赖的不可变快照：

```yaml
pipeline_id: RNAFlow-qc-mapping
version: 2026.06.01
repository_commit: abc123
compiler:
  id: flow.pipeline_compiler
  version: 1.0.0
rules:
  - rule_ref: rna.raw.check_md5@1.1.0
  - rule_ref: rna.qc.short_read_qc_r1@1.0.0
environments:
  - env.fastqc@1.0.0
references:
  - reference.hg38.star@2026.05.15
validation_policies: [rna.qc.outputs@1.0.0]
```

发布后 pipeline lock、组合 Snakefile、有效默认配置、测试证据和 review 记录不可变。客户“换样本/路径”生成新的 run，不改变 pipeline 版本；改变 rule 组合、科学参数、参考数据、环境或验证策略时创建新的 pipeline revision/release。

### 5.4 Agent 与 Skills 版本

Agent 系统另行发布，例如：

```yaml
agent_release: flow-agent@1.3.0
compiler: flow-compiler@1.1.0
validator_engine: flow-validator@1.0.0
skills:
  - bulk-rna@2.0.0
  - qc-interpretation@1.1.0
```

组学 Skill 提供方案模板、方法适用条件、异常解释和校验策略引用；它不能直接放宽执行权限，也不能替代校验器。每次 Build/Run 的 lock 必须记录实际使用的 Agent、compiler、validator、Skills 与策略版本。

### 5.5 参考数据与对象存储

参考 FASTA、GTF、索引和其它大型固定文件放在对象存储是推荐方案。使用内容不可变 URI、对象存储版本 ID 或内容摘要，不能只写会被覆盖的 `s3://bucket/hg38/latest`。

```yaml
reference_id: reference.hg38.star
version: 2026.05.15
objects:
  - uri: s3://flow-reference/hg38/star/2026.05.15/index.tar.zst
    object_version_id: "..."
    sha256: "..."
  - uri: s3://flow-reference/hg38/annotation/2026.05.15.gtf.gz
    sha256: "..."
metadata: {genome: hg38, annotation: GENCODE_v47, index_tool: STAR_2.7}
```

下载到运行目录后再次校验 hash；URI、对象版本、解压目录和构建工具版本写入 run lock。对象不可用或摘要不符时停止，不能自动改用 latest。

### 5.6 升级、历史运行与回退

历史 release 必须可读取且禁止原地修改。Agent 根据客户记录查找原 pipeline release，创建隔离 checkout/workspace，解析对应 rule、环境锁和参考对象，生成新的 run lock 和输出目录，然后照常执行预检、dry-run 和授权。

```text
客户任务 → pipeline@2025.06.01 → commit/lock/reference@固定版本
                    ↓
          新 run_id + 新输出目录 + 新 ledger
```

回退是“选择旧发布版本创建新运行”，不是把当前开发分支执行 `git reset`，也不是覆盖已有结果。若要把旧版本输出用于比较，必须记录输入相同与否、结果校验策略及差异报告。

---

## 六、校验与结果质量

### 6.1 四级验证门

1. **契约验证**：manifest schema、端口属性、配置必填项、依赖闭包和静态漂移检查。
2. **工作流验证**：Snakemake 加载、目标解析、实际 DAG、`--dry-run`、资源与路径预检。
3. **执行验证**：最小 fixture 或项目运行中的工具退出码、日志、输出存在性、格式、样本集合和参考身份。
4. **科学结果验证**：按批准的 validation policy 检查指标、阈值、允许差异、质量报告和关键统计结果。

前三级通常可自动执行。第四级由版本化策略和校验器执行；发现需要科学判断的 WARN/NEEDS_REVIEW 时提交专家，而不是由模型自行宣布通过。

### 6.2 Validation Policy

```yaml
policy_id: rna.qc.outputs
version: 1.0.0
applies_to: [rna.qc.short_read_qc_r1@">=1,<2"]
checks:
  - id: sample_ids_match
    validator: matrix_sample_ids
    severity: error
  - id: alignment_rate
    validator: star_metrics
    thresholds_ref: analysis_plan
    severity: review
  - id: count_matrix_contract
    validator: count_matrix_structure
    severity: error
```

每个检查必须定义输入、代码版本、预期条件、严重性和处置：

- **PASS**：条件满足；
- **WARN**：保存警告并可按策略继续；
- **FAIL**：阻断下游或交付；
- **NEEDS_REVIEW**：暂停等待专家；
- **NOT_APPLICABLE**：记录为何不适用。

常见检查包括：

- 样本表、目标文件和矩阵的样本 ID 集合一致；
- 矩阵行列、数据类型、缺失值、ID 唯一性符合契约；
- FASTQ/BAM/索引可读且配对完整；
- 比对、测序深度、重复率等指标在该项目批准的范围或参考分布内；
- 参考基因组、注释和索引身份与计划一致；
- 输出数量、目录和文件格式完整；
- 最小测试或历史参考结果在策略规定的差异范围内。

阈值必须来自实验类型、批准的分析方案、验证数据或专家维护的策略，不得由 Agent 临时猜测。规则允许“缺失阈值时需要审核”，不允许为了通过而自动放宽阈值。参考结果比较可以定义结构、统计指标和容许差异，不强制所有文件逐字节相同。

### 6.3 校验器的实现位置

校验器不是另一个必须独立部署的 Agent。建议由一个通用 validator engine 加载各组学校验插件：

```text
agent/
  validators/
    common.py
    bulk_rna.py
    scrna.py
    atac.py
  policies/
    bulk_rna/*.yaml
    scrna/*.yaml
  skills/
    bulk-rna/SKILL.md
    scrna/SKILL.md
```

插件负责读取真实文件、计算指标并输出结构化结果；Skill 负责告诉 Agent 如何选择策略和解释证据；专家批准方案及策略。大型组织可以另设结果审查 Agent 生成第二意见，但它不能绕过上述校验器或人工闸门。

校验器可以作为 Snakemake rule 运行，也可以作为提交前/交付前服务。对于会影响下游的检查，优先设为显式校验 rule，产出带版本的 `validation.json` 和 `validation_passed` 标记。


## 八、Rule 库增长与治理

### 8.1 候选 rule 入库门禁

新增或修改 rule 的候选包必须包含 rule diff、manifest、相关 helper/script、环境 YAML 和锁文件、校验器、最小测试数据、预期输出、dry-run、最小真实运行和来源说明。入库必须通过：

1. Schema 与依赖校验；
2. 静态加载和 dry-run；
3. 最小真实运行和输出校验；
4. 专家审核 checklist；
5. 新版本 release 和不可变制品保存。

专家批准的是候选 rule 的适用范围和证据，不只是代码能否运行。被驳回的草案和修正意见可作为 Agent 的提示范例，但不得被当作已发布能力。

### 8.2 候选 pipeline 发布门禁

复用已发布 rule 生成的候选 pipeline 必须包含 AnalysisPlan、PipelinePlan、依赖 lock、编译产物、dry-run、最小真实运行、结果校验和人工审核记录。审核通过后发布不可变 pipeline；变更 rule 组合、关键参数、参考、环境或策略时创建新的 pipeline revision。

### 8.3 缺口与持续测试

端口断链、缺 rule、缺 deliverable、校验失败和专家否决必须写入 gap log。高频缺口用于安排 rule 策展。CI 定期复测已发布 rule 和 pipeline；环境或工具升级导致失效的版本标记为待修复，Agent 组链时禁用，历史运行仍保留原 lock。

---

## 九、实现形态与工具接口

### 9.1 两种工作模式

同一个 Flow Agent 同时提供 Build 和 Run。Build 允许在隔离分支生成候选文件；Run 使用不可变 release。两种模式共用 catalog、manifest、validator、ledger 和审批服务。

第一版优先实现标准入口模式：Agent 生成 analysisyaml、样本表副本和 target，调用现有入口完成验证。只有现有入口无法表达需求时，才使用固定模板编译组合 Snakefile。当前模块依赖 samples、config、logger、rule_resource 和 workflow 等入口上下文，必须先完成上下文审计和 bootstrap 契约，再开放任意 module + use rule 组合。

### 9.2 组合文件约定

组合文件由编译器生成，位置为 composed/<pipeline-id>/，不覆盖已有目录：

```text
composed/<pipeline-id>/
  plan.yaml
  Snakefile
  config/analysis.yaml
  config/samples.csv
  release.lock.yaml
  validation/dry-run.txt
  review/approval.json
```

禁止把 rule 本体复制到组合文件；只允许使用已发布模块和编译器支持的固定模板。组合文件中的每个 rule 引用必须能追溯到 rule_id@version。

### 9.3 推荐工具

```text
query_catalog / query_ports / query_rules
validate_plan / compile_pipeline
validate_manifests / dry_run / detailed_summary
validate_outputs / read_log / write_gap_log
submit_cluster / pause / resume / rollback_run
```

工具输出必须结构化，同时保留可读摘要。Agent 的自然语言回答不能作为唯一审计证据。

---

## 十、阶段化实施与验收

### 10.1 阶段 0：流程现状审计

只查不改，产出模块/rule 清单、I/O 衔接、config key、helper 和动态 I/O 清单、入口上下文、样本配对现状、环境与参考依赖清单。抽查文件和行号必须能对应实际代码。

### 10.2 阶段 1：Manifest 与 Rule 版本试点

选 rules/03.short_read_qc.smk 建立 manifest、rule_id、环境声明、最小测试和一个 Rule Release Bundle。验证 manifest、代码、helper、配置和环境能共同完成加载、dry-run 与最小运行。

### 10.3 阶段 2：校验器、端口和 catalog

建立 validate_manifests.py、端口属性兼容检查、catalog、派生索引、validation policy 和结果校验插件。故意修改 manifest、环境或输出结构时，CI 必须识别漂移或回归。

### 10.4 阶段 3：PipelinePlan 与确定性编译器

实现 PipelinePlan Schema、版本解析器和固定模板编译器。优先支持标准入口；确认 bootstrap 和上下文契约后，再支持组合模块。Agent 不得直接生成任意生产代码。

### 10.5 阶段 4：Build/Run 端到端验收

- 方案审核：Agent 生成 AnalysisPlan，专家修改或批准；改变科学含义后必须生成新修订。
- 候选 pipeline：复用已发布 rule，生成 PipelinePlan 和编译产物，经过 dry-run、最小运行、结果校验和专家审核后发布。
- 候选 rule：新增/修改 rule 连同依赖和测试进入独立入库门禁。
- 运行授权：分别验证 autonomous、supervised、manual；未经授权的科学调整、版本变更和锁漂移必须被阻断。
- 故障处理：验证日志诊断、授权内 rerun-incomplete、专家核对和重试熔断。
- 历史回退：从旧 pipeline release 创建新 run，不覆盖现有输出，并能通过 run lock 还原代码、环境、参考和配置。

### 10.6 阶段 5：平台化 Agent

在上述资产稳定后，再决定采用 MCP、Agent SDK、CLI 或 Web UI。SDK 是实现选择，不应成为 manifest、release、lock 和验证契约的前置依赖。

---

## 十一、风险与回退原则

| 风险 | 应对 |
|---|---|
| rule/manifest/环境漂移 | 同一原子变更或明确依赖锁；CI、hash 和不可变 release |
| Agent 生成错误 | 只生成 Plan，确定性编译；Schema、dry-run、最小运行和专家审核 |
| 科学方案错误 | AnalysisPlan 专家审核；结果异常进入 NEEDS_REVIEW |
| 自动修复越权 | execution policy、动作白名单、审批服务和工具层 submit guard |
| 参考数据被替换 | 对象存储不可变版本、hash 校验，禁止自动改用 latest |
| 环境源失效 | Conda lock、平台记录、镜像/包制品缓存 |
| 版本回退覆盖结果 | 隔离 checkout、独立 run_id 和输出目录 |
| 动态 I/O 不可静态预测 | 标记高风险，实际 dry-run 和最小运行验证 |

任何阶段出问题，新增资产均应以增量方式叠加，不得改变既有生产 release；需要修改生产 rule 时只能发布新版本。

---

## 十二、结论与残余边界

本方案把开放式生成降维为：检索已审核 rule → 专家审核科学方案 → 结构化计划 → 确定性编译 → 自动验证 → 专家批准发布 → 按授权策略运行。它支持同一个 Agent 兼任 Build 和 Run，也支持 rule、环境、pipeline、参考数据和运行实例的完整版本锁定，因此可以服务新流程建设、客户历史版本复跑、升级比较和回退。

结果校验不需要额外的必选 Agent：校验器负责可重复计算，组学 Skill 负责方法知识和异常解释，专家负责科学判断。可选的第二意见 Agent 不能替代校验器或人工审核。

仍需明确的边界：dry-run 不证明科学正确；锁定版本不保证所有工具逐字节确定；Agent 不能替代专家对新研究设计的判断。凡是改变科学含义的调整，都必须回到 AnalysisPlan 和发布审核流程。

---

## 附录：命令与记录速查

```bash
snakemake -n <targets>
snakemake --detailed-summary <targets>
snakemake --dag <targets> | dot -Tsvg > dag.svg
snakemake --rulegraph | dot -Tsvg > rulegraph.svg
snakemake --lint
snakemake --list-target-rules
snakemake --rerun-incomplete
python agent/validate_manifests.py
python agent/validate_plan.py plan.yaml
python agent/compile_pipeline.py plan.yaml
```

每次 Build/Run 至少保存：release.lock.yaml、run.lock.yaml、有效 config、样本表、reference lock、环境 lock、dry-run、summary、validation.json、approval 和 JSONL ledger。大文件放对象存储，记录 URI、版本 ID 和 SHA-256。

[^1]: Masera M, Leone A, Köster J, et al. Snakemaker: Seamlessly transforming ad-hoc analyses into sustainable Snakemake workflows with generative AI[J]. arXiv preprint:2505.02841, 2025.
