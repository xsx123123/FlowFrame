import type { FlowAgentConfig } from '../config';
import { createRegistryTools } from './registry';
import { createValidateTool } from './validate';
import { createSnakemakeTools } from './snakemake';
import { createWriteTools } from './writes';

/**
 * 工具集(规范 §8.2 / §9.6):
 * query_catalog / query_ports / query_rules / get_rule / validate_manifests
 * dry_run / detailed_summary / read_log / submit_cluster(硬约束)
 * write_gap_log / write_composed
 * 全部只读仓库;写操作仅限 composed/、agent/gap_log.yaml、agent/run_ledger.jsonl。
 */
export function createTools(cfg: FlowAgentConfig) {
  return {
    ...createRegistryTools(cfg),
    ...createValidateTool(cfg),
    ...createSnakemakeTools(cfg),
    ...createWriteTools(cfg),
  };
}
