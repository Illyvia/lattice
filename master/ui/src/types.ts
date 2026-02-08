export type NodeState = "pending" | "paired";
export type ViewMode = "dashboard" | "nodes" | "node-detail";
export type HealthState = "healthy" | "degraded" | "down" | "pending";
export type ThemeMode = "light" | "dark";

export type RuntimeMetrics = {
  cpu_percent?: number;
  memory_percent?: number;
  memory_used_bytes?: number;
  memory_total_bytes?: number;
  storage_percent?: number;
  storage_used_bytes?: number;
  storage_total_bytes?: number;
  updated_at?: string;
};

export type NodeRecord = {
  id: string;
  name: string;
  pair_code: string;
  state: NodeState;
  created_at: string;
  paired_at: string | null;
  last_heartbeat_at: string | null;
  agent_commit?: string | null;
  agent_info?: Record<string, unknown> | null;
  runtime_metrics?: RuntimeMetrics | null;
};

export type NodeLogRecord = {
  id: number;
  node_id: string;
  created_at: string;
  level: string;
  message: string;
  meta: Record<string, unknown> | null;
};
