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

export type NodeCapabilityVm = {
  provider?: string;
  ready?: boolean;
  message?: string;
  missing_tools?: string[];
};

export type NodeCapabilities = {
  vm?: NodeCapabilityVm;
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
  capabilities?: NodeCapabilities | null;
};

export type NodeLogRecord = {
  id: number;
  node_id: string;
  created_at: string;
  level: string;
  message: string;
  meta: Record<string, unknown> | null;
};

export type VmImageRecord = {
  id: string;
  name: string;
  os_family: string;
  source_url: string;
  sha256: string | null;
  default_username: string;
  cloud_init_enabled: boolean;
  created_at: string;
};

export type VmOperationRecord = {
  id: string;
  node_id: string;
  vm_id: string | null;
  operation_type: "create" | "start" | "stop" | "reboot" | "delete" | "sync";
  status: "queued" | "running" | "succeeded" | "failed";
  request: Record<string, unknown> | null;
  result: Record<string, unknown> | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
};

export type NodeVmRecord = {
  id: string;
  node_id: string;
  name: string;
  state: "creating" | "running" | "stopped" | "rebooting" | "deleting" | "error" | "unknown";
  provider: string;
  domain_name: string;
  domain_uuid: string | null;
  image_id: string;
  image_name: string;
  vcpu: number;
  memory_mb: number;
  disk_gb: number;
  bridge: string;
  ip_address: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
  last_operation?: {
    id: string;
    operation_type: string;
    status: string;
    created_at: string;
    started_at: string | null;
    ended_at: string | null;
    error: string | null;
  } | null;
};

export type TerminalCommandRecord = {
  id: string;
  node_id: string;
  status: "queued" | "running" | "succeeded" | "failed";
  command_text: string;
  stdout_text: string | null;
  stderr_text: string | null;
  exit_code: number | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
};
