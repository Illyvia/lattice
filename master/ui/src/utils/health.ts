import { HealthState, NodeRecord } from "../types";

export function getHeartbeatHealth(node: NodeRecord): HealthState {
  if (node.state !== "paired" || !node.last_heartbeat_at) {
    return "pending";
  }

  const heartbeatMs = new Date(node.last_heartbeat_at).getTime();
  if (Number.isNaN(heartbeatMs)) {
    return "down";
  }

  const ageSeconds = Math.floor((Date.now() - heartbeatMs) / 1000);
  if (ageSeconds <= 20) {
    return "healthy";
  }
  if (ageSeconds <= 60) {
    return "degraded";
  }
  return "down";
}

export function formatTimestamp(value: string | null): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
