import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import type { IconProp } from "@fortawesome/fontawesome-svg-core";
import {
  faArrowLeft,
  faPenToSquare,
  faRotateRight,
  faTrashCan
} from "@fortawesome/free-solid-svg-icons";
import { faApple, faLinux, faWindows } from "@fortawesome/free-brands-svg-icons";
import { useNavigate, useParams } from "react-router-dom";
import { NodeLogRecord, NodeRecord, RuntimeMetrics } from "../types";
import { formatTimestamp, getHeartbeatHealth } from "../utils/health";

type NodeDetailPageProps = {
  nodes: NodeRecord[];
  apiBaseUrl: string;
  onDeleteNode: (nodeId: string) => Promise<void>;
  onRenameNode: (nodeId: string, name: string) => Promise<void>;
};

type NodeLogsResponse = {
  items?: NodeLogRecord[];
  next_since_id?: number | null;
};

function heartbeatAgeText(timestamp: string | null): string {
  if (!timestamp) {
    return "-";
  }
  const ms = new Date(timestamp).getTime();
  if (Number.isNaN(ms)) {
    return "-";
  }
  const ageSeconds = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  return `${ageSeconds}s ago`;
}

function formatLogTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleTimeString();
}

function getMetaHostname(meta: Record<string, unknown> | null): string | null {
  if (!meta) {
    return null;
  }
  const hostname = meta.hostname;
  return typeof hostname === "string" && hostname.trim() ? hostname.trim() : null;
}

function getAgentOs(agentInfo: Record<string, unknown> | null | undefined): string | null {
  if (!agentInfo || typeof agentInfo !== "object") {
    return null;
  }
  const value = agentInfo["os"];
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  return null;
}

function getOsIcon(os: string | null): IconProp | null {
  if (!os) {
    return null;
  }
  const normalized = os.toLowerCase();
  if (normalized.includes("win")) return faWindows as IconProp;
  if (normalized.includes("mac") || normalized.includes("darwin") || normalized.includes("os x"))
    return faApple as IconProp;
  if (normalized.includes("linux")) return faLinux as IconProp;
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function percentValue(value: unknown): number | null {
  const num = asNumber(value);
  if (num === null) {
    return null;
  }
  return Math.max(0, Math.min(100, num));
}

function formatPercent(value: number | null): string {
  if (value === null) {
    return "-";
  }
  return `${value.toFixed(1)}%`;
}

function formatBytes(value: number | null): string {
  if (value === null || value < 0) {
    return "-";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let bytes = value;
  let unitIndex = 0;
  while (bytes >= 1024 && unitIndex < units.length - 1) {
    bytes /= 1024;
    unitIndex += 1;
  }
  return `${bytes.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function usageDetail(usedBytes: number | null, totalBytes: number | null): string {
  if (usedBytes === null || totalBytes === null || totalBytes <= 0) {
    return "-";
  }
  return `${formatBytes(usedBytes)} / ${formatBytes(totalBytes)}`;
}

type UsageTone = "good" | "warn" | "bad" | "unknown";

function getUsageTone(percent: number | null): UsageTone {
  if (percent === null) {
    return "unknown";
  }
  if (percent <= 55) {
    return "good";
  }
  if (percent < 85) {
    return "warn";
  }
  return "bad";
}

function MetricBar({
  label,
  percent,
  detail
}: {
  label: string;
  percent: number | null;
  detail: string;
}) {
  const tone = getUsageTone(percent);
  return (
    <article className="usage-metric-card">
      <div className="usage-metric-top">
        <span className="usage-metric-label">{label}</span>
        <strong className={`usage-metric-value usage-metric-value-${tone}`}>{formatPercent(percent)}</strong>
      </div>
      <div className="usage-bar-track">
        <span
          className={`usage-bar-fill usage-bar-fill-${tone}`}
          style={{ width: `${percent ?? 0}%` }}
        />
      </div>
      <p className="usage-metric-detail">{detail}</p>
    </article>
  );
}

export default function NodeDetailPage({
  nodes,
  apiBaseUrl,
  onDeleteNode,
  onRenameNode
}: NodeDetailPageProps) {
  const navigate = useNavigate();
  const { nodeId } = useParams();
  const node = nodes.find((candidate) => candidate.id === nodeId) ?? null;
  const [logs, setLogs] = useState<NodeLogRecord[]>([]);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [streamConnected, setStreamConnected] = useState(false);
  const [streamRevision, setStreamRevision] = useState(0);
  const [usePolling, setUsePolling] = useState(false);
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameValue, setRenameValue] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const logsContainerRef = useRef<HTMLDivElement | null>(null);

  const nodeLogsWsUrl = useMemo(() => {
    if (!node?.id) {
      return null;
    }
    const base = apiBaseUrl.replace(/\/+$/, "");
    const wsBase = base.replace(/^http/i, (value) =>
      value.toLowerCase() === "https" ? "wss" : "ws"
    );
    return `${wsBase}/ws/node-logs?node_id=${encodeURIComponent(node.id)}&limit=200`;
  }, [apiBaseUrl, node?.id]);

  const nodeLogsHttpBase = useMemo(() => {
    if (!node?.id) {
      return null;
    }
    const base = apiBaseUrl.replace(/\/+$/, "");
    return `${base}/api/nodes/${encodeURIComponent(node.id)}/logs`;
  }, [apiBaseUrl, node?.id]);

  useEffect(() => {
    setUsePolling(false);
    setStreamConnected(false);
  }, [node?.id]);

  useEffect(() => {
    const currentNodeId = node?.id;
    if (!nodeLogsWsUrl || !currentNodeId) {
      setLogs([]);
      setLogsError(null);
      setLogsLoading(false);
      setStreamConnected(false);
      setUsePolling(false);
      return;
    }
    if (usePolling) {
      return;
    }

    let isDisposed = false;
    let shouldReconnect = true;
    let reconnectDelayMs = 1000;
    let reconnectTimer: number | null = null;
    let socket: WebSocket | null = null;
    let hasReceivedFrame = false;

    const connect = () => {
      if (isDisposed || !shouldReconnect) {
        return;
      }
      setLogsLoading(true);

      try {
        socket = new WebSocket(nodeLogsWsUrl);
      } catch {
        setStreamConnected(false);
        setLogsLoading(false);
        setLogsError("Failed to open live log stream.");
        reconnectTimer = window.setTimeout(connect, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 10000);
        return;
      }

      socket.onopen = () => {
        if (isDisposed) {
          return;
        }
        setStreamConnected(true);
        setLogsError(null);
        reconnectDelayMs = 1000;
      };

      socket.onmessage = (event) => {
        if (isDisposed) {
          return;
        }
        hasReceivedFrame = true;
        let payload: unknown;
        try {
          payload = JSON.parse(event.data as string);
        } catch {
          return;
        }
        if (!payload || typeof payload !== "object") {
          return;
        }
        const parsed = payload as {
          type?: unknown;
          error?: unknown;
          items?: unknown;
        };
        const messageType = typeof parsed.type === "string" ? parsed.type : "";

        if (messageType === "snapshot") {
          const items = Array.isArray(parsed.items) ? (parsed.items as NodeLogRecord[]) : [];
          setLogs(items);
          setLogsLoading(false);
          return;
        }

        if (messageType === "append") {
          const items = Array.isArray(parsed.items) ? (parsed.items as NodeLogRecord[]) : [];
          if (items.length === 0) {
            return;
          }
          setLogs((current) => {
            const existingIds = new Set(current.map((entry) => entry.id));
            const merged = [...current];
            for (const item of items) {
              if (!existingIds.has(item.id)) {
                existingIds.add(item.id);
                merged.push(item);
              }
            }
            return merged.slice(-500);
          });
          setLogsLoading(false);
          return;
        }

        if (messageType === "error") {
          const errorCode = typeof parsed.error === "string" ? parsed.error : "stream_error";
          if (errorCode === "node_not_found") {
            shouldReconnect = false;
            setLogs([]);
            setLogsError("Node logs are unavailable because this node no longer exists on master.");
          } else {
            shouldReconnect = false;
            setUsePolling(true);
            setLogsError("Live stream unavailable. Falling back to polling.");
          }
          setLogsLoading(false);
          setStreamConnected(false);
          socket?.close();
        }
      };

      socket.onerror = () => {
        if (isDisposed) {
          return;
        }
        setStreamConnected(false);
        if (!hasReceivedFrame) {
          shouldReconnect = false;
          setUsePolling(true);
          setLogsLoading(false);
          setLogsError("Live stream unavailable. Falling back to polling.");
          socket?.close();
        }
      };

      socket.onclose = () => {
        if (isDisposed) {
          return;
        }
        setStreamConnected(false);
        setLogsLoading(false);
        if (!shouldReconnect) {
          return;
        }
        setLogsError((current) => current ?? "Live log stream disconnected. Reconnecting...");
        reconnectTimer = window.setTimeout(connect, reconnectDelayMs);
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, 10000);
      };
    };

    connect();

    return () => {
      isDisposed = true;
      shouldReconnect = false;
      setStreamConnected(false);
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
      }
    };
  }, [nodeLogsWsUrl, node?.id, streamRevision, usePolling]);

  useEffect(() => {
    if (!usePolling || !nodeLogsHttpBase) {
      return;
    }

    let isDisposed = false;
    let sinceId: number | null = null;

    const loadLogs = async (fullRefresh: boolean) => {
      const url =
        fullRefresh || sinceId === null
          ? `${nodeLogsHttpBase}?limit=200`
          : `${nodeLogsHttpBase}?limit=200&since_id=${sinceId}`;
      try {
        if (fullRefresh) {
          setLogsLoading(true);
        }
        const response = await fetch(url, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Failed to load logs (${response.status})`);
        }
        const payload = (await response.json()) as NodeLogsResponse;
        const items = Array.isArray(payload.items) ? payload.items : [];
        const nextSinceId =
          typeof payload.next_since_id === "number" ? payload.next_since_id : sinceId;
        sinceId = nextSinceId;

        if (fullRefresh || sinceId === null) {
          setLogs(items);
        } else if (items.length > 0) {
          setLogs((current) => {
            const existingIds = new Set(current.map((entry) => entry.id));
            const merged = [...current];
            for (const item of items) {
              if (!existingIds.has(item.id)) {
                existingIds.add(item.id);
                merged.push(item);
              }
            }
            return merged.slice(-500);
          });
        }
        setLogsError(null);
      } catch (err) {
        setLogsError(err instanceof Error ? err.message : "Failed to load logs");
      } finally {
        if (fullRefresh) {
          setLogsLoading(false);
        }
      }
    };

    void loadLogs(true);
    const timer = window.setInterval(() => {
      if (!isDisposed) {
        void loadLogs(false);
      }
    }, 2000);

    return () => {
      isDisposed = true;
      window.clearInterval(timer);
    };
  }, [nodeLogsHttpBase, usePolling]);

  useEffect(() => {
    const container = logsContainerRef.current;
    if (!container) {
      return;
    }
    container.scrollTop = container.scrollHeight;
  }, [logs]);

  if (!node) {
    return (
      <section>
        <button type="button" className="secondary-button" onClick={() => navigate("/nodes")}>
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Nodes</span>
        </button>
        <p className="muted">Node not found.</p>
      </section>
    );
  }

  const health = getHeartbeatHealth(node);
  const heartbeatAge = heartbeatAgeText(node.last_heartbeat_at);
  const metrics: RuntimeMetrics | null = node.runtime_metrics ?? null;
  const agentOs = getAgentOs(node.agent_info);
  const agentOsIcon = getOsIcon(agentOs);
  const cpuPercent = percentValue(metrics?.cpu_percent);
  const memoryPercent = percentValue(metrics?.memory_percent);
  const storagePercent = percentValue(metrics?.storage_percent);
  const memoryUsedBytes = asNumber(metrics?.memory_used_bytes);
  const memoryTotalBytes = asNumber(metrics?.memory_total_bytes);
  const storageUsedBytes = asNumber(metrics?.storage_used_bytes);
  const storageTotalBytes = asNumber(metrics?.storage_total_bytes);
  const metricsUpdatedAt = formatTimestamp(metrics?.updated_at ?? node.last_heartbeat_at);

  async function confirmRename() {
    if (!node) {
      return;
    }
    setRenaming(true);
    try {
      await onRenameNode(node.id, renameValue);
      setRenameOpen(false);
      setRenameError(null);
    } catch (err) {
      setRenameError(err instanceof Error ? err.message : "Failed to rename node");
    } finally {
      setRenaming(false);
    }
  }

  async function confirmDelete() {
    if (!node) {
      return;
    }
    setDeleting(true);
    try {
      await onDeleteNode(node.id);
      setConfirmDeleteOpen(false);
      setDeleteError(null);
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : "Failed to delete node");
    } finally {
      setDeleting(false);
    }
  }

  return (
    <section>
      <h2 className="node-page-heading">{node.name}</h2>
      <div className="row">
        <button type="button" className="secondary-button" onClick={() => navigate("/nodes")}>
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Nodes</span>
        </button>
        <button
          type="button"
          className="secondary-button"
          onClick={() => {
            setRenameOpen(true);
            setRenameValue(node.name);
            setRenameError(null);
          }}
        >
          <FontAwesomeIcon icon={faPenToSquare} />
          <span>Rename</span>
        </button>
        <button
          type="button"
          className="node-delete-button"
          onClick={() => {
            setConfirmDeleteOpen(true);
            setDeleteError(null);
          }}
        >
          <FontAwesomeIcon icon={faTrashCan} />
          <span>Delete Node</span>
        </button>
      </div>

      <div className="node-detail-header">
        <div className="node-detail-main">
          <div className="node-detail-title-row">
            <p className="stat-label">Node Details</p>
            <div className="node-health-badges">
              <span className={`health health-${health}`}>
                <span className="dot" />
                {health}
              </span>
            </div>
          </div>
          <p className="muted node-detail-subtitle">
            id: <code>{node.id}</code>
          </p>
          <p className="muted node-detail-subtitle os-row">
            {agentOs ?? "Unknown OS"}
            {agentOsIcon ? <FontAwesomeIcon icon={agentOsIcon} className="os-icon" /> : null}
          </p>
          <div className="node-meta-grid">
            <div className="node-meta-item">
              <span className="node-meta-label">Pair Code</span>
              <strong className="node-meta-value">
                <code>{node.pair_code}</code>
              </strong>
            </div>
            <div className="node-meta-item">
              <span className="node-meta-label">Created</span>
              <strong className="node-meta-value">{formatTimestamp(node.created_at)}</strong>
            </div>
            <div className="node-meta-item">
              <span className="node-meta-label">Paired At</span>
              <strong className="node-meta-value">{formatTimestamp(node.paired_at)}</strong>
            </div>
          </div>
        </div>
        <div className="node-detail-status">
          <p className="muted node-health-meta">
            Last heartbeat: {formatTimestamp(node.last_heartbeat_at)} ({heartbeatAge})
          </p>
        </div>
      </div>

      <section className="usage-card">
        <div className="usage-card-header">
          <h3>Resource Usage</h3>
          <span className="muted usage-updated">Updated: {metricsUpdatedAt}</span>
        </div>
        <div className="usage-metrics-grid">
          <MetricBar label="CPU" percent={cpuPercent} detail="Processor load" />
          <MetricBar
            label="Memory"
            percent={memoryPercent}
            detail={usageDetail(memoryUsedBytes, memoryTotalBytes)}
          />
          <MetricBar
            label="Storage"
            percent={storagePercent}
            detail={usageDetail(storageUsedBytes, storageTotalBytes)}
          />
        </div>
      </section>

      <section className="log-card">
        <div className="log-card-header">
          <div className="log-card-heading">
            <h3>Log</h3>
            {streamConnected && node.state !== "pending" ? (
              <span className="health health-healthy">
                <span className="dot" />
                live
              </span>
            ) : null}
          </div>
          <button
            type="button"
            className="log-refresh-button"
            onClick={() => {
              setUsePolling(false);
              setLogsError(null);
              setStreamRevision((value) => value + 1);
            }}
            disabled={logsLoading}
          >
            <FontAwesomeIcon icon={faRotateRight} />
            <span>Refresh</span>
          </button>
        </div>

        {logsError ? <p className="error">{logsError}</p> : null}

        <div className="log-stream" ref={logsContainerRef}>
          {logsLoading && logs.length === 0 ? (
            <p className="muted">Loading logs...</p>
          ) : null}
          {!logsLoading && logs.length === 0 ? <p className="muted">No logs available yet.</p> : null}
          {logs.length > 0 ? (
            <ul className="log-lines">
              {logs.map((entry) => {
                const hostname = getMetaHostname(entry.meta);
                return (
                  <li key={entry.id} className="log-line">
                    <span className="log-time">{formatLogTime(entry.created_at)}</span>
                    <span className={`log-level log-level-${entry.level}`}>{entry.level}</span>
                    <span className="log-message">
                      {entry.message}
                      {hostname ? <span className="log-meta"> [{hostname}]</span> : null}
                    </span>
                  </li>
                );
              })}
            </ul>
          ) : null}
        </div>
      </section>

      {renameOpen ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!renaming) {
              setRenameOpen(false);
              setRenameError(null);
            }
          }}
        >
          <div
            className="modal-card"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Rename Node</h2>
            <p className="muted">Choose a new name for this node.</p>
            <label className="form-label" htmlFor="node-detail-rename">
              Node Name
            </label>
            <input
              id="node-detail-rename"
              value={renameValue}
              onChange={(event) => setRenameValue(event.target.value)}
              disabled={renaming}
              autoFocus
            />
            {renameError ? <p className="error">{renameError}</p> : null}
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={renaming}
                onClick={() => {
                  setRenameOpen(false);
                  setRenameError(null);
                }}
              >
                Cancel
              </button>
              <button type="button" disabled={renaming} onClick={() => void confirmRename()}>
                {renaming ? "Saving..." : "Save Name"}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {confirmDeleteOpen ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!deleting) {
              setConfirmDeleteOpen(false);
              setDeleteError(null);
            }
          }}
        >
          <div
            className="modal-card"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Delete Node</h2>
            <p className="muted">
              Are you sure you want to delete <strong>{node.name}</strong>? This cannot be undone.
            </p>
            {deleteError ? <p className="error">{deleteError}</p> : null}
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={deleting}
                onClick={() => {
                  setConfirmDeleteOpen(false);
                  setDeleteError(null);
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger-button"
                disabled={deleting}
                onClick={() => void confirmDelete()}
              >
                {deleting ? "Deleting..." : "Confirm Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
