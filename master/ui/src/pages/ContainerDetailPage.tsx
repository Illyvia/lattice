import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faArrowLeft,
  faPlay,
  faPowerOff,
  faRotateRight,
  faTrashCan,
} from "@fortawesome/free-solid-svg-icons";
import { Id as ToastId, toast } from "react-toastify";
import { useNavigate, useParams } from "react-router-dom";
import ContainerLogsPanel from "../components/ContainerLogsPanel";
import ContainerTerminalPanel from "../components/ContainerTerminalPanel";
import { ContainerOperationRecord, NodeContainerRecord, NodeRecord } from "../types";
import { formatTimestamp } from "../utils/health";

type ContainerDetailPageProps = {
  nodes: NodeRecord[];
  apiBaseUrl: string;
};

type ContainerAction = "start" | "stop" | "restart" | "delete";

function containerApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export default function ContainerDetailPage({ nodes, apiBaseUrl }: ContainerDetailPageProps) {
  const navigate = useNavigate();
  const { nodeId, containerId } = useParams();
  const cleanNodeId = (nodeId || "").trim();
  const cleanContainerId = (containerId || "").trim();
  const node = nodes.find((candidate) => candidate.id === cleanNodeId) ?? null;

  const [container, setContainer] = useState<NodeContainerRecord | null>(null);
  const [operations, setOperations] = useState<ContainerOperationRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [opsLoading, setOpsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [opsError, setOpsError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState<ContainerAction | null>(null);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"main" | "logs" | "terminal">("main");
  const [refreshNonce, setRefreshNonce] = useState(0);
  const pendingOperationToastsRef = useRef<Map<string, ToastId>>(new Map());
  const pendingOperationTimeoutsRef = useRef<Map<string, number>>(new Map());
  const pendingDeleteToastRef = useRef<{ toastId: ToastId; containerId: string } | null>(null);

  const containerActionsDisabled = useMemo(
    () => actionBusy !== null || !container,
    [actionBusy, container]
  );
  const isContainerRunning = container?.state === "running";

  function registerPendingOperationToast(
    nextContainer: NodeContainerRecord,
    fallbackAction: ContainerAction,
    operation: ContainerOperationRecord | null = null
  ): void {
    const op = operation ?? nextContainer.last_operation ?? null;
    const operationType = op?.operation_type ?? fallbackAction;
    const status = op?.status;

    if (op?.id && (status === "queued" || status === "running")) {
      if (pendingOperationToastsRef.current.has(op.id)) {
        return;
      }
      const toastId = toast.loading(`Container ${operationType} in progress...`);
      pendingOperationToastsRef.current.set(op.id, toastId);
      const timeoutId = window.setTimeout(() => {
        const pendingToastId = pendingOperationToastsRef.current.get(op.id);
        if (!pendingToastId) {
          return;
        }
        toast.update(pendingToastId, {
          render: `Container ${operationType} is still in progress. Check operation timeline for updates.`,
          type: "info",
          isLoading: false,
          autoClose: 6000,
          closeButton: true,
        });
        pendingOperationToastsRef.current.delete(op.id);
        pendingOperationTimeoutsRef.current.delete(op.id);
      }, 180000);
      pendingOperationTimeoutsRef.current.set(op.id, timeoutId);
      if (operationType === "delete") {
        pendingDeleteToastRef.current = { toastId, containerId: nextContainer.id };
      }
      return;
    }

    if (status === "succeeded") {
      toast.success(`Container ${operationType} completed`);
      return;
    }
    if (status === "failed") {
      toast.error(
        op?.error ? `Container ${operationType} failed: ${op.error}` : `Container ${operationType} failed`
      );
      return;
    }

    toast.info(`Container ${operationType} request accepted`);
  }

  useEffect(() => {
    setActiveTab("main");
    setConfirmDeleteOpen(false);
  }, [cleanNodeId, cleanContainerId]);

  useEffect(() => {
    if (!cleanNodeId || !cleanContainerId) {
      setContainer(null);
      setOperations([]);
      return;
    }

    let cancelled = false;
    async function loadContainer() {
      setLoading(true);
      try {
        const resp = await fetch(
          containerApiUrl(
            apiBaseUrl,
            `/api/nodes/${encodeURIComponent(cleanNodeId)}/containers/${encodeURIComponent(cleanContainerId)}`
          ),
          { cache: "no-store" }
        );
        if (resp.status === 404 && pendingDeleteToastRef.current) {
          const pendingDelete = pendingDeleteToastRef.current;
          toast.update(pendingDelete.toastId, {
            render: "Container deleted",
            type: "success",
            isLoading: false,
            autoClose: 4200,
            closeButton: true,
          });
          pendingDeleteToastRef.current = null;
          for (const timeoutId of pendingOperationTimeoutsRef.current.values()) {
            window.clearTimeout(timeoutId);
          }
          pendingOperationTimeoutsRef.current.clear();
          pendingOperationToastsRef.current.clear();
          navigate(`/node/${encodeURIComponent(cleanNodeId)}?tab=containers`);
          return;
        }
        const body = (await resp.json().catch(() => ({}))) as { error?: string };
        if (!resp.ok) {
          throw new Error(body.error ?? `Failed to load container (${resp.status})`);
        }
        if (!cancelled) {
          setContainer(body as unknown as NodeContainerRecord);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load container");
          setContainer(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadContainer();
    const timer = window.setInterval(loadContainer, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, cleanNodeId, cleanContainerId, refreshNonce, navigate]);

  useEffect(() => {
    if (!cleanNodeId || !cleanContainerId) {
      return;
    }

    let cancelled = false;
    async function loadOps() {
      setOpsLoading(true);
      try {
        const resp = await fetch(
          containerApiUrl(
            apiBaseUrl,
            `/api/nodes/${encodeURIComponent(cleanNodeId)}/containers/${encodeURIComponent(
              cleanContainerId
            )}/operations?limit=50`
          ),
          { cache: "no-store" }
        );
        const body = (await resp.json().catch(() => ({}))) as { error?: string };
        if (!resp.ok) {
          throw new Error(body.error ?? `Failed to load container operations (${resp.status})`);
        }
        if (!cancelled) {
          setOperations(body as unknown as ContainerOperationRecord[]);
          setOpsError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setOpsError(err instanceof Error ? err.message : "Failed to load container operations");
        }
      } finally {
        if (!cancelled) {
          setOpsLoading(false);
        }
      }
    }

    void loadOps();
    const timer = window.setInterval(loadOps, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, cleanNodeId, cleanContainerId, refreshNonce]);

  useEffect(() => {
    for (const operation of operations) {
      const toastId = pendingOperationToastsRef.current.get(operation.id);
      if (!toastId) {
        continue;
      }
      if (operation.status === "succeeded") {
        toast.update(toastId, {
          render: `Container ${operation.operation_type} completed`,
          type: "success",
          isLoading: false,
          autoClose: 4200,
          closeButton: true,
        });
        pendingOperationToastsRef.current.delete(operation.id);
        const timeoutId = pendingOperationTimeoutsRef.current.get(operation.id);
        if (typeof timeoutId === "number") {
          window.clearTimeout(timeoutId);
        }
        pendingOperationTimeoutsRef.current.delete(operation.id);
        if (operation.operation_type === "delete") {
          pendingDeleteToastRef.current = null;
          navigate(`/node/${encodeURIComponent(cleanNodeId)}?tab=containers`);
        }
        continue;
      }
      if (operation.status === "failed") {
        toast.update(toastId, {
          render:
            operation.error
              ? `Container ${operation.operation_type} failed: ${operation.error}`
              : `Container ${operation.operation_type} failed`,
          type: "error",
          isLoading: false,
          autoClose: 7000,
          closeButton: true,
        });
        pendingOperationToastsRef.current.delete(operation.id);
        const timeoutId = pendingOperationTimeoutsRef.current.get(operation.id);
        if (typeof timeoutId === "number") {
          window.clearTimeout(timeoutId);
        }
        pendingOperationTimeoutsRef.current.delete(operation.id);
        if (operation.operation_type === "delete") {
          pendingDeleteToastRef.current = null;
        }
      }
    }
  }, [cleanNodeId, navigate, operations]);

  useEffect(() => {
    return () => {
      for (const timeoutId of pendingOperationTimeoutsRef.current.values()) {
        window.clearTimeout(timeoutId);
      }
      pendingOperationTimeoutsRef.current.clear();
      pendingOperationToastsRef.current.clear();
      pendingDeleteToastRef.current = null;
    };
  }, []);

  async function runContainerAction(action: ContainerAction) {
    if (!container || !cleanNodeId || !cleanContainerId) {
      return;
    }
    setActionBusy(action);
    try {
      const resp = await fetch(
        containerApiUrl(
          apiBaseUrl,
          `/api/nodes/${encodeURIComponent(cleanNodeId)}/containers/${encodeURIComponent(
            cleanContainerId
          )}/actions/${action}`
        ),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        }
      );
      const body = (await resp.json().catch(() => ({}))) as {
        container?: NodeContainerRecord;
        operation?: ContainerOperationRecord;
        error?: string;
      };
      if (!resp.ok) {
        throw new Error(body.error ?? `Failed container ${action} (${resp.status})`);
      }
      if (body.container) {
        const nextContainer = body.container;
        setContainer(nextContainer);
        registerPendingOperationToast(nextContainer, action, body.operation ?? null);
      } else {
        toast.info(`Container ${action} request accepted`);
      }
      if (action === "delete") {
        setConfirmDeleteOpen(false);
        return;
      }
      setRefreshNonce((value) => value + 1);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed container ${action}`);
    } finally {
      setActionBusy(null);
      if (action === "delete") {
        setConfirmDeleteOpen(false);
      }
    }
  }

  if (!cleanNodeId || !cleanContainerId) {
    return (
      <section>
        <button type="button" className="secondary-button" onClick={() => navigate("/nodes")}>
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Nodes</span>
        </button>
        <p className="muted">Container not found.</p>
      </section>
    );
  }

  return (
    <section>
      <h2 className="node-page-heading">{container?.name ?? "Container"}</h2>
      <div className="row">
        <button
          type="button"
          className="secondary-button"
          onClick={() => navigate(`/node/${encodeURIComponent(cleanNodeId)}?tab=containers`)}
        >
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Node</span>
        </button>
        {!isContainerRunning ? (
          <button
            type="button"
            className="secondary-button"
            disabled={containerActionsDisabled}
            onClick={() => void runContainerAction("start")}
          >
            <FontAwesomeIcon icon={faPlay} />
            <span>Start</span>
          </button>
        ) : null}
        {isContainerRunning ? (
          <button
            type="button"
            className="secondary-button"
            disabled={containerActionsDisabled}
            onClick={() => void runContainerAction("stop")}
          >
            <FontAwesomeIcon icon={faPowerOff} />
            <span>Stop</span>
          </button>
        ) : null}
        <button
          type="button"
          className="secondary-button"
          disabled={containerActionsDisabled}
          onClick={() => void runContainerAction("restart")}
        >
          <FontAwesomeIcon icon={faRotateRight} />
          <span>Restart</span>
        </button>
        <button
          type="button"
          className="node-delete-button"
          disabled={containerActionsDisabled}
          onClick={() => setConfirmDeleteOpen(true)}
        >
          <FontAwesomeIcon icon={faTrashCan} />
          <span>Delete Container</span>
        </button>
      </div>

      <div className="node-tabs" role="tablist" aria-label="Container Detail Tabs">
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "main"}
          className={`node-tab ${activeTab === "main" ? "node-tab-active" : ""}`}
          onClick={() => setActiveTab("main")}
        >
          Main
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "logs"}
          className={`node-tab ${activeTab === "logs" ? "node-tab-active" : ""}`}
          onClick={() => setActiveTab("logs")}
        >
          Logs
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === "terminal"}
          className={`node-tab ${activeTab === "terminal" ? "node-tab-active" : ""}`}
          onClick={() => setActiveTab("terminal")}
        >
          Terminal
        </button>
      </div>

      {error ? <p className="error">{error}</p> : null}
      {loading && !container ? <p className="muted">Loading container...</p> : null}

      {activeTab === "main" && container ? (
        <>
          <div className="node-detail-header">
            <div className="node-detail-main">
              <div className="node-detail-title-row">
                <p className="stat-label">Container Details</p>
                <span className={`badge vm-state-${container.state}`}>{container.state}</span>
              </div>
              <p className="muted node-detail-subtitle">
                id: <code>{container.id}</code>
              </p>
              <p className="muted node-detail-subtitle">
                runtime: <code>{container.runtime_name}</code>
              </p>
              <p className="muted node-detail-subtitle">node: {node?.name ?? cleanNodeId}</p>
              <div className="node-meta-grid">
                <div className="node-meta-item">
                  <span className="node-meta-label">Image</span>
                  <strong className="node-meta-value">{container.image}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Provider</span>
                  <strong className="node-meta-value">{container.provider}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Runtime ID</span>
                  <strong className="node-meta-value">
                    {container.runtime_id ? <code>{container.runtime_id.slice(0, 12)}</code> : "-"}
                  </strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">IP Address</span>
                  <strong className="node-meta-value">{container.ip_address || "-"}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Published Ports</span>
                  <strong className="node-meta-value">{container.published_ports || "-"}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Command</span>
                  <strong className="node-meta-value">{container.command_text || "-"}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Created</span>
                  <strong className="node-meta-value">{formatTimestamp(container.created_at)}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Updated</span>
                  <strong className="node-meta-value">{formatTimestamp(container.updated_at)}</strong>
                </div>
              </div>
              {container.last_error ? <p className="error">Last error: {container.last_error}</p> : null}
            </div>
          </div>

          <section className="vm-ops-panel">
            <h4>Operation Timeline: {container.name}</h4>
            {opsError ? <p className="error">{opsError}</p> : null}
            <ul className="vm-ops-list">
              {operations.map((op) => (
                <li key={op.id}>
                  <strong>{op.operation_type}</strong>
                  <span className={`badge vm-op-${op.status}`}>{op.status}</span>
                  <span className="muted">{formatTimestamp(op.created_at)}</span>
                  {op.error ? <span className="error"> {op.error}</span> : null}
                </li>
              ))}
            </ul>
            {opsLoading && operations.length === 0 ? <p className="muted">Loading operations...</p> : null}
          </section>
        </>
      ) : null}

      {activeTab === "logs" && container ? (
        <ContainerLogsPanel
          nodeId={cleanNodeId}
          containerId={cleanContainerId}
          containerName={container.name}
          apiBaseUrl={apiBaseUrl}
        />
      ) : null}

      {activeTab === "terminal" && container ? (
        <ContainerTerminalPanel
          nodeId={cleanNodeId}
          containerId={cleanContainerId}
          containerName={container.name}
          apiBaseUrl={apiBaseUrl}
        />
      ) : null}

      {confirmDeleteOpen && container ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!containerActionsDisabled) {
              setConfirmDeleteOpen(false);
            }
          }}
        >
          <div
            className="modal-card"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Delete Container</h2>
            <p className="muted">
              Are you sure you want to delete <strong>{container.name}</strong>? This cannot be undone.
            </p>
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={containerActionsDisabled}
                onClick={() => setConfirmDeleteOpen(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger-button"
                disabled={containerActionsDisabled}
                onClick={() => void runContainerAction("delete")}
              >
                {actionBusy === "delete" ? "Deleting..." : "Confirm Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
