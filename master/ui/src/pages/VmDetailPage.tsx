import { useEffect, useMemo, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faArrowLeft,
  faPlay,
  faPowerOff,
  faRotateRight,
  faTrashCan,
} from "@fortawesome/free-solid-svg-icons";
import { useNavigate, useParams } from "react-router-dom";
import { toast } from "react-toastify";
import VmTerminalPanel from "../components/VmTerminalPanel";
import { NodeRecord, NodeVmRecord, VmOperationRecord } from "../types";
import { formatTimestamp } from "../utils/health";

type VmDetailPageProps = {
  nodes: NodeRecord[];
  apiBaseUrl: string;
};

type VmAction = "start" | "stop" | "reboot" | "delete";

function vmApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export default function VmDetailPage({ nodes, apiBaseUrl }: VmDetailPageProps) {
  const navigate = useNavigate();
  const { nodeId, vmId } = useParams();
  const cleanNodeId = (nodeId || "").trim();
  const cleanVmId = (vmId || "").trim();
  const node = nodes.find((candidate) => candidate.id === cleanNodeId) ?? null;

  const [vm, setVm] = useState<NodeVmRecord | null>(null);
  const [operations, setOperations] = useState<VmOperationRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [opsLoading, setOpsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [opsError, setOpsError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState<VmAction | null>(null);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"main" | "terminal">("main");
  const [refreshNonce, setRefreshNonce] = useState(0);

  const vmActionsDisabled = useMemo(() => actionBusy !== null || !vm, [actionBusy, vm]);

  useEffect(() => {
    setActiveTab("main");
    setConfirmDeleteOpen(false);
  }, [cleanNodeId, cleanVmId]);

  useEffect(() => {
    if (!cleanNodeId || !cleanVmId) {
      setVm(null);
      setOperations([]);
      return;
    }

    let cancelled = false;
    async function loadVm() {
      setLoading(true);
      try {
        const resp = await fetch(
          vmApiUrl(
            apiBaseUrl,
            `/api/nodes/${encodeURIComponent(cleanNodeId)}/vms/${encodeURIComponent(cleanVmId)}`
          ),
          { cache: "no-store" }
        );
        const body = (await resp.json().catch(() => ({}))) as { error?: string };
        if (!resp.ok) {
          throw new Error(body.error ?? `Failed to load VM (${resp.status})`);
        }
        if (!cancelled) {
          setVm(body as unknown as NodeVmRecord);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load VM");
          setVm(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadVm();
    const timer = window.setInterval(loadVm, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, cleanNodeId, cleanVmId, refreshNonce]);

  useEffect(() => {
    if (!cleanNodeId || !cleanVmId) {
      return;
    }

    let cancelled = false;
    async function loadOps() {
      setOpsLoading(true);
      try {
        const resp = await fetch(
          vmApiUrl(
            apiBaseUrl,
            `/api/nodes/${encodeURIComponent(cleanNodeId)}/vms/${encodeURIComponent(
              cleanVmId
            )}/operations?limit=50`
          ),
          { cache: "no-store" }
        );
        const body = (await resp.json().catch(() => ({}))) as { error?: string };
        if (!resp.ok) {
          throw new Error(body.error ?? `Failed to load VM operations (${resp.status})`);
        }
        if (!cancelled) {
          setOperations(body as unknown as VmOperationRecord[]);
          setOpsError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setOpsError(err instanceof Error ? err.message : "Failed to load VM operations");
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
  }, [apiBaseUrl, cleanNodeId, cleanVmId, refreshNonce]);

  async function runVmAction(action: VmAction) {
    if (!vm || !cleanNodeId || !cleanVmId) {
      return;
    }
    setActionBusy(action);
    try {
      const resp = await fetch(
        vmApiUrl(
          apiBaseUrl,
          `/api/nodes/${encodeURIComponent(cleanNodeId)}/vms/${encodeURIComponent(
            cleanVmId
          )}/actions/${action}`
        ),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        }
      );
      const body = (await resp.json().catch(() => ({}))) as { vm?: NodeVmRecord; error?: string };
      if (!resp.ok) {
        throw new Error(body.error ?? `Failed VM ${action} (${resp.status})`);
      }
      if (body.vm) {
        setVm(body.vm);
      }
      if (action === "delete") {
        toast.success("VM delete queued");
        navigate(`/node/${encodeURIComponent(cleanNodeId)}?tab=vms`);
        return;
      }
      toast.info(`VM ${action} queued`);
      setRefreshNonce((value) => value + 1);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed VM ${action}`);
    } finally {
      setActionBusy(null);
      if (action === "delete") {
        setConfirmDeleteOpen(false);
      }
    }
  }

  if (!cleanNodeId || !cleanVmId) {
    return (
      <section>
        <button type="button" className="secondary-button" onClick={() => navigate("/nodes")}>
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Nodes</span>
        </button>
        <p className="muted">VM not found.</p>
      </section>
    );
  }

  return (
    <section>
      <h2 className="node-page-heading">{vm?.name ?? "VM"}</h2>
      <div className="row">
        <button
          type="button"
          className="secondary-button"
          onClick={() => navigate(`/node/${encodeURIComponent(cleanNodeId)}?tab=vms`)}
        >
          <FontAwesomeIcon icon={faArrowLeft} />
          <span>Back to Node</span>
        </button>
        <button
          type="button"
          className="secondary-button"
          disabled={vmActionsDisabled}
          onClick={() => void runVmAction("start")}
        >
          <FontAwesomeIcon icon={faPlay} />
          <span>Start</span>
        </button>
        <button
          type="button"
          className="secondary-button"
          disabled={vmActionsDisabled}
          onClick={() => void runVmAction("stop")}
        >
          <FontAwesomeIcon icon={faPowerOff} />
          <span>Stop</span>
        </button>
        <button
          type="button"
          className="secondary-button"
          disabled={vmActionsDisabled}
          onClick={() => void runVmAction("reboot")}
        >
          <FontAwesomeIcon icon={faRotateRight} />
          <span>Reboot</span>
        </button>
        <button
          type="button"
          className="node-delete-button"
          disabled={vmActionsDisabled}
          onClick={() => setConfirmDeleteOpen(true)}
        >
          <FontAwesomeIcon icon={faTrashCan} />
          <span>Delete VM</span>
        </button>
      </div>

      <div className="node-tabs" role="tablist" aria-label="VM Detail Tabs">
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
          aria-selected={activeTab === "terminal"}
          className={`node-tab ${activeTab === "terminal" ? "node-tab-active" : ""}`}
          onClick={() => setActiveTab("terminal")}
        >
          Terminal
        </button>
      </div>

      {error ? <p className="error">{error}</p> : null}
      {loading && !vm ? <p className="muted">Loading VM...</p> : null}

      {activeTab === "main" && vm ? (
        <>
          <div className="node-detail-header">
            <div className="node-detail-main">
              <div className="node-detail-title-row">
                <p className="stat-label">VM Details</p>
                <span className={`badge vm-state-${vm.state}`}>{vm.state}</span>
              </div>
              <p className="muted node-detail-subtitle">
                id: <code>{vm.id}</code>
              </p>
              <p className="muted node-detail-subtitle">
                domain: <code>{vm.domain_name}</code>
              </p>
              <p className="muted node-detail-subtitle">node: {node?.name ?? cleanNodeId}</p>
              <div className="node-meta-grid">
                <div className="node-meta-item">
                  <span className="node-meta-label">Image</span>
                  <strong className="node-meta-value">{vm.image_name}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">vCPU</span>
                  <strong className="node-meta-value">{vm.vcpu}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Memory</span>
                  <strong className="node-meta-value">{vm.memory_mb} MB</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Disk</span>
                  <strong className="node-meta-value">{vm.disk_gb} GB</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">IP Address</span>
                  <strong className="node-meta-value">{vm.ip_address ?? "-"}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Bridge</span>
                  <strong className="node-meta-value">{vm.bridge || "-"}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Created</span>
                  <strong className="node-meta-value">{formatTimestamp(vm.created_at)}</strong>
                </div>
                <div className="node-meta-item">
                  <span className="node-meta-label">Updated</span>
                  <strong className="node-meta-value">{formatTimestamp(vm.updated_at)}</strong>
                </div>
              </div>
              {vm.last_error ? <p className="error">Last error: {vm.last_error}</p> : null}
            </div>
          </div>

          <section className="vm-ops-panel">
            <h4>Operation Timeline: {vm.name}</h4>
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

      {activeTab === "terminal" && vm ? (
        <VmTerminalPanel nodeId={cleanNodeId} vmId={cleanVmId} vmName={vm.name} apiBaseUrl={apiBaseUrl} />
      ) : null}

      {confirmDeleteOpen && vm ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!vmActionsDisabled) {
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
            <h2>Delete VM</h2>
            <p className="muted">
              Are you sure you want to delete <strong>{vm.name}</strong>? This removes disks and cannot
              be undone.
            </p>
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={vmActionsDisabled}
                onClick={() => setConfirmDeleteOpen(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger-button"
                disabled={vmActionsDisabled}
                onClick={() => void runVmAction("delete")}
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

