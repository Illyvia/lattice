import { useEffect, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faPlay,
  faPlus,
  faPowerOff,
  faRotateRight,
  faTrashCan,
} from "@fortawesome/free-solid-svg-icons";
import { Id as ToastId, toast } from "react-toastify";
import { useNavigate } from "react-router-dom";
import {
  ContainerOperationRecord,
  NodeContainerRecord,
  NodeRecord,
} from "../types";
import { formatTimestamp } from "../utils/health";

type NodeContainersPanelProps = {
  node: NodeRecord;
  apiBaseUrl: string;
};

type ContainerAction = "start" | "stop" | "restart" | "delete";

type CreateContainerForm = {
  name: string;
  image: string;
  command_text: string;
};

const DEFAULT_FORM: CreateContainerForm = {
  name: "",
  image: "ubuntu:24.04",
  command_text: "sleep infinity",
};

type PendingContainerToast = {
  toastId: ToastId;
  containerId: string;
  operationType: string;
};

function containerApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export default function NodeContainersPanel({ node, apiBaseUrl }: NodeContainersPanelProps) {
  const navigate = useNavigate();
  const containerCapability = node.capabilities?.container;
  const containerReady = node.state === "paired" && containerCapability?.ready === true;
  const [containers, setContainers] = useState<NodeContainerRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateContainerForm>(DEFAULT_FORM);
  const [createBusy, setCreateBusy] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [actionBusyKey, setActionBusyKey] = useState<string | null>(null);
  const [confirmDeleteContainer, setConfirmDeleteContainer] = useState<NodeContainerRecord | null>(null);
  const pendingOperationToastsRef = useRef<Map<string, PendingContainerToast>>(new Map());
  const pendingDeleteToastsRef = useRef<Map<string, ToastId>>(new Map());
  const pendingOperationTimeoutsRef = useRef<Map<string, number>>(new Map());

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const response = await fetch(
          containerApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/containers`),
          { cache: "no-store" }
        );
        if (!response.ok) {
          throw new Error(`Failed to load containers (${response.status})`);
        }
        const payload = (await response.json()) as NodeContainerRecord[];
        if (cancelled) {
          return;
        }
        setContainers(payload);
        setError(null);
      } catch (err) {
        if (cancelled) {
          return;
        }
        setError(err instanceof Error ? err.message : "Failed to load containers");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    void load();
    const timer = window.setInterval(load, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, node.id]);

  useEffect(() => {
    const currentContainerIds = new Set(containers.map((container) => container.id));

    for (const container of containers) {
      const op = container.last_operation;
      if (!op) {
        continue;
      }
      const pending = pendingOperationToastsRef.current.get(op.id);
      if (!pending) {
        continue;
      }
      if (op.status === "succeeded") {
        toast.update(pending.toastId, {
          render: `Container ${op.operation_type} completed`,
          type: "success",
          isLoading: false,
          autoClose: 4200,
          closeButton: true,
        });
        pendingOperationToastsRef.current.delete(op.id);
        const timeoutId = pendingOperationTimeoutsRef.current.get(op.id);
        if (typeof timeoutId === "number") {
          window.clearTimeout(timeoutId);
        }
        pendingOperationTimeoutsRef.current.delete(op.id);
        if (op.operation_type === "delete") {
          pendingDeleteToastsRef.current.delete(pending.containerId);
        }
        continue;
      }
      if (op.status === "failed") {
        toast.update(pending.toastId, {
          render: op.error
            ? `Container ${op.operation_type} failed: ${op.error}`
            : `Container ${op.operation_type} failed`,
          type: "error",
          isLoading: false,
          autoClose: 7000,
          closeButton: true,
        });
        pendingOperationToastsRef.current.delete(op.id);
        const timeoutId = pendingOperationTimeoutsRef.current.get(op.id);
        if (typeof timeoutId === "number") {
          window.clearTimeout(timeoutId);
        }
        pendingOperationTimeoutsRef.current.delete(op.id);
        if (op.operation_type === "delete") {
          pendingDeleteToastsRef.current.delete(pending.containerId);
        }
      }
    }

    for (const [containerId, toastId] of pendingDeleteToastsRef.current.entries()) {
      if (!currentContainerIds.has(containerId)) {
        toast.update(toastId, {
          render: "Container deleted",
          type: "success",
          isLoading: false,
          autoClose: 4200,
          closeButton: true,
        });
        pendingDeleteToastsRef.current.delete(containerId);
        for (const [operationId, pending] of pendingOperationToastsRef.current.entries()) {
          if (pending.containerId === containerId && pending.operationType === "delete") {
            pendingOperationToastsRef.current.delete(operationId);
            const timeoutId = pendingOperationTimeoutsRef.current.get(operationId);
            if (typeof timeoutId === "number") {
              window.clearTimeout(timeoutId);
            }
            pendingOperationTimeoutsRef.current.delete(operationId);
          }
        }
      }
    }
  }, [containers]);

  useEffect(() => {
    return () => {
      for (const timeoutId of pendingOperationTimeoutsRef.current.values()) {
        window.clearTimeout(timeoutId);
      }
      pendingOperationTimeoutsRef.current.clear();
      pendingOperationToastsRef.current.clear();
      pendingDeleteToastsRef.current.clear();
    };
  }, []);

  function validateCreateForm(form: CreateContainerForm): string | null {
    if (!/^[a-z0-9-]{3,32}$/.test(form.name.trim())) {
      return "Container name must be lowercase alphanumeric with dashes (3-32 chars).";
    }
    if (!form.image.trim()) {
      return "Container image is required.";
    }
    return null;
  }

  function registerPendingOperationToast(
    containerRecord: NodeContainerRecord,
    fallbackOperationType: string,
    operation: ContainerOperationRecord | null = null
  ): void {
    const op = operation ?? containerRecord.last_operation ?? null;
    const operationType = op?.operation_type ?? fallbackOperationType;
    const status = op?.status;

    if (op?.id && (status === "queued" || status === "running")) {
      const existing = pendingOperationToastsRef.current.get(op.id);
      if (existing) {
        return;
      }
      const toastId = toast.loading(`Container ${operationType} in progress...`);
      pendingOperationToastsRef.current.set(op.id, {
        toastId,
        containerId: containerRecord.id,
        operationType,
      });
      const timeoutId = window.setTimeout(() => {
        const pending = pendingOperationToastsRef.current.get(op.id);
        if (!pending) {
          return;
        }
        toast.update(pending.toastId, {
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
        pendingDeleteToastsRef.current.set(containerRecord.id, toastId);
      }
      return;
    }

    if (status === "succeeded") {
      toast.success(`Container ${operationType} completed`);
      return;
    }
    if (status === "failed") {
      toast.error(op?.error ? `Container ${operationType} failed: ${op.error}` : `Container ${operationType} failed`);
      return;
    }

    toast.info(`Container ${operationType} request accepted`);
  }

  async function submitCreate() {
    const validationError = validateCreateForm(createForm);
    if (validationError) {
      setCreateError(validationError);
      return;
    }
    setCreateBusy(true);
    try {
      const response = await fetch(
        containerApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/containers`),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name: createForm.name.trim(),
            image: createForm.image.trim(),
            command_text: createForm.command_text.trim() || null,
          }),
        }
      );
      const body = (await response.json().catch(() => ({}))) as {
        container?: NodeContainerRecord;
        operation?: ContainerOperationRecord;
        error?: string;
      };
      if (!response.ok) {
        throw new Error(body.error ?? `Failed to queue container create (${response.status})`);
      }
      if (body.container) {
        const nextContainer = body.container as NodeContainerRecord;
        setContainers((current) => [nextContainer, ...current.filter((item) => item.id !== nextContainer.id)]);
        registerPendingOperationToast(nextContainer, "create", body.operation ?? null);
      } else {
        toast.info("Container create request accepted");
      }
      setCreateOpen(false);
      setCreateForm(DEFAULT_FORM);
      setCreateError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to queue container create";
      setCreateError(message);
      toast.error(message);
    } finally {
      setCreateBusy(false);
    }
  }

  async function runContainerAction(container: NodeContainerRecord, action: ContainerAction) {
    const busyKey = `${container.id}:${action}`;
    setActionBusyKey(busyKey);
    try {
      const response = await fetch(
        containerApiUrl(
          apiBaseUrl,
          `/api/nodes/${encodeURIComponent(node.id)}/containers/${encodeURIComponent(
            container.id
          )}/actions/${action}`
        ),
        { method: "POST", headers: { "Content-Type": "application/json" } }
      );
      const body = (await response.json().catch(() => ({}))) as {
        container?: NodeContainerRecord;
        operation?: ContainerOperationRecord;
        error?: string;
      };
      if (!response.ok) {
        throw new Error(body.error ?? `Failed container ${action} (${response.status})`);
      }
      if (body.container) {
        const nextContainer = body.container as NodeContainerRecord;
        setContainers((current) =>
          current.map((item) => (item.id === nextContainer.id ? nextContainer : item))
        );
        registerPendingOperationToast(nextContainer, action, body.operation ?? null);
      } else {
        toast.info(`Container ${action} request accepted`);
      }
      if (action === "delete") {
        setConfirmDeleteContainer(null);
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed container ${action}`);
    } finally {
      setActionBusyKey(null);
    }
  }

  return (
    <section className="vm-panel">
      <div className="vm-panel-header">
        <h3>Containers</h3>
        <button
          type="button"
          className="secondary-button"
          disabled={!containerReady}
          onClick={() => {
            setCreateOpen(true);
            setCreateForm(DEFAULT_FORM);
            setCreateError(null);
          }}
        >
          <FontAwesomeIcon icon={faPlus} />
          <span>Create Container</span>
        </button>
      </div>
      {!containerReady ? (
        <p className="muted">{containerCapability?.message ?? "Container capability is not ready on this node."}</p>
      ) : null}
      {error ? <p className="error">{error}</p> : null}
      <div className="table-wrap">
        <table className="vm-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>State</th>
              <th>Image</th>
              <th>Runtime</th>
              <th>IP</th>
              <th>Ports</th>
              <th>Last Op</th>
              <th>Updated</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {containers.map((container) => {
              const isRunning = container.state === "running";
              return (
              <tr
                key={container.id}
                className="clickable-row"
                onClick={() =>
                  navigate(
                    `/node/${encodeURIComponent(node.id)}/container/${encodeURIComponent(container.id)}`
                  )
                }
              >
                <td>{container.name}</td>
                <td>
                  <span className={`badge vm-state-${container.state}`}>{container.state}</span>
                </td>
                <td>{container.image}</td>
                <td>{container.runtime_name}</td>
                <td>{container.ip_address || "-"}</td>
                <td>{container.published_ports || "-"}</td>
                <td>
                  {container.last_operation
                    ? `${container.last_operation.operation_type}:${container.last_operation.status}`
                    : "-"}
                </td>
                <td>{formatTimestamp(container.updated_at)}</td>
                <td className="vm-actions-cell">
                  {!isRunning ? (
                    <button
                      type="button"
                      className="icon-button"
                      disabled={actionBusyKey !== null}
                      onClick={(event) => {
                        event.stopPropagation();
                        void runContainerAction(container, "start");
                      }}
                    >
                      <FontAwesomeIcon icon={faPlay} />
                    </button>
                  ) : null}
                  {isRunning ? (
                    <button
                      type="button"
                      className="icon-button"
                      disabled={actionBusyKey !== null}
                      onClick={(event) => {
                        event.stopPropagation();
                        void runContainerAction(container, "stop");
                      }}
                    >
                      <FontAwesomeIcon icon={faPowerOff} />
                    </button>
                  ) : null}
                  <button
                    type="button"
                    className="icon-button"
                    disabled={actionBusyKey !== null}
                    onClick={(event) => {
                      event.stopPropagation();
                      void runContainerAction(container, "restart");
                    }}
                  >
                    <FontAwesomeIcon icon={faRotateRight} />
                  </button>
                  <button
                    type="button"
                    className="icon-button vm-action-danger"
                    disabled={actionBusyKey !== null}
                    onClick={(event) => {
                      event.stopPropagation();
                      setConfirmDeleteContainer(container);
                    }}
                  >
                    <FontAwesomeIcon icon={faTrashCan} />
                  </button>
                </td>
              </tr>
              );
            })}
            {!loading && containers.length === 0 ? (
              <tr>
                <td colSpan={9} className="empty">
                  No containers yet.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      {createOpen ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!createBusy) {
              setCreateOpen(false);
            }
          }}
        >
          <div
            className="modal-card vm-create-modal"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Create Container</h2>
            <div className="vm-form-grid">
              <label className="vm-field">
                <span className="vm-field-label">Container Name</span>
                <input
                  value={createForm.name}
                  onChange={(event) => setCreateForm((current) => ({ ...current, name: event.target.value }))}
                  placeholder="my-container"
                />
              </label>
              <label className="vm-field">
                <span className="vm-field-label">Image</span>
                <input
                  value={createForm.image}
                  onChange={(event) => setCreateForm((current) => ({ ...current, image: event.target.value }))}
                  placeholder="ubuntu:24.04"
                />
              </label>
              <label className="vm-field">
                <span className="vm-field-label">Command (optional)</span>
                <input
                  value={createForm.command_text}
                  onChange={(event) =>
                    setCreateForm((current) => ({ ...current, command_text: event.target.value }))
                  }
                  placeholder="sleep infinity"
                />
              </label>
            </div>
            {createError ? <p className="error">{createError}</p> : null}
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={createBusy}
                onClick={() => setCreateOpen(false)}
              >
                Cancel
              </button>
              <button type="button" disabled={createBusy} onClick={() => void submitCreate()}>
                {createBusy ? "Queuing..." : "Create Container"}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {confirmDeleteContainer ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (actionBusyKey === null) {
              setConfirmDeleteContainer(null);
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
              Delete <strong>{confirmDeleteContainer.name}</strong>? This cannot be undone.
            </p>
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={actionBusyKey !== null}
                onClick={() => setConfirmDeleteContainer(null)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger-button"
                disabled={actionBusyKey !== null}
                onClick={() => void runContainerAction(confirmDeleteContainer, "delete")}
              >
                Confirm Delete
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
