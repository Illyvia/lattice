import { useEffect, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faPlay,
  faPlus,
  faPowerOff,
  faRotateRight,
  faTrashCan
} from "@fortawesome/free-solid-svg-icons";
import { Id as ToastId, toast } from "react-toastify";
import { useNavigate } from "react-router-dom";
import { NodeRecord, NodeVmRecord, VmImageRecord, VmOperationRecord } from "../types";
import { formatTimestamp } from "../utils/health";

type NodeVmsPanelProps = {
  node: NodeRecord;
  apiBaseUrl: string;
  openCreateIntent: number;
};

type VmAction = "start" | "stop" | "reboot" | "delete";

type CreateVmForm = {
  name: string;
  image_id: string;
  vcpu: string;
  memory_mb: string;
  disk_gb: string;
  bridge: string;
  guest_username: string;
  guest_password: string;
  guest_password_confirm: string;
};

const DEFAULT_FORM: CreateVmForm = {
  name: "",
  image_id: "",
  vcpu: "2",
  memory_mb: "2048",
  disk_gb: "20",
  bridge: "br0",
  guest_username: "",
  guest_password: "",
  guest_password_confirm: "",
};

type PendingVmToast = {
  toastId: ToastId;
  vmId: string;
  operationType: string;
};

function vmApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export default function NodeVmsPanel({ node, apiBaseUrl, openCreateIntent }: NodeVmsPanelProps) {
  const navigate = useNavigate();
  const vmCapability = node.capabilities?.vm;
  const vmReady = node.state === "paired" && vmCapability?.ready === true;
  const [images, setImages] = useState<VmImageRecord[]>([]);
  const [vms, setVms] = useState<NodeVmRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createStep, setCreateStep] = useState(0);
  const [createForm, setCreateForm] = useState<CreateVmForm>(DEFAULT_FORM);
  const [createBusy, setCreateBusy] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [actionBusyKey, setActionBusyKey] = useState<string | null>(null);
  const [confirmDeleteVm, setConfirmDeleteVm] = useState<NodeVmRecord | null>(null);
  const pendingOperationToastsRef = useRef<Map<string, PendingVmToast>>(new Map());
  const pendingDeleteToastsRef = useRef<Map<string, ToastId>>(new Map());
  const pendingOperationTimeoutsRef = useRef<Map<string, number>>(new Map());

  useEffect(() => {
    if (!vmReady || openCreateIntent <= 0) {
      return;
    }
    setCreateOpen(true);
    setCreateStep(0);
    setCreateForm(DEFAULT_FORM);
    setCreateError(null);
  }, [openCreateIntent, vmReady]);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const [imagesResp, vmsResp] = await Promise.all([
          fetch(vmApiUrl(apiBaseUrl, "/api/vm-images"), { cache: "no-store" }),
          fetch(vmApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/vms`), { cache: "no-store" }),
        ]);
        if (!imagesResp.ok) {
          throw new Error(`Failed to load VM images (${imagesResp.status})`);
        }
        if (!vmsResp.ok) {
          throw new Error(`Failed to load VMs (${vmsResp.status})`);
        }
        const imagesPayload = (await imagesResp.json()) as VmImageRecord[];
        const vmsPayload = (await vmsResp.json()) as NodeVmRecord[];
        if (cancelled) return;
        setImages(imagesPayload);
        setVms(vmsPayload);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load VMs");
      } finally {
        if (!cancelled) setLoading(false);
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
    const currentVmIds = new Set(vms.map((vm) => vm.id));

    for (const vm of vms) {
      const op = vm.last_operation;
      if (!op) {
        continue;
      }
      const pending = pendingOperationToastsRef.current.get(op.id);
      if (!pending) {
        continue;
      }
      if (op.status === "succeeded") {
        toast.update(pending.toastId, {
          render: `VM ${op.operation_type} completed`,
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
          pendingDeleteToastsRef.current.delete(pending.vmId);
        }
        continue;
      }
      if (op.status === "failed") {
        toast.update(pending.toastId, {
          render: op.error ? `VM ${op.operation_type} failed: ${op.error}` : `VM ${op.operation_type} failed`,
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
          pendingDeleteToastsRef.current.delete(pending.vmId);
        }
      }
    }

    for (const [vmId, toastId] of pendingDeleteToastsRef.current.entries()) {
      if (!currentVmIds.has(vmId)) {
        toast.update(toastId, {
          render: "VM deleted",
          type: "success",
          isLoading: false,
          autoClose: 4200,
          closeButton: true,
        });
        pendingDeleteToastsRef.current.delete(vmId);
        for (const [operationId, pending] of pendingOperationToastsRef.current.entries()) {
          if (pending.vmId === vmId && pending.operationType === "delete") {
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
  }, [vms]);

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

  function updateForm<K extends keyof CreateVmForm>(key: K, value: CreateVmForm[K]) {
    setCreateForm((current) => ({ ...current, [key]: value }));
    if (key === "image_id") {
      const selected = images.find((image) => image.id === value);
      if (selected && !createForm.guest_username.trim()) {
        setCreateForm((current) => ({ ...current, guest_username: selected.default_username }));
      }
    }
  }

  function validateCreateStep(step: number): string | null {
    if (step === 0 && !/^[a-z0-9-]{3,32}$/.test(createForm.name.trim())) return "VM name must be lowercase alphanumeric with dashes (3-32 chars).";
    if (step === 1 && !createForm.image_id.trim()) return "Select a VM image.";
    if (step === 2) {
      const vcpu = Number(createForm.vcpu);
      const memoryMb = Number(createForm.memory_mb);
      const diskGb = Number(createForm.disk_gb);
      if (!Number.isFinite(vcpu) || vcpu < 1 || vcpu > 32) return "vCPU must be 1-32.";
      if (!Number.isFinite(memoryMb) || memoryMb < 512 || memoryMb > 262144) return "Memory must be 512-262144 MB.";
      if (!Number.isFinite(diskGb) || diskGb < 10 || diskGb > 4096) return "Disk must be 10-4096 GB.";
    }
    if (step === 3 && !createForm.bridge.trim()) return "Bridge is required.";
    if (step === 4) {
      if (!createForm.guest_username.trim()) return "Guest username is required.";
      if (!createForm.guest_password.trim()) return "Guest password is required.";
      if (createForm.guest_password !== createForm.guest_password_confirm) return "Guest passwords do not match.";
    }
    return null;
  }

  function registerPendingOperationToast(
    vmRecord: NodeVmRecord,
    fallbackOperationType: string,
    operation: VmOperationRecord | null = null
  ): void {
    const op = operation ?? vmRecord.last_operation ?? null;
    const operationType = op?.operation_type ?? fallbackOperationType;
    const status = op?.status;

    if (op?.id && (status === "queued" || status === "running")) {
      const existing = pendingOperationToastsRef.current.get(op.id);
      if (existing) {
        return;
      }
      const toastId = toast.loading(`VM ${operationType} in progress...`);
      pendingOperationToastsRef.current.set(op.id, {
        toastId,
        vmId: vmRecord.id,
        operationType,
      });
      const timeoutId = window.setTimeout(() => {
        const pending = pendingOperationToastsRef.current.get(op.id);
        if (!pending) {
          return;
        }
        toast.update(pending.toastId, {
          render: `VM ${operationType} is still in progress. Check operation timeline for updates.`,
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
        pendingDeleteToastsRef.current.set(vmRecord.id, toastId);
      }
      return;
    }

    if (status === "succeeded") {
      toast.success(`VM ${operationType} completed`);
      return;
    }
    if (status === "failed") {
      toast.error(op?.error ? `VM ${operationType} failed: ${op.error}` : `VM ${operationType} failed`);
      return;
    }

    toast.info(`VM ${operationType} request accepted`);
  }

  async function submitCreate() {
    const finalError = validateCreateStep(4);
    if (finalError) {
      setCreateError(finalError);
      return;
    }
    setCreateBusy(true);
    try {
      const resp = await fetch(vmApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/vms`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: createForm.name.trim(),
          image_id: createForm.image_id,
          vcpu: Number(createForm.vcpu),
          memory_mb: Number(createForm.memory_mb),
          disk_gb: Number(createForm.disk_gb),
          bridge: createForm.bridge.trim(),
          guest: {
            username: createForm.guest_username.trim(),
            password: createForm.guest_password,
          },
        }),
      });
      const body = (await resp.json().catch(() => ({}))) as {
        vm?: NodeVmRecord;
        operation?: VmOperationRecord;
        error?: string;
      };
      if (!resp.ok) throw new Error(body.error ?? `Failed to queue VM create (${resp.status})`);
      if (body.vm) {
        const nextVm = body.vm as NodeVmRecord;
        setVms((current) => [nextVm, ...current.filter((vm) => vm.id !== nextVm.id)]);
        registerPendingOperationToast(nextVm, "create", body.operation ?? null);
      } else {
        toast.info("VM create request accepted");
      }
      setCreateOpen(false);
      setCreateForm(DEFAULT_FORM);
      setCreateStep(0);
      setCreateError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to queue VM create";
      setCreateError(message);
      toast.error(message);
    } finally {
      setCreateBusy(false);
    }
  }

  async function runVmAction(vm: NodeVmRecord, action: VmAction) {
    const busyKey = `${vm.id}:${action}`;
    setActionBusyKey(busyKey);
    try {
      const resp = await fetch(
        vmApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/vms/${encodeURIComponent(vm.id)}/actions/${action}`),
        { method: "POST", headers: { "Content-Type": "application/json" } }
      );
      const body = (await resp.json().catch(() => ({}))) as {
        vm?: NodeVmRecord;
        operation?: VmOperationRecord;
        error?: string;
      };
      if (!resp.ok) throw new Error(body.error ?? `Failed VM ${action} (${resp.status})`);
      if (body.vm) {
        const nextVm = body.vm as NodeVmRecord;
        setVms((current) => current.map((item) => (item.id === nextVm.id ? nextVm : item)));
        registerPendingOperationToast(nextVm, action, body.operation ?? null);
      } else {
        toast.info(`VM ${action} request accepted`);
      }
      if (action === "delete") setConfirmDeleteVm(null);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed VM ${action}`);
    } finally {
      setActionBusyKey(null);
    }
  }

  return (
    <section className="vm-panel">
      <div className="vm-panel-header">
        <h3>Virtual Machines</h3>
        <button
          type="button"
          className="secondary-button"
          disabled={!vmReady}
          onClick={() => {
            setCreateOpen(true);
            setCreateStep(0);
            setCreateForm(DEFAULT_FORM);
            setCreateError(null);
          }}
        >
          <FontAwesomeIcon icon={faPlus} />
          <span>Create VM</span>
        </button>
      </div>
      {!vmReady ? <p className="muted">{vmCapability?.message ?? "VM capability is not ready on this node."}</p> : null}
      {error ? <p className="error">{error}</p> : null}
      <div className="table-wrap">
        <table className="vm-table">
          <thead><tr><th>Name</th><th>State</th><th>Image</th><th>CPU</th><th>Memory</th><th>Disk</th><th>IP</th><th>Last Op</th><th>Updated</th><th>Actions</th></tr></thead>
          <tbody>
            {vms.map((vm) => (
              <tr
                key={vm.id}
                className="clickable-row"
                onClick={() =>
                  navigate(
                    `/node/${encodeURIComponent(node.id)}/vm/${encodeURIComponent(vm.id)}`
                  )
                }
              >
                <td>{vm.name}</td><td><span className={`badge vm-state-${vm.state}`}>{vm.state}</span></td><td>{vm.image_name}</td><td>{vm.vcpu}</td><td>{vm.memory_mb} MB</td><td>{vm.disk_gb} GB</td><td>{vm.ip_address ?? "-"}</td><td>{vm.last_operation ? `${vm.last_operation.operation_type}:${vm.last_operation.status}` : "-"}</td><td>{formatTimestamp(vm.updated_at)}</td>
                <td className="vm-actions-cell">
                  <button type="button" className="icon-button" disabled={actionBusyKey !== null} onClick={(e) => { e.stopPropagation(); void runVmAction(vm, "start"); }}><FontAwesomeIcon icon={faPlay} /></button>
                  <button type="button" className="icon-button" disabled={actionBusyKey !== null} onClick={(e) => { e.stopPropagation(); void runVmAction(vm, "stop"); }}><FontAwesomeIcon icon={faPowerOff} /></button>
                  <button type="button" className="icon-button" disabled={actionBusyKey !== null} onClick={(e) => { e.stopPropagation(); void runVmAction(vm, "reboot"); }}><FontAwesomeIcon icon={faRotateRight} /></button>
                  <button type="button" className="icon-button vm-action-danger" disabled={actionBusyKey !== null} onClick={(e) => { e.stopPropagation(); setConfirmDeleteVm(vm); }}><FontAwesomeIcon icon={faTrashCan} /></button>
                </td>
              </tr>
            ))}
            {!loading && vms.length === 0 ? <tr><td colSpan={10} className="empty">No VMs yet.</td></tr> : null}
          </tbody>
        </table>
      </div>
      {createOpen ? (
        <div className="modal-overlay" onClick={() => !createBusy && setCreateOpen(false)}>
          <div className="modal-card vm-create-modal" onClick={(e) => e.stopPropagation()}>
            <h2>Create VM</h2>
            <p className="muted">Step {createStep + 1} of 6</p>
            {createStep === 0 ? <input value={createForm.name} placeholder="vm-name" onChange={(e) => updateForm("name", e.target.value)} /> : null}
            {createStep === 1 ? <select className="vm-select" value={createForm.image_id} onChange={(e) => updateForm("image_id", e.target.value)}><option value="">Select image</option>{images.map((image) => <option key={image.id} value={image.id}>{image.name}</option>)}</select> : null}
            {createStep === 2 ? (
              <div className="vm-form-grid">
                <label className="vm-field">
                  <span className="vm-field-label">vCPU (cores)</span>
                  <input
                    type="number"
                    min={1}
                    max={32}
                    value={createForm.vcpu}
                    onChange={(e) => updateForm("vcpu", e.target.value)}
                    placeholder="2"
                  />
                </label>
                <label className="vm-field">
                  <span className="vm-field-label">Memory (MB)</span>
                  <input
                    type="number"
                    min={512}
                    max={262144}
                    step={256}
                    value={createForm.memory_mb}
                    onChange={(e) => updateForm("memory_mb", e.target.value)}
                    placeholder="2048"
                  />
                </label>
                <label className="vm-field">
                  <span className="vm-field-label">Disk Size (GB)</span>
                  <input
                    type="number"
                    min={10}
                    max={4096}
                    step={1}
                    value={createForm.disk_gb}
                    onChange={(e) => updateForm("disk_gb", e.target.value)}
                    placeholder="20"
                  />
                </label>
              </div>
            ) : null}
            {createStep === 3 ? (
              <label className="vm-field">
                <span className="vm-field-label">Network Bridge</span>
                <input
                  value={createForm.bridge}
                  onChange={(e) => updateForm("bridge", e.target.value)}
                  placeholder="br0"
                />
              </label>
            ) : null}
            {createStep === 4 ? (
              <div className="vm-form-grid">
                <label className="vm-field">
                  <span className="vm-field-label">Guest Username</span>
                  <input
                    value={createForm.guest_username}
                    onChange={(e) => updateForm("guest_username", e.target.value)}
                    placeholder="ubuntu"
                  />
                </label>
                <label className="vm-field">
                  <span className="vm-field-label">Guest Password</span>
                  <input
                    type="password"
                    value={createForm.guest_password}
                    onChange={(e) => updateForm("guest_password", e.target.value)}
                    placeholder="Enter password"
                  />
                </label>
                <label className="vm-field">
                  <span className="vm-field-label">Confirm Password</span>
                  <input
                    type="password"
                    value={createForm.guest_password_confirm}
                    onChange={(e) => updateForm("guest_password_confirm", e.target.value)}
                    placeholder="Re-enter password"
                  />
                </label>
              </div>
            ) : null}
            {createStep === 5 ? <pre className="vm-review">{JSON.stringify({ name: createForm.name, image_id: createForm.image_id, vcpu: createForm.vcpu, memory_mb: createForm.memory_mb, disk_gb: createForm.disk_gb, bridge: createForm.bridge, guest_username: createForm.guest_username }, null, 2)}</pre> : null}
            {createError ? <p className="error">{createError}</p> : null}
            <div className="modal-actions">
              <button type="button" className="secondary-button" disabled={createBusy} onClick={() => { if (createStep === 0) setCreateOpen(false); else setCreateStep((step) => step - 1); }}>Back</button>
              {createStep < 5 ? <button type="button" disabled={createBusy} onClick={() => { const stepError = validateCreateStep(createStep); if (stepError) { setCreateError(stepError); return; } setCreateError(null); setCreateStep((step) => step + 1); }}>Next</button> : <button type="button" disabled={createBusy} onClick={() => void submitCreate()}>{createBusy ? "Queuing..." : "Create VM"}</button>}
            </div>
          </div>
        </div>
      ) : null}
      {confirmDeleteVm ? (
        <div className="modal-overlay" onClick={() => actionBusyKey === null && setConfirmDeleteVm(null)}>
          <div className="modal-card" onClick={(e) => e.stopPropagation()}>
            <h2>Delete VM</h2>
            <p className="muted">Delete <strong>{confirmDeleteVm.name}</strong>? This will remove disks and cannot be undone.</p>
            <div className="modal-actions">
              <button type="button" className="secondary-button" disabled={actionBusyKey !== null} onClick={() => setConfirmDeleteVm(null)}>Cancel</button>
              <button type="button" className="danger-button" disabled={actionBusyKey !== null} onClick={() => void runVmAction(confirmDeleteVm, "delete")}>Confirm Delete</button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
