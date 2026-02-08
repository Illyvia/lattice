import { useEffect, useMemo, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faPlay,
  faPlus,
  faPowerOff,
  faRotateRight,
  faTrashCan
} from "@fortawesome/free-solid-svg-icons";
import { toast } from "react-toastify";
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

function vmApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

export default function NodeVmsPanel({ node, apiBaseUrl, openCreateIntent }: NodeVmsPanelProps) {
  const vmCapability = node.capabilities?.vm;
  const vmReady = node.state === "paired" && vmCapability?.ready === true;
  const [images, setImages] = useState<VmImageRecord[]>([]);
  const [vms, setVms] = useState<NodeVmRecord[]>([]);
  const [selectedVmId, setSelectedVmId] = useState<string | null>(null);
  const [operations, setOperations] = useState<VmOperationRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [opsLoading, setOpsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [opsError, setOpsError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [createStep, setCreateStep] = useState(0);
  const [createForm, setCreateForm] = useState<CreateVmForm>(DEFAULT_FORM);
  const [createBusy, setCreateBusy] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [actionBusyKey, setActionBusyKey] = useState<string | null>(null);
  const [confirmDeleteVm, setConfirmDeleteVm] = useState<NodeVmRecord | null>(null);

  const selectedVm = useMemo(
    () => vms.find((item) => item.id === selectedVmId) ?? null,
    [vms, selectedVmId]
  );

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
        setSelectedVmId((current) => current ?? (vmsPayload[0]?.id ?? null));
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
    if (!selectedVmId) {
      setOperations([]);
      return;
    }
    const vmId = selectedVmId;
    let cancelled = false;
    async function loadOps() {
      setOpsLoading(true);
      try {
        const resp = await fetch(
          vmApiUrl(
            apiBaseUrl,
            `/api/nodes/${encodeURIComponent(node.id)}/vms/${encodeURIComponent(vmId)}/operations?limit=50`
          ),
          { cache: "no-store" }
        );
        if (!resp.ok) {
          throw new Error(`Failed to load VM operations (${resp.status})`);
        }
        const payload = (await resp.json()) as VmOperationRecord[];
        if (cancelled) return;
        setOperations(payload);
        setOpsError(null);
      } catch (err) {
        if (cancelled) return;
        setOpsError(err instanceof Error ? err.message : "Failed to load VM operations");
      } finally {
        if (!cancelled) setOpsLoading(false);
      }
    }
    void loadOps();
    const timer = window.setInterval(loadOps, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, node.id, selectedVmId]);

  useEffect(() => {
    if (!selectedVmId && vms.length > 0) {
      setSelectedVmId(vms[0].id);
    }
    if (selectedVmId && !vms.some((vm) => vm.id === selectedVmId)) {
      setSelectedVmId(vms[0]?.id ?? null);
    }
  }, [vms, selectedVmId]);

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
      const body = (await resp.json().catch(() => ({}))) as { vm?: NodeVmRecord; error?: string };
      if (!resp.ok) throw new Error(body.error ?? `Failed to queue VM create (${resp.status})`);
      if (body.vm) setVms((current) => [body.vm as NodeVmRecord, ...current.filter((vm) => vm.id !== body.vm!.id)]);
      setCreateOpen(false);
      setCreateForm(DEFAULT_FORM);
      setCreateStep(0);
      setCreateError(null);
      toast.success("VM create queued");
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
      const body = (await resp.json().catch(() => ({}))) as { vm?: NodeVmRecord; error?: string };
      if (!resp.ok) throw new Error(body.error ?? `Failed VM ${action} (${resp.status})`);
      if (body.vm) setVms((current) => current.map((item) => (item.id === body.vm!.id ? (body.vm as NodeVmRecord) : item)));
      toast.info(`VM ${action} queued`);
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
              <tr key={vm.id} className="clickable-row" onClick={() => setSelectedVmId(vm.id)}>
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
      {selectedVm ? <section className="vm-ops-panel"><h4>Operation Timeline: {selectedVm.name}</h4>{opsError ? <p className="error">{opsError}</p> : null}<ul className="vm-ops-list">{operations.map((op) => <li key={op.id}><strong>{op.operation_type}</strong> <span className={`badge vm-op-${op.status}`}>{op.status}</span> <span className="muted">{formatTimestamp(op.created_at)}</span>{op.error ? <span className="error"> {op.error}</span> : null}</li>)}</ul>{opsLoading && operations.length === 0 ? <p className="muted">Loading operations...</p> : null}</section> : null}
      {createOpen ? (
        <div className="modal-overlay" onClick={() => !createBusy && setCreateOpen(false)}>
          <div className="modal-card vm-create-modal" onClick={(e) => e.stopPropagation()}>
            <h2>Create VM</h2>
            <p className="muted">Step {createStep + 1} of 6</p>
            {createStep === 0 ? <input value={createForm.name} placeholder="vm-name" onChange={(e) => updateForm("name", e.target.value)} /> : null}
            {createStep === 1 ? <select className="vm-select" value={createForm.image_id} onChange={(e) => updateForm("image_id", e.target.value)}><option value="">Select image</option>{images.map((image) => <option key={image.id} value={image.id}>{image.name}</option>)}</select> : null}
            {createStep === 2 ? <div className="vm-form-grid"><input value={createForm.vcpu} onChange={(e) => updateForm("vcpu", e.target.value)} placeholder="vCPU" /><input value={createForm.memory_mb} onChange={(e) => updateForm("memory_mb", e.target.value)} placeholder="Memory MB" /><input value={createForm.disk_gb} onChange={(e) => updateForm("disk_gb", e.target.value)} placeholder="Disk GB" /></div> : null}
            {createStep === 3 ? <input value={createForm.bridge} onChange={(e) => updateForm("bridge", e.target.value)} placeholder="Bridge (br0)" /> : null}
            {createStep === 4 ? <div className="vm-form-grid"><input value={createForm.guest_username} onChange={(e) => updateForm("guest_username", e.target.value)} placeholder="Guest username" /><input type="password" value={createForm.guest_password} onChange={(e) => updateForm("guest_password", e.target.value)} placeholder="Guest password" /><input type="password" value={createForm.guest_password_confirm} onChange={(e) => updateForm("guest_password_confirm", e.target.value)} placeholder="Confirm password" /></div> : null}
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
