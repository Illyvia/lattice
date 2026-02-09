import { useEffect, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faBoxOpen, faDownload } from "@fortawesome/free-solid-svg-icons";
import { Id as ToastId, toast } from "react-toastify";
import { NodeContainerRecord, NodeRecord } from "../types";

type NodeMarketplacePanelProps = {
  node: NodeRecord;
  apiBaseUrl: string;
};

type MarketplaceItem = {
  id: string;
  name: string;
  description: string;
  image: string;
  defaultName: string;
  commandText: string | null;
};

type PendingInstallToast = {
  operationId: string;
  containerId: string;
  itemName: string;
  toastId: ToastId;
};

const MARKETPLACE_ITEMS: MarketplaceItem[] = [
  {
    id: "nginx",
    name: "NGINX",
    description: "Lightweight web server and reverse proxy.",
    image: "nginx:alpine",
    defaultName: "nginx",
    commandText: null,
  },
  {
    id: "redis",
    name: "Redis",
    description: "In-memory key-value cache and data store.",
    image: "redis:7-alpine",
    defaultName: "redis",
    commandText: null,
  },
  {
    id: "whoami",
    name: "Whoami",
    description: "Simple HTTP service for quick networking tests.",
    image: "traefik/whoami:latest",
    defaultName: "whoami",
    commandText: null,
  },
  {
    id: "ubuntu-shell",
    name: "Ubuntu Shell",
    description: "General-purpose Ubuntu container kept alive for terminal use.",
    image: "ubuntu:24.04",
    defaultName: "ubuntu-shell",
    commandText: "sleep infinity",
  },
  {
    id: "alpine-shell",
    name: "Alpine Shell",
    description: "Minimal Alpine container kept alive for quick tasks.",
    image: "alpine:3.20",
    defaultName: "alpine-shell",
    commandText: "sleep infinity",
  },
];

function containerApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

function toValidContainerName(raw: string): string {
  const normalized = raw
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  if (!normalized) {
    return "container";
  }
  return normalized;
}

function buildContainerName(base: string): string {
  const cleanBase = toValidContainerName(base);
  const suffix = Date.now().toString(36).slice(-4);
  const maxBaseLength = Math.max(3, 32 - suffix.length - 1);
  const truncated = cleanBase.slice(0, maxBaseLength).replace(/-$/g, "");
  return `${truncated || "container"}-${suffix}`;
}

export default function NodeMarketplacePanel({ node, apiBaseUrl }: NodeMarketplacePanelProps) {
  const [installingItemId, setInstallingItemId] = useState<string | null>(null);
  const [pollRevision, setPollRevision] = useState(0);
  const pendingToastsRef = useRef<Map<string, PendingInstallToast>>(new Map());
  const pendingTimeoutsRef = useRef<Map<string, number>>(new Map());

  const containerCapability = node.capabilities?.container;
  const containerReady = node.state === "paired" && containerCapability?.ready === true;

  useEffect(() => {
    return () => {
      for (const timeoutId of pendingTimeoutsRef.current.values()) {
        window.clearTimeout(timeoutId);
      }
      pendingTimeoutsRef.current.clear();
      pendingToastsRef.current.clear();
    };
  }, []);

  useEffect(() => {
    if (pendingToastsRef.current.size === 0) {
      return;
    }

    let cancelled = false;

    async function pollContainers() {
      if (cancelled || pendingToastsRef.current.size === 0) {
        return;
      }
      try {
        const response = await fetch(
          containerApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/containers`),
          { cache: "no-store" }
        );
        if (!response.ok) {
          return;
        }
        const containers = (await response.json()) as NodeContainerRecord[];
        const byId = new Map(containers.map((container) => [container.id, container]));
        for (const pending of [...pendingToastsRef.current.values()]) {
          const current = byId.get(pending.containerId);
          const operation = current?.last_operation;
          if (!operation || operation.id !== pending.operationId) {
            continue;
          }
          if (operation.status === "succeeded") {
            toast.update(pending.toastId, {
              render: `${pending.itemName} installed`,
              type: "success",
              isLoading: false,
              autoClose: 4200,
              closeButton: true,
            });
            pendingToastsRef.current.delete(pending.operationId);
            const timeoutId = pendingTimeoutsRef.current.get(pending.operationId);
            if (typeof timeoutId === "number") {
              window.clearTimeout(timeoutId);
            }
            pendingTimeoutsRef.current.delete(pending.operationId);
            continue;
          }
          if (operation.status === "failed") {
            toast.update(pending.toastId, {
              render: operation.error
                ? `${pending.itemName} install failed: ${operation.error}`
                : `${pending.itemName} install failed`,
              type: "error",
              isLoading: false,
              autoClose: 7000,
              closeButton: true,
            });
            pendingToastsRef.current.delete(pending.operationId);
            const timeoutId = pendingTimeoutsRef.current.get(pending.operationId);
            if (typeof timeoutId === "number") {
              window.clearTimeout(timeoutId);
            }
            pendingTimeoutsRef.current.delete(pending.operationId);
          }
        }
      } catch {
        // Keep polling; transient failures should not clear pending toasts.
      }
    }

    void pollContainers();
    const timer = window.setInterval(pollContainers, 4000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [apiBaseUrl, node.id, pollRevision]);

  async function installItem(item: MarketplaceItem): Promise<void> {
    if (!containerReady) {
      return;
    }
    setInstallingItemId(item.id);
    try {
      const name = buildContainerName(item.defaultName);
      const response = await fetch(
        containerApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(node.id)}/containers`),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name,
            image: item.image,
            command_text: item.commandText,
          }),
        }
      );
      const payload = (await response.json().catch(() => ({}))) as {
        container?: NodeContainerRecord;
        operation?: { id?: string; status?: string; error?: string | null } | null;
        error?: string;
      };
      if (!response.ok) {
        throw new Error(payload.error ?? `Failed to install ${item.name} (${response.status})`);
      }

      const operationId = payload.operation?.id;
      const containerId = payload.container?.id;
      if (typeof operationId === "string" && operationId && typeof containerId === "string" && containerId) {
        const toastId = toast.loading(`Installing ${item.name}...`);
        pendingToastsRef.current.set(operationId, {
          operationId,
          containerId,
          itemName: item.name,
          toastId,
        });
        const timeoutId = window.setTimeout(() => {
          const pending = pendingToastsRef.current.get(operationId);
          if (!pending) {
            return;
          }
          toast.update(pending.toastId, {
            render: `${item.name} install is still in progress. Check the Containers tab for updates.`,
            type: "info",
            isLoading: false,
            autoClose: 7000,
            closeButton: true,
          });
          pendingToastsRef.current.delete(operationId);
          pendingTimeoutsRef.current.delete(operationId);
        }, 180000);
        pendingTimeoutsRef.current.set(operationId, timeoutId);
        setPollRevision((value) => value + 1);
      } else {
        toast.success(`${item.name} install queued`);
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : `Failed to install ${item.name}`);
    } finally {
      setInstallingItemId(null);
    }
  }

  return (
    <section className="vm-panel">
      <div className="vm-panel-header">
        <h3>Marketplace</h3>
      </div>
      <p className="muted">
        Install prebuilt Docker containers on this node.
      </p>
      {!containerReady ? (
        <p className="muted">
          {containerCapability?.message ?? "Container capability is not ready on this node."}
        </p>
      ) : null}
      <div className="marketplace-grid">
        {MARKETPLACE_ITEMS.map((item) => (
          <article className="marketplace-card" key={item.id}>
            <div className="marketplace-card-title">
              <h4>{item.name}</h4>
            </div>
            <p className="marketplace-description">{item.description}</p>
            <div className="marketplace-meta">
              <span className="marketplace-meta-label">Image</span>
              <code>{item.image}</code>
            </div>
            <div className="marketplace-meta">
              <span className="marketplace-meta-label">Default Name</span>
              <code>{item.defaultName}</code>
            </div>
            <div className="marketplace-meta">
              <span className="marketplace-meta-label">Command</span>
              <code>{item.commandText ?? "(image default)"}</code>
            </div>
            <button
              type="button"
              className="secondary-button marketplace-install-button"
              disabled={!containerReady || installingItemId !== null}
              onClick={() => void installItem(item)}
            >
              <FontAwesomeIcon icon={installingItemId === item.id ? faBoxOpen : faDownload} />
              <span>{installingItemId === item.id ? "Installing..." : "Install"}</span>
            </button>
          </article>
        ))}
      </div>
    </section>
  );
}
