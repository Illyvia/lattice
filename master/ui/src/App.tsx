import { useCallback, useEffect, useRef, useState } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import HomePage from "./pages/HomePage";
import NewNodePage from "./pages/NewNodePage";
import NodeDetailPage from "./pages/NodeDetailPage";
import NodesPage from "./pages/NodesPage";
import ContainerDetailPage from "./pages/ContainerDetailPage";
import VmDetailPage from "./pages/VmDetailPage";
import { NodeRecord, ThemeMode } from "./types";
import { ToastContainer, Id as ToastId, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

const configuredApiBaseUrl = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim();
const defaultApiBaseUrl =
  typeof window !== "undefined"
    ? `${window.location.protocol}//${window.location.hostname}:8000`
    : "http://127.0.0.1:8000";
const API_BASE_URL = configuredApiBaseUrl || defaultApiBaseUrl;

function apiUrl(path: string): string {
  return `${API_BASE_URL.replace(/\/+$/, "")}${path}`;
}

export default function App() {
  const [nodes, setNodes] = useState<NodeRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<string>("-");
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return window.localStorage.getItem("lattice-sidebar-collapsed") === "1";
  });
  const [theme, setTheme] = useState<ThemeMode>(() => {
    if (typeof window === "undefined") {
      return "light";
    }
    const stored = window.localStorage.getItem("lattice-theme");
    if (stored === "light" || stored === "dark") {
      return stored;
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });

  const location = useLocation();
  const navigate = useNavigate();
  const updateToastTimersRef = useRef<Map<string, number>>(new Map());
  const updateToastIdsRef = useRef<Map<string, ToastId>>(new Map());

  const loadNodes = useCallback(async () => {
    try {
      const resp = await fetch(apiUrl("/api/nodes"));
      if (!resp.ok) {
        throw new Error(`Failed to load nodes (${resp.status})`);
      }
      const data = (await resp.json()) as NodeRecord[];
      setNodes(data);
      setLastRefresh(new Date().toLocaleTimeString());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load nodes");
      toast.error(err instanceof Error ? err.message : "Failed to load nodes");
    }
  }, []);

  useEffect(() => {
    loadNodes();
    const timer = setInterval(loadNodes, 5000);
    return () => clearInterval(timer);
  }, [loadNodes]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("lattice-theme", theme);
  }, [theme]);

  useEffect(() => {
    window.localStorage.setItem("lattice-sidebar-collapsed", sidebarCollapsed ? "1" : "0");
  }, [sidebarCollapsed]);

  const clearUpdateToastTracker = useCallback((nodeId: string) => {
    const timer = updateToastTimersRef.current.get(nodeId);
    if (typeof timer === "number") {
      window.clearTimeout(timer);
    }
    updateToastTimersRef.current.delete(nodeId);
    updateToastIdsRef.current.delete(nodeId);
  }, []);

  useEffect(() => {
    return () => {
      for (const timer of updateToastTimersRef.current.values()) {
        window.clearTimeout(timer);
      }
      updateToastTimersRef.current.clear();
      updateToastIdsRef.current.clear();
    };
  }, []);

  const trackUpdateCommand = useCallback(
    (nodeId: string, commandId: string, toastId: ToastId) => {
      clearUpdateToastTracker(nodeId);
      updateToastIdsRef.current.set(nodeId, toastId);
      const startedAt = Date.now();

      const poll = async () => {
        const currentToastId = updateToastIdsRef.current.get(nodeId);
        if (currentToastId !== toastId) {
          return;
        }
        try {
          const response = await fetch(apiUrl(`/api/nodes/${encodeURIComponent(nodeId)}/logs?limit=200`), {
            cache: "no-store",
          });
          if (response.ok) {
            const body = (await response.json().catch(() => ({}))) as {
              items?: Array<{ message?: unknown; meta?: Record<string, unknown> | null }>;
            };
            const items = Array.isArray(body.items) ? body.items : [];
            const match = items
              .slice()
              .reverse()
              .find((item) => {
                if (!item || typeof item !== "object") {
                  return false;
                }
                const message = typeof item.message === "string" ? item.message : "";
                const meta = item.meta;
                const metaCommandType =
                  meta && typeof meta === "object" && typeof meta.command_type === "string"
                    ? meta.command_type
                    : null;
                if (metaCommandType !== "update_agent" && !message.includes("Agent command update_agent ->")) {
                  return false;
                }
                const metaCommandId =
                  meta && typeof meta === "object" && typeof meta.command_id === "string"
                    ? meta.command_id
                    : null;
                return metaCommandId === commandId;
              });

            if (match) {
              const meta = match.meta;
              const metaStatus =
                meta && typeof meta === "object" && typeof meta.status === "string"
                  ? meta.status.trim().toLowerCase()
                  : null;
              const message = typeof match.message === "string" ? match.message : "";
              if (metaStatus === "updated" || metaStatus === "up_to_date" || metaStatus === "succeeded") {
                toast.update(toastId, {
                  render:
                    metaStatus === "up_to_date"
                      ? "Agent already up to date"
                      : "Agent update completed",
                  type: "success",
                  isLoading: false,
                  autoClose: 4200,
                  closeButton: true,
                });
                clearUpdateToastTracker(nodeId);
                return;
              }
              if (metaStatus === "failed" || metaStatus === "error" || metaStatus === "busy") {
                toast.update(toastId, {
                  render: message || "Agent update failed",
                  type: "error",
                  isLoading: false,
                  autoClose: 6000,
                  closeButton: true,
                });
                clearUpdateToastTracker(nodeId);
                return;
              }
              if (message.includes("-> succeeded")) {
                toast.update(toastId, {
                  render: "Agent update completed",
                  type: "success",
                  isLoading: false,
                  autoClose: 4200,
                  closeButton: true,
                });
                clearUpdateToastTracker(nodeId);
                return;
              }
              if (message.includes("-> failed") || message.includes("-> error")) {
                toast.update(toastId, {
                  render: message,
                  type: "error",
                  isLoading: false,
                  autoClose: 6000,
                  closeButton: true,
                });
                clearUpdateToastTracker(nodeId);
                return;
              }
            }
          }
        } catch {
          // Ignore polling blips and keep waiting for command completion.
        }

        if (Date.now() - startedAt > 180000) {
          toast.update(toastId, {
            render: "Update is still running. Check node logs for completion.",
            type: "info",
            isLoading: false,
            autoClose: 6000,
            closeButton: true,
          });
          clearUpdateToastTracker(nodeId);
          return;
        }

        const timer = window.setTimeout(() => {
          void poll();
        }, 2000);
        updateToastTimersRef.current.set(nodeId, timer);
      };

      void poll();
    },
    [clearUpdateToastTracker]
  );

  async function onCreateNode(name: string | null) {
    setLoading(true);
    try {
      const resp = await fetch(apiUrl("/api/nodes"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name
        })
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        throw new Error(body.error ?? `Failed to create node (${resp.status})`);
      }
      await loadNodes();
      setError(null);
      toast.success("Node created");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to create node";
      setError(message);
      toast.error(message);
      throw new Error(message);
    } finally {
      setLoading(false);
    }
  }

  async function onDeleteNode(nodeId: string) {
    let firstResp: Response | null = null;
    try {
      firstResp = await fetch(apiUrl(`/api/nodes/${nodeId}`), {
        method: "DELETE"
      });
    } catch {
      firstResp = null;
    }

    if (firstResp?.ok) {
      if (location.pathname === `/node/${nodeId}`) {
        navigate("/nodes");
      }
      await loadNodes();
      toast.success("Node deleted");
      return;
    }

    let fallbackResp: Response | null = null;
    try {
      fallbackResp = await fetch(apiUrl(`/api/nodes/${nodeId}/delete`), {
        method: "POST"
      });
    } catch {
      fallbackResp = null;
    }

    if (!fallbackResp?.ok) {
      let bodyDeleteResp: Response | null = null;
      try {
        bodyDeleteResp = await fetch(apiUrl("/api/nodes/delete"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ node_id: nodeId })
        });
      } catch {
        bodyDeleteResp = null;
      }

      if (bodyDeleteResp?.ok) {
        if (location.pathname === `/node/${nodeId}`) {
          navigate("/nodes");
        }
        await loadNodes();
        toast.success("Node deleted");
        return;
      }

      const errorSource = bodyDeleteResp ?? fallbackResp ?? firstResp;
      if (errorSource) {
        const body = await errorSource.json().catch(() => ({}));
        toast.error(body.error ?? `Failed to delete node (${errorSource.status})`);
        throw new Error(body.error ?? `Failed to delete node (${errorSource.status})`);
      }
      toast.error("Failed to delete node (network error)");
      throw new Error("Failed to delete node (network error)");
    }

    if (location.pathname === `/node/${nodeId}`) {
      navigate("/nodes");
    }
    await loadNodes();
    toast.success("Node deleted");
  }

  async function onRenameNode(nodeId: string, name: string) {
    const cleanName = name.trim();
    if (!cleanName) {
      throw new Error("Name is required");
    }

    let primaryResp: Response | null = null;
    try {
      primaryResp = await fetch(apiUrl(`/api/nodes/${nodeId}`), {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: cleanName })
      });
    } catch {
      primaryResp = null;
    }

    if (!primaryResp?.ok) {
      let fallbackResp: Response | null = null;
      try {
        fallbackResp = await fetch(apiUrl(`/api/nodes/${nodeId}/rename`), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: cleanName })
        });
      } catch {
        fallbackResp = null;
      }

      if (!fallbackResp?.ok) {
        const errorSource = fallbackResp ?? primaryResp;
        if (errorSource) {
          const body = await errorSource.json().catch(() => ({}));
          toast.error(body.error ?? `Failed to rename node (${errorSource.status})`);
          throw new Error(body.error ?? `Failed to rename node (${errorSource.status})`);
        }
        toast.error("Failed to rename node (network error)");
        throw new Error("Failed to rename node (network error)");
      }
    }

    await loadNodes();
    toast.success("Node renamed");
  }

  async function onUpdateNode(nodeId: string) {
    const loadingToastId = toast.loading("Sending update command...");
    const resp = await fetch(apiUrl(`/api/nodes/${nodeId}/actions/update-agent`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    });

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      const message = body.error ?? `Failed to queue agent update (${resp.status})`;
      toast.update(loadingToastId, {
        render: message,
        type: "error",
        isLoading: false,
        autoClose: 6000,
        closeButton: true,
      });
      throw new Error(message);
    }

    const body = (await resp.json()) as {
      command_id?: string;
      agent_connected?: boolean;
      agent_ws_connected?: boolean;
      recent_heartbeat?: boolean;
    };
    if (body.agent_connected === false) {
      toast.update(loadingToastId, {
        render: "Update queued. Waiting for node command channel reconnect...",
        isLoading: true,
        autoClose: false,
      });
    } else if (body.agent_ws_connected === false && body.recent_heartbeat === true) {
      toast.update(loadingToastId, {
        render: "Update queued. Node is online and command dispatch is catching up...",
        isLoading: true,
        autoClose: false,
      });
    } else {
      toast.update(loadingToastId, {
        render: "Update running...",
        isLoading: true,
        autoClose: false,
      });
    }

    if (typeof body.command_id === "string" && body.command_id.trim()) {
      trackUpdateCommand(nodeId, body.command_id.trim(), loadingToastId);
    } else {
      toast.update(loadingToastId, {
        render: "Update command accepted",
        type: "success",
        isLoading: false,
        autoClose: 4200,
        closeButton: true,
      });
    }
  }

  const pageTitle = location.pathname.startsWith("/node/")
    ? null
    : location.pathname === "/nodes/new"
      ? "New Node"
    : location.pathname === "/nodes"
      ? "Nodes"
      : "Home";

  return (
    <main className={`layout ${sidebarCollapsed ? "layout-sidebar-collapsed" : ""}`}>
      <Sidebar
        theme={theme}
        collapsed={sidebarCollapsed}
        onToggleCollapsed={() => setSidebarCollapsed((value) => !value)}
        onToggleTheme={() => setTheme(theme === "light" ? "dark" : "light")}
      />

      <section className="content">
        {pageTitle ? <h1>{pageTitle}</h1> : null}
        {error ? <p className="error">{error}</p> : null}

        <Routes>
          <Route path="/" element={<Navigate to="/home" replace />} />
          <Route
            path="/home"
            element={
              <HomePage
                lastRefresh={lastRefresh}
                nodes={nodes}
                onSelectNode={(nodeId) => navigate(`/node/${nodeId}`)}
              />
            }
          />
          <Route
            path="/nodes"
            element={
              <NodesPage
                nodes={nodes}
                loading={loading}
                onCreateNodeClick={() => navigate("/nodes/new")}
                onDeleteNode={onDeleteNode}
                onRenameNode={onRenameNode}
                onUpdateNode={onUpdateNode}
                onSelectNode={(nodeId) => navigate(`/node/${nodeId}`)}
              />
            }
          />
          <Route
            path="/nodes/new"
            element={
              <NewNodePage
                loading={loading}
                onCancel={() => navigate("/nodes")}
                onCreateNode={async (name) => {
                  await onCreateNode(name);
                  navigate("/nodes");
                }}
              />
            }
          />
          <Route
            path="/node/:nodeId"
            element={
              <NodeDetailPage
                nodes={nodes}
                apiBaseUrl={API_BASE_URL}
                onDeleteNode={onDeleteNode}
                onRenameNode={onRenameNode}
                onUpdateNode={onUpdateNode}
              />
            }
          />
          <Route
            path="/node/:nodeId/vm/:vmId"
            element={<VmDetailPage nodes={nodes} apiBaseUrl={API_BASE_URL} />}
          />
          <Route
            path="/node/:nodeId/container/:containerId"
            element={<ContainerDetailPage nodes={nodes} apiBaseUrl={API_BASE_URL} />}
          />
          <Route path="*" element={<Navigate to="/home" replace />} />
        </Routes>
      </section>

      <ToastContainer
        position="bottom-right"
        autoClose={4200}
        newestOnTop
        closeOnClick
        pauseOnHover
        theme={theme}
      />
    </main>
  );
}
