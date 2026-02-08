import { useCallback, useEffect, useState } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import HomePage from "./pages/HomePage";
import NewNodePage from "./pages/NewNodePage";
import NodeDetailPage from "./pages/NodeDetailPage";
import NodesPage from "./pages/NodesPage";
import { NodeRecord, ThemeMode } from "./types";
import { ToastContainer, toast } from "react-toastify";
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
    const resp = await fetch(apiUrl(`/api/nodes/${nodeId}/actions/update-agent`), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    });

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      const message = body.error ?? `Failed to queue agent update (${resp.status})`;
      toast.error(message);
      throw new Error(message);
    }

    const body = (await resp.json()) as {
      agent_connected?: boolean;
      agent_ws_connected?: boolean;
      recent_heartbeat?: boolean;
    };
    if (body.agent_connected === false) {
      toast.info("Update request accepted. It will run when the node command channel reconnects.");
    } else if (body.agent_ws_connected === false && body.recent_heartbeat === true) {
      toast.info("Update request accepted. Node is online and command dispatch is catching up.");
    } else {
      toast.info("Update command sent to node.");
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
