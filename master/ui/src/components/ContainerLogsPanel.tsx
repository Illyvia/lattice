import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faFileLines, faPlug, faRotateRight } from "@fortawesome/free-solid-svg-icons";
import { toast } from "react-toastify";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";

type ContainerLogsPanelProps = {
  nodeId: string;
  containerId: string;
  containerName: string;
  apiBaseUrl: string;
};

type LogsWireMessage =
  | { type: "terminal_ready"; session_id: string }
  | { type: "terminal_data"; session_id: string; data: string }
  | { type: "terminal_exit"; session_id: string; exit_code?: number }
  | { type: "terminal_error"; session_id?: string; error: string }
  | { type: "pong" };

const DEFAULT_COLS = 120;
const DEFAULT_ROWS = 30;
const DEFAULT_TAIL = 200;

function buildLogsWsUrl(apiBaseUrl: string, nodeId: string, containerId: string): string {
  const base = apiBaseUrl.replace(/\/+$/, "");
  const wsBase = base.replace(/^http/i, (value) => (value.toLowerCase() === "https" ? "wss" : "ws"));
  const query = `tail=${encodeURIComponent(String(DEFAULT_TAIL))}`;
  return `${wsBase}/ws/nodes/${encodeURIComponent(nodeId)}/containers/${encodeURIComponent(
    containerId
  )}/logs?${query}`;
}

function readThemeMode(): "light" | "dark" {
  const raw = document.documentElement.getAttribute("data-theme");
  return raw === "dark" ? "dark" : "light";
}

function normalizeLogsChunk(chunk: string): string {
  if (!chunk) {
    return chunk;
  }
  return chunk.replace(/\u0000/g, "").replace(/\r?\n/g, "\r\n");
}

function isFatalLogsError(detail: string): boolean {
  const normalized = detail.toLowerCase();
  return (
    normalized.includes("container_not_found") ||
    normalized.includes("node_not_found") ||
    normalized.includes("node_not_paired") ||
    normalized.includes("container_runtime_missing")
  );
}

export default function ContainerLogsPanel({
  nodeId,
  containerId,
  containerName,
  apiBaseUrl,
}: ContainerLogsPanelProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const terminalRef = useRef<Terminal | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectScheduledRef = useRef(false);
  const disableAutoReconnectRef = useRef(false);
  const [connected, setConnected] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [reconnectKey, setReconnectKey] = useState(0);

  const wsUrl = useMemo(
    () => buildLogsWsUrl(apiBaseUrl, nodeId, containerId),
    [apiBaseUrl, nodeId, containerId]
  );

  useEffect(() => {
    const mount = containerRef.current;
    if (!mount) {
      return;
    }

    const theme = readThemeMode();
    const terminal = new Terminal({
      disableStdin: true,
      cursorBlink: false,
      convertEol: true,
      fontFamily: "JetBrains Mono, Consolas, monospace",
      fontSize: 13,
      lineHeight: 1.25,
      cols: DEFAULT_COLS,
      rows: DEFAULT_ROWS,
      theme:
        theme === "dark"
          ? {
              background: "#050507",
              foreground: "#e8ecf2",
              cursor: "#e8ecf2",
              selectionBackground: "#2f3b52",
            }
          : {
              background: "#ffffff",
              foreground: "#0f172a",
              cursor: "#0f172a",
              selectionBackground: "#cbd5e1",
            },
    });
    const fitAddon = new FitAddon();
    terminal.loadAddon(fitAddon);
    terminal.open(mount);
    fitAddon.fit();
    terminal.writeln("Lattice Container Logs");
    terminal.writeln(`Connecting to ${containerName}...`);

    terminalRef.current = terminal;
    fitAddonRef.current = fitAddon;

    const observer = new MutationObserver(() => {
      const activeTheme = readThemeMode();
      terminal.options.theme =
        activeTheme === "dark"
          ? {
              background: "#050507",
              foreground: "#e8ecf2",
              cursor: "#e8ecf2",
              selectionBackground: "#2f3b52",
            }
          : {
              background: "#ffffff",
              foreground: "#0f172a",
              cursor: "#0f172a",
              selectionBackground: "#cbd5e1",
            };
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

    return () => {
      observer.disconnect();
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      reconnectScheduledRef.current = false;
      socketRef.current?.close();
      socketRef.current = null;
      terminalRef.current?.dispose();
      terminalRef.current = null;
      fitAddonRef.current = null;
    };
  }, [containerName]);

  useEffect(() => {
    const terminal = terminalRef.current;
    const fitAddon = fitAddonRef.current;
    if (!terminal || !fitAddon) {
      return;
    }

    let closedByClient = false;
    let isDisposed = false;
    setConnecting(true);
    setConnected(false);
    setSessionId(null);
    reconnectScheduledRef.current = false;
    disableAutoReconnectRef.current = false;
    if (reconnectTimerRef.current !== null) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }

    const socket = new WebSocket(wsUrl);
    socketRef.current = socket;

    const onResize = () => {
      if (!fitAddonRef.current) {
        return;
      }
      fitAddonRef.current.fit();
    };
    window.addEventListener("resize", onResize);

    socket.onopen = () => {
      setConnecting(false);
      setConnected(true);
      reconnectAttemptsRef.current = 0;
      onResize();
      terminal.writeln("");
      terminal.writeln("Connected.");
    };

    socket.onmessage = (event) => {
      const terminalInstance = terminalRef.current;
      if (!terminalInstance) {
        return;
      }
      let payload: unknown;
      try {
        payload = JSON.parse(String(event.data));
      } catch {
        return;
      }
      if (!payload || typeof payload !== "object") {
        return;
      }
      const message = payload as LogsWireMessage;
      if (message.type === "terminal_ready") {
        setSessionId(message.session_id);
        return;
      }
      if (message.type === "terminal_data") {
        if (typeof message.data === "string") {
          terminalInstance.write(normalizeLogsChunk(message.data));
        }
        return;
      }
      if (message.type === "terminal_exit") {
        const code = typeof message.exit_code === "number" ? message.exit_code : 0;
        terminalInstance.writeln(`\r\n[logs stream ended: ${code}]`);
        return;
      }
      if (message.type === "terminal_error") {
        const detail = message.error || "logs error";
        if (isFatalLogsError(detail)) {
          disableAutoReconnectRef.current = true;
        }
        terminalInstance.writeln(`\r\n[logs error] ${detail}`);
        toast.error(`Container logs error: ${detail}`);
      }
    };

    socket.onerror = () => {
      const terminalInstance = terminalRef.current;
      if (terminalInstance && !closedByClient) {
        terminalInstance.writeln("\r\n[connection error]");
      }
    };

    const scheduleReconnect = () => {
      if (
        closedByClient ||
        isDisposed ||
        reconnectScheduledRef.current ||
        disableAutoReconnectRef.current
      ) {
        return;
      }
      reconnectScheduledRef.current = true;
      const nextAttempt = Math.min(reconnectAttemptsRef.current + 1, 8);
      reconnectAttemptsRef.current = nextAttempt;
      const delayMs = Math.min(1000 * 2 ** (nextAttempt - 1), 10000);
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        reconnectScheduledRef.current = false;
        if (!closedByClient && !isDisposed) {
          setReconnectKey((value) => value + 1);
        }
      }, delayMs);
    };

    socket.onclose = (event: CloseEvent) => {
      setConnecting(false);
      setConnected(false);
      setSessionId(null);
      const terminalInstance = terminalRef.current;
      if (terminalInstance) {
        terminalInstance.writeln("");
        terminalInstance.writeln("[logs disconnected]");
      }
      if (!closedByClient && !disableAutoReconnectRef.current && event.code !== 1000) {
        scheduleReconnect();
      }
    };

    return () => {
      isDisposed = true;
      window.removeEventListener("resize", onResize);
      closedByClient = true;
      reconnectScheduledRef.current = false;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      try {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: "close" }));
        }
      } catch {
        // Best effort close message.
      }
      socket.close();
      if (socketRef.current === socket) {
        socketRef.current = null;
      }
    };
  }, [wsUrl, reconnectKey]);

  return (
    <section className="terminal-card">
      <div className="terminal-header">
        <h3>
          <FontAwesomeIcon icon={faFileLines} /> Logs
        </h3>
        <div className="terminal-controls">
          <span className={`terminal-connection ${connected ? "terminal-connection-on" : "terminal-connection-off"}`}>
            <FontAwesomeIcon icon={faPlug} />
            {connected ? "connected" : connecting ? "connecting" : "disconnected"}
          </span>
          <button
            type="button"
            className="secondary-button"
            onClick={() => setReconnectKey((value) => value + 1)}
            disabled={connecting}
          >
            <FontAwesomeIcon icon={faRotateRight} />
            <span>Reconnect</span>
          </button>
        </div>
      </div>
      <p className="muted">
        Live logs for {containerName} {sessionId ? `(session ${sessionId.slice(0, 8)})` : ""}.
      </p>
      <div ref={containerRef} className="terminal-emulator" />
    </section>
  );
}
