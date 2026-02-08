import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faPlug, faRotateRight, faTerminal } from "@fortawesome/free-solid-svg-icons";
import { toast } from "react-toastify";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";

type VmTerminalPanelProps = {
  nodeId: string;
  vmId: string;
  vmName: string;
  apiBaseUrl: string;
};

type TerminalWireMessage =
  | { type: "terminal_ready"; session_id: string }
  | { type: "terminal_data"; session_id: string; data: string }
  | { type: "terminal_exit"; session_id: string; exit_code?: number }
  | { type: "terminal_error"; session_id?: string; error: string }
  | { type: "pong" };

const DEFAULT_COLS = 80;
const DEFAULT_ROWS = 25;

function buildTerminalWsUrl(
  apiBaseUrl: string,
  nodeId: string,
  vmId: string,
  cols: number,
  rows: number
): string {
  const base = apiBaseUrl.replace(/\/+$/, "");
  const wsBase = base.replace(/^http/i, (value) => (value.toLowerCase() === "https" ? "wss" : "ws"));
  const query = `cols=${encodeURIComponent(String(cols))}&rows=${encodeURIComponent(String(rows))}`;
  return `${wsBase}/ws/nodes/${encodeURIComponent(nodeId)}/vms/${encodeURIComponent(vmId)}/terminal?${query}`;
}

function readThemeMode(): "light" | "dark" {
  const raw = document.documentElement.getAttribute("data-theme");
  return raw === "dark" ? "dark" : "light";
}

function isFatalVmTerminalError(detail: string): boolean {
  const normalized = detail.toLowerCase();
  return (
    normalized.includes("vm is not running") ||
    normalized.includes("domain not found") ||
    normalized.includes("vm_not_found") ||
    normalized.includes("vm_domain_missing")
  );
}

function normalizeVmConsoleChunk(chunk: string): string {
  if (!chunk) {
    return chunk;
  }
  // Serial consoles sometimes emit bare LF and NUL padding; normalize for xterm.
  return chunk.replace(/\u0000/g, "").replace(/\r?\n/g, "\r\n");
}

export default function VmTerminalPanel({ nodeId, vmId, vmName, apiBaseUrl }: VmTerminalPanelProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const terminalRef = useRef<Terminal | null>(null);
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
    () => buildTerminalWsUrl(apiBaseUrl, nodeId, vmId, DEFAULT_COLS, DEFAULT_ROWS),
    [apiBaseUrl, nodeId, vmId]
  );

  useEffect(() => {
    const mount = containerRef.current;
    if (!mount) {
      return;
    }

    const theme = readThemeMode();
    const terminal = new Terminal({
      cursorBlink: true,
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
    terminal.open(mount);

    terminalRef.current = terminal;

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
    };
  }, [vmName]);

  useEffect(() => {
    const terminal = terminalRef.current;
    if (!terminal) {
      return;
    }

    let closedByClient = false;
    let isDisposed = false;
    let sawAgentNotConnected = false;
    let didOpen = false;
    let sentInitialCarriageReturn = false;
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

    const onDataDisposable = terminal.onData((data) => {
      if (socket.readyState !== WebSocket.OPEN) {
        return;
      }
      socket.send(JSON.stringify({ type: "input", data }));
    });

    socket.onopen = () => {
      didOpen = true;
      setConnecting(false);
      setConnected(true);
      reconnectAttemptsRef.current = 0;
      socket.send(
        JSON.stringify({
          type: "resize",
          cols: DEFAULT_COLS,
          rows: DEFAULT_ROWS,
        })
      );
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
      const message = payload as TerminalWireMessage;
      if (message.type === "terminal_ready") {
        setSessionId(message.session_id);
        if (!sentInitialCarriageReturn && socket.readyState === WebSocket.OPEN) {
          sentInitialCarriageReturn = true;
          // Many VM serial consoles need an initial Enter to reveal the login prompt.
          socket.send(JSON.stringify({ type: "input", data: "\r" }));
        }
        return;
      }
      if (message.type === "terminal_data") {
        if (typeof message.data === "string") {
          terminalInstance.write(normalizeVmConsoleChunk(message.data));
        }
        return;
      }
      if (message.type === "terminal_exit") {
        const code = typeof message.exit_code === "number" ? message.exit_code : 0;
        toast.info(`VM console closed (exit ${code})`);
        return;
      }
      if (message.type === "terminal_error") {
        const detail = message.error || "terminal error";
        if (detail === "agent_not_connected" || detail.toLowerCase().includes("agent websocket disconnected")) {
          sawAgentNotConnected = true;
          return;
        }
        if (isFatalVmTerminalError(detail)) {
          disableAutoReconnectRef.current = true;
        }
        toast.error(`VM terminal error: ${detail}`);
      }
    };

    socket.onerror = () => {
      if (!sawAgentNotConnected && !closedByClient) {
        toast.error("VM terminal connection error");
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
      // Avoid injecting status text into the VM console stream.
      // Full-screen firmware/installer UIs can render incorrectly if mixed.
      if (!closedByClient && didOpen) {
        toast.info("VM terminal disconnected");
      }
      // A clean close (1000) is usually intentional; don't reconnect-loop.
      if (!closedByClient && !disableAutoReconnectRef.current && event.code !== 1000) {
        scheduleReconnect();
      }
    };

    return () => {
      isDisposed = true;
      onDataDisposable.dispose();
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
          <FontAwesomeIcon icon={faTerminal} /> VM Console
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
        Serial console for {vmName} {sessionId ? `(session ${sessionId.slice(0, 8)})` : ""}.
      </p>
      <div ref={containerRef} className="terminal-emulator" />
    </section>
  );
}
