import { useEffect, useMemo, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faPlug, faRotateRight, faTerminal } from "@fortawesome/free-solid-svg-icons";
import { toast } from "react-toastify";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
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

const DEFAULT_COLS = 110;
const DEFAULT_ROWS = 28;

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

export default function VmTerminalPanel({ nodeId, vmId, vmName, apiBaseUrl }: VmTerminalPanelProps) {
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
      convertEol: false,
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
    terminal.write("\u001b[1;34mLattice VM Console\u001b[0m\r\n");
    terminal.write(`Connecting to ${vmName}...\r\n`);

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
  }, [vmName]);

  useEffect(() => {
    const terminal = terminalRef.current;
    const fitAddon = fitAddonRef.current;
    if (!terminal || !fitAddon) {
      return;
    }

    let closedByClient = false;
    let isDisposed = false;
    let sawAgentNotConnected = false;
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

    const onResize = () => {
      if (!terminalRef.current || !fitAddonRef.current) {
        return;
      }
      fitAddonRef.current.fit();
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(
          JSON.stringify({
            type: "resize",
            cols: terminalRef.current.cols,
            rows: terminalRef.current.rows,
          })
        );
      }
    };
    window.addEventListener("resize", onResize);

    socket.onopen = () => {
      setConnecting(false);
      setConnected(true);
      reconnectAttemptsRef.current = 0;
      onResize();
      terminal.writeln("");
      terminal.writeln("\u001b[32mConnected.\u001b[0m");
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
        return;
      }
      if (message.type === "terminal_data") {
        if (typeof message.data === "string") {
          terminalInstance.write(message.data);
        }
        return;
      }
      if (message.type === "terminal_exit") {
        const code = typeof message.exit_code === "number" ? message.exit_code : 0;
        terminalInstance.writeln(`\r\n\u001b[33m[process exited: ${code}]\u001b[0m`);
        return;
      }
      if (message.type === "terminal_error") {
        const detail = message.error || "terminal error";
        if (detail === "agent_not_connected" || detail.toLowerCase().includes("agent websocket disconnected")) {
          sawAgentNotConnected = true;
          terminalInstance.writeln("\r\n\u001b[33m[waiting for agent websocket connection...]\u001b[0m");
          return;
        }
        if (isFatalVmTerminalError(detail)) {
          disableAutoReconnectRef.current = true;
        }
        terminalInstance.writeln(`\r\n\u001b[31m[terminal error] ${detail}\u001b[0m`);
        toast.error(`VM terminal error: ${detail}`);
      }
    };

    socket.onerror = () => {
      const terminalInstance = terminalRef.current;
      if (terminalInstance && !sawAgentNotConnected) {
        terminalInstance.writeln("\r\n\u001b[31m[connection error]\u001b[0m");
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
      const terminalInstance = terminalRef.current;
      if (terminalInstance) {
        terminalInstance.writeln(`\u001b[90m[reconnecting in ${Math.round(delayMs / 1000)}s]\u001b[0m`);
      }
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        reconnectScheduledRef.current = false;
        if (!closedByClient && !isDisposed) {
          setReconnectKey((value) => value + 1);
        }
      }, delayMs);
    };

    socket.onclose = () => {
      setConnecting(false);
      setConnected(false);
      setSessionId(null);
      const terminalInstance = terminalRef.current;
      if (terminalInstance) {
        terminalInstance.writeln("");
        terminalInstance.writeln(
          closedByClient
            ? "\u001b[90m[terminal disconnected]\u001b[0m"
            : "\u001b[31m[terminal connection closed]\u001b[0m"
        );
      }
      scheduleReconnect();
    };

    return () => {
      isDisposed = true;
      onDataDisposable.dispose();
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

