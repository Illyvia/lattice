import { useEffect, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faPaperPlane, faRotateRight, faTerminal } from "@fortawesome/free-solid-svg-icons";
import { toast } from "react-toastify";
import { TerminalCommandRecord } from "../types";
import { formatTimestamp } from "../utils/health";

type NodeTerminalPanelProps = {
  nodeId: string;
  apiBaseUrl: string;
};

function terminalApiUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

function terminalStatusClass(status: TerminalCommandRecord["status"]): string {
  if (status === "succeeded") return "terminal-status-succeeded";
  if (status === "failed") return "terminal-status-failed";
  if (status === "running") return "terminal-status-running";
  return "terminal-status-queued";
}

export default function NodeTerminalPanel({ nodeId, apiBaseUrl }: NodeTerminalPanelProps) {
  const [commandText, setCommandText] = useState("");
  const [items, setItems] = useState<TerminalCommandRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadCommands(withLoading: boolean) {
    if (withLoading) {
      setLoading(true);
    }
    try {
      const response = await fetch(
        terminalApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(nodeId)}/terminal/commands?limit=100`),
        { cache: "no-store" }
      );
      if (!response.ok) {
        throw new Error(`Failed to load terminal history (${response.status})`);
      }
      const payload = (await response.json()) as TerminalCommandRecord[];
      setItems(Array.isArray(payload) ? payload : []);
      setError(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to load terminal history";
      setError(message);
    } finally {
      if (withLoading) {
        setLoading(false);
      }
    }
  }

  useEffect(() => {
    void loadCommands(true);
    const timer = window.setInterval(() => {
      void loadCommands(false);
    }, 2000);
    return () => {
      window.clearInterval(timer);
    };
  }, [nodeId, apiBaseUrl]);

  async function runCommand() {
    const clean = commandText.trim();
    if (!clean) {
      toast.error("Enter a command first.");
      return;
    }

    setRunning(true);
    try {
      const response = await fetch(
        terminalApiUrl(apiBaseUrl, `/api/nodes/${encodeURIComponent(nodeId)}/terminal/exec`),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ command: clean }),
        }
      );
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.error ?? `Failed to queue command (${response.status})`);
      }
      setCommandText("");
      toast.info("Terminal command queued.");
      await loadCommands(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to queue command";
      setError(message);
      toast.error(message);
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="terminal-card">
      <div className="terminal-header">
        <h3>
          <FontAwesomeIcon icon={faTerminal} /> Terminal
        </h3>
        <button
          type="button"
          className="secondary-button"
          onClick={() => void loadCommands(true)}
          disabled={loading}
        >
          <FontAwesomeIcon icon={faRotateRight} />
          <span>Refresh</span>
        </button>
      </div>

      <p className="muted">Runs shell commands on this node through the agent command queue.</p>

      <div className="terminal-runner">
        <input
          value={commandText}
          onChange={(event) => setCommandText(event.target.value)}
          placeholder="Type command, e.g. uname -a"
          disabled={running}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              void runCommand();
            }
          }}
        />
        <button type="button" onClick={() => void runCommand()} disabled={running}>
          <FontAwesomeIcon icon={faPaperPlane} />
          <span>{running ? "Queueing..." : "Run"}</span>
        </button>
      </div>

      {error ? <p className="error">{error}</p> : null}

      <div className="terminal-history">
        {loading && items.length === 0 ? <p className="muted">Loading terminal history...</p> : null}
        {!loading && items.length === 0 ? <p className="muted">No terminal commands yet.</p> : null}
        {items.map((item) => (
          <article key={item.id} className="terminal-entry">
            <div className="terminal-entry-header">
              <code className="terminal-command-text">{item.command_text}</code>
              <span className={`terminal-status ${terminalStatusClass(item.status)}`}>{item.status}</span>
            </div>
            <div className="terminal-entry-meta">
              <span>{formatTimestamp(item.created_at)}</span>
              <span>exit: {item.exit_code ?? "-"}</span>
            </div>
            {item.stdout_text ? (
              <pre className="terminal-output">
                <code>{item.stdout_text}</code>
              </pre>
            ) : null}
            {item.stderr_text ? (
              <pre className="terminal-output terminal-output-error">
                <code>{item.stderr_text}</code>
              </pre>
            ) : null}
            {item.error ? <p className="error">{item.error}</p> : null}
          </article>
        ))}
      </div>
    </section>
  );
}
