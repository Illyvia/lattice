import { HealthState, NodeRecord } from "../types";
import { formatTimestamp, getHeartbeatHealth } from "../utils/health";

type HomePageProps = {
  lastRefresh: string;
  nodes: NodeRecord[];
  onSelectNode: (nodeId: string) => void;
};

type ProblemNode = {
  node: NodeRecord;
  health: Exclude<HealthState, "healthy" | "pending">;
  heartbeatAgeSeconds: number;
};

function heartbeatAgeSeconds(value: string | null): number {
  if (!value) {
    return Number.POSITIVE_INFINITY;
  }
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.max(0, Math.floor((Date.now() - parsed) / 1000));
}

function compareProblemNodes(a: ProblemNode, b: ProblemNode): number {
  if (a.health !== b.health) {
    return a.health === "down" ? -1 : 1;
  }
  return b.heartbeatAgeSeconds - a.heartbeatAgeSeconds;
}

export default function HomePage({ lastRefresh, nodes, onSelectNode }: HomePageProps) {
  const nodeHealth = nodes.map((node) => ({ node, health: getHeartbeatHealth(node) }));
  const problemNodes = nodeHealth
    .filter((item): item is { node: NodeRecord; health: "degraded" | "down" } => {
      return item.health === "degraded" || item.health === "down";
    })
    .map((item) => ({
      ...item,
      heartbeatAgeSeconds: heartbeatAgeSeconds(item.node.last_heartbeat_at),
    }))
    .sort(compareProblemNodes);

  const allNodesDown =
    nodeHealth.length > 0 && nodeHealth.every((item) => item.health === "down");

  const clusterState: "healthy" | "degraded" | "down" = allNodesDown
    ? "down"
    : problemNodes.length > 0
      ? "degraded"
      : "healthy";

  const clusterTitle =
    clusterState === "healthy" ? "Healthy" : clusterState === "degraded" ? "Degraded" : "Down";
  const clusterMessage =
    clusterState === "healthy"
      ? "All nodes are healthy."
      : clusterState === "degraded"
        ? "Some nodes are degraded or down."
        : "All nodes are down.";

  return (
    <section>
      <p className="muted">Last update: {lastRefresh}</p>

      <section className={`cluster-health cluster-health-${clusterState}`}>
        <p className="cluster-health-label">Cluster Health</p>
        <h2 className="cluster-health-title">{clusterTitle}</h2>
        <p className="muted cluster-health-message">{clusterMessage}</p>
      </section>

      {clusterState !== "healthy" ? (
        <section className="cluster-problem-list">
          <h3>Affected Nodes</h3>
          <div className="cluster-problem-items">
            {problemNodes.map((entry) => (
              <button
                key={entry.node.id}
                type="button"
                className="cluster-problem-item"
                onClick={() => onSelectNode(entry.node.id)}
              >
                <span className={`health health-${entry.health}`}>
                  <span className="dot" />
                  {entry.health}
                </span>
                <strong className="cluster-problem-name">{entry.node.name}</strong>
                <span className="cluster-problem-meta">
                  Last heartbeat: {formatTimestamp(entry.node.last_heartbeat_at)}
                </span>
              </button>
            ))}
          </div>
        </section>
      ) : null}
    </section>
  );
}
