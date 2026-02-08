type HomePageProps = {
  lastRefresh: string;
  total: number;
  pending: number;
  paired: number;
  healthy: number;
  degraded: number;
  down: number;
};

export default function HomePage({
  lastRefresh,
  total,
  pending,
  paired,
  healthy,
  degraded,
  down
}: HomePageProps) {
  return (
    <section>
      <p className="muted">Last update: {lastRefresh}</p>
      <div className="stats-grid">
        <div className="stat-card">
          <span className="stat-label">Total Nodes</span>
          <strong>{total}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-label">Pending Pairing</span>
          <strong>{pending}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-label">Paired</span>
          <strong>{paired}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-label">Healthy</span>
          <strong>{healthy}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-label">Degraded</span>
          <strong>{degraded}</strong>
        </div>
        <div className="stat-card">
          <span className="stat-label">Down</span>
          <strong>{down}</strong>
        </div>
      </div>
    </section>
  );
}
