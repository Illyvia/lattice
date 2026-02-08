import { FormEvent, useState } from "react";

type NewNodePageProps = {
  loading: boolean;
  onCreateNode: (name: string | null) => Promise<void>;
  onCancel: () => void;
};

export default function NewNodePage({ loading, onCreateNode, onCancel }: NewNodePageProps) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await onCreateNode(name.trim() || null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create node");
    }
  }

  return (
    <section>
      <p className="muted">Set the node name and create it.</p>
      <form className="new-node-form" onSubmit={handleSubmit}>
        <label className="form-label" htmlFor="new-node-name">
          Node Name
        </label>
        <input
          id="new-node-name"
          type="text"
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="My Node"
          maxLength={64}
        />
        {error ? <p className="error">{error}</p> : null}
        <div className="modal-actions">
          <button type="button" className="secondary-button" disabled={loading} onClick={onCancel}>
            Cancel
          </button>
          <button type="submit" disabled={loading}>
            {loading ? "Creating..." : "Create Node"}
          </button>
        </div>
      </form>
    </section>
  );
}
