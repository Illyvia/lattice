import { useEffect, useRef, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faEllipsisVertical, faPenToSquare, faTrashCan } from "@fortawesome/free-solid-svg-icons";
import { NodeRecord } from "../types";
import { formatTimestamp, getHeartbeatHealth } from "../utils/health";

type NodesPageProps = {
  nodes: NodeRecord[];
  loading: boolean;
  onCreateNodeClick: () => void;
  onDeleteNode: (nodeId: string) => Promise<void>;
  onRenameNode: (nodeId: string, name: string) => Promise<void>;
  onSelectNode: (nodeId: string) => void;
};

export default function NodesPage({
  nodes,
  loading,
  onCreateNodeClick,
  onDeleteNode,
  onRenameNode,
  onSelectNode
}: NodesPageProps) {
  const [menuNodeId, setMenuNodeId] = useState<string | null>(null);
  const [menuPosition, setMenuPosition] = useState<{ left: number; top: number } | null>(null);
  const [renameNode, setRenameNode] = useState<NodeRecord | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [renaming, setRenaming] = useState(false);
  const [renameError, setRenameError] = useState<string | null>(null);
  const [confirmDeleteNode, setConfirmDeleteNode] = useState<NodeRecord | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!menuNodeId) {
      return;
    }
    const closeMenu = () => {
      setMenuNodeId(null);
      setMenuPosition(null);
    };
    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Element | null;
      if (!target) {
        closeMenu();
        return;
      }
      if (menuRef.current?.contains(target)) {
        return;
      }
      if (target.closest(".icon-button")) {
        return;
      }
      closeMenu();
    };
    const onEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeMenu();
      }
    };
    window.addEventListener("scroll", closeMenu, true);
    window.addEventListener("resize", closeMenu);
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onEscape);
    return () => {
      window.removeEventListener("scroll", closeMenu, true);
      window.removeEventListener("resize", closeMenu);
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onEscape);
    };
  }, [menuNodeId]);

  async function confirmDelete() {
    if (!confirmDeleteNode) {
      return;
    }
    setDeleting(true);
    try {
      await onDeleteNode(confirmDeleteNode.id);
      setConfirmDeleteNode(null);
      setMenuNodeId(null);
      setDeleteError(null);
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : "Failed to delete node");
    } finally {
      setDeleting(false);
    }
  }

  async function confirmRename() {
    if (!renameNode) {
      return;
    }
    setRenaming(true);
    try {
      await onRenameNode(renameNode.id, renameValue);
      setRenameNode(null);
      setRenameError(null);
    } catch (err) {
      setRenameError(err instanceof Error ? err.message : "Failed to rename node");
    } finally {
      setRenaming(false);
    }
  }

  return (
    <section>
      <p className="muted">View and manage your nodes.</p>
      <div className="row">
        <button type="button" disabled={loading} onClick={onCreateNodeClick}>
          {loading ? "Creating..." : "Create Node"}
        </button>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Pair Code</th>
              <th>State</th>
              <th>Health</th>
              <th>Created</th>
              <th>Paired At</th>
              <th>Last Heartbeat</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((node) => {
              const health = getHeartbeatHealth(node);
              return (
                <tr key={node.id} className="clickable-row" onClick={() => onSelectNode(node.id)}>
                  <td>{node.name}</td>
                  <td>
                    <code>{node.pair_code}</code>
                  </td>
                  <td>
                    <span className={`badge badge-${node.state}`}>{node.state}</span>
                  </td>
                  <td>
                    <span className={`health health-${health}`}>
                      <span className="dot" />
                      {health}
                    </span>
                  </td>
                  <td>{formatTimestamp(node.created_at)}</td>
                  <td>{formatTimestamp(node.paired_at)}</td>
                  <td>{formatTimestamp(node.last_heartbeat_at)}</td>
                  <td className="actions-cell">
                    <button
                      type="button"
                      className="icon-button"
                      aria-label={`Actions for ${node.name}`}
                      title={`Actions for ${node.name}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (menuNodeId === node.id) {
                          setMenuNodeId(null);
                          setMenuPosition(null);
                          return;
                        }
                        const trigger = event.currentTarget;
                        const rect = trigger.getBoundingClientRect();
                        const menuWidth = 156;
                        const menuHeight = 96;
                        const offset = 6;
                        const left = Math.max(
                          8,
                          Math.min(rect.right - menuWidth, window.innerWidth - menuWidth - 8)
                        );
                        let top = rect.bottom + offset;
                        if (top + menuHeight > window.innerHeight - 8) {
                          top = Math.max(8, rect.top - menuHeight - offset);
                        }
                        setMenuNodeId(node.id);
                        setMenuPosition({ left, top });
                      }}
                    >
                      <FontAwesomeIcon icon={faEllipsisVertical} />
                    </button>
                  </td>
                </tr>
              );
            })}
            {nodes.length === 0 ? (
              <tr>
                <td colSpan={8} className="empty">
                  No nodes yet.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      {menuNodeId && menuPosition ? (
        <div
          ref={menuRef}
          className="dropdown-menu dropdown-menu-floating"
          style={{
            left: `${menuPosition.left}px`,
            top: `${menuPosition.top}px`
          }}
        >
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              const node = nodes.find((item) => item.id === menuNodeId);
              if (node) {
                setRenameNode(node);
                setRenameValue(node.name);
              }
              setMenuNodeId(null);
              setMenuPosition(null);
              setRenameError(null);
            }}
          >
            <FontAwesomeIcon icon={faPenToSquare} />
            <span>Rename</span>
          </button>
          <button
            type="button"
            className="dropdown-item dropdown-item-danger"
            onClick={() => {
              const node = nodes.find((item) => item.id === menuNodeId);
              if (node) {
                setConfirmDeleteNode(node);
              }
              setMenuNodeId(null);
              setMenuPosition(null);
              setDeleteError(null);
            }}
          >
            <FontAwesomeIcon icon={faTrashCan} />
            <span>Delete</span>
          </button>
        </div>
      ) : null}

      {renameNode ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!renaming) {
              setRenameNode(null);
              setRenameError(null);
            }
          }}
        >
          <div
            className="modal-card"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Rename Node</h2>
            <p className="muted">Choose a new name for this node.</p>
            <label className="form-label" htmlFor="rename-node-name">
              Node Name
            </label>
            <input
              id="rename-node-name"
              value={renameValue}
              onChange={(event) => setRenameValue(event.target.value)}
              disabled={renaming}
              autoFocus
            />
            {renameError ? <p className="error">{renameError}</p> : null}
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={renaming}
                onClick={() => {
                  setRenameNode(null);
                  setRenameError(null);
                }}
              >
                Cancel
              </button>
              <button type="button" disabled={renaming} onClick={() => void confirmRename()}>
                {renaming ? "Saving..." : "Save Name"}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {confirmDeleteNode ? (
        <div
          className="modal-overlay"
          onClick={() => {
            if (!deleting) {
              setConfirmDeleteNode(null);
              setDeleteError(null);
            }
          }}
        >
          <div
            className="modal-card"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h2>Delete Node</h2>
            <p className="muted">
              Are you sure you want to delete <strong>{confirmDeleteNode.name}</strong>? This cannot be
              undone.
            </p>
            {deleteError ? <p className="error">{deleteError}</p> : null}
            <div className="modal-actions">
              <button
                type="button"
                className="secondary-button"
                disabled={deleting}
                onClick={() => {
                  setConfirmDeleteNode(null);
                  setDeleteError(null);
                }}
              >
                Cancel
              </button>
              <button
                type="button"
                className="danger-button"
                disabled={deleting}
                onClick={confirmDelete}
              >
                {deleting ? "Deleting..." : "Confirm Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
