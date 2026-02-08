import { useEffect, useMemo, useState } from "react";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faCircleCheck, faCircleInfo, faTriangleExclamation } from "@fortawesome/free-solid-svg-icons";

export type ToastType = "success" | "error" | "info";

export type Toast = {
  id: string;
  type: ToastType;
  title: string;
  message?: string;
};

type ToastHostProps = {
  toasts: Toast[];
  onDismiss: (id: string) => void;
  durationMs?: number;
};

export default function ToastHost({ toasts, onDismiss, durationMs = 4200 }: ToastHostProps) {
  const [closingIds, setClosingIds] = useState<Set<string>>(new Set());

  const handleClose = useMemo(
    () => (id: string) => {
      setClosingIds((current) => {
        const next = new Set(current);
        next.add(id);
        return next;
      });
      window.setTimeout(() => onDismiss(id), 180);
    },
    [onDismiss]
  );

  useEffect(() => {
    if (toasts.length === 0) return;
    const timers = toasts.map((toast) =>
      window.setTimeout(() => {
        handleClose(toast.id);
      }, durationMs)
    );
    return () => {
      timers.forEach((timer) => window.clearTimeout(timer));
    };
  }, [toasts, durationMs, handleClose]);

  const getIcon = (type: ToastType) => {
    switch (type) {
      case "success":
        return faCircleCheck;
      case "error":
        return faTriangleExclamation;
      default:
        return faCircleInfo;
    }
  };

  return (
    <div className="toast-container" aria-live="polite" aria-atomic="true">
      {toasts.map((toast) => (
        <div
          key={toast.id}
          className={`toast toast-${toast.type} ${closingIds.has(toast.id) ? "toast-leave" : "toast-enter"}`}
        >
          <div className="toast-icon">
            <FontAwesomeIcon icon={getIcon(toast.type)} />
          </div>
          <div className="toast-body">
            <strong>{toast.title}</strong>
            {toast.message ? <p>{toast.message}</p> : null}
          </div>
          <button className="toast-close" type="button" aria-label="Dismiss" onClick={() => handleClose(toast.id)}>
            ×
          </button>
        </div>
      ))}
    </div>
  );
}
