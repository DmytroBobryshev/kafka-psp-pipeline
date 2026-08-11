import type { ConnectionState } from "../hooks/useEventStream";

const CONFIG: Record<ConnectionState, { label: string; dot: string; text: string }> = {
  idle: { label: "Idle", dot: "bg-slate-300", text: "text-slate-500" },
  connecting: { label: "Connecting…", dot: "bg-amber-400 animate-pulse", text: "text-amber-600" },
  live: { label: "Live", dot: "bg-emerald-500 animate-pulse", text: "text-emerald-600" },
  reconnecting: {
    label: "Reconnecting…",
    dot: "bg-amber-400 animate-pulse",
    text: "text-amber-600",
  },
  closed: { label: "Closed", dot: "bg-red-500", text: "text-red-600" },
};

interface Props {
  state: ConnectionState;
  onReconnect: () => void;
}

export function ConnectionStatus({ state, onReconnect }: Props) {
  const cfg = CONFIG[state];
  return (
    <div className="flex items-center gap-3">
      <span className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-1 text-sm font-medium shadow-sm">
        <span className={`h-2 w-2 rounded-full ${cfg.dot}`} aria-hidden />
        <span className={cfg.text}>{cfg.label}</span>
      </span>
      {state === "closed" && (
        <button
          type="button"
          onClick={onReconnect}
          className="text-sm font-medium text-indigo-600 hover:text-indigo-800 hover:underline"
        >
          Reconnect
        </button>
      )}
    </div>
  );
}
