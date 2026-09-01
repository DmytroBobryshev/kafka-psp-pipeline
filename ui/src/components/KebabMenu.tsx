import { useState } from "react";

export interface KebabItem {
  label: string;
  onClick: () => void;
  disabled?: boolean;
  title?: string;
  tone?: "danger";
}

export function KebabMenu({ items }: { items: KebabItem[] }) {
  // position:fixed escapes any overflow container's clipping, so the menu never gets cut off
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);
  return (
    <>
      <button
        onClick={(e) => {
          e.stopPropagation();
          if (pos) {
            setPos(null);
            return;
          }
          const r = e.currentTarget.getBoundingClientRect();
          setPos({ top: r.bottom + 4, left: r.right - 192 });
        }}
        title="Actions"
        className="rounded-md border border-slate-400 bg-white px-2 py-0.5 text-base font-bold leading-tight text-slate-700 shadow-sm hover:bg-slate-200 hover:text-slate-900"
      >
        ⋮
      </button>
      {pos && (
        <>
          <div
            className="fixed inset-0 z-10"
            onClick={(e) => {
              e.stopPropagation();
              setPos(null);
            }}
          />
          <div
            style={{ top: pos.top, left: pos.left }}
            className="fixed z-20 w-48 rounded-md border border-slate-300 bg-white py-1 text-left shadow-lg"
          >
            {items.map((it) => (
              <button
                key={it.label}
                disabled={it.disabled}
                title={it.title}
                onClick={(e) => {
                  e.stopPropagation();
                  setPos(null);
                  it.onClick();
                }}
                className={`block w-full px-3 py-1.5 text-left text-xs disabled:opacity-40 ${
                  it.tone === "danger" ? "text-rose-700 hover:bg-rose-50" : "text-slate-700 hover:bg-slate-100"
                }`}
              >
                {it.label}
              </button>
            ))}
          </div>
        </>
      )}
    </>
  );
}
