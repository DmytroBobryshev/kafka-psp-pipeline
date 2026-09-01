import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { peekDlq } from "../api/clusterApi";
import { DLQ_TOPICS, replayDlq } from "../api/dlqApi";
import type { DlqRecordView } from "../api/types";

/**
 * Page 3: DLQ console. Browsing is a non-destructive peek through realtime-gateway's ops API
 * (no offsets committed - looking never eats a record); replaying goes to the service that OWNS
 * the DLQ, because ownership is what makes replay safe: each owner's idempotency (M5 dedup,
 * webhook delivery dedup) absorbs a double-replay, and the Connect sink's DLQ - with no owning
 * service - deliberately has no replay button at all.
 */
export function DlqConsolePage() {
  const queryClient = useQueryClient();
  const [selected, setSelected] = useState<(typeof DLQ_TOPICS)[number]>(DLQ_TOPICS[0]);
  const [expanded, setExpanded] = useState<string | null>(null);

  const records = useQuery({
    queryKey: ["dlq", selected.topic],
    queryFn: () => peekDlq(selected.topic, 20),
  });

  const replay = useMutation({
    mutationFn: () => replayDlq(selected.replayPath!, 10),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["dlq", selected.topic] }),
  });

  return (
    <main className="mx-auto max-w-[1500px] px-6 py-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium text-slate-700">DLQ topic</span>
          <select
            value={selected.topic}
            onChange={(e) => {
              const t = DLQ_TOPICS.find((d) => d.topic === e.target.value)!;
              setSelected(t);
              setExpanded(null);
            }}
            className="w-[480px] max-w-full rounded-md border border-slate-400 px-3 py-2 font-mono text-xs"
          >
            {DLQ_TOPICS.map((d) => (
              <option key={d.topic} value={d.topic}>
                {d.topic}
              </option>
            ))}
          </select>
          <span className="mt-1 block text-xs text-slate-600">owner: {selected.owner}</span>
        </label>

        {selected.replayPath ? (
          <button
            onClick={() => replay.mutate()}
            disabled={replay.isPending || (records.data?.length ?? 0) === 0}
            className="min-w-36 rounded-md bg-slate-900 px-4 py-2 text-center text-sm font-medium text-white disabled:opacity-40"
          >
            {replay.isPending ? "Replaying…" : "Replay up to 10"}
          </button>
        ) : (
          <span className="text-xs text-slate-500">
            browse-only: this DLQ is written by Kafka Connect, no service owns a replay path
          </span>
        )}
      </div>

      {replay.data && (
        <div className="mb-4 rounded-md border border-emerald-300 bg-emerald-50 px-4 py-3 text-sm text-emerald-800">
          Replayed {replay.data.replayedCount} record(s) from{" "}
          <span className="font-mono text-xs">{replay.data.dlqTopic}</span> back to{" "}
          <span className="font-mono text-xs">{replay.data.republishedToTopic}</span>.
        </div>
      )}
      {replay.error && <p className="mb-4 text-sm text-rose-600">{replay.error.message}</p>}
      {records.error && (
        <p className="mb-4 text-sm text-rose-600">Failed to peek: {records.error.message}</p>
      )}

      <div className="space-y-2">
        {(records.data ?? []).map((r: DlqRecordView) => {
          const id = `${r.partition}-${r.offset}`;
          const isOpen = expanded === id;
          return (
            <div key={id} className="rounded-lg border border-slate-300 bg-white">
              <button
                onClick={() => setExpanded(isOpen ? null : id)}
                className="flex w-full items-center justify-between px-4 py-3 text-left"
              >
                <span className="font-mono text-xs">
                  p{r.partition} · offset {r.offset} · key {r.keyString ?? "null"}
                </span>
                <span className="text-xs text-slate-600">
                  {new Date(r.timestamp).toLocaleString()}
                  <span className="ml-3 text-slate-500">{isOpen ? "▲" : "▼"}</span>
                </span>
              </button>
              {isOpen && (
                <div className="border-t border-slate-200 px-4 py-3">
                  <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                    Retry / dead-letter headers
                  </h4>
                  <dl className="mb-4 grid grid-cols-[280px_1fr] gap-y-1 text-xs">
                    {Object.entries(r.headers).map(([k, v]) => (
                      <div key={k} className="contents">
                        <dt className="font-mono text-slate-600">{k}</dt>
                        <dd className={`font-mono ${k.includes("stacktrace") ? "whitespace-pre-wrap break-all text-[11px] leading-tight" : "break-all"}`}>
                          {v}
                        </dd>
                      </div>
                    ))}
                  </dl>
                  <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-600">
                    Value {r.valueBase64 && "(binary Avro, base64)"}
                  </h4>
                  <pre className="max-h-48 overflow-auto rounded bg-slate-100 p-3 font-mono text-[11px] leading-snug">
                    {r.valuePreview}
                  </pre>
                </div>
              )}
            </div>
          );
        })}
        {records.data?.length === 0 && (
          <p className="rounded-lg border border-dashed border-slate-300 bg-slate-100 px-4 py-8 text-center text-sm text-slate-500">
            DLQ is empty - which is the state you want it in.
          </p>
        )}
      </div>
    </main>
  );
}
