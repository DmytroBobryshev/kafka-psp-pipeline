import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { getGroupLag, listGroups, listTopics } from "../api/clusterApi";

/**
 * Page 5: cluster ops - the AdminClient's view of the cluster (topics, consumer groups,
 * per-partition lag), the same numbers M19's drills read via kafka-consumer-groups.sh, now one
 * click away. Read-only by design: mutating cluster state belongs to operators and CRs, not a
 * demo UI.
 */
export function ClusterOpsPage() {
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null);

  const topics = useQuery({ queryKey: ["topics"], queryFn: listTopics });
  const groups = useQuery({ queryKey: ["groups"], queryFn: listGroups });
  const lag = useQuery({
    queryKey: ["lag", selectedGroup],
    queryFn: () => getGroupLag(selectedGroup!),
    enabled: !!selectedGroup,
  });

  return (
    <main className="mx-auto grid max-w-[1500px] gap-8 px-6 py-8 lg:grid-cols-2">
      <section>
        <h2 className="mb-3 text-base font-semibold">
          Topics {topics.data && <span className="text-sm font-normal text-slate-500">({topics.data.length})</span>}
        </h2>
        {topics.error && <p className="text-sm text-rose-600">{topics.error.message}</p>}
        <div className="max-h-[560px] overflow-auto rounded-lg border border-slate-300 bg-white">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-slate-100 text-left text-xs uppercase tracking-wide text-slate-600">
              <tr>
                <th className="px-4 py-2">Name</th>
                <th className="px-4 py-2 text-right">Partitions</th>
                <th className="px-4 py-2 text-right">RF</th>
              </tr>
            </thead>
            <tbody>
              {(topics.data ?? []).map((t) => (
                <tr key={t.name} className="border-t border-slate-200">
                  <td className="px-4 py-1.5 font-mono text-xs">{t.name}</td>
                  <td className="px-4 py-1.5 text-right">{t.partitionCount}</td>
                  <td className="px-4 py-1.5 text-right">{t.replicationFactor}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <h2 className="mb-3 text-base font-semibold">Consumer groups</h2>
        {groups.error && <p className="text-sm text-rose-600">{groups.error.message}</p>}
        <div className="space-y-2">
          {(groups.data ?? []).map((g) => (
            <div key={g.groupId} className="rounded-lg border border-slate-300 bg-white">
              <button
                onClick={() => setSelectedGroup(selectedGroup === g.groupId ? null : g.groupId)}
                className="flex w-full items-center justify-between px-4 py-2.5 text-left"
              >
                <span className="font-mono text-xs">{g.groupId}</span>
                <span className="text-xs text-slate-600">
                  {g.state} · {g.memberCount} member{g.memberCount === 1 ? "" : "s"}
                </span>
              </button>
              {selectedGroup === g.groupId && (
                <div className="border-t border-slate-200 px-4 py-3">
                  {lag.isPending && <p className="text-xs text-slate-500">loading lag…</p>}
                  {lag.error && <p className="text-xs text-rose-600">{lag.error.message}</p>}
                  {lag.data && (
                    <>
                      <p className="mb-2 text-sm">
                        total lag{" "}
                        <span
                          className={`font-mono font-semibold ${lag.data.totalLag > 0 ? "text-amber-600" : "text-emerald-600"}`}
                        >
                          {lag.data.totalLag}
                        </span>
                      </p>
                      <table className="w-full text-xs">
                        <thead className="text-left text-slate-600">
                          <tr>
                            <th className="py-1 pr-2">topic</th>
                            <th className="py-1 pr-2 text-right">p</th>
                            <th className="py-1 pr-2 text-right">current</th>
                            <th className="py-1 pr-2 text-right">end</th>
                            <th className="py-1 text-right">lag</th>
                          </tr>
                        </thead>
                        <tbody className="font-mono">
                          {lag.data.partitions
                            .filter((p) => p.lag > 0 || lag.data!.totalLag === 0)
                            .slice(0, 30)
                            .map((p) => (
                              <tr key={`${p.topic}-${p.partition}`} className="border-t border-slate-50">
                                <td className="py-0.5 pr-2">{p.topic}</td>
                                <td className="py-0.5 pr-2 text-right">{p.partition}</td>
                                <td className="py-0.5 pr-2 text-right">{p.currentOffset}</td>
                                <td className="py-0.5 pr-2 text-right">{p.endOffset}</td>
                                <td className={`py-0.5 text-right ${p.lag > 0 ? "text-amber-600" : ""}`}>
                                  {p.lag}
                                </td>
                              </tr>
                            ))}
                        </tbody>
                      </table>
                    </>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
