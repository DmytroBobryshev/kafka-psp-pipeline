import { apiFetch } from "./client";
import type { ConsumerGroupInfo, DlqRecordView, GroupLagResponse, TopicInfo } from "./types";

/**
 * realtime-gateway's AdminClient-backed ops API (M17 page 5). Everything is read-only
 * inspection; the DLQ peek is non-destructive (no offsets committed), so browsing never
 * consumes a record out from under the replay endpoints.
 */
export const listTopics = () => apiFetch<TopicInfo[]>("/api/realtime/cluster/topics");

export const listGroups = () => apiFetch<ConsumerGroupInfo[]>("/api/realtime/cluster/groups");

export const getGroupLag = (groupId: string) =>
  apiFetch<GroupLagResponse>(`/api/realtime/cluster/groups/${encodeURIComponent(groupId)}/lag`);

export const peekDlq = (topic: string, max: number) =>
  apiFetch<{ topic: string; count: number; records: DlqRecordView[] }>(
    `/api/realtime/cluster/dlq/${encodeURIComponent(topic)}/records?max=${max}`,
  ).then((r) => r.records);
