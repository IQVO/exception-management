/**
 * Wire shapes of exception-management's REST API (apis/openapi.yaml; the JSON
 * field names are the truth in internal/adapters/inbound/http/dto.go). JSON
 * is camelCase; optional parts of a resource are OMITTED, never null, except
 * dedupKey.siteId and note, which are genuinely nullable.
 */

/** The fixed status machine: OPEN -> TRIAGED -> ASSIGNED -> RESOLVED (terminal). */
export const CASE_STATUSES = ["OPEN", "TRIAGED", "ASSIGNED", "RESOLVED"] as const;
export type CaseStatus = (typeof CASE_STATUSES)[number];

export const SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"] as const;
export type Severity = (typeof SEVERITIES)[number];

/** One transition of the case, in order; `action` is one of OPENED, TOUCHED,
 *  TRIAGED, ASSIGNED, ESCALATED, RESOLVED, AUTO_RESOLVED. */
export interface AuditEntry {
  action: string;
  actor: string;
  at: string;
}

/** What opened the case: the source fact's type, its subject and its site
 *  (null when the fact carried none). */
export interface DedupKey {
  sourceType: string;
  sourceSubject: string;
  siteId: string | null;
}

export interface ExceptionCase {
  caseId: string;
  dedupKey: DedupKey;
  status: CaseStatus;
  severity: Severity;
  queue: string;
  occurrenceCount: number;
  escalated: boolean;
  /** Absent before the first assignment. */
  assignee?: string;
  /** Absent until RESOLVED. */
  resolutionCode?: string;
  /** Absent until RESOLVED. */
  rootCause?: string;
  /** Absent until RESOLVED; null when resolved without a note. */
  note?: string | null;
  /** Absent until the case is resolved or auto-resolved. */
  resolvedAt?: string;
  /** Present only on an auto-resolved case. */
  autoResolveRuleId?: string;
  openedAt: string;
  lastSeenAt: string;
  slaDueAt: string;
  /** The CloudEvents id of the source fact that opened the case. */
  sourceEventId: string;
  /** The frozen copy of the source fact's data; never changes after open. */
  sourcePayload: Record<string, unknown>;
  auditTrail: AuditEntry[];
  /** Starts at 1, +1 per accepted transition. */
  version: number;
}

/** The auto-resolve rule of a policy: the correlated CloudEvents type that
 *  closes a case and how fresh it must be. Null/absent when the source type
 *  has no rule. */
export interface AutoResolveRule {
  onType: string;
  withinMinutes: number;
}

export interface ExceptionPolicy {
  sourceType: string;
  version: number;
  severity: Severity;
  queue: string;
  slaMinutes: number;
  autoResolve?: AutoResolveRule | null;
  declaredAt: string;
}

export interface ListExceptionsQuery {
  status?: CaseStatus;
  queue?: string;
  site?: string;
  /** The full CloudEvents type of the source fact. */
  sourceType?: string;
}

/** The body of POST /exceptions/{caseId}/triage. */
export interface TriageBody {
  severity: Severity;
  queue: string;
}

/** The body of POST /exceptions/{caseId}/assign. */
export interface AssignBody {
  assignee: string;
}

/** The body of POST /exceptions/{caseId}/resolve. `note` omitted when empty. */
export interface ResolveBody {
  resolutionCode: string;
  rootCause: string;
  note?: string;
}
