import type { ExceptionCase, ExceptionPolicy } from "../types";

/** Pinned to apis/openapi.yaml's ExceptionCase example (the wire shape is the
 *  truth in internal/adapters/inbound/http/dto.go), not to a sibling
 *  context's conventions. */
export const CASE_ID = "case-1d2e3f4a-5b6c-4789-9abc-def012345678";
export const SOURCE_TYPE = "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed";

export function openCase(): ExceptionCase {
  return {
    caseId: CASE_ID,
    dedupKey: {
      sourceType: SOURCE_TYPE,
      sourceSubject: "task-8a1f",
      siteId: null,
    },
    status: "OPEN",
    severity: "HIGH",
    queue: "cps-front",
    occurrenceCount: 3,
    escalated: false,
    openedAt: "2026-10-21T09:00:05Z",
    lastSeenAt: "2026-10-21T09:20:05Z",
    slaDueAt: "2026-10-21T09:30:05Z",
    sourceEventId: "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
    sourcePayload: {
      task_id: "task-8a1f",
      order_ref: "wu-8a1f",
      task_type: "PICK",
      cpt: "2026-09-30T14:55:00Z",
    },
    auditTrail: [
      { action: "OPENED", actor: "system", at: "2026-10-21T09:00:05Z" },
      { action: "TOUCHED", actor: "system", at: "2026-10-21T09:10:05Z" },
      { action: "TOUCHED", actor: "system", at: "2026-10-21T09:20:05Z" },
    ],
    version: 1,
  };
}

/** A case past its triage, with an owner, as the assign/resolve flows see it. */
export function assignedCase(): ExceptionCase {
  return {
    ...openCase(),
    status: "ASSIGNED",
    assignee: "op-marc",
    auditTrail: [
      ...openCase().auditTrail,
      { action: "TRIAGED", actor: "op-marc", at: "2026-10-21T09:25:05Z" },
      { action: "ASSIGNED", actor: "op-marc", at: "2026-10-21T09:26:05Z" },
    ],
    version: 3,
  };
}

/** A terminal case: resolution fields present, no further command possible. */
export function resolvedCase(): ExceptionCase {
  return {
    ...assignedCase(),
    status: "RESOLVED",
    resolutionCode: "WORK_COMPLETED",
    rootCause: "OPERATOR_UNAVAILABLE",
    note: "Picked during the recovery sweep.",
    resolvedAt: "2026-10-21T10:02:11Z",
    auditTrail: [
      ...assignedCase().auditTrail,
      { action: "RESOLVED", actor: "op-marc", at: "2026-10-21T10:02:11Z" },
    ],
    version: 4,
  };
}

export function policy(): ExceptionPolicy {
  return {
    sourceType: SOURCE_TYPE,
    version: 1,
    severity: "HIGH",
    queue: "cps-front",
    slaMinutes: 30,
    autoResolve: {
      onType: "com.warehouse.wes.order-management.order.OrderRepromised",
      withinMinutes: 60,
    },
    declaredAt: "2026-10-21T08:00:00Z",
  };
}

/** A source type governed without an auto-resolve rule. */
export function manualPolicy(): ExceptionPolicy {
  return {
    ...policy(),
    sourceType: "com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected",
    severity: "MEDIUM",
    queue: "cps-inventory",
    slaMinutes: 120,
    autoResolve: null,
    declaredAt: "2026-10-21T08:05:00Z",
  };
}
