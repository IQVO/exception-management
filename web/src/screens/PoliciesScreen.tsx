import { Card, DataTable, NO_VALUE } from "@warehouse/ui-kit";
import { listExceptionPolicies } from "../api";
import { SeverityPill, PageHeader, Stack } from "../components/layout";
import { RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { utcLabel } from "../lib";
import type { ExceptionPolicy } from "../types";

/**
 * The handling-policy table: GET /exception-policies, the current version of
 * every declared source type. v1 of this remote is read-only here --
 * declaration stays an API/automation concern (PUT /exception-policies/{type},
 * versioned append-only), so nothing in this screen writes.
 */
export function PoliciesScreen() {
  const policies = useRequest(() => listExceptionPolicies(), "policies");
  return (
    <Stack gap={5}>
      <PageHeader
        title="Exception policies"
        subtitle="exception-management · severity, queue, SLA and auto-resolve per source type"
      />
      <Card title={`Policies${policies.status === "success" ? ` (${policies.data.length})` : ""}`}>
        <RequestView
          state={policies}
          what="exception policies"
          isEmpty={(items) => items.length === 0}
          empty="No policies declared. A source type with no declared policy is not governed yet."
        >
          {(items) => <PolicyTable policies={items} />}
        </RequestView>
      </Card>
    </Stack>
  );
}

function PolicyTable({ policies }: { policies: ExceptionPolicy[] }) {
  return (
    <DataTable
      rowKey={(p) => p.sourceType}
      rows={policies}
      columns={[
        { key: "sourceType", header: "Source type", render: (p) => p.sourceType },
        { key: "version", header: "Version", align: "right", render: (p) => String(p.version) },
        { key: "severity", header: "Severity", render: (p) => <SeverityPill severity={p.severity} /> },
        { key: "queue", header: "Queue", render: (p) => p.queue },
        { key: "sla", header: "SLA (min)", align: "right", render: (p) => String(p.slaMinutes) },
        {
          key: "autoResolve",
          header: "Auto-resolve",
          render: (p) =>
            p.autoResolve
              ? `on ${p.autoResolve.onType} within ${p.autoResolve.withinMinutes} min`
              : NO_VALUE,
        },
        { key: "declared", header: "Declared", render: (p) => utcLabel(p.declaredAt) },
      ]}
    />
  );
}
