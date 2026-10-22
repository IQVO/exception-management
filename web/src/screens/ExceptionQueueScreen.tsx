import { useState } from "react";
import { Link } from "react-router-dom";
import { Card, DataTable } from "@warehouse/ui-kit";
import { listExceptions } from "../api";
import { Form, FormRow, SelectField, SubmitButton, TextField } from "../components/formkit";
import { CaseStatusPill, PageHeader, SeverityPill, Stack } from "../components/layout";
import { RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { optionalShortIdProblem, optionalTextProblem, utcLabel } from "../lib";
import { CASE_STATUSES } from "../types";
import type { CaseStatus, ExceptionCase } from "../types";

const STATUS_OPTIONS = CASE_STATUSES.map((s) => ({ value: s, label: s }));

interface Filters {
  status: "" | CaseStatus;
  queue: string;
  site: string;
  sourceType: string;
}

const INITIAL: Filters = { status: "", queue: "", site: "", sourceType: "" };

/**
 * The problem-solve queue: GET /exceptions, newest first, narrowed by status,
 * queue, site and source type (the full CloudEvents type of the source fact).
 * Without a status filter resolved cases are listed too -- resolve activity is
 * part of the record. Cases are never created here: every case opens from a
 * consumed fact.
 */
export function ExceptionQueueScreen() {
  const [draft, setDraft] = useState<Filters>(INITIAL);
  const [applied, setApplied] = useState<Filters>(INITIAL);
  const cases = useRequest(
    () =>
      listExceptions({
        status: applied.status || undefined,
        queue: applied.queue || undefined,
        site: applied.site || undefined,
        sourceType: applied.sourceType || undefined,
      }),
    `exceptions|${applied.status}|${applied.queue}|${applied.site}|${applied.sourceType}`,
  );

  const problems = [
    optionalShortIdProblem("queue", draft.queue),
    optionalTextProblem("site", draft.site),
    optionalTextProblem("source type", draft.sourceType),
  ];
  const apply = () => {
    if (problems.some((p) => p !== null)) return;
    setApplied({
      status: draft.status,
      queue: draft.queue.trim(),
      site: draft.site.trim(),
      sourceType: draft.sourceType.trim(),
    });
  };

  return (
    <Stack gap={5}>
      <PageHeader
        title="Problem-solve queue"
        subtitle="exception-management · cases opened from consumed facts, never created here"
      />
      <Card title="Filters">
        <Form label="Exception filters" onSubmit={apply}>
          <FormRow>
            <SelectField
              label="Status"
              value={draft.status}
              onChange={(v) => setDraft({ ...draft, status: v as Filters["status"] })}
              options={STATUS_OPTIONS}
              emptyLabel="Any status"
            />
            <TextField label="Queue" value={draft.queue} onChange={(v) => setDraft({ ...draft, queue: v })} placeholder="Every queue" />
            <TextField label="Site" value={draft.site} onChange={(v) => setDraft({ ...draft, site: v })} placeholder="Every site" />
            <TextField
              label="Source type"
              value={draft.sourceType}
              onChange={(v) => setDraft({ ...draft, sourceType: v })}
              placeholder="Every source type"
              hint="The full CloudEvents type, e.g. …task.TaskCPTMissed"
            />
            <SubmitButton disabled={problems.some((p) => p !== null)}>Apply filters</SubmitButton>
          </FormRow>
        </Form>
      </Card>
      <Card title={`Cases${cases.status === "success" ? ` (${cases.data.length})` : ""}`}>
        <RequestView
          state={cases}
          what="exception cases"
          isEmpty={(items) => items.length === 0}
          empty="No cases match these filters."
        >
          {(items) => <CaseTable cases={items} />}
        </RequestView>
      </Card>
    </Stack>
  );
}

function CaseTable({ cases }: { cases: ExceptionCase[] }) {
  return (
    <DataTable
      rowKey={(c) => c.caseId}
      rows={cases}
      columns={[
        {
          key: "case",
          header: "Case",
          render: (c) => <Link to={`../exceptions/${encodeURIComponent(c.caseId)}`}>{c.caseId}</Link>,
        },
        { key: "status", header: "Status", render: (c) => <CaseStatusPill status={c.status} /> },
        { key: "severity", header: "Severity", render: (c) => <SeverityPill severity={c.severity} /> },
        { key: "queue", header: "Queue", render: (c) => c.queue },
        { key: "source", header: "Source subject", render: (c) => c.dedupKey.sourceSubject },
        { key: "occurrences", header: "Seen", align: "right", render: (c) => String(c.occurrenceCount) },
        {
          key: "sla",
          header: "SLA due",
          render: (c) => (
            <span style={{ fontWeight: c.escalated ? 600 : 400 }}>{utcLabel(c.slaDueAt)}</span>
          ),
        },
        { key: "opened", header: "Opened", render: (c) => utcLabel(c.openedAt) },
      ]}
    />
  );
}
