import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Card, DataTable } from "@warehouse/ui-kit";
import { assignException, getException, resolveException, toApiError, triageException } from "../api";
import type { ApiError } from "../api";
import { Form, FormRow, InlineError, InlineSuccess, SelectField, SubmitButton, TextField } from "../components/formkit";
import { CaseStatusPill, Facts, PageHeader, SeverityPill, Stack } from "../components/layout";
import { ProblemAlert, RequestView } from "../components/ProblemAlert";
import { useRequest } from "../hooks/useRequest";
import { codeProblem, noteProblem, shortIdProblem, utcLabel } from "../lib";
import { SEVERITIES } from "../types";
import type { AuditEntry, ExceptionCase, Severity } from "../types";

/** Reads :caseId and keys the screen on it, so moving between cases never
 *  carries form state over. */
export function CaseDetailRoute() {
  const { caseId = "" } = useParams();
  return <CaseDetailScreen key={caseId} caseId={caseId} />;
}

/**
 * One exception case: the frozen source fact, the SLA clock, the audit trail
 * (one entry per transition) and, while the case is not resolved, the
 * triage/assign/resolve forms. Every submission is its own POST carrying a
 * fresh Idempotency-Key (minted per submission in api.ts), so a retry or a
 * double-click replays the same logical command instead of applying it twice.
 */
export function CaseDetailScreen({ caseId }: { caseId: string }) {
  const request = useRequest(() => getException(caseId), `case|${caseId}`);
  // The success note lives HERE: a successful command reloads the case; when
  // the command resolved it, the forms unmount with the reloaded case.
  const [note, setNote] = useState<string | null>(null);
  return (
    <Stack gap={5}>
      <div>
        <Link to="../exceptions">← Problem-solve queue</Link>
      </div>
      <RequestView state={request} what="exception case">
        {(c) => (
          <CaseBody
            case_={c}
            note={note}
            onDone={(what) => {
              setNote(what);
              request.reload();
            }}
          />
        )}
      </RequestView>
    </Stack>
  );
}

function CaseBody({
  case_,
  note,
  onDone,
}: {
  case_: ExceptionCase;
  note: string | null;
  onDone: (what: string) => void;
}) {
  const resolved = case_.status === "RESOLVED";
  return (
    <>
      <PageHeader title={case_.caseId} subtitle="exception-management · problem-solve case" />
      {note && !resolved && <InlineSuccess message={note} />}
      <Card title="Case">
        <Facts
          items={[
            { label: "Status", value: <CaseStatusPill status={case_.status} /> },
            { label: "Severity", value: <SeverityPill severity={case_.severity} /> },
            { label: "Queue", value: case_.queue },
            { label: "Assignee", value: case_.assignee ?? "—" },
            { label: "Occurrences", value: String(case_.occurrenceCount) },
            { label: "Escalated", value: case_.escalated ? "yes — SLA breached" : "no" },
            { label: "Opened", value: utcLabel(case_.openedAt) },
            { label: "Last seen", value: utcLabel(case_.lastSeenAt) },
            { label: "SLA due", value: utcLabel(case_.slaDueAt) },
            { label: "Source type", value: case_.dedupKey.sourceType },
            { label: "Source subject", value: case_.dedupKey.sourceSubject },
            { label: "Site", value: case_.dedupKey.siteId ?? "—" },
            ...(case_.resolutionCode ? [{ label: "Resolution code", value: case_.resolutionCode }] : []),
            ...(case_.rootCause ? [{ label: "Root cause", value: case_.rootCause }] : []),
            ...(case_.note != null ? [{ label: "Note", value: case_.note || "—" }] : []),
            ...(case_.resolvedAt ? [{ label: "Resolved", value: utcLabel(case_.resolvedAt) }] : []),
            ...(case_.autoResolveRuleId ? [{ label: "Auto-resolve rule", value: case_.autoResolveRuleId }] : []),
            { label: "Version", value: String(case_.version) },
          ]}
        />
      </Card>
      <Card title="Source fact (frozen at open)">
        <pre
          style={{
            margin: 0,
            fontFamily: "var(--wh-font-mono)",
            fontSize: "var(--wh-font-size-sm)",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {JSON.stringify(case_.sourcePayload, null, 2)}
        </pre>
      </Card>
      <Card title="Audit trail">
        <AuditTable entries={case_.auditTrail} />
      </Card>
      {!resolved && (
        <>
          <TriageCard case_={case_} onDone={onDone} />
          <AssignCard case_={case_} onDone={onDone} />
          <ResolveCard case_={case_} onDone={onDone} />
        </>
      )}
      {resolved && (
        <Card title="Closed">
          <div style={{ fontSize: "var(--wh-font-size-sm)", color: "var(--wh-color-text-muted)" }}>
            RESOLVED is terminal: a source fact arriving later only touches the case (occurrence count,
            last seen) and never reopens it.
          </div>
        </Card>
      )}
    </>
  );
}

function AuditTable({ entries }: { entries: AuditEntry[] }) {
  return (
    <DataTable
      rowKey={(e) => `${e.action}-${e.at}-${e.actor}`}
      rows={entries}
      columns={[
        { key: "action", header: "Action", render: (e) => e.action },
        { key: "actor", header: "Actor", render: (e) => e.actor },
        { key: "at", header: "At", render: (e) => utcLabel(e.at) },
      ]}
    />
  );
}

/** Operator guidance for the problems the commands can actually hit. */
function commandLead(error: ApiError): string | undefined {
  if (error.slug === "case-already-resolved") {
    return "This case is already resolved -- RESOLVED is terminal and accepts no further command. Reload to see the latest state.";
  }
  if (error.slug === "concurrent-modification") {
    return "The case changed under you (another request applied a transition). Reload and submit again.";
  }
  return undefined;
}

function TriageCard({ case_, onDone }: { case_: ExceptionCase; onDone: (what: string) => void }) {
  const [severity, setSeverity] = useState<Severity>(case_.severity);
  const [queue, setQueue] = useState(case_.queue);
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [saving, setSaving] = useState(false);
  const queueMsg = shortIdProblem("queue", queue);

  const submit = async () => {
    if (queueMsg) return;
    setProblem(null);
    setSaving(true);
    try {
      await triageException(case_.caseId, { severity, queue: queue.trim() });
      onDone("Case triaged.");
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Triage">
      <Form label="Triage case" onSubmit={() => void submit()}>
        <FormRow>
          <SelectField
            label="Severity"
            value={severity}
            onChange={(v) => setSeverity(v as Severity)}
            options={SEVERITIES.map((s) => ({ value: s, label: s }))}
          />
          <TextField label="Queue" value={queue} onChange={setQueue} placeholder="cps-front" required />
          <SubmitButton disabled={queueMsg !== null || saving}>Triage case</SubmitButton>
        </FormRow>
        <InlineError message={queueMsg} />
        {problem && <ProblemAlert error={problem} lead={commandLead(problem)} />}
      </Form>
    </Card>
  );
}

function AssignCard({ case_, onDone }: { case_: ExceptionCase; onDone: (what: string) => void }) {
  const [assignee, setAssignee] = useState(case_.assignee ?? "");
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [saving, setSaving] = useState(false);
  const assigneeMsg = shortIdProblem("assignee", assignee);

  const submit = async () => {
    if (assigneeMsg) return;
    setProblem(null);
    setSaving(true);
    try {
      await assignException(case_.caseId, { assignee: assignee.trim() });
      onDone("Case assigned.");
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Assign">
      <Form label="Assign case" onSubmit={() => void submit()}>
        <FormRow>
          <TextField label="Assignee" value={assignee} onChange={setAssignee} placeholder="op-marc" required />
          <SubmitButton disabled={assigneeMsg !== null || saving}>
            {case_.assignee ? "Reassign case" : "Assign case"}
          </SubmitButton>
        </FormRow>
        <InlineError message={assigneeMsg} />
        {problem && <ProblemAlert error={problem} lead={commandLead(problem)} />}
      </Form>
    </Card>
  );
}

function ResolveCard({ case_, onDone }: { case_: ExceptionCase; onDone: (what: string) => void }) {
  const [resolutionCode, setResolutionCode] = useState("");
  const [rootCause, setRootCause] = useState("");
  const [note, setNote] = useState("");
  const [problem, setProblem] = useState<ApiError | null>(null);
  const [saving, setSaving] = useState(false);
  const codeMsg = codeProblem("resolution code", resolutionCode);
  const causeMsg = codeProblem("root cause", rootCause);
  const noteMsg = noteProblem(note);

  const submit = async () => {
    if (codeMsg || causeMsg || noteMsg) return;
    setProblem(null);
    setSaving(true);
    try {
      await resolveException(case_.caseId, {
        resolutionCode: resolutionCode.trim(),
        rootCause: rootCause.trim(),
        ...(note.trim() === "" ? {} : { note: note.trim() }),
      });
      onDone("Case resolved.");
    } catch (err) {
      setProblem(toApiError(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card title="Resolve (terminal)">
      <Form label="Resolve case" onSubmit={() => void submit()}>
        <FormRow>
          <TextField
            label="Resolution code"
            value={resolutionCode}
            onChange={setResolutionCode}
            placeholder="WORK_COMPLETED"
            hint="A code, not prose: uppercase letters, digits, underscores."
            required
          />
          <TextField
            label="Root cause"
            value={rootCause}
            onChange={setRootCause}
            placeholder="OPERATOR_UNAVAILABLE"
            hint="A code, not prose: uppercase letters, digits, underscores."
            required
          />
        </FormRow>
        <TextField label="Note (optional)" value={note} onChange={setNote} placeholder="What was done, in a sentence" />
        <FormRow>
          <SubmitButton danger disabled={codeMsg !== null || causeMsg !== null || noteMsg !== null || saving}>
            Resolve case
          </SubmitButton>
        </FormRow>
        <InlineError message={codeMsg ?? causeMsg ?? noteMsg} />
        {problem && <ProblemAlert error={problem} lead={commandLead(problem)} />}
      </Form>
    </Card>
  );
}
