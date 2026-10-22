import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { CaseDetailScreen } from "./CaseDetailScreen";
import { json, mockApi, problem } from "../test/fetchMock";
import { CASE_ID, assignedCase, openCase, resolvedCase } from "../test/fixtures";

function mount() {
  return render(
    <MemoryRouter initialEntries={[`/exceptions/${CASE_ID}`]}>
      <CaseDetailScreen caseId={CASE_ID} />
    </MemoryRouter>,
  );
}

function routes(case_: ReturnType<typeof openCase>) {
  return mockApi({
    [`GET /exceptions/${CASE_ID}`]: json(case_),
    [`POST /exceptions/${CASE_ID}/triage`]: json({ ...case_, status: "TRIAGED" }),
    [`POST /exceptions/${CASE_ID}/assign`]: json({ ...case_, status: "ASSIGNED", assignee: "op-marc" }),
    [`POST /exceptions/${CASE_ID}/resolve`]: json(resolvedCase()),
  });
}

describe("CaseDetailScreen", () => {
  it("shows the case facts, the frozen source payload and the audit trail", async () => {
    routes(openCase());
    mount();
    expect(await screen.findByText("task-8a1f")).toBeInTheDocument();
    expect(screen.getByText(/TaskCPTMissed/)).toBeInTheDocument(); // source type
    // the frozen payload renders as one <pre> text node; match inside it
    expect(screen.getByText(/"order_ref": "wu-8a1f"/)).toBeInTheDocument();
    expect(screen.getByText("OPENED")).toBeInTheDocument();
    expect(screen.getAllByText("TOUCHED")).toHaveLength(2);
    expect(screen.getAllByText("system")).toHaveLength(3); // actor of OPENED + 2x TOUCHED
    expect(screen.getByText("3")).toBeInTheDocument(); // occurrence count
    expect(screen.getByText("2026-10-21 09:30Z")).toBeInTheDocument(); // SLA due
  });

  it("triages: POSTs severity + queue with a fresh Idempotency-Key, then reloads", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    await userEvent.selectOptions(screen.getByLabelText("Severity"), "CRITICAL");
    const queue = screen.getByLabelText(/^Queue/); // required label reads "Queue *"
    await userEvent.clear(queue);
    await userEvent.type(queue, "cps-inbound");
    await userEvent.click(screen.getByRole("button", { name: "Triage case" }));
    expect(await screen.findByText("Case triaged.")).toBeInTheDocument();
    const post = api.to(`POST /exceptions/${CASE_ID}/triage`)[0];
    expect(post.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(post.body).toEqual({ severity: "CRITICAL", queue: "cps-inbound" });
    // the reload after the command re-fetched the case
    expect(api.to(`GET /exceptions/${CASE_ID}`).length).toBeGreaterThanOrEqual(2);
  });

  it("blocks an empty queue in the triage form (no POST leaves the page)", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    const queue = screen.getByLabelText(/^Queue/);
    await userEvent.clear(queue);
    expect(screen.getByRole("button", { name: "Triage case" })).toBeDisabled();
    expect(screen.getByText("A queue is required.")).toBeInTheDocument();
    expect(api.to(`POST /exceptions/${CASE_ID}/triage`)).toHaveLength(0);
  });

  it("assigns: POSTs the operator id with a fresh Idempotency-Key", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    await userEvent.type(screen.getByLabelText(/^Assignee/), "op-marc");
    await userEvent.click(screen.getByRole("button", { name: "Assign case" }));
    expect(await screen.findByText("Case assigned.")).toBeInTheDocument();
    const post = api.to(`POST /exceptions/${CASE_ID}/assign`)[0];
    expect(post.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(post.body).toEqual({ assignee: "op-marc" });
  });

  it("offers REASSIGN on a case that already has an owner", async () => {
    routes(assignedCase());
    mount();
    expect(await screen.findByRole("button", { name: "Reassign case" })).toBeInTheDocument();
  });

  it("resolves: POSTs the codes, omits an empty note, then shows the terminal case", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    await userEvent.type(screen.getByLabelText(/^Resolution code/), "WORK_COMPLETED");
    await userEvent.type(screen.getByLabelText(/^Root cause/), "OPERATOR_UNAVAILABLE");
    await userEvent.click(screen.getByRole("button", { name: "Resolve case" }));
    expect(await screen.findByText("Case resolved.")).toBeInTheDocument();
    const post = api.to(`POST /exceptions/${CASE_ID}/resolve`)[0];
    expect(post.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(post.body).toEqual({ resolutionCode: "WORK_COMPLETED", rootCause: "OPERATOR_UNAVAILABLE" });
  });

  it("sends the note only when the operator typed one", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    await userEvent.type(screen.getByLabelText(/^Resolution code/), "WORK_COMPLETED");
    await userEvent.type(screen.getByLabelText(/^Root cause/), "OPERATOR_UNAVAILABLE");
    await userEvent.type(screen.getByLabelText(/Note \(optional\)/), "Picked during the recovery sweep.");
    await userEvent.click(screen.getByRole("button", { name: "Resolve case" }));
    expect(await screen.findByText("Case resolved.")).toBeInTheDocument();
    expect(api.to(`POST /exceptions/${CASE_ID}/resolve`)[0].body).toEqual({
      resolutionCode: "WORK_COMPLETED",
      rootCause: "OPERATOR_UNAVAILABLE",
      note: "Picked during the recovery sweep.",
    });
  });

  it("rejects prose resolution codes before any POST (invalid-resolution is the API's word)", async () => {
    const api = routes(openCase());
    mount();
    await screen.findByText("OPENED");
    await userEvent.type(screen.getByLabelText(/^Resolution code/), "work completed");
    await userEvent.type(screen.getByLabelText(/^Root cause/), "OPERATOR_UNAVAILABLE");
    expect(screen.getByRole("button", { name: "Resolve case" })).toBeDisabled();
    expect(screen.getByText(/uppercase letters, digits and underscores/)).toBeInTheDocument();
    expect(api.to(`POST /exceptions/${CASE_ID}/resolve`)).toHaveLength(0);
  });

  it("words the terminal 409 for the operator and keeps the case on screen", async () => {
    mockApi({
      [`GET /exceptions/${CASE_ID}`]: json(openCase()),
      [`POST /exceptions/${CASE_ID}/resolve`]: problem(
        409,
        "case-already-resolved",
        "The case is already resolved",
        "a resolved case accepts no further command",
      ),
    });
    mount();
    await screen.findByText("OPENED");
    await userEvent.type(screen.getByLabelText(/^Resolution code/), "WORK_COMPLETED");
    await userEvent.type(screen.getByLabelText(/^Root cause/), "OPERATOR_UNAVAILABLE");
    await userEvent.click(screen.getByRole("button", { name: "Resolve case" }));
    expect(await screen.findByText(/RESOLVED is terminal and accepts no further command/)).toBeInTheDocument();
    expect(screen.getByText("The case is already resolved")).toBeInTheDocument();
  });

  it("shows the CLOSED card and no command forms once the case is RESOLVED", async () => {
    mockApi({ [`GET /exceptions/${CASE_ID}`]: json(resolvedCase()) });
    mount();
    expect(await screen.findByText(/never reopens it/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Triage case" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Assign case" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolve case" })).not.toBeInTheDocument();
  });

  it("renders the 404 problem when the case does not exist", async () => {
    mockApi({
      [`GET /exceptions/${CASE_ID}`]: problem(404, "case-not-found", "Exception case not found", "exception case not found"),
    });
    mount();
    expect(await screen.findByText("Exception case not found")).toBeInTheDocument();
  });
});
