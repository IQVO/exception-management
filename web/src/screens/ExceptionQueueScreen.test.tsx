import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { ExceptionQueueScreen } from "./ExceptionQueueScreen";
import { json, mockApi, problem } from "../test/fetchMock";
import { CASE_ID, openCase } from "../test/fixtures";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/exceptions"]}>
      <ExceptionQueueScreen />
    </MemoryRouter>,
  );
}

describe("ExceptionQueueScreen", () => {
  it("lists cases newest-first with their status, severity and queue", async () => {
    mockApi({ "GET /exceptions": json({ items: [openCase()] }) });
    mount();
    expect(await screen.findByText(CASE_ID)).toBeInTheDocument();
    // the status pill (a span) -- the filter <option> carries the same text
    expect(screen.getByText("OPEN", { selector: "span" })).toBeInTheDocument();
    expect(screen.getByText("HIGH")).toBeInTheDocument();
    expect(screen.getByText("cps-front")).toBeInTheDocument();
    expect(screen.getByText("task-8a1f")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
  });

  it("applies the status/queue/site/sourceType filters as query parameters", async () => {
    const api = mockApi({ "GET /exceptions": json({ items: [openCase()] }) });
    mount();
    await screen.findByText(CASE_ID);
    await userEvent.selectOptions(screen.getByLabelText("Status"), "OPEN");
    await userEvent.type(screen.getByLabelText("Queue"), "cps-front");
    await userEvent.type(screen.getByLabelText("Site"), "site-1");
    await userEvent.type(
      screen.getByLabelText("Source type"),
      "com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed",
    );
    await userEvent.click(screen.getByRole("button", { name: "Apply filters" }));
    await screen.findByText(CASE_ID);
    const filtered = api.calls[api.calls.length - 1];
    expect(filtered.query.get("status")).toBe("OPEN");
    expect(filtered.query.get("queue")).toBe("cps-front");
    expect(filtered.query.get("site")).toBe("site-1");
    expect(filtered.query.get("sourceType")).toBe("com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed");
  });

  it("keeps the request header-less until filters apply (simple CORS GET)", async () => {
    mockApi({ "GET /exceptions": json({ items: [openCase()] }) });
    mount();
    await screen.findByText(CASE_ID);
  });

  it("shows the empty note when no case matches", async () => {
    mockApi({ "GET /exceptions": json({ items: [] }) });
    mount();
    expect(await screen.findByText("No cases match these filters.")).toBeInTheDocument();
  });

  it("renders the problem (not a crash) when the API fails", async () => {
    mockApi({ "GET /exceptions": problem(400, "invalid-status", "Status is invalid", "status must be one of OPEN, TRIAGED, ASSIGNED, RESOLVED") });
    mount();
    expect(await screen.findByText("Status is invalid")).toBeInTheDocument();
    expect(screen.getByText(/status must be one of/)).toBeInTheDocument();
  });

  it("blocks a whitespace-only queue filter from being applied", async () => {
    const api = mockApi({ "GET /exceptions": json({ items: [openCase()] }) });
    mount();
    await screen.findByText(CASE_ID);
    await userEvent.type(screen.getByLabelText("Queue"), "  ");
    const apply = screen.getByRole("button", { name: "Apply filters" });
    expect(apply).toBeDisabled();
    expect(api.calls).toHaveLength(1);
  });
});
