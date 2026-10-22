import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import App from "./App";
import { json, mockApi } from "./test/fetchMock";
import { CASE_ID, openCase, policy, resolvedCase } from "./test/fixtures";

function mount(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
}

/** Mounted the way the console mounts the remote: inside the host's own
 *  splat route, where relative links resolve differently than at the root. */
function mountUnderHost(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/exception-management/*" element={<App />} />
      </Routes>
    </MemoryRouter>,
  );
}

function routes() {
  return mockApi({
    "GET /exceptions": json([openCase()]),
    [`GET /exceptions/${CASE_ID}`]: json(openCase()),
    "GET /exception-policies": json([policy()]),
  });
}

function subNavHrefs(): string[] {
  const nav = screen.getByRole("navigation", { name: "Exception management sections" });
  return within(nav)
    .getAllByRole("link")
    .map((a) => a.getAttribute("href") ?? "");
}

describe("App (the exposed ./App)", () => {
  it("redirects the index route to the problem-solve queue and shows the sub-nav", async () => {
    routes();
    mount("/");
    expect(await screen.findByRole("heading", { name: "Problem-solve queue" })).toBeInTheDocument();
    const nav = screen.getByRole("navigation", { name: "Exception management sections" });
    for (const label of ["Problem-solve queue", "Policies"]) expect(nav).toHaveTextContent(label);
  });

  it("routes relatively at the root: every section and detail route renders", async () => {
    routes();
    const cases: [string, string][] = [
      ["/exceptions", "Problem-solve queue"],
      [`/exceptions/${CASE_ID}`, CASE_ID],
      ["/policies", "Exception policies"],
    ];
    for (const [path, heading] of cases) {
      const { unmount } = mount(path);
      expect(await screen.findByRole("heading", { name: heading })).toBeInTheDocument();
      unmount();
    }
  });

  it("degrades without crashing when the API is unreachable", async () => {
    mockApi({});
    mount("/policies");
    expect(await screen.findByRole("heading", { name: "Exception policies" })).toBeInTheDocument();
    expect(await screen.findByRole("alert")).toBeInTheDocument();
  });

  it("keeps sub-nav links under the host's mount point from any section", async () => {
    routes();
    mountUnderHost("/exception-management/policies");
    await screen.findByRole("heading", { name: "Exception policies" });
    expect(subNavHrefs()).toEqual([
      "/exception-management/exceptions",
      "/exception-management/policies",
    ]);
    await userEvent.click(screen.getByRole("link", { name: "Problem-solve queue" }));
    expect(await screen.findByRole("heading", { name: "Problem-solve queue" })).toBeInTheDocument();
    expect(subNavHrefs()[0]).toBe("/exception-management/exceptions");
  });

  it("only ever calls its own API (no sibling base)", async () => {
    const api = routes();
    mount("/exceptions");
    await screen.findByText(CASE_ID);
    for (const c of api.calls) expect(c.url.startsWith("http://localhost:8080/")).toBe(true);
  });

  it("renders a resolved case's terminal view without any command form", async () => {
    mockApi({
      "GET /exceptions": json([]),
      [`GET /exceptions/${CASE_ID}`]: json(resolvedCase()),
    });
    mount(`/exceptions/${CASE_ID}`);
    expect(await screen.findByRole("heading", { name: CASE_ID })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Triage case" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Assign case" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolve case" })).not.toBeInTheDocument();
    expect(screen.getByText("WORK_COMPLETED")).toBeInTheDocument();
    expect(screen.getByText("OPERATOR_UNAVAILABLE")).toBeInTheDocument();
  });
});
