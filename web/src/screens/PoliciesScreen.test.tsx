import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { PoliciesScreen } from "./PoliciesScreen";
import { json, mockApi, problem } from "../test/fetchMock";
import { manualPolicy, policy } from "../test/fixtures";

function mount() {
  return render(
    <MemoryRouter initialEntries={["/policies"]}>
      <PoliciesScreen />
    </MemoryRouter>,
  );
}

describe("PoliciesScreen", () => {
  it("lists the current policy version of every declared source type", async () => {
    mockApi({ "GET /exception-policies": json([policy(), manualPolicy()]) });
    mount();
    expect(await screen.findByText(/TaskCPTMissed/)).toBeInTheDocument();
    expect(screen.getByText(/DiscrepancyDetected/)).toBeInTheDocument();
    expect(screen.getAllByText("cps-front").length).toBeGreaterThan(0);
    expect(screen.getByText("cps-inventory")).toBeInTheDocument();
    expect(screen.getByText("120")).toBeInTheDocument(); // slaMinutes of the manual policy
  });

  it("spells the auto-resolve rule and shows no-reading for its absence", async () => {
    mockApi({ "GET /exception-policies": json([policy(), manualPolicy()]) });
    mount();
    expect(await screen.findByText(/OrderRepromised within 60 min/)).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument(); // manualPolicy has no rule
  });

  it("notes when no policy is declared at all", async () => {
    mockApi({ "GET /exception-policies": json([]) });
    mount();
    expect(await screen.findByText(/No policies declared/)).toBeInTheDocument();
  });

  it("renders the problem when the policies read fails", async () => {
    mockApi({ "GET /exception-policies": problem(500, "internal-error", "Internal server error", "an unexpected error occurred") });
    mount();
    expect(await screen.findByText("Internal server error")).toBeInTheDocument();
  });
});
