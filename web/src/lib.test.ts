import { describe, expect, it } from "vitest";
import { codeProblem, noteProblem, optionalShortIdProblem, optionalTextProblem, shortIdProblem, utcLabel } from "./lib";

describe("utcLabel", () => {
  it("formats an RFC 3339 instant as a UTC calendar minute", () => {
    expect(utcLabel("2026-10-21T09:05:07Z")).toBe("2026-10-21 09:05Z");
  });
  it("returns the raw text when unparseable and the placeholder when absent", () => {
    expect(utcLabel("not-a-date")).toBe("not-a-date");
    expect(utcLabel(undefined)).toBe("—");
  });
});

describe("shortIdProblem (queue / assignee)", () => {
  it("requires a non-blank value of at most 64 characters", () => {
    expect(shortIdProblem("queue", "")).toContain("required");
    expect(shortIdProblem("queue", "   ")).toContain("required");
    expect(shortIdProblem("assignee", "x".repeat(65))).toContain("at most 64");
    expect(shortIdProblem("queue", "cps-front")).toBeNull();
    expect(shortIdProblem("assignee", "op-marc")).toBeNull();
  });
  it("lets an empty optional filter through (means 'no filter')", () => {
    expect(optionalShortIdProblem("queue", "")).toBeNull();
    expect(optionalShortIdProblem("queue", " ")).not.toBeNull();
  });
});

describe("codeProblem (resolution code / root cause)", () => {
  it("requires an uppercase code of letters, digits and underscores", () => {
    expect(codeProblem("resolution code", "")).toContain("required");
    expect(codeProblem("root cause", "work completed")).not.toBeNull();
    expect(codeProblem("root cause", "lower_case")).not.toBeNull();
    expect(codeProblem("root cause", "x".repeat(65))).toContain("at most 64");
    expect(codeProblem("resolution code", "WORK_COMPLETED")).toBeNull();
    expect(codeProblem("root cause", "OPERATOR_UNAVAILABLE_2")).toBeNull();
  });
});

describe("noteProblem", () => {
  it("allows anything up to 2000 characters", () => {
    expect(noteProblem("")).toBeNull();
    expect(noteProblem("x".repeat(2000))).toBeNull();
    expect(noteProblem("x".repeat(2001))).toContain("2000");
  });
});

describe("optionalTextProblem (site / source type filters)", () => {
  it("lets empty and reasonable values through, rejects junk", () => {
    expect(optionalTextProblem("site", "")).toBeNull();
    expect(optionalTextProblem("site", "site-1")).toBeNull();
    expect(optionalTextProblem("source type", "  ")).not.toBeNull();
    expect(optionalTextProblem("source type", "x".repeat(201))).toContain("200");
  });
});
