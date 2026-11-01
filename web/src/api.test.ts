import { describe, expect, it } from "vitest";
import { ApiError, assignException, getException, listExceptionPolicies, listExceptions, newIdempotencyKey, resolveException, toApiError, triageException } from "./api";
import { json, mockApi, problem } from "./test/fetchMock";
import { CASE_ID, openCase, policy } from "./test/fixtures";

describe("newIdempotencyKey", () => {
  it("mints a distinct RFC 4122 v4 UUID per call (one per submission)", () => {
    const keys = new Set(Array.from({ length: 32 }, newIdempotencyKey));
    expect(keys.size).toBe(32);
    for (const key of keys) expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });
});

describe("listExceptions", () => {
  it("sends every filter as a query parameter and omits the empty ones", async () => {
    const api = mockApi({ "GET /exceptions": json({ items: [openCase()] }) });
    await listExceptions({ status: "OPEN", queue: "cps-front", site: "site-1", sourceType: undefined });
    const url = new URL(api.calls[0].url);
    expect(url.searchParams.get("status")).toBe("OPEN");
    expect(url.searchParams.get("queue")).toBe("cps-front");
    expect(url.searchParams.get("site")).toBe("site-1");
    expect(url.searchParams.has("sourceType")).toBe(false);
    expect(url.searchParams.get("limit")).toBe("500");
    expect(url.searchParams.has("cursor")).toBe(false);
  });

  it("follows nextCursor until it is absent, repeating the filters on every page", async () => {
    const pages: Record<string, ReturnType<typeof json>> = {
      "": json({ items: [{ ...openCase(), caseId: "case-1" }], nextCursor: "c1" }),
      c1: json({ items: [{ ...openCase(), caseId: "case-2" }], nextCursor: "c2" }),
      c2: json({ items: [{ ...openCase(), caseId: "case-3" }] }),
    };
    const api = mockApi({ "GET /exceptions": (call) => pages[call.query.get("cursor") ?? ""] });
    const cases = await listExceptions({ status: "OPEN", queue: "cps-front" });
    expect(cases.map((c) => c.caseId)).toEqual(["case-1", "case-2", "case-3"]);
    expect(api.calls.map((c) => c.query.get("cursor"))).toEqual([null, "c1", "c2"]);
    for (const call of api.calls) {
      expect(call.query.get("status")).toBe("OPEN");
      expect(call.query.get("queue")).toBe("cps-front");
    }
  });

  it("is a header-less GET (a simple CORS request)", async () => {
    const api = mockApi({ "GET /exceptions": json({ items: [] }) });
    await listExceptions();
    expect(api.calls[0].method).toBe("GET");
    expect(Object.keys(api.calls[0].headers)).toEqual([]);
  });

  it("normalizes a null body to an empty list (the API never 404s)", async () => {
    mockApi({ "GET /exceptions": json(null) });
    await expect(listExceptions()).resolves.toEqual([]);
    mockApi({ "GET /exceptions": json({ items: [] }) });
    await expect(listExceptions()).resolves.toEqual([]);
  });
});

describe("getException", () => {
  it("URL-encodes the case id path segment", async () => {
    const api = mockApi({ [`GET /exceptions/${CASE_ID}`]: json(openCase()) });
    await getException(CASE_ID);
    expect(api.calls[0].path).toBe(`/exceptions/${CASE_ID}`);
  });

  it("surfaces a 404 as an ApiError carrying the problem slug", async () => {
    mockApi({
      [`GET /exceptions/${CASE_ID}`]: problem(404, "case-not-found", "Exception case not found", "exception case not found"),
    });
    const err = await getException(CASE_ID).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(404);
    expect(apiErr.slug).toBe("case-not-found");
    expect(apiErr.title).toBe("Exception case not found");
    expect(apiErr.detail).toBe("exception case not found");
  });
});

describe("triage/assign/resolve submissions", () => {
  it("triage POSTs the severity and queue with a fresh Idempotency-Key", async () => {
    const api = mockApi({ [`POST /exceptions/${CASE_ID}/triage-decisions`]: json(openCase()) });
    await triageException(CASE_ID, { severity: "CRITICAL", queue: "cps-inbound" });
    const call = api.to(`POST /exceptions/${CASE_ID}/triage-decisions`)[0];
    expect(call.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(call.body).toEqual({ severity: "CRITICAL", queue: "cps-inbound" });
  });

  it("assign POSTs the operator id with a fresh Idempotency-Key", async () => {
    const api = mockApi({ [`POST /exceptions/${CASE_ID}/assignments`]: json(openCase()) });
    await assignException(CASE_ID, { assignee: "op-marc" });
    const call = api.to(`POST /exceptions/${CASE_ID}/assignments`)[0];
    expect(call.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(call.body).toEqual({ assignee: "op-marc" });
  });

  it("resolve POSTs codes and omits an empty note", async () => {
    const api = mockApi({ [`POST /exceptions/${CASE_ID}/resolutions`]: json(openCase()) });
    await resolveException(CASE_ID, { resolutionCode: "WORK_COMPLETED", rootCause: "OPERATOR_UNAVAILABLE" });
    const call = api.to(`POST /exceptions/${CASE_ID}/resolutions`)[0];
    expect(call.headers["Idempotency-Key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(call.body).toEqual({ resolutionCode: "WORK_COMPLETED", rootCause: "OPERATOR_UNAVAILABLE" });
  });

  it("resolve sends the note only when there is one", async () => {
    const api = mockApi({ [`POST /exceptions/${CASE_ID}/resolutions`]: json(openCase()) });
    await resolveException(CASE_ID, { resolutionCode: "WORK_COMPLETED", rootCause: "OPERATOR_UNAVAILABLE", note: "ok" });
    expect(api.to(`POST /exceptions/${CASE_ID}/resolutions`)[0].body).toEqual({
      resolutionCode: "WORK_COMPLETED",
      rootCause: "OPERATOR_UNAVAILABLE",
      note: "ok",
    });
  });

  it("mints a DIFFERENT key per submission (per logical command)", async () => {
    const api = mockApi({ [`POST /exceptions/${CASE_ID}/triage-decisions`]: json(openCase()) });
    await triageException(CASE_ID, { severity: "LOW", queue: "q" });
    await triageException(CASE_ID, { severity: "HIGH", queue: "q" });
    const keys = api.to(`POST /exceptions/${CASE_ID}/triage-decisions`).map((c) => c.headers["Idempotency-Key"]);
    expect(keys[0]).not.toBe(keys[1]);
  });

  it("keeps the problem+json body of a 409 (slug + detail)", async () => {
    mockApi({
      [`POST /exceptions/${CASE_ID}/resolutions`]: problem(
        409,
        "case-already-resolved",
        "The case is already resolved",
        "a resolved case accepts no further command",
      ),
    });
    const err = await resolveException(CASE_ID, { resolutionCode: "X", rootCause: "Y" }).catch((e: unknown) => e);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(409);
    expect(apiErr.slug).toBe("case-already-resolved");
  });

  it("falls back to the status line when the error body is not JSON", async () => {
    mockApi({
      [`POST /exceptions/${CASE_ID}/assignments`]: new Response("gateway exploded", { status: 502, statusText: "Bad Gateway" }),
    });
    const err = await assignException(CASE_ID, { assignee: "op" }).catch((e: unknown) => e);
    const apiErr = err as ApiError;
    expect(apiErr.status).toBe(502);
    expect(apiErr.title).toBe("502 Bad Gateway");
  });

  it("wraps a network failure as a status-0 ApiError", async () => {
    mockApi({});
    const err = await getException(CASE_ID).catch((e: unknown) => e);
    expect(toApiError(err)).toBeInstanceOf(ApiError);
    expect((toApiError(err) as ApiError).status).toBe(0);
  });
});

describe("listExceptionPolicies", () => {
  it("returns the declared policies and normalizes null to []", async () => {
    mockApi({ "GET /exception-policies": json({ items: [policy()] }) });
    await expect(listExceptionPolicies()).resolves.toHaveLength(1);
    mockApi({ "GET /exception-policies": json(null) });
    await expect(listExceptionPolicies()).resolves.toEqual([]);
  });

  it("follows nextCursor until it is absent", async () => {
    const pages: Record<string, ReturnType<typeof json>> = {
      "": json({ items: [{ ...policy(), sourceType: "a" }], nextCursor: "p1" }),
      p1: json({ items: [{ ...policy(), sourceType: "b" }] }),
    };
    const api = mockApi({ "GET /exception-policies": (call) => pages[call.query.get("cursor") ?? ""] });
    const policies = await listExceptionPolicies();
    expect(policies.map((p) => p.sourceType)).toEqual(["a", "b"]);
    expect(api.calls.map((c) => c.query.get("cursor"))).toEqual([null, "p1"]);
  });
});
