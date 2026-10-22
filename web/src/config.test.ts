import { describe, expect, it } from "vitest";
import { EXCEPTIONS_API_BASE, resolveExceptionsApiBase } from "./config";

describe("resolveExceptionsApiBase", () => {
  it("builds the production API base from the runtime API origin", () => {
    expect(resolveExceptionsApiBase({ apiOrigin: "http://localhost:8000" }, true)).toBe(
      "http://localhost:8000/api/exception-management",
    );
  });

  it("normalizes trailing slashes on the runtime API origin", () => {
    expect(resolveExceptionsApiBase({ apiOrigin: "https://warehouse.example/" }, true)).toBe(
      "https://warehouse.example/api/exception-management",
    );
    expect(resolveExceptionsApiBase({ apiOrigin: "https://warehouse.example///" }, true)).toBe(
      "https://warehouse.example/api/exception-management",
    );
  });

  it("fails loudly when production runtime configuration has no API origin", () => {
    expect(() => resolveExceptionsApiBase({}, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
    expect(() => resolveExceptionsApiBase({ apiOrigin: "" }, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
  });

  it("falls back to the service's own dev port outside production", () => {
    expect(resolveExceptionsApiBase({}, false)).toBe("http://localhost:8080");
  });

  it("still prefers a runtime origin over the dev fallback", () => {
    expect(resolveExceptionsApiBase({ apiOrigin: "http://localhost:8000" }, false)).toBe(
      "http://localhost:8000/api/exception-management",
    );
  });
});

describe("EXCEPTIONS_API_BASE", () => {
  it("loads without /config.json (window.__WAREHOUSE_CONFIG__ absent) in a non-production build", () => {
    expect(window.__WAREHOUSE_CONFIG__).toBeUndefined();
    expect(EXCEPTIONS_API_BASE).toBe("http://localhost:8080");
  });
});
