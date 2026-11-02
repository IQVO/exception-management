import { EXCEPTIONS_API_BASE } from "./config";
import type {
  AssignBody,
  ExceptionCase,
  ExceptionPolicy,
  ListExceptionsQuery,
  Page,
  ResolveBody,
  TriageBody,
} from "./types";

/** RFC 7807 problem+json body every error response from exception-management
 *  returns (`type` = https://errors.exception-management.warehouse-systems.dev/<slug>). */
export interface ProblemDetails {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  instance?: string;
}

/**
 * Every failed call -- an HTTP error with a problem+json body, an HTTP error
 * without one, or a network failure -- surfaces as one ApiError, so a screen
 * has a single shape to render: `title` (the problem's category) and `detail`
 * (what exactly was wrong), plus `slug` to branch on a specific problem
 * (e.g. `case-already-resolved`, `concurrent-modification`).
 */
export class ApiError extends Error {
  readonly status: number;
  readonly problem: ProblemDetails | null;
  readonly title: string;
  readonly detail: string;
  /** Last path segment of problem.type (e.g. "case-already-resolved"); "" when unknown. */
  readonly slug: string;

  constructor(status: number, problem: ProblemDetails | null, fallbackTitle: string) {
    const title = problem?.title || fallbackTitle;
    const detail = problem?.detail ?? "";
    super(detail || title);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
    this.title = title;
    this.detail = detail;
    this.slug = problem?.type ? (problem.type.split("/").pop() ?? "") : "";
  }
}

/** Normalizes anything thrown by a request into an ApiError. */
export function toApiError(err: unknown): ApiError {
  if (err instanceof ApiError) return err;
  const message = err instanceof Error ? err.message : String(err);
  return new ApiError(0, { detail: message }, "Network error");
}

/**
 * A fresh RFC 4122 v4 UUID for an Idempotency-Key header: minted once per
 * POST (one per triage/assign/resolve submission; a new user action mints a
 * new key, and the server replays a repeated one with
 * `Idempotency-Replayed: true` instead of running the command twice).
 * `crypto.randomUUID` only exists in secure contexts, and the console can be
 * opened over plain http on a non-localhost host, so fall back to
 * getRandomValues.
 */
export function newIdempotencyKey(): string {
  if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

type Query = Record<string, string | undefined>;

function withQuery(path: string, query?: Query): string {
  if (!query) return path;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") params.set(key, value);
  }
  const qs = params.toString();
  return qs ? `${path}?${qs}` : path;
}

/**
 * Every POST (triage, assign and resolve, all of which change state and
 * REQUIRE the key in every deployed mode) carries its own `Idempotency-Key`,
 * a UUID minted for that submission. GETs send no custom header, so they stay
 * simple CORS requests.
 */
async function request<T>(method: "GET" | "POST", path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (method === "POST") headers["Idempotency-Key"] = newIdempotencyKey();
  let res: Response;
  try {
    res = await fetch(`${EXCEPTIONS_API_BASE}${path}`, {
      method,
      ...(Object.keys(headers).length > 0 ? { headers } : {}),
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
  } catch (err) {
    throw toApiError(err);
  }
  if (!res.ok) {
    let problem: ProblemDetails | null = null;
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      // non-JSON error body -- fall through with problem = null
    }
    throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`.trim());
  }
  return (await res.json()) as T;
}

const seg = encodeURIComponent;

/** The page size every list call asks for (the API maximum). */
const PAGE_LIMIT = "500";

/**
 * Reads a whole collection: GETs `path` with the filters repeated on every
 * page request, takes the rows from `items` and follows `nextCursor` until it
 * is absent, so a collection that outgrows one page is never truncated.
 */
async function listAll<T>(path: string, filters: Query): Promise<T[]> {
  const rows: T[] = [];
  let cursor: string | undefined;
  do {
    const page = await request<Page<T> | null>("GET", withQuery(path, { ...filters, limit: PAGE_LIMIT, cursor }));
    rows.push(...(page?.items ?? []));
    cursor = page?.nextCursor || undefined;
  } while (cursor);
  return rows;
}

/** GET /exceptions?status=&queue=&site=&sourceType= -- newest first, every
 *  page followed; an empty array (never a 404) when nothing matches. */
export function listExceptions(q: ListExceptionsQuery = {}): Promise<ExceptionCase[]> {
  return listAll<ExceptionCase>("/exceptions", {
    status: q.status,
    queue: q.queue,
    site: q.site,
    sourceType: q.sourceType,
  });
}

/** GET /exceptions/{caseId} (404 case-not-found). */
export function getException(caseId: string): Promise<ExceptionCase> {
  return request<ExceptionCase>("GET", `/exceptions/${seg(caseId)}`);
}

/**
 * POST /exceptions/{caseId}/triage-decisions: set or change severity and
 * queue, moving an OPEN case to TRIAGED. 409 case-already-resolved once
 * terminal; 400 invalid-severity / invalid-queue.
 */
export function triageException(caseId: string, body: TriageBody): Promise<ExceptionCase> {
  return request<ExceptionCase>("POST", `/exceptions/${seg(caseId)}/triage-decisions`, body);
}

/**
 * POST /exceptions/{caseId}/assignments: give the case an owner and move it
 * to ASSIGNED; reassigning is allowed while not resolved. 400 invalid-assignee.
 */
export function assignException(caseId: string, body: AssignBody): Promise<ExceptionCase> {
  return request<ExceptionCase>("POST", `/exceptions/${seg(caseId)}/assignments`, body);
}

/**
 * POST /exceptions/{caseId}/resolutions: record resolution code + root cause
 * and move the case to RESOLVED, terminal. 409 case-already-resolved on any
 * later command; 400 invalid-resolution.
 */
export function resolveException(caseId: string, body: ResolveBody): Promise<ExceptionCase> {
  return request<ExceptionCase>("POST", `/exceptions/${seg(caseId)}/resolutions`, body);
}

/** GET /exception-policies -- the current version of every declared source
 *  type, ordered by source type, every page followed; an empty array when
 *  none are declared. */
export function listExceptionPolicies(): Promise<ExceptionPolicy[]> {
  return listAll<ExceptionPolicy>("/exception-policies", {});
}
