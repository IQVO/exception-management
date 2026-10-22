import { NO_VALUE } from "@warehouse/ui-kit";

/** "2026-10-17 09:30Z" for an RFC 3339 instant; the raw text when unparseable. */
export function utcLabel(iso: string | undefined): string {
  if (!iso) return NO_VALUE;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return iso;
  const d = new Date(t).toISOString();
  return `${d.slice(0, 10)} ${d.slice(11, 16)}Z`;
}

/** A problem-solve queue or an operator id: 1..64 characters and not blank
 *  (invalid-queue / invalid-assignee otherwise). */
export function shortIdProblem(what: "queue" | "assignee", value: string): string | null {
  const s = value.trim();
  if (s.length === 0) return `A ${what} is required.`;
  if (s.length > 64) return `A ${what} is at most 64 characters.`;
  return null;
}

/** The optional filter form of shortIdProblem: empty means "no filter". */
export function optionalShortIdProblem(what: "queue" | "assignee", value: string): string | null {
  return value.length === 0 ? null : shortIdProblem(what, value);
}

/** A resolution code or root cause: uppercase letters, digits and
 *  underscores, 1..64 characters (invalid-resolution otherwise). */
export function codeProblem(what: "resolution code" | "root cause", value: string): string | null {
  const s = value.trim();
  if (s.length === 0) return `A ${what} is required.`;
  if (s.length > 64) return `A ${what} is at most 64 characters.`;
  if (!/^[A-Z0-9_]+$/.test(s)) return `A ${what} is uppercase letters, digits and underscores only.`;
  return null;
}

/** The optional resolve note: at most 2000 characters. */
export function noteProblem(value: string): string | null {
  if (value.length <= 2000) return null;
  return "A note is at most 2000 characters.";
}

/** The optional free-text filters (site, source type): at most 200
 *  characters, no whitespace padding applied by the caller. */
export function optionalTextProblem(what: "site" | "source type", value: string): string | null {
  if (value.length === 0) return null;
  if (value.trim().length === 0) return `A ${what} cannot be only whitespace.`;
  if (value.length > 200) return `A ${what} is at most 200 characters.`;
  return null;
}
