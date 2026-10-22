import { Navigate, NavLink, Outlet, Route, Routes } from "react-router-dom";
import { CaseDetailRoute } from "./screens/CaseDetailScreen";
import { ExceptionQueueScreen } from "./screens/ExceptionQueueScreen";
import { PoliciesScreen } from "./screens/PoliciesScreen";

const SUB_NAV = [
  { to: "exceptions", label: "Problem-solve queue", end: false },
  { to: "policies", label: "Policies", end: false },
];

const linkStyle = ({ isActive }: { isActive: boolean }) => ({
  display: "inline-flex",
  padding: "6px 12px",
  borderRadius: "var(--wh-radius-pill)",
  fontSize: "var(--wh-font-size-sm)",
  fontWeight: isActive ? 600 : 500,
  color: isActive ? "var(--wh-color-text)" : "var(--wh-color-text-muted)",
  background: isActive ? "var(--wh-color-accent-muted)" : "transparent",
  textDecoration: "none",
});

/** The sub-nav, rendered by a layout route with the path "/".
 *
 *  The console mounts this component inside its own `<Route
 *  path="/exception-management/*">`. A relative `<NavLink>` rendered straight
 *  in that splat route resolves against the FULL current URL (react-router 7
 *  uses the last path-contributing match's pathname, splat included), so from
 *  /exception-management/policies the link `exceptions` would point at
 *  /exception-management/policies/exceptions. A pathless layout route does
 *  not help (react-router drops pathless routes from relative resolution); a
 *  layout route WITH path "/" does: its own match is the mount point, under
 *  any prefix and standalone at `/`. (Same fix as every remote in the fleet;
 *  App.test.tsx mounts under the host's splat route to prove it.) */
function ExceptionsLayout() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <nav aria-label="Exception management sections" style={{ display: "flex", gap: "var(--wh-space-2)" }}>
        {SUB_NAV.map((item) => (
          <NavLink key={item.to} to={item.to} end={item.end} style={linkStyle}>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <Outlet />
    </div>
  );
}

/** Exposed as exceptions_mfe/App via Module Federation. Takes NO props: the
 *  console mounts it under `/exception-management/*` and provides the
 *  BrowserRouter, the design tokens and `window.__WAREHOUSE_CONFIG__`. Routes
 *  here are RELATIVE, so the component works identically under that prefix
 *  (in the shell) or at / (standalone dev, see main.tsx).
 *
 *   - (index): redirects to the problem-solve queue.
 *   - exceptions: the open-exceptions queue with status/queue/site/sourceType
 *     filters (GET /exceptions).
 *   - exceptions/:caseId: one case with its frozen source payload, its audit
 *     trail and, while it is not resolved, the triage/assign/resolve forms
 *     (each POST carries a fresh Idempotency-Key).
 *   - policies: the handling-policy table (GET /exception-policies).
 *
 *  There is deliberately no case-creation screen: every case opens from a
 *  consumed fact (ADR 0001), and there is no POST /exceptions to call. */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<ExceptionsLayout />}>
        <Route index element={<Navigate to="exceptions" replace />} />
        <Route path="exceptions" element={<ExceptionQueueScreen />} />
        <Route path="exceptions/:caseId" element={<CaseDetailRoute />} />
        <Route path="policies" element={<PoliciesScreen />} />
      </Route>
    </Routes>
  );
}
