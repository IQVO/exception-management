---
paths:
  - "web/**"
  - "charts/exception-management/templates/frontend-*.yaml"
---

# Frontend micro-frontend remote (`web/`)

This repo also owns `web/`: `exceptions-mfe`, a Vite + React + TypeScript **Module Federation remote**
consumed by the separate `warehouse-console` shell repo. It is a plain browser client of this service's own
REST API (`apis/openapi.yaml`): nothing in `web/` talks to another bounded context, and nothing in
`internal/` knows `web/` exists.

## Pinned host/remote contract (change both repos together or not at all)

| Fact | Value |
|---|---|
| Federation container | `exceptions_mfe` |
| Exposed module | `./App` (`web/src/App.tsx`, default export, no props) |
| Console route | `/exception-management/*` (the shell provides `BrowserRouter`, tokens, `window.__WAREHOUSE_CONFIG__`) |
| Production base / gateway path | `/mfes/exception-management/` |
| Dev/preview port | **4011** (strictPort; off the fleet's 51xx ladder, which is full) |
| API base | `${apiOrigin}/api/exception-management` (`web/src/config.ts`); dev fallback `http://localhost:8080` |

Routes are relative under a layout route with `path="/"`: `exceptions` (queue), `exceptions/:caseId`
(detail + triage / assign / resolve), `policies`.

- Own `package.json`, build and tests; not part of the Go module, so `make check` never touches it. CI's `web`
  job builds the sibling `warehouse-ui-kit`, then runs `npm run lint` (oxlint), `npx tsc -b`, `npm test`,
  `npm run build`, builds `web/Dockerfile` and smoke-checks nginx.
- `web/vite.config.ts` stays in OBJECT form; `base` comes from `process.argv.includes("build")`.
  `web/vitest.config.ts` is standalone (it does not import or merge the Vite config).
- The Docker build takes the ui-kit as a named build context:
  `docker build --build-context uikit=../../warehouse-ui-kit -t exception-management-frontend:local web`.
- nginx (`web/nginx.conf`): one `Cache-Control` per response, never `expires` plus `add_header Cache-Control`;
  `remoteEntry.js` and `index.html` are `no-cache`.
- Use only `@warehouse/ui-kit` components and tokens. No Dependabot `npm` entry for `/web`: it depends on
  `file:../../warehouse-ui-kit`, which Dependabot cannot fetch (repo_lint R3).
- Tests mock `fetch` (`web/src/test/fetchMock.ts`: an unmocked request throws). An RFC 7807 failure surfaces
  BOTH `title` and `detail` (`web/src/components/ProblemAlert.tsx`).
- No auth anywhere (fleet rule); the remote sends no credentials and no `X-Actor` header today, so REST
  records its writes with actor `system`.
- Chart: `charts/exception-management/templates/frontend-deployment.yaml` and `frontend-service.yaml`
  (component `frontend`, ClusterIP, no Ingress), `frontend.enabled: false` by default in
  `charts/exception-management/values.yaml`; warehouse-infra turns it on. It is served through the Nginx web
  gateway, never through Kong.
