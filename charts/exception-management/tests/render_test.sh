#!/usr/bin/env bash
# Chart render assertions: the chart deploys the REAL runtime of this context
# (real Postgres and real Kafka only, no in-memory mode), so the mandatory
# configuration has no silent default, no *_MODE variable is ever rendered,
# and the env the binary reads is the env the chart renders.
# Run: bash charts/exception-management/tests/render_test.sh   (needs helm)
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_dir="$(cd "$chart_dir/../.." && pwd)"
release=exception-management
# Dummy values, no password: the chart refuses to render without a database
# source and without Kafka brokers.
base=(--set database.url=postgres://u@example.invalid:5432/db --set kafka.brokers=kafka.example.invalid:9092)
# Every optional value set (ct lint's values file), so every env var the
# binary can read is rendered.
everything=(-f "$chart_dir/ci/all-components-values.yaml")

fail() { echo "FAIL: $*" >&2; exit 1; }

render() { helm template "$release" "$chart_dir" "$@"; }
# render_only <template> <helm args...>
render_only() { local t="$1"; shift; render "$@" --show-only "templates/$t"; }

configmap="$(render_only configmap.yaml "${base[@]}")"
deployment="$(render_only deployment.yaml "${base[@]}")"
configmap_name="$(awk '/^  name:/ {print $2; exit}' <<<"$configmap")"
[[ -n "$configmap_name" ]] || fail "could not read the ConfigMap name"

# --- the ConfigMap is mounted (gap fix D14) ---------------------------------
grep -A3 'envFrom:' <<<"$deployment" | grep -q 'configMapRef:' \
  || fail "Deployment does not mount the ConfigMap via envFrom"
grep -A3 'envFrom:' <<<"$deployment" | grep -q "name: $configmap_name" \
  || fail "Deployment envFrom does not reference ConfigMap $configmap_name"

# --- mandatory configuration is rendered from values -------------------------
env_value() { # env_value <rendered> <NAME>
  grep -A1 "name: $2$" <<<"$1" | grep -m1 'value:' | sed -E 's/.*value: "?//; s/"$//'
}
[[ "$(env_value "$deployment" KAFKA_BROKERS)" == "kafka.example.invalid:9092" ]] || fail "KAFKA_BROKERS is not rendered from kafka.brokers"
[[ "$(env_value "$deployment" FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP)" == "exception-management-fulfillment" ]] || fail "FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP is not rendered from config.fulfillmentExceptionsConsumerGroup"
[[ "$(env_value "$deployment" WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP)" == "exception-management-work-planning" ]] || fail "WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP is not rendered from config.workPlanningExceptionsConsumerGroup"
[[ "$(env_value "$deployment" NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP)" == "exception-management-network-fulfillment" ]] || fail "NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP is not rendered from config.networkFulfillmentExceptionsConsumerGroup"
[[ "$(env_value "$deployment" ORDER_CORRELATION_CONSUMER_GROUP)" == "exception-management-order-correlation" ]] || fail "ORDER_CORRELATION_CONSUMER_GROUP is not rendered from config.orderCorrelationConsumerGroup"
[[ "$(env_value "$deployment" INVENTORY_EXCEPTIONS_CONSUMER_GROUP)" == "exception-management-inventory" ]] || fail "INVENTORY_EXCEPTIONS_CONSUMER_GROUP is not rendered from config.inventoryExceptionsConsumerGroup"
[[ "$(env_value "$deployment" RETURNS_EXCEPTIONS_CONSUMER_GROUP)" == "exception-management-returns" ]] || fail "RETURNS_EXCEPTIONS_CONSUMER_GROUP is not rendered from config.returnsExceptionsConsumerGroup"
grep -A4 'name: DATABASE_URL$' <<<"$deployment" | grep -q 'secretKeyRef:' || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
grep -A6 'name: MIGRATIONS_DATABASE_URL$' <<<"$deployment" | grep -q 'optional: true' || fail "MIGRATIONS_DATABASE_URL must be an optional secretKeyRef"
for var in KAFKA_BROKERS FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP \
  NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP ORDER_CORRELATION_CONSUMER_GROUP \
  INVENTORY_EXCEPTIONS_CONSUMER_GROUP RETURNS_EXCEPTIONS_CONSUMER_GROUP; do
  grep -q "^  $var: " <<<"$configmap" || fail "the ConfigMap does not carry $var"
done
if grep -qE '^  (MIGRATIONS_)?DATABASE_URL:' <<<"$configmap"; then fail "a DSN must never land in the ConfigMap"; fi

# The consumer group ids come from values: overriding them changes the env.
renamed="$(render_only deployment.yaml "${base[@]}" \
  --set config.fulfillmentExceptionsConsumerGroup=g-ff \
  --set config.workPlanningExceptionsConsumerGroup=g-wp \
  --set config.networkFulfillmentExceptionsConsumerGroup=g-nf \
  --set config.orderCorrelationConsumerGroup=g-oc \
  --set config.inventoryExceptionsConsumerGroup=g-inv \
  --set config.returnsExceptionsConsumerGroup=g-ret)"
[[ "$(env_value "$renamed" FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP)" == "g-ff" ]] || fail "config.fulfillmentExceptionsConsumerGroup does not reach FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP"
[[ "$(env_value "$renamed" WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP)" == "g-wp" ]] || fail "config.workPlanningExceptionsConsumerGroup does not reach WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP"
[[ "$(env_value "$renamed" NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP)" == "g-nf" ]] || fail "config.networkFulfillmentExceptionsConsumerGroup does not reach NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP"
[[ "$(env_value "$renamed" ORDER_CORRELATION_CONSUMER_GROUP)" == "g-oc" ]] || fail "config.orderCorrelationConsumerGroup does not reach ORDER_CORRELATION_CONSUMER_GROUP"
[[ "$(env_value "$renamed" INVENTORY_EXCEPTIONS_CONSUMER_GROUP)" == "g-inv" ]] || fail "config.inventoryExceptionsConsumerGroup does not reach INVENTORY_EXCEPTIONS_CONSUMER_GROUP"
[[ "$(env_value "$renamed" RETURNS_EXCEPTIONS_CONSUMER_GROUP)" == "g-ret" ]] || fail "config.returnsExceptionsConsumerGroup does not reach RETURNS_EXCEPTIONS_CONSUMER_GROUP"

# --- no in-memory / permissive mode, ever ------------------------------------
no_mode() { # no_mode <label> <rendered>
  if grep -qE '(name: |^  )[A-Z_]+_MODE:?$|^  [A-Z_]+_MODE: ' <<<"$2"; then fail "$1 renders a *_MODE variable; this service has no modes"; fi
  if grep -qiE 'value: "?permissive' <<<"$2"; then fail "$1 renders permissive, which no binary of this service accepts"; fi
}
no_mode "ConfigMap" "$configmap"
no_mode "Deployment" "$deployment"
all_docs="$(render "${base[@]}" "${everything[@]}")"
no_mode "the full render" "$all_docs"
if grep -q 'DATABASE_URL: ""' <<<"$all_docs"; then fail "an empty DATABASE_URL must never be rendered"; fi

# --- probes: /healthz liveness (and startup), /readyz readiness --------------
probe_block() { # probe_block <probeName> <rendered deployment>
  awk -v p="$1:" '$1 == p {f=1; next} f && /^          [a-zA-Z]/ {exit} f {print}' <<<"$2"
}
grep -q 'path: /healthz' <<<"$(probe_block livenessProbe "$deployment")" || fail "livenessProbe must hit /healthz"
grep -q 'path: /healthz' <<<"$(probe_block startupProbe "$deployment")" || fail "startupProbe must hit /healthz"
readiness="$(probe_block readinessProbe "$deployment")"
grep -q 'path: /readyz' <<<"$readiness" || fail "readinessProbe must hit /readyz"
# /readyz checks Postgres AND that a Kafka broker answers (reachability only,
# cached): give it a sane delay, period, timeout and failure budget so a
# broker blip does not flap.
grep -q 'initialDelaySeconds: [1-9]' <<<"$readiness" || fail "readinessProbe needs an initialDelaySeconds"
grep -q 'periodSeconds: [1-9]' <<<"$readiness" || fail "readinessProbe needs a periodSeconds"
grep -q 'timeoutSeconds: [3-9]' <<<"$readiness" || fail "readinessProbe timeoutSeconds must give the dependency check room"
grep -q 'failureThreshold: [3-9]' <<<"$readiness" || fail "readinessProbe needs a failureThreshold of at least 3"
probe_block livenessProbe "$deployment" | grep -q 'failureThreshold: [3-9]' || fail "livenessProbe needs a failureThreshold of at least 3"

# --- container hardening ------------------------------------------------------
grep -q 'runAsNonRoot: true' <<<"$deployment" || fail "the pod must run as non-root"
grep -q 'readOnlyRootFilesystem: true' <<<"$deployment" || fail "the root filesystem must be read-only"
grep -q 'allowPrivilegeEscalation: false' <<<"$deployment" || fail "privilege escalation must be disabled"
grep -A2 'capabilities:' <<<"$deployment" | grep -q 'ALL' || fail "capabilities must drop ALL"

# --- graceful shutdown room ----------------------------------------------------
# SHUTDOWN_DRAIN_DELAY (5s default) + HTTP drain 10s + outbox relay drain 10s
# + inbound consumers drain 15s + consumer Close 5s = 45s.
grace="$(awk '/terminationGracePeriodSeconds:/ {print $2; exit}' <<<"$deployment")"
(( grace > 45 )) || fail "terminationGracePeriodSeconds is $grace; want more than 45 (drain delay + HTTP/relay/consumer drain bounds)"

# --- optional values reach the ConfigMap and the Deployment -------------------
tuned_args=("${base[@]}" --set config.outboxRelayInterval=7s --set config.outboxMaxAttempts=5 \
  --set config.slaSweepInterval=33s --set config.shutdownDrainDelay=3s \
  --set config.httpAddr=:9090 --set config.logLevel=debug)
tuned_cm="$(render_only configmap.yaml "${tuned_args[@]}")"
tuned_dep="$(render_only deployment.yaml "${tuned_args[@]}")"
for kv in 'OUTBOX_RELAY_INTERVAL: "7s"' 'OUTBOX_MAX_ATTEMPTS: "5"' 'SLA_SWEEP_INTERVAL: "33s"' \
  'SHUTDOWN_DRAIN_DELAY: "3s"' 'HTTP_ADDR: ":9090"' 'LOG_LEVEL: "debug"'; do
  grep -q "$kv" <<<"$tuned_cm" || fail "the ConfigMap does not render $kv"
done
for pair in OUTBOX_RELAY_INTERVAL=7s OUTBOX_MAX_ATTEMPTS=5 SLA_SWEEP_INTERVAL=33s SHUTDOWN_DRAIN_DELAY=3s \
  HTTP_ADDR=:9090 LOG_LEVEL=debug; do
  [[ "$(env_value "$tuned_dep" "${pair%%=*}")" == "${pair#*=}" ]] || fail "the Deployment does not render ${pair%%=*}=${pair#*=}"
done
# Unset optional values are not rendered at all, so the binary's defaults apply.
for var in OUTBOX_RELAY_INTERVAL SLA_SWEEP_INTERVAL SHUTDOWN_DRAIN_DELAY; do
  if grep -q "$var" <<<"$deployment"; then fail "$var must not be rendered while its value is empty"; fi
done
# The relay attempt cap defaults to 20 and 0 (unlimited) is rendered as is.
grep -q 'OUTBOX_MAX_ATTEMPTS: "20"' <<<"$configmap" || fail "OUTBOX_MAX_ATTEMPTS must default to 20"
unlimited="$(render_only configmap.yaml "${base[@]}" --set config.outboxMaxAttempts=0)"
grep -q 'OUTBOX_MAX_ATTEMPTS: "0"' <<<"$unlimited" || fail "config.outboxMaxAttempts=0 is not rendered"

# --- chart vs binary ------------------------------------------------------------
# Every env var cmd/api reads must be rendered into the Deployment when every
# optional value is set. The names come from the binary's own os.Getenv /
# getenv("...") literals and its *_CONSUMER_GROUP specs, so a new variable in
# cmd/api fails this test until the chart renders it. MIGRATIONS_DATABASE_URL
# is read by internal/adapters/outbound/postgres/migrate.go.
binary_env() {
  local f
  for f in "$repo_dir"/cmd/api/*.go; do
    case "$f" in *_test.go) continue ;; esac
    grep -hoE '[Gg]etenv\("[A-Z][A-Z0-9_]*"' "$f" || true
    grep -hoE '"[A-Z0-9_]+_CONSUMER_GROUP"' "$f" || true
  done | sed -E 's/.*"([A-Z0-9_]+)".*/\1/' | sort -u
}
# Word splitting is safe: the names are [A-Z0-9_] only (bash 3.2 has no mapfile).
# shellcheck disable=SC2207
api_vars=($(binary_env))
(( ${#api_vars[@]} >= 10 )) || fail "found only ${#api_vars[@]} env literals in cmd/api; the scan is looking in the wrong place"
full_deployment="$(render_only deployment.yaml "${everything[@]}")"
for var in "${api_vars[@]}" MIGRATIONS_DATABASE_URL; do
  grep -q "name: $var$" <<<"$full_deployment" || fail "the Deployment does not render $var, which the binary reads"
done
# Every key the ConfigMap renders is a variable cmd/api reads (no dead keys).
full_configmap="$(render_only configmap.yaml "${everything[@]}")"
known=" ${api_vars[*]} "
while read -r key; do
  [[ "$known" == *" $key "* ]] || fail "ConfigMap key $key is read by no binary"
done < <(awk '/^data:/ {d=1; next} d && /^  [A-Z]/ {sub(/:.*/, ""); sub(/^  /, ""); print}' <<<"$full_configmap")

# --- the Secret ----------------------------------------------------------------
secret="$(render_only secret.yaml "${base[@]}")"
grep -q 'DATABASE_URL: "postgres://u@example.invalid:5432/db"' <<<"$secret" || fail "database.url does not land in the chart Secret"
existing="$(render --set database.existingSecret=my-db --set kafka.brokers=k:9092)"
if grep -q 'kind: Secret' <<<"$existing"; then fail "an existingSecret must suppress the chart Secret"; fi
grep -A4 'name: DATABASE_URL$' <<<"$existing" | grep -q 'name: my-db' || fail "an existingSecret is not referenced by DATABASE_URL"

# --- refusals: the mandatory values name themselves -------------------------------
expect_refusal() { # expect_refusal <why> <needle> <helm args...>
  local why="$1" needle="$2" err; shift 2
  if err="$(helm template "$release" "$chart_dir" "$@" 2>&1)"; then
    fail "the chart must refuse to render: $why"
  fi
  grep -qF -- "$needle" <<<"$err" || fail "the refusal for '$why' does not name '$needle':
$err"
}
expect_refusal "no database source and no brokers" "database.url or database.existingSecret"
expect_refusal "no database source" "database.url or database.existingSecret" --set kafka.brokers=k:9092
expect_refusal "no brokers" "kafka.brokers" --set database.url=postgres://u@h/db
expect_refusal "an empty fulfillment group" "config.fulfillmentExceptionsConsumerGroup" "${base[@]}" --set config.fulfillmentExceptionsConsumerGroup=
expect_refusal "an empty work-planning group" "config.workPlanningExceptionsConsumerGroup" "${base[@]}" --set config.workPlanningExceptionsConsumerGroup=
expect_refusal "an empty network-fulfillment group" "config.networkFulfillmentExceptionsConsumerGroup" "${base[@]}" --set config.networkFulfillmentExceptionsConsumerGroup=
expect_refusal "an empty order-correlation group" "config.orderCorrelationConsumerGroup" "${base[@]}" --set config.orderCorrelationConsumerGroup=
expect_refusal "an empty inventory group" "config.inventoryExceptionsConsumerGroup" "${base[@]}" --set config.inventoryExceptionsConsumerGroup=
expect_refusal "an empty returns group" "config.returnsExceptionsConsumerGroup" "${base[@]}" --set config.returnsExceptionsConsumerGroup=
expect_refusal "a short grace period" "terminationGracePeriodSeconds" "${base[@]}" --set terminationGracePeriodSeconds=20
expect_refusal "a *_MODE variable through extraEnv" "_MODE" "${base[@]}" --set 'extraEnv[0].name=EXCEPTIONS_MODE' --set 'extraEnv[0].value=permissive'
expect_refusal "a *_MODE variable through mcp.extraEnv" "_MODE" "${base[@]}" --set mcp.enabled=true --set 'mcp.extraEnv[0].name=MCP_MODE' --set 'mcp.extraEnv[0].value=stdio'
expect_refusal "a *_MODE variable through analytics.projector.extraEnv" "_MODE" "${base[@]}" --set analytics.enabled=true --set analytics.url=postgres://u@h/a --set 'analytics.projector.extraEnv[0].name=PROJECTOR_MODE' --set 'analytics.projector.extraEnv[0].value=memory'
expect_refusal "analytics on but no DSN source" "analytics.url nor analytics.existingSecret" "${base[@]}" --set analytics.enabled=true
expect_refusal "analytics on but an empty consumer group" "analytics.consumerGroup" "${base[@]}" --set analytics.enabled=true --set analytics.url=postgres://u@h/a --set analytics.consumerGroup=
expect_refusal "mcp on but no database source" "database.url or database.existingSecret" --set kafka.brokers=k:9092 --set mcp.enabled=true

# --- the optional mcp component ---------------------------------------------------
# Off by default: the default render is the api alone.
default_render="$(render "${base[@]}")"
if grep -q 'app.kubernetes.io/component: mcp$' <<<"$default_render"; then
  fail "the mcp component must not render by default"
fi

# A binary's env literals (os.Getenv / getenv("...")) -- for cmd/<dir> -- must all be
# rendered by its Deployment, so a new variable fails until the chart carries it.
cmd_env() { # cmd_env <dir>...
  local d f
  for d in "$@"; do
    for f in "$repo_dir/cmd/$d"/*.go; do
      case "$f" in *_test.go) continue ;; esac
      grep -hoE '(os\.Getenv|getenv)\("[A-Z][A-Z0-9_]*"' "$f" || true
    done
  done | sed -E 's/.*"([A-Z0-9_]+)".*/\1/' | sort -u
}
assert_renders_env() { # assert_renders_env <label> <rendered deployment> <dir>...
  local label="$1" rendered="$2" var; shift 2
  local vars; vars="$(cmd_env "$@")"
  [[ -n "$vars" ]] || fail "found no env literals in cmd/$* ($label); the scan is looking in the wrong place"
  for var in $vars; do
    grep -q "name: $var$" <<<"$rendered" || fail "the $label Deployment does not render $var, which cmd/$* reads"
  done
}

# The READ-ONLY MCP server: Postgres only, its own Service, never Kafka.
mcp_dep="$(render_only mcp-deployment.yaml "${base[@]}" --set mcp.enabled=true)"
mcp_svc="$(render_only mcp-service.yaml "${base[@]}" --set mcp.enabled=true)"
grep -q 'app.kubernetes.io/component: mcp' <<<"$mcp_dep" || fail "the mcp Deployment is not labelled component=mcp"
grep -q 'command: \["/app/mcp"\]' <<<"$mcp_dep" || fail "the mcp Deployment must run /app/mcp"
grep -q 'app.kubernetes.io/component: mcp' <<<"$mcp_svc" || fail "the mcp Service must select component=mcp"
grep -A4 'name: DATABASE_URL$' <<<"$mcp_dep" | grep -q 'secretKeyRef:' || fail "the mcp DATABASE_URL must come from a Secret"
grep -A6 'name: MIGRATIONS_DATABASE_URL$' <<<"$mcp_dep" | grep -q 'optional: true' || fail "the mcp MIGRATIONS_DATABASE_URL must be an optional secretKeyRef"
if grep -Eq 'name: (KAFKA_BROKERS|[A-Z]+_CONSUMER_GROUP|OUTBOX_[A-Z_]+|SLA_SWEEP_INTERVAL)$' <<<"$mcp_dep"; then
  fail "the mcp Deployment must not get Kafka/relay/sweep/consumer env (read-only, never dials Kafka)"
fi
no_mode "mcp Deployment" "$mcp_dep"
mcp_full="$(render_only mcp-deployment.yaml "${everything[@]}")"
assert_renders_env "mcp" "$mcp_full" mcp
grep -q "name: MIGRATIONS_DATABASE_URL$" <<<"$mcp_full" || fail "the mcp Deployment does not render MIGRATIONS_DATABASE_URL (migrations step)"
[[ "$(env_value "$(render_only mcp-deployment.yaml "${base[@]}" --set mcp.enabled=true --set mcp.httpAddr=:9191)" MCP_ADDR)" == ":9191" ]] || fail "mcp.httpAddr does not reach MCP_ADDR"

# api + mcp together: both components render, each labelled.
together="$(render "${base[@]}" --set mcp.enabled=true)"
for component in api mcp; do
  grep -q "app.kubernetes.io/component: $component\$" <<<"$together" || fail "$component was not rendered when mcp.enabled=true"
done

# --- the optional frontend component (exceptions_mfe) -----------------------
# Off by default: the default render is the api alone.
if grep -q 'app.kubernetes.io/component: frontend$' <<<"$default_render"; then
  fail "the frontend component must not render by default"
fi

# On: its own Deployment + Service, both pinned to component=frontend (so the
# api Service never selects the frontend pods), probes on /healthz, the image
# from frontend.image, and NO env at all -- a static nginx pod that never sees
# the DSN, the brokers or a consumer group (the SPA reads its API origin at
# runtime from the console's /config.json).
fe_dep="$(render_only frontend-deployment.yaml "${base[@]}" --set frontend.enabled=true)"
fe_svc="$(render_only frontend-service.yaml "${base[@]}" --set frontend.enabled=true)"
grep -q 'app.kubernetes.io/component: frontend' <<<"$fe_dep" || fail "the frontend Deployment is not labelled component=frontend"
grep -q 'app.kubernetes.io/component: frontend' <<<"$fe_svc" || fail "the frontend Service must select component=frontend"
grep -q 'path: /healthz' <<<"$fe_dep" || fail "the frontend probes must hit /healthz"
if grep -qE '^          env:' <<<"$fe_dep"; then
  fail "the frontend Deployment must carry no env (static nginx; it never sees the DSN, brokers or consumer groups)"
fi
grep -q 'image: "warehouse/exception-management-frontend:' <<<"$fe_dep" || fail "the frontend image repository does not come from frontend.image.repository"
fe_tag="$(render_only frontend-deployment.yaml "${base[@]}" --set frontend.enabled=true --set frontend.image.tag=local-abc123)"
grep -q 'warehouse/exception-management-frontend:local-abc123' <<<"$fe_tag" || fail "frontend.image.tag does not reach the frontend image reference"
no_mode "frontend Deployment" "$fe_dep"

# api + mcp + frontend together: every component renders, each labelled.
all3="$(render "${base[@]}" --set mcp.enabled=true --set frontend.enabled=true)"
for component in api mcp frontend; do
  grep -q "app.kubernetes.io/component: $component\$" <<<"$all3" || fail "$component was not rendered when mcp.enabled=true and frontend.enabled=true"
done

# --- the optional analytics components (ADR 0006) ---------------------------
# Off by default: the default render is the api alone.
if grep -q 'app.kubernetes.io/component: analytics-' <<<"$default_render"; then
  fail "the analytics components must not render by default"
fi

# On: the projector Deployment (no Service) and the reports Deployment +
# Service, each pinned to its own component label; the reports Service
# selects ONLY the reports pods.
analytics_args=(--set analytics.enabled=true --set analytics.url=postgres://u@h/analytics)
prj_dep="$(render_only projector-deployment.yaml "${base[@]}" "${analytics_args[@]}")"
rep_docs="$(render_only reports-deployment.yaml "${base[@]}" "${analytics_args[@]}")"
grep -q 'app.kubernetes.io/component: analytics-projector' <<<"$prj_dep" || fail "the projector Deployment is not labelled component=analytics-projector"
grep -q 'command: \["/app/exceptions-projector"\]' <<<"$prj_dep" || fail "the projector Deployment must run /app/exceptions-projector"
if grep -q 'kind: Service' <<<"$prj_dep"; then fail "the projector must not render a Service (admin port only)"; fi
grep -q 'app.kubernetes.io/component: analytics-reports' <<<"$rep_docs" || fail "the reports Deployment/Service is not labelled component=analytics-reports"
grep -q 'command: \["/app/exceptions-reports"\]' <<<"$rep_docs" || fail "the reports Deployment must run /app/exceptions-reports"
grep -A4 'app.kubernetes.io/component: analytics-reports' <<<"$rep_docs" | grep -q 'selector:' || fail "the reports Service must select component=analytics-reports"
# The projector gets the analytical DSN from a Secret, never from a ConfigMap.
grep -A4 'name: ANALYTICS_DATABASE_URL$' <<<"$prj_dep" | grep -q 'secretKeyRef:' || fail "the projector ANALYTICS_DATABASE_URL must come from a Secret"
# The projector never sees OLTP env; the reports binary never dials Kafka.
if grep -Eq 'name: (DATABASE_URL|MIGRATIONS_DATABASE_URL)$' <<<"$prj_dep"; then
  fail "the projector Deployment must not get OLTP env (it never opens the OLTP database)"
fi
if grep -Eq 'name: (KAFKA_BROKERS|ANALYTICS_CONSUMER_GROUP|[A-Z]+_CONSUMER_GROUP)$' <<<"$rep_docs"; then
  fail "the reports Deployment must not get Kafka/consumer env (read-only, never dials Kafka)"
fi
no_mode "projector Deployment" "$prj_dep"
no_mode "reports Deployment" "$rep_docs"
# analytics.url lands in the chart's analytics Secret; existingSecret suppresses it.
an_secret="$(render_only analytics-secret.yaml "${base[@]}" "${analytics_args[@]}")"
grep -q 'ANALYTICS_DATABASE_URL: "postgres://u@h/analytics"' <<<"$an_secret" || fail "analytics.url does not land in the analytics Secret"
an_existing="$(render "${base[@]}" --set analytics.enabled=true --set analytics.existingSecret=my-analytics-db)"
if grep -q 'kind: Secret' <<<"$(render_only analytics-secret.yaml "${base[@]}" --set analytics.enabled=true --set analytics.existingSecret=my-analytics-db 2>/dev/null)"; then
  fail "an analytics.existingSecret must suppress the chart analytics Secret"
fi
grep -A4 'name: ANALYTICS_DATABASE_URL$' <<<"$an_existing" | grep -q 'name: my-analytics-db' || fail "an analytics.existingSecret is not referenced by ANALYTICS_DATABASE_URL"
# The consumer group and admin/http addr reach the containers.
[[ "$(env_value "$prj_dep" ANALYTICS_CONSUMER_GROUP)" == "exception-management-analytics" ]] || fail "analytics.consumerGroup does not reach ANALYTICS_CONSUMER_GROUP"
[[ "$(env_value "$prj_dep" ADMIN_ADDR)" == ":8091" ]] || fail "analytics.projector.adminAddr does not reach ADMIN_ADDR"
[[ "$(env_value "$rep_docs" HTTP_ADDR)" == ":8092" ]] || fail "analytics.reports.httpAddr does not reach HTTP_ADDR"
# Every env literal of the two analytics binaries is rendered (chart vs binary).
prj_full="$(render_only projector-deployment.yaml "${everything[@]}")"
rep_full="$(render_only reports-deployment.yaml "${everything[@]}")"
assert_renders_env "projector" "$prj_full" exceptions-projector
assert_renders_env "reports" "$rep_full" exceptions-reports

# api + mcp + frontend + analytics together: every component renders, each labelled.
all4="$(render "${base[@]}" --set mcp.enabled=true --set frontend.enabled=true "${analytics_args[@]}")"
for component in api mcp frontend analytics-projector analytics-reports; do
  grep -q "app.kubernetes.io/component: $component\$" <<<"$all4" || fail "$component was not rendered when every optional component is on"
done

# --- the Gateway API routes + Kong plugins (fleet decision 2026-10-13) ------
# Off by default: standalone installs without Kong are unaffected.
if grep -q 'kind: HTTPRoute' <<<"$default_render"; then
  fail "an HTTPRoute must not render by default (gatewayApi.enabled is false)"
fi
if grep -q 'kind: KongPlugin' <<<"$default_render"; then
  fail "a KongPlugin must not render by default (every kongPlugins.*.enabled is false)"
fi
# The mcp route stays off until mcp.route.enabled, even with mcp on.
if render_only mcp-route.yaml "${base[@]}" --set mcp.enabled=true 2>&1 | grep -q 'kind: HTTPRoute'; then
  fail "the mcp route must stay off until mcp.route.enabled"
fi
# With everything on (ct lint's values file): the api route strips
# /api/exception-management, the mcp route rewrites to /mcp and both carry
# the konghq.com/plugins annotation naming three rendered KongPlugins (the
# deep contract — forbidden plugins, cors headers, backends — is
# tests/test_kong_config.py).
route="$(render_only httproute.yaml "${everything[@]}")"
grep -q 'kind: HTTPRoute' <<<"$route" || fail "httproute.yaml does not render with gatewayApi.enabled=true"
grep -q 'value: /api/exception-management' <<<"$route" || fail "the api HTTPRoute does not match /api/exception-management"
grep -q 'replacePrefixMatch: /' <<<"$route" || fail "the api HTTPRoute does not strip its prefix"
grep -q 'konghq.com/plugins:' <<<"$route" || fail "the api HTTPRoute has no konghq.com/plugins annotation"
mcp_route="$(render_only mcp-route.yaml "${everything[@]}")"
grep -q 'value: /api/mcp/exception-management' <<<"$mcp_route" || fail "the mcp HTTPRoute does not match /api/mcp/exception-management"
grep -q 'replacePrefixMatch: /mcp' <<<"$mcp_route" || fail "the mcp HTTPRoute does not rewrite to /mcp"
grep -q "name: $release-mcp" <<<"$mcp_route" || fail "the mcp HTTPRoute does not target the -mcp Service"
plugins="$(render_only kong-plugins.yaml "${everything[@]}")"
for plugin in cors prometheus opentelemetry; do
  grep -q "plugin: $plugin" <<<"$plugins" || fail "the $plugin KongPlugin does not render when enabled"
done
if grep -q 'origins:\s*$' <<<"$plugins" && grep -A1 'origins:' <<<"$plugins" | grep -q '\*'; then
  fail "the cors plugin must never allow origin '*' (endpoints are unauthenticated)"
fi
# The analytics reports Service and the frontend stay off the host: routing
# for them is not this chart's business (reports is ClusterIP-only, the
# Nginx web gateway owns the frontend).
if grep -E 'kind: (Ingress|HTTPRoute)' <<<"$all4" | grep -q frontend; then
  fail "frontend routing must not live in this chart (the Nginx web gateway owns it)"
fi
if grep -E 'kind: (Ingress|HTTPRoute)' <<<"$all4" | grep -q reports; then
  fail "the reports Service stays ClusterIP-only; it gets no route"
fi

echo "PASS: exception-management chart render assertions"
