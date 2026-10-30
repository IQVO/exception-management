#!/usr/bin/env python3
"""Assert this chart's Service selectors isolate each component, and that every
Deployment carries the real runtime's environment.

Why the selector check exists (fleet-wide): `app.kubernetes.io/name` +
`instance` are identical on every pod a release creates. A Service that
selects on those two alone selects ALL components -- verified live in other
fleet charts, where a request for an api /healthz was answered by the wrong
pod. Every Deployment here carries `app.kubernetes.io/component` in
selector.matchLabels and its pod labels, and every Service selects on it, so
each Service selects EXACTLY ONE Deployment. This test fails if that ever
stops being true.

Components: api (always), mcp (the read-only ops-agent surface, default off),
a2a (the read-only Agent2Agent agent, default off)
and frontend (the exceptions_mfe Module Federation remote, default off). The
enabled renders run the components TOGETHER.

It also asserts the real runtime: the mandatory env (database secret, brokers,
the six pinned consumer-group ids) is rendered, no *_MODE variable is ever
rendered (this service has no modes), and unset optional values stay absent so
the binary's defaults apply. The frontend is a static nginx pod: it carries NO
env at all (never a DSN, never brokers, never a consumer group).

Run: python3 charts/exception-management/tests/test_service_selectors.py
Needs: helm, PyYAML.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
RELEASE = "exception-management"

# Dummy values, no password: the chart refuses to render without a database
# source and without Kafka brokers.
BASE = [
    "--set", "database.url=postgres://u@example.invalid:5432/db",
    "--set", "kafka.brokers=kafka.example.invalid:9092",
]
ENABLE_EVERYTHING = BASE + [
    "--set", "mcp.enabled=true",
    "--set", "a2a.enabled=true",
    "--set", "config.outboxRelayInterval=500ms",
    "--set", "config.shutdownDrainDelay=5s",
    "--set", "config.slaSweepInterval=45s",
    "--set", "config.logLevel=debug",
]

# Every env var cmd/api reads that has a dedicated chart value.
API_ENV = (
    "HTTP_ADDR", "LOG_LEVEL", "DATABASE_URL", "MIGRATIONS_DATABASE_URL", "KAFKA_BROKERS",
    "FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP",
    "NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "ORDER_CORRELATION_CONSUMER_GROUP",
    "INVENTORY_EXCEPTIONS_CONSUMER_GROUP", "RETURNS_EXCEPTIONS_CONSUMER_GROUP",
    "CARTON_DISCREPANCY_CONSUMER_GROUP",
    "OUTBOX_RELAY_INTERVAL", "OUTBOX_MAX_ATTEMPTS", "SLA_SWEEP_INTERVAL", "SHUTDOWN_DRAIN_DELAY",
)
# Env that must stay off with default values so the binary's defaults apply.
OFF_BY_DEFAULT = ("OUTBOX_RELAY_INTERVAL", "SLA_SWEEP_INTERVAL", "SHUTDOWN_DRAIN_DELAY")


def helm_template(extra_args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR), *extra_args],
        capture_output=True, text=True,
    )


def render(extra_args: list[str]) -> list[dict]:
    result = helm_template(extra_args)
    if result.returncode != 0:
        raise SystemExit(f"FAIL: helm template {' '.join(extra_args)}:\n{result.stderr}")
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def selector_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("selector") or {}


def pod_labels_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("template", {}).get("metadata", {}).get("labels") or {}


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def env_of(dep: dict) -> list[dict]:
    return dep["spec"]["template"]["spec"]["containers"][0].get("env", [])


def env_names(dep: dict) -> list[str]:
    return [e["name"] for e in env_of(dep)]


def env_value(dep: dict, name: str) -> str | None:
    return next((e.get("value") for e in env_of(dep) if e["name"] == name), None)


def mode_names(names: list[str]) -> list[str]:
    return sorted(n for n in names if n.endswith("_MODE"))


def check_selectors(docs: list[dict], failures: list[str]) -> None:
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    expected = {RELEASE, f"{RELEASE}-mcp", f"{RELEASE}-a2a"}
    if set(deployments) != expected:
        failures.append(f"expected Deployments {sorted(expected)}, got {sorted(deployments)}")
    if set(services) != expected:
        failures.append(f"expected a Service per Deployment, got {sorted(services)}")

    for name, component in ((RELEASE, "api"), (f"{RELEASE}-mcp", "mcp"), (f"{RELEASE}-a2a", "a2a")):
        if name in services and selector_of(services[name]).get("app.kubernetes.io/component") != component:
            failures.append(f"Service {name} selector must pin component={component}")
        if name in deployments:
            labels = deployments[name]["metadata"].get("labels", {})
            if labels.get("app.kubernetes.io/component") != component:
                failures.append(f"Deployment {name} must be labelled component={component}")

    # Every Deployment's own selector must pin a component too, and be
    # satisfied by its pod labels.
    for name, dep in deployments.items():
        match_labels = dep["spec"]["selector"].get("matchLabels") or {}
        if "app.kubernetes.io/component" not in match_labels:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
        if not matches(match_labels, pod_labels_of(dep)):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")

    # Every Service's named target port exists on its container.
    for name, svc in services.items():
        dep = deployments.get(name)
        if dep is None:
            continue
        ports = {p["name"] for p in dep["spec"]["template"]["spec"]["containers"][0].get("ports", [])}
        if "http" not in ports:
            failures.append(f"the {name} container does not expose the named port 'http' the Service targets")

    api = deployments.get(RELEASE)
    if api is None:
        failures.append("the api Deployment was not rendered")
        return

    check_env(api, failures)
    check_mcp(deployments.get(f"{RELEASE}-mcp"), failures)
    check_a2a(deployments.get(f"{RELEASE}-a2a"), failures)


def command_of(dep: dict) -> list[str] | None:
    return dep["spec"]["template"]["spec"]["containers"][0].get("command")


def check_mcp(dep: dict | None, failures: list[str]) -> None:
    if dep is None:
        return
    if command_of(dep) != ["/app/mcp"]:
        failures.append("the MCP Deployment must run /app/mcp")
    env = set(env_names(dep))
    # Read-only: it never relays, never sweeps, never consumes, never dials Kafka.
    stray = {
        "KAFKA_BROKERS", "OUTBOX_RELAY_INTERVAL", "OUTBOX_MAX_ATTEMPTS", "SHUTDOWN_DRAIN_DELAY",
        "SLA_SWEEP_INTERVAL",
        "FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP",
        "NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "ORDER_CORRELATION_CONSUMER_GROUP",
        "INVENTORY_EXCEPTIONS_CONSUMER_GROUP", "RETURNS_EXCEPTIONS_CONSUMER_GROUP",
    "CARTON_DISCREPANCY_CONSUMER_GROUP",
    } & env
    stray.update(mode_names(env_names(dep)))
    if stray:
        failures.append(f"the MCP Deployment must not get Kafka/relay/sweep/consumer env (it never dials Kafka): {sorted(stray)}")
    for want in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL", "MCP_ADDR", "LOG_LEVEL"):
        if want not in env:
            failures.append(f"the MCP Deployment does not render {want}")


def check_a2a(dep: dict | None, failures: list[str]) -> None:
    """The A2A agent is cmd/mcp's read-only twin: Postgres only, never Kafka."""
    if dep is None:
        return
    if command_of(dep) != ["/app/a2a"]:
        failures.append("the A2A Deployment must run /app/a2a")
    env = set(env_names(dep))
    stray = {
        "KAFKA_BROKERS", "EVENT_PUBLISHER", "OUTBOX_RELAY_INTERVAL", "OUTBOX_MAX_ATTEMPTS", "SHUTDOWN_DRAIN_DELAY",
        "SLA_SWEEP_INTERVAL",
        "FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP",
        "NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "ORDER_CORRELATION_CONSUMER_GROUP",
        "INVENTORY_EXCEPTIONS_CONSUMER_GROUP", "RETURNS_EXCEPTIONS_CONSUMER_GROUP",
        "CARTON_DISCREPANCY_CONSUMER_GROUP",
    } & env
    stray.update(mode_names(env_names(dep)))
    if stray:
        failures.append(f"the A2A Deployment must not get Kafka/publisher/relay/sweep/consumer env (read-only, never dials Kafka): {sorted(stray)}")
    for want in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL", "A2A_ADDR", "A2A_AGENT_URL", "LOG_LEVEL"):
        if want not in env:
            failures.append(f"the A2A Deployment does not render {want}")
    if env_value(dep, "A2A_ADDR") != ":8093":
        failures.append(f"the A2A Deployment must listen on :8093, got {env_value(dep, 'A2A_ADDR')!r}")
    url = env_value(dep, "A2A_AGENT_URL") or ""
    if not (url.startswith("http://") and url.endswith("-a2a.default.svc.cluster.local:8093/")):
        failures.append(f"A2A_AGENT_URL must default to the in-cluster Service DNS, got {url!r}")


def check_env(api: dict, failures: list[str]) -> None:
    api_env = env_names(api)
    for want in API_ENV:
        if want not in api_env:
            failures.append(f"the api Deployment does not render {want} from its dedicated value")
    banned = mode_names(api_env)
    if banned:
        failures.append(f"the api Deployment renders {banned}: *_MODE variables do not exist in this service")
    duplicates = sorted({n for n in api_env if api_env.count(n) > 1})
    if duplicates:
        failures.append(f"the api Deployment renders env vars twice: {duplicates}")
    for name, want in (
        ("KAFKA_BROKERS", "kafka.example.invalid:9092"),
        ("FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "exception-management-fulfillment"),
        ("WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP", "exception-management-work-planning"),
        ("NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP", "exception-management-network-fulfillment"),
        ("ORDER_CORRELATION_CONSUMER_GROUP", "exception-management-order-correlation"),
        ("INVENTORY_EXCEPTIONS_CONSUMER_GROUP", "exception-management-inventory"),
        ("RETURNS_EXCEPTIONS_CONSUMER_GROUP", "exception-management-returns"),
        ("CARTON_DISCREPANCY_CONSUMER_GROUP", "exception-management-carton-discrepancy"),
        ("OUTBOX_RELAY_INTERVAL", "500ms"),
        ("SLA_SWEEP_INTERVAL", "45s"),
        ("SHUTDOWN_DRAIN_DELAY", "5s"),
    ):
        if env_value(api, name) != want:
            failures.append(f"the api Deployment renders {name}={env_value(api, name)!r}, want {want!r}")


def check_defaults(failures: list[str]) -> None:
    """Default values deploy the api only."""
    defaults = render(BASE)
    kinds = sorted(d["kind"] for d in defaults)
    if sorted(k for k in kinds if k in {"Deployment", "Service"}) != ["Deployment", "Service"]:
        failures.append("default values must deploy the api only (one Deployment, one Service)")
    stray = [d["metadata"]["name"] for d in defaults if d["metadata"]["name"].endswith(("-mcp", "-a2a", "-frontend", "-projector", "-reports"))]
    if stray:
        failures.append(f"optional components rendered with default values: {stray}")
    dep = next(d for d in defaults if d.get("kind") == "Deployment")
    names = env_names(dep)
    banned = mode_names(names)
    if banned:
        failures.append(f"{banned} must not be rendered with default values")
    if set(OFF_BY_DEFAULT) & set(names):
        failures.append(f"these must be off with default values: {sorted(set(OFF_BY_DEFAULT) & set(names))}")


def check_refusals(failures: list[str]) -> None:
    for label, args, needle in (
        ("without a database source", ["--set", "kafka.brokers=k:9092"], "database.url or database.existingSecret"),
        ("without Kafka brokers", ["--set", "database.url=postgres://u@h/db"], "kafka.brokers"),
        ("an empty fulfillment group", BASE + ["--set", "config.fulfillmentExceptionsConsumerGroup="], "config.fulfillmentExceptionsConsumerGroup"),
        ("an empty work-planning group", BASE + ["--set", "config.workPlanningExceptionsConsumerGroup="], "config.workPlanningExceptionsConsumerGroup"),
        ("an empty network-fulfillment group", BASE + ["--set", "config.networkFulfillmentExceptionsConsumerGroup="], "config.networkFulfillmentExceptionsConsumerGroup"),
        ("an empty order-correlation group", BASE + ["--set", "config.orderCorrelationConsumerGroup="], "config.orderCorrelationConsumerGroup"),
        ("an empty inventory group", BASE + ["--set", "config.inventoryExceptionsConsumerGroup="], "config.inventoryExceptionsConsumerGroup"),
        ("an empty returns group", BASE + ["--set", "config.returnsExceptionsConsumerGroup="], "config.returnsExceptionsConsumerGroup"),
        ("an empty carton-discrepancy group", BASE + ["--set", "config.cartonDiscrepancyConsumerGroup="], "config.cartonDiscrepancyConsumerGroup"),
        ("a *_MODE variable through extraEnv",
         BASE + ["--set", "extraEnv[0].name=EXCEPTIONS_MODE", "--set", "extraEnv[0].value=permissive"], "EXCEPTIONS_MODE"),
        ("a *_MODE variable through mcp.extraEnv",
         BASE + ["--set", "mcp.enabled=true", "--set", "mcp.extraEnv[0].name=MCP_MODE", "--set", "mcp.extraEnv[0].value=stdio"], "MCP_MODE"),
        ("a *_MODE variable through a2a.extraEnv",
         BASE + ["--set", "a2a.enabled=true", "--set", "a2a.extraEnv[0].name=A2A_MODE", "--set", "a2a.extraEnv[0].value=stdio"], "A2A_MODE"),
        ("analytics on without a DSN source",
         BASE + ["--set", "analytics.enabled=true"], "ANALYTICS_DATABASE_URL"),
        ("analytics on with an empty consumer group",
         BASE + ["--set", "analytics.enabled=true", "--set", "analytics.url=postgres://u@h/a", "--set", "analytics.consumerGroup="], "analytics.consumerGroup"),
        ("a short grace period", BASE + ["--set", "terminationGracePeriodSeconds=20"], "terminationGracePeriodSeconds"),
    ):
        result = helm_template(args)
        if result.returncode == 0 or needle not in result.stderr:
            failures.append(f"chart rendered (or failed for another reason) {label}")


def check_component_set(extra_args: list[str], expected: dict[str, str], failures: list[str]) -> None:
    """Render BASE + extra_args and assert the per-component invariants:
    exactly the expected Deployment/Service set, every one pinned to its own
    app.kubernetes.io/component, and each Service selecting EXACTLY ONE
    Deployment."""
    docs = render(BASE + extra_args)
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    if set(deployments) != set(expected):
        failures.append(f"expected Deployments {sorted(expected)}, got {sorted(deployments)} ({extra_args})")
        return
    if set(services) != set(expected):
        failures.append(f"expected a Service per Deployment, got {sorted(services)} ({extra_args})")

    for name, component in expected.items():
        if name in services and selector_of(services[name]).get("app.kubernetes.io/component") != component:
            failures.append(f"Service {name} selector must pin component={component}")
        if name in deployments:
            labels = deployments[name]["metadata"].get("labels", {})
            if labels.get("app.kubernetes.io/component") != component:
                failures.append(f"Deployment {name} must be labelled component={component}")
        if name in deployments:
            match_labels = deployments[name]["spec"]["selector"].get("matchLabels") or {}
            if "app.kubernetes.io/component" not in match_labels:
                failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
            if not matches(match_labels, pod_labels_of(deployments[name])):
                failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")

    for name, svc in services.items():
        dep = deployments.get(name)
        if dep is None:
            continue
        ports = {p["name"] for p in dep["spec"]["template"]["spec"]["containers"][0].get("ports", [])}
        if "http" not in ports:
            failures.append(f"the {name} container does not expose the named port 'http' the Service targets")


def check_frontend(failures: list[str]) -> None:
    # frontend alone with the api, and all three components together.
    check_component_set(
        ["--set", "frontend.enabled=true"],
        {RELEASE: "api", f"{RELEASE}-frontend": "frontend"},
        failures,
    )
    check_component_set(
        ["--set", "mcp.enabled=true", "--set", "frontend.enabled=true"],
        {RELEASE: "api", f"{RELEASE}-mcp": "mcp", f"{RELEASE}-frontend": "frontend"},
        failures,
    )

    check_component_set(
        ["--set", "mcp.enabled=true", "--set", "a2a.enabled=true", "--set", "frontend.enabled=true"],
        {RELEASE: "api", f"{RELEASE}-mcp": "mcp", f"{RELEASE}-a2a": "a2a", f"{RELEASE}-frontend": "frontend"},
        failures,
    )

    docs = render(BASE + ["--set", "frontend.enabled=true"])
    fe = next(
        (d for d in docs if d.get("kind") == "Deployment" and d["metadata"]["name"] == f"{RELEASE}-frontend"),
        None,
    )
    if fe is None:
        failures.append("the frontend Deployment was not rendered with frontend.enabled=true")
        return
    # A static nginx pod: no env AT ALL -- it never sees the DSN, the brokers
    # or a consumer group (it serves bytes; the SPA reads its API origin at
    # runtime from the console's /config.json).
    if env_of(fe):
        failures.append(f"the frontend Deployment must carry no env (static nginx), got {env_names(fe)}")
    container = fe["spec"]["template"]["spec"]["containers"][0]
    for probe in ("livenessProbe", "readinessProbe"):
        if container.get(probe, {}).get("httpGet", {}).get("path") != "/healthz":
            failures.append(f"the frontend {probe} must hit /healthz (nginx's own probe endpoint)")
    fesvc = next(
        (d for d in docs if d.get("kind") == "Service" and d["metadata"]["name"] == f"{RELEASE}-frontend"),
        None,
    )
    if fesvc is not None and fesvc["spec"]["type"] != "ClusterIP":
        failures.append("the frontend Service must stay ClusterIP (the Nginx web gateway owns exposure)")


def check_analytics(failures: list[str]) -> None:
    """The analytics read side (ADR 0006): the projector Deployment (NO
    Service of its own) and the reports Deployment + Service, each pinned to
    its own component so a Service selects exactly one Deployment."""
    analytics = ["--set", "analytics.enabled=true", "--set", "analytics.url=postgres://u@h/analytics"]
    docs = render(BASE + analytics)
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}

    expected_deps = {RELEASE: "api", f"{RELEASE}-projector": "analytics-projector", f"{RELEASE}-reports": "analytics-reports"}
    if set(deployments) != set(expected_deps):
        failures.append(f"expected Deployments {sorted(expected_deps)} with analytics on, got {sorted(deployments)}")
        return
    # The projector deliberately has NO Service (admin port only): exactly
    # the api and the reports Service may exist.
    if set(services) != {RELEASE, f"{RELEASE}-reports"}:
        failures.append(f"expected Services [{RELEASE}, {RELEASE}-reports] with analytics on (no projector Service), got {sorted(services)}")
    for name, component in expected_deps.items():
        labels = deployments[name]["metadata"].get("labels", {})
        if labels.get("app.kubernetes.io/component") != component:
            failures.append(f"Deployment {name} must be labelled component={component}")
        match_labels = deployments[name]["spec"]["selector"].get("matchLabels") or {}
        if match_labels.get("app.kubernetes.io/component") != component:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component={component}")
        if not matches(match_labels, pod_labels_of(deployments[name])):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")

    projector = deployments[f"{RELEASE}-projector"]
    reports = deployments[f"{RELEASE}-reports"]
    if command_of(projector) != ["/app/exceptions-projector"]:
        failures.append("the projector Deployment must run /app/exceptions-projector")
    if command_of(reports) != ["/app/exceptions-reports"]:
        failures.append("the reports Deployment must run /app/exceptions-reports")

    prj_env = set(env_names(projector))
    for want in ("ANALYTICS_DATABASE_URL", "KAFKA_BROKERS", "ANALYTICS_CONSUMER_GROUP", "ADMIN_ADDR", "LOG_LEVEL"):
        if want not in prj_env:
            failures.append(f"the projector Deployment does not render {want}")
    # The projector never opens the OLTP database and reads none of cmd/api's
    # consumer groups.
    prj_stray = ({"DATABASE_URL", "MIGRATIONS_DATABASE_URL"}
                 | {n for n in prj_env if n.endswith("_CONSUMER_GROUP") and n != "ANALYTICS_CONSUMER_GROUP"}) & prj_env
    prj_stray |= set(mode_names(list(prj_env)))
    if prj_stray:
        failures.append(f"the projector Deployment must not get OLTP/consumer env: {sorted(prj_stray)}")

    rep_env = set(env_names(reports))
    for want in ("ANALYTICS_DATABASE_URL", "HTTP_ADDR", "LOG_LEVEL"):
        if want not in rep_env:
            failures.append(f"the reports Deployment does not render {want}")
    rep_stray = ({"KAFKA_BROKERS", "DATABASE_URL", "MIGRATIONS_DATABASE_URL"}
                 | {n for n in rep_env if n.endswith("_CONSUMER_GROUP")}) & rep_env
    rep_stray |= set(mode_names(list(rep_env)))
    if rep_stray:
        failures.append(f"the reports Deployment must not get Kafka/OLTP/consumer env (read-only, never dials Kafka): {sorted(rep_stray)}")

    # Both get the analytical DSN from the analytics Secret, never from values.
    for dep, label in ((projector, "projector"), (reports, "reports")):
        dsn = next((e for e in env_of(dep) if e["name"] == "ANALYTICS_DATABASE_URL"), None)
        ref = (dsn or {}).get("valueFrom", {}).get("secretKeyRef", {})
        if not ref:
            failures.append(f"the {label} ANALYTICS_DATABASE_URL must come from a Secret (secretKeyRef)")
        elif ref.get("name") != f"{RELEASE}-analytics" or ref.get("key") != "ANALYTICS_DATABASE_URL":
            failures.append(f"the {label} ANALYTICS_DATABASE_URL secretKeyRef is {ref}")

    # Every optional component together: api + mcp + frontend + projector +
    # reports, each labelled, each Service selecting exactly one Deployment.
    everything = render(BASE + analytics + ["--set", "mcp.enabled=true", "--set", "frontend.enabled=true"])
    all_deps = {d["metadata"]["name"]: d for d in everything if d.get("kind") == "Deployment"}
    all_svcs = {d["metadata"]["name"]: d for d in everything if d.get("kind") == "Service"}
    want = {RELEASE: "api", f"{RELEASE}-mcp": "mcp", f"{RELEASE}-frontend": "frontend",
            f"{RELEASE}-projector": "analytics-projector", f"{RELEASE}-reports": "analytics-reports"}
    if set(all_deps) != set(want):
        failures.append(f"expected every component's Deployment {sorted(want)}, got {sorted(all_deps)}")
    if set(all_svcs) != {RELEASE, f"{RELEASE}-mcp", f"{RELEASE}-frontend", f"{RELEASE}-reports"}:
        failures.append(f"expected Services for api/mcp/frontend/reports only, got {sorted(all_svcs)}")
    for svc_name, svc in all_svcs.items():
        sel = selector_of(svc)
        hit = [d for d, dep in all_deps.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")


def main() -> int:
    failures: list[str] = []

    check_selectors(render(ENABLE_EVERYTHING), failures)
    check_frontend(failures)
    check_analytics(failures)
    check_defaults(failures)
    check_refusals(failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print("PASS: every Service selects exactly one Deployment (api, mcp, a2a, frontend, analytics projector/reports); "
          "mcp, a2a, frontend and analytics are off by default; the projector has no Service and never sees OLTP env, "
          "the reports binary never dials Kafka; the frontend pod carries no env and its Service stays ClusterIP; "
          "every cmd/api env var has a dedicated value and no *_MODE is rendered; "
          "the chart refuses to render without a database source, without brokers, "
          "with an empty consumer group, with analytics on but no DSN source or consumer group, "
          "with a *_MODE injected through extraEnv, mcp.extraEnv or a2a.extraEnv, or with a too-short termination grace period")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
