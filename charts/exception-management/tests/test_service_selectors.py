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

Components: api (always) and mcp (the read-only ops-agent surface, default
off). The enabled render runs both TOGETHER.

It also asserts the real runtime: the mandatory env (database secret, brokers,
the six pinned consumer-group ids) is rendered, no *_MODE variable is ever
rendered (this service has no modes), and unset optional values stay absent so
the binary's defaults apply.

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

    expected = {RELEASE, f"{RELEASE}-mcp"}
    if set(deployments) != expected:
        failures.append(f"expected Deployments {sorted(expected)}, got {sorted(deployments)}")
    if set(services) != expected:
        failures.append(f"expected a Service per Deployment, got {sorted(services)}")

    for name, component in ((RELEASE, "api"), (f"{RELEASE}-mcp", "mcp")):
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
    } & env
    stray.update(mode_names(env_names(dep)))
    if stray:
        failures.append(f"the MCP Deployment must not get Kafka/relay/sweep/consumer env (it never dials Kafka): {sorted(stray)}")
    for want in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL", "MCP_ADDR", "LOG_LEVEL"):
        if want not in env:
            failures.append(f"the MCP Deployment does not render {want}")


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
    stray = [d["metadata"]["name"] for d in defaults if d["metadata"]["name"].endswith("-mcp")]
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
        ("a *_MODE variable through extraEnv",
         BASE + ["--set", "extraEnv[0].name=EXCEPTIONS_MODE", "--set", "extraEnv[0].value=permissive"], "EXCEPTIONS_MODE"),
        ("a *_MODE variable through mcp.extraEnv",
         BASE + ["--set", "mcp.enabled=true", "--set", "mcp.extraEnv[0].name=MCP_MODE", "--set", "mcp.extraEnv[0].value=stdio"], "MCP_MODE"),
        ("a short grace period", BASE + ["--set", "terminationGracePeriodSeconds=20"], "terminationGracePeriodSeconds"),
    ):
        result = helm_template(args)
        if result.returncode == 0 or needle not in result.stderr:
            failures.append(f"chart rendered (or failed for another reason) {label}")


def main() -> int:
    failures: list[str] = []

    check_selectors(render(ENABLE_EVERYTHING), failures)
    check_defaults(failures)
    check_refusals(failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print("PASS: every Service selects exactly one Deployment (api, mcp); mcp is off by default; "
          "every cmd/api env var has a dedicated value and no *_MODE is rendered; "
          "the chart refuses to render without a database source, without brokers, "
          "with an empty consumer group, with a *_MODE injected through extraEnv or "
          "mcp.extraEnv, or with a too-short termination grace period")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
