{{/*
Expand the name of the chart.
*/}}
{{- define "exception-management.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "exception-management.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "exception-management.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "exception-management.labels" -}}
helm.sh/chart: {{ include "exception-management.chart" . }}
{{ include "exception-management.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels. The Deployment and the Service ALSO pin
app.kubernetes.io/component (api, mcp), so a Service never selects more than
the pods of one component (tests/test_service_selectors.py).
*/}}
{{- define "exception-management.selectorLabels" -}}
app.kubernetes.io/name: {{ include "exception-management.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "exception-management.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "exception-management.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL: the operator's own
(database.existingSecret) or the one this chart creates from database.url.
*/}}
{{- define "exception-management.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "exception-management.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Image reference.
*/}}
{{- define "exception-management.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Fully qualified name of the optional mcp component. It is its own workload
with its own app.kubernetes.io/component label; the api Service never
selects it (tests/test_service_selectors.py).
*/}}
{{- define "exception-management.mcpFullname" -}}
{{- include "exception-management.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the optional frontend component (the exceptions_mfe
Module Federation remote served by nginx). It is its own workload with its
own app.kubernetes.io/component label; the api Service never selects it
(tests/test_service_selectors.py).
*/}}
{{- define "exception-management.frontendFullname" -}}
{{- include "exception-management.fullname" . }}-frontend
{{- end }}

{{/*
Fully qualified names of the analytics components (ADR 0006): the projector
(the ONLY writer of the analytical database) and the read-only reports
binary. Each is its own workload with its own app.kubernetes.io/component
label; a Service never selects more than one of them
(tests/test_service_selectors.py).
*/}}
{{- define "exception-management.analyticsProjectorFullname" -}}
{{- include "exception-management.fullname" . }}-projector
{{- end }}

{{- define "exception-management.analyticsReportsFullname" -}}
{{- include "exception-management.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding ANALYTICS_DATABASE_URL: the operator's own
(analytics.existingSecret) or the one this chart creates from analytics.url
(analytics-secret.yaml).
*/}}
{{- define "exception-management.analyticsSecretName" -}}
{{- if .Values.analytics.existingSecret -}}
{{- .Values.analytics.existingSecret -}}
{{- else -}}
{{- include "exception-management.fullname" . }}-analytics
{{- end -}}
{{- end }}

{{/*
Analytics guard (ADR 0006): the projector and reports binaries exit at boot
without ANALYTICS_DATABASE_URL, and the projector without KAFKA_BROKERS
(always required by this chart) and ANALYTICS_CONSUMER_GROUP (no default).
Refuse them at render time, only while analytics is on, naming the value.
*/}}
{{- define "exception-management.requireAnalyticsConfig" -}}
{{- if not (or .Values.analytics.url .Values.analytics.existingSecret) -}}
{{- fail "analytics.enabled is true but neither analytics.url nor analytics.existingSecret is set: the projector and reports binaries exit at boot without ANALYTICS_DATABASE_URL (ADR 0006)." -}}
{{- end -}}
{{- $_ := required "analytics.enabled is true but analytics.consumerGroup is empty: the projector exits at boot without ANALYTICS_CONSUMER_GROUP, a stable Kafka group id (ADR 0006)." .Values.analytics.consumerGroup -}}
{{- end -}}

{{/*
MANDATORY: real Postgres is the only runtime. The binary exits at boot
without DATABASE_URL, so a render without a database source is never a
deployable state: surface it at render time, naming the values.
*/}}
{{- define "exception-management.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "exception-management requires database.url or database.existingSecret to be set (DATABASE_URL): the service runs on real Postgres only and exits at boot without it." -}}
{{- end -}}
{{- end -}}

{{/*
MANDATORY: real Kafka is the only runtime. The binary exits at boot
without KAFKA_BROKERS; there is no default broker.
*/}}
{{- define "exception-management.requireKafka" -}}
{{- $_ := required "exception-management requires kafka.brokers to be set (KAFKA_BROKERS, comma-separated): the service runs on real Kafka only and exits at boot without it." .Values.kafka.brokers -}}
{{- end -}}

{{/*
MANDATORY: the six fact consumers always run, each under its STABLE group id
read from the environment (cmd/api loadBootConfig exits at boot without one;
the names are pinned in .claude/rules/integration-events.md). The group ids
live in values (config.*ConsumerGroup), never in a template literal.
*/}}
{{- define "exception-management.requireConsumerGroups" -}}
{{- $_ := required "exception-management requires config.fulfillmentExceptionsConsumerGroup to be set (FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.fulfillmentExceptionsConsumerGroup -}}
{{- $_ := required "exception-management requires config.workPlanningExceptionsConsumerGroup to be set (WORK_PLANNING_EXCEPTIONS_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.workPlanningExceptionsConsumerGroup -}}
{{- $_ := required "exception-management requires config.networkFulfillmentExceptionsConsumerGroup to be set (NETWORK_FULFILLMENT_EXCEPTIONS_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.networkFulfillmentExceptionsConsumerGroup -}}
{{- $_ := required "exception-management requires config.orderCorrelationConsumerGroup to be set (ORDER_CORRELATION_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.orderCorrelationConsumerGroup -}}
{{- $_ := required "exception-management requires config.inventoryExceptionsConsumerGroup to be set (INVENTORY_EXCEPTIONS_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.inventoryExceptionsConsumerGroup -}}
{{- $_ := required "exception-management requires config.returnsExceptionsConsumerGroup to be set (RETURNS_EXCEPTIONS_CONSUMER_GROUP): a stable Kafka group id; the binary exits at boot without it." .Values.config.returnsExceptionsConsumerGroup -}}
{{- end -}}

{{/*
Shutdown is sequential in the worst case: SHUTDOWN_DRAIN_DELAY (default 5s) +
HTTP drain (10s) + outbox relay drain (10s) + inbound consumers drain (15s) +
consumer Close (5s) = 45s. A shorter grace period gets the pod SIGKILLed
mid-drain.
*/}}
{{- define "exception-management.requireGracePeriod" -}}
{{- if lt (int .Values.terminationGracePeriodSeconds) 45 -}}
{{- fail (printf "terminationGracePeriodSeconds is %v — want at least 45: SHUTDOWN_DRAIN_DELAY (5s default) + 10s HTTP drain + 10s outbox relay drain + 15s inbound consumer drain + 5s consumer Close." .Values.terminationGracePeriodSeconds) -}}
{{- end -}}
{{- end -}}

{{/*
This service has no modes at all (real Postgres and real Kafka are the only
runtime), so a *_MODE variable must never be injected through extraEnv — not
on the api, not on the mcp component and not on the analytics components.
*/}}
{{- define "exception-management.requireNoModeEnv" -}}
{{- range .Values.extraEnv -}}
{{- if hasSuffix "_MODE" (toString .name) -}}
{{- fail (printf "extraEnv sets %s — the *_MODE variables do not exist: this service has no modes, real Postgres and real Kafka are the only runtime." .name) -}}
{{- end -}}
{{- end -}}
{{- range .Values.mcp.extraEnv -}}
{{- if hasSuffix "_MODE" (toString .name) -}}
{{- fail (printf "mcp.extraEnv sets %s — the *_MODE variables do not exist: this service has no modes, real Postgres and real Kafka are the only runtime." .name) -}}
{{- end -}}
{{- end -}}
{{- range .Values.analytics.projector.extraEnv -}}
{{- if hasSuffix "_MODE" (toString .name) -}}
{{- fail (printf "analytics.projector.extraEnv sets %s — the *_MODE variables do not exist: this service has no modes, real Postgres and real Kafka are the only runtime." .name) -}}
{{- end -}}
{{- end -}}
{{- range .Values.analytics.reports.extraEnv -}}
{{- if hasSuffix "_MODE" (toString .name) -}}
{{- fail (printf "analytics.reports.extraEnv sets %s — the *_MODE variables do not exist: this service has no modes, real Postgres and real Kafka are the only runtime." .name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
