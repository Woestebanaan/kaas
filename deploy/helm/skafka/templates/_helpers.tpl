{{/*
Common template helpers.
*/}}

{{- define "skafka.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "skafka.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "skafka.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "skafka.headlessName" -}}
{{- printf "%s-headless" (include "skafka.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "skafka.pvcName" -}}
{{- printf "%s-data" (include "skafka.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "skafka.brokerSAName" -}}
{{- if .Values.serviceAccount.broker.create -}}
{{- default (printf "%s-broker" (include "skafka.fullname" .)) .Values.serviceAccount.broker.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.broker.name -}}
{{- end -}}
{{- end -}}

{{- define "skafka.operatorSAName" -}}
{{- if .Values.serviceAccount.operator.create -}}
{{- default (printf "%s-operator" (include "skafka.fullname" .)) .Values.serviceAccount.operator.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.operator.name -}}
{{- end -}}
{{- end -}}

{{- define "skafka.selectorLabels" -}}
app.kubernetes.io/name: {{ include "skafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end -}}

{{- define "skafka.operatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "skafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "skafka.labels" -}}
{{ include "skafka.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "skafka.operatorLabels" -}}
{{ include "skafka.operatorSelectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/*
skafka.brokerImage / skafka.operatorImage — Phase 9 flavor knob
(gh #152). An explicit .repository always wins (renders exactly as it
did before the knob existed). Otherwise the repository is derived from
image.flavor (go | rust — one knob switches broker AND operator) plus
a "-preview" suffix when the resolved tag is a pre-release (contains
"-"), mirroring the exact rule docker-publish.yml uses to compute
image names. Unknown flavors fail the render loudly.
*/}}
{{- define "skafka.brokerImage" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- $repo := .Values.image.repository -}}
{{- if not $repo -}}
  {{- if not (has .Values.image.flavor (list "go" "rust")) -}}
    {{- fail (printf "image.flavor must be \"go\" or \"rust\", got %q" .Values.image.flavor) -}}
  {{- end -}}
  {{- $rs := eq .Values.image.flavor "rust" | ternary "-rs" "" -}}
  {{- $pre := contains "-" $tag | ternary "-preview" "" -}}
  {{- $repo = printf "ghcr.io/woestebanaan/skafka%s%s" $rs $pre -}}
{{- end -}}
{{ $repo }}:{{ $tag }}
{{- end -}}

{{/*
skafka.listenersJSON — gh #126 helper. Iterates the user-facing
listeners array (Strimzi shape) and emits the SKAFKA_LISTENERS env
value as a JSON list-of-objects matching cmd/skafka/listeners.go's
listenerSpec.

Listener entries with no `enabled` key are treated as enabled
(always-on listeners). Entries with `enabled: false` are skipped.
This preserves the "internal is always on, external/authed are
opt-in" convention from earlier versions while letting custom
listeners freely toggle.

The broker's parser validates the result; constraint violations
(mtls without tls, duplicate ports/names) fail at startup with a
clear error. Output is single-line JSON — fits cleanly into an env
var without escape gymnastics.
*/}}
{{- define "skafka.listenersJSON" -}}
{{- $out := list -}}
{{- range .Values.listeners -}}
{{- if or (not (hasKey . "enabled")) .enabled -}}
{{- $auth := "none" -}}
{{- if .authentication -}}
{{- $auth = .authentication.type | default "none" -}}
{{- end -}}
{{- $tls := false -}}
{{- if hasKey . "tls" -}}
{{- $tls = .tls -}}
{{- end -}}
{{- $out = append $out (dict
  "name" .name
  "port" (.port | int)
  "type" .type
  "tls" $tls
  "authentication" (dict "type" $auth)) -}}
{{- end -}}
{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{/*
skafka.findListener — return the listener entry matching `name`, or
an empty dict if no such entry exists. Templates that need the
legacy "look up one listener by name" pattern (e.g. the
KafkaCluster CR emitter, the external-listener-only TLS/Gateway
plumbing) consult this instead of `.Values.listeners.<name>`.

Usage:
  {{- $ext := include "skafka.findListener" (dict "ctx" . "name" "external") | fromYaml -}}
  {{- if $ext.enabled }}
    port: {{ $ext.port }}
  {{- end }}

Returns YAML (parseable via `fromYaml`); empty `{}` when no match.
*/}}
{{- define "skafka.findListener" -}}
{{- $name := .name -}}
{{- $found := dict -}}
{{- range .ctx.Values.listeners -}}
{{- if eq .name $name -}}
{{- $found = . -}}
{{- end -}}
{{- end -}}
{{- $found | toYaml -}}
{{- end -}}

{{/*
skafka.firstByType — return the first listener entry with the given
type, or an empty dict. Used by the external-listener machinery
(cert-manager Certificate, per-broker Service, TLSRoute) which
today supports one external listener. The multi-external follow-up
will rewire these to iterate; this helper isolates the assumption.
*/}}
{{- define "skafka.firstByType" -}}
{{- $type := .type -}}
{{- $found := dict -}}
{{- range .ctx.Values.listeners -}}
{{- if and (eq .type $type) (or (not (hasKey . "enabled")) .enabled) -}}
{{- if not $found -}}
{{- $found = . -}}
{{- else if eq (len $found) 0 -}}
{{- $found = . -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $found | toYaml -}}
{{- end -}}

{{/*
skafka.primaryListenerName — name of the first enabled listener in
.Values.listeners. Used as the container-port name for the broker's
startup/liveness probes (which need *some* listener port to TCP-probe
but don't care which one). Fails the chart render if no listener is
enabled — that would mean no Kafka traffic, which is never the intent.
*/}}
{{- define "skafka.primaryListenerName" -}}
{{- $name := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq $name "") (or (not (hasKey . "enabled")) .enabled) -}}
{{- $name = .name -}}
{{- end -}}
{{- end -}}
{{- if eq $name "" -}}
{{- fail "skafka chart: at least one entry in .Values.listeners must be enabled" -}}
{{- end -}}
{{- $name -}}
{{- end -}}

{{/*
skafka.hasInternalTLSListener — true (returns "1") if any enabled
listener has type: internal + tls: true. Distinct from the external
TLS path, which is reconciled by the operator from the KafkaCluster
CR — the internal-TLS case is chart-managed end-to-end via a
selfSigned cert-manager Issuer + Certificate (gh #131).
*/}}
{{- define "skafka.hasInternalTLSListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq .type "internal") .tls (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
skafka.hasAnyTLSListener — true if any enabled listener has tls: true,
regardless of type. Drives the volume mount + SKAFKA_TLS_CERT_FILE env
in broker-statefulset.yaml — internal-TLS and external-TLS share the
same broker-side cert loader (one *tls.Config across all listeners,
per cmd/skafka/listeners.go).
*/}}
{{- define "skafka.hasAnyTLSListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and .tls (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
skafka.internalTLSSecretName — Secret name for the chart-managed
internal-TLS cert. Cert-manager populates it with tls.crt + tls.key
(no separate ca.crt; the self-signed leaf cert IS the trust anchor).
*/}}
{{- define "skafka.internalTLSSecretName" -}}
{{- printf "%s-broker-internal-tls" (include "skafka.fullname" .) -}}
{{- end -}}

{{/*
skafka.hasEnabledExternalListener — true if any listener has type:
external + (enabled missing or true). Convenience predicate so
templates don't have to range-fold themselves.
*/}}
{{- define "skafka.hasEnabledExternalListener" -}}
{{- $hit := "" -}}
{{- range .Values.listeners -}}
{{- if and (eq .type "external") (or (not (hasKey . "enabled")) .enabled) -}}
{{- $hit = "1" -}}
{{- end -}}
{{- end -}}
{{- $hit -}}
{{- end -}}

{{/*
skafka.superUsersList — emit the cluster-wide superUsers as a
comma-separated string for SKAFKA_SUPER_USERS. Empty when the list
is empty (broker treats unset env as "no superUsers").
*/}}
{{- define "skafka.superUsersList" -}}
{{- if .Values.authorization -}}
{{- join "," (.Values.authorization.superUsers | default list) -}}
{{- end -}}
{{- end -}}

{{- define "skafka.operatorImage" -}}
{{- $tag := .Values.operator.image.tag | default .Chart.AppVersion -}}
{{- $repo := .Values.operator.image.repository -}}
{{- if not $repo -}}
  {{- if not (has .Values.image.flavor (list "go" "rust")) -}}
    {{- fail (printf "image.flavor must be \"go\" or \"rust\", got %q" .Values.image.flavor) -}}
  {{- end -}}
  {{- $rs := eq .Values.image.flavor "rust" | ternary "-rs" "" -}}
  {{- $pre := contains "-" $tag | ternary "-preview" "" -}}
  {{- $repo = printf "ghcr.io/woestebanaan/skafka-operator%s%s" $rs $pre -}}
{{- end -}}
{{ $repo }}:{{ $tag }}
{{- end -}}
