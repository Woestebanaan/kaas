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

{{- define "skafka.brokerImage" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/*
skafka.listenersJSON — gh #124 helper. Emits the SKAFKA_LISTENERS env
value as JSON. Walks the existing listeners.{internal,external,authed}
map and builds a list-of-objects matching cmd/skafka/listeners.go's
listenerSpec. Only listener entries that are actually enabled appear
in the output (internal is always emitted; external/authed are gated
on their `enabled` flag). The broker's parser validates the result;
constraint violations (mtls without tls, duplicate ports/names) fail
at startup with a clear error.

Output is single-line JSON — fits cleanly into an env var without
escape gymnastics.
*/}}
{{- define "skafka.listenersJSON" -}}
{{- $list := list -}}

{{/* internal listener — always enabled */}}
{{- $internalAuth := "none" -}}
{{- if .Values.listeners.internal.authentication -}}
{{- $internalAuth = .Values.listeners.internal.authentication.type | default "none" -}}
{{- end -}}
{{- $list = append $list (dict "name" "internal" "port" (.Values.listeners.internal.port | int) "type" "internal" "tls" false "authentication" (dict "type" $internalAuth)) -}}

{{/* external listener — opt-in */}}
{{- if .Values.listeners.external.enabled -}}
{{- $extAuth := "none" -}}
{{- if .Values.listeners.external.authentication -}}
{{- $extAuth = .Values.listeners.external.authentication.type | default "none" -}}
{{- end -}}
{{- $list = append $list (dict "name" "external" "port" (.Values.listeners.external.port | int) "type" "external" "tls" true "authentication" (dict "type" $extAuth)) -}}
{{- end -}}

{{/* authed listener (gh #139) — opt-in plaintext + SASL-required */}}
{{- if .Values.listeners.authed.enabled -}}
{{- $authedAuth := "scram-sha-512" -}}
{{- if .Values.listeners.authed.authentication -}}
{{- $authedAuth = .Values.listeners.authed.authentication.type | default "scram-sha-512" -}}
{{- end -}}
{{- $list = append $list (dict "name" "authed" "port" (.Values.listeners.authed.port | int) "type" "internal" "tls" false "authentication" (dict "type" $authedAuth)) -}}
{{- end -}}

{{- $list | toJson -}}
{{- end -}}

{{- define "skafka.operatorImage" -}}
{{ .Values.operator.image.repository }}:{{ .Values.operator.image.tag | default .Chart.AppVersion }}
{{- end -}}
