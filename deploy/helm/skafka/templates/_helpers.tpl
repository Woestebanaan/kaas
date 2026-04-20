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

{{- define "skafka.operatorImage" -}}
{{ .Values.operator.image.repository }}:{{ .Values.operator.image.tag | default .Chart.AppVersion }}
{{- end -}}
