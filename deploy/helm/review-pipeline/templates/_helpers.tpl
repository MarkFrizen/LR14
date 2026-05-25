{{/*
Разворачиваем имя чарта.
*/}}
{{- define "review-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Полное имя приложения.
*/}}
{{- define "review-pipeline.fullname" -}}
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
Общие метки.
*/}}
{{- define "review-pipeline.labels" -}}
helm.sh/chart: {{ include "review-pipeline.name" . }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Селекторы для компонентов.
*/}}
{{- define "review-pipeline.collector.matchLabels" -}}
app.kubernetes.io/component: collector
{{- end }}

{{- define "review-pipeline.analyzer.matchLabels" -}}
app.kubernetes.io/component: analyzer
{{- end }}

{{- define "review-pipeline.streamlit.matchLabels" -}}
app.kubernetes.io/component: streamlit
{{- end }}

{{/*
Имена сервисов инфраструктуры.
*/}}
{{- define "review-pipeline.etcd.endpoint" -}}
{{- if .Values.etcd.enabled -}}
{{ .Release.Name }}-etcd:2379
{{- else -}}
{{ .Values.etcd.externalEndpoints | first }}
{{- end -}}
{{- end }}

{{- define "review-pipeline.nats.url" -}}
{{- if .Values.nats.enabled -}}
nats://{{ .Release.Name }}-nats:4222
{{- else -}}
{{ .Values.nats.externalURL }}
{{- end -}}
{{- end }}
