{{/*
Chart name.
*/}}
{{- define "fleet-dashboard.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name. If the release name already contains the chart name, use it as-is.
*/}}
{{- define "fleet-dashboard.fullname" -}}
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

{{- define "fleet-dashboard.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "fleet-dashboard.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "fleet-dashboard.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fleet-dashboard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "fleet-dashboard.serviceAccountName" -}}
{{- default (include "fleet-dashboard.fullname" .) .Values.serviceAccount.name }}
{{- end }}

{{/*
Name of the TLS Secret the OpenShift service CA populates for the oauth-proxy.
*/}}
{{- define "fleet-dashboard.tlsSecretName" -}}
{{ include "fleet-dashboard.fullname" . }}-tls
{{- end }}

{{/*
Name of the oauth-proxy session cookie Secret.
*/}}
{{- define "fleet-dashboard.cookieSecretName" -}}
{{ include "fleet-dashboard.fullname" . }}-cookie
{{- end }}
