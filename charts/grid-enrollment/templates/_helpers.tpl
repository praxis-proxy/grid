{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "grid-enrollment.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "grid-enrollment.fullname" -}}
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

{{- define "grid-enrollment.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "grid-enrollment.labels" -}}
helm.sh/chart: {{ include "grid-enrollment.chart" . }}
{{ include "grid-enrollment.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "grid-enrollment.selectorLabels" -}}
app.kubernetes.io/name: {{ include "grid-enrollment.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector labels for the enrollment workload: base selectors plus its component,
so it never overlaps the db pods (which carry component: db).
*/}}
{{- define "grid-enrollment.enrollmentSelectorLabels" -}}
{{ include "grid-enrollment.selectorLabels" . }}
app.kubernetes.io/component: enrollment
{{- end }}

{{/*
The listen port, parsed once from enrollment.listenAddr, so the container port,
Service port, and probes share one source.
*/}}
{{- define "grid-enrollment.listenPort" -}}
{{- (splitList ":" .Values.enrollment.listenAddr) | last -}}
{{- end }}

{{/*
The CA is provided (BYO) when a keySecretRef is set or method is "provided";
otherwise the bootstrap Job generates it.
*/}}
{{- define "grid-enrollment.caProvided" -}}
{{- or (eq .Values.ca.method "provided") (ne (default "" .Values.ca.provided.keySecretRef) "") -}}
{{- end }}

{{/*
Secret holding the CA signing key (tls.crt/tls.key). Provided ref wins.
*/}}
{{- define "grid-enrollment.caKeySecret" -}}
{{- if ne (default "" .Values.ca.provided.keySecretRef) "" -}}
{{- .Values.ca.provided.keySecretRef -}}
{{- else -}}
{{- .Values.ca.keySecretName -}}
{{- end }}
{{- end }}

{{/*
Secret holding the serving cert (tls.crt/tls.key). Provided ref wins.
*/}}
{{- define "grid-enrollment.servingSecret" -}}
{{- if ne (default "" .Values.serving.existingSecretRef) "" -}}
{{- .Values.serving.existingSecretRef -}}
{{- else -}}
{{- .Values.serving.secretName -}}
{{- end }}
{{- end }}

{{- define "grid-enrollment.serviceAccountName" -}}
{{- default (include "grid-enrollment.fullname" .) .Values.enrollment.serviceAccount.name }}
{{- end }}

{{/*
Bootstrap Job SA name.
*/}}
{{- define "grid-enrollment.bootstrapServiceAccountName" -}}
{{- printf "%s-ca-bootstrap" (include "grid-enrollment.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
In-cluster DNS name of the enrollment Service, used as the serving cert SAN.
*/}}
{{- define "grid-enrollment.serviceDns" -}}
{{- printf "%s.%s.svc" (include "grid-enrollment.fullname" .) .Release.Namespace }}
{{- end }}

{{/*
Builtin Postgres Service name.
*/}}
{{- define "grid-enrollment.dbHost" -}}
{{- printf "%s-db" (include "grid-enrollment.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret holding DB_CONNECTION_URL. External ref wins; builtin uses the generated one.
*/}}
{{- define "grid-enrollment.dbUrlSecret" -}}
{{- if eq .Values.db.type "external" -}}
{{- required "db.external.connectionUrlSecretRef is required when db.type=external" .Values.db.external.connectionUrlSecretRef -}}
{{- else if .Values.db.builtin.auth.existingSecretRef -}}
{{- .Values.db.builtin.auth.existingSecretRef -}}
{{- else -}}
{{- printf "%s-db" (include "grid-enrollment.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
{{- end }}

{{- define "grid-enrollment.dbUrlSecretKey" -}}
{{- if eq .Values.db.type "external" -}}
{{- default "DB_CONNECTION_URL" .Values.db.external.connectionUrlSecretKey -}}
{{- else -}}
DB_CONNECTION_URL
{{- end }}
{{- end }}
