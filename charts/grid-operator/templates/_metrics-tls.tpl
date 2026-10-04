{{/*
The metrics TLS source: serviceCA, siteIdentity, existingSecret, or empty for
plaintext. Every grid site has a source, so only an install with no grid
identity, or metrics.tls.enabled=false, serves plaintext.
*/}}
{{- define "grid-operator.metricsTls.source" -}}
{{- $tls := (.Values.metrics).tls | default dict -}}
{{- $openshift := .Capabilities.APIVersions.Has "security.openshift.io/v1" -}}
{{- if ne (toString $tls.enabled) "false" -}}
{{- $source := $tls.source | default "auto" -}}
{{- if eq $source "auto" -}}
{{- if $tls.existingSecret -}}
existingSecret
{{- else if $openshift -}}
serviceCA
{{- else if or (.Values.grid).id (.Values.enrollment).enabled -}}
siteIdentity
{{- end -}}
{{- else if and (eq $source "serviceCA") (not $openshift) -}}
{{- fail "metrics.tls.source serviceCA needs OpenShift, where the service CA issues the certificate" -}}
{{- else if and (eq $source "existingSecret") (not $tls.existingSecret) -}}
{{- fail "metrics.tls.source existingSecret needs metrics.tls.existingSecret" -}}
{{- else -}}
{{- $source -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Emits "true" when the metrics port serves TLS. */}}
{{- define "grid-operator.metricsTls.enabled" -}}
{{- if include "grid-operator.metricsTls.source" . -}}
true
{{- end -}}
{{- end }}

{{/* Emits "true" when the OpenShift service CA issues the metrics certificate. */}}
{{- define "grid-operator.metricsTls.serviceCa" -}}
{{- if eq (include "grid-operator.metricsTls.source" .) "serviceCA" -}}
true
{{- end -}}
{{- end }}

{{/* The mounted Secret for the file-based sources. */}}
{{- define "grid-operator.metricsTls.secret" -}}
{{- .Values.metrics.tls.existingSecret | default (printf "%s-metrics-tls" (include "grid-operator.fullname" .)) -}}
{{- end }}

{{/* The Secret the site identity lives in, the one the GridNetwork names. */}}
{{- define "grid-operator.metricsTls.identitySecret" -}}
{{- (.Values.enrollment).identitySecretName | default "grid-site-identity" -}}
{{- end }}

{{- define "grid-operator.metricsTls.validate" -}}
{{- if and (include "grid-operator.metricsTls.serviceCa" .) (not .Values.metrics.service.enabled) }}
{{- fail "metrics.tls from the OpenShift service CA needs metrics.service.enabled, whose annotation requests the certificate" }}
{{- end }}
{{- end }}

{{- define "grid-operator.metricsTls.env" -}}
{{- $source := include "grid-operator.metricsTls.source" . }}
{{- if eq $source "siteIdentity" }}
- name: GRID_METRICS_TLS_SITE_SECRET
  value: {{ include "grid-operator.metricsTls.identitySecret" . | quote }}
{{- else if $source }}
- name: GRID_METRICS_TLS_CERT
  value: {{ printf "%s/tls.crt" .Values.metrics.tls.mountPath | quote }}
- name: GRID_METRICS_TLS_KEY
  value: {{ printf "%s/tls.key" .Values.metrics.tls.mountPath | quote }}
{{- end }}
{{- end }}

{{/* File-based sources mount the Secret; the site identity is read through the API once enrollment writes it. */}}
{{- define "grid-operator.metricsTls.mounted" -}}
{{- $source := include "grid-operator.metricsTls.source" . }}
{{- if and $source (ne $source "siteIdentity") -}}
true
{{- end -}}
{{- end }}

{{- define "grid-operator.metricsTls.volumeMount" -}}
{{- if include "grid-operator.metricsTls.mounted" . }}
- name: metrics-tls
  mountPath: {{ .Values.metrics.tls.mountPath | quote }}
  readOnly: true
{{- end }}
{{- end }}

{{- define "grid-operator.metricsTls.volume" -}}
{{- if include "grid-operator.metricsTls.mounted" . }}
- name: metrics-tls
  secret:
    secretName: {{ include "grid-operator.metricsTls.secret" . | quote }}
    defaultMode: 0440
{{- end }}
{{- end }}

{{/* Emits "true" when the NetworkPolicy renders. auto: on where OpenShift runs. */}}
{{- define "grid-operator.networkPolicy.enabled" -}}
{{- $mode := toString (.Values.networkPolicy).enabled | default "auto" -}}
{{- if or (eq $mode "true") (and (eq $mode "auto") (.Capabilities.APIVersions.Has "security.openshift.io/v1")) -}}
true
{{- end -}}
{{- end }}
