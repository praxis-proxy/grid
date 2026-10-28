{{- define "grid-bootstrap.labels" -}}
app.kubernetes.io/name: grid-bootstrap
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end }}

{{- define "grid-bootstrap.retain" -}}
helm.sh/resource-policy: keep
argocd.argoproj.io/sync-options: Prune=false,Delete=false
{{- end }}

{{- define "grid-bootstrap.validate" -}}
{{- if and .Values.enabled (eq .Values.ca.mode "Generate") -}}
{{- if ge (int .Values.ca.renewBeforeHours) (int .Values.ca.durationHours) -}}
{{- fail "ca.renewBeforeHours must be less than ca.durationHours" -}}
{{- end -}}
{{- if eq .Values.issuer.name .Values.ca.bootstrapIssuerName -}}
{{- fail "issuer.name and ca.bootstrapIssuerName must differ" -}}
{{- end -}}
{{- end -}}
{{- end }}
