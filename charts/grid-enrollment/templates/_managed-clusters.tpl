{{- define "grid-enrollment.membership.name" -}}
{{- printf "%s-membership" (include "grid-enrollment.fullname" . | trunc 48 | trimSuffix "-") -}}
{{- end -}}

{{- define "grid-enrollment.membership.clusterRoleName" -}}
{{- $name := printf "%s-%s" .Release.Namespace (include "grid-enrollment.membership.name" .) -}}
{{- printf "%s-%s" ($name | trunc 54 | trimSuffix "-") ($name | sha256sum | trunc 8) -}}
{{- end -}}
