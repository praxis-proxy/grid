{{/* Emits "true" when a signals Route renders: set, or auto on a cluster serving Routes. */}}
{{- define "grid-operator.signalsRoute.enabled" -}}
{{- $e := dig "route" "enabled" "auto" (.Values.signals | default dict) -}}
{{- if or (eq (toString $e) "true") (and (eq (toString $e) "auto") (.Capabilities.APIVersions.Has "route.openshift.io/v1")) -}}
true
{{- end -}}
{{- end }}

{{/* Refuse a signals Route that would serve nothing, reach nothing, or be unreachable. */}}
{{- define "grid-operator.signalsRoute.validate" -}}
{{- $r := dig "route" dict (.Values.signals | default dict) }}
{{- if eq (toString ($r.enabled | default "auto")) "true" }}
{{- if not (.Capabilities.APIVersions.Has "route.openshift.io/v1") }}
{{- fail "signals.route.enabled=true needs the route.openshift.io/v1 API: leave it auto off OpenShift" }}
{{- end }}
{{- end }}
{{- if include "grid-operator.signalsRoute.enabled" . }}
{{- if not (.Values.signals | default dict).enabled }}
{{- fail "signals.route needs signals.enabled: a Route to a site that serves no signals answers nothing" }}
{{- end }}
{{- if not (dig "service" "enabled" false (.Values.signals | default dict)) }}
{{- fail "signals.route needs signals.service.enabled: the Route sends traffic to that Service" }}
{{- end }}
{{- if not (dig "route" "host" "" (.Values.signals | default dict)) }}
{{- fail "signals.route.host is required: peers dial the site's own name and the router picks this backend by it" }}
{{- end }}
{{- end }}
{{- end }}
