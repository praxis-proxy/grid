{{/* The hostname peers dial, defaulting to this site's name under the grid trust domain,
     which is the convention a peer's serverName follows. */}}
{{- define "grid-operator.signalsRoute.host" -}}
{{- $r := dig "route" dict (.Values.signals | default dict) -}}
{{- with $r.host -}}
{{- . -}}
{{- else -}}
{{- with .Values.swim.siteName -}}
{{- printf "%s-signals.grid.internal" . -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/* Handshakes one source may hold. Behind a Route every peer arrives as the router, so the
     default rises to the listener's whole share rather than one peer's. */}}
{{- define "grid-operator.signalsRoute.handshakes" -}}
{{- dig "maxHandshakesPerSource" "" (.Values.signals | default dict) | default 32 -}}
{{- end }}

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
{{- if not (include "grid-operator.signalsRoute.host" .) }}
{{- fail "signals.route.host needs site.name to default, or set it: peers dial the site's own name and the router picks this backend by it" }}
{{- end }}
{{- $type := dig "service" "type" "" (.Values.signals | default dict) }}
{{- if eq $type "LoadBalancer" }}
{{- fail "signals.service.type LoadBalancer beside signals.route publishes the listener twice, on two addresses with different limits: pick the Route or the load balancer" }}
{{- end }}
{{- end }}
{{- end }}
