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

{{/* Emits "true" when a signals Route renders: set, or auto on a cluster serving Routes where
     the signals Service is on and no other Service type was asked for. */}}
{{- define "grid-operator.signalsRoute.enabled" -}}
{{- $s := .Values.signals | default dict -}}
{{- $e := dig "route" "enabled" "auto" $s -}}
{{- $own := dig "service" "type" "" $s -}}
{{- if eq (toString $e) "true" -}}
true
{{- else if and (eq (toString $e) "auto") (.Capabilities.APIVersions.Has "route.openshift.io/v1") $s.enabled (dig "service" "enabled" false $s) (has $own (list "" "ClusterIP")) -}}
true
{{- end -}}
{{- end }}

{{/* The signals Service type: its own, else ClusterIP behind a Route, else swim.service.type. */}}
{{- define "grid-operator.signalsService.type" -}}
{{- $own := dig "service" "type" "" (.Values.signals | default dict) -}}
{{- if $own -}}
{{- $own -}}
{{- else if include "grid-operator.signalsRoute.enabled" . -}}
ClusterIP
{{- else -}}
{{- .Values.swim.service.type -}}
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
{{- if ne (include "grid-operator.signalsService.type" .) "ClusterIP" }}
{{- fail "signals.service.type beside signals.route must be ClusterIP: a NodePort or LoadBalancer publishes the listener a second time, past the router's limits; pick the Route or the Service" }}
{{- end }}
{{- end }}
{{- end }}
