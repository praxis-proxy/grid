{{/* Enrollment values with defaults: the grid-ca-bundle Secret and grid-invite-<siteName>. */}}
{{- define "grid-operator.enrollment.settings" -}}
{{- $e := deepCopy (.Values.enrollment | default dict) }}
{{- $b := $e.caBundle | default dict }}
{{- if not $b.secret }}
{{- $_ := set $e "caBundle" (merge (dict "secret" "grid-ca-bundle") $b) }}
{{- end }}
{{- $t := $e.tokenSecretRef | default dict }}
{{- if and (not $t.name) $e.siteName }}
{{- $_ := set $e "tokenSecretRef" (merge (dict "name" (printf "grid-invite-%s" $e.siteName)) $t) }}
{{- end }}
{{- toYaml $e }}
{{- end }}

{{- define "grid-operator.enrollment.validate" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- if and $e.enabled (not .Values.grid.id) (or (ne ($e.identitySecretName | default "grid-site-identity") "grid-site-identity") (ne ($e.caSecretName | default "grid-ca") "grid-ca")) }}
{{- fail "enrollment.identitySecretName and caSecretName name the Secrets in the GridNetwork this chart renders: set grid.id, or keep grid-site-identity and grid-ca" }}
{{- end }}
{{- if and $e.rotation (not (or $e.enabled $e.url)) }}
{{- fail "enrollment.rotation sits under signerEndpoint: set enrollment.url, or enrollment.enabled" }}
{{- end }}
{{- if and $e.url (not (hasPrefix "https://" $e.url)) }}
{{- fail "enrollment.url must be an https URL" }}
{{- end }}
{{- if and $e.enabled (not $e.siteName) }}
{{- fail "enrollment.siteName is required when enrollment.enabled is true" }}
{{- end }}
{{- if and $e.url (not (or $e.siteName .Values.swim.siteName)) }}
{{- fail "enrollment.url renders the operator config's site section, which needs a site name: set swim.siteName or enrollment.siteName" }}
{{- end }}
{{- if $e.enabled }}
{{- if not (hasPrefix "https://" ($e.url | default "")) }}
{{- fail "enrollment.url must be an https URL when enrollment.enabled is true" }}
{{- end }}
{{- if not (dig "tokenSecretRef" "name" "" $e) }}
{{- fail "enrollment.tokenSecretRef.name is required when enrollment.enabled is true" }}
{{- end }}
{{- if not (dig "caBundle" "secret" "" $e) }}
{{- fail "enrollment.caBundle.secret is required when enrollment.enabled is true" }}
{{- end }}
{{- end }}
{{- end }}

{{/* A BundleRef for a caBundle-shaped value: a Secret, the only kind a bundle may be. */}}
{{- define "grid-operator.enrollment.bundleRef" -}}
kind: Secret
name: {{ dig "secret" "" . | quote }}
key: {{ dig "key" "ca.crt" . | quote }}
{{- end }}

{{/* The signer hosts the operator config may name, comma-separated: enrollment.signerHosts, else the host and port of the rendered URL. Empty when neither is set. */}}
{{- define "grid-operator.enrollment.signerHosts" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- if $e.signerHosts }}
{{- join "," $e.signerHosts }}
{{- else if $e.url }}
{{- (urlParse $e.url).host }}
{{- end }}
{{- end }}

{{/* The operator config's site section: the site name, signerEndpoint with its rotation, and inviteSecretRef. A hub whose identity bootstrap issued sets only the URL, and renews pinned to the grid CA it holds. Unset, rotation renders Disabled unless grid.peerTrust is spiffe, since pin peer trust refuses a renewed leaf. */}}
{{- define "grid-operator.enrollment.site" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- $raw := .Values.enrollment | default dict }}
{{- $pinned := or $e.enabled (dig "caBundle" "secret" "" $raw) }}
{{- $rotation := $e.rotation }}
{{- if and (not $rotation) (ne (.Values.grid.peerTrust | default "") "spiffe") }}
{{- $rotation = "Disabled" }}
{{- end }}
{{- if or $e.enabled $e.url }}
site:
  name: {{ $e.siteName | default .Values.swim.siteName | quote }}
  signerEndpoint:
    url: {{ $e.url | quote }}
    {{- if $pinned }}
    caBundleRef:
      {{- include "grid-operator.enrollment.bundleRef" $e.caBundle | trim | nindent 6 }}
    {{- end }}
    {{- if include "grid-operator.enrollment.hasGridCa" . }}
    gridCaBundleRef:
      {{- include "grid-operator.enrollment.bundleRef" $e.gridCaBundle | trim | nindent 6 }}
    {{- end }}
    {{- with $rotation }}
    rotation: {{ . | quote }}
    {{- end }}
  {{- if $e.enabled }}
  inviteSecretRef:
    name: {{ dig "tokenSecretRef" "name" "" $e | quote }}
    key: {{ dig "tokenSecretRef" "key" "token" $e | quote }}
  {{- end }}
{{- end }}
{{- end }}

{{/* The operator config ConfigMap's name. */}}
{{- define "grid-operator.operatorConfig.name" -}}
{{- (.Values.operatorConfig | default dict).name | default "grid-operator-config" }}
{{- end }}

{{/* Emits "true" or nothing. */}}
{{- define "grid-operator.enrollment.hasGridCa" -}}
{{- $e := include "grid-operator.enrollment.settings" . | fromYaml }}
{{- if dig "gridCaBundle" "secret" "" $e -}}
true
{{- end -}}
{{- end }}
