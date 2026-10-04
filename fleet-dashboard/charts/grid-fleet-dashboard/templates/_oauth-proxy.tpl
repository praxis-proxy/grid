{{/*
The oauth-proxy sidecar. Authenticates against the OpenShift OAuth server and
authorizes with a SubjectAccessReview for "get configmaps" in the release
namespace, so anyone who can read the registry ConfigMap can open the map.
*/}}
{{- define "fleet-dashboard.oauthProxyContainer" -}}
- name: oauth-proxy
  image: {{ .Values.auth.oauthProxy.image }}
  imagePullPolicy: IfNotPresent
  args:
    - --provider=openshift
    - --https-address=:8443
    - --http-address=
    - --upstream=http://localhost:8080
    - --openshift-service-account={{ include "fleet-dashboard.serviceAccountName" . }}
    - '--openshift-sar={"namespace":"{{ .Release.Namespace }}","resource":"configmaps","verb":"get"}'
    - --cookie-secret-file=/etc/proxy/secrets/session_secret
    - --tls-cert=/etc/tls/private/tls.crt
    - --tls-key=/etc/tls/private/tls.key
  ports:
    - name: https
      containerPort: 8443
      protocol: TCP
  readinessProbe:
    tcpSocket:
      port: https
    initialDelaySeconds: 5
    periodSeconds: 10
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    capabilities:
      drop: ["ALL"]
  resources:
    {{- toYaml .Values.auth.oauthProxy.resources | nindent 4 }}
  volumeMounts:
    - name: proxy-tls
      mountPath: /etc/tls/private
      readOnly: true
    - name: proxy-cookie
      mountPath: /etc/proxy/secrets
      readOnly: true
{{- end }}
