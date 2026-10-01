#!/usr/bin/env bash
# Starts the praxis-gateway chart's rendered config in real images.
#
#   DEFAULT_GATEWAY_IMAGE  auth.mode none: --validate, then run it behind an echo backend.
#                          Authorization is stripped by default and forwarded with
#                          stripAuthorization=false (so the absence check is not vacuous).
#   API_KEY_IMAGE          auth.mode api-key behind an https validate stub on a private CA
#                          trusted through auth.validateCA (skipped when unset): no key and
#                          a bad key get 401, a good key gets 200 without Authorization
#                          upstream, and without validateCA the good key gets 401.
#
# API_KEY_IMAGE_CONFIG_FLAG is the flag before the config path ("" for a positional path).
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
GW_DIR="$ROOT/charts/praxis-gateway"
# The chart's own default image, so the check tracks the chart.
chart_image() {
  helm show values "$GW_DIR" | awk '/^image:/{f=1; next} f&&/^[^ ]/{exit} f&&/repository:/{r=$2} f&&/tag:/{t=$2} END{gsub(/"/,"",r); gsub(/"/,"",t); print r":"t}'
}
DEFAULT_GATEWAY_IMAGE=${DEFAULT_GATEWAY_IMAGE:-$(chart_image)}
API_KEY_IMAGE=${API_KEY_IMAGE:-}
API_KEY_IMAGE_CONFIG_FLAG=${API_KEY_IMAGE_CONFIG_FLAG---config}
FIXTURE_IMAGE=${FIXTURE_IMAGE:-docker.io/library/python:3.12-alpine}
CRT=${CONTAINER_RUNTIME:-$(command -v docker >/dev/null && echo docker || echo podman)}

PASS=0 FAIL=0
pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1" >&2; }
check() { if [ "$2" = "$3" ]; then pass "$1"; else fail "$1: want $2, got $3"; fi; }

WORK=$(mktemp -d)
NET=gw-verify-$$
cleanup() {
  "$CRT" ps -aq --filter "name=^$NET-" | xargs -r "$CRT" rm -f -t 0 >/dev/null 2>&1 || true
  "$CRT" network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# yaml_get <file> <yq expr> <python expr over d>: print a value, fail if absent.
yaml_get() {
  if command -v yq >/dev/null; then
    yq -e "$2" "$1" 2>/dev/null
  else
    python3 -c 'import sys, yaml; d = yaml.safe_load(open(sys.argv[1])); v = eval(sys.argv[2]); print(v) if v is not None else sys.exit(1)' \
      "$1" "$3" 2>/dev/null
  fi
}

# render <dir> <helm args...>: write praxis.yaml, policy.yaml, and SSL_CERT_FILE if set.
render() {
  local dir=$1; shift
  mkdir -p "$dir"
  helm template verify "$GW_DIR" --namespace grid-system "$@" --show-only templates/gateway-config.yaml > "$dir/cm.yaml"
  helm template verify "$GW_DIR" --namespace grid-system "$@" --show-only templates/deployment.yaml > "$dir/deploy.yaml"
  yaml_get "$dir/cm.yaml" '.data["praxis.yaml"]' 'd["data"]["praxis.yaml"]' > "$dir/praxis.yaml"
  yaml_get "$dir/cm.yaml" '.data["policy.yaml"]' 'd["data"].get("policy.yaml")' > "$dir/policy.yaml" \
    || rm -f "$dir/policy.yaml"
  yaml_get "$dir/deploy.yaml" \
    '.spec.template.spec.containers[] | select(.name == "praxis") | .env[] | select(.name == "SSL_CERT_FILE") | .value' \
    'next((e["value"] for c in d["spec"]["template"]["spec"]["containers"] if c["name"] == "praxis" for e in c.get("env", []) if e["name"] == "SSL_CERT_FILE"), None)' \
    > "$dir/ssl_cert_file" || rm -f "$dir/ssl_cert_file"
}

ip() { "$CRT" inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }

# gateway <name> <image> <flag> <dir> [ca dir]: run the rendered config, print its host port.
# The ca dir is mounted where the chart points SSL_CERT_FILE, as the Deployment would.
gateway() {
  local name=$1 image=$2 flag=$3 dir=$4 ca=${5:-} args=()
  if [ -n "$ca" ] && [ -f "$dir/ssl_cert_file" ]; then
    args=(-v "$ca:$(dirname "$(cat "$dir/ssl_cert_file")"):ro,z" -e "SSL_CERT_FILE=$(cat "$dir/ssl_cert_file")")
  fi
  # shellcheck disable=SC2086 # empty flag means a positional config path
  "$CRT" run -d --name "$NET-$name" --network "$NET" -p 127.0.0.1::8080 -v "$dir:/etc/praxis:ro,z" "${args[@]}" \
    "$image" $flag /etc/praxis/praxis.yaml >/dev/null
  "$CRT" port "$NET-$name" 8080 | head -1 | sed 's/.*://'
}

# chat <port> [curl args]: POST a chat request, print the status; the body lands in $WORK/out.
chat() {
  local port=$1; shift
  curl -s -o "$WORK/out" -w '%{http_code}' -m 10 -X POST "http://127.0.0.1:$port/v1/chat/completions" \
    -H 'Content-Type: application/json' "$@" --data '{"model":"qwen3","messages":[{"role":"user","content":"hi"}]}' || true
}

# wait_up <port> [curl args]: poll until the gateway answers with something other than 000/502/503.
wait_up() {
  local port=$1 code; shift
  for _ in $(seq 1 30); do
    code=$(chat "$port" "$@")
    case $code in 000 | 502 | 503) sleep 1 ;; *) return 0 ;; esac
  done
  return 1
}

seen_auth() { grep -o '"authorization": [^}]*' "$WORK/out" || true; }

echo "=== Rendered config starts (gateway) ==="
"$CRT" network create "$NET" >/dev/null
"$CRT" run -d --name "$NET-backend" --network "$NET" "$FIXTURE_IMAGE" python3 -c '
import json, http.server
class H(http.server.BaseHTTPRequestHandler):
    def reply(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self): self.reply({"ok": True})
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.reply({"authorization": self.headers.get("Authorization")})
http.server.HTTPServer(("0.0.0.0", 8000), H).serve_forever()' >/dev/null
BASE=(
  --set gatewayConfig.render=true --set gatewayConfig.model=qwen3
  --set "gatewayConfig.backends[0].cluster=site-a"
  --set "gatewayConfig.backends[0].transport.mode=plaintext"
  --set "gatewayConfig.backends[0].endpoints[0]=$(ip "$NET-backend"):8000"
)

render "$WORK/none" "${BASE[@]}" --set gatewayConfig.auth.mode=none
if "$CRT" run --rm -v "$WORK/none:/etc/praxis:ro,z" "$DEFAULT_GATEWAY_IMAGE" \
    --config /etc/praxis/praxis.yaml --validate >"$WORK/none.log" 2>&1; then
  pass "none: validates on $DEFAULT_GATEWAY_IMAGE"
else
  fail "none: rejected by $DEFAULT_GATEWAY_IMAGE: $(tail -1 "$WORK/none.log")"
fi
port=$(gateway none "$DEFAULT_GATEWAY_IMAGE" --config "$WORK/none")
wait_up "$port" || fail "none: gateway never answered"
check "none: request reaches the backend" 200 "$(chat "$port" -H 'Authorization: Bearer caller')"
check "none: backend never sees Authorization" '"authorization": null' "$(seen_auth)"

render "$WORK/keep" "${BASE[@]}" --set gatewayConfig.auth.mode=none --set gatewayConfig.auth.stripAuthorization=false
port=$(gateway keep "$DEFAULT_GATEWAY_IMAGE" --config "$WORK/keep")
wait_up "$port" || fail "keep: gateway never answered"
chat "$port" -H 'Authorization: Bearer caller' >/dev/null
check "none, stripAuthorization=false: backend sees Authorization (the echo is live)" \
  '"authorization": "Bearer caller"' "$(seen_auth)"

# tls backend: the server cert chains to a private CA and names the Service host, like a
# KServe workload behind the OpenShift service CA. The endpoint is the literal IP (a
# hostname resolving to a private address is refused), sni names the cert, CA via transport.ca.
bca=$WORK/backend-ca && mkdir -p "$bca"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 -subj /CN=backend-ca \
  -keyout "$bca/ca.key" -out "$bca/ca.crt" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=tls-backend \
  -keyout "$bca/tls.key" -out "$bca/tls.csr" 2>/dev/null
printf 'subjectAltName=DNS:tls-backend\nextendedKeyUsage=serverAuth\n' > "$bca/ext"
openssl x509 -req -in "$bca/tls.csr" -CA "$bca/ca.crt" -CAkey "$bca/ca.key" -CAcreateserial -days 1 \
  -extfile "$bca/ext" -out "$bca/tls.crt" 2>/dev/null
cp "$bca/ca.crt" "$bca/service-ca.crt"
chmod 644 "$bca"/*
"$CRT" run -d --name "$NET-tls-backend" --network "$NET" --network-alias tls-backend -v "$bca:/ca:ro,z" \
  "$FIXTURE_IMAGE" python3 -c '
import http.server, ssl
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200); self.send_header("Content-Length", "2"); self.end_headers(); self.wfile.write(b"{}")
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain("/ca/tls.crt", "/ca/tls.key")
server = http.server.HTTPServer(("0.0.0.0", 8443), H)
server.socket = ctx.wrap_socket(server.socket, server_side=True)
server.serve_forever()' >/dev/null
TLS_BACKEND=(--set gatewayConfig.render=true --set gatewayConfig.model=qwen3 --set gatewayConfig.auth.mode=none
  --set "gatewayConfig.backends[0].cluster=kserve" --set "gatewayConfig.backends[0].endpoints[0]=$(ip "$NET-tls-backend"):8443"
  --set "gatewayConfig.backends[0].transport.mode=tls" --set "gatewayConfig.backends[0].transport.sni=tls-backend")
render "$WORK/tls" "${TLS_BACKEND[@]}" \
  --set "gatewayConfig.backends[0].transport.ca.configMap=service-ca" --set "gatewayConfig.backends[0].transport.ca.key=service-ca.crt"
# Docker cannot create a mountpoint inside the read-only /etc/praxis mount.
mkdir -p "$WORK/tls/backend-ca/0"
if "$CRT" run --rm -v "$WORK/tls:/etc/praxis:ro,z" -v "$bca:/etc/praxis/backend-ca/0:ro,z" "$DEFAULT_GATEWAY_IMAGE" \
    --config /etc/praxis/praxis.yaml --validate >"$WORK/tls.log" 2>&1; then
  pass "tls backend: validates on $DEFAULT_GATEWAY_IMAGE"
else
  fail "tls backend: rejected by $DEFAULT_GATEWAY_IMAGE: $(tail -1 "$WORK/tls.log")"
fi
"$CRT" run -d --name "$NET-tls" --network "$NET" -p 127.0.0.1::8080 -v "$WORK/tls:/etc/praxis:ro,z" \
  -v "$bca:/etc/praxis/backend-ca/0:ro,z" "$DEFAULT_GATEWAY_IMAGE" --config /etc/praxis/praxis.yaml >/dev/null
port=$("$CRT" port "$NET-tls" 8080 | head -1 | sed 's/.*://')
wait_up "$port" || fail "tls backend: gateway never answered"
check "tls backend: verified by transport.ca and transport.sni" 200 "$(chat "$port")"
render "$WORK/tls-noca" "${TLS_BACKEND[@]}"
port=$(gateway tls-noca "$DEFAULT_GATEWAY_IMAGE" --config "$WORK/tls-noca")
wait_up "$port" || true
code=$(chat "$port")
# A 502 alone could be routing; the log must show the certificate was refused.
"$CRT" logs "$NET-tls-noca" >"$WORK/tls-noca.log" 2>&1 || true
if [ "$code" = 502 ] && grep -qiE 'certificate|unknownissuer' "$WORK/tls-noca.log"; then
  pass "tls backend: without transport.ca the private-CA certificate is refused (502)"
else
  fail "tls backend: without transport.ca, want 502 with a certificate error, got $code: $(grep -iE 'tls|upstream|error' "$WORK/tls-noca.log" | tail -2)"
fi

if [ -z "$API_KEY_IMAGE" ]; then
  echo "  SKIP: api-key runtime (set API_KEY_IMAGE to an image that registers identity/api-key)"
else
  # Private CA and a server cert for the validate stub's DNS name.
  ca=$WORK/ca && mkdir -p "$ca"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 1 -subj /CN=verify-ca \
    -keyout "$ca/ca.key" -out "$ca/ca.crt" 2>/dev/null
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=validate \
    -keyout "$ca/tls.key" -out "$ca/tls.csr" 2>/dev/null
  printf 'subjectAltName=DNS:validate\nextendedKeyUsage=serverAuth\n' > "$ca/ext"
  openssl x509 -req -in "$ca/tls.csr" -CA "$ca/ca.crt" -CAkey "$ca/ca.key" -CAcreateserial -days 1 \
    -extfile "$ca/ext" -out "$ca/tls.crt" 2>/dev/null
  cp "$ca/ca.crt" "$ca/service-ca.crt"
  chmod 644 "$ca"/*
  # Validate stub over https: sk-good is the only valid key.
  "$CRT" run -d --name "$NET-validate" --network "$NET" --network-alias validate -v "$ca:/ca:ro,z" \
    "$FIXTURE_IMAGE" python3 -c '
import json, http.server, ssl
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        key = json.loads(self.rfile.read(int(self.headers["Content-Length"]))).get("key")
        body = json.dumps({"valid": key == "sk-good", "username": "u", "groups": []}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); ctx.load_cert_chain("/ca/tls.crt", "/ca/tls.key")
class S(http.server.HTTPServer):
    def get_request(self):
        sock, addr = self.socket.accept()
        try:
            return ctx.wrap_socket(sock, server_side=True), addr
        except ssl.SSLError as e:
            print("handshake failed:", e, flush=True)
            raise
S(("0.0.0.0", 9443), H).serve_forever()' >/dev/null
  # A non-default tag: the chart refuses api-key on its default image.
  APIKEY=(--set image.tag=api-key-image --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.allowPrivateEndpoint=true
    --set gatewayConfig.auth.validateUrl=https://validate:9443/v)

  render "$WORK/apikey" "${BASE[@]}" "${APIKEY[@]}" \
    --set gatewayConfig.auth.validateCA.configMap=service-ca --set gatewayConfig.auth.validateCA.key=service-ca.crt
  port=$(gateway apikey "$API_KEY_IMAGE" "$API_KEY_IMAGE_CONFIG_FLAG" "$WORK/apikey" "$ca")
  wait_up "$port" -H 'Authorization: Bearer sk-good' || fail "api-key: gateway never answered"
  check "api-key: no key gets 401" 401 "$(chat "$port")"
  check "api-key: bad key gets 401" 401 "$(chat "$port" -H 'Authorization: Bearer sk-bad')"
  check "api-key: good key gets 200 over https with validateCA" 200 "$(chat "$port" -H 'Authorization: Bearer sk-good')"
  check "api-key: backend never sees Authorization" '"authorization": null' "$(seen_auth)"

  render "$WORK/noca" "${BASE[@]}" "${APIKEY[@]}"
  port=$(gateway noca "$API_KEY_IMAGE" "$API_KEY_IMAGE_CONFIG_FLAG" "$WORK/noca")
  wait_up "$port" || fail "api-key without validateCA: gateway never answered"
  before=$("$CRT" logs "$NET-validate" 2>&1 | grep -c 'handshake failed' || true)
  check "api-key: without validateCA the private-CA validator is refused" 401 \
    "$(chat "$port" -H 'Authorization: Bearer sk-good')"
  # The stub logs each refused handshake, so the 401 is the TLS verify, not a stub answer.
  after=$("$CRT" logs "$NET-validate" 2>&1 | grep -c 'handshake failed' || true)
  if [ "$after" -gt "$before" ]; then
    pass "api-key: the refusal is the gateway rejecting the validator's certificate"
  else
    fail "api-key: 401 without a TLS handshake failure at the validator"
  fi
fi

echo "gateway config runtime: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
