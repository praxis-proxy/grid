// gen-types generates Go API types from Grid CRD OpenAPI v3 schemas.
//
// Usage:
//
//	go run ./cmd/gen-types --crds <path-to-deploy/crds> --out <path-to-api/v1alpha1>
//
// The generator reads every *.yaml CRD in --crds, walks the OpenAPI v3 spec
// and status schemas, and writes two files to --out:
//
//   - zz_generated_types.go   -  struct/enum type definitions
//   - zz_generated_deepcopy.go  -  DeepCopyInto / DeepCopy / DeepCopyObject
//
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"sigs.k8s.io/yaml"
)

// ---------------------------------------------------------------------------
// CRD YAML schema types
// ---------------------------------------------------------------------------

type crdDoc struct {
	Spec struct {
		Group string `json:"group"`
		Names struct {
			Kind string `json:"kind"`
		} `json:"names"`
		Versions []struct {
			Name   string `json:"name"`
			Schema struct {
				OpenAPIV3Schema schemaNode `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

type schemaNode struct {
	Type                 string                `json:"type,omitempty"`
	Nullable             bool                  `json:"nullable,omitempty"`
	Description          string                `json:"description,omitempty"`
	Properties           map[string]schemaNode `json:"properties,omitempty"`
	Required             []string              `json:"required,omitempty"`
	Items                *schemaNode           `json:"items,omitempty"`
	Enum                 []interface{}         `json:"enum,omitempty"`
	Format               string                `json:"format,omitempty"`
	AdditionalProperties *schemaNode           `json:"additionalProperties,omitempty"`
	Default              json.RawMessage       `json:"default,omitempty"`
	// OneOf holds the CRD's oneOf constraints. Each entry is an object that
	// may carry a required list, acting as a discriminated-union selector.
	OneOf []schemaNode `json:"oneOf,omitempty"`
}

// ---------------------------------------------------------------------------
// Generated type model
// ---------------------------------------------------------------------------

type goField struct {
	JSONName string
	GoName   string
	GoType   string
	Doc      string
	Optional bool
}

type goDef struct {
	Name       string
	Doc        string
	Fields     []goField
	IsEnum     bool
	EnumValues []string
}


// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

type registry struct {
	defs      []goDef
	byHash    map[string]string // schema hash -> type name
	builtDefs map[string]bool   // type name -> goDef built and appended
	byName    map[string]bool   // type name -> exists (for collision avoidance)
	// Root kinds, in encounter order, for groupversion and root types.
	kinds []kindInfo
}

type kindInfo struct {
	Kind  string
	Group string
}

func newRegistry() *registry {
	return &registry{
		byHash:    make(map[string]string),
		builtDefs: make(map[string]bool),
		byName:    make(map[string]bool),
	}
}

// schemaHash returns a content hash of the schema's structural shape.
// The root node's Nullable and Default are stripped because they express
// how the parent field references this schema (pointer vs value), not the
// structure of the type itself. Description is stripped recursively so
// schemas that differ only in documentation text hash the same. Nested
// Nullable and Default are intentionally preserved: they drive pointer vs
// value decisions for the nested struct fields, so two schemas whose fields
// differ only in nullability must produce distinct hashes.
func schemaHash(n schemaNode) string {
	n.Nullable = false
	n.Default = nil
	b, _ := json.Marshal(stripDescriptions(n))
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:12])
}

func stripDescriptions(n schemaNode) schemaNode {
	n.Description = ""
	if len(n.Properties) > 0 {
		clean := make(map[string]schemaNode, len(n.Properties))
		for k, v := range n.Properties {
			clean[k] = stripDescriptions(v)
		}
		n.Properties = clean
	}
	if n.Items != nil {
		items := stripDescriptions(*n.Items)
		n.Items = &items
	}
	if n.AdditionalProperties != nil {
		ap := stripDescriptions(*n.AdditionalProperties)
		n.AdditionalProperties = &ap
	}
	return n
}

// preRegister records the name for a schema hash without building the goDef.
// Called during the breadth-first name-assignment pass so that shallow types
// claim their natural names before DFS recurses into deeper paths.
func (r *registry) preRegister(schema schemaNode, name string) bool {
	if len(schema.Properties) == 0 && len(schema.Enum) == 0 {
		return false
	}
	isWellKnown := false
	if wk, ok := wellKnownTypeName(schema); ok {
		name = wk
		isWellKnown = true
	} else if schema.Type == "string" && len(schema.Enum) > 0 {
		name = wellKnownEnumName(schema, name)
		isWellKnown = true
	}
	hash := schemaHash(schema)
	if _, exists := r.byHash[hash]; exists {
		return false
	}
	if r.byName[name] {
		if isWellKnown {
			// Two structurally distinct schemas resolved to the same well-known
			// name. Extend wellKnownTypeName (or wellKnownEnumName) to give the
			// second schema its own stable name before re-running the generator.
			panic(fmt.Sprintf("preRegister: well-known name collision %q — extend wellKnownTypeName to distinguish the two schemas", name))
		}
		// Path-derived collision: uniqueName will panic with a clear message.
		name = r.uniqueName(name)
	}
	r.byHash[hash] = name
	r.byName[name] = true
	return true
}

// walkObject registers (if new) and builds the goDef for an object schema.
// Returns the final type name.
func (r *registry) walkObject(schema schemaNode, proposedName string) string {
	hash := schemaHash(schema)
	name, exists := r.byHash[hash]
	if !exists {
		// Well-known types get a stable friendly name regardless of path.
		if wk, ok := wellKnownTypeName(schema); ok {
			proposedName = wk
		}
		name = r.uniqueName(proposedName)
		r.byHash[hash] = name
		r.byName[name] = true
	}
	if !r.builtDefs[name] {
		r.builtDefs[name] = true
		def := r.buildStruct(schema, name)
		r.defs = append(r.defs, def)
	}
	return name
}

// walkEnum registers (if new) and returns the type name for an enum schema.
func (r *registry) walkEnum(schema schemaNode, proposedName string) string {
	hash := schemaHash(schema)
	name, exists := r.byHash[hash]
	if !exists {
		proposedName = wellKnownEnumName(schema, proposedName)
		name = r.uniqueName(proposedName)
		r.byHash[hash] = name
		r.byName[name] = true
	}
	if !r.builtDefs[name] {
		r.builtDefs[name] = true
		def := goDef{Name: name, IsEnum: true}
		for _, v := range schema.Enum {
			if s, ok := v.(string); ok {
				def.EnumValues = append(def.EnumValues, s)
			}
		}
		r.defs = append(r.defs, def)
	}
	return name
}

// uniqueName returns proposed if it is unclaimed, or panics on collision.
// A collision means two structurally distinct schemas produced the same
// path-derived name, which would rename an existing exported type if CRD
// processing order changed. Fix by improving childTypeName to produce a more
// specific name, or by adding a wellKnownTypeName case for one of the schemas.
func (r *registry) uniqueName(proposed string) string {
	if r.byName[proposed] {
		panic(fmt.Sprintf("uniqueName: path-derived name collision %q — improve childTypeName or add a wellKnownTypeName case to disambiguate", proposed))
	}
	return proposed
}

// walkRootObject builds the type definition for a CRD's root Spec or Status
// type. Unlike walkObject, root types are never collapsed via hash
// deduplication: <Kind>Spec and <Kind>Status always receive their own
// definitions even when two CRDs happen to share identical schemas.
func (r *registry) walkRootObject(schema schemaNode, name string) {
	hash := schemaHash(schema)
	if _, exists := r.byHash[hash]; !exists {
		r.byHash[hash] = name
	}
	r.byName[name] = true
	if !r.builtDefs[name] {
		r.builtDefs[name] = true
		def := r.buildStruct(schema, name)
		r.defs = append(r.defs, def)
	}
}

// buildStruct constructs a goDef for an object schema node.
func (r *registry) buildStruct(schema schemaNode, typeName string) goDef {
	doc := formatDoc(schema.Description)
	// Append a union note when the CRD declares oneOf constraints on the
	// object's fields. Each oneOf branch identifies which field is required
	// for that variant, so exactly one field must be set.
	if len(schema.OneOf) > 0 {
		if doc != "" {
			doc += " "
		}
		doc += "Exactly one source field must be set (CRD oneOf)."
	}
	def := goDef{Name: typeName, Doc: doc}
	req := requiredSet(schema)
	for _, jsonName := range sortedKeysDeprecatedLast(schema.Properties) {
		fieldSchema := schema.Properties[jsonName]
		goName := toPascalCase(jsonName)
		cn := childTypeName(typeName, jsonName)
		goType, optional := r.resolveFieldType(fieldSchema, cn, req[jsonName])
		def.Fields = append(def.Fields, goField{
			JSONName: jsonName,
			GoName:   goName,
			GoType:   goType,
			Doc:      formatDoc(fieldSchema.Description),
			Optional: optional,
		})
	}
	return def
}

// resolveFieldType returns the Go type string and whether the field is optional.
func (r *registry) resolveFieldType(schema schemaNode, childName string, isRequired bool) (string, bool) {
	isOptional := !isRequired || schema.Nullable

	// Map (additionalProperties)
	if schema.AdditionalProperties != nil {
		valType, _ := r.resolveFieldType(*schema.AdditionalProperties, childName+"Val", true)
		return "map[string]" + valType, isOptional
	}

	// Array
	if schema.Type == "array" && schema.Items != nil {
		if isMetav1ConditionSchema(*schema.Items) {
			return "[]metav1.Condition", isOptional
		}
		itemName := childTypeName(childName, "Item")
		elemType, _ := r.resolveFieldType(*schema.Items, itemName, true)
		return "[]" + elemType, isOptional
	}

	// Object with properties
	if len(schema.Properties) > 0 {
		if isMetav1ConditionSchema(schema) {
			if isOptional {
				return "*metav1.Condition", true
			}
			return "metav1.Condition", false
		}
		typeName := r.walkObject(schema, childName)
		if isOptional {
			return "*" + typeName, true
		}
		return typeName, false
	}

	// Enum
	if schema.Type == "string" && len(schema.Enum) > 0 {
		typeName := r.walkEnum(schema, childName)
		if schema.Nullable {
			return "*" + typeName, true
		}
		return typeName, isOptional
	}

	// Scalar
	base := scalarGoType(schema)
	if schema.Nullable {
		return "*" + base, true
	}
	// Optional integer, number, and bool fields use a pointer when a client
	// may need to distinguish "not set" from the zero value:
	//   - No default: any value including 0/false is meaningful.
	//   - Non-zero/non-false default: a client that sends 0/false must use a
	//     pointer, otherwise omitempty drops the field and the API server
	//     fills in the non-zero default.
	// Exception: a zero default (0/0.0/false). Dropping it is equivalent to
	// the server filling the default, so a plain type with omitempty is safe.
	if isOptional && (schema.Type == "integer" || schema.Type == "number" || schema.Type == "boolean") && !isZeroDefault(schema.Default) {
		return "*" + base, true
	}
	// Optional string fields use a pointer only when the CRD has a non-empty
	// default. An empty string sent by the caller would be dropped by omitempty,
	// and the API server would then restore the default instead of storing "".
	// No-default optional strings do not need a pointer: "" has no server value
	// to conflict with, and there is no meaningful distinction between absent
	// and empty for fields the server leaves unset.
	if isOptional && schema.Type == "string" && hasNonEmptyStringDefault(schema.Default) {
		return "*" + base, true
	}
	return base, isOptional
}

// isZeroDefault reports whether raw is the zero value for its type:
// 0/0.0 for numbers, or false for booleans.
// An absent (nil/empty) default returns false so that no-default fields still
// get pointer treatment.
func isZeroDefault(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	if string(raw) == "false" {
		return true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		return false
	}
	return f == 0
}

// hasNonEmptyStringDefault reports whether raw is a non-empty JSON string
// literal. Used to decide whether an optional string field needs *string: only
// when the CRD declares a non-empty default can "" (the zero value) conflict
// with the API server restoring that default via omitempty.
func hasNonEmptyStringDefault(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return false
	}
	return s != ""
}

type bfsNode struct {
	schema schemaNode
	name   string
}

// preRegisterBFS assigns names to shared types breadth-first across all seed
// roots. Processing level-by-level ensures that shallower fields always claim
// a type name before deeper paths that share the same schema hash, regardless
// of the order CRDs or sibling fields are visited.
func (r *registry) preRegisterBFS(seeds ...bfsNode) {
	queue := make([]bfsNode, 0)

	enqueue := func(parent bfsNode) {
		for _, jsonName := range sortedKeysDeprecatedLast(parent.schema.Properties) {
			child := bfsNode{parent.schema.Properties[jsonName], childTypeName(parent.name, jsonName)}
			if len(child.schema.Properties) > 0 {
				r.preRegister(child.schema, child.name)
				queue = append(queue, child)
			}
			if child.schema.Type == "array" && child.schema.Items != nil {
				item := bfsNode{*child.schema.Items, childTypeName(child.name, "Item")}
				if len(item.schema.Properties) > 0 {
					r.preRegister(item.schema, item.name)
					queue = append(queue, item)
				}
			}
			if child.schema.Type == "string" && len(child.schema.Enum) > 0 {
				r.preRegister(child.schema, child.name)
			}
		}
	}

	for _, seed := range seeds {
		enqueue(seed)
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		enqueue(node)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func requiredSet(schema schemaNode) map[string]bool {
	m := make(map[string]bool, len(schema.Required))
	for _, r := range schema.Required {
		m[r] = true
	}
	return m
}

func sortedKeys(m map[string]schemaNode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedKeysDeprecatedLast returns property keys sorted alphabetically with
// fields whose description begins with "Deprecated" ordered after all others.
// Used in the name-claiming pass so non-deprecated fields always claim the
// natural type name before their deprecated aliases, which share the same
// schema hash and inherit the already-registered name. This keeps the
// exported type name stable when a deprecated alias is eventually removed.
func sortedKeysDeprecatedLast(m map[string]schemaNode) []string {
	keys := sortedKeys(m)
	sort.SliceStable(keys, func(i, j int) bool {
		di := strings.HasPrefix(strings.ToLower(m[keys[i]].Description), "deprecated")
		dj := strings.HasPrefix(strings.ToLower(m[keys[j]].Description), "deprecated")
		if di != dj {
			return !di
		}
		return keys[i] < keys[j]
	})
	return keys
}

// acronyms maps lowercase word segments to their canonical Go form.
var acronyms = map[string]string{
	"ai":     "AI",
	"api":    "API",
	"ca":     "CA",
	"crdt":   "CRDT",
	"id":     "ID",
	"kv":     "KV",
	"mcp":    "MCP",
	"mtls":   "MTLS",
	"oauth2": "OAuth2",
	"otlp":   "OTLP",
	"pem":    "PEM",
	"sigv4":  "SigV4",
	"sni":    "SNI",
	"swim":   "SWIM",
	"tls":    "TLS",
	"ttl":    "TTL",
	"url":    "URL",
	"usd":    "USD",
}

// splitCamelCaseWords breaks a camelCase identifier into component words.
// "caSecretRef"  -> ["ca", "Secret", "Ref"]
// "otlpEndpoint" -> ["otlp", "Endpoint"]
// "tlsConfig"    -> ["tls", "Config"]
func splitCamelCaseWords(s string) []string {
	if s == "" {
		return nil
	}
	runes := []rune(s)
	var words []string
	start := 0
	for i := 1; i < len(runes); i++ {
		if !unicode.IsUpper(runes[i]) {
			continue
		}
		// Split before an uppercase letter that follows a lowercase letter.
		// e.g., camelCase -> [camel, Case]
		if unicode.IsLower(runes[i-1]) {
			words = append(words, string(runes[start:i]))
			start = i
			continue
		}
		// Split before an uppercase letter that starts a new CamelCase word
		// inside an all-uppercase run. e.g., TLSConfig -> [TLS, Config]
		if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
			words = append(words, string(runes[start:i]))
			start = i
		}
	}
	words = append(words, string(runes[start:]))
	return words
}

func toPascalCase(s string) string {
	if s == "" {
		return ""
	}
	words := splitCamelCaseWords(s)
	var sb strings.Builder
	for _, w := range words {
		if upper, ok := acronyms[strings.ToLower(w)]; ok {
			sb.WriteString(upper)
			continue
		}
		runes := []rune(w)
		sb.WriteRune(unicode.ToUpper(runes[0]))
		sb.WriteString(string(runes[1:]))
	}
	return sb.String()
}

func scalarGoType(schema schemaNode) string {
	switch schema.Type {
	case "boolean":
		return "bool"
	case "integer":
		return "int64"
	case "number":
		return "float64"
	case "string":
		return "string"
	default:
		panic(fmt.Sprintf("unhandled schema type %q at scalarGoType; add a case or handle before calling", schema.Type))
	}
}

func isScalar(goType string) bool {
	switch goType {
	case "string", "bool", "int64", "float64":
		return true
	}
	return false
}

func isPointerType(goType string) bool {
	return strings.HasPrefix(goType, "*")
}

func isSliceType(goType string) bool {
	return strings.HasPrefix(goType, "[]")
}

func isMapType(goType string) bool {
	return strings.HasPrefix(goType, "map[")
}


func derefType(goType string) string {
	return strings.TrimPrefix(goType, "*")
}

func elemType(goType string) string {
	return strings.TrimPrefix(goType, "[]")
}

// mapValueType returns the value type of a map type string, e.g.
// "map[string]string" -> "string", "map[string][]Foo" -> "[]Foo".
func mapValueType(goType string) string {
	idx := strings.Index(goType, "]")
	if idx < 0 {
		return ""
	}
	return goType[idx+1:]
}

// isMetav1ConditionSchema returns true when a schema matches the metav1.Condition shape.
func isMetav1ConditionSchema(n schemaNode) bool {
	if len(n.Properties) != 6 {
		return false
	}
	for _, want := range []string{"lastTransitionTime", "message", "observedGeneration", "reason", "status", "type"} {
		if _, ok := n.Properties[want]; !ok {
			return false
		}
	}
	return true
}

// wellKnownTypeName returns a stable, friendly type name for schemas that are
// recognised as shared types across multiple CRDs. Returns ("", false) if the
// schema is not a well-known type.
func wellKnownTypeName(n schemaNode) (string, bool) {
	props := n.Properties
	if len(props) == 0 {
		return "", false
	}
	keys := sortedKeys(props)

	switch {
	// SecretKeyRef: {key, name, namespace} - all three required (e.g. SiteInvite token secret).
	case eqKeys(keys, "key", "name", "namespace") && hasRequired(n, "key", "name", "namespace"):
		return "SecretKeyRef", true

	// ObjectKeyRef: {key?, name, namespace} - name+namespace required, key optional.
	// Used for both Secret and ConfigMap references that share this shape.
	case eqKeys(keys, "key", "name", "namespace") && hasRequired(n, "name", "namespace"):
		return "ObjectKeyRef", true

	// SelectorConfig: {matchLabels} with additionalProperties map.
	case eqKeys(keys, "matchLabels") && props["matchLabels"].AdditionalProperties != nil:
		return "SelectorConfig", true

	// AccessPolicy: {siteSelector} where siteSelector is a SelectorConfig.
	case eqKeys(keys, "siteSelector") && len(props["siteSelector"].Properties) == 1:
		inner := props["siteSelector"].Properties
		if _, ok := inner["matchLabels"]; ok {
			return "AccessPolicy", true
		}

	// AuthConfig: required `strategy` plus at least one credential or management
	// field. Uses subsetOfKeys instead of eqKeys so new optional fields added to
	// the schema don't break detection without requiring an exact match.
	case hasAllKeys(keys, "strategy") &&
		(hasKey(keys, "secretRef") || hasKey(keys, "manual") || hasKey(keys, "credentialsManagedExternally")) &&
		subsetOfKeys(keys, "strategy", "secretRef", "manual", "credentialsManagedExternally"):
		return "AuthConfig", true

	// EndpointTLSConfig: {caConfigMapRef?, caSecretRef?, clientCertificateSecretRef?}.
	case eqKeys(keys, "caConfigMapRef", "caSecretRef", "clientCertificateSecretRef"):
		return "EndpointTLSConfig", true

	// ClientCertificateSecretRef: {certificateKey, name, namespace, privateKeyKey}.
	case eqKeys(keys, "certificateKey", "name", "namespace", "privateKeyKey"):
		return "ClientCertificateSecretRef", true

	// EndpointTransport (consumer cluster transport): {caSecretRef?, mode, sni?}.
	case eqKeys(keys, "caSecretRef", "mode", "sni") ||
		eqKeys(keys, "mode", "sni"):
		if modeField, ok := props["mode"]; ok && len(modeField.Enum) > 0 {
			return "EndpointTransport", true
		}

	// GatewayCASecretRef (transport caSecretRef with just name+key, no namespace).
	case eqKeys(keys, "key", "name") && hasRequired(n, "name"):
		return "GatewayCASecretRef", true

	// MetricsConfig: {auth?, metricsEndpoint?, path, poolName?, queueCapacity?, signalNames, ...}
	case hasAllKeys(keys, "signalNames", "path") && hasKey(keys, "timeout"):
		return "MetricsConfig", true

	// MetricSignalNames: all nullable string signal fields.
	case hasAllKeys(keys, "queueDepth", "kvCacheUtilization", "latencyP99Ms"):
		return "MetricSignalNames", true

	// ModelInfo: {capabilities?, contextWindow?, name}.
	case eqKeys(keys, "capabilities", "contextWindow", "name") ||
		eqKeys(keys, "contextWindow", "name") ||
		eqKeys(keys, "capabilities", "name"):
		if _, ok := props["name"]; ok {
			return "ModelInfo", true
		}

	// ToolInfo: {description?, name}.
	case eqKeys(keys, "description", "name") || eqKeys(keys, "name"):
		if _, ok := props["name"]; ok && len(props) <= 2 {
			return "ToolInfo", true
		}

	// TrafficPolicy: {drain}.
	case eqKeys(keys, "drain"):
		return "TrafficPolicy", true

	// HealthCheckConfig: {endpoint?, interval?, path?, timeout?, tls?}
	case hasAllKeys(keys, "interval", "path", "timeout") && hasKey(keys, "tls"):
		return "HealthCheckConfig", true

	// ModelDiscoveryConfig: {openAiModels}.
	case eqKeys(keys, "openAiModels"):
		return "ModelDiscoveryConfig", true

	// OpenAIModelsSource: {endpoint?, path, tls?}
	case hasAllKeys(keys, "path") && hasKey(keys, "endpoint") && !hasKey(keys, "name"):
		if _, hasTLS := props["tls"]; hasTLS || len(keys) <= 3 {
			return "OpenAIModelsSource", true
		}

	// CostConfig: {perMillionInputTokens, perMillionOutputTokens}
	case eqKeys(keys, "perMillionInputTokens", "perMillionOutputTokens"):
		return "CostConfig", true

	// MetricsAuth: {allowPlaintext?, type}
	case eqKeys(keys, "allowPlaintext", "type") || eqKeys(keys, "type"):
		if typeField, ok := props["type"]; ok && len(typeField.Enum) > 0 {
			return "MetricsAuth", true
		}
	}
	return "", false
}

// wellKnownEnumName returns a friendly name for common enum schemas.
func wellKnownEnumName(schema schemaNode, proposedName string) string {
	if schema.Type != "string" || len(schema.Enum) == 0 {
		return proposedName
	}
	vals := make([]string, 0, len(schema.Enum))
	for _, v := range schema.Enum {
		if s, ok := v.(string); ok {
			vals = append(vals, s)
		}
	}
	sort.Strings(vals)
	key := strings.Join(vals, ",")
	switch key {
	case "Available,Degraded,Pending,Unavailable":
		return "ProviderPhase"
	case "Active,Connecting,Discovered,Left,Pending,Unreachable":
		return "GridSitePhase"
	case "Active,Degraded,Initializing,Pending":
		return "GridNetworkPhase"
	case "Disabled,Error,Rendered":
		return "ConsumerConfigPhase"
	case "Error,MountsReconciling,Ready,RequirementsRendered,WaitingForRollout,WaitingForSecret":
		return "MountReconciliationPhase"
	case "Distributed,Error,Pending,Retained":
		return "OverlayPhase"
	case "api_key,bearer_token,custom,mtls_only,oauth2,service_account,sigv4":
		return "AuthStrategy"
	case "serviceAccountToken":
		return "MetricsAuthType"
	case "Mutual,Plaintext":
		return "EgressTLSMode"
	case "mutual_tls,plaintext,tls":
		return "TransportMode"
	case "excluded,existingOnly":
		return "MissingMetricsPolicy"
	case "instantaneous,stabilized":
		return "AdmissionMode"
	case "deterministic,random,roundRobin,weightedRandom":
		return "SelectionMode"
	case "gossip,poll":
		return "SignalMode"
	case "geographyFirst,scoreFirst":
		return "RoutingPolicy"
	case "kvCachePressure,noMetrics,queueDepth":
		return "ScoringStrategy"
	case "pin,spiffe":
		return "PeerTrustMode"
	case "static":
		return "PlacementStrategy"
	}
	return proposedName
}

func eqKeys(actual []string, want ...string) bool {
	if len(actual) != len(want) {
		return false
	}
	sort.Strings(want)
	for i, k := range actual {
		if k != want[i] {
			return false
		}
	}
	return true
}

func hasRequired(n schemaNode, fields ...string) bool {
	req := requiredSet(n)
	for _, f := range fields {
		if !req[f] {
			return false
		}
	}
	return true
}

func hasAllKeys(actual []string, want ...string) bool {
	set := make(map[string]bool, len(actual))
	for _, k := range actual {
		set[k] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

func hasKey(actual []string, key string) bool {
	for _, k := range actual {
		if k == key {
			return true
		}
	}
	return false
}

// subsetOfKeys reports whether every key in actual is contained in allowed.
// Use with hasAllKeys to match a type that may grow new optional fields:
//
//	hasAllKeys(keys, "strategy") && subsetOfKeys(keys, "strategy", "secretRef", ...)
func subsetOfKeys(actual []string, allowed ...string) bool {
	set := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		set[a] = true
	}
	for _, k := range actual {
		if !set[k] {
			return false
		}
	}
	return true
}

// formatDoc normalises a CRD description into Go doc comment text.
// Each paragraph (blank-line-separated block) becomes one line; single
// newlines within a paragraph are joined with a space. Backtick markdown
// from rustdoc is stripped. Paragraphs are separated by "\n" in the
// returned string so callers can emit one "// line" per paragraph.
func formatDoc(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Split on blank lines to get paragraphs, then normalise each.
	rawParas := strings.Split(s, "\n\n")
	paras := make([]string, 0, len(rawParas))
	for _, p := range rawParas {
		p = strings.Join(strings.Fields(p), " ")
		p = strings.ReplaceAll(p, "`", "")
		if p != "" {
			paras = append(paras, p)
		}
	}
	return strings.Join(paras, "\n")
}

func trimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		return s[:len(s)-len(suffix)]
	}
	return s
}

// childTypeName computes the proposed type name for a nested field.
// It strips Spec/Status suffix from the parent name so that top-level children
// get names like "InferenceProviderMetricsConfig" instead of
// "InferenceProviderSpecMetricsConfig".
func childTypeName(parentName, fieldName string) string {
	base := trimSuffix(trimSuffix(parentName, "Spec"), "Status")
	return base + toPascalCase(fieldName)
}

// ---------------------------------------------------------------------------
// Code emission  -  types file
// ---------------------------------------------------------------------------

func emitTypesFile(reg *registry, group string) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "// Code generated by cmd/gen-types. DO NOT EDIT.\n")
	fmt.Fprintf(&b, "// Source: deploy/crds/*.yaml  Regenerate: make generate-go-types\n\n")
	fmt.Fprintf(&b, "package v1alpha1\n\n")
	fmt.Fprintf(&b, "import (\n")
	fmt.Fprintf(&b, "\tmetav1 %q\n", "k8s.io/apimachinery/pkg/apis/meta/v1")
	fmt.Fprintf(&b, "\t%q\n", "k8s.io/apimachinery/pkg/runtime")
	fmt.Fprintf(&b, "\t%q\n", "k8s.io/apimachinery/pkg/runtime/schema")
	fmt.Fprintf(&b, ")\n\n")

	// SchemeGroupVersion and registration
	fmt.Fprintf(&b, "// SchemeGroupVersion is the group and version for all Grid CRD types.\n")
	fmt.Fprintf(&b, "var SchemeGroupVersion = schema.GroupVersion{Group: %q, Version: %q}\n\n", group, "v1alpha1")
	fmt.Fprintf(&b, "// SchemeBuilder adds the Grid CRD types to a scheme.\n")
	fmt.Fprintf(&b, "var SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)\n\n")
	fmt.Fprintf(&b, "// AddToScheme registers all Grid CRD types with the given scheme.\n")
	fmt.Fprintf(&b, "var AddToScheme = SchemeBuilder.AddToScheme\n\n")
	fmt.Fprintf(&b, "func addKnownTypes(scheme *runtime.Scheme) error {\n")
	fmt.Fprintf(&b, "\tscheme.AddKnownTypes(SchemeGroupVersion,\n")
	for _, ki := range reg.kinds {
		fmt.Fprintf(&b, "\t\t&%s{},\n", ki.Kind)
		fmt.Fprintf(&b, "\t\t&%sList{},\n", ki.Kind)
	}
	fmt.Fprintf(&b, "\t)\n\tmetav1.AddToGroupVersion(scheme, SchemeGroupVersion)\n\treturn nil\n}\n\n")

	// Root resource types
	for _, ki := range reg.kinds {
		k := ki.Kind
		fmt.Fprintf(&b, "// %s is the root resource type.\n", k)
		fmt.Fprintf(&b, "type %s struct {\n", k)
		fmt.Fprintf(&b, "\tmetav1.TypeMeta   `json:\",inline\"`\n")
		fmt.Fprintf(&b, "\tmetav1.ObjectMeta `json:\"metadata,omitempty\"`\n\n")
		fmt.Fprintf(&b, "\tSpec   %sSpec   `json:\"spec,omitempty\"`\n", k)
		fmt.Fprintf(&b, "\tStatus %sStatus `json:\"status,omitempty\"`\n", k)
		fmt.Fprintf(&b, "}\n\n")

		fmt.Fprintf(&b, "// %sList is a list of %s resources.\n", k, k)
		fmt.Fprintf(&b, "type %sList struct {\n", k)
		fmt.Fprintf(&b, "\tmetav1.TypeMeta `json:\",inline\"`\n")
		fmt.Fprintf(&b, "\tmetav1.ListMeta `json:\"metadata,omitempty\"`\n\n")
		fmt.Fprintf(&b, "\tItems []%s `json:\"items\"`\n", k)
		fmt.Fprintf(&b, "}\n\n")
	}

	// Enum types first, then struct types
	var enums, structs []goDef
	for _, d := range reg.defs {
		if d.IsEnum {
			enums = append(enums, d)
		} else {
			structs = append(structs, d)
		}
	}

	for _, d := range enums {
		emitEnum(&b, d)
	}
	for _, d := range structs {
		emitStruct(&b, d)
	}

	return format.Source(b.Bytes())
}

func emitEnum(b *bytes.Buffer, d goDef) {
	fmt.Fprintf(b, "// %s is a string enumeration.\n", d.Name)
	fmt.Fprintf(b, "type %s string\n\n", d.Name)
	fmt.Fprintf(b, "const (\n")
	for _, v := range d.EnumValues {
		constName := d.Name + enumConstSuffix(v)
		fmt.Fprintf(b, "\t%s %s = %q\n", constName, d.Name, v)
	}
	fmt.Fprintf(b, ")\n\n")
}

// enumConstSuffix converts an enum value to a Go constant suffix.
// snake_case segments that match a known acronym are fully uppercased.
// e.g., "api_key" -> "APIKey", "bearer_token" -> "BearerToken"
func enumConstSuffix(v string) string {
	parts := strings.FieldsFunc(v, func(r rune) bool { return r == '_' || r == '-' })
	var sb strings.Builder
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		for _, w := range splitCamelCaseWords(p) {
			if upper, ok := acronyms[strings.ToLower(w)]; ok {
				sb.WriteString(upper)
			} else {
				r := []rune(w)
				r[0] = unicode.ToUpper(r[0])
				sb.WriteString(string(r))
			}
		}
	}
	return sb.String()
}

func emitStruct(b *bytes.Buffer, d goDef) {
	if d.Doc != "" {
		first := true
		for _, line := range strings.Split(d.Doc, "\n") {
			if first {
				fmt.Fprintf(b, "// %s %s\n", d.Name, line)
				first = false
			} else {
				fmt.Fprintf(b, "// %s\n", line)
			}
		}
	} else {
		fmt.Fprintf(b, "// %s is a generated type.\n", d.Name)
	}
	fmt.Fprintf(b, "type %s struct {\n", d.Name)
	for _, f := range d.Fields {
		tag := f.JSONName
		if f.Optional {
			tag += ",omitempty"
		}
		if f.Doc != "" {
			for _, line := range strings.Split(f.Doc, "\n") {
				fmt.Fprintf(b, "\t// %s\n", line)
			}
		}
		fmt.Fprintf(b, "\t%s %s `json:\"%s\"`\n", f.GoName, f.GoType, tag)
	}
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// Code emission  -  deep copy file
// ---------------------------------------------------------------------------

func emitDeepCopyFile(reg *registry) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "//go:build !ignore_autogenerated\n\n")
	fmt.Fprintf(&b, "// Code generated by cmd/gen-types. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package v1alpha1\n\n")
	fmt.Fprintf(&b, "import (\n")
	fmt.Fprintf(&b, "\tmetav1 %q\n", "k8s.io/apimachinery/pkg/apis/meta/v1")
	fmt.Fprintf(&b, "\t%q\n", "k8s.io/apimachinery/pkg/runtime")
	fmt.Fprintf(&b, ")\n\n")

	// Root types: DeepCopyObject
	for _, ki := range reg.kinds {
		k := ki.Kind
		emitRootDeepCopy(&b, k)
	}

	// Generated struct types
	for _, d := range reg.defs {
		if d.IsEnum {
			continue
		}
		emitStructDeepCopy(&b, d, reg)
	}

	return format.Source(b.Bytes())
}

func emitRootDeepCopy(b *bytes.Buffer, kind string) {
	fmt.Fprintf(b, "func (in *%s) DeepCopyInto(out *%s) {\n", kind, kind)
	fmt.Fprintf(b, "\t*out = *in\n")
	// TypeMeta is a plain struct (no pointers), already copied by *out = *in.
	fmt.Fprintf(b, "\tin.ObjectMeta.DeepCopyInto(&out.ObjectMeta)\n")
	fmt.Fprintf(b, "\tin.Spec.DeepCopyInto(&out.Spec)\n")
	fmt.Fprintf(b, "\tin.Status.DeepCopyInto(&out.Status)\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "func (in *%s) DeepCopy() *%s {\n", kind, kind)
	fmt.Fprintf(b, "\tif in == nil { return nil }\n")
	fmt.Fprintf(b, "\tout := new(%s)\n", kind)
	fmt.Fprintf(b, "\tin.DeepCopyInto(out)\n")
	fmt.Fprintf(b, "\treturn out\n}\n\n")

	fmt.Fprintf(b, "func (in *%s) DeepCopyObject() runtime.Object {\n", kind)
	fmt.Fprintf(b, "\tif c := in.DeepCopy(); c != nil { return c }\n")
	fmt.Fprintf(b, "\treturn nil\n}\n\n")

	list := kind + "List"
	fmt.Fprintf(b, "func (in *%s) DeepCopyInto(out *%s) {\n", list, list)
	fmt.Fprintf(b, "\t*out = *in\n")
	fmt.Fprintf(b, "\tin.ListMeta.DeepCopyInto(&out.ListMeta)\n")
	fmt.Fprintf(b, "\tif in.Items != nil {\n")
	fmt.Fprintf(b, "\t\tout.Items = make([]%s, len(in.Items))\n", kind)
	fmt.Fprintf(b, "\t\tfor i := range in.Items { in.Items[i].DeepCopyInto(&out.Items[i]) }\n")
	fmt.Fprintf(b, "\t}\n}\n\n")

	fmt.Fprintf(b, "func (in *%s) DeepCopy() *%s {\n", list, list)
	fmt.Fprintf(b, "\tif in == nil { return nil }\n")
	fmt.Fprintf(b, "\tout := new(%s)\n", list)
	fmt.Fprintf(b, "\tin.DeepCopyInto(out)\n")
	fmt.Fprintf(b, "\treturn out\n}\n\n")

	fmt.Fprintf(b, "func (in *%s) DeepCopyObject() runtime.Object {\n", list)
	fmt.Fprintf(b, "\tif c := in.DeepCopy(); c != nil { return c }\n")
	fmt.Fprintf(b, "\treturn nil\n}\n\n")
}

func emitStructDeepCopy(b *bytes.Buffer, d goDef, reg *registry) {
	fmt.Fprintf(b, "func (in *%s) DeepCopyInto(out *%s) {\n", d.Name, d.Name)
	fmt.Fprintf(b, "\t*out = *in\n")
	for _, f := range d.Fields {
		emitFieldDeepCopy(b, f, reg)
	}
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "func (in *%s) DeepCopy() *%s {\n", d.Name, d.Name)
	fmt.Fprintf(b, "\tif in == nil { return nil }\n")
	fmt.Fprintf(b, "\tout := new(%s)\n", d.Name)
	fmt.Fprintf(b, "\tin.DeepCopyInto(out)\n")
	fmt.Fprintf(b, "\treturn out\n}\n\n")
}

func emitFieldDeepCopy(b *bytes.Buffer, f goField, reg *registry) {
	t := f.GoType
	fname := f.GoName

	switch {
	case isPointerType(t):
		inner := derefType(t)
		if isScalar(inner) || reg.isEnum(inner) {
			fmt.Fprintf(b, "\tif in.%s != nil { x := *in.%s; out.%s = &x }\n", fname, fname, fname)
		} else {
			fmt.Fprintf(b, "\tif in.%s != nil {\n", fname)
			fmt.Fprintf(b, "\t\tout.%s = new(%s)\n", fname, inner)
			fmt.Fprintf(b, "\t\tin.%s.DeepCopyInto(out.%s)\n", fname, fname)
			fmt.Fprintf(b, "\t}\n")
		}

	case isMapType(t):
		vt := mapValueType(t)
		fmt.Fprintf(b, "\tif in.%s != nil {\n", fname)
		fmt.Fprintf(b, "\t\tout.%s = make(%s, len(in.%s))\n", fname, t, fname)
		switch {
		case isScalar(vt) || reg.isEnum(vt):
			fmt.Fprintf(b, "\t\tfor k, v := range in.%s { out.%s[k] = v }\n", fname, fname)
		case isPointerType(vt):
			inner := derefType(vt)
			if isScalar(inner) || reg.isEnum(inner) {
				// *scalar: copy the pointed-to value; no DeepCopy method on scalar pointers.
				fmt.Fprintf(b, "\t\tfor k, v := range in.%s { if v != nil { tmp := *v; out.%s[k] = &tmp } }\n", fname, fname)
			} else {
				fmt.Fprintf(b, "\t\tfor k, v := range in.%s { if v != nil { out.%s[k] = v.DeepCopy() } }\n", fname, fname)
			}
		case isSliceType(vt):
			elem := elemType(vt)
			if isScalar(elem) || reg.isEnum(elem) {
				fmt.Fprintf(b, "\t\tfor k, v := range in.%s { tmp := make(%s, len(v)); copy(tmp, v); out.%s[k] = tmp }\n", fname, vt, fname)
			} else {
				fmt.Fprintf(b, "\t\tfor k, v := range in.%s { tmp := make(%s, len(v)); for i := range v { v[i].DeepCopyInto(&tmp[i]) }; out.%s[k] = tmp }\n", fname, vt, fname)
			}
		case isMapType(vt):
			// Nested map[string]map[...] is not yet supported; fail at generation time
			// rather than emitting code that does not compile.
			fmt.Fprintf(os.Stderr, "deep-copy: nested map value type %q in field %s is not supported\n", vt, fname)
			os.Exit(1)
		default:
			// Struct value: copy via its own DeepCopyInto.
			fmt.Fprintf(b, "\t\tfor k := range in.%s { tmp := in.%s[k]; var cp %s; tmp.DeepCopyInto(&cp); out.%s[k] = cp }\n", fname, fname, vt, fname)
		}
		fmt.Fprintf(b, "\t}\n")

	case isSliceType(t):
		elem := elemType(t)
		fmt.Fprintf(b, "\tif in.%s != nil {\n", fname)
		fmt.Fprintf(b, "\t\tout.%s = make(%s, len(in.%s))\n", fname, t, fname)
		switch {
		case isScalar(elem) || reg.isEnum(elem):
			fmt.Fprintf(b, "\t\tcopy(out.%s, in.%s)\n", fname, fname)
		case isPointerType(elem):
			inner := derefType(elem)
			if isScalar(inner) || reg.isEnum(inner) {
				// []*scalar: copy the pointed-to value.
				fmt.Fprintf(b, "\t\tfor i, v := range in.%s { if v != nil { tmp := *v; out.%s[i] = &tmp } }\n", fname, fname)
			} else {
				// []*T: call DeepCopy on the non-nil pointer.
				fmt.Fprintf(b, "\t\tfor i, v := range in.%s { if v != nil { out.%s[i] = v.DeepCopy() } }\n", fname, fname)
			}
		case isSliceType(elem) || isMapType(elem):
			// Nested slice/map elements are not yet supported; fail at generation
			// time rather than emitting code that does not compile.
			fmt.Fprintf(os.Stderr, "deep-copy: nested %s element type %q in field %s is not supported\n",
				map[bool]string{true: "slice", false: "map"}[isSliceType(elem)], elem, fname)
			os.Exit(1)
		default:
			// Struct value: element-wise DeepCopyInto.
			fmt.Fprintf(b, "\t\tfor i := range in.%s { in.%s[i].DeepCopyInto(&out.%s[i]) }\n", fname, fname, fname)
		}
		fmt.Fprintf(b, "\t}\n")

	default:
		// Enum and scalar fields are already handled by *out = *in.
		if !isScalar(t) && !reg.isEnum(t) {
			fmt.Fprintf(b, "\tin.%s.DeepCopyInto(&out.%s)\n", fname, fname)
		}
	}
}

func (r *registry) isEnum(typeName string) bool {
	for _, d := range r.defs {
		if d.Name == typeName && d.IsEnum {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	crdsDir := flag.String("crds", "", "path to deploy/crds directory")
	outDir := flag.String("out", "", "path to api/v1alpha1 directory")
	flag.Parse()

	if *crdsDir == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "usage: gen-types --crds <path> --out <path>")
		os.Exit(1)
	}

	entries, err := os.ReadDir(*crdsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read crds dir: %v\n", err)
		os.Exit(1)
	}

	reg := newRegistry()
	var group string

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") || e.Name() == "kustomization.yaml" {
			continue
		}
		path := filepath.Join(*crdsDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
			os.Exit(1)
		}
		var doc crdDoc
		if err := yaml.Unmarshal(data, &doc); err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
			os.Exit(1)
		}
		if len(doc.Spec.Versions) == 0 {
			continue
		}
		var versionSchema *schemaNode
		for i := range doc.Spec.Versions {
			if doc.Spec.Versions[i].Name == "v1alpha1" {
				s := doc.Spec.Versions[i].Schema.OpenAPIV3Schema
				versionSchema = &s
				break
			}
		}
		if versionSchema == nil {
			fmt.Fprintf(os.Stderr, "%s: no v1alpha1 version found\n", path)
			os.Exit(1)
		}
		kind := doc.Spec.Names.Kind
		if group == "" {
			group = doc.Spec.Group
		} else if doc.Spec.Group != group {
			fmt.Fprintf(os.Stderr, "%s: group %q differs from previously seen %q; all CRDs must share one API group\n",
				path, doc.Spec.Group, group)
			os.Exit(1)
		}
		root := *versionSchema

		reg.kinds = append(reg.kinds, kindInfo{Kind: kind, Group: group})

		specSchema := root.Properties["spec"]
		statusSchema := root.Properties["status"]

		specName := kind + "Spec"
		statusName := kind + "Status"

		// BFS pre-registration: register both spec and status roots, then
		// fan out level-by-level so shallower fields always claim names first.
		reg.preRegister(specSchema, specName)
		reg.preRegister(statusSchema, statusName)
		reg.preRegisterBFS(
			bfsNode{specSchema, specName},
			bfsNode{statusSchema, statusName},
		)

		// DFS walk builds the actual defs. Root types use walkRootObject so
		// that structurally identical schemas across CRDs each get their own
		// <Kind>Spec / <Kind>Status definition and the package always compiles.
		reg.walkRootObject(specSchema, specName)
		reg.walkRootObject(statusSchema, statusName)
	}

	typesFile := filepath.Join(*outDir, "zz_generated_types.go")
	typesBytes, err := emitTypesFile(reg, group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "format types: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(typesFile, typesBytes, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", typesFile, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s\n", typesFile)

	dcFile := filepath.Join(*outDir, "zz_generated_deepcopy.go")
	dcBytes, err := emitDeepCopyFile(reg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "format deepcopy: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(dcFile, dcBytes, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", dcFile, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s\n", dcFile)
}