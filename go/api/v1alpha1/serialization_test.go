// Tests for the generated Grid CRD API types.
//
// Run "make generate-go-types" first to produce the zz_generated_*.go files,
// then "make go-types-check" to run these tests.
//
package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	v1alpha1 "github.com/praxis-proxy/grid/go/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// TestSchemeRegistration verifies all four public CRD kinds and their list
// types are registered after calling AddToScheme.
func TestSchemeRegistration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	types := scheme.KnownTypes(v1alpha1.SchemeGroupVersion)
	for _, kind := range []string{
		"AgentToolProvider", "AgentToolProviderList",
		"InferenceProvider", "InferenceProviderList",
		"GridNetwork", "GridNetworkList",
		"GridSite", "GridSiteList",
		"SiteInvite", "SiteInviteList",
	} {
		if _, ok := types[kind]; !ok {
			t.Errorf("kind %q not registered in scheme", kind)
		}
	}
}

// TestCRDSpecPropertiesMatchGoJSONTags is the structural drift check.
// It recursively walks the full OpenAPI v3 spec and status schemas of each
// deployed CRD and verifies that every property  -  at every nesting level  - 
// appears as a json struct tag on the corresponding Go type with a compatible
// Go kind (string/int64/float64/bool/slice/map/struct).
//
// A failure here means a field was renamed, added, restructured, or retyped in
// the Rust CRD types without the Go types being regenerated  -  run make generate-go-types.
func TestCRDSpecPropertiesMatchGoJSONTags(t *testing.T) {
	const crdDir = "../../../deploy/crds"

	checks := []struct {
		crdFile   string
		schemaKey string
		goType    reflect.Type
	}{
		{"inferenceprovider.yaml", "spec", reflect.TypeOf(v1alpha1.InferenceProviderSpec{})},
		{"inferenceprovider.yaml", "status", reflect.TypeOf(v1alpha1.InferenceProviderStatus{})},
		{"gridnetwork.yaml", "spec", reflect.TypeOf(v1alpha1.GridNetworkSpec{})},
		{"gridnetwork.yaml", "status", reflect.TypeOf(v1alpha1.GridNetworkStatus{})},
		{"gridsite.yaml", "spec", reflect.TypeOf(v1alpha1.GridSiteSpec{})},
		{"gridsite.yaml", "status", reflect.TypeOf(v1alpha1.GridSiteStatus{})},
		{"agenttoolprovider.yaml", "spec", reflect.TypeOf(v1alpha1.AgentToolProviderSpec{})},
		{"agenttoolprovider.yaml", "status", reflect.TypeOf(v1alpha1.AgentToolProviderStatus{})},
		{"siteinvite.yaml", "spec", reflect.TypeOf(v1alpha1.SiteInviteSpec{})},
		{"siteinvite.yaml", "status", reflect.TypeOf(v1alpha1.SiteInviteStatus{})},
	}

	for _, c := range checks {
		name := strings.TrimSuffix(c.crdFile, ".yaml") + "/" + c.schemaKey
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(crdDir + "/" + c.crdFile)
			if err != nil {
				t.Fatalf("read %s: %v", c.crdFile, err)
			}
			var crd map[string]interface{}
			if err := yaml.Unmarshal(data, &crd); err != nil {
				t.Fatalf("unmarshal %s: %v", c.crdFile, err)
			}
			versions, _ := dig(crd, "spec", "versions").([]interface{})
			if len(versions) == 0 {
				t.Fatalf("%s: no versions", c.crdFile)
			}
			v0, _ := versions[0].(map[string]interface{})
			schema, _ := dig(v0, "schema", "openAPIV3Schema", "properties", c.schemaKey).(map[string]interface{})
			if schema == nil {
				if c.schemaKey == "status" {
					t.Skipf("%s: no status schema (optional)", c.crdFile)
				} else {
					t.Fatalf("%s: no %s schema; CRD layout may have changed", c.crdFile, c.schemaKey)
				}
			}
			checkSchemaVsType(t, c.schemaKey, schema, c.goType)
		})
	}
}

// TestEnumValuesMatchCRD verifies that every string enum value declared in any
// CRD schema appears as a constant assignment (= "value") in the generated
// types file. A missing value means a CRD enum was added or renamed without
// regenerating  -  run make generate-go-types.
func TestEnumValuesMatchCRD(t *testing.T) {
	const crdDir = "../../../deploy/crds"
	generatedBytes, err := os.ReadFile("zz_generated_types.go")
	if err != nil {
		t.Fatalf("read zz_generated_types.go: %v", err)
	}
	generated := string(generatedBytes)

	for _, f := range []string{
		"inferenceprovider.yaml",
		"gridnetwork.yaml",
		"gridsite.yaml",
		"agenttoolprovider.yaml",
		"siteinvite.yaml",
	} {
		data, err := os.ReadFile(crdDir + "/" + f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var crd map[string]interface{}
		if err := yaml.Unmarshal(data, &crd); err != nil {
			t.Fatalf("unmarshal %s: %v", f, err)
		}
		versions, _ := dig(crd, "spec", "versions").([]interface{})
		if len(versions) == 0 {
			t.Fatalf("%s: no versions; CRD layout may have changed", f)
		}
		v0, _ := versions[0].(map[string]interface{})
		root, _ := dig(v0, "schema", "openAPIV3Schema").(map[string]interface{})
		checkEnumValuesInGenerated(t, f, root, generated)
	}
}

// ---------------------------------------------------------------------------
// Test utilities
// ---------------------------------------------------------------------------

// checkSchemaVsType recursively verifies that every property in schema has a
// matching json tag on goType with a compatible Go kind and optionality,
// descending into nested object and array schemas. External types
// (metav1.Condition, etc.) are skipped.
func checkSchemaVsType(t *testing.T, path string, schema map[string]interface{}, goType reflect.Type) {
	t.Helper()
	for goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}
	if pkg := goType.PkgPath(); pkg != "" && !strings.HasPrefix(pkg, "github.com/praxis-proxy/grid/go") {
		return
	}
	if goType.Kind() != reflect.Struct {
		return
	}
	props, _ := schema["properties"].(map[string]interface{})
	if len(props) == 0 {
		return
	}
	requiredSet := map[string]bool{}
	if req, _ := schema["required"].([]interface{}); len(req) > 0 {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}
	tagToField := make(map[string]reflect.StructField, goType.NumField())
	for i := 0; i < goType.NumField(); i++ {
		f := goType.Field(i)
		if f.Anonymous {
			continue
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			tagToField[name] = f
		}
	}
	for propName, propVal := range props {
		propSchema, ok := propVal.(map[string]interface{})
		if !ok {
			continue
		}
		sf, exists := tagToField[propName]
		if !exists {
			t.Errorf("%s: CRD property %q not found in Go type %s", path, propName, goType.Name())
			continue
		}
		childPath := path + "." + propName
		childType := sf.Type
		isRequired := requiredSet[propName]
		_, hasDefault := propSchema["default"]
		isPtr := childType.Kind() == reflect.Ptr
		if isRequired && isPtr {
			t.Errorf("%s: required in CRD but Go type is pointer %s", childPath, childType)
		}
		// Optional object fields (structs, not maps) must be pointers so callers
		// can distinguish "field not present" from "field present with zero
		// values". Fields with CRD defaults or scalar types use omitempty
		// instead of a pointer  -  those are intentional generator choices.
		if !isRequired && !hasDefault {
			st, _ := propSchema["type"].(string)
			nestedProps, _ := propSchema["properties"].(map[string]interface{})
			_, hasAdditionalProps := propSchema["additionalProperties"]
			if st == "object" && len(nestedProps) > 0 && !hasAdditionalProps && !isPtr {
				t.Errorf("%s: optional object field but Go type is non-pointer %s", childPath, childType)
			}
		}
		checkGoTypeMatchesCRD(t, childPath, propSchema, childType)
		if nestedProps, _ := propSchema["properties"].(map[string]interface{}); len(nestedProps) > 0 {
			checkSchemaVsType(t, childPath, propSchema, childType)
		}
		if items, _ := propSchema["items"].(map[string]interface{}); len(items) > 0 {
			elem := childType
			for elem.Kind() == reflect.Slice || elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			checkGoTypeMatchesCRD(t, childPath+"[]", items, elem)
			if _, hasProps := items["properties"]; hasProps {
				checkSchemaVsType(t, childPath+"[]", items, elem)
			}
		}
	}
}

// checkGoTypeMatchesCRD verifies that the Go type's kind is compatible with the
// CRD schema's type field.
func checkGoTypeMatchesCRD(t *testing.T, path string, schema map[string]interface{}, goType reflect.Type) {
	t.Helper()
	underlying := goType
	for underlying.Kind() == reflect.Ptr {
		underlying = underlying.Elem()
	}
	schemaType, _ := schema["type"].(string)
	_, hasAdditionalProps := schema["additionalProperties"]
	props, _ := schema["properties"].(map[string]interface{})
	switch schemaType {
	case "string":
		if underlying.Kind() != reflect.String {
			t.Errorf("%s: CRD type string, Go type %s (want string kind)", path, goType)
		}
	case "integer":
		if underlying.Kind() != reflect.Int64 {
			t.Errorf("%s: CRD type integer, Go type %s (want int64)", path, goType)
		}
	case "number":
		if underlying.Kind() != reflect.Float64 {
			t.Errorf("%s: CRD type number, Go type %s (want float64)", path, goType)
		}
	case "boolean":
		if underlying.Kind() != reflect.Bool {
			t.Errorf("%s: CRD type boolean, Go type %s (want bool)", path, goType)
		}
	case "array":
		if underlying.Kind() != reflect.Slice {
			t.Errorf("%s: CRD type array, Go type %s (want slice)", path, goType)
		}
	case "object":
		if hasAdditionalProps && len(props) == 0 {
			if underlying.Kind() != reflect.Map {
				t.Errorf("%s: CRD map type, Go type %s (want map)", path, goType)
			}
		} else if len(props) > 0 {
			if underlying.Kind() != reflect.Struct {
				t.Errorf("%s: CRD object type, Go type %s (want struct)", path, goType)
			}
		}
	}
}

// checkEnumValuesInGenerated walks schema recursively and for every string enum
// field checks that each value appears as a constant assignment (= "value") in
// the generated types file.
func checkEnumValuesInGenerated(t *testing.T, crdFile string, schema map[string]interface{}, generated string) {
	t.Helper()
	if schema == nil {
		return
	}
	if st, _ := schema["type"].(string); st == "string" {
		if enum, _ := schema["enum"].([]interface{}); len(enum) > 0 {
			for _, v := range enum {
				s, ok := v.(string)
				if !ok {
					continue
				}
				if !strings.Contains(generated, `= "`+s+`"`) {
					t.Errorf("%s: CRD enum value %q has no matching constant in zz_generated_types.go", crdFile, s)
				}
			}
		}
	}
	if props, ok := schema["properties"].(map[string]interface{}); ok {
		for _, v := range props {
			if child, ok := v.(map[string]interface{}); ok {
				checkEnumValuesInGenerated(t, crdFile, child, generated)
			}
		}
	}
	if items, ok := schema["items"].(map[string]interface{}); ok {
		checkEnumValuesInGenerated(t, crdFile, items, generated)
	}
}

func dig(m map[string]interface{}, keys ...string) interface{} {
	var cur interface{} = m
	for _, k := range keys {
		mm, ok := cur.(map[string]interface{})
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}

// TestInferenceProviderFullRoundTrip loads the full InferenceProvider testdata
// YAML and verifies that optional and nested fields survive unmarshal.
// Strict decoding is used so any field in the fixture that has no corresponding
// json tag on the Go types (i.e. does not exist in the CRD schema) fails the test.
func TestInferenceProviderFullRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/inferenceprovider_full.yaml")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("yaml->json: %v", err)
	}
	var ip v1alpha1.InferenceProvider
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ip); err != nil {
		t.Fatalf("strict unmarshal: %v", err)
	}

	if ip.Spec.GridNetworkRef != "production" {
		t.Errorf("GridNetworkRef = %q, want production", ip.Spec.GridNetworkRef)
	}
	if ip.Spec.ProviderKind != "self_hosted" {
		t.Errorf("ProviderKind = %q", ip.Spec.ProviderKind)
	}
	if ip.Spec.Auth == nil || ip.Spec.Auth.Strategy != v1alpha1.AuthStrategyMTLSOnly {
		t.Errorf("Auth.Strategy not mtls_only")
	}
	if ip.Spec.TLS == nil || ip.Spec.TLS.CASecretRef == nil || ip.Spec.TLS.CASecretRef.Name != "grid-ca" {
		t.Errorf("TLS.CASecretRef missing or wrong")
	}
	if ip.Spec.HealthCheck == nil || ip.Spec.HealthCheck.Path == nil || *ip.Spec.HealthCheck.Path != "/healthz" {
		t.Errorf("HealthCheck.Path not preserved")
	}
	if ip.Spec.MetricsConfig == nil || ip.Spec.MetricsConfig.SignalNames == nil || ip.Spec.MetricsConfig.SignalNames.QueueDepth == nil {
		t.Errorf("MetricsConfig.SignalNames.QueueDepth missing")
	}
	if ip.Spec.ModelDiscovery == nil || ip.Spec.ModelDiscovery.OpenAIModels == nil {
		t.Errorf("ModelDiscovery.OpenAiModels missing")
	}
	if len(ip.Spec.Models) != 1 || ip.Spec.Models[0].Name != "meta-llama/Llama-3.1-8B-Instruct" {
		t.Errorf("Models not preserved")
	}
	if ip.Spec.TrafficPolicy == nil || !ip.Spec.TrafficPolicy.Drain {
		t.Errorf("TrafficPolicy.Drain not true")
	}
	if ip.Spec.Cost == nil || ip.Spec.Cost.PerMillionInputTokens != 0.15 {
		t.Errorf("Cost not preserved")
	}

	// Round-trip through JSON to confirm fields survive a marshal/unmarshal cycle.
	b, err := json.Marshal(&ip)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var ip2 v1alpha1.InferenceProvider
	dec2 := json.NewDecoder(bytes.NewReader(b))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&ip2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !reflect.DeepEqual(ip, ip2) {
		t.Errorf("round-trip produced different object:\n  got  %+v\n  want %+v", ip2, ip)
	}
}

// TestGridNetworkRoundTrip loads the basic GridNetwork testdata fixture and
// verifies that the key fields survive a strict unmarshal/marshal cycle.
func TestGridNetworkRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/gridnetwork_basic.yaml")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("yaml->json: %v", err)
	}
	var gn v1alpha1.GridNetwork
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&gn); err != nil {
		t.Fatalf("strict unmarshal: %v", err)
	}
	if gn.Name != "production" {
		t.Errorf("Name = %q, want production", gn.Name)
	}
	if len(gn.Spec.Seeds) != 2 {
		t.Errorf("Seeds len = %d, want 2", len(gn.Spec.Seeds))
	}
	if gn.Spec.SelectionPolicy == nil || gn.Spec.SelectionPolicy.Mode != v1alpha1.SelectionModeRoundRobin {
		t.Errorf("SelectionPolicy.Mode not roundRobin")
	}
	if gn.Spec.ScoringPolicy == nil || gn.Spec.ScoringPolicy.Strategy != v1alpha1.ScoringStrategyQueueDepth {
		t.Errorf("ScoringPolicy.Strategy not queueDepth")
	}
	if gn.Spec.PeerTrust == nil || gn.Spec.PeerTrust.Mode != v1alpha1.PeerTrustModePin {
		t.Errorf("PeerTrust.Mode not pin")
	}
	if gn.Spec.SWIM == nil || gn.Spec.SWIM.ProbeInterval == nil || *gn.Spec.SWIM.ProbeInterval != "5s" {
		t.Errorf("SWIM.ProbeInterval not preserved")
	}
	// gossipNodes has a non-zero CRD default (3), so it must be *int64 to let
	// a client send 0 without omitempty silently dropping it to the default.
	if gn.Spec.SWIM == nil || gn.Spec.SWIM.GossipNodes == nil || *gn.Spec.SWIM.GossipNodes != 5 {
		t.Errorf("SWIM.GossipNodes not preserved as pointer")
	}
	if gn.Spec.TLS == nil || gn.Spec.TLS.CASecretRef == nil || gn.Spec.TLS.CASecretRef.Name != "grid-ca" {
		t.Errorf("TLS.CASecretRef not preserved")
	}
	b, err := json.Marshal(&gn)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var gn2 v1alpha1.GridNetwork
	dec2 := json.NewDecoder(bytes.NewReader(b))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&gn2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !reflect.DeepEqual(gn, gn2) {
		t.Errorf("round-trip produced different object:\n  got  %+v\n  want %+v", gn2, gn)
	}
}

// TestGossipNodesZeroPreserved verifies that gossipNodes:0 survives a
// marshal/unmarshal cycle without being swallowed by omitempty. The field has
// a non-zero CRD default (3), so the generator must emit *int64, not int64.
func TestGossipNodesZeroPreserved(t *testing.T) {
	zero := int64(0)
	gn := v1alpha1.GridNetwork{
		Spec: v1alpha1.GridNetworkSpec{
			SWIM: &v1alpha1.GridNetworkSWIM{GossipNodes: &zero},
		},
	}
	b, err := json.Marshal(&gn)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var gn2 v1alpha1.GridNetwork
	if err := json.Unmarshal(b, &gn2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if gn2.Spec.SWIM == nil || gn2.Spec.SWIM.GossipNodes == nil {
		t.Fatal("SWIM.GossipNodes is nil after round-trip; zero was dropped by omitempty")
	}
	if *gn2.Spec.SWIM.GossipNodes != 0 {
		t.Errorf("SWIM.GossipNodes = %d after round-trip, want 0", *gn2.Spec.SWIM.GossipNodes)
	}
}

// TestGridSiteRoundTrip loads the basic GridSite testdata fixture and verifies
// that required and optional fields survive a strict unmarshal/marshal cycle.
func TestGridSiteRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/gridsite_basic.yaml")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("yaml->json: %v", err)
	}
	var gs v1alpha1.GridSite
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&gs); err != nil {
		t.Fatalf("strict unmarshal: %v", err)
	}
	if gs.Spec.GridNetworkRef != "production" {
		t.Errorf("GridNetworkRef = %q, want production", gs.Spec.GridNetworkRef)
	}
	if gs.Spec.Egress == nil || gs.Spec.Egress.Address != "site-a.example.com:443" {
		t.Errorf("Egress.Address not preserved")
	}
	if gs.Spec.Region == nil || *gs.Spec.Region != "us-east-1" {
		t.Errorf("Region not preserved")
	}
	if gs.Spec.Zone == nil || *gs.Spec.Zone != "us-east-1a" {
		t.Errorf("Zone not preserved")
	}
	b, err := json.Marshal(&gs)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var gs2 v1alpha1.GridSite
	dec2 := json.NewDecoder(bytes.NewReader(b))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&gs2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !reflect.DeepEqual(gs, gs2) {
		t.Errorf("round-trip produced different object:\n  got  %+v\n  want %+v", gs2, gs)
	}
}

// TestAgentToolProviderRoundTrip loads the basic AgentToolProvider testdata
// fixture and verifies that required and optional fields survive a strict
// unmarshal/marshal cycle.
func TestAgentToolProviderRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/agenttoolprovider_basic.yaml")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("yaml->json: %v", err)
	}
	var atp v1alpha1.AgentToolProvider
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&atp); err != nil {
		t.Fatalf("strict unmarshal: %v", err)
	}
	if atp.Spec.GridNetworkRef != "production" {
		t.Errorf("GridNetworkRef = %q, want production", atp.Spec.GridNetworkRef)
	}
	if atp.Spec.Endpoint != "https://tools.example.com" {
		t.Errorf("Endpoint = %q", atp.Spec.Endpoint)
	}
	if atp.Spec.Auth == nil || atp.Spec.Auth.Strategy != v1alpha1.AuthStrategyBearerToken {
		t.Errorf("Auth.Strategy not bearer_token")
	}
	if atp.Spec.Protocol == nil || *atp.Spec.Protocol != "mcp" {
		t.Errorf("Protocol = %v, want mcp", atp.Spec.Protocol)
	}
	if len(atp.Spec.Tools) != 2 {
		t.Fatalf("Tools len = %d, want 2", len(atp.Spec.Tools))
	}
	if atp.Spec.Tools[0].Name != "web_search" {
		t.Errorf("Tools[0].Name = %q, want web_search", atp.Spec.Tools[0].Name)
	}
	if atp.Spec.Tools[0].Description == nil || *atp.Spec.Tools[0].Description != "Search the web for current information" {
		t.Errorf("Tools[0].Description not preserved")
	}
	b, err := json.Marshal(&atp)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var atp2 v1alpha1.AgentToolProvider
	dec2 := json.NewDecoder(bytes.NewReader(b))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&atp2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !reflect.DeepEqual(atp, atp2) {
		t.Errorf("round-trip produced different object:\n  got  %+v\n  want %+v", atp2, atp)
	}
}

// TestSiteInviteRoundTrip loads the basic SiteInvite testdata fixture and
// verifies that required fields survive a strict unmarshal/marshal cycle.
func TestSiteInviteRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/siteinvite_basic.yaml")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	jsonBytes, err := yaml.YAMLToJSON(data)
	if err != nil {
		t.Fatalf("yaml->json: %v", err)
	}
	var si v1alpha1.SiteInvite
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&si); err != nil {
		t.Fatalf("strict unmarshal: %v", err)
	}
	if si.Spec.SiteName != "site-b" {
		t.Errorf("SiteName = %q, want site-b", si.Spec.SiteName)
	}
	if len(si.Spec.Jurisdictions) != 1 || si.Spec.Jurisdictions[0] != "us-east" {
		t.Errorf("Jurisdictions = %v, want [us-east]", si.Spec.Jurisdictions)
	}
	if si.Spec.Lifetime == nil || *si.Spec.Lifetime != "24h" {
		t.Errorf("Lifetime = %v, want 24h", si.Spec.Lifetime)
	}
	b, err := json.Marshal(&si)
	if err != nil {
		t.Fatalf("remarshal: %v", err)
	}
	var si2 v1alpha1.SiteInvite
	dec2 := json.NewDecoder(bytes.NewReader(b))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&si2); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if !reflect.DeepEqual(si, si2) {
		t.Errorf("round-trip produced different object:\n  got  %+v\n  want %+v", si2, si)
	}
}

// TestDeepCopyIsolation verifies that DeepCopy does not share nested pointer
// or slice memory with the original.
func TestDeepCopyIsolation(t *testing.T) {
	secretName := "original-secret"
	modelName := "original-model"
	sig := "vllm_queue_depth"
	ip := v1alpha1.InferenceProvider{
		Spec: v1alpha1.InferenceProviderSpec{
			GridNetworkRef: "net",
			Auth: &v1alpha1.AuthConfig{
				SecretRef: &v1alpha1.ObjectKeyRef{Name: secretName},
			},
			Models: []v1alpha1.ModelInfo{{Name: modelName}},
			MetricsConfig: &v1alpha1.MetricsConfig{
				SignalNames: &v1alpha1.MetricSignalNames{QueueDepth: &sig},
			},
		},
	}
	cp := ip.DeepCopy()

	ip.Spec.Auth.SecretRef.Name = "mutated"
	if cp.Spec.Auth.SecretRef.Name != secretName {
		t.Error("Auth.SecretRef.Name: copy sees mutation in original")
	}
	ip.Spec.Models[0].Name = "mutated"
	if cp.Spec.Models[0].Name != modelName {
		t.Error("Models[0].Name: copy sees mutation in original")
	}
	*ip.Spec.MetricsConfig.SignalNames.QueueDepth = "mutated"
	if *cp.Spec.MetricsConfig.SignalNames.QueueDepth == "mutated" {
		t.Error("MetricsConfig.SignalNames.QueueDepth: copy sees mutation in original")
	}
}

// TestMetricsPathEmptyPreserved verifies that path: "" (scrape the endpoint
// root) survives a marshal/unmarshal round-trip. The field defaults to
// "/metrics" server-side, so the Go type must be *string so that omitempty
// does not drop an explicit empty string and trigger the default.
func TestMetricsPathEmptyPreserved(t *testing.T) {
	emptyPath := ""
	metricsEndpoint := "https://llmd.svc:9443"
	ip := v1alpha1.InferenceProvider{
		Spec: v1alpha1.InferenceProviderSpec{
			GridNetworkRef: "net",
			MetricsConfig: &v1alpha1.MetricsConfig{
				MetricsEndpoint: &metricsEndpoint,
				Path:            &emptyPath,
			},
		},
	}
	b, err := json.Marshal(&ip)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"path":""`) {
		t.Errorf("empty path dropped by omitempty: %s", b)
	}
	var ip2 v1alpha1.InferenceProvider
	if err := json.Unmarshal(b, &ip2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ip2.Spec.MetricsConfig == nil || ip2.Spec.MetricsConfig.Path == nil || *ip2.Spec.MetricsConfig.Path != "" {
		t.Errorf("round-trip lost empty path: %+v", ip2.Spec.MetricsConfig)
	}
}
