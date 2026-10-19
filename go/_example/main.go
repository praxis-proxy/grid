// Example shows how to construct an InferenceProvider and register the Grid
// API types with a runtime scheme (as you would with controller-runtime).
package main

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	v1alpha1 "github.com/praxis-proxy/grid/go/api/v1alpha1"
)

func main() {
	// Register Grid types with a scheme.
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	secretKey := "token"
	secretName := "anthropic-key"
	secretNamespace := "grid-system"

	provider := &v1alpha1.InferenceProvider{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
			Kind:       "InferenceProvider",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "anthropic-production",
		},
		Spec: v1alpha1.InferenceProviderSpec{
			GridNetworkRef: "production",
			ProviderKind:   "anthropic",
			BackendKind:    "api_provider",
			Endpoint:       "https://api.anthropic.com",
			Auth: &v1alpha1.AuthConfig{
				Strategy: v1alpha1.AuthStrategyBearerToken,
				SecretRef: &v1alpha1.ObjectKeyRef{
					Name:      secretName,
					Namespace: secretNamespace,
					Key:       &secretKey,
				},
			},
			Models: []v1alpha1.ModelInfo{
				{Name: "claude-sonnet-4"},
			},
		},
	}

	b, err := json.MarshalIndent(provider, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}
