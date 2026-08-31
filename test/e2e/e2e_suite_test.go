//go:build e2e

/*
Copyright 2024 egonlin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	e2eOptInEnv        = "MYSQL_OPERATOR_E2E"
	e2eContextEnv      = "MYSQL_OPERATOR_E2E_CONTEXT"
	forbiddenOpsLab    = "kubernetes-admin@opslab-k8s"
	defaultKindCluster = "kind"
)

func requireExplicitE2E(t *testing.T) {
	t.Helper()

	if strings.TrimSpace(os.Getenv(e2eOptInEnv)) != "1" {
		t.Fatalf(
			"E2E disabled: set %s=1 only for an explicitly isolated E2E environment",
			e2eOptInEnv,
		)
	}

	expectedContext := strings.TrimSpace(os.Getenv(e2eContextEnv))
	if expectedContext == "" {
		t.Fatalf(
			"E2E disabled: %s must explicitly name the allowed Kubernetes context",
			e2eContextEnv,
		)
	}

	output, err := exec.Command("kubectl", "config", "current-context").CombinedOutput()
	if err != nil {
		t.Fatalf(
			"E2E disabled: cannot determine current Kubernetes context: %v: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}

	currentContext := strings.TrimSpace(string(output))

	if currentContext == forbiddenOpsLab || strings.Contains(currentContext, "opslab-k8s") {
		t.Fatalf(
			"E2E denied: protected OpsLab context %q must never be used for mysql-operator E2E",
			currentContext,
		)
	}

	if currentContext != expectedContext {
		t.Fatalf(
			"E2E denied: current context %q does not match explicitly allowed context %q",
			currentContext,
			expectedContext,
		)
	}

	kindCluster := strings.TrimSpace(os.Getenv("KIND_CLUSTER"))
	if kindCluster == "" {
		kindCluster = defaultKindCluster
	}

	requiredKindContext := "kind-" + kindCluster
	if currentContext != requiredKindContext {
		t.Fatalf(
			"E2E denied: current E2E implementation requires dedicated Kind context %q, got %q",
			requiredKindContext,
			currentContext,
		)
	}
}

// Run e2e tests using the Ginkgo runner.
func TestE2E(t *testing.T) {
	requireExplicitE2E(t)

	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting mysql-operator suite\n")
	RunSpecs(t, "e2e suite")
}
