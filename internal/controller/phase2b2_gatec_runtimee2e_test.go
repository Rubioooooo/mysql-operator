//go:build runtimee2e

package controller

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetes "k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	phase2B2GateCRuntimeEnableEnvironment = "PHASE2B2_GATE_C_RUNTIME"
	phase2B2GateCExpectedErrorEnvironment = "PHASE2B2_GATE_C_EXPECT_ERROR_SUBSTRING"
	phase2B2GateCNamespace                = "phase2b2-runtime"
	phase2B2GateCName                     = "phase2b2-regression"
	phase2B2GateCMaxStderrBytes           = 1024
)

var phase2B2GateCTarget = types.NamespacedName{
	Namespace: phase2B2GateCNamespace,
	Name:      phase2B2GateCName,
}

func TestPhase2B2GateCRuntimeOneShot(t *testing.T) {
	if os.Getenv(phase2B2GateCRuntimeEnableEnvironment) != "1" {
		t.Skipf("set %s=1 with the runtimee2e build tag to enable the real-cluster one-shot reconciliation", phase2B2GateCRuntimeEnableEnvironment)
	}
	if phase2B2GateCTarget.Namespace != "phase2b2-runtime" || phase2B2GateCTarget.Name != "phase2b2-regression" {
		t.Fatalf("refusing non-authorized Gate C target %s", phase2B2GateCTarget)
	}

	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		t.Fatal("KUBECONFIG must name the namespace-restricted Gate C credential file")
	}
	restConfig, err := phase2B2GateCRestConfig(kubeconfigPath)
	if err != nil {
		t.Fatalf("build Gate C REST config: %v", err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("register Kubernetes API scheme: %v", err)
	}
	if err := databasev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register MysqlCluster API scheme: %v", err)
	}
	directClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("construct direct Gate C Kubernetes client: %v", err)
	}
	podExec, err := phase2B2GateCPodExec(restConfig)
	if err != nil {
		t.Fatalf("construct Gate C Pod executor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	fmt.Printf("GATEC_TARGET_NAMESPACE=%s\n", phase2B2GateCTarget.Namespace)
	fmt.Printf("GATEC_TARGET_NAME=%s\n", phase2B2GateCTarget.Name)
	if err := phase2B2GateCPrintState(ctx, directClient, "BEFORE"); err != nil {
		t.Fatalf("read Gate C state before reconciliation: %v", err)
	}

	// SnapGoIsEnabled prevents a one-shot invocation from starting the
	// process-local periodic GTID snapshot goroutine after Reconcile succeeds.
	reconciler := &MysqlClusterReconciler{
		Client:             directClient,
		Scheme:             scheme,
		SnapGoIsEnabled:    true,
		execCommandOnPodFn: podExec,
	}

	// This is the only Reconcile call in the runtime harness. Shell-driven Gate
	// C sequencing deliberately invokes a new test process for every boundary.
	result, reconcileErr := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: phase2B2GateCTarget})
	fmt.Printf("RECONCILE_RESULT=requeue=%t,requeueAfter=%s\n", result.Requeue, result.RequeueAfter)
	if reconcileErr == nil {
		fmt.Println("RECONCILE_ERROR=NONE")
	} else {
		fmt.Printf("RECONCILE_ERROR=%s\n", phase2B2GateCOneLine(reconcileErr.Error()))
	}

	if err := phase2B2GateCPrintState(ctx, directClient, "AFTER"); err != nil {
		t.Fatalf("read Gate C state after reconciliation: %v", err)
	}

	expectedError := os.Getenv(phase2B2GateCExpectedErrorEnvironment)
	switch {
	case expectedError == "" && reconcileErr != nil:
		t.Fatalf("unexpected reconciliation error: %v", reconcileErr)
	case expectedError != "" && reconcileErr == nil:
		t.Fatalf("expected reconciliation error containing %q, got nil", expectedError)
	case expectedError != "" && !strings.Contains(reconcileErr.Error(), expectedError):
		t.Fatalf("expected reconciliation error containing %q, got %q", expectedError, reconcileErr.Error())
	}
}

func TestPhase2B2GateCStderrSanitization(t *testing.T) {
	t.Run("normalizes useful stderr to one bounded line", func(t *testing.T) {
		diagnostic := phase2B2GateCSanitizeStderr("ERROR 1201 (HY000):\nCould not initialize master info structure")
		if strings.ContainsAny(diagnostic, "\r\n") {
			t.Fatalf("stderr diagnostic is not one line: %q", diagnostic)
		}
		if !strings.Contains(diagnostic, "Could not initialize master info structure") {
			t.Fatalf("stderr diagnostic lost useful content: %q", diagnostic)
		}

		longDiagnostic := phase2B2GateCSanitizeStderr(strings.Repeat("x", phase2B2GateCMaxStderrBytes+100))
		if !strings.HasSuffix(longDiagnostic, "...[truncated]") {
			t.Fatalf("long stderr was not marked truncated: %q", longDiagnostic)
		}
		if len(longDiagnostic) > phase2B2GateCMaxStderrBytes+len("...[truncated]") {
			t.Fatalf("stderr diagnostic exceeded its bound: %d", len(longDiagnostic))
		}
	})

	t.Run("redacts stderr that appears to contain command or password material", func(t *testing.T) {
		for _, value := range []string{
			"MYSQL_ROOT_PASSWORD=do-not-print",
			"MYSQL_REPLICATION_PASSWORD=do-not-print",
			"MASTER_PASSWORD='do-not-print'",
			"replication_password_sql=do-not-print",
		} {
			if diagnostic := phase2B2GateCSanitizeStderr(value); diagnostic != "[REDACTED_SENSITIVE_STDERR]" {
				t.Fatalf("sensitive stderr was not redacted: %q", diagnostic)
			}
		}
	})
}

func phase2B2GateCRestConfig(kubeconfigPath string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	)
	restConfig, err := config.ClientConfig()
	if err != nil {
		return nil, err
	}
	restConfig.UserAgent = "mysql-operator-phase2b2-gatec-one-shot"
	return restConfig, nil
}

func phase2B2GateCPodExec(restConfig *rest.Config) (func(*corev1.Pod, string) (string, error), error) {
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	restClient := kubeClient.CoreV1().RESTClient()

	return func(pod *corev1.Pod, command string) (string, error) {
		if pod.Namespace != phase2B2GateCNamespace {
			return "", fmt.Errorf("refusing Pod exec outside Gate C namespace: %s/%s", pod.Namespace, pod.Name)
		}
		if len(pod.Spec.Containers) == 0 {
			return "", fmt.Errorf("Pod %s/%s has no containers", pod.Namespace, pod.Name)
		}

		req := restClient.
			Post().
			Resource("pods").
			Name(pod.Name).
			Namespace(phase2B2GateCNamespace).
			SubResource("exec").
			Param("stdin", "false").
			Param("stdout", "true").
			Param("stderr", "true").
			Param("tty", "false").
			Param("container", pod.Spec.Containers[0].Name).
			Param("command", "/bin/sh").
			Param("command", "-c").
			Param("command", command)
		executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
		if err != nil {
			return "", err
		}

		var stdout strings.Builder
		var stderr strings.Builder
		if err := executor.Stream(remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
			stderrDiagnostic := phase2B2GateCSanitizeStderr(stderr.String())
			if stderrDiagnostic != "" {
				return "", fmt.Errorf("Pod exec failed: %w; stderr=%s", err, stderrDiagnostic)
			}
			return "", fmt.Errorf("Pod exec failed: %w", err)
		}
		return stdout.String(), nil
	}, nil
}

func phase2B2GateCPrintState(ctx context.Context, directClient client.Client, stage string) error {
	cluster := &databasev1.MysqlCluster{}
	if err := directClient.Get(ctx, phase2B2GateCTarget, cluster); err != nil {
		return err
	}

	fmt.Printf("GATEC_STATE_STAGE=%s\n", stage)
	fmt.Printf("MYSQLCLUSTER_GENERATION=%d\n", cluster.Generation)
	fmt.Printf("MYSQLCLUSTER_SPEC_REPLICAS=%s\n", phase2B2GateCInt32Pointer(cluster.Spec.Replicas))
	initialized, found := cluster.Annotations[mysqlClusterInitializedAnnotation]
	if !found {
		initialized = "ABSENT"
	}
	fmt.Printf("MYSQLCLUSTER_INITIALIZED=%s\n", phase2B2GateCOneLine(initialized))
	fmt.Printf("MYSQLCLUSTER_LAST_CONVERGED_REPLICAS=%s\n", phase2B2GateCInt32Pointer(cluster.Status.LastConvergedReplicas))
	if cluster.Status.ReplicaTransition == nil {
		fmt.Println("MYSQLCLUSTER_REPLICA_TRANSITION=NONE")
	} else {
		fmt.Printf(
			"MYSQLCLUSTER_REPLICA_TRANSITION=from:%d,target:%d\n",
			cluster.Status.ReplicaTransition.FromReplicas,
			cluster.Status.ReplicaTransition.TargetReplicas,
		)
	}
	if cluster.Status.CredentialsSecretUID == "" {
		fmt.Println("MYSQLCLUSTER_CREDENTIALS_SECRET_UID=ABSENT")
	} else {
		fmt.Printf("MYSQLCLUSTER_CREDENTIALS_SECRET_UID=%s\n", phase2B2GateCOneLine(cluster.Status.CredentialsSecretUID))
	}

	statefulSet := &appsv1.StatefulSet{}
	statefulSetKey := types.NamespacedName{
		Namespace: phase2B2GateCNamespace,
		Name:      mysqlStatefulSetName(cluster),
	}
	if err := directClient.Get(ctx, statefulSetKey, statefulSet); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return err
		}
		fmt.Println("STATEFULSET_PRESENT=false")
	} else {
		fmt.Println("STATEFULSET_PRESENT=true")
		fmt.Printf("STATEFULSET_UID=%s\n", statefulSet.UID)
		fmt.Printf("STATEFULSET_SPEC_REPLICAS=%s\n", phase2B2GateCInt32Pointer(statefulSet.Spec.Replicas))
		fmt.Printf("STATEFULSET_STATUS_REPLICAS=%d\n", statefulSet.Status.Replicas)
		fmt.Printf("STATEFULSET_STATUS_READY_REPLICAS=%d\n", statefulSet.Status.ReadyReplicas)
	}

	pods := &corev1.PodList{}
	if err := directClient.List(
		ctx,
		pods,
		client.InNamespace(phase2B2GateCNamespace),
		client.MatchingLabels{LabelAppInstance: string(cluster.UID)},
	); err != nil {
		return err
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	fmt.Printf("MANAGED_POD_COUNT=%d\n", len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		prefix := fmt.Sprintf("MANAGED_POD_%d", i)
		fmt.Printf("%s_NAME=%s\n", prefix, pod.Name)
		fmt.Printf("%s_UID=%s\n", prefix, pod.UID)
		fmt.Printf("%s_POD_INDEX=%s\n", prefix, phase2B2GateCOneLine(pod.Labels[statefulSetPodIndexLabel]))
		fmt.Printf("%s_ROLE=%s\n", prefix, phase2B2GateCOneLine(pod.Labels[LabelMysqlRole]))
		fmt.Printf("%s_PHASE=%s\n", prefix, pod.Status.Phase)
		fmt.Printf("%s_MYSQL_READY=%t\n", prefix, phase2B2GateCMysqlReady(pod))
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := directClient.List(
		ctx,
		pvcs,
		client.InNamespace(phase2B2GateCNamespace),
		client.MatchingLabels{LabelAppInstance: string(cluster.UID)},
	); err != nil {
		return err
	}
	sort.Slice(pvcs.Items, func(i, j int) bool { return pvcs.Items[i].Name < pvcs.Items[j].Name })
	fmt.Printf("MANAGED_PVC_COUNT=%d\n", len(pvcs.Items))
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		prefix := fmt.Sprintf("MANAGED_PVC_%d", i)
		fmt.Printf("%s_NAME=%s\n", prefix, pvc.Name)
		fmt.Printf("%s_UID=%s\n", prefix, pvc.UID)
		fmt.Printf("%s_PHASE=%s\n", prefix, pvc.Status.Phase)
		fmt.Printf("%s_VOLUME_NAME=%s\n", prefix, phase2B2GateCOneLine(pvc.Spec.VolumeName))
	}
	return nil
}

func phase2B2GateCMysqlReady(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name == mysqlContainerName {
			return status.Ready
		}
	}
	return false
}

func phase2B2GateCInt32Pointer(value *int32) string {
	if value == nil {
		return "ABSENT"
	}
	return fmt.Sprintf("%d", *value)
}

func phase2B2GateCOneLine(value string) string {
	value = strings.ReplaceAll(value, "\r", `\r`)
	return strings.ReplaceAll(value, "\n", `\n`)
}

func phase2B2GateCSanitizeStderr(value string) string {
	diagnostic := strings.Join(strings.Fields(value), " ")
	if diagnostic == "" {
		return ""
	}
	for _, sensitiveMarker := range []string{
		"MYSQL_ROOT_PASSWORD",
		"MYSQL_REPLICATION_PASSWORD",
		"MASTER_PASSWORD",
		"replication_password_sql",
	} {
		if strings.Contains(diagnostic, sensitiveMarker) {
			return "[REDACTED_SENSITIVE_STDERR]"
		}
	}
	if len(diagnostic) > phase2B2GateCMaxStderrBytes {
		diagnostic = diagnostic[:phase2B2GateCMaxStderrBytes] + "...[truncated]"
	}
	return diagnostic
}
