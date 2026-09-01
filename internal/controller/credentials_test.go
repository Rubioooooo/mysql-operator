package controller

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func immutableCredentialSecret(cluster *databasev1.MysqlCluster, rootPassword, replicationPassword []byte) *corev1.Secret {
	immutable := true
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Spec.CredentialsSecretName,
			Namespace: cluster.Namespace,
			UID:       types.UID("credential-secret-uid"),
		},
		Immutable: &immutable,
		Data: map[string][]byte{
			mysqlRootPasswordSecretKey:        append([]byte(nil), rootPassword...),
			mysqlReplicationPasswordSecretKey: append([]byte(nil), replicationPassword...),
		},
	}
}

func credentialTestCluster(name, namespace string) *databasev1.MysqlCluster {
	cluster := statefulSetResourceTestCluster(name, types.UID(name+"-uid"))
	cluster.Namespace = namespace
	cluster.Spec.MasterService = name + "-primary"
	cluster.Spec.SlaveService = name + "-replica"
	return cluster
}

func TestMysqlCredentialValidationAndIdentityPin(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects a missing Secret", func(t *testing.T) {
		g := NewWithT(t)
		cluster := credentialTestCluster("credentials-missing", "mysql-system")
		reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster)
		g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(MatchError(ContainSubstring("does not exist")))
	})

	t.Run("rejects mutable Secrets", func(t *testing.T) {
		for _, immutable := range []*bool{nil, func() *bool { value := false; return &value }()} {
			g := NewWithT(t)
			cluster := credentialTestCluster("credentials-mutable", "mysql-system")
			secret := immutableCredentialSecret(cluster, []byte("root-valid"), []byte("replica-valid"))
			secret.Immutable = immutable
			reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
			g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(MatchError(ContainSubstring("must set immutable: true")))
		}
	})

	t.Run("rejects missing required keys", func(t *testing.T) {
		for _, missingKey := range []string{mysqlRootPasswordSecretKey, mysqlReplicationPasswordSecretKey} {
			g := NewWithT(t)
			cluster := credentialTestCluster("credentials-missing-key", "mysql-system")
			secret := immutableCredentialSecret(cluster, []byte("root-valid"), []byte("replica-valid"))
			delete(secret.Data, missingKey)
			reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
			g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(MatchError(ContainSubstring("missing required key " + missingKey)))
		}
	})

	t.Run("rejects values unsafe for environment and SQL transport", func(t *testing.T) {
		testCases := []struct {
			name  string
			key   string
			value []byte
		}{
			{name: "empty-root", key: mysqlRootPasswordSecretKey, value: nil},
			{name: "empty-replication", key: mysqlReplicationPasswordSecretKey, value: nil},
			{name: "nul", key: mysqlRootPasswordSecretKey, value: []byte("bad\x00value")},
			{name: "cr", key: mysqlReplicationPasswordSecretKey, value: []byte("bad\rvalue")},
			{name: "lf", key: mysqlReplicationPasswordSecretKey, value: []byte("bad\nvalue")},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				cluster := credentialTestCluster("credentials-invalid-"+testCase.name, "mysql-system")
				secret := immutableCredentialSecret(cluster, []byte("root-valid"), []byte("replica-valid"))
				secret.Data[testCase.key] = testCase.value
				reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
				err := reconciler.ensureMysqlCredentials(ctx, cluster)
				g.Expect(err).To(HaveOccurred())
				if len(testCase.value) > 0 {
					g.Expect(err.Error()).NotTo(ContainSubstring(string(testCase.value)))
				}
			})
		}
	})

	t.Run("accepts an externally owned immutable Secret without mutation or adoption", func(t *testing.T) {
		g := NewWithT(t)
		cluster := credentialTestCluster("credentials-external", "mysql-system")
		secret := immutableCredentialSecret(cluster, []byte("root-'\\-$-valid"), []byte("replica-'\\-$-valid"))
		controller := true
		secret.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "external.example/v1", Kind: "CredentialSource", Name: "external-owner",
			UID: types.UID("external-owner-uid"), Controller: &controller,
		}}
		reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
		before := &corev1.Secret{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(secret), before)).To(Succeed())

		g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(Succeed())
		g.Expect(cluster.Status.CredentialsSecretUID).To(Equal(string(secret.UID)))
		storedSecret := &corev1.Secret{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(secret), storedSecret)).To(Succeed())
		g.Expect(storedSecret).To(Equal(before))
		g.Expect(metav1.IsControlledBy(storedSecret, cluster)).To(BeFalse())
	})

	t.Run("pins once, preserves status, accepts the same UID, and rejects recreated Secrets", func(t *testing.T) {
		g := NewWithT(t)
		cluster := credentialTestCluster("credentials-identity", "mysql-system")
		cluster.Status.Phase = databasev1.MysqlClusterPhaseRunning
		cluster.Status.Primary = "mysql-primary-1"
		cluster.Status.CurrentReplicas = 3
		cluster.Status.ReadyReplicas = 2
		rootPassword := []byte("known-root-test-value")
		replicationPassword := []byte("known-replication-test-value")
		secret := immutableCredentialSecret(cluster, rootPassword, replicationPassword)
		secret.UID = types.UID("credential-secret-uid-a")
		reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
		memoryClient := reconciler.Client.(*statefulSetReconcileMemoryClient)

		g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(Succeed())
		g.Expect(cluster.Status.CredentialsSecretUID).To(Equal("credential-secret-uid-a"))
		g.Expect(memoryClient.statusPatchCount).To(Equal(1))
		storedCluster := &databasev1.MysqlCluster{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(cluster), storedCluster)).To(Succeed())
		g.Expect(storedCluster.Status.CredentialsSecretUID).To(Equal("credential-secret-uid-a"))
		g.Expect(storedCluster.Status.Phase).To(Equal(databasev1.MysqlClusterPhaseRunning))
		g.Expect(storedCluster.Status.Primary).To(Equal("mysql-primary-1"))
		g.Expect(storedCluster.Status.CurrentReplicas).To(Equal(int32(3)))
		g.Expect(storedCluster.Status.ReadyReplicas).To(Equal(int32(2)))

		g.Expect(reconciler.ensureMysqlCredentials(ctx, cluster)).To(Succeed())
		g.Expect(memoryClient.statusPatchCount).To(Equal(1))

		recreated := immutableCredentialSecret(cluster, rootPassword, replicationPassword)
		recreated.UID = types.UID("credential-secret-uid-b")
		recreated.ResourceVersion = "2"
		memoryClient.objects[memoryClient.objectKey(recreated)] = recreated.DeepCopy()
		err := reconciler.ensureMysqlCredentials(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("UID does not match")))
		g.Expect(err.Error()).NotTo(ContainSubstring(string(rootPassword)))
		g.Expect(err.Error()).NotTo(ContainSubstring(string(replicationPassword)))

		changed := immutableCredentialSecret(cluster, rootPassword, []byte("different-replication-value"))
		changed.UID = types.UID("credential-secret-uid-c")
		changed.ResourceVersion = "3"
		memoryClient.objects[memoryClient.objectKey(changed)] = changed.DeepCopy()
		err = reconciler.ensureMysqlCredentials(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("UID does not match")))
		g.Expect(err.Error()).NotTo(ContainSubstring(string(rootPassword)))
		g.Expect(err.Error()).NotTo(ContainSubstring("different-replication-value"))
	})

	t.Run("returns a status pin failure before creating routing or workload resources", func(t *testing.T) {
		g := NewWithT(t)
		cluster := credentialTestCluster("credentials-order", "mysql-system")
		secret := immutableCredentialSecret(cluster, []byte("root-valid"), []byte("replication-valid"))
		reconciler := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), cluster, secret)
		reconciler.Client.(*statefulSetReconcileMemoryClient).statusPatchError = errors.New("status patch conflict")

		_, complete, err := reconciler.reconcileStatefulSetInitialization(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("failed to pin credential Secret UID")))
		g.Expect(err).To(MatchError(ContainSubstring("status patch conflict")))
		g.Expect(complete).To(BeFalse())
		for _, object := range []client.Object{
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: mysqlSharedConfigMapName(cluster), Namespace: cluster.Namespace}},
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: mysqlStatefulSetName(cluster), Namespace: cluster.Namespace}},
		} {
			getErr := reconciler.Get(ctx, client.ObjectKeyFromObject(object), object)
			g.Expect(apierrors.IsNotFound(getErr)).To(BeTrue(), "unexpected object %T: %v", object, getErr)
		}
	})
}

func TestCredentialSecretWatchMapping(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared-credentials", Namespace: "team-a"}}
	clusterA := credentialTestCluster("cluster-a", "team-a")
	clusterA.Spec.CredentialsSecretName = secret.Name
	clusterB := credentialTestCluster("cluster-b", "team-a")
	clusterB.Spec.CredentialsSecretName = "unrelated-credentials"
	clusterC := credentialTestCluster("cluster-c", "team-b")
	clusterC.Spec.CredentialsSecretName = secret.Name
	clusterD := credentialTestCluster("cluster-d", "team-a")
	clusterD.Spec.CredentialsSecretName = secret.Name
	reconciler := newStatefulSetReconcileTestReconciler(
		t,
		newStatefulSetReconcileTestScheme(t),
		clusterD,
		clusterC,
		clusterB,
		clusterA,
	)

	g.Expect(reconciler.mapCredentialSecretToMysqlClusters(ctx, secret)).To(Equal([]reconcile.Request{
		{NamespacedName: client.ObjectKeyFromObject(clusterA)},
		{NamespacedName: client.ObjectKeyFromObject(clusterD)},
	}))
	unrelated := secret.DeepCopy()
	unrelated.Name = "no-cluster-uses-this"
	g.Expect(reconciler.mapCredentialSecretToMysqlClusters(ctx, unrelated)).To(BeEmpty())
	sameNameOtherNamespace := secret.DeepCopy()
	sameNameOtherNamespace.Namespace = "team-b"
	g.Expect(reconciler.mapCredentialSecretToMysqlClusters(ctx, sameNameOtherNamespace)).To(Equal([]reconcile.Request{
		{NamespacedName: client.ObjectKeyFromObject(clusterC)},
	}))
	g.Expect(reconciler.mapCredentialSecretToMysqlClusters(ctx, &corev1.ConfigMap{})).To(BeEmpty())
}

func TestMysqlCredentialCommandBoundary(t *testing.T) {
	g := NewWithT(t)
	knownRootPassword := "known-root-test-value"
	knownReplicationPassword := "known-replication-test-value"
	commands := []string{
		mysqlPreparePrimaryCommand(),
		mysqlConfigureReplicaCommand("mysql-primary"),
		mysqlShowSlaveStatusCommand(),
		mysqlShowMasterGTIDCommand(),
		mysqlShowSlaveGTIDCommand(),
	}
	for _, command := range commands {
		g.Expect(command).To(ContainSubstring(`MYSQL_PWD="$MYSQL_ROOT_PASSWORD"`))
		g.Expect(command).NotTo(ContainSubstring("-p" + knownRootPassword))
		g.Expect(command).NotTo(ContainSubstring(knownRootPassword))
		g.Expect(command).NotTo(ContainSubstring(knownReplicationPassword))
	}
	for _, command := range commands[:2] {
		g.Expect(command).To(ContainSubstring("MYSQL_REPLICATION_PASSWORD"))
		g.Expect(command).To(ContainSubstring("replication_password_sql"))
		g.Expect(command).To(ContainSubstring(`sed "s/\\\\/\\\\\\\\/g; s/'/''/g"`))
	}

	for _, productionFile := range []string{
		"mysqlcluster_controller.go",
		"utlis.go",
		"reconcile_master_slave.go",
		"elect_new_master.go",
		"statefulset_resources.go",
	} {
		source, err := os.ReadFile(productionFile)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(source)).NotTo(ContainSubstring("MySQLPassword"))
		g.Expect(string(source)).NotTo(ContainSubstring("IDENTIFIED BY 'password'"))
		g.Expect(string(source)).NotTo(ContainSubstring("MASTER_PASSWORD='password'"))
	}
}

func TestMysqlReplicationPasswordSQLAssignmentWithPOSIXShell(t *testing.T) {
	shellPath, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("POSIX sh is unavailable on this platform: %v", err)
	}

	shellSource := mysqlReplicationPasswordSQLAssignment + `; printf '%s' "$replication_password_sql"`
	testCases := []struct {
		name     string
		password string
		expected string
	}{
		{name: "ordinary", password: "ordinary-password", expected: "ordinary-password"},
		{name: "single quote", password: "quote'password", expected: "quote''password"},
		{name: "backslash", password: `back\slash`, expected: `back\\slash`},
		{name: "dollar expansion syntax", password: `value$HOME`, expected: `value$HOME`},
		{name: "command substitution syntax", password: `value$(printf injected)`, expected: `value$(printf injected)`},
		{name: "backticks", password: "value`uname`", expected: "value`uname`"},
		{name: "shell separators and spaces", password: `semi; amp& space`, expected: `semi; amp& space`},
		{name: "double quote", password: `value"quoted"`, expected: `value"quoted"`},
		{
			name:     "mixed shell and SQL metacharacters",
			password: "mix'\\$HOME$(printf injected)`uname`; & \"done\"",
			expected: "mix''\\\\$HOME$(printf injected)`uname`; & \"done\"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if strings.Contains(shellSource, testCase.password) {
				t.Fatalf("test password must not be embedded in sh -c source")
			}

			command := exec.Command(shellPath, "-c", shellSource)
			command.Env = append(os.Environ(), "MYSQL_REPLICATION_PASSWORD="+testCase.password)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("POSIX shell execution failed for %q: %v; output=%q", testCase.password, err, output)
			}
			if string(output) != testCase.expected {
				t.Fatalf("replication password escaping mismatch: input=%q output=%q expected=%q", testCase.password, output, testCase.expected)
			}
		})
	}
}

func TestGeneratedSecretRBACIsReadOnly(t *testing.T) {
	g := NewWithT(t)
	manifestPath := filepath.Join("..", "..", "config", "rbac", "role.yaml")
	manifest, err := os.Open(manifestPath)
	g.Expect(err).NotTo(HaveOccurred())
	defer func() { g.Expect(manifest.Close()).To(Succeed()) }()

	role := &rbacv1.ClusterRole{}
	g.Expect(yaml.NewYAMLOrJSONDecoder(manifest, 4096).Decode(role)).To(Succeed())
	var secretVerbs []string
	for _, rule := range role.Rules {
		if containsString(rule.Resources, "secrets") {
			secretVerbs = append(secretVerbs, rule.Verbs...)
		}
	}
	g.Expect(secretVerbs).To(Equal([]string{"get", "list", "watch"}))
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
