package controller

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	mysqlRootPasswordSecretKey        = "root-password"
	mysqlReplicationPasswordSecretKey = "replication-password"
)

func validateMysqlCredentialValue(secretIdentity, key string, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("credential Secret %s key %s must not be empty", secretIdentity, key)
	}
	if bytes.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("credential Secret %s key %s must not contain NUL bytes", secretIdentity, key)
	}
	if bytes.ContainsAny(value, "\r\n") {
		return fmt.Errorf("credential Secret %s key %s must not contain CR or LF bytes", secretIdentity, key)
	}
	return nil
}

func (r *MysqlClusterReconciler) ensureMysqlCredentials(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.CredentialsSecretName}
	if err := r.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("credential Secret %s referenced by MysqlCluster %s does not exist", key, cluster.Name)
		}
		return fmt.Errorf("failed to get credential Secret %s for MysqlCluster %s: %w", key, cluster.Name, err)
	}
	if secret.Immutable == nil || !*secret.Immutable {
		return fmt.Errorf("credential Secret %s referenced by MysqlCluster %s must set immutable: true", key, cluster.Name)
	}

	rootPassword, found := secret.Data[mysqlRootPasswordSecretKey]
	if !found {
		return fmt.Errorf("credential Secret %s is missing required key %s", key, mysqlRootPasswordSecretKey)
	}
	if err := validateMysqlCredentialValue(key.String(), mysqlRootPasswordSecretKey, rootPassword); err != nil {
		return err
	}
	replicationPassword, found := secret.Data[mysqlReplicationPasswordSecretKey]
	if !found {
		return fmt.Errorf("credential Secret %s is missing required key %s", key, mysqlReplicationPasswordSecretKey)
	}
	if err := validateMysqlCredentialValue(key.String(), mysqlReplicationPasswordSecretKey, replicationPassword); err != nil {
		return err
	}

	secretUID := string(secret.UID)
	if secretUID == "" {
		return fmt.Errorf("credential Secret %s referenced by MysqlCluster %s has no UID", key, cluster.Name)
	}
	if cluster.Status.CredentialsSecretUID != "" {
		if cluster.Status.CredentialsSecretUID != secretUID {
			return fmt.Errorf("credential Secret %s UID does not match the identity pinned by MysqlCluster %s", key, cluster.Name)
		}
		return nil
	}

	base := cluster.DeepCopy()
	cluster.Status.CredentialsSecretUID = secretUID
	if err := r.Status().Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("failed to pin credential Secret UID on MysqlCluster %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) mapCredentialSecretToMysqlClusters(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	secret, ok := object.(*corev1.Secret)
	if !ok {
		return nil
	}

	clusters := &databasev1.MysqlClusterList{}
	if err := r.List(ctx, clusters, client.InNamespace(secret.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "failed to list MysqlClusters referencing credential Secret", "namespace", secret.Namespace, "secret", secret.Name)
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.Spec.CredentialsSecretName == secret.Name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		}
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].NamespacedName.String() < requests[j].NamespacedName.String()
	})
	return requests
}
