package controller

import (
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
)

func validateMysqlClusterUpgradeStatus(status *databasev1.MysqlClusterStatus) error {
	upgrade := status.Upgrade
	if upgrade == nil {
		return nil
	}
	if strings.TrimSpace(status.LastConvergedImage) == "" || strings.TrimSpace(upgrade.FromImage) == "" || strings.TrimSpace(upgrade.TargetImage) == "" {
		return fmt.Errorf("upgrade requires non-empty lastConvergedImage, fromImage and targetImage")
	}
	if upgrade.FromImage != status.LastConvergedImage || upgrade.FromImage == upgrade.TargetImage {
		return fmt.Errorf("upgrade requires fromImage equal to lastConvergedImage and different from targetImage")
	}
	switch upgrade.Stage {
	case databasev1.MysqlClusterUpgradeStagePreparing, databasev1.MysqlClusterUpgradeStageTemplatePending, databasev1.MysqlClusterUpgradeStageTemplateReady, databasev1.MysqlClusterUpgradeStageReplicasVerified:
	default:
		return fmt.Errorf("unknown durable upgrade stage")
	}
	replacement := upgrade.Replacement
	if replacement == nil {
		return nil
	}
	if upgrade.Stage != databasev1.MysqlClusterUpgradeStageTemplateReady {
		return fmt.Errorf("replacement requires TemplateReady")
	}
	if replacement.Ordinal < 1 || strings.TrimSpace(replacement.PodName) == "" || strings.TrimSpace(replacement.OldPodUID) == "" {
		return fmt.Errorf("replacement requires positive ordinal and non-empty Pod name and old identity")
	}
	switch replacement.Stage {
	case databasev1.MysqlClusterUpgradeReplacementStageDeletePending, databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement:
		if replacement.NewPodUID != "" {
			return fmt.Errorf("replacement new identity is only valid in Verifying")
		}
	case databasev1.MysqlClusterUpgradeReplacementStageVerifying:
		if strings.TrimSpace(replacement.NewPodUID) == "" || replacement.NewPodUID == replacement.OldPodUID {
			return fmt.Errorf("Verifying requires a different non-empty new identity")
		}
	default:
		return fmt.Errorf("unknown durable replacement stage")
	}
	return nil
}

const defaultMysqlReplicas int32 = 3

// desiredReplicas returns the replica count requested by the API object.
//
// The API server defaults spec.replicas to 3. The nil fallback keeps the
// controller defensive when handling objects constructed directly in tests
// or by callers that have not passed through API-server defaulting.
func desiredReplicas(cluster *databasev1.MysqlCluster) int32 {
	if cluster.Spec.Replicas == nil {
		return defaultMysqlReplicas
	}

	return *cluster.Spec.Replicas
}

func validateMysqlClusterReplicaTransitionStatus(status *databasev1.MysqlClusterStatus) error {
	if status.LastConvergedReplicas != nil && *status.LastConvergedReplicas <= 0 {
		return fmt.Errorf(
			"lastConvergedReplicas must be greater than zero, got %d",
			*status.LastConvergedReplicas,
		)
	}

	transition := status.ReplicaTransition
	if transition == nil {
		return nil
	}
	if status.LastConvergedReplicas == nil {
		return fmt.Errorf("replicaTransition requires lastConvergedReplicas")
	}
	if transition.FromReplicas <= 0 {
		return fmt.Errorf(
			"replicaTransition.fromReplicas must be greater than zero, got %d",
			transition.FromReplicas,
		)
	}
	if transition.TargetReplicas <= 0 {
		return fmt.Errorf(
			"replicaTransition.targetReplicas must be greater than zero, got %d",
			transition.TargetReplicas,
		)
	}
	if transition.FromReplicas != *status.LastConvergedReplicas {
		return fmt.Errorf(
			"replicaTransition.fromReplicas %d does not match lastConvergedReplicas %d",
			transition.FromReplicas,
			*status.LastConvergedReplicas,
		)
	}

	return nil
}
