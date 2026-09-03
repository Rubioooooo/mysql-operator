package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageConfig defines the persistent storage requested for each MySQL member.
type StorageConfig struct {
	// StorageClassName is the StorageClass used by MySQL persistent storage.
	// +kubebuilder:validation:MinLength=1
	StorageClassName string `json:"storageClassName"`

	// Size is the persistent storage capacity requested for each MySQL member.
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self > 0 : sign(quantity(self)) == 1",message="storage size must be greater than zero"
	Size resource.Quantity `json:"size"`
}

// ResourceRequests defines CPU and memory requests for a MySQL container.
type ResourceRequests struct {
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : sign(quantity(self)) >= 0",message="cpu request must not be negative"
	CPU resource.Quantity `json:"cpu"`

	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : sign(quantity(self)) >= 0",message="memory request must not be negative"
	Memory resource.Quantity `json:"memory"`
}

// ResourceLimits defines CPU and memory limits for a MySQL container.
type ResourceLimits struct {
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : sign(quantity(self)) >= 0",message="cpu limit must not be negative"
	CPU resource.Quantity `json:"cpu"`

	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : sign(quantity(self)) >= 0",message="memory limit must not be negative"
	Memory resource.Quantity `json:"memory"`
}

// ResourceRequirements defines resource requests and limits for MySQL.
type ResourceRequirements struct {
	Requests ResourceRequests `json:"requests"`
	Limits   ResourceLimits   `json:"limits"`
}

// MysqlClusterSpec defines the desired state of MysqlCluster.
//
// +kubebuilder:validation:XValidation:rule="self.masterService != self.slaveService",message="masterService and slaveService must be different"
// +kubebuilder:validation:XValidation:rule="self.masterService == oldSelf.masterService",message="masterService is immutable"
// +kubebuilder:validation:XValidation:rule="self.slaveService == oldSelf.slaveService",message="slaveService is immutable"
// +kubebuilder:validation:XValidation:rule="self.credentialsSecretName == oldSelf.credentialsSecretName",message="credentialsSecretName is immutable"
type MysqlClusterSpec struct {
	// Image is the MySQL container image.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Replicas is the desired number of MySQL members.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	Replicas *int32 `json:"replicas,omitempty"`

	// MasterService is the stable Service name used to route traffic to the primary.
	// It is retained for API compatibility and is immutable after creation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=60
	// +kubebuilder:validation:Pattern="^[a-z]([-a-z0-9]*[a-z0-9])?$"
	MasterService string `json:"masterService"`

	// SlaveService is the stable Service name used to route traffic to replicas.
	// It is retained for API compatibility and is immutable after creation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z]([-a-z0-9]*[a-z0-9])?$"
	SlaveService string `json:"slaveService"`

	// CredentialsSecretName is the name of an external immutable Secret in the
	// MysqlCluster namespace containing root-password and replication-password.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
	CredentialsSecretName string `json:"credentialsSecretName"`

	Storage   StorageConfig        `json:"storage"`
	Resources ResourceRequirements `json:"resources"`
}

// MysqlClusterPhase describes the high-level lifecycle state reported by the Operator.
type MysqlClusterPhase string

const (
	MysqlClusterPhasePending      MysqlClusterPhase = "Pending"
	MysqlClusterPhaseInitializing MysqlClusterPhase = "Initializing"
	MysqlClusterPhaseRunning      MysqlClusterPhase = "Running"
	MysqlClusterPhaseDegraded     MysqlClusterPhase = "Degraded"
	MysqlClusterPhaseFailed       MysqlClusterPhase = "Failed"
)

// MysqlClusterReplicaTransitionStatus records an active replica-count transition.
type MysqlClusterReplicaTransitionStatus struct {
	// FromReplicas is the last stable replica count from which the transition began.
	FromReplicas int32 `json:"fromReplicas"`

	// TargetReplicas is the current desired replica count for the transition.
	TargetReplicas int32 `json:"targetReplicas"`
}

// MysqlClusterHAState describes the durable high-availability state machine.
type MysqlClusterHAState string

const (
	MysqlClusterHAStateHealthy            MysqlClusterHAState = "Healthy"
	MysqlClusterHAStateSuspected          MysqlClusterHAState = "Suspected"
	MysqlClusterHAStateFailoverRequired   MysqlClusterHAState = "FailoverRequired"
	MysqlClusterHAStateFailoverInProgress MysqlClusterHAState = "FailoverInProgress"
	MysqlClusterHAStateVerifying          MysqlClusterHAState = "Verifying"
	MysqlClusterHAStateDegraded           MysqlClusterHAState = "Degraded"
)

// MysqlClusterFailoverStage describes the durable stage of an active failover.
// +kubebuilder:validation:Enum=Fencing;CandidateSelected;Promoting;Reconfiguring
type MysqlClusterFailoverStage string

const (
	MysqlClusterFailoverStageFencing           MysqlClusterFailoverStage = "Fencing"
	MysqlClusterFailoverStageCandidateSelected MysqlClusterFailoverStage = "CandidateSelected"
	MysqlClusterFailoverStagePromoting         MysqlClusterFailoverStage = "Promoting"
	MysqlClusterFailoverStageReconfiguring     MysqlClusterFailoverStage = "Reconfiguring"
)

// MysqlClusterFenceState describes the durable state of failed-primary fencing.
// +kubebuilder:validation:Enum=Pending;Verified;Blocked
type MysqlClusterFenceState string

const (
	MysqlClusterFenceStatePending  MysqlClusterFenceState = "Pending"
	MysqlClusterFenceStateVerified MysqlClusterFenceState = "Verified"
	MysqlClusterFenceStateBlocked  MysqlClusterFenceState = "Blocked"
)

// MysqlClusterFenceMethod identifies the authority used to fence writes.
// +kubebuilder:validation:Enum=MySQLSuperReadOnly
type MysqlClusterFenceMethod string

const (
	MysqlClusterFenceMethodMySQLSuperReadOnly MysqlClusterFenceMethod = "MySQLSuperReadOnly"
)

// MysqlClusterFailoverStatus records durable failover, fence, and election progress.
// +kubebuilder:validation:XValidation:rule="self.fenceState != 'Verified' || (has(self.fencedPrimaryUID) && self.fencedPrimaryUID.size() > 0 && self.fencedPrimaryUID == self.failedPrimaryUID)",message="a verified fence must identify the failed primary UID"
// +kubebuilder:validation:XValidation:rule="self.stage == 'Fencing' || (self.fenceState == 'Verified' && has(self.fencedPrimaryUID) && self.fencedPrimaryUID == self.failedPrimaryUID && has(self.candidate) && self.candidate.size() > 0 && has(self.candidateUID) && self.candidateUID.size() > 0 && has(self.failedPrimaryServerUUID) && self.failedPrimaryServerUUID.size() > 0 && has(self.failedPrimaryGTIDSet))",message="CandidateSelected and later stages require a verified fence and complete election proof"
// +kubebuilder:validation:XValidation:rule="self.stage == 'Fencing' || (self.candidate != self.failedPrimary && self.candidateUID != self.failedPrimaryUID)",message="the election candidate must differ from the failed primary identity"
// +kubebuilder:validation:XValidation:rule="self.stage != 'Fencing' || (!has(self.candidate) && !has(self.candidateUID) && !has(self.failedPrimaryServerUUID) && !has(self.failedPrimaryGTIDSet))",message="Fencing must not carry candidate-selection proof"
type MysqlClusterFailoverStatus struct {
	Stage MysqlClusterFailoverStage `json:"stage"`

	// +kubebuilder:validation:MinLength=1
	FailedPrimary string `json:"failedPrimary"`

	// +kubebuilder:validation:MinLength=1
	FailedPrimaryUID string `json:"failedPrimaryUID"`

	FenceState MysqlClusterFenceState `json:"fenceState"`

	FenceMethod MysqlClusterFenceMethod `json:"fenceMethod,omitempty"`

	FencedPrimaryUID string `json:"fencedPrimaryUID,omitempty"`

	// Candidate is the canonical StatefulSet Pod selected by GTID-safe election.
	// +kubebuilder:validation:MinLength=1
	Candidate string `json:"candidate,omitempty"`

	// CandidateUID binds Candidate to the exact Pod incarnation that was elected.
	// +kubebuilder:validation:MinLength=1
	CandidateUID string `json:"candidateUID,omitempty"`

	// FailedPrimaryServerUUID is the freshly observed MySQL server UUID used to
	// validate each candidate's replication-source identity.
	// +kubebuilder:validation:MinLength=1
	FailedPrimaryServerUUID string `json:"failedPrimaryServerUUID,omitempty"`

	// FailedPrimaryGTIDSet is the authoritative fenced-primary GTID set used by
	// election. Nil means no snapshot was captured; a pointer to "" is a valid
	// authoritative empty GTID set.
	FailedPrimaryGTIDSet *string `json:"failedPrimaryGTIDSet,omitempty"`
}

// MysqlClusterHAStatus records durable primary-failure observations and HA progress.
type MysqlClusterHAStatus struct {
	// State is the current durable HA state.
	// +kubebuilder:validation:Enum=Healthy;Suspected;FailoverRequired;FailoverInProgress;Verifying;Degraded
	State MysqlClusterHAState `json:"state"`

	// Primary is the observed primary Pod name associated with this HA decision.
	Primary string `json:"primary"`

	// PrimaryUID is the observed primary Pod UID associated with this HA decision.
	PrimaryUID string `json:"primaryUID"`

	// FailureCount is the number of consecutive failure observations for PrimaryUID.
	FailureCount int32 `json:"failureCount"`

	// FirstFailureTime is when the current same-UID failure sequence began.
	FirstFailureTime *metav1.Time `json:"firstFailureTime,omitempty"`

	// Failover records an active durable failover plan.
	Failover *MysqlClusterFailoverStatus `json:"failover,omitempty"`
}

// MysqlClusterStatus defines the observed state of MysqlCluster.
//
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.credentialsSecretUID) || (has(self.credentialsSecretUID) && self.credentialsSecretUID == oldSelf.credentialsSecretUID)",message="credentialsSecretUID is write-once and cannot be changed or cleared"
type MysqlClusterStatus struct {
	// ObservedGeneration is the most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is the high-level lifecycle state of the cluster.
	// +kubebuilder:validation:Enum=Pending;Initializing;Running;Degraded;Failed
	Phase MysqlClusterPhase `json:"phase,omitempty"`

	// Primary is the currently observed primary MySQL member.
	Primary string `json:"primary,omitempty"`

	// CurrentReplicas is the number of MySQL members currently observed.
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// ReadyReplicas is the number of MySQL members currently ready.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// LastConvergedReplicas is the replica count of the most recently completed
	// stable lifecycle state.
	LastConvergedReplicas *int32 `json:"lastConvergedReplicas,omitempty"`

	// ReplicaTransition records an active replica-count transition. Nil means no
	// replica-count transition is currently recorded.
	ReplicaTransition *MysqlClusterReplicaTransitionStatus `json:"replicaTransition,omitempty"`

	// HA records durable primary-failure observations and failover progress.
	HA *MysqlClusterHAStatus `json:"ha,omitempty"`

	// CredentialsSecretUID is the UID of the immutable external credential
	// Secret accepted for this MysqlCluster lifecycle.
	CredentialsSecretUID string `json:"credentialsSecretUID,omitempty"`

	// Conditions describe the current reconciliation and availability state.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Master is retained for compatibility with the original API.
	// Deprecated: use Primary.
	Master string `json:"master,omitempty"`

	// Slaves is retained for compatibility with the original API.
	// Deprecated: replica membership will be represented by the new status model.
	Slaves []string `json:"slaves,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Primary",type=string,JSONPath=".status.primary"
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=".status.readyReplicas"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type MysqlCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MysqlClusterSpec   `json:"spec,omitempty"`
	Status MysqlClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type MysqlClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MysqlCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MysqlCluster{}, &MysqlClusterList{})
}
