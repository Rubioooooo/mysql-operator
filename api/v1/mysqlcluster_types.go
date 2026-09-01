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
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z]([-a-z0-9]*[a-z0-9])?$"
	MasterService string `json:"masterService"`

	// SlaveService is the stable Service name used to route traffic to replicas.
	// It is retained for API compatibility and is immutable after creation.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z]([-a-z0-9]*[a-z0-9])?$"
	SlaveService string `json:"slaveService"`

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

// MysqlClusterStatus defines the observed state of MysqlCluster.
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
