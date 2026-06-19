package cilium

import (
	"context"
	"fmt"

	"github.com/ncloud/platform/internal/ports"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// MultiRegionScheduler implements the Scheduler port for Phase 3.
// It leverages a Cilium Cluster Mesh to deploy workloads across Lagos, Abuja, and Accra.
type MultiRegionScheduler struct {
	// We use a single client pointing to a global/federated API server, 
	// or the primary cluster that uses Cilium Global Services.
	client *kubernetes.Clientset
}

func NewMultiRegionScheduler(config *rest.Config) (*MultiRegionScheduler, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &MultiRegionScheduler{client: clientset}, nil
}

func (s *MultiRegionScheduler) Deploy(ctx context.Context, spec ports.DeploymentSpec) error {
	replicas := int32(spec.Replicas)

	var envs []corev1.EnvVar
	for k, v := range spec.EnvVars {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	// PHASE 3 MAGIC: Node Affinity based on Domain Region
	// If the user requested 'abuja', we ensure the pod is scheduled on nodes labeled with region=abuja
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "topology.kubernetes.io/region",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{spec.Region},
							},
						},
					},
				},
			},
		},
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.DeploymentID,
			Namespace: "default",
			// Annotate for Cilium Global Service discovery
			Annotations: map[string]string{
				"io.cilium/global-service": "true",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": spec.DeploymentID}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": spec.DeploymentID}},
				Spec: corev1.PodSpec{
					Affinity: affinity,
					Containers: []corev1.Container{
						{
							Name:  "app-container",
							Image: spec.Image,
							Env:   envs,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(spec.CPURequest),
									corev1.ResourceMemory: resource.MustParse(spec.MemoryRequest),
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := s.client.AppsV1().Deployments("default").Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create multi-region deployment: %w", err)
	}

	return nil
}

// ... Scale, Stop, GetLogs implementations omitted ...
func (s *MultiRegionScheduler) Scale(ctx context.Context, deploymentID string, replicas int) error { return nil }
func (s *MultiRegionScheduler) Stop(ctx context.Context, deploymentID string) error { return nil }
func (s *MultiRegionScheduler) GetLogs(ctx context.Context, deploymentID string, tail int) ([]ports.LogLine, error) { return nil, nil }
func (s *MultiRegionScheduler) GetMetrics(ctx context.Context, deploymentID string) (ports.Metrics, error) { return ports.Metrics{}, nil }
