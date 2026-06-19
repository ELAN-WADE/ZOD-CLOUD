package k8s

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

// K8sScheduler implements the Scheduler port using the Kubernetes API.
// This supports K3s (Phase 2) and full K8s (Phase 3).
type K8sScheduler struct {
	client *kubernetes.Clientset
}

func NewK8sScheduler(config *rest.Config) (*K8sScheduler, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &K8sScheduler{client: clientset}, nil
}

func (s *K8sScheduler) Deploy(ctx context.Context, spec ports.DeploymentSpec) error {
	// Map the generic domain.DeploymentSpec to a Kubernetes Deployment object
	
	// Convert int to pointer for replicas
	replicas := int32(spec.Replicas)

	// Convert env vars
	var envs []corev1.EnvVar
	for k, v := range spec.EnvVars {
		envs = append(envs, corev1.EnvVar{Name: k, Value: v})
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.DeploymentID,
			Namespace: "default", // Multi-tenant systems would use projectID
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": spec.DeploymentID},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": spec.DeploymentID},
				},
				Spec: corev1.PodSpec{
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

	// Call the Kubernetes API to create it
	_, err := s.client.AppsV1().Deployments("default").Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create k8s deployment: %w", err)
	}

	return nil
}

func (s *K8sScheduler) Scale(ctx context.Context, deploymentID string, replicas int) error {
	// Implementation omitted: We would fetch the Deployment, update the Replicas pointer, and Update()
	return nil
}

func (s *K8sScheduler) Stop(ctx context.Context, deploymentID string) error {
	err := s.client.AppsV1().Deployments("default").Delete(ctx, deploymentID, metav1.DeleteOptions{})
	return err
}

func (s *K8sScheduler) GetLogs(ctx context.Context, deploymentID string, tail int) ([]ports.LogLine, error) {
	// Implementation omitted: We would find the Pods for the deployment and call PodLogs
	return nil, nil
}

func (s *K8sScheduler) GetMetrics(ctx context.Context, deploymentID string) (ports.Metrics, error) {
	// Implementation omitted: We would call the metrics-server API
	return ports.Metrics{}, nil
}
