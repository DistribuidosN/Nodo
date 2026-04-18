package ports

import (
	"context"
	"orchestrator-node/internal/core/domain"
)

// OrchestratorClient es la interfaz que abstrae al servidor Java.
type OrchestratorClient interface {
	PullTasks(ctx context.Context, reqSlots int32) ([]*domain.ImageTask, error)
	SubmitResult(ctx context.Context, result *domain.TaskResult) error
	SendHeartbeat(ctx context.Context, metrics *domain.NodeMetrics) error
	// Dependiendo de si decides usarlo a futuro:
	// UpdateTaskProgress(ctx context.Context, taskID string, progress int32) error
}
