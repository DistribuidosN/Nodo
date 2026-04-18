package ports

import (
	"context"
	"orchestrator-node/internal/core/domain"
)

// ScriptExecutor abstrae la ejecución del cli (python).
// Retornará un TaskResult parcial (con códigos de éxito y métricas si python las pudo proveer).
type ScriptExecutor interface {
	ExecuteV2(ctx context.Context, task *domain.ImageTask) (*domain.TaskResult, error)
}
