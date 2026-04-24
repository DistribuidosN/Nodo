package ports

import (
	"context"
	"orchestrator-node/internal/core/domain"
)

// OrchestratorClient es la interfaz que abstrae al servidor Java.
type OrchestratorClient interface {
	// PullTasks solicita tareas cuando el nodo tiene slots disponibles.
	PullTasks(ctx context.Context, reqSlots int32, metrics *domain.NodeMetrics) ([]*domain.ImageTask, error)

	// SubmitResult entrega el resultado de una imagen procesada al servidor.
	SubmitResult(ctx context.Context, result *domain.TaskResult) error

	// RegisterNode registra o actualiza el nodo en el servidor Java.
	// Retorna el campo "status" de la respuesta ("REGISTERED" | "UPDATED") y un error.
	// Se invoca al arranque (autodescubrimiento) y periódicamente como heartbeat de registro.
	RegisterNode(ctx context.Context, nodeID, ip string, port int32, metrics *domain.NodeMetrics) (string, error)

	// SendHeartbeat envía métricas en tiempo real al servidor (heartbeat ligero de telemetría).
	SendHeartbeat(ctx context.Context, metrics *domain.NodeMetrics) error

	// LogEvent envía un log al orquestador central.
	LogEvent(level, message, imageUUID string, transformationID int32)
}
