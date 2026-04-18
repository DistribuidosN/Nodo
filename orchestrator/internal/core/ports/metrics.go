package ports

import (
	"orchestrator-node/internal/core/domain"
)

// MetricsRepository mantiene el almacenamiento thread-safe local de métricas producidas por Python/Go.
type MetricsRepository interface {
	GetMetrics() *domain.NodeMetrics
	UpdateWithTaskResult(result *domain.TaskResult)
	IncrementBusyWorkers(i int32)
	SetQueueSize(size int32)
	RecordSteal(count int32)
}
