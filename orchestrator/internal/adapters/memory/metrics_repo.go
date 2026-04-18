package memory

import (
	"sync"
	"time"

	"orchestrator-node/internal/core/domain"
	"orchestrator-node/internal/core/ports"
)

type InMemoryMetricsRepo struct {
	mu      sync.RWMutex
	metrics *domain.NodeMetrics
}

func NewInMemoryMetricsRepo(nodeID, ipAddress string, totalWorkers int32, queueCapacity int32) ports.MetricsRepository {
	return &InMemoryMetricsRepo{
		metrics: &domain.NodeMetrics{
			NodeID:        nodeID,
			IPAddress:     ipAddress,
			WorkersTotal:  totalWorkers,
			QueueCapacity: queueCapacity,
			Status:        "IDLE",
			UptimeSeconds: time.Now().Unix(), // Guardamos timestamp inicio, luego calculamos uptime.
		},
	}
}

func (r *InMemoryMetricsRepo) GetMetrics() *domain.NodeMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Crear copia para retornar y evitar mutaciones externas
	copy := *r.metrics
	
	// Calcular uptime real antes de devolver
	copy.UptimeSeconds = time.Now().Unix() - copy.UptimeSeconds

	// Si hay python stats
	return &copy
}

func (r *InMemoryMetricsRepo) UpdateWithTaskResult(result *domain.TaskResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if result.Success {
		r.metrics.TasksDone++
	}

	// Python retorna métricas en el TaskResult. Lo integramos en nuestra base de RAM/Latencia.
	if result.Metrics != nil {
		r.metrics.RAMUsedMB = result.Metrics.RAMUsedMB
		r.metrics.CPUPercent = result.Metrics.CPUPercent
		r.metrics.AvgLatencyMs = result.Metrics.AvgLatencyMs
		r.metrics.P95LatencyMs = result.Metrics.P95LatencyMs
	}
}

func (r *InMemoryMetricsRepo) IncrementBusyWorkers(i int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.WorkersBusy += i
	if r.metrics.WorkersBusy > r.metrics.WorkersTotal {
		r.metrics.WorkersBusy = r.metrics.WorkersTotal
	}
	if r.metrics.WorkersBusy < 0 {
		r.metrics.WorkersBusy = 0
	}
	if r.metrics.WorkersBusy > 0 {
		r.metrics.Status = "BUSY"
	} else {
		r.metrics.Status = "IDLE"
	}
}

func (r *InMemoryMetricsRepo) SetQueueSize(size int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.QueueSize = size
}

func (r *InMemoryMetricsRepo) RecordSteal(count int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.StealsPerformed += count
}
