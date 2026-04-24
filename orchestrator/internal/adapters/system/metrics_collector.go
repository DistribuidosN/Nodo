package system

import (
	"context"
	"time"

	"orchestrator-node/internal/core/ports"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// SystemMetricsCollector recolecta métricas reales del SO (CPU, RAM) 
// y las inyecta en el repositorio de métricas local.
type SystemMetricsCollector struct {
	metricsRepo ports.MetricsRepository
	interval    time.Duration
}

func NewSystemMetricsCollector(repo ports.MetricsRepository, interval time.Duration) *SystemMetricsCollector {
	return &SystemMetricsCollector{
		metricsRepo: repo,
		interval:    interval,
	}
}

func (c *SystemMetricsCollector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.collect()
			}
		}
	}()
}

func (c *SystemMetricsCollector) collect() {
	// 1. Recolectar RAM
	v, err := mem.VirtualMemory()
	var usedMB, totalMB float32
	if err == nil {
		usedMB = float32(v.Used) / (1024 * 1024)
		totalMB = float32(v.Total) / (1024 * 1024)
	}

	// 2. Recolectar CPU (promedio de los últimos 200ms)
	percentages, err := cpu.Percent(200*time.Millisecond, false)
	var cpuPercent float32
	if err == nil && len(percentages) > 0 {
		cpuPercent = float32(percentages[0])
	}

	// 3. Actualizar repositorio
	c.metricsRepo.UpdateSystemMetrics(cpuPercent, usedMB, totalMB)
}
