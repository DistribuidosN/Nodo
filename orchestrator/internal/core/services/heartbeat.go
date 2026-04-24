package services

import (
	"context"
	"log"
	"time"

	"orchestrator-node/internal/core/ports"
)

// HeartbeatService envía métricas periódicas al servidor Java como latido ligero.
// El registro y la re-conexión con backoff son responsabilidad del RegistrationService.
// Este servicio actúa como capa secundaria de telemetría en tiempo real.
type HeartbeatService struct {
	client      ports.OrchestratorClient
	metricsRepo ports.MetricsRepository
	interval    time.Duration
}

func NewHeartbeatService(client ports.OrchestratorClient, repo ports.MetricsRepository, interval time.Duration) *HeartbeatService {
	return &HeartbeatService{
		client:      client,
		metricsRepo: repo,
		interval:    interval,
	}
}

func (s *HeartbeatService) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("[Heartbeat] Contexto cancelado. Deteniendo latidos de telemetría.")
				return
			case <-ticker.C:
				m := s.metricsRepo.GetMetrics()
				if err := s.client.SendHeartbeat(ctx, m); err != nil {
					log.Printf("[Heartbeat] ⚠ Error enviando latido de telemetría: %v", err)
				} else {
					log.Printf("[Heartbeat] 📡 Telemetría enviada — CPU: %.1f%%, RAM: %.1f/%.1f MB, workers: %d/%d, cola: %d, estado: %s",
						m.CPUPercent, m.RAMUsedMB, m.RAMTotalMB, m.WorkersBusy, m.WorkersTotal, m.QueueSize, m.Status)
				}
			}
		}
	}()
}
