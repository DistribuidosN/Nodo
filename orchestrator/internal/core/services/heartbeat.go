package services

import (
	"context"
	"log"
	"time"

	"orchestrator-node/internal/core/ports"
)

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
				return
			case <-ticker.C:
				m := s.metricsRepo.GetMetrics()
				if err := s.client.SendHeartbeat(ctx, m); err != nil {
					log.Printf("[Heartbeat] Error enviando latido: %v", err)
				}
			}
		}
	}()
}
