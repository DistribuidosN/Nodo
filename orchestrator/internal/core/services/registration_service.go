package services

import (
	"context"
	"log"
	"time"
	"fmt"

	"orchestrator-node/internal/core/ports"
)

// RegistrationConfig contiene la configuración de comportamiento del registro.
type RegistrationConfig struct {
	// Intervalo base entre reintentos (se multiplica con backoff exponencial)
	RetryBaseDelay time.Duration
	// Máximo delay entre reintentos (techo del backoff)
	RetryMaxDelay time.Duration
	// Factor multiplicador del backoff (ej: 2.0 = duplicar)
	BackoffFactor float64
	// Máximo de intentos (0 = infinito)
	MaxAttempts int
	// Intervalo de heartbeat periódico tras el registro exitoso
	HeartbeatInterval time.Duration
}

// DefaultRegistrationConfig devuelve una configuración razonable lista para producción.
func DefaultRegistrationConfig() RegistrationConfig {
	return RegistrationConfig{
		RetryBaseDelay:    2 * time.Second,
		RetryMaxDelay:     60 * time.Second,
		BackoffFactor:     2.0,
		MaxAttempts:       0, // 0 = infinito
		HeartbeatInterval: 15 * time.Second,
	}
}

// RegistrationService gestiona el ciclo de vida de la conexión del nodo con el servidor Java:
//  1. Registro inicial con reintentos y backoff exponencial.
//  2. Heartbeat periódico una vez registrado.
//  3. Re-registro automático si el servidor no responde N heartbeats consecutivos.
type RegistrationService struct {
	client      ports.OrchestratorClient
	metricsRepo ports.MetricsRepository
	cfg         RegistrationConfig
	grpcPort    int32 // Puerto gRPC local del nodo que se informa al servidor
}

// NewRegistrationService crea una nueva instancia del servicio de registro.
func NewRegistrationService(
	client ports.OrchestratorClient,
	metricsRepo ports.MetricsRepository,
	cfg RegistrationConfig,
	grpcPort int32,
) *RegistrationService {
	return &RegistrationService{
		client:      client,
		metricsRepo: metricsRepo,
		cfg:         cfg,
		grpcPort:    grpcPort,
	}
}

// Start lanza el ciclo de registro + heartbeat en background.
// Se bloquea solo mientras intenta el primer registro. El heartbeat periódico corre en background.
func (s *RegistrationService) Start(ctx context.Context) {
	go s.runRegistrationLoop(ctx)
}

// runRegistrationLoop gestiona todo el ciclo: registro inicial → heartbeat → re-registro si falla.
func (s *RegistrationService) runRegistrationLoop(ctx context.Context) {
	metrics := s.metricsRepo.GetMetrics()
	nodeID := metrics.NodeID
	ip := metrics.IPAddress

	log.Printf("[Registration] Iniciando ciclo de registro para nodo '%s' (IP: %s)", nodeID, ip)

	// ── Fase 1: Registro inicial con reintentos y backoff exponencial ──────────
	if !s.waitUntilRegistered(ctx, nodeID, ip) {
		// El contexto fue cancelado antes de registrarse
		log.Printf("[Registration] Contexto cancelado antes de completar el registro inicial.")
		return
	}

	// ── Fase 2: Heartbeat periódico con detección de fallo y re-registro ──────
	s.runHeartbeatLoop(ctx, nodeID, ip)
}

// waitUntilRegistered intenta registrar al nodo con backoff exponencial hasta lograrlo o que el
// contexto sea cancelado. Retorna true si se registró, false si el ctx fue cancelado.
func (s *RegistrationService) waitUntilRegistered(ctx context.Context, nodeID, ip string) bool {
	delay := s.cfg.RetryBaseDelay
	attempt := 0

	for {
		attempt++
		if s.cfg.MaxAttempts > 0 && attempt > s.cfg.MaxAttempts {
			log.Printf("[Registration] Se alcanzó el máximo de %d intentos. Nodo NO registrado.", s.cfg.MaxAttempts)
			return false
		}

	// Obtener métricas frescas en cada intento
		m := s.metricsRepo.GetMetrics()

		log.Printf("[Registration Intento #%d — Registrando nodo '%s' en el servidor...", attempt, nodeID)

		result, err := s.client.RegisterNode(ctx, nodeID, ip, s.grpcPort, m)
		if err != nil {
			log.Printf("[Registration]Intento #%d fallido: %v. Reintentando en %s...", attempt, err, delay)
			if !s.sleepOrCancel(ctx, delay) {
				return false
			}
			delay = nextDelay(delay, s.cfg.BackoffFactor, s.cfg.RetryMaxDelay)
			continue
		}

		// Registro exitoso
		if result == "REGISTERED" {
			log.Printf("[Registration] Nodo '%s' REGISTRADO correctamente en el servidor.", nodeID)
		} else {
			// UPDATED: el nodo ya existía, solo se actualizó su estado
			log.Printf("[Registration] Nodo '%s' ya existía → estado ACTUALIZADO en el servidor.", nodeID)
		}
		
		// Log de éxito inicial (Regla 6)
		s.client.LogEvent("INFO", "Node Worker Ready - Initial registration successful", "", 0)
		return true
	}
}

// runHeartbeatLoop envía heartbeats periódicamente. Si falla consecutivamente más de
// un umbral configurable, reinicia el proceso de registro.
func (s *RegistrationService) runHeartbeatLoop(ctx context.Context, nodeID, ip string) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	consecutiveFails := 0
	const failureThreshold = 3

	log.Printf("[Registration]   Heartbeat periódico activo (intervalo: %s)", s.cfg.HeartbeatInterval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Registration] Contexto cancelado. Deteniendo heartbeat de '%s'.", nodeID)
			return

		case <-ticker.C:
			m := s.metricsRepo.GetMetrics()
			result, err := s.client.RegisterNode(ctx, nodeID, ip, s.grpcPort, m)
			if err != nil {
				consecutiveFails++
				log.Printf("[Registration] Heartbeat fallido (%d/%d): %v",
					consecutiveFails, failureThreshold, err)
				s.client.LogEvent("ERROR", fmt.Sprintf("Heartbeat failed (%d/%d): %v", consecutiveFails, failureThreshold, err), "", 0)

				if consecutiveFails >= failureThreshold {
					log.Printf("[Registration] %d fallos consecutivos detectados. Re-iniciando registro...", consecutiveFails)
					consecutiveFails = 0
					// Volver a registrar, si falla el ctx se habrá cancelado
					if !s.waitUntilRegistered(ctx, nodeID, ip) {
						return
					}
					// Resetear ticker tras re-registro exitoso
					ticker.Reset(s.cfg.HeartbeatInterval)
				}
				continue
			}

			consecutiveFails = 0
			log.Printf("[Registration] Heartbeat OK — nodo '%s' activo (%s)", nodeID, result)
		}
	}
}

// sleepOrCancel duerme durante d o retorna false si el contexto fue cancelado.
func (s *RegistrationService) sleepOrCancel(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// nextDelay calcula el próximo delay con backoff exponencial con techo máximo.
func nextDelay(current time.Duration, factor float64, max time.Duration) time.Duration {
	next := time.Duration(float64(current) * factor)
	if next > max {
		return max
	}
	return next
}
