package grpc

import (
	"context"
	"log"

	"connectrpc.com/connect"

	"orchestrator-node/internal/core/ports"
	pb "orchestrator-node/proto"
)

// ConnectWorkerServer implementa el servicio pb.WorkerNodeServer.
// Usamos el repositorio de métricas para responder cuando el orquestador pregunta vía HTTP.
type ConnectWorkerServer struct {
	metricsRepo ports.MetricsRepository
}

func NewConnectWorkerServer(metricsRepo ports.MetricsRepository) *ConnectWorkerServer {
	return &ConnectWorkerServer{
		metricsRepo: metricsRepo,
	}
}

func (s *ConnectWorkerServer) GetMetrics(ctx context.Context, req *connect.Request[pb.QueueStatusRequest]) (*connect.Response[pb.NodeMetrics], error) {
	m := s.metricsRepo.GetMetrics()
	log.Printf("[Connect-Server] Java Orquestador solicitó GetMetrics — Envíando CPU: %.1f%%, RAM: %.1f MB", m.CPUPercent, m.RAMUsedMB)
	res := connect.NewResponse(mapDomainToPbMetrics(m))
	return res, nil
}

func (s *ConnectWorkerServer) YieldTasks(ctx context.Context, req *connect.Request[pb.StealRequest]) (*connect.Response[pb.StealResponse], error) {
	log.Printf("[Connect-Server] Java Orquestador solicitó YieldTasks: nodo %s pide robar %d", req.Msg.ThiefNodeId, req.Msg.StealCount)

	s.metricsRepo.RecordSteal(0) // 0 robos permitidos por ahora como default simple

	res := connect.NewResponse(&pb.StealResponse{
		Allowed: false,
		Reason:  "Go Connect Agent YieldTasks is effectively locking the channel. Robbery denied.",
	})
	return res, nil
}
