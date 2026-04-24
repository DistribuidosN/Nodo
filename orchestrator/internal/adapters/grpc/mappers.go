package grpc

import (
	"orchestrator-node/internal/core/domain"
	pb "orchestrator-node/proto"
)

// mapDomainToPbMetrics convierte el modelo de dominio a su representación Protobuf.
func mapDomainToPbMetrics(m *domain.NodeMetrics) *pb.NodeMetrics {
	if m == nil {
		return nil
	}
	return &pb.NodeMetrics{
		NodeId:          m.NodeID,
		IpAddress:       m.IPAddress,
		RamUsedMb:       m.RAMUsedMB,
		RamTotalMb:      m.RAMTotalMB,
		CpuPercent:      m.CPUPercent,
		WorkersBusy:     m.WorkersBusy,
		WorkersTotal:    m.WorkersTotal,
		QueueSize:       m.QueueSize,
		QueueCapacity:   m.QueueCapacity,
		TasksDone:       m.TasksDone,
		StealsPerformed: m.StealsPerformed,
		AvgLatencyMs:    m.AvgLatencyMs,
		P95LatencyMs:    m.P95LatencyMs,
		UptimeSeconds:   m.UptimeSeconds,
		Status:          m.Status,
	}
}
