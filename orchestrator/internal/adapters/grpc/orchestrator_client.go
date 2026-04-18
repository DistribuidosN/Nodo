package grpc

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"

	"orchestrator-node/internal/core/domain"
	"orchestrator-node/internal/core/ports"
	pb "orchestrator-node/proto"
	"orchestrator-node/proto/protoconnect"
)

type ConnectOrchestratorClient struct {
	client protoconnect.OrchestratorClient
	nodeID string
}

func NewConnectOrchestratorClient(httpClient *http.Client, baseURL string, nodeID string) ports.OrchestratorClient {
	return &ConnectOrchestratorClient{
		client: protoconnect.NewOrchestratorClient(httpClient, baseURL),
		nodeID: nodeID,
	}
}

func (c *ConnectOrchestratorClient) PullTasks(ctx context.Context, reqSlots int32) ([]*domain.ImageTask, error) {
	req := connect.NewRequest(&pb.PullRequest{
		NodeId:    c.nodeID,
		SlotsFree: reqSlots,
	})

	resp, err := c.client.PullTasks(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error PullTasks ConnectRPC: %w", err)
	}

	var tasks []*domain.ImageTask
	for _, t := range resp.Msg.Tasks {
		tasks = append(tasks, &domain.ImageTask{
			TaskID:       t.TaskId,
			ImageData:    t.ImageData, // Se trajo a bytes a RAM desde la red local
			Filename:     t.Filename,
			FilterType:   t.FilterType,
			Priority:     t.Priority,
			TargetWidth:  t.TargetWidth,
			TargetHeight: t.TargetHeight,
			ImageFormat:  t.ImageFormat,
			Metadata:     t.Metadata,
		})
	}
	return tasks, nil
}

func (c *ConnectOrchestratorClient) SubmitResult(ctx context.Context, result *domain.TaskResult) error {
	var pbMetrics *pb.NodeMetrics
	if result.Metrics != nil {
		pbMetrics = &pb.NodeMetrics{
			NodeId:       result.Metrics.NodeID,
			RamUsedMb:    result.Metrics.RAMUsedMB,
			CpuPercent:   result.Metrics.CPUPercent,
			AvgLatencyMs: result.Metrics.AvgLatencyMs,
		}
	}

	req := connect.NewRequest(&pb.TaskResult{
		TaskId:       result.TaskID,
		NodeId:       result.NodeID,
		WorkerId:     result.WorkerID,
		Success:      result.Success,
		ErrorMsg:     result.ErrorMsg,
		StartTs:      result.StartTs,
		FinishTs:     result.FinishTs,
		ProcessingMs: result.ProcessingMs,
		Metrics:      pbMetrics,
		Metadata:     result.Metadata,
	})

	_, err := c.client.SubmitResult(ctx, req)
	return err
}

func (c *ConnectOrchestratorClient) SendHeartbeat(ctx context.Context, metrics *domain.NodeMetrics) error {
	req := connect.NewRequest(&pb.HeartbeatRequest{
		NodeId:    metrics.NodeID,
		IpAddress: metrics.IPAddress,
		Metrics: &pb.NodeMetrics{
			NodeId:        metrics.NodeID,
			IpAddress:     metrics.IPAddress,
			RamUsedMb:     metrics.RAMUsedMB,
			CpuPercent:    metrics.CPUPercent,
			WorkersBusy:   metrics.WorkersBusy,
			WorkersTotal:  metrics.WorkersTotal,
			QueueSize:     metrics.QueueSize,
			QueueCapacity: metrics.QueueCapacity,
			TasksDone:     metrics.TasksDone,
			UptimeSeconds: metrics.UptimeSeconds,
			Status:        metrics.Status,
		},
	})
	_, err := c.client.SendHeartbeat(ctx, req)
	return err
}
