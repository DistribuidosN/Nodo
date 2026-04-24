package grpc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

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
		client: protoconnect.NewOrchestratorClient(httpClient, baseURL, connect.WithGRPC()),
		nodeID: nodeID,
	}
}

func (c *ConnectOrchestratorClient) PullTasks(ctx context.Context, reqSlots int32, metrics *domain.NodeMetrics) ([]*domain.ImageTask, error) {
	req := connect.NewRequest(&pb.PullRequest{
		NodeId:    c.nodeID,
		SlotsFree: reqSlots,
		Metrics:   mapDomainToPbMetrics(metrics),
	})

	if metrics != nil {
		fmt.Printf("[PullTasks] 🔍 Solicitando %d tareas — CPU: %.1f%%, RAM: %.1f MB\n", reqSlots, metrics.CPUPercent, metrics.RAMUsedMB)
	}

	resp, err := c.client.PullTasks(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error PullTasks ConnectRPC: %w", err)
	}

	var tasks []*domain.ImageTask
	for _, t := range resp.Msg.Tasks {
		// Convertir pipeline gRPC a Dominio
		var pipeline []domain.TransformationItem
		var filterNames []string
		
		if t.TransformationPipeline != nil {
			for _, item := range t.TransformationPipeline.Items {
				pipeline = append(pipeline, domain.TransformationItem{
					Type:       item.Type,
					ParamsJSON: item.ParamsJson,
				})
				filterNames = append(filterNames, item.Type)
			}
		}

		tasks = append(tasks, &domain.ImageTask{
			TaskID:       t.TaskId,
			ImageData:    t.ImageData,
			Filename:     t.Filename,
			// Mantenemos FilterType como un string unido por comas por retrocompatibilidad
			FilterType:   strings.Join(filterNames, ","), 
			Pipeline:     pipeline,
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
	req := connect.NewRequest(&pb.TaskResult{
		TaskId:       result.TaskID,
		NodeId:       result.NodeID,
		WorkerId:     result.WorkerID,
		Success:      result.Success,
		ResultData:   result.ResultData,
		ErrorMsg:     result.ErrorMsg,
		StartTs:      result.StartTs,
		FinishTs:     result.FinishTs,
		ProcessingMs: result.ProcessingMs,
		Metrics:      mapDomainToPbMetrics(result.Metrics),
		Metadata:     result.Metadata,
	})

	if result.Metrics != nil {
		fmt.Printf("[SubmitResult] ✅ Task %s enviada — CPU: %.1f%%, RAM: %.1f MB, tiempo: %dms\n", 
			result.TaskID, result.Metrics.CPUPercent, result.Metrics.RAMUsedMB, result.ProcessingMs)
	}

	_, err := c.client.SubmitResult(ctx, req)
	return err
}

// RegisterNode registra (o actualiza) el nodo en el servidor Java.
//
// NOTA: Actualmente usa SendHeartbeat como transporte ya que RegisterNode es un RPC
// nuevo definido en el proto que aún requiere regeneración de código y ser implementado
// por el servidor Java. Una vez regenerado el proto con buf/protoc, esta función
// llamará directamente al RPC RegisterNode con el mensaje RegisterNodeRequest.
func (c *ConnectOrchestratorClient) RegisterNode(ctx context.Context, nodeID, ip string, port int32, metrics *domain.NodeMetrics) (string, error) {

	req := connect.NewRequest(&pb.RegisterNodeRequest{
		NodeId:    nodeID,
		IpAddress: ip,
		Port:      port,
		Metrics:   mapDomainToPbMetrics(metrics),
	})
	resp, err := c.client.RegisterNode(ctx, req)
	if err != nil {
		return "", fmt.Errorf("RegisterNode RPC falló: %w", err)
	}

	if !resp.Msg.Ok {
		return "", fmt.Errorf("servidor rechazó el registro: %s", resp.Msg.Msg)
	}

	return resp.Msg.Status, nil
}

// SendHeartbeat envía un heartbeat ligero (latido) al servidor con las métricas actuales.
func (c *ConnectOrchestratorClient) SendHeartbeat(ctx context.Context, metrics *domain.NodeMetrics) error {
	req := connect.NewRequest(&pb.HeartbeatRequest{
		NodeId:    metrics.NodeID,
		IpAddress: metrics.IPAddress,
		Metrics:   mapDomainToPbMetrics(metrics),
	})
	_, err := c.client.SendHeartbeat(ctx, req)
	return err
}

// LogEvent envía un log al orquestador de manera asíncrona (Fire & Forget).
func (c *ConnectOrchestratorClient) LogEvent(level, message, imageUUID string, transformationID int32) {
	// Regla: No loggear DEBUG para no saturar la red local.
	if level == "DEBUG" {
		return
	}

	// Ejecutar en goroutine separada para no bloquear el hilo de procesamiento de imagen
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := connect.NewRequest(&pb.LogRequest{
			NodeId:           c.nodeID,
			ImageUuid:        imageUUID,
			Level:            level,
			Message:          message,
			TransformationId: transformationID,
		})

		_, err := c.client.LogEvent(ctx, req)
		if err != nil {
			// Si el log falla, lo imprimimos localmente como última defensa
			fmt.Printf("[LOCAL-LOG-FAIL] %s: %s (UUID: %s)\n", level, message, imageUUID)
		}
	}()
}

