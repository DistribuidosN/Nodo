package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	// Adaptadores
	adaptergrpc "orchestrator-node/internal/adapters/grpc"
	"orchestrator-node/internal/adapters/memory"
	"orchestrator-node/internal/adapters/python"

	// Servicios
	"orchestrator-node/internal/core/services"

	// Protoconnect handlers
	"orchestrator-node/proto/protoconnect"
)

const (
	javaOrchestratorURL = "http://localhost:50051" // Base URL de Java
	localGrpcPort       = ":50052"
	nodeID              = "go-agent-1" // Puedes leerlo de os.Getenv("NODE_ID")
	localIP             = "127.0.0.1"
	pythonScript        = "../worker/mi_script.py" // Ajustar ruta
)

func main() {
	log.Println("Iniciando Go Node Agent (ConnectRPC + Supervisor)...")

	// 1. Calcular Workers
	numWorkers := int32((2 * runtime.NumCPU()) - 1)
	if numWorkers < 1 {
		numWorkers = 1
	}

	// 2. Base Context & Graceful Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Inicializar Repositorio (Memoria)
	metricsRepo := memory.NewInMemoryMetricsRepo(nodeID, localIP, numWorkers, numWorkers)

	// 4. Configurar HTTP Client robusto para ConnectRPC (soporte H2C opcional / puro)
	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
		},
		Timeout: 30 * time.Second,
	}

	// 5. Inicializar Conexión a Java Orquestador (Cliente ConnectRPC)
	orchClient := adaptergrpc.NewConnectOrchestratorClient(httpClient, javaOrchestratorURL, nodeID)

	// 6. Inicializar Executor (Python File I/O)
	pyExecutor := python.NewCLIExecutor(pythonScript)

	// 7. Instanciar Servicios (incluye Supervisor de Workers y Fetcher)
	workerPoolSvc := services.NewWorkerPoolService(orchClient, pyExecutor, metricsRepo, numWorkers)
	heartbeatSvc := services.NewHeartbeatService(orchClient, metricsRepo, 5*time.Second)

	// 8. Arrancar Cliente y Servicios (Background)
	workerPoolSvc.Start(ctx)
	heartbeatSvc.Start(ctx)

	// 9. Construir y exponen servidor HTTP/2 de Go (como servidor ConnectRPC)
	// Registraremos el handler nativo generado por protoc-gen-connect-go
	mux := http.NewServeMux()
	workerNodeServer := adaptergrpc.NewConnectWorkerServer(metricsRepo)
	
	path, handler := protoconnect.NewWorkerNodeHandler(workerNodeServer)
	mux.Handle(path, handler)

	// h2c permite HTTP/2 cleartext. 
	server := &http.Server{
		Addr:    localGrpcPort,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		log.Printf("[Connect-Server] El Node Agent ha expuesto su servidor en http://0.0.0.0%s", localGrpcPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("El servidor ConnectRPC colapsó: %v", err)
		}
	}()

	// 10. Esperar terminación y limpieza
	<-sigChan
	log.Println("Apagando agente temporal y cancelando goroutines prefetching...")
	cancel()
	
	// Graceful shutdown del HTTP Server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
	
	log.Println("Go Node Agent apagado de forma inmaculada.")
}
