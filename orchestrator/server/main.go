package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	// Adaptadores
	adaptergrpc "orchestrator-node/internal/adapters/grpc"
	"orchestrator-node/internal/adapters/memory"

	// Servicios
	"orchestrator-node/internal/core/services"

	// Protoconnect handlers
	"orchestrator-node/proto/protoconnect"
)

// getLocalIP escanea las interfaces de red del PC para obtener la IP real en la red local.
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip:= ipnet.IP.String()
				fmt.Println("IP Local: ", ip)
				return "127.0.0.1"
			}
		}
	}
	return "127.0.0.1"
}

// generateNodeID crea un identificador único aleatorio rápido (ej. go-agent-1f3a)
func generateNodeID() string {
	return fmt.Sprintf("go-agent-%d", time.Now().UnixNano()%100000)
}

func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func main() {
	// Cargar entorno desde .env si existe
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("Aviso: No se encontró archivo .env, usando variables del sistema o defaults.")
	}

	javaOrchestratorURL := getEnvOrDefault("JAVA_ORCHESTRATOR_URL", "http://localhost:9000")
	localGrpcPort := getEnvOrDefault("LOCAL_GRPC_PORT", ":50051")
	pythonScript := getEnvOrDefault("PYTHON_SCRIPT", "../../worker/worker.py")
	
	nodeID := getEnvOrDefault("NODE_ID", generateNodeID())
	localIP := getLocalIP()

	log.Printf("======================================")
	log.Printf(" Node ID        : %s", nodeID)
	log.Printf(" IP Local       : %s", localIP)
	log.Printf(" Puerto gRPC    : %s", localGrpcPort)
	log.Printf(" Python Script  : %s", pythonScript)
	log.Printf(" Server Java    : %s", javaOrchestratorURL)
	log.Printf("======================================")
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

	// 4. Configurar HTTP Client robusto para ConnectRPC (Forzar HTTP/2 en texto plano / H2C)
	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			// Esta función engaña internamente al cliente para que la conexión HTTP/2 no exija certificados
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
		Timeout: 30 * time.Second,
	}

	// 5. Inicializar Conexión a Java Orquestador (Cliente ConnectRPC)
	orchClient := adaptergrpc.NewConnectOrchestratorClient(httpClient, javaOrchestratorURL, nodeID)

	// 6. Instanciar Servicios (incluye Supervisor de Workers con IPC y Fetcher)
	workerPoolSvc := services.NewWorkerPoolService(orchClient, pythonScript, metricsRepo, numWorkers)
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
