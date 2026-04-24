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
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	// Adaptadores
	adaptergrpc "orchestrator-node/internal/adapters/grpc"
	"orchestrator-node/internal/adapters/memory"
	"orchestrator-node/internal/adapters/system"

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
				return ipnet.IP.String()
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

// getDurationEnv lee una variable de entorno expresada en segundos (float) y la convierte a Duration.
func getDurationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	secs, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Printf("[Config] Variable %s='%s' no es un número válido. Usando default: %s", key, raw, fallback)
		return fallback
	}
	return time.Duration(secs * float64(time.Second))
}

func main() {
	// ── 0. Cargar entorno desde .env si existe ────────────────────────────────
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("Aviso: No se encontró archivo .env, usando variables del sistema o defaults.")
	}

	javaOrchestratorURL := getEnvOrDefault("JAVA_ORCHESTRATOR_URL", "http://localhost:9000")
	localGrpcPort       := getEnvOrDefault("LOCAL_GRPC_PORT", ":50051")
	pythonScript        := getEnvOrDefault("PYTHON_SCRIPT", "../../worker/worker.py")
	nodeID              := getEnvOrDefault("NODE_ID", generateNodeID())
	localIP             := getLocalIP()

	// Extraer el número de puerto (sin ":") para informarlo al servidor en el registro
	grpcPortNum := int32(50051)
	if portStr := getEnvOrDefault("LOCAL_GRPC_PORT", "50051"); portStr != "" {
		clean := portStr
		if len(clean) > 0 && clean[0] == ':' {
			clean = clean[1:]
		}
		if p, err := strconv.Atoi(clean); err == nil {
			grpcPortNum = int32(p)
		}
	}

	// ── 1. Calcular número de Workers (2*CPU - 1, mínimo 1) ──────────────────
	numWorkers := int32((2 * runtime.NumCPU()) - 1)
	if numWorkers < 1 {
		numWorkers = 1
	}

	// ── Configuración del RegistrationService desde variables de entorno ──────
	regCfg := services.RegistrationConfig{
		RetryBaseDelay:    getDurationEnv("REG_RETRY_BASE_SECS", 2*time.Second),
		RetryMaxDelay:     getDurationEnv("REG_RETRY_MAX_SECS", 60*time.Second),
		BackoffFactor:     2.0,
		MaxAttempts:       0, // 0 = infinito
		HeartbeatInterval: getDurationEnv("REG_HEARTBEAT_INTERVAL_SECS", 15*time.Second),
	}

	log.Printf("======================================")
	log.Printf(" Node ID        : %s", nodeID)
	log.Printf(" IP Local       : %s", localIP)
	log.Printf(" Puerto gRPC    : %s", localGrpcPort)
	log.Printf(" Python Script  : %s", pythonScript)
	log.Printf(" Server Java    : %s", javaOrchestratorURL)
	log.Printf(" Workers        : %d (2*CPU-1 = 2*%d-1)", numWorkers, runtime.NumCPU())
	log.Printf(" Reg. base      : %s | max: %s | hb: %s",
		regCfg.RetryBaseDelay, regCfg.RetryMaxDelay, regCfg.HeartbeatInterval)
	log.Printf("======================================")
	log.Println("Iniciando Go Node Agent (ConnectRPC + Supervisor)...")

	// ── 2. Base Context & Graceful Shutdown ───────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// ── 3. Inicializar Repositorio (Memoria) ──────────────────────────────────
	metricsRepo := memory.NewInMemoryMetricsRepo(nodeID, localIP, numWorkers, numWorkers)

	// ── 4. Configurar HTTP Client robusto para ConnectRPC ─────────────────────
	// Forzar HTTP/2 en texto plano (H2C): el cliente conecta sin TLS aunque el
	// protocolo sea HTTP/2, necesario para servidores Java sin certificado local.
	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			// DialTLS redirigido a Dial normal → HTTP/2 cleartext (H2C)
			DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
		Timeout: 30 * time.Second,
	}

	// ── 5. Inicializar Cliente ConnectRPC hacia el Orquestador Java ───────────
	orchClient := adaptergrpc.NewConnectOrchestratorClient(httpClient, javaOrchestratorURL, nodeID)

	// ── 6. Registro automático contra el servidor Java ────────────────────────
	// El RegistrationService:
	//   • Intenta registrar el nodo en el servidor en cuanto arranca.
	//   • Si el servidor no responde, reintenta con backoff exponencial
	//     (base=REG_RETRY_BASE_SECS, techo=REG_RETRY_MAX_SECS).
	//   • Una vez registrado, envía heartbeats periódicos (REG_HEARTBEAT_INTERVAL_SECS)
	//     para que el servidor sepa que el nodo sigue activo.
	//   • Si el heartbeat falla 3 veces seguidas, reinicia el ciclo de registro.
	registrationSvc := services.NewRegistrationService(orchClient, metricsRepo, regCfg, grpcPortNum)
	registrationSvc.Start(ctx)

	// ── 7. Instanciar Servicios (Worker Pool + Heartbeat de telemetría) ───────
	workerPoolSvc := services.NewWorkerPoolService(orchClient, pythonScript, metricsRepo, numWorkers)
	heartbeatSvc  := services.NewHeartbeatService(orchClient, metricsRepo, 5*time.Second)

	// ── 8. Arrancar Servicios en Background ───────────────────────────────────
	sysCollector := system.NewSystemMetricsCollector(metricsRepo, 5*time.Second)
	sysCollector.Start(ctx)

	workerPoolSvc.Start(ctx)
	heartbeatSvc.Start(ctx)

	// ── 9. Construir y exponer servidor HTTP/2 de Go (ConnectRPC) ─────────────
	// Registramos el handler generado por protoc-gen-connect-go para WorkerNode
	mux := http.NewServeMux()
	workerNodeServer := adaptergrpc.NewConnectWorkerServer(metricsRepo)

	path, handler := protoconnect.NewWorkerNodeHandler(workerNodeServer)
	mux.Handle(path, handler)

	// h2c permite HTTP/2 cleartext (sin TLS) en el lado servidor también
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

	// ── 10. Esperar señal de terminación y hacer Graceful Shutdown ────────────
	<-sigChan
	log.Println("Apagando agente temporal y cancelando goroutines prefetching...")
	cancel()

	// Dar 5 segundos para que el servidor HTTP/2 termine sus conexiones activas
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx) //nolint:errcheck

	log.Println("Go Node Agent apagado de forma inmaculada.")
}
