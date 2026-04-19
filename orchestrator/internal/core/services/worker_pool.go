package services

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"orchestrator-node/internal/core/domain"
	"orchestrator-node/internal/core/ports"
)

type WorkerPoolService struct {
	client      ports.OrchestratorClient
	scriptPath  string
	metricsRepo ports.MetricsRepository
	tasksChan   chan *domain.ImageTask
	numWorkers  int32
}

func NewWorkerPoolService(
	client ports.OrchestratorClient,
	scriptPath string,
	metricsRepo ports.MetricsRepository,
	numWorkers int32,
) *WorkerPoolService {
	queueSize := numWorkers * 2
	return &WorkerPoolService{
		client:      client,
		scriptPath:  scriptPath,
		metricsRepo: metricsRepo,
		tasksChan:   make(chan *domain.ImageTask, queueSize),
		numWorkers:  numWorkers,
	}
}

// Start lanza los workers (numWorkers) y el Fetcher (productor).
func (s *WorkerPoolService) Start(ctx context.Context) {
	log.Printf("[WorkerPool] Supervisor iniciando pool con %d workers persistentes", s.numWorkers)

	for i := int32(1); i <= s.numWorkers; i++ {
		go s.workerSupervisor(ctx, i)
	}

	go s.fetcherLoop(ctx)
}

func (s *WorkerPoolService) fetcherLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("[Fetcher] Contexto cancelado. Deteniendo prefetching.")
			return
		default:
			queueCap := int32(cap(s.tasksChan))
			currentLen := int32(len(s.tasksChan))
			slotsFree := queueCap - currentLen

			s.metricsRepo.SetQueueSize(currentLen)

			// Backpressure simple: Si la cola está llena, o con poco espacio, dormimos
			if slotsFree <= 0 {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			tasks, err := s.client.PullTasks(ctx, slotsFree)
			if err != nil {
				log.Printf("[Fetcher] Error pidiendo tareas: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if len(tasks) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			// Productor a la cola local
			for _, t := range tasks {
				select {
				case <-ctx.Done():
					return
				case s.tasksChan <- t:
				}
			}
			s.metricsRepo.SetQueueSize(int32(len(s.tasksChan)))
		}
	}
}

// 4. WORKER POOL AUTO-SANABLE (SUPERVISOR PATTERN)
func (s *WorkerPoolService) workerSupervisor(ctx context.Context, id int32) {
	workerID := fmt.Sprintf("w-%d", id)
	
	// Función envoltorio para capturar panics masivos de la goroutine entera
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Supervisor] 🚨 ALERTA CRÍTICA: %s sufrió un PANIC: %v\nStack: %s", workerID, r, string(debug.Stack()))
			log.Printf("[Supervisor] Clonando un nuevo hilo de reemplazo para %s...", workerID)
			go s.workerSupervisor(ctx, id)
		}
	}()

	s.workerLoop(ctx, workerID)
}

func (s *WorkerPoolService) workerLoop(ctx context.Context, workerID string) {
	// Bucle exterior de persistencia del sub-proceso Python
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Arrancar el proceso de Python persistente
		cmd := exec.CommandContext(ctx, "python", s.scriptPath)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			log.Fatalf("[%s] Error obteniendo STDIN de python: %v", workerID, err)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Fatalf("[%s] Error obteniendo STDOUT de python: %v", workerID, err)
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[%s] Error arrancando python: %v. Reintentando en 2s...", workerID, err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("[%s] Subproceso Python (PID: %d) IPC conectado", workerID, cmd.Process.Pid)
		scanner := bufio.NewScanner(stdout)

		// Bucle interior: Alimentar tareas a este subproceso
		s.processTasksInPipe(ctx, workerID, stdin, scanner, cmd)
	}
}

// processTasksInPipe extrae continuamente de s.tasksChan y las pasa al Worker Python activo
func (s *WorkerPoolService) processTasksInPipe(ctx context.Context, workerID string, stdin io.WriteCloser, scanner *bufio.Scanner, cmd *exec.Cmd) {
	for {
		var task *domain.ImageTask
		select {
		case <-ctx.Done():
			cmd.Process.Kill()
			return
		case task = <-s.tasksChan:
		}

		s.metricsRepo.IncrementBusyWorkers(1)
		
		healthy := s.processSingleTaskSafe(ctx, workerID, task, stdin, scanner, cmd)
		
		s.metricsRepo.IncrementBusyWorkers(-1)

		if !healthy {
			// Si processSingleTaskSafe devuelve false, el proceso Python colapsó o el pipe roto
			// Se rompe este bucle y workerLoop generará un proceso Python nuevo.
			break
		}
	}
}

type pythonRequest struct {
	TaskID       string                   `json:"task_id"`
	InputPath    string                   `json:"input_path"`
	OutputDir    string                   `json:"output_dir"`
	OutputFormat string                   `json:"output_format"`
	Transforms   []map[string]interface{} `json:"transforms"`
}

type pythonResponse struct {
	TaskID     string            `json:"task_id"`
	Success    bool              `json:"success"`
	ResultPath string            `json:"result_path"`
	Error      string            `json:"error"`
	Metadata   map[string]string `json:"metadata"`
}

// processSingleTaskSafe envuelve una tarea en un Panic Recovery específico, gestiona I/O y reporte RPC.
// Retorna 'true' si el conducto de comunicación (Python) sigue sano.
func (s *WorkerPoolService) processSingleTaskSafe(ctx context.Context, workerID string, task *domain.ImageTask, stdin io.WriteCloser, scanner *bufio.Scanner, cmd *exec.Cmd) bool {
	startTime := time.Now()
	var inputPath string

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] Task %s colapsó con PANIC: %v. Re-encolando para otro worker...", workerID, task.TaskID, r)
			if inputPath != "" {
				os.Remove(inputPath)
			}
			go func() { s.tasksChan <- task }() // Auto-sanación: Devolver tarea a cola
			cmd.Process.Kill() // Eliminar al zombie manchado
		}
	}()

	// 1. I/O: Salvar los bits de red al FileSystem Temporal Local
	tempDir := os.TempDir()
	ext := task.ImageFormat
	if ext == "" {
		ext = "png"
	}
	inputPath = filepath.Join(tempDir, fmt.Sprintf("in_%s.%s", task.TaskID, ext))
	if err := os.WriteFile(inputPath, task.ImageData, 0644); err != nil {
		s.reportFail(workerID, task.TaskID, fmt.Sprintf("Error I/O guardando imagen: %v", err))
		return true // Falló la tarea pero el Python sigue sano, no hace falta romper tubería
	}
	defer os.Remove(inputPath)

	// 2. Construir la Petición JSON-RPC
	req := pythonRequest{
		TaskID:       task.TaskID,
		InputPath:    inputPath,
		OutputDir:    tempDir,
		OutputFormat: ext,
		Transforms:   make([]map[string]interface{}, 0),
	}
	
	cleanFilters := strings.Trim(task.FilterType, "[]")
	operations := strings.Split(cleanFilters, ",")

	for _, op := range operations {
		opClean := strings.TrimSpace(op)
		if opClean == "" {
			continue
		}
		
		transform := map[string]interface{}{
			"operation": opClean,
			"params":    make(map[string]interface{}),
		}
		params := transform["params"].(map[string]interface{})
		
		// Inyectamos width y height comunes (operaciones como 'resize' las usarán)
		if task.TargetWidth > 0 {
			params["width"] = strconv.Itoa(int(task.TargetWidth))
		}
		if task.TargetHeight > 0 {
			params["height"] = strconv.Itoa(int(task.TargetHeight))
		}
		req.Transforms = append(req.Transforms, transform)
	}

	jsonTask, _ := json.Marshal(req)

	// 3. Escribir a Python via STDIN + Salto de línea
	if _, err := fmt.Fprintln(stdin, string(jsonTask)); err != nil {
		log.Printf("[%s] Excepción escribiendo STDIN de %s: %v", workerID, task.TaskID, err)
		go func() { s.tasksChan <- task }()
		cmd.Process.Kill()
		return false // Tubería rota
	}

	// 4. Bloquear y leer respuesta de Python vía STDOUT
	if !scanner.Scan() {
		errScan := scanner.Err()
		log.Printf("[%s] EOF/Fallo en STDOUT de Python (PID %d): %v", workerID, cmd.Process.Pid, errScan)
		go func() { s.tasksChan <- task }()
		cmd.Process.Kill()
		return false // Proceso Python murió. Devolveremos "false" para que inicie otro.
	}

	responseLine := scanner.Text()
	
	// 5. Interpretar la respuesta JSON
	var resp pythonResponse
	if err := json.Unmarshal([]byte(responseLine), &resp); err != nil {
		log.Printf("[%s] JSON Inválido de Python: '%s' Error: %v", workerID, responseLine, err)
		s.reportFail(workerID, task.TaskID, "Python devolvió un formato no legible")
		return true // La tarea falló pero el pipe quizá sirva (o matar si se prefiere estricto)
	}

	// 6. Preparar Resultado Enriquecido para enviarlo al Orquestador Java
	result := &domain.TaskResult{
		TaskID:       task.TaskID,
		NodeID:       s.metricsRepo.GetMetrics().NodeID,
		WorkerID:     workerID,
		Success:      resp.Success,
		ErrorMsg:     resp.Error,
		Metadata:     resp.Metadata,     // AI / OCR Context Data
		StartTs:      startTime.UnixMilli(),
		FinishTs:     time.Now().UnixMilli(),
		ProcessingMs: int32(time.Since(startTime).Milliseconds()),
	}

	if resp.Success {
		// Recuperar bytes del ResultPath creado por Python
		resBytes, err := os.ReadFile(resp.ResultPath)
		if err != nil {
			result.Success = false
			result.ErrorMsg = fmt.Sprintf("Error leyendo imagen de salida %s: %v", resp.ResultPath, err)
		} else {
			result.ResultData = resBytes
		}
		// Limpieza de disco local
		_ = os.Remove(resp.ResultPath)
	}

	s.metricsRepo.UpdateWithTaskResult(result)

	// 7. Notificar el fallo/éxito a Java
	if err := s.client.SubmitResult(context.Background(), result); err != nil {
		log.Printf("[%s] Error RPC reportando a Java (%s): %v", workerID, task.TaskID, err)
	}

	return true // El subproceso sigue existiendo sano y salvo para más tareas
}

func (s *WorkerPoolService) reportFail(workerID string, taskID string, errMsg string) {
	res := &domain.TaskResult{
		TaskID:   taskID,
		NodeID:   s.metricsRepo.GetMetrics().NodeID,
		WorkerID: workerID,
		Success:  false,
		ErrorMsg: errMsg,
	}
	s.client.SubmitResult(context.Background(), res)
}
