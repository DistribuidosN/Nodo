package services

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"orchestrator-node/internal/core/domain"
	"orchestrator-node/internal/core/ports"
)

type WorkerPoolService struct {
	client      ports.OrchestratorClient
	executor    ports.ScriptExecutor
	metricsRepo ports.MetricsRepository
	tasksChan   chan *domain.ImageTask
	numWorkers  int32
}

func NewWorkerPoolService(
	client ports.OrchestratorClient,
	executor ports.ScriptExecutor,
	metricsRepo ports.MetricsRepository,
	numWorkers int32,
) *WorkerPoolService {
	// 2. CONSTANTES Y COLA (PREFETCHING): queueSize = numWorkers * 2
	queueSize := numWorkers * 2
	return &WorkerPoolService{
		client:      client,
		executor:    executor,
		metricsRepo: metricsRepo,
		tasksChan:   make(chan *domain.ImageTask, queueSize),
		numWorkers:  numWorkers,
	}
}

// Start lanza los workers (numWorkers) y el Fetcher (productor).
func (s *WorkerPoolService) Start(ctx context.Context) {
	log.Printf("[WorkerPool] Supervisor iniciando pool con %d workers inmortales", s.numWorkers)

	for i := int32(1); i <= s.numWorkers; i++ {
		go s.workerSupervisor(ctx, i)
	}

	// 3. EL FETCHER (PRODUCTOR)
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
			// Solo pedimos si hay más de 1 slot libre (o adaptamos al gusto).
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
				// Queue seca por ahora
				time.Sleep(1 * time.Second)
				continue
			}

			// Producir y empujar tareas a la cola (se bloqueará automáticamente si se llena el buffer).
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
	
	// Función envoltorio para capturar panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Supervisor] 🚨 ALERTA CRÍTICA: %s sufrió un PANIC: %v\nStack: %s", workerID, r, string(debug.Stack()))
			
			// a) y b) son difíciles genéricamente aquí sin saber qué tarea estaba haciendo si paniqueó.
			// Lo resolveremos poniendo el recover iterativamente DE FORMA INDIVIDUAL POR TAREA.
			
			// c) Lanzar una NUEVA goroutine para reemlpcazarla y mantener numWorkers constante
			log.Printf("[Supervisor] Clonando un nuevo hilo de reemplazo para %s...", workerID)
			go s.workerSupervisor(ctx, id)
		}
	}()

	s.workerLoop(ctx, workerID)
}

func (s *WorkerPoolService) workerLoop(ctx context.Context, workerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.tasksChan:
			s.metricsRepo.IncrementBusyWorkers(1)

			// Sub-envoltorio de panic seguro para que el Panic no mate la goroutine, 
			// sino que aborte la tarea y procese la siguiente de la cola.
			s.processTaskSafe(ctx, workerID, task)

			s.metricsRepo.IncrementBusyWorkers(-1)
		}
	}
}

func (s *WorkerPoolService) processTaskSafe(ctx context.Context, workerID string, task *domain.ImageTask) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("PANIC durante ejecución: %v", r)
			log.Printf("[%s] Task %s colapsada: %s", workerID, task.TaskID, errMsg)
			
			// Reportamos el fallo al orquestador Java
			res := &domain.TaskResult{
				TaskID:   task.TaskID,
				NodeID:   s.metricsRepo.GetMetrics().NodeID,
				WorkerID: workerID,
				Success:  false,
				ErrorMsg: errMsg,
			}
			if err := s.client.SubmitResult(context.Background(), res); err != nil {
				log.Printf("[%s] Error reportando PANIC de %s: %v", workerID, task.TaskID, err)
			}
		}
	}()

	// 5. Gestión delegada al executor (incluye guardado de fichero en FileSystem)
	result, err := s.executor.ExecuteV2(ctx, task)
	if err != nil {
		// I/O Error en Go, el Executor ya debería proveer TaskResult encapsulado fallido.
		log.Printf("[%s] Task error I/O de executor: %v", workerID, err)
		if result == nil {
			result = &domain.TaskResult{
				TaskID: task.TaskID,
				Success: false,
				ErrorMsg: err.Error(),
			}
		}
	}

	result.NodeID = s.metricsRepo.GetMetrics().NodeID
	result.WorkerID = workerID
	
	s.metricsRepo.UpdateWithTaskResult(result)

	// Notificar resultado
	if err := s.client.SubmitResult(context.Background(), result); err != nil {
		log.Printf("[%s] Error enviando resultado de %s: %v", workerID, task.TaskID, err)
	}
}
