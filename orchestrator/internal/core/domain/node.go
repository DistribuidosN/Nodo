package domain

// NodeMetrics refleja las métricas asociadas a nuestro Worker en Go.
// Gran parte de estos números nos los proveerá Python al terminar una tarea (y nosotros los guardaremos en memoria).
type NodeMetrics struct {
	NodeID          string
	IPAddress       string
	RAMUsedMB       float32
	RAMTotalMB      float32
	CPUPercent      float32
	WorkersBusy     int32
	WorkersTotal    int32
	QueueSize       int32
	QueueCapacity   int32
	TasksDone       int32
	StealsPerformed int32
	AvgLatencyMs    float32
	P95LatencyMs    float32
	UptimeSeconds   int64
	Status          string
}
