package domain

// ImageTask representa una tarea independiente de infraestructura (Protobuf).
type ImageTask struct {
	TaskID       string
	ImageData    []byte // Buffer de bytes cargado en memoria temporalmente
	Filename     string
	FilterType   string
	Priority     int32
	TargetWidth  int32
	TargetHeight int32
	ImageFormat  string
	Metadata     map[string]string
}


// TaskResult representa el resultado devuelto de un script de Python.
type TaskResult struct {
	TaskID       string
	NodeID       string
	WorkerID     string
	Success      bool
	ErrorMsg     string
	StartTs      int64
	FinishTs     int64
	ProcessingMs int32
	Metrics      *NodeMetrics
	Metadata     map[string]string
}
