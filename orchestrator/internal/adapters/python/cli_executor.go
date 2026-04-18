package python

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"orchestrator-node/internal/core/domain"
	"orchestrator-node/internal/core/ports"
)

type CLIExecutor struct {
	scriptPath string
}

func NewCLIExecutor(scriptPath string) ports.ScriptExecutor {
	return &CLIExecutor{
		scriptPath: scriptPath,
	}
}

func (e *CLIExecutor) ExecuteV2(ctx context.Context, task *domain.ImageTask) (*domain.TaskResult, error) {
	startTime := time.Now()
	
	// Pre-requisito I/O Seguro: Guardar byte payload a disco temporal
	tempDir := os.TempDir()
	ext := task.ImageFormat
	if ext == "" {
		ext = "bin"
	}
	
	fileName := fmt.Sprintf("task_%s_payload.%s", task.TaskID, ext)
	filePath := filepath.Join(tempDir, fileName)

	// Escribir a disco. Si entra en Pánico aquí o falla, el domain service se entera vía nil + error.
	errIO := os.WriteFile(filePath, task.ImageData, 0644)
	if errIO != nil {
		return nil, fmt.Errorf("error guardando I/O ImageData en %s: %w", filePath, errIO)
	}

	// 5. DEFER os.Remove asegurando limpiar disco para no tumbar la VM a largo plazo.
	defer func() {
		if err := os.Remove(filePath); err != nil {
			log.Printf("[CLIExecutor] Precaución: No se pudo eliminar temporal %s: %v", filePath, err)
		}
	}()

	// Invocación a Python usando la URI local
	cmd := exec.CommandContext(ctx, "python", e.scriptPath,
		"--task_id", task.TaskID,
		"--filter", task.FilterType,
		"--file_input", filePath, // Nueva variable indicando a Python donde leer
	)

	outBytes, errCmd := cmd.CombinedOutput()
	processingMs := int32(time.Since(startTime).Milliseconds())

	result := &domain.TaskResult{
		TaskID:       task.TaskID,
		Success:      true,
		StartTs:      startTime.UnixMilli(),
		FinishTs:     time.Now().UnixMilli(),
		ProcessingMs: processingMs,
	}

	if errCmd != nil {
		result.Success = false
		if exitError, ok := errCmd.(*exec.ExitError); ok {
			result.ErrorMsg = fmt.Sprintf("Python exit %d: %s", exitError.ExitCode(), string(outBytes))
		} else {
			result.ErrorMsg = fmt.Sprintf("OS Error: %v. Output: %s", errCmd, string(outBytes))
		}
		return result, nil
	}

	var pythonOutput struct {
		RAMUsedMB    float32 `json:"ram_used_mb"`
		CPUPercent   float32 `json:"cpu_percent"`
		AvgLatencyMs float32 `json:"avg_latency_ms"`
	}

	if err := json.Unmarshal(outBytes, &pythonOutput); err == nil {
		result.Metrics = &domain.NodeMetrics{
			RAMUsedMB:    pythonOutput.RAMUsedMB,
			CPUPercent:   pythonOutput.CPUPercent,
			AvgLatencyMs: pythonOutput.AvgLatencyMs,
		}
	}

	return result, nil
}
