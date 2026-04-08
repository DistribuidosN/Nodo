# Demo extremo a extremo

Esta carpeta guarda artefactos reproducibles de una corrida completa contra los
workers del proyecto.

Archivos esperados despues de ejecutar `.\run.ps1 demo`:

- `demo-input.png`: imagen de entrada generada por el script de demo.
- `demo-output.png`: resultado principal de `ProcessToData`.
- `demo-batch-1.webp`: primer resultado del `ProcessBatch`.
- `demo-batch-2.jpeg` o `demo-batch-2.jpg`: segundo resultado del `ProcessBatch`.
- `demo-summary.json`: resumen estructurado con health, metricas y resultado.
- `demo-run.txt`: salida compacta de la corrida.

Flujo recomendado:

1. `.\run.ps1 worker-stack`
2. `.\run.ps1 demo`
3. revisar esta carpeta
4. `.\run.ps1 worker-down`
