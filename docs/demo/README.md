# Demo extremo a extremo

Esta carpeta guarda artefactos reproducibles de una corrida completa contra el coordinador y los tres workers.

Archivos esperados despues de ejecutar `.\run.ps1 demo`:

- `demo-input.png`: imagen de entrada generada por el script de demo.
- `demo-output.png`: resultado de `ProcessToData` pasando por el coordinador.
- `demo-batch-1.webp`: primer resultado del `ProcessBatch`.
- `demo-batch-2.jpeg` o `demo-batch-2.jpg`: segundo resultado del `ProcessBatch`.
- `demo-summary.json`: resumen estructurado con health, metricas y resultado.
- `demo-run.txt`: salida compacta de la corrida extremo a extremo.

Flujo:

1. `.\run.ps1 stack`
2. `.\run.ps1 demo`
3. revisar esta carpeta
4. `.\run.ps1 down`
