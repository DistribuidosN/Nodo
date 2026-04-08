# Scripts

Organizacion actual:

- `backends/`: procesos auxiliares que el worker ejecuta para OCR e inferencia.
- `demo/`: corridas reproducibles contra un worker en ejecucion.
- `dev/`: utilidades para preparar entorno local y certificados.
- `ops/`: healthchecks y tareas operativas simples.

Si agregas un script nuevo, intenta ponerlo en una de esas carpetas antes de
dejarlo suelto en la raiz.
