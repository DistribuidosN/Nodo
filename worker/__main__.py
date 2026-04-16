import sys
from pathlib import Path

# Añadimos la carpeta 'proto' al sys.path para que los archivos generados por gRPC 
# puedan encontrarse entre sí (corrige el ModuleNotFoundError: No module named 'orchestrator_pb2')
sys.path.append(str(Path(__file__).parent.parent / "proto"))

from worker.server import main


if __name__ == "__main__":
    main()
