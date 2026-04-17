class DomainError(Exception):
    """Excepción base para todos los errores de dominio."""
    pass

class TaskProcessingError(DomainError):
    """Error al procesar una tarea de imagen."""
    pass

class UnsupportedOperationError(TaskProcessingError):
    """La operación de imagen solicitada no está soportada por el adaptador actual."""
    pass

class TaskCancelledError(DomainError):
    """La tarea ha sido cancelada por el usuario o el sistema."""
    pass

class ResourceError(DomainError):
    """Error relacionado con recursos (memoria, almacenamiento, etc)."""
    pass
