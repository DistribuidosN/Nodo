from __future__ import annotations
from abc import ABC, abstractmethod

class StoragePort(ABC):
    """
    Puerto para el almacenamiento de archivos.
    Desacopla la lógica de negocio de la ubicación física de los bytes.
    """

    @abstractmethod
    def read_bytes(self, uri: str) -> bytes:
        """Lee bytes de una ubicación."""
        pass

    @abstractmethod
    def write_bytes(self, uri: str, payload: bytes) -> None:
        """Escribe bytes en una ubicación."""
        pass

    @abstractmethod
    def read_text(self, uri: str) -> str:
        """Lee el contenido de un archivo como texto."""
        pass

    @abstractmethod
    def write_text(self, uri: str, content: str) -> None:
        """Escribe contenido de texto en una ubicación."""
        pass

    @abstractmethod
    def exists(self, uri: str) -> bool:
        """Verifica si una ubicación existe."""
        pass

    @abstractmethod
    def delete(self, uri: str) -> None:
        """Elimina un recurso."""
        pass

    @abstractmethod
    def mkdir(self, uri: str) -> None:
        """Crea un directorio si no existe."""
        pass

    @abstractmethod
    def list_uris(self, uri: str, suffix: str | None = None) -> list[str]:
        """Lista las URIs dentro de un directorio, opcionalmente filtrando por sufijo."""
        pass

    @abstractmethod
    def join(self, *parts: str) -> str:
        """Une varias partes de una ruta."""
        pass
