from __future__ import annotations
import json
import logging
import math
import os
import subprocess
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import cast
from PIL import Image, ImageColor, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

from worker.application.ports.image_processor import ImageProcessorPort
from worker.application.ports.storage import StoragePort
from worker.domain.models import OperationType, Task
from worker.domain.exceptions import TaskCancelledError, UnsupportedOperationError

FORMAT_MAP = {
    "jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "tif": "TIFF",
    "tiff": "TIFF", "webp": "WEBP", "bmp": "BMP", "gif": "GIF",
    "ico": "ICO",
}

INTERNAL_RESULT_METADATA_KEYS = {
    "node_id", "execution_isolation", "child_pid", "ocr_command",
    "inference_command", "adapter_timeout_seconds",
}

class PillowAdapter(ImageProcessorPort):
    """
    Adaptador que implementa el procesamiento de imágenes usando la librería Pillow.
    """

    def __init__(
        self, 
        storage: StoragePort, 
        default_tile_size: int = 256,
        ocr_command: str | None = None,
        inference_command: str | None = None
    ) -> None:
        self._storage = storage
        self._tile_size_default = default_tile_size
        self._ocr_command = ocr_command
        self._inference_command = inference_command
        self._logger = logging.getLogger("worker.processor.pillow")

    def process_task(self, task: Task, output_dir: str) -> tuple[str, str, int, int, int, dict[str, str]]:
        self._check_cancelled(task)
        image = self._load_image(task)
        
        for transform in task.transforms:
            image = self._apply_transform_checked(task, image, transform.operation, transform.params)

        requested_format = (task.output_format or task.input_image.image_format or "png").lower()
        output_bytes, _ = self.serialize_image(image, requested_format)
        width, height = image.size
        
        output_path_str, size_bytes = self._persist_output(task.task_id, output_bytes, requested_format, output_dir)
        
        metadata = self._build_metadata(task, processor="pillow", output_backend=self._get_backend(output_path_str))
        return output_path_str, requested_format, width, height, size_bytes, metadata

    def process_transform_stage(
        self,
        task: Task,
        payload: bytes,
        operation: OperationType,
        params: dict[str, str],
        stage_output_format: str = "png"
    ) -> tuple[bytes, str, int, int, dict[str, str]]:
        self._check_cancelled(task)
        image = Image.open(BytesIO(payload)).copy()
        image = self._apply_transform_checked(task, image, operation, params)
        
        output_bytes, save_format = self.serialize_image(image, stage_output_format)
        width, height = image.size
        metadata = self._build_metadata(task, processor="pillow", stage_operation=operation.value)
        return output_bytes, stage_output_format, width, height, metadata

    # --- Métodos Privados de Lógica (Migrados de image_processor.py) ---

    def _load_image(self, task: Task) -> Image.Image:
        if task.input_image.payload:
            payload = task.input_image.payload
        elif task.input_image.input_path:
            payload = Path(task.input_image.input_path).read_bytes()
        elif task.input_image.input_uri:
            payload = self._storage.read_bytes(task.input_image.input_uri)
        else:
            raise ValueError("No image source found in task")
        
        return Image.open(BytesIO(payload)).copy()

    def serialize_image(self, image: Image.Image, requested_format: str) -> tuple[bytes, str]:
        requested_format = requested_format.lower()
        save_format = FORMAT_MAP.get(requested_format, requested_format.upper())
        buffer = BytesIO()
        image.save(buffer, format=save_format)
        return buffer.getvalue(), save_format

    def load_input_bytes(self, task: Task) -> tuple[bytes, str]:
        """Extrae los bytes de entrada de una tarea."""
        if task.input_image.payload:
            return task.input_image.payload, (task.input_image.image_format or "png").lower()
        if task.input_image.input_path:
            path = Path(task.input_image.input_path)
            return path.read_bytes(), (task.input_image.image_format or path.suffix.lstrip(".") or "png").lower()
        if task.input_image.input_uri:
            payload = self._storage.read_bytes(task.input_image.input_uri)
            parsed_suffix = Path(task.input_image.input_uri).suffix.lstrip(".")
            image_format = (task.input_image.image_format or parsed_suffix or "png").lower()
            return payload, image_format
        raise ValueError("task does not contain an image source")

    def persist_output_bytes(self, task_id: str, output_bytes: bytes, requested_format: str, output_dir: str) -> tuple[str, int]:
        """Persiste los bytes de salida en el almacenamiento."""
        output_path = Path(output_dir) / f"{task_id}.{requested_format}"
        self._storage.write_bytes(str(output_path), output_bytes)
        return str(output_path), output_path.stat().st_size if output_path.exists() else 0

    def _persist_output(self, task_id: str, output_bytes: bytes, requested_format: str, output_dir: str) -> tuple[str, int]:
        output_path = Path(output_dir) / f"{task_id}.{requested_format}"
        self._storage.write_bytes(str(output_path), output_bytes)
        return str(output_path), output_path.stat().st_size if output_path.exists() else 0

    def _check_cancelled(self, task: Task) -> None:
        if task.cancel_token_path and Path(task.cancel_token_path).exists():
            raise TaskCancelledError(f"task {task.task_id} cancelled")

    def _apply_transform_checked(self, task: Task, image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
        self._check_cancelled(task)
        if operation == OperationType.RESIZE:
            return self._resize_cooperative(task, image, params)
        if operation == OperationType.ROTATE:
            return self._rotate_cooperative(task, image, params)
        if operation == OperationType.BLUR:
            return self._blur_cooperative(task, image, params)
        if operation == OperationType.SHARPEN:
            return self._sharpen_cooperative(task, image, params)
        if operation == OperationType.BRIGHTNESS_CONTRAST:
            return self._brightness_contrast_cooperative(task, image, params)
        
        self._check_step(task, params)
        return self._apply_core_transform(task, image, operation, params)

    def _apply_core_transform(self, task: Task, image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
        if operation == OperationType.GRAYSCALE:
            return ImageOps.grayscale(image)
        if operation == OperationType.CROP:
            box = (float(params["left"]), float(params["upper"]), float(params["right"]), float(params["lower"]))
            return image.crop(box)
        if operation == OperationType.FLIP:
            direction = params.get("direction", "horizontal").lower()
            if direction == "vertical": return ImageOps.flip(image)
            if direction == "both": return ImageOps.flip(ImageOps.mirror(image))
            return ImageOps.mirror(image)
        if operation == OperationType.WATERMARK_TEXT:
            return self._apply_watermark_text(image, params)
        if operation in {OperationType.OCR, OperationType.INFERENCE}:
            return self._run_adapter_operation(task, image, operation, params)
        return image

    def _check_step(self, task: Task, params: dict[str, str]) -> None:
        delay_ms = int(params.get("delay_ms", "0"))
        if delay_ms > 0: time.sleep(delay_ms / 1000.0)
        self._check_cancelled(task)

    def _build_metadata(self, task: Task, **extras: str) -> dict[str, str]:
        metadata = {k: v for k, v in task.metadata.items() if k not in INTERNAL_RESULT_METADATA_KEYS}
        metadata.update(extras)
        return metadata

    def _get_backend(self, path: str) -> str:
        return "uri" if "://" in path else "filesystem"

    # --- Métodos Cooperativos ---

    def _resize_cooperative(self, task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
        target_width = int(params["width"])
        target_height = int(params["height"])
        if image.size == (target_width, target_height): return image
        output = Image.new(image.mode, (target_width, target_height))
        tile = int(params.get("tile_size", str(self._tile_size_default)))
        scale_x, scale_y = image.width / target_width, image.height / target_height
        for top in range(0, target_height, tile):
            for left in range(0, target_width, tile):
                self._check_step(task, params)
                th, tw = min(tile, target_height - top), min(tile, target_width - left)
                box = (int(left * scale_x), int(top * scale_y), int((left + tw) * scale_x), int((top + th) * scale_y))
                patch = image.crop(box).resize((tw, th), resample=Image.Resampling.LANCZOS)
                output.paste(patch, (left, top))
        return output

    def _rotate_cooperative(self, task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
        angle, expand = float(params.get("angle", "0")), params.get("expand", "true").lower() == "true"
        self._check_step(task, params)
        return image.rotate(angle, expand=expand, resample=Image.Resampling.BICUBIC)

    def _blur_cooperative(self, task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
        radius = float(params.get("radius", "1.5"))
        passes = max(1, math.ceil(radius / 2.0))
        curr = image
        for _ in range(passes):
            self._check_step(task, params)
            curr = curr.filter(ImageFilter.GaussianBlur(radius=radius/passes))
        return curr

    def _sharpen_cooperative(self, task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
        self._check_step(task, params)
        return ImageEnhance.Sharpness(image).enhance(float(params.get("factor", "2.0")))

    def _brightness_contrast_cooperative(self, task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
        b, c = float(params.get("brightness", "1.0")), float(params.get("contrast", "1.0"))
        self._check_step(task, params)
        image = ImageEnhance.Brightness(image).enhance(b)
        self._check_step(task, params)
        return ImageEnhance.Contrast(image).enhance(c)

    def _apply_watermark_text(self, image: Image.Image, params: dict[str, str]) -> Image.Image:
        text = params.get("text", "WATERMARK")
        draw = ImageDraw.Draw(image)
        draw.text((10, 10), text, fill=(255, 255, 255, 128))
        return image

    def _run_adapter_operation(self, task: Task, image: Image.Image, op: OperationType, params: dict[str, str]) -> Image.Image:
        command = self._ocr_command if op == OperationType.OCR else self._inference_command
        if not command:
            self._logger.warning(f"Operation {op} requested but no command configured")
            return image

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.png")
            output_json = os.path.join(tmpdir, "output.json")
            
            # Guardamos la imagen actual para que el backend la procese
            image.save(input_file, format="PNG")
            
            env = os.environ.copy()
            env["WORKER_INPUT_FILE"] = input_file
            env["WORKER_OUTPUT_JSON"] = output_json
            env["WORKER_PARAMS_JSON"] = json.dumps(params)
            
            try:
                # Ejecutamos el backend externo
                result = subprocess.run(
                    command,
                    shell=True,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=params.get("timeout_seconds", 30) # Default timeout
                )
                
                if result.returncode != 0:
                    self._logger.error(f"Backend {op} failed (code {result.returncode}): {result.stderr}")
                    return image
                
                # Leemos el resultado del JSON
                if os.path.exists(output_json):
                    with open(output_json, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # Inyectamos el resultado en los metadatos de la tarea para este hilo de ejecución
                        # Nota: Usamos una convención de nombres para que Java pueda identificarlo.
                        key = "extracted_text" if op == OperationType.OCR else "classification_result"
                        # Serializamos el JSON interno a string para cumplir con map<string, string> de gRPC
                        task.metadata[key] = json.dumps(data, ensure_ascii=True)
                        self._logger.info(f"AI Metadata captured for task {task.task_id}: {key}")
                
            except subprocess.TimeoutExpired:
                self._logger.error(f"Backend {op} timed out for task {task.task_id}")
            except Exception as e:
                self._logger.error(f"Error running backend {op}: {e}")

        return image
