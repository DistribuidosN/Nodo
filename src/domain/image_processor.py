"""
Lógica pura de procesamiento de imágenes.
"""
from __future__ import annotations

import io
import time

from config.settings import get_logger

log = get_logger("image_processor")

# ── Pillow opcional ───────────────────────────────────────────────────────────
try:
    from PIL import Image, ImageFilter, ImageOps
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    log.warning("Pillow no disponible — el procesamiento será simulado.")


def process_image(image_data: bytes, filter_type: str,
                  target_width: int = 0, target_height: int = 0) -> bytes:
    """
    Procesa los bytes de una imagen y devuelve los bytes del resultado.

    Filtros soportados:
        thumbnail  → redimensiona respetando aspecto  (default)
        grayscale  → escala de grises
        sobel      → detección de bordes
        blur       → desenfoque gaussiano
        ocr        → preprocesamiento para OCR (grises + sharpen)

    Si PIL no está disponible o image_data está vacío → simulación con sleep.
    """
    if not image_data or not PIL_AVAILABLE:
        sim_ms = max(20, min(300, len(image_data) // 5_000))
        time.sleep(sim_ms / 1000)
        return b"simulated_result"

    img = Image.open(io.BytesIO(image_data)).convert("RGB")
    ft  = (filter_type or "thumbnail").lower()

    if ft == "thumbnail":
        w = target_width  or 256
        h = target_height or 256
        img.thumbnail((w, h), Image.LANCZOS)

    elif ft == "grayscale":
        img = ImageOps.grayscale(img)

    elif ft == "sobel":
        gray = ImageOps.grayscale(img)
        img  = gray.filter(ImageFilter.FIND_EDGES)

    elif ft == "blur":
        img = img.filter(ImageFilter.GaussianBlur(radius=3))

    elif ft == "ocr":
        img = ImageOps.grayscale(img)
        img = img.filter(ImageFilter.SHARPEN)

    else:
        log.warning("filter_type='%s' desconocido, aplicando thumbnail", ft)
        img.thumbnail((256, 256), Image.LANCZOS)

    buf = io.BytesIO()
    fmt = "JPEG" if img.mode == "RGB" else "PNG"
    img.save(buf, format=fmt, quality=85, optimize=True)
    return buf.getvalue()
