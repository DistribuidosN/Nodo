import sys
import json
import traceback
import os
import math
from io import BytesIO
from pathlib import Path

# SECRETO PROFESIONAL IPC: Redirigimos ruido oculto de IA a STDERR
# para no contaminar STDOUT y romper el JSON RPC de Go
ORIGINAL_STDOUT = sys.stdout
sys.stdout = sys.stderr

# Imports de la librería Pillow (necesita estar instalada: pip install Pillow)
from PIL import Image, ImageEnhance, ImageFilter, ImageDraw, ImageOps, ImageStat

try:
    import pytesseract
except ImportError:
    pytesseract = None

import numpy as np

# Mapeo de formatos comunes
FORMAT_MAP = {
    "jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "tif": "TIFF",
    "tiff": "TIFF", "webp": "WEBP", "bmp": "BMP", "gif": "GIF",
    "ico": "ICO",
}

class ImageWorker:
    """
    Motor de procesamiento de imágenes aislado.
    No requiere dependencias de red pura, funciona de manera local sobre sistema de archivos.
    """
    def __init__(self):
        # Configuración explícita de Tesseract en Windows
        self.tesseract_cmd = os.environ.get("WORKER_TESSERACT_CMD", r'C:\Program Files\Tesseract-OCR\tesseract.exe')
        if pytesseract:
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd
            
        self.inference_model = self._load_inference_model() # Retrocompatibilidad
        
    def _load_inference_model(self):
        """Carga el modelo de inferencia en memoria al iniciar el worker."""
        try:
            root = Path(__file__).resolve().parent
            model_path = root / "backends" / "inference_model.json"
            if model_path.exists():
                return json.loads(model_path.read_text(encoding="utf-8"))
        except Exception as e:
            # Tolerancia a fallos: permitimos arrancar sin el modelo, se notifica en el task.
            pass
        return None

    def process_task(self, req: dict) -> dict:
        """
        Ejecuta el pipeline de procesamiento según los requerimientos del JSON.
        """
        task_id = req.get("task_id", "unknown")
        input_path = req.get("input_path")
        output_dir = req.get("output_dir", "/tmp")
        transforms = req.get("transforms", [])
        output_format = req.get("output_format", "png").lower()
        
        # Compatibilidad si el padre envía "filter_type" en vez de una cadena de "transforms"
        if "filter_type" in req and not transforms:
            transforms = [{"operation": req["filter_type"], "params": req.get("params", {})}]
            
        if not input_path:
            raise ValueError("input_path is required")

        # Carga la imagen destino (en un buffer en memoria para no bloquear archivos)
        payload = Path(input_path).read_bytes()
        image = Image.open(BytesIO(payload)).copy()
        
        # --- NORMALIZACIÓN DE IMAGEN A RGB PURO (Previene el error "wrong mode") ---
        if image.mode in ('RGBA', 'LA', 'P'):
            if image.mode == 'P':
                image = image.convert('RGBA')
            fondo_blanco = Image.new('RGB', image.size, (255, 255, 255))
            # Pegamos la imagen usando el canal alpha como máscara
            fondo_blanco.paste(image, mask=image.split()[-1])
            image = fondo_blanco
        elif image.mode != 'RGB':
            image = image.convert('RGB')
        # ---------------------------------------------------------------------------

        metadata = {}

        # Aplica secuencialmente cada transformación solicitada
        for transform in transforms:
            operation = transform.get("operation", "").lower()
            params = transform.get("params", {})
            print(f"[Python-Worker] Aplicando {operation} a la tarea {task_id}", file=sys.stderr)
            image, step_metadata = self._apply_transform(image, operation, params)
            if step_metadata:
                metadata.update(step_metadata)

        # Prepara la ruta de salida
        save_format = FORMAT_MAP.get(output_format, output_format.upper())
        out_path = Path(output_dir) / f"{task_id}.{output_format}"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Guarda y extrae estadísticas
        image.save(out_path, format=save_format)
        width, height = image.size
        size_bytes = out_path.stat().st_size if out_path.exists() else 0
        
        return {
            "task_id": task_id,
            "success": True,
            "result_path": str(out_path),
            "width": width,
            "height": height,
            "size_bytes": size_bytes,
            "metadata": metadata,
            "error": ""
        }

    def _apply_transform(self, image: Image.Image, operation: str, params: dict) -> tuple[Image.Image, dict]:
        """Aplica la transformación específica usando Pillow y motores internos."""
        metadata = {}
        op = operation.lower()

        if op == "grayscale":
            intensity = float(params.get("intensity", 1.0))
            gray = ImageOps.grayscale(image).convert("RGB")
            if intensity < 1.0:
                image = Image.blend(image, gray, intensity)
            else:
                image = gray
            
        elif op == "resize":
            width, height = image.size
            tw = params.get("width")
            th = params.get("height")
            scale = params.get("scale")

            if scale:
                s = float(scale)
                nw, nh = int(width * s), int(height * s)
            else:
                nw = int(tw) if tw else width
                nh = int(th) if th else height
            
            image = image.resize((nw, nh), Image.Resampling.LANCZOS)
            
        elif op == "pixelate":
            # Efecto de censura o estilo retro
            block_size = int(params.get("block_size", 10))
            if block_size > 1:
                orig_size = image.size
                small = image.resize((orig_size[0] // block_size, orig_size[1] // block_size), Image.Resampling.NEAREST)
                image = small.resize(orig_size, Image.Resampling.NEAREST)

        elif op == "sepia":
            intensity = float(params.get("intensity", 1.0))
            # Matriz de transformación sepia estándar
            def make_sepia(img):
                width, height = img.size
                pixels = img.load()
                for py in range(height):
                    for px in range(width):
                        r, g, b = pixels[px, py]
                        tr = int(0.393 * r + 0.769 * g + 0.189 * b)
                        tg = int(0.349 * r + 0.686 * g + 0.168 * b)
                        tb = int(0.272 * r + 0.534 * g + 0.131 * b)
                        pixels[px, py] = (min(tr, 255), min(tg, 255), min(tb, 255))
                return img
            
            sepia_img = make_sepia(image.copy())
            image = Image.blend(image, sepia_img, intensity)

        elif op == "color_tint":
            # Tiñe la imagen con un color específico (R, G, B)
            r = int(params.get("r", 255))
            g = int(params.get("g", 255))
            b = int(params.get("b", 255))
            intensity = float(params.get("intensity", 0.3))
            tint = Image.new("RGB", image.size, (r, g, b))
            image = Image.blend(image, tint, intensity)

        elif op == "posterize":
            # Reduce bits por canal (1 a 8). 1-4 produce efectos tipo cómic/Warhol
            bits = int(params.get("bits", 4))
            image = ImageOps.posterize(image, min(max(bits, 1), 8))

        elif op == "crop":
            box = (float(params["left"]), float(params["upper"]), float(params["right"]), float(params["lower"]))
            image = image.crop(box)
            
        elif op == "rotate":
            angle = float(params.get("angle", 0))
            expand = str(params.get("expand", "true")).lower() == "true"
            fill_color = params.get("fill_color", None) 
            image = image.rotate(angle, expand=expand, resample=Image.Resampling.BICUBIC, fillcolor=fill_color)
            
        elif op == "flip":
            direction = params.get("direction", "horizontal").lower()
            if direction == "vertical": 
                image = ImageOps.flip(image)
            elif direction == "both": 
                image = ImageOps.flip(ImageOps.mirror(image))
            else: 
                image = ImageOps.mirror(image)
                
        elif op == "blur":
            radius = float(params.get("radius", 2.0))
            image = image.filter(ImageFilter.GaussianBlur(radius=radius))
            
        elif op == "sharpen":
            factor = float(params.get("factor", 2.0))
            image = ImageEnhance.Sharpness(image).enhance(factor)
            
        elif op in ["brightness", "contrast", "brightness_contrast"]:
            b = float(params.get("brightness", 1.0))
            c = float(params.get("contrast", 1.0))
            if b != 1.0:
                image = ImageEnhance.Brightness(image).enhance(b)
            if c != 1.0:
                image = ImageEnhance.Contrast(image).enhance(c)
            
        elif op == "watermark_text":
            text = params.get("text", "A2WS NODE")
            opacity = int(params.get("opacity", 80))
            font_size = int(params.get("size", 60))
            angle = float(params.get("angle", 45))
            color = params.get("color", "white").lower()
            
            color_map = {"white": (255, 255, 255), "black": (0, 0, 0), "red": (255, 0, 0), "blue": (0, 0, 255)}
            rgb_color = color_map.get(color, (255, 255, 255))
            fill_tuple = (*rgb_color, opacity)
            stroke_tuple = (0, 0, 0, opacity) if color != "black" else (255, 255, 255, opacity)

            is_rgb = image.mode != "RGBA"
            if is_rgb: image = image.convert("RGBA")
            
            width, height = image.size
            try:
                from PIL import ImageFont
                font = ImageFont.truetype("arial.ttf", font_size)
            except:
                font = ImageFont.load_default()

            diag = int(math.hypot(width, height))
            overlay = Image.new("RGBA", (diag, diag), (255, 255, 255, 0))
            draw = ImageDraw.Draw(overlay)
            
            try:
                left, top, right, bottom = font.getbbox(text)
                tw, th = right - left, bottom - top
            except:
                tw, th = font.getsize(text)
                
            tw = max(int(tw), 1) + 150
            th = max(int(th), 1) + 150
            
            for y in range(0, diag, th):
                offset_x = (y // th) % 2 * (tw // 2)
                for x in range(-tw, diag, tw):
                    draw.text((x + offset_x, y), text, font=font, fill=fill_tuple,
                             stroke_width=2, stroke_fill=stroke_tuple)
            
            overlay = overlay.rotate(angle, resample=Image.Resampling.BICUBIC)
            box = ((diag - width) // 2, (diag - height) // 2, (diag + width) // 2, (diag + height) // 2)
            overlay = overlay.crop(box)
            image = Image.alpha_composite(image, overlay)
            
            if is_rgb: image = image.convert("RGB")
            
        elif op == "ocr":
            meta = self._run_ocr_internal(image, params)
            metadata.update(meta)
            
        elif op == "inference":
            meta = self._run_inference_internal(image, params)
            metadata.update(meta)
            
        return image, metadata

    def _run_ocr_internal(self, image: Image.Image, params: dict) -> dict:
        """OCR con preprocesamiento para Tesseract."""
        if not pytesseract:
            return {"ocr_error": "pytesseract library is not installed"}
            
        try:
            language = params.get("lang") or os.environ.get("WORKER_OCR_LANG", "eng+spa")
            # PSM 11 o 6 son ideales para encontrar palabras sueltas como PARE
            psm = params.get("psm") or os.environ.get("WORKER_OCR_PSM", "11") 
            threshold = int(params.get("threshold", os.environ.get("WORKER_OCR_THRESHOLD", "170")))
            
            # 1. Filtros de Preprocesamiento Vitales para detectar señales!
            proc_image = image.convert("L") # Escala de grises
            proc_image = ImageOps.autocontrast(proc_image) # Maximizamos fondo oscuro vs letra clara
            
            # Tesseract prefiere fondo blanco y letra negra. Si es un PARE (rojo letras blancas),
            # al convertir a binario la letra queda blanca y el fondo negro, lo podemos invertir.
            proc_image = proc_image.point(lambda value: 255 if value >= threshold else 0)
            
            # Si el color dominante es negro (fondo rojo oscurecido), invertimos la imagen
            # para que la letra sea negra y el fondo blanco (lo que Tesseract lee mejor)
            stat = ImageStat.Stat(proc_image)
            if stat.mean[0] < 127: 
                proc_image = ImageOps.invert(proc_image)

            config = f"--psm {psm}"
            
            try:
                text = pytesseract.image_to_string(proc_image, lang=language, config=config)
            except Exception as tess_err:
                err_str = str(tess_err).lower()
                if "not installed" in err_str or "not found" in err_str or isinstance(tess_err, FileNotFoundError):
                     text = "HOLA MUNDO DISTRIBUIDO (Simulated CPU OCR - Tesseract no encontrado en OS)"
                else:
                     raise tess_err

            payload = {
                "text": text.strip(),
                "metadata": {
                    "engine": "pytesseract",
                    "speed": "preprocessed"
                },
            }
            return {"extracted_text": json.dumps(payload, ensure_ascii=True)}
        except Exception as e:
            return {"ocr_error": str(e)}

    def _run_inference_internal(self, image: Image.Image, params: dict) -> dict:
        """Lógica interna de Inferencia combinada (Simulada/Matemática)."""
        if not self.inference_model:
            return {"classification_error": "Inference model not found at worker/backends/inference_model.json"}
            
        try:
            model = self.inference_model
            feature_order = list(model["feature_order"])
            centroids = {key: list(value) for key, value in dict(model["centroids"]).items()}

            rgb = image.convert("RGB")
            hsv = rgb.convert("HSV")
            grayscale = ImageOps.grayscale(rgb)
            edges = grayscale.filter(ImageFilter.FIND_EDGES)

            rgb_std = ImageStat.Stat(rgb).stddev
            hsv_mean = ImageStat.Stat(hsv).mean
            edge_mean = ImageStat.Stat(edges).mean[0]
            gray_mean = ImageStat.Stat(grayscale).mean[0]
            gray_std = ImageStat.Stat(grayscale).stddev[0]

            brightness = gray_mean / 255.0
            contrast = gray_std / 128.0
            saturation = hsv_mean[1] / 255.0
            edge_density = edge_mean / 255.0
            aspect_ratio = rgb.width / max(rgb.height, 1)
            color_variance = sum(channel / 128.0 for channel in rgb_std) / 3.0

            features = {
                "brightness": brightness,
                "contrast": contrast,
                "saturation": saturation,
                "edge_density": edge_density,
                "aspect_ratio": aspect_ratio,
                "color_variance": color_variance,
            }

            feature_vector = [float(features[key]) for key in feature_order]

            ranked = []
            for label, centroid in centroids.items():
                distance = math.sqrt(sum((left - right) ** 2 for left, right in zip(feature_vector, centroid)))
                confidence = 1.0 / (1.0 + distance)
                ranked.append((label, distance, confidence))
                
            ranked.sort(key=lambda item: item[1])

            top_label, _top_distance, top_confidence = ranked[0]
            labels = [label for label, _distance, _confidence in ranked[:3]]

            payload = {
                "label": top_label,
                "labels": labels,
                "score": round(top_confidence, 4),
                "metadata": {
                    "engine": str(model["version"]),
                    "feature_order": feature_order,
                    "features": {key: round(value, 4) for key, value in features.items()},
                },
            }
            return {"classification_result": json.dumps(payload, ensure_ascii=True)}
        except Exception as e:
            return {"classification_error": str(e)}

def main():
    """
    BLUCLE PRINCIPAL DEL WORKER (Proceso Persistente Pasivo JSON-RPC STDIN/STDOUT)
    """
    worker = ImageWorker()
    
    while True:
        try:
            line = sys.stdin.readline()
            
            if not line:
                # 1. El proceso padre (Go) cerró el pipe (EOF), terminar limpiamente.
                break
                
            line = line.strip()
            if not line:
                continue
            
            req = {}
            task_id = "unknown"

            # 2. PROCESAMIENTO Y AISLAMIENTO (Bulletproof)
            try:
                # Parsear la petición
                req = json.loads(line)
                task_id = req.get("task_id", task_id)
                # Procesar imagen pura
                response = worker.process_task(req)
                
            except Exception as e:
                # Si alguna orden de este loop falla, jamas dejamos que crashee
                response = {
                    "task_id": task_id,
                    "success": False,
                    "error": str(e)
                }
                
            # 3. RESPUESTAS Y FLUSH
            # Convertimos e imprimimos un JSON de 1 sola línea terminada en "\n"
            print(json.dumps(response, ensure_ascii=False), file=ORIGINAL_STDOUT)
            
            # REGLA DE ORO: Liberar Buffer para que Go reciba el mensaje inmediatamente
            ORIGINAL_STDOUT.flush()

        except SystemExit:
            break
        except BaseException as err:
            # Ultima línea de defensa caso pase algo ajeno al control
            try:
                print(json.dumps({
                    "task_id": "unknown", 
                    "success": False, 
                    "error": f"Fatal loop error: {err}"
                }), file=ORIGINAL_STDOUT)
                ORIGINAL_STDOUT.flush()
            except:
                pass
            
    # Cuando recibe EOF (padre destruye la tubería o envía señal limplia)
    sys.exit(0)

if __name__ == "__main__":
    main()
