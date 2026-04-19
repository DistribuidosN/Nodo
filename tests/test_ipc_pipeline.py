import subprocess
import json
import time
import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

def create_dummy_image(filepath: str):
    """Crea una imagen física .jpg para usar en las pruebas."""
    img = Image.new('RGB', (800, 600), color='salmon')
    draw = ImageDraw.Draw(img)
    # Dibujamos algo de texto grande para que el OCR tenga posibilidades de detectarlo
    draw.text((50, 250), "HOLA MUNDO DISTRIBUIDO", fill='white', font=None)
    draw.rectangle([10, 10, 790, 590], outline="white", width=5)
    img.save(filepath)
    print(f"[+] Dummy image created at: {filepath}")

def main():
    work_dir = Path("data/tmp")
    work_dir.mkdir(parents=True, exist_ok=True)
    
    input_img = str(work_dir / "test_input.jpg")
    create_dummy_image(input_img)

    print("[*] Iniciando el Proceso worker.py...")
    worker_process = subprocess.Popen(
        ["python", "worker/worker.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    time.sleep(0.5)

    tests = [
        # --- PRUEBAS FILTRANDO UNO A UNO ---
        {"name": "SINGLE: Grayscale", "transforms": [{"operation": "grayscale", "params": {}}]},
        
        {"name": "SINGLE: Resize", "transforms": [{"operation": "resize", "params": {"width": "400", "height": "300"}}]},
        
        {"name": "SINGLE: Crop", "transforms": [{"operation": "crop", "params": {"left": "100", "upper": "100", "right": "500", "lower": "400"}}]},
        
        {"name": "SINGLE: Rotate 45", "transforms": [{"operation": "rotate", "params": {"angle": "45", "expand": "true"}}]},
        
        {"name": "SINGLE: Flip Vertical", "transforms": [{"operation": "flip", "params": {"direction": "vertical"}}]},
        
        {"name": "SINGLE: Flip Horizontal", "transforms": [{"operation": "flip", "params": {"direction": "horizontal"}}]},
        
        {"name": "SINGLE: Blur", "transforms": [{"operation": "blur", "params": {"radius": "5.0"}}]},
        
        {"name": "SINGLE: Sharpen", "transforms": [{"operation": "sharpen", "params": {"factor": "3.0"}}]},
        
        {"name": "SINGLE: Brightness & Contrast", "transforms": [{"operation": "brightness_contrast", "params": {"brightness": "1.5", "contrast": "1.2"}}]},
        
        {"name": "SINGLE: Watermark Text", "transforms": [{"operation": "watermark_text", "params": {"text": "CONFIDENTIAL"}}]},
        
        {"name": "SINGLE: OCR Extract Text", "transforms": [{"operation": "ocr", "params": {"lang": "eng"}}]},
        
        {"name": "SINGLE: AI Inference", "transforms": [{"operation": "inference", "params": {}}]},
        
        # --- COMBINACIONES COMPLEJAS ---
        {
           "name": "COMBO: Resize -> Brightness -> Blur", 
           "transforms": [
               {"operation": "resize", "params": {"width": "300", "height": "300"}},
               {"operation": "brightness_contrast", "params": {"brightness": "2.0", "contrast": "0.8"}},
               {"operation": "blur", "params": {"radius": "2.0"}}
           ]
        },
        
        {
           "name": "COMBO: Rotate -> Crop -> Grayscale", 
           "transforms": [
               {"operation": "rotate", "params": {"angle": "90", "expand": "true"}},
               {"operation": "crop", "params": {"left": "10", "upper": "10", "right": "200", "lower": "200"}},
               {"operation": "grayscale", "params": {}}
           ]
        },
        
        {
           "name": "COMBO: Watermark -> OCR (Simula leer con watermark)", 
           "transforms": [
               {"operation": "watermark_text", "params": {"text": "FAKE_DOCUMENT"}},
               {"operation": "ocr", "params": {"lang": "eng"}}
           ]
        },
        
        {
           "name": "COMBO: Force Error (Flip incorrecto)", 
           "transforms": [
               {"operation": "flip", "params": {"direction": "DIAGONAL_INVALIDO"}}
           ]
        }
    ]

    print("\n--- INICIANDO BATERÍA DE PRUEBAS COMPLETAS (FULL COVERAGE) ---")
    for i, test in enumerate(tests):
        task_id = f"task_{i:02d}"
        
        request = {
            "task_id": task_id,
            "input_path": input_img,
            "output_dir": str(work_dir),
            "output_format": "jpg", # Unificamos a jpg la salida
            "transforms": test["transforms"]
        }
        
        json_line = json.dumps(request) + "\n"
        print(f"\n[GO -> WORKER] Ejecutando: {test['name']}")
        
        worker_process.stdin.write(json_line)
        worker_process.stdin.flush()
        
        start_time = time.time()
        response_line = worker_process.stdout.readline()
        elapsed = (time.time() - start_time) * 1000
        
        if not response_line:
            print("[X] ERROR: El worker se cerró inesperadamente.")
            print("STDERR:", worker_process.stderr.read())
            break
            
        try:
            response = json.loads(response_line)
            if response.get("success"):
                meta_str = ""
                if response.get("metadata"):
                    # Solo mostramos que hay metadata si es OCR o Inference
                    keys = response.get("metadata")
                    meta_str = f" | +Meta: {keys}"
                    
                print(f"[WORKER -> GO] V ÉXITO ({elapsed:.2f}ms) | Ruta: {response.get('result_path')}{meta_str}")
            else:
                print(f"[WORKER -> GO] X FALLO CONTROLADO ({elapsed:.2f}ms) | Error: {response.get('error')}")
        except json.JSONDecodeError:
            print("[X] ERROR AL PARSEAR JSON:", response_line)

    print("\n[*] Cerrando tubería. El Worker debería auto-terminar...")
    worker_process.stdin.close()
    worker_process.wait(timeout=2)
    print(f"[*] Worker terminó con exit_code: {worker_process.returncode}")

if __name__ == "__main__":
    main()
