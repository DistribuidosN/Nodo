from __future__ import annotations

import json
import os
from pathlib import Path

import pytesseract
from PIL import Image, ImageOps


def main() -> None:
    input_file = os.environ["WORKER_INPUT_FILE"]
    output_json = os.environ["WORKER_OUTPUT_JSON"]
    params = json.loads(os.environ.get("WORKER_PARAMS_JSON", "{}"))
    language = params.get("lang") or os.environ.get("WORKER_OCR_LANG", "eng+spa")
    psm = params.get("psm") or os.environ.get("WORKER_OCR_PSM", "6")
    tesseract_cmd = os.environ.get("WORKER_TESSERACT_CMD")
    if tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    image = Image.open(input_file).convert("L")
    image = ImageOps.autocontrast(image)
    threshold = int(params.get("threshold", os.environ.get("WORKER_OCR_THRESHOLD", "170")))
    image = image.point(lambda value: 255 if value >= threshold else 0)

    config = f"--psm {psm}"
    text = pytesseract.image_to_string(image, lang=language, config=config)

    payload = {
        "text": text.strip(),
        "metadata": {
            "engine": "tesseract",
            "language": language,
            "psm": psm,
        },
    }
    Path(output_json).write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


if __name__ == "__main__":
    main()
