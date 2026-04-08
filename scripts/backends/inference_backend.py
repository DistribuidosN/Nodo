from __future__ import annotations

import json
import math
import os
from pathlib import Path

from PIL import Image, ImageFilter, ImageOps, ImageStat


def _load_model() -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    model_path = root / "worker" / "backends" / "inference_model.json"
    return json.loads(model_path.read_text(encoding="utf-8"))


def _extract_features(image: Image.Image) -> dict[str, float]:
    rgb = image.convert("RGB")
    hsv = rgb.convert("HSV")
    grayscale = ImageOps.grayscale(rgb)
    edges = grayscale.filter(ImageFilter.FIND_EDGES)

    rgb_mean = ImageStat.Stat(rgb).mean
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

    return {
        "brightness": brightness,
        "contrast": contrast,
        "saturation": saturation,
        "edge_density": edge_density,
        "aspect_ratio": aspect_ratio,
        "color_variance": color_variance,
    }


def _score(features: list[float], centroid: list[float]) -> tuple[float, float]:
    distance = math.sqrt(sum((left - right) ** 2 for left, right in zip(features, centroid, strict=False)))
    return distance, 1.0 / (1.0 + distance)


def main() -> None:
    input_file = os.environ["WORKER_INPUT_FILE"]
    output_json = os.environ["WORKER_OUTPUT_JSON"]

    model = _load_model()
    feature_order: list[str] = list(model["feature_order"])
    centroids: dict[str, list[float]] = {key: list(value) for key, value in dict(model["centroids"]).items()}

    image = Image.open(input_file)
    features = _extract_features(image)
    feature_vector = [float(features[key]) for key in feature_order]

    ranked: list[tuple[str, float, float]] = []
    for label, centroid in centroids.items():
        distance, confidence = _score(feature_vector, centroid)
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
    Path(output_json).write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


if __name__ == "__main__":
    main()
