#!/usr/bin/env python3
"""Визуальное подтверждение детекции YOLO (критерий Б.2)."""
import os
from pathlib import Path
import sys

MODULE_B = Path(__file__).resolve().parents[1]
PROJECT_ROOT = MODULE_B.parent

import cv2
import yaml
from ultralytics import YOLO

OUT = MODULE_B / "output"
OUT.mkdir(parents=True, exist_ok=True)

MODEL = os.getenv("YOLO_MODEL", str(PROJECT_ROOT / "models" / "yolov8n.pt"))


def resolve_video_path() -> Path:
    """Путь к видео: DEMO_VIDEO → cameras.yaml → cam1.mp4 / test_video.mp4."""
    if os.getenv("DEMO_VIDEO"):
        return Path(os.environ["DEMO_VIDEO"]).expanduser().resolve()

    cfg = MODULE_B / "config" / "cameras.yaml"
    if cfg.exists():
        with open(cfg, encoding="utf-8") as f:
            cameras = yaml.safe_load(f).get("cameras", [])
        if cameras and cameras[0].get("video_source"):
            src = Path(cameras[0]["video_source"])
            if not src.is_absolute():
                src = (PROJECT_ROOT / src).resolve()
            if src.exists():
                return src

    candidates = [
        PROJECT_ROOT.parent / "data_analitick_3_11" / "video" / "cam1.mp4",
        PROJECT_ROOT.parent / "data_analitick_3_11" / "video" / "test_video.mp4",
        PROJECT_ROOT / "video" / "cam1.mp4",
    ]
    for p in candidates:
        if p.exists():
            return p.resolve()
    return candidates[0]


def main():
    video = resolve_video_path()
    if not Path(MODEL).exists():
        print(f"ERROR: модель не найдена: {MODEL}", file=sys.stderr)
        print("  ln -sf ~/data_analitick_3_11/yolov8n.pt models/yolov8n.pt", file=sys.stderr)
        sys.exit(1)

    if not video.exists():
        print(f"ERROR: видео не найдено: {video}", file=sys.stderr)
        print("  Положите .mp4 в ~/data_analitick_3_11/video/ или задайте:", file=sys.stderr)
        print("  export DEMO_VIDEO=/path/to/video.mp4", file=sys.stderr)
        sys.exit(1)

    print(f"Model: {MODEL}")
    print(f"Video: {video}")

    model = YOLO(MODEL)
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened():
        print(f"ERROR: OpenCV не открыл видео: {video}", file=sys.stderr)
        sys.exit(1)

    saved = 0
    frame_idx = 0
    while cap.isOpened() and saved < 5:
        ret, frame = cap.read()
        if not ret:
            break
        frame_idx += 1
        # Каждый 30-й кадр — быстрее на длинных роликах
        if frame_idx % 30 != 1 and saved > 0:
            continue

        results = model.track(frame, persist=True, classes=[2, 3, 5, 7], verbose=False)
        annotated = results[0].plot()
        path = OUT / f"demo_detection_{saved:03d}.jpg"
        if not cv2.imwrite(str(path), annotated):
            print(f"ERROR: не удалось записать {path}", file=sys.stderr)
            sys.exit(1)
        print(f"Saved {path}")
        saved += 1

    cap.release()

    if saved == 0:
        print("ERROR: не сохранено ни одного кадра (видео пустое или слишком короткое).", file=sys.stderr)
        sys.exit(1)

    print(f"Visual demo complete: {saved} file(s) in {OUT}/")


if __name__ == "__main__":
    main()
