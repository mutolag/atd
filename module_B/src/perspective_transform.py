"""Перспективная трансформация для метрической скорости (модуль В, критерий перспективы)."""
import cv2
import numpy as np


class PerspectiveTransformer:
    def __init__(self, image_points, width_m, height_m):
        self.image_points = np.float32(image_points)
        self.width_m = width_m
        self.height_m = height_m
        self.world_points = np.float32([
            [0, height_m],
            [width_m, height_m],
            [width_m, 0],
            [0, 0],
        ])
        self.matrix = cv2.getPerspectiveTransform(self.image_points, self.world_points)

    def pixel_to_world(self, x, y):
        pt = np.array([[[float(x), float(y)]]], dtype=np.float32)
        world = cv2.perspectiveTransform(pt, self.matrix)[0][0]
        return float(world[0]), float(world[1])

    def speed_kmh(self, x1, y1, t1, x2, y2, t2):
        if t2 <= t1:
            return 0.0
        w1 = self.pixel_to_world(x1, y1)
        w2 = self.pixel_to_world(x2, y2)
        dist_m = np.hypot(w2[0] - w1[0], w2[1] - w1[1])
        hours = (t2 - t1) / 3600.0
        if hours <= 0:
            return 0.0
        return (dist_m / 1000.0) / hours
