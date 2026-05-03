#!/usr/bin/env python3
import math
import os
import struct
import zlib


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "assets", "emojis")
PNG_OUT = os.path.join(OUT, "png")
GIF_FRAMES = os.path.join(OUT, "gif_frames")
SIZE = 128
SCALE = 3
W = SIZE * SCALE
H = SIZE * SCALE

BLUE = (42, 143, 218, 255)
BLUE_DARK = (10, 50, 92, 255)
BLUE_MID = (20, 94, 154, 255)
CYAN = (128, 221, 255, 255)
ICE = (231, 250, 255, 255)
SILVER = (189, 218, 232, 255)
SILVER_DARK = (70, 111, 140, 255)
NAVY = (6, 23, 45, 255)
GOLD = (255, 206, 86, 255)
RED = (232, 66, 82, 255)
GREEN = (79, 218, 142, 255)
PURPLE = (154, 96, 232, 255)
TRANSPARENT = (0, 0, 0, 0)


def sc(v):
    return int(round(v * SCALE))


def blend(dst, src):
    sr, sg, sb, sa = src
    if sa <= 0:
        return dst
    if sa >= 255:
        return src
    dr, dg, db, da = dst
    a = sa / 255
    ia = 1 - a
    out_a = sa + da * ia
    if out_a <= 0:
        return TRANSPARENT
    return (
        int(sr * a + dr * ia),
        int(sg * a + dg * ia),
        int(sb * a + db * ia),
        int(out_a),
    )


class Canvas:
    def __init__(self, w=W, h=H):
        self.w = w
        self.h = h
        self.px = [TRANSPARENT] * (w * h)

    def set(self, x, y, color):
        if 0 <= x < self.w and 0 <= y < self.h:
            i = y * self.w + x
            self.px[i] = blend(self.px[i], color)

    def circle(self, cx, cy, r, color, outline=None, width=0):
        cx, cy, r, width = sc(cx), sc(cy), sc(r), sc(width)
        x0, x1 = max(0, cx - r - width), min(self.w, cx + r + width + 1)
        y0, y1 = max(0, cy - r - width), min(self.h, cy + r + width + 1)
        rr = r * r
        inner = max(0, r - width)
        inner2 = inner * inner
        outer = (r + width) * (r + width)
        for y in range(y0, y1):
            for x in range(x0, x1):
                d2 = (x - cx) ** 2 + (y - cy) ** 2
                if d2 <= rr:
                    self.set(x, y, color)
                if outline and inner2 <= d2 <= outer:
                    self.set(x, y, outline)

    def ellipse(self, cx, cy, rx, ry, color, outline=None, width=0):
        cx, cy, rx, ry, width = sc(cx), sc(cy), sc(rx), sc(ry), sc(width)
        x0, x1 = max(0, cx - rx - width), min(self.w, cx + rx + width + 1)
        y0, y1 = max(0, cy - ry - width), min(self.h, cy + ry + width + 1)
        for y in range(y0, y1):
            for x in range(x0, x1):
                v = ((x - cx) ** 2) / max(rx * rx, 1) + ((y - cy) ** 2) / max(ry * ry, 1)
                if v <= 1:
                    self.set(x, y, color)
                if outline and 1 - width / max(rx, ry, 1) <= v <= 1 + width / max(rx, ry, 1):
                    self.set(x, y, outline)

    def line(self, x1, y1, x2, y2, color, width=3):
        x1, y1, x2, y2, width = sc(x1), sc(y1), sc(x2), sc(y2), sc(width)
        steps = max(abs(x2 - x1), abs(y2 - y1), 1)
        for i in range(steps + 1):
            t = i / steps
            x = x1 + (x2 - x1) * t
            y = y1 + (y2 - y1) * t
            self._disk(int(x), int(y), width, color)

    def _disk(self, cx, cy, r, color):
        for y in range(max(0, cy - r), min(self.h, cy + r + 1)):
            for x in range(max(0, cx - r), min(self.w, cx + r + 1)):
                if (x - cx) ** 2 + (y - cy) ** 2 <= r * r:
                    self.set(x, y, color)

    def poly(self, pts, color, outline=None, width=2):
        pts = [(sc(x), sc(y)) for x, y in pts]
        ys = [p[1] for p in pts]
        for y in range(max(0, min(ys)), min(self.h, max(ys) + 1)):
            nodes = []
            j = len(pts) - 1
            for i in range(len(pts)):
                xi, yi = pts[i]
                xj, yj = pts[j]
                if (yi < y <= yj) or (yj < y <= yi):
                    nodes.append(int(xi + (y - yi) / (yj - yi) * (xj - xi)))
                j = i
            nodes.sort()
            for a, b in zip(nodes[0::2], nodes[1::2]):
                for x in range(max(0, a), min(self.w, b + 1)):
                    self.set(x, y, color)
        if outline:
            for i, p in enumerate(pts):
                q = pts[(i + 1) % len(pts)]
                self.line(p[0] / SCALE, p[1] / SCALE, q[0] / SCALE, q[1] / SCALE, outline, width)

    def rect(self, x, y, w, h, color, outline=None, width=2):
        x, y, w, h = sc(x), sc(y), sc(w), sc(h)
        for yy in range(max(0, y), min(self.h, y + h)):
            for xx in range(max(0, x), min(self.w, x + w)):
                self.set(xx, yy, color)
        if outline:
            self.line(x / SCALE, y / SCALE, (x + w) / SCALE, y / SCALE, outline, width)
            self.line((x + w) / SCALE, y / SCALE, (x + w) / SCALE, (y + h) / SCALE, outline, width)
            self.line((x + w) / SCALE, (y + h) / SCALE, x / SCALE, (y + h) / SCALE, outline, width)
            self.line(x / SCALE, (y + h) / SCALE, x / SCALE, y / SCALE, outline, width)

    def text_q(self, cx=64, cy=65, size=44, color=BLUE, shadow=True):
        # Vector-ish Q: ring plus slash, readable at emoji size.
        if shadow:
            self.ellipse(cx + 2, cy + 2, size * 0.43, size * 0.50, (2, 18, 38, 150), width=0)
        self.ellipse(cx, cy, size * 0.44, size * 0.50, TRANSPARENT, outline=color, width=max(5, size * 0.12))
        self.ellipse(cx, cy, size * 0.25, size * 0.30, TRANSPARENT, outline=ICE, width=max(2, size * 0.035))
        self.line(cx + size * 0.17, cy + size * 0.23, cx + size * 0.42, cy + size * 0.48, color, max(5, size * 0.12))
        self.line(cx + size * 0.18, cy + size * 0.20, cx + size * 0.37, cy + size * 0.39, ICE, max(2, size * 0.035))

    def sparkle(self, cx, cy, r=7):
        self.line(cx - r, cy, cx + r, cy, ICE, 2)
        self.line(cx, cy - r, cx, cy + r, ICE, 2)
        self.line(cx - r * 0.55, cy - r * 0.55, cx + r * 0.55, cy + r * 0.55, CYAN, 1.5)
        self.line(cx - r * 0.55, cy + r * 0.55, cx + r * 0.55, cy - r * 0.55, CYAN, 1.5)

    def downsample(self):
        out = Canvas(SIZE, SIZE)
        for y in range(SIZE):
            for x in range(SIZE):
                acc = [0, 0, 0, 0]
                for yy in range(SCALE):
                    for xx in range(SCALE):
                        p = self.px[(y * SCALE + yy) * self.w + x * SCALE + xx]
                        for k in range(4):
                            acc[k] += p[k]
                out.px[y * SIZE + x] = tuple(int(v / (SCALE * SCALE)) for v in acc)
        return out


def save_png(canvas, path):
    canvas = canvas.downsample() if canvas.w != SIZE else canvas
    rows = []
    for y in range(canvas.h):
        raw = bytearray()
        for x in range(canvas.w):
            raw.extend(canvas.px[y * canvas.w + x])
        rows.append(b"\x00" + bytes(raw))
    data = zlib.compress(b"".join(rows), 9)

    def chunk(kind, payload):
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF)

    png = b"\x89PNG\r\n\x1a\n"
    png += chunk(b"IHDR", struct.pack(">IIBBBBB", canvas.w, canvas.h, 8, 6, 0, 0, 0))
    png += chunk(b"IDAT", data)
    png += chunk(b"IEND", b"")
    with open(path, "wb") as f:
        f.write(png)


def coin(c, cx=64, cy=64, r=38, q=True):
    c.circle(cx + 2, cy + 4, r, (1, 14, 30, 120))
    c.circle(cx, cy, r, SILVER, outline=BLUE_DARK, width=3)
    c.circle(cx - 3, cy - 4, r * 0.82, ICE, outline=BLUE_MID, width=2)
    c.circle(cx - 12, cy - 18, 4, CYAN)
    c.circle(cx + 20, cy - 15, 3, BLUE_MID)
    c.poly([(cx - 26, cy + 3), (cx - 18, cy - 2), (cx - 10, cy + 3), (cx - 18, cy + 8)], BLUE_MID)
    if q:
        c.text_q(cx, cy + 1, r * 1.0, BLUE_MID)
    c.sparkle(cx + r * 0.72, cy + r * 0.38, 6)


def qoin_bag():
    c = Canvas()
    c.ellipse(64, 76, 39, 34, BLUE_MID, outline=BLUE_DARK, width=4)
    c.poly([(33, 45), (50, 28), (78, 28), (95, 45), (82, 57), (45, 57)], BLUE, outline=BLUE_DARK, width=3)
    c.line(42, 47, 86, 47, CYAN, 4)
    c.line(44, 55, 36, 66, ICE, 3)
    coin(c, 68, 81, 22)
    c.sparkle(95, 40, 7)
    return c


def qoin_chest():
    c = Canvas()
    c.rect(23, 58, 82, 38, BLUE_DARK, outline=NAVY, width=4)
    c.poly([(26, 58), (39, 36), (89, 36), (102, 58)], BLUE_MID, outline=NAVY, width=4)
    c.rect(28, 62, 72, 29, BLUE, outline=CYAN, width=2)
    c.rect(58, 53, 13, 45, SILVER, outline=BLUE_DARK, width=2)
    c.circle(64, 73, 7, GOLD, outline=BLUE_DARK, width=2)
    c.ellipse(64, 54, 34, 10, (115, 226, 255, 120))
    coin(c, 43, 51, 13, False)
    coin(c, 85, 51, 13, False)
    c.sparkle(99, 35, 7)
    return c


def qticket():
    c = Canvas()
    # Angled ticket silhouette with clipped/notched sides for readability at emoji size.
    c.poly(
        [(21, 46), (84, 28), (107, 80), (44, 101)],
        BLUE,
        outline=BLUE_DARK,
        width=4,
    )
    c.poly(
        [(34, 52), (80, 39), (94, 72), (49, 87)],
        ICE,
        outline=CYAN,
        width=2,
    )
    c.circle(28, 67, 9, TRANSPARENT, outline=BLUE_DARK, width=3)
    c.circle(99, 61, 9, TRANSPARENT, outline=BLUE_DARK, width=3)
    for x, y in [(44, 49), (52, 47), (60, 45), (68, 43)]:
        c.line(x, y, x + 4, y + 10, BLUE_MID, 1.5)
    c.line(51, 83, 86, 72, BLUE_MID, 3)
    c.line(43, 72, 76, 62, SILVER_DARK, 2)
    coin(c, 66, 65, 18)
    c.sparkle(98, 34, 6)
    return c


def qoin_transfer():
    c = Canvas()
    coin(c, 42, 72, 24)
    coin(c, 86, 55, 24)
    c.line(38, 36, 82, 30, CYAN, 6)
    c.poly([(82, 30), (72, 22), (77, 40)], CYAN, outline=BLUE_DARK, width=2)
    c.line(90, 92, 46, 98, CYAN, 6)
    c.poly([(46, 98), (56, 89), (51, 108)], CYAN, outline=BLUE_DARK, width=2)
    return c


def qxp():
    c = Canvas()
    pts = []
    for i in range(10):
        a = -math.pi / 2 + i * math.pi / 5
        r = 43 if i % 2 == 0 else 20
        pts.append((64 + math.cos(a) * r, 64 + math.sin(a) * r))
    c.poly(pts, CYAN, outline=BLUE_DARK, width=4)
    c.poly([(44, 55), (58, 55), (64, 65), (70, 55), (84, 55), (72, 72), (84, 91), (70, 91), (64, 79), (58, 91), (44, 91), (56, 72)], BLUE_MID, outline=ICE, width=1.5)
    c.sparkle(97, 35, 6)
    return c


def qlevel_up():
    c = Canvas()
    c.poly([(64, 18), (102, 60), (80, 60), (80, 105), (48, 105), (48, 60), (26, 60)], BLUE, outline=BLUE_DARK, width=5)
    c.poly([(64, 29), (88, 55), (73, 55), (73, 94), (55, 94), (55, 55), (40, 55)], ICE)
    c.text_q(64, 72, 26, BLUE_MID)
    c.sparkle(97, 27, 6)
    return c


def qquest():
    c = Canvas()
    c.rect(30, 35, 68, 58, ICE, outline=BLUE_DARK, width=3)
    c.circle(30, 35, 10, SILVER, outline=BLUE_DARK, width=2)
    c.circle(98, 93, 10, SILVER, outline=BLUE_DARK, width=2)
    c.line(45, 51, 82, 51, BLUE_MID, 3)
    c.line(45, 63, 77, 63, BLUE_MID, 3)
    c.line(45, 75, 69, 75, BLUE_MID, 3)
    coin(c, 70, 81, 16)
    return c


def qshop():
    c = Canvas()
    c.rect(31, 55, 66, 44, BLUE_MID, outline=BLUE_DARK, width=4)
    c.rect(42, 72, 18, 27, NAVY, outline=CYAN, width=2)
    c.rect(66, 70, 20, 16, ICE, outline=BLUE_DARK, width=2)
    c.poly([(27, 55), (36, 31), (92, 31), (101, 55)], ICE, outline=BLUE_DARK, width=3)
    for x, col in [(35, BLUE), (49, SILVER), (63, BLUE), (77, SILVER), (91, BLUE)]:
        c.rect(x - 7, 40, 14, 18, col, outline=BLUE_DARK, width=1.5)
    coin(c, 64, 28, 14)
    return c


def flip_coin(c, cx=64, cy=64, angle=0):
    face = abs(math.cos(angle))
    rx = 5 + face * 34
    ry = 39
    side = 1 if math.cos(angle) >= 0 else -1
    c.ellipse(cx + 2, cy + 5, rx, ry, (1, 14, 30, 105))
    c.ellipse(cx, cy, rx, ry, SILVER if side > 0 else BLUE_MID, outline=BLUE_DARK, width=3)
    c.ellipse(cx - min(3, rx * 0.18), cy - 4, max(2, rx * 0.76), ry * 0.78, ICE if side > 0 else BLUE, outline=CYAN, width=2)
    if face > 0.42:
        q_size = max(18, min(40, rx * 1.12))
        if side > 0:
            c.text_q(cx, cy + 1, q_size, BLUE_MID)
        else:
            c.text_q(cx, cy + 1, q_size, ICE)
    else:
        c.line(cx, cy - 32, cx, cy + 32, ICE, 3)
        c.line(cx - 2, cy - 31, cx - 2, cy + 31, CYAN, 2)
    c.sparkle(cx + max(8, rx * 0.72), cy + 15, 5)


def qflip(angle=0):
    c = Canvas()
    flip_coin(c, 64, 64, angle)
    c.line(18, 45, 40, 35, CYAN, 4)
    c.poly([(40, 35), (31, 30), (35, 44)], CYAN, outline=BLUE_DARK, width=1.5)
    c.line(88, 94, 110, 83, CYAN, 4)
    c.poly([(88, 94), (98, 97), (94, 84)], CYAN, outline=BLUE_DARK, width=1.5)
    c.sparkle(100, 34, 6)
    return c


def qslots():
    c = Canvas()
    c.rect(25, 33, 72, 67, BLUE_DARK, outline=NAVY, width=4)
    c.rect(34, 46, 54, 28, ICE, outline=CYAN, width=3)
    for x in [43, 61, 79]:
        coin(c, x, 60, 10)
    c.line(99, 45, 111, 33, SILVER, 4)
    c.circle(113, 30, 6, RED, outline=BLUE_DARK, width=2)
    c.rect(39, 82, 44, 10, BLUE, outline=CYAN, width=2)
    return c


def qwheel(rotation=0):
    c = Canvas()
    colors = [BLUE, SILVER, CYAN, BLUE_DARK, PURPLE, GREEN, GOLD, ICE]
    cx, cy, r = 64, 67, 43
    for i, col in enumerate(colors):
        a1 = rotation + i * math.tau / len(colors)
        a2 = rotation + (i + 1) * math.tau / len(colors)
        pts = [(cx, cy)]
        for s in range(8):
            a = a1 + (a2 - a1) * s / 7
            pts.append((cx + math.cos(a) * r, cy + math.sin(a) * r))
        c.poly(pts, col)
    c.circle(cx, cy, r, TRANSPARENT, outline=BLUE_DARK, width=4)
    coin(c, cx, cy, 15)
    c.poly([(64, 12), (55, 30), (73, 30)], RED, outline=BLUE_DARK, width=2)
    return c


def qmine():
    c = Canvas()
    c.circle(64, 72, 36, NAVY, outline=BLUE_DARK, width=4)
    c.rect(54, 34, 20, 13, BLUE_DARK, outline=CYAN, width=2)
    c.line(70, 35, 92, 22, CYAN, 4)
    c.sparkle(96, 21, 8)
    c.text_q(64, 73, 25, CYAN)
    c.sparkle(37, 52, 5)
    return c


def qsuccess():
    c = Canvas()
    c.circle(64, 64, 45, BLUE_MID, outline=BLUE_DARK, width=5)
    c.line(38, 65, 56, 84, ICE, 9)
    c.line(56, 84, 92, 43, ICE, 9)
    c.line(39, 65, 56, 82, GREEN, 5)
    c.line(56, 82, 91, 44, GREEN, 5)
    c.sparkle(96, 33, 6)
    return c


def qdenied():
    c = Canvas()
    c.circle(64, 64, 45, SILVER, outline=BLUE_DARK, width=5)
    c.line(40, 40, 88, 88, RED, 10)
    c.line(88, 40, 40, 88, RED, 10)
    c.line(40, 40, 88, 88, ICE, 3)
    c.line(88, 40, 40, 88, ICE, 3)
    c.sparkle(96, 35, 5)
    return c


def qtimer(hand_angle=0):
    c = Canvas()
    c.rect(54, 15, 20, 12, SILVER, outline=BLUE_DARK, width=2)
    c.circle(64, 70, 42, ICE, outline=BLUE_DARK, width=5)
    c.circle(64, 70, 31, SILVER, outline=CYAN, width=2)
    c.line(64, 70, 64 + math.cos(hand_angle) * 21, 70 + math.sin(hand_angle) * 21, BLUE_MID, 5)
    c.line(64, 70, 52, 59, CYAN, 4)
    c.circle(64, 70, 6, BLUE_DARK)
    c.sparkle(96, 39, 5)
    return c


STATIC = {
    "QoinBag": qoin_bag,
    "QoinChest": qoin_chest,
    "QTicket": qticket,
    "QoinTransfer": qoin_transfer,
    "QXP": qxp,
    "QLevelUp": qlevel_up,
    "QQuest": qquest,
    "QShop": qshop,
    "QFlip": qflip,
    "QSlots": qslots,
    "QWheel": qwheel,
    "QMine": qmine,
    "QSuccess": qsuccess,
    "QDenied": qdenied,
    "QTimer": qtimer,
}


def make_static():
    os.makedirs(PNG_OUT, exist_ok=True)
    for name, fn in STATIC.items():
        save_png(fn(), os.path.join(PNG_OUT, f"{name}.png"))


def make_frames():
    os.makedirs(GIF_FRAMES, exist_ok=True)
    animations = {
        "QFlipSpin": lambda i, n: qflip(i / n * math.tau * 2),
        "QWheelSpin": lambda i, n: qwheel(i / n * math.tau),
        "QTimerTick": lambda i, n: qtimer(-math.pi / 2 + i / n * math.tau),
        "QLevelPulse": lambda i, n: qlevel_up(),
        "QMineSpark": lambda i, n: qmine(),
    }
    for name, fn in animations.items():
        folder = os.path.join(GIF_FRAMES, name)
        os.makedirs(folder, exist_ok=True)
        for i in range(16):
            canvas = fn(i, 16)
            if name == "QLevelPulse":
                pulse = 4 * math.sin(i / 16 * math.tau)
                canvas.circle(64, 64, 48 + pulse, TRANSPARENT, outline=(128, 221, 255, 80), width=2)
            if name == "QMineSpark" and i % 4 in (0, 1):
                canvas.sparkle(96, 21, 11)
            save_png(canvas, os.path.join(folder, f"{i:03d}.png"))


if __name__ == "__main__":
    make_static()
    make_frames()
    print(PNG_OUT)
    print(GIF_FRAMES)
