#!/usr/bin/env python3
import math
import os
import shutil
import struct
import subprocess
import tempfile
import zlib


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(ROOT, "assets", "emojis")
UPLOAD_OUT = os.path.join(OUT, "upload")
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


def qticket_minus():
    c = qticket()
    c.circle(91, 91, 23, RED, outline=BLUE_DARK, width=4)
    c.circle(91, 91, 17, (255, 95, 108, 255), outline=SILVER, width=2)
    c.line(78, 91, 104, 91, ICE, 8)
    c.line(78, 91, 104, 91, BLUE_DARK, 2)
    c.sparkle(109, 72, 5)
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


def qlucky_charm():
    c = Canvas()
    for cx, cy in [(51, 52), (75, 52), (51, 76), (75, 76)]:
        c.circle(cx, cy, 18, GREEN, outline=BLUE_DARK, width=3)
    c.line(63, 78, 46, 102, GREEN, 7)
    coin(c, 63, 64, 18)
    c.sparkle(96, 35, 7)
    return c


def qfortune_vial():
    c = Canvas()
    c.rect(50, 20, 28, 13, SILVER, outline=BLUE_DARK, width=2)
    c.rect(45, 32, 38, 14, ICE, outline=CYAN, width=2)
    c.poly([(42, 45), (86, 45), (92, 101), (36, 101)], BLUE_MID, outline=BLUE_DARK, width=4)
    c.poly([(45, 64), (87, 56), (88, 94), (40, 94)], CYAN, outline=BLUE, width=2)
    c.circle(64, 76, 20, ICE, outline=BLUE_DARK, width=3)
    c.text_q(64, 77, 22, BLUE_MID)
    for x, y in [(48, 57), (77, 63), (56, 91)]:
        c.circle(x, y, 4, GOLD, outline=BLUE_DARK, width=1)
    c.sparkle(95, 34, 7)
    c.sparkle(32, 74, 5)
    return c


def qactivity():
    c = Canvas()
    c.rect(27, 42, 74, 54, NAVY, outline=BLUE_DARK, width=4)
    bars = [(39, 78, 14), (55, 65, 27), (71, 53, 39), (87, 35, 57)]
    for x, y, h in bars:
        c.rect(x, y, 10, h, BLUE, outline=CYAN, width=1.5)
    c.line(34, 84, 52, 70, CYAN, 3)
    c.line(52, 70, 68, 57, CYAN, 3)
    c.line(68, 57, 90, 32, CYAN, 3)
    c.poly([(90, 32), (79, 34), (88, 43)], CYAN, outline=BLUE_DARK, width=1.5)
    coin(c, 37, 38, 14)
    c.sparkle(100, 25, 6)
    return c


def qxp_tonic():
    c = Canvas()
    c.rect(52, 25, 24, 14, SILVER, outline=BLUE_DARK, width=2)
    c.rect(45, 37, 38, 66, CYAN, outline=BLUE_DARK, width=4)
    c.ellipse(64, 42, 19, 8, ICE, outline=CYAN, width=2)
    c.rect(49, 59, 30, 24, ICE, outline=BLUE_DARK, width=2)
    c.poly([(52, 65), (60, 65), (64, 72), (68, 65), (76, 65), (69, 77), (76, 88), (68, 88), (64, 80), (60, 88), (52, 88), (59, 77)], BLUE_MID)
    c.sparkle(94, 32, 6)
    return c


def qqueso_magnet():
    c = Canvas()
    c.circle(44, 80, 16, RED, outline=BLUE_DARK, width=3)
    c.circle(84, 80, 16, RED, outline=BLUE_DARK, width=3)
    c.rect(30, 39, 28, 43, BLUE, outline=BLUE_DARK, width=4)
    c.rect(70, 39, 28, 43, BLUE, outline=BLUE_DARK, width=4)
    c.rect(51, 30, 26, 20, SILVER, outline=BLUE_DARK, width=3)
    coin(c, 64, 73, 17)
    c.sparkle(97, 36, 6)
    return c


def qdaily_spice():
    c = Canvas()
    c.rect(43, 31, 42, 12, SILVER, outline=BLUE_DARK, width=2)
    c.rect(38, 43, 52, 58, BLUE_MID, outline=BLUE_DARK, width=4)
    c.rect(43, 58, 42, 26, ICE, outline=CYAN, width=2)
    c.poly([(53, 84), (63, 55), (75, 84)], GOLD, outline=BLUE_DARK, width=2)
    c.text_q(64, 72, 18, BLUE_MID)
    c.sparkle(94, 34, 6)
    return c


def qstreak_polish():
    c = Canvas()
    c.rect(52, 23, 24, 18, NAVY, outline=CYAN, width=2)
    c.rect(43, 40, 42, 60, BLUE, outline=BLUE_DARK, width=4)
    c.rect(49, 54, 30, 28, ICE, outline=CYAN, width=2)
    c.line(55, 89, 76, 56, SILVER, 4)
    c.sparkle(81, 50, 8)
    c.text_q(64, 69, 19, BLUE_MID)
    return c


def qgold_badge():
    c = Canvas()
    c.circle(64, 61, 39, GOLD, outline=BLUE_DARK, width=5)
    c.poly([(64, 91), (78, 113), (64, 106), (50, 113)], BLUE_MID, outline=BLUE_DARK, width=2)
    c.circle(64, 61, 25, ICE, outline=CYAN, width=3)
    c.text_q(64, 62, 29, BLUE_MID)
    c.sparkle(96, 31, 6)
    return c


def qhigh_roller():
    c = Canvas()
    c.poly([(27, 86), (39, 33), (89, 33), (101, 86)], NAVY, outline=BLUE_DARK, width=4)
    c.rect(32, 78, 64, 18, BLUE, outline=CYAN, width=2)
    for x in [44, 64, 84]:
        coin(c, x, 61, 14)
    c.poly([(50, 38), (64, 20), (78, 38)], GOLD, outline=BLUE_DARK, width=2)
    c.sparkle(97, 36, 6)
    return c


def qvelvet_frame():
    c = Canvas()
    c.rect(28, 27, 72, 72, PURPLE, outline=BLUE_DARK, width=5)
    c.rect(41, 40, 46, 46, ICE, outline=CYAN, width=3)
    c.circle(64, 63, 18, BLUE_MID, outline=BLUE_DARK, width=3)
    c.text_q(64, 64, 21, ICE)
    c.sparkle(97, 32, 6)
    return c


def qticket_charm():
    c = qticket()
    c.circle(91, 88, 20, GREEN, outline=BLUE_DARK, width=3)
    c.line(80, 88, 88, 97, ICE, 5)
    c.line(88, 97, 104, 78, ICE, 5)
    c.sparkle(100, 61, 5)
    return c


def qcooldown_clock():
    c = qtimer(-math.pi / 3)
    c.poly([(83, 25), (106, 25), (91, 44)], CYAN, outline=BLUE_DARK, width=2)
    c.line(96, 29, 82, 29, CYAN, 4)
    c.sparkle(99, 48, 5)
    return c


def qroyal_crown():
    c = Canvas()
    c.poly([(25, 83), (35, 37), (54, 65), (64, 27), (75, 65), (94, 37), (103, 83)], GOLD, outline=BLUE_DARK, width=4)
    c.rect(32, 80, 64, 18, BLUE_MID, outline=BLUE_DARK, width=3)
    for x, y in [(35, 37), (64, 27), (94, 37)]:
        c.circle(x, y, 7, ICE, outline=CYAN, width=2)
    coin(c, 64, 74, 16)
    c.sparkle(101, 28, 6)
    return c


def qtower():
    c = Canvas()
    c.rect(38, 37, 52, 67, BLUE_MID, outline=BLUE_DARK, width=4)
    c.poly([(34, 37), (48, 22), (64, 37), (80, 22), (94, 37)], SILVER, outline=BLUE_DARK, width=3)
    for y in [50, 68, 86]:
        c.line(43, y, 85, y, CYAN, 2)
    for x in [51, 64, 77]:
        c.rect(x - 4, 52, 8, 12, ICE, outline=BLUE_DARK, width=1.5)
    c.rect(56, 83, 16, 21, NAVY, outline=CYAN, width=2)
    c.text_q(64, 72, 22, ICE)
    c.sparkle(96, 30, 6)
    return c


def qtower_door():
    c = Canvas()
    c.rect(34, 24, 60, 84, BLUE_MID, outline=BLUE_DARK, width=5)
    c.rect(43, 34, 42, 64, ICE, outline=CYAN, width=3)
    c.circle(77, 67, 4, GOLD, outline=BLUE_DARK, width=1)
    c.text_q(61, 64, 28, BLUE_MID)
    c.sparkle(96, 32, 6)
    return c


def qtower_trap():
    c = Canvas()
    c.rect(33, 36, 62, 64, BLUE_DARK, outline=NAVY, width=5)
    c.poly([(34, 36), (52, 24), (64, 36), (76, 24), (94, 36)], RED, outline=BLUE_DARK, width=3)
    c.line(45, 82, 83, 50, RED, 7)
    c.line(45, 50, 83, 82, RED, 7)
    c.circle(64, 66, 18, TRANSPARENT, outline=ICE, width=4)
    c.sparkle(96, 31, 6)
    return c


def qvault():
    c = Canvas()
    c.rect(25, 29, 78, 78, BLUE_DARK, outline=NAVY, width=5)
    c.circle(64, 68, 28, SILVER, outline=BLUE_MID, width=4)
    c.circle(64, 68, 13, NAVY, outline=CYAN, width=3)
    for angle in [0, math.tau / 3, math.tau * 2 / 3]:
        c.line(64, 68, 64 + math.cos(angle) * 21, 68 + math.sin(angle) * 21, ICE, 3)
    c.text_q(64, 68, 18, CYAN)
    c.sparkle(96, 32, 6)
    return c


def qvault_dial():
    c = qvault()
    c.circle(92, 91, 18, GOLD, outline=BLUE_DARK, width=3)
    c.line(92, 91, 104, 83, ICE, 4)
    c.line(92, 91, 84, 80, ICE, 3)
    return c

def qbank():
    c = Canvas()
    c.rect(25, 50, 78, 48, BLUE_DARK, outline=NAVY, width=5)
    c.poly([(20, 50), (64, 24), (108, 50)], SILVER, outline=BLUE_DARK, width=4)
    for x in [38, 54, 74, 90]:
        c.rect(x - 5, 56, 10, 34, ICE, outline=BLUE_MID, width=2)
    c.rect(28, 93, 72, 9, BLUE_MID, outline=BLUE_DARK, width=2)
    coin(c, 64, 68, 18)
    c.sparkle(100, 32, 6)
    return c

def qstreak_freeze():
    c = qstreak_fire()
    c.circle(41, 41, 18, ICE, outline=CYAN, width=4)
    for angle in [0, math.tau / 6, math.tau / 3]:
        x1 = 41 + math.cos(angle) * 4
        y1 = 41 + math.sin(angle) * 4
        x2 = 41 + math.cos(angle) * 15
        y2 = 41 + math.sin(angle) * 15
        c.line(x1, y1, x2, y2, BLUE_MID, 2)
        c.line(41 - (x2 - 41), 41 - (y2 - 41), 41 - (x1 - 41), 41 - (y1 - 41), BLUE_MID, 2)
    c.sparkle(28, 27, 5)
    return c

def qrob():
    c = Canvas()
    c.rect(30, 54, 68, 45, NAVY, outline=BLUE_DARK, width=4)
    c.poly([(32, 54), (43, 34), (87, 34), (98, 54)], BLUE_MID, outline=BLUE_DARK, width=4)
    c.circle(64, 76, 19, SILVER, outline=CYAN, width=3)
    c.text_q(64, 77, 21, BLUE_MID)
    c.line(35, 31, 95, 101, RED, 8)
    c.line(36, 31, 94, 101, ICE, 2)
    c.sparkle(100, 34, 5)
    return c

def qtutorial():
    c = qbook()
    c.circle(43, 85, 18, GOLD, outline=BLUE_DARK, width=3)
    c.line(43, 77, 43, 88, NAVY, 5)
    c.circle(43, 95, 3, NAVY)
    c.sparkle(97, 29, 6)
    return c

def qrecommend():
    c = Canvas()
    c.circle(64, 58, 35, BLUE_MID, outline=BLUE_DARK, width=5)
    c.poly([(54, 96), (74, 96), (69, 111), (59, 111)], GOLD, outline=BLUE_DARK, width=3)
    c.poly([(45, 56), (58, 68), (84, 40), (62, 79)], ICE, outline=CYAN, width=3)
    c.text_q(64, 59, 22, BLUE_MID)
    c.sparkle(99, 33, 7)
    return c

def qseason_pass():
    c = Canvas()
    c.rect(28, 31, 72, 74, BLUE_MID, outline=BLUE_DARK, width=5)
    c.rect(38, 44, 52, 12, ICE, outline=CYAN, width=2)
    c.rect(38, 64, 36, 8, SILVER, outline=BLUE_DARK, width=1.5)
    c.rect(38, 80, 44, 8, SILVER, outline=BLUE_DARK, width=1.5)
    c.circle(88, 91, 19, GOLD, outline=BLUE_DARK, width=3)
    c.text_q(88, 92, 18, BLUE_MID)
    c.sparkle(99, 28, 6)
    return c


def arrow_arc(c, cx, cy, r, start, end, color=CYAN, width=5):
    steps = 18
    last = None
    for i in range(steps + 1):
        t = i / steps
        a = start + (end - start) * t
        point = (cx + math.cos(a) * r, cy + math.sin(a) * r)
        if last is not None:
            c.line(last[0], last[1], point[0], point[1], color, width)
        last = point
    a = end
    tip = (cx + math.cos(a) * r, cy + math.sin(a) * r)
    tangent = a + math.pi / 2 if end >= start else a - math.pi / 2
    c.poly(
        [
            tip,
            (tip[0] - math.cos(tangent - 0.75) * 14, tip[1] - math.sin(tangent - 0.75) * 14),
            (tip[0] - math.cos(tangent + 0.75) * 14, tip[1] - math.sin(tangent + 0.75) * 14),
        ],
        color,
        outline=BLUE_DARK,
        width=1.5,
    )


def qrefresh():
    c = Canvas()
    coin(c, 64, 64, 31)
    arrow_arc(c, 64, 64, 47, -math.pi * 0.85, math.pi * 0.35, CYAN, 5)
    arrow_arc(c, 64, 64, 47, math.pi * 0.15, math.pi * 1.35, ICE, 4)
    c.sparkle(100, 31, 6)
    return c


def qqueue():
    c = Canvas()
    c.rect(27, 29, 74, 74, NAVY, outline=BLUE_DARK, width=5)
    for i, y in enumerate([45, 63, 81]):
        c.circle(41, y, 6, [CYAN, SILVER, BLUE][i], outline=BLUE_DARK, width=1.5)
        c.rect(52, y - 5, 35, 10, [ICE, BLUE, CYAN][i], outline=BLUE_MID, width=1.5)
    c.line(90, 38, 103, 25, CYAN, 4)
    c.poly([(103, 25), (91, 27), (101, 38)], CYAN, outline=BLUE_DARK, width=1.5)
    coin(c, 36, 99, 14)
    c.sparkle(98, 34, 5)
    return c


def qrecovery():
    c = Canvas()
    c.circle(64, 65, 42, NAVY, outline=BLUE_DARK, width=5)
    arrow_arc(c, 64, 65, 30, math.pi * 0.15, math.pi * 1.65, CYAN, 6)
    c.line(39, 66, 50, 66, ICE, 4)
    c.line(50, 66, 56, 53, ICE, 4)
    c.line(56, 53, 66, 78, ICE, 4)
    c.line(66, 78, 75, 62, ICE, 4)
    c.line(75, 62, 89, 62, ICE, 4)
    c.text_q(64, 66, 20, BLUE_MID)
    c.sparkle(98, 34, 6)
    return c


def qerror_log():
    c = Canvas()
    c.rect(35, 25, 58, 78, ICE, outline=BLUE_DARK, width=4)
    c.poly([(76, 25), (93, 42), (76, 42)], SILVER, outline=BLUE_MID, width=1.5)
    c.poly([(64, 45), (88, 88), (40, 88)], RED, outline=BLUE_DARK, width=4)
    c.line(64, 58, 64, 74, ICE, 5)
    c.circle(64, 81, 3.5, ICE)
    c.line(45, 36, 68, 36, BLUE_MID, 2)
    c.sparkle(99, 31, 5)
    return c


def qdatabase():
    c = Canvas()
    c.ellipse(64, 35, 34, 14, ICE, outline=BLUE_DARK, width=4)
    c.rect(30, 35, 68, 58, BLUE_MID, outline=BLUE_DARK, width=4)
    c.ellipse(64, 35, 34, 14, ICE, outline=CYAN, width=2)
    c.ellipse(64, 64, 34, 14, BLUE, outline=CYAN, width=2)
    c.ellipse(64, 93, 34, 14, SILVER, outline=BLUE_DARK, width=4)
    c.text_q(64, 64, 28, ICE)
    c.sparkle(99, 34, 6)
    return c


def qsnipe():
    c = Canvas()
    c.circle(64, 64, 43, TRANSPARENT, outline=BLUE_DARK, width=5)
    c.circle(64, 64, 30, TRANSPARENT, outline=CYAN, width=3)
    c.circle(64, 64, 12, TRANSPARENT, outline=ICE, width=3)
    c.line(64, 16, 64, 39, CYAN, 4)
    c.line(64, 89, 64, 112, CYAN, 4)
    c.line(16, 64, 39, 64, CYAN, 4)
    c.line(89, 64, 112, 64, CYAN, 4)
    coin(c, 64, 64, 13)
    c.sparkle(98, 34, 6)
    return c


def qmemory():
    c = Canvas()
    c.rect(27, 27, 34, 34, BLUE_MID, outline=BLUE_DARK, width=3)
    c.rect(67, 27, 34, 34, ICE, outline=BLUE_DARK, width=3)
    c.rect(27, 67, 34, 34, ICE, outline=BLUE_DARK, width=3)
    c.rect(67, 67, 34, 34, BLUE_MID, outline=BLUE_DARK, width=3)
    c.text_q(44, 44, 18, ICE)
    c.text_q(84, 84, 18, ICE)
    c.circle(84, 44, 9, GOLD, outline=BLUE_DARK, width=2)
    c.circle(44, 84, 9, GOLD, outline=BLUE_DARK, width=2)
    c.sparkle(102, 28, 6)
    return c


def qmemory_tile():
    c = qtile(BLUE_DARK, NAVY, False)
    c.circle(64, 64, 25, BLUE_MID, outline=CYAN, width=3)
    c.text_q(64, 65, 29, ICE)
    return c


def qheist():
    c = Canvas()
    c.rect(32, 45, 64, 50, NAVY, outline=BLUE_DARK, width=4)
    c.poly([(32, 45), (44, 28), (84, 28), (96, 45)], BLUE_MID, outline=BLUE_DARK, width=3)
    c.rect(42, 58, 44, 28, ICE, outline=CYAN, width=2)
    c.line(47, 51, 81, 88, RED, 6)
    c.line(81, 51, 47, 88, RED, 6)
    coin(c, 64, 71, 16)
    c.poly([(89, 27), (106, 34), (94, 45)], GOLD, outline=BLUE_DARK, width=2)
    c.sparkle(101, 29, 6)
    return c


def qdice_duel():
    c = Canvas()
    c.rect(24, 31, 42, 42, ICE, outline=BLUE_DARK, width=4)
    c.rect(62, 55, 42, 42, BLUE_MID, outline=BLUE_DARK, width=4)
    for x, y in [(37, 44), (53, 60), (45, 52)]:
        c.circle(x, y, 3.5, BLUE_MID)
    for x, y in [(75, 68), (91, 84)]:
        c.circle(x, y, 3.5, ICE)
    c.line(31, 91, 94, 28, CYAN, 4)
    c.poly([(94, 28), (82, 29), (92, 41)], CYAN, outline=BLUE_DARK, width=1.5)
    c.sparkle(98, 35, 6)
    return c


def qcases():
    c = Canvas()
    c.rect(25, 44, 78, 55, BLUE_DARK, outline=NAVY, width=5)
    c.rect(36, 33, 56, 18, SILVER, outline=BLUE_DARK, width=3)
    c.rect(53, 40, 22, 61, BLUE, outline=CYAN, width=2)
    c.circle(64, 70, 17, GOLD, outline=BLUE_DARK, width=3)
    c.text_q(64, 71, 20, BLUE_MID)
    c.ellipse(64, 47, 31, 7, (128, 221, 255, 120))
    c.sparkle(99, 33, 7)
    return c


def qplinko(ball_step=None):
    c = Canvas()
    c.poly([(27, 28), (101, 28), (91, 103), (37, 103)], NAVY, outline=BLUE_DARK, width=4)
    for y in [45, 61, 77]:
        for x in [46, 64, 82] if y != 61 else [37, 55, 73, 91]:
            c.circle(x, y, 4, ICE, outline=CYAN, width=1)
    if ball_step is None:
        bx, by = 64, 35
    else:
        path = [(64, 35), (56, 48), (68, 60), (53, 74), (65, 88)]
        step = min(len(path) - 1, max(0, int(ball_step)))
        bx, by = path[step]
    c.circle(bx, by, 10, GOLD, outline=BLUE_DARK, width=2)
    for x, col in [(43, BLUE), (64, GREEN), (85, PURPLE)]:
        c.rect(x - 9, 91, 18, 15, col, outline=CYAN, width=1.5)
    c.line(64, 44, 54, 58, CYAN, 2)
    c.line(54, 58, 74, 75, CYAN, 2)
    c.sparkle(98, 29, 6)
    return c


def qlucky_number():
    c = Canvas()
    c.circle(64, 64, 42, BLUE_MID, outline=BLUE_DARK, width=5)
    c.circle(64, 64, 30, ICE, outline=CYAN, width=3)
    c.line(64, 42, 64, 66, BLUE_DARK, 8)
    c.circle(64, 85, 5, BLUE_DARK)
    coin(c, 92, 91, 15)
    c.sparkle(96, 32, 7)
    return c


def qjackpot_spin(offset=0):
    c = Canvas()
    c.rect(24, 33, 80, 62, NAVY, outline=BLUE_DARK, width=5)
    c.rect(34, 45, 60, 28, ICE, outline=CYAN, width=3)
    colors = [BLUE_MID, CYAN, GOLD, PURPLE]
    for index, x in enumerate([45, 64, 83]):
        col = colors[(index + int(offset)) % len(colors)]
        c.circle(x, 59, 8, col, outline=BLUE_DARK, width=1.5)
    c.poly([(51, 31), (64, 14), (77, 31)], GOLD, outline=BLUE_DARK, width=2)
    c.rect(40, 80, 48, 9, BLUE, outline=CYAN, width=2)
    c.sparkle(100, 32, 7)
    return c


def qdouble_nothing(angle=0):
    c = Canvas()
    flip_coin(c, 47, 67, angle)
    flip_coin(c, 82, 61, math.pi + angle)
    c.line(31, 28, 92, 28, CYAN, 5)
    c.poly([(92, 28), (80, 19), (84, 39)], CYAN, outline=BLUE_DARK, width=1.5)
    c.line(97, 99, 36, 99, CYAN, 5)
    c.poly([(36, 99), (48, 90), (44, 110)], CYAN, outline=BLUE_DARK, width=1.5)
    c.sparkle(101, 45, 6)
    return c


def qgame_stats():
    c = Canvas()
    c.rect(27, 35, 74, 62, NAVY, outline=BLUE_DARK, width=4)
    for x, h, col in [(42, 24, BLUE), (60, 38, CYAN), (78, 50, GOLD)]:
        c.rect(x, 86 - h, 11, h, col, outline=BLUE_DARK, width=1.5)
    c.line(37, 88, 92, 88, ICE, 2)
    c.circle(91, 38, 13, GREEN, outline=BLUE_DARK, width=2)
    c.text_q(91, 39, 13, ICE)
    c.sparkle(100, 27, 6)
    return c


def qbadge():
    c = Canvas()
    c.circle(64, 55, 34, GOLD, outline=BLUE_DARK, width=5)
    c.poly([(46, 82), (56, 112), (64, 94), (72, 112), (82, 82)], BLUE_MID, outline=BLUE_DARK, width=2)
    c.circle(64, 55, 20, ICE, outline=CYAN, width=3)
    c.text_q(64, 56, 24, BLUE_MID)
    c.sparkle(95, 28, 6)
    return c

def qaudit():
    c = qgame_stats()
    c.circle(84, 82, 18, TRANSPARENT, outline=ICE, width=5)
    c.line(97, 95, 111, 109, ICE, 6)
    c.line(97, 95, 111, 109, BLUE_DARK, 2)
    c.sparkle(23, 30, 6)
    return c

def qlimits():
    c = Canvas()
    c.poly([(64, 14), (103, 30), (96, 83), (64, 113), (32, 83), (25, 30)], BLUE_MID, outline=BLUE_DARK, width=5)
    c.circle(64, 62, 27, NAVY, outline=CYAN, width=4)
    c.line(64, 62, 79, 47, GOLD, 6)
    c.circle(64, 62, 5, ICE)
    c.line(44, 83, 84, 83, ICE, 4)
    c.sparkle(96, 29, 7)
    return c

def qhistory():
    c = Canvas()
    c.rect(30, 24, 62, 82, ICE, outline=BLUE_DARK, width=4)
    c.rect(38, 36, 46, 8, BLUE, outline=CYAN, width=1)
    c.rect(38, 53, 38, 7, BLUE_MID, outline=CYAN, width=1)
    c.rect(38, 69, 46, 7, BLUE_MID, outline=CYAN, width=1)
    c.circle(85, 86, 20, NAVY, outline=CYAN, width=4)
    c.line(85, 86, 85, 73, ICE, 4)
    c.line(85, 86, 96, 92, ICE, 4)
    return c

def qachievement_locked():
    c = qbadge()
    c.rect(78, 72, 28, 25, NAVY, outline=BLUE_DARK, width=3)
    c.circle(92, 72, 11, TRANSPARENT, outline=ICE, width=4)
    c.circle(92, 84, 4, GOLD)
    return c

def qachievement_unlocked():
    c = qbadge()
    c.circle(91, 84, 18, GREEN, outline=BLUE_DARK, width=3)
    c.line(82, 84, 89, 91, ICE, 4)
    c.line(89, 91, 101, 76, ICE, 4)
    return c

def qrisk(color):
    c = Canvas()
    c.poly([(64, 14), (111, 102), (17, 102)], color, outline=BLUE_DARK, width=5)
    c.line(64, 42, 64, 75, NAVY, 8)
    c.circle(64, 90, 5, NAVY)
    c.sparkle(98, 36, 6)
    return c

def qrisk_low():
    return qrisk(GREEN)

def qrisk_medium():
    return qrisk(GOLD)

def qrisk_high():
    return qrisk(RED)

def qrisk_extreme():
    return qrisk(PURPLE)

def qperf():
    c = Canvas()
    c.circle(64, 71, 43, NAVY, outline=BLUE_DARK, width=5)
    c.circle(64, 71, 31, BLUE_MID, outline=CYAN, width=3)
    for angle in [-0.85, -0.35, 0.15, 0.65]:
        x1 = 64 + math.cos(angle) * 23
        y1 = 71 + math.sin(angle) * 23
        x2 = 64 + math.cos(angle) * 30
        y2 = 71 + math.sin(angle) * 30
        c.line(x1, y1, x2, y2, ICE, 2)
    c.line(64, 71, 89, 52, GOLD, 5)
    c.circle(64, 71, 5, ICE)
    c.text_q(64, 104, 18, CYAN)
    return c

def qfilter():
    c = Canvas()
    c.poly([(21, 28), (107, 28), (76, 66), (76, 100), (54, 112), (54, 66)], BLUE, outline=BLUE_DARK, width=5)
    c.line(34, 39, 94, 39, ICE, 4)
    c.line(49, 58, 79, 58, CYAN, 3)
    c.sparkle(94, 88, 7)
    return c

def qdungeon():
    c = Canvas()
    c.poly([(26, 104), (26, 50), (39, 31), (64, 22), (89, 31), (102, 50), (102, 104)], NAVY, outline=BLUE_DARK, width=5)
    c.poly([(39, 104), (39, 58), (48, 43), (64, 36), (80, 43), (89, 58), (89, 104)], BLUE_MID, outline=CYAN, width=3)
    c.rect(52, 66, 24, 38, NAVY, outline=ICE, width=2)
    c.circle(64, 75, 18, BLUE_DARK, outline=CYAN, width=3)
    c.text_q(64, 76, 21, ICE)
    c.line(35, 95, 93, 95, SILVER, 3)
    c.sparkle(99, 36, 7)
    return c

def qdungeon_heart():
    c = Canvas()
    c.circle(49, 52, 20, CYAN, outline=BLUE_DARK, width=4)
    c.circle(79, 52, 20, CYAN, outline=BLUE_DARK, width=4)
    c.poly([(31, 60), (64, 104), (97, 60), (84, 44), (64, 56), (44, 44)], BLUE, outline=BLUE_DARK, width=4)
    c.circle(64, 70, 18, ICE, outline=CYAN, width=2)
    c.text_q(64, 71, 20, BLUE_MID)
    c.sparkle(96, 32, 6)
    return c

def qdungeon_key():
    c = Canvas()
    c.circle(45, 57, 23, GOLD, outline=BLUE_DARK, width=4)
    c.circle(45, 57, 11, NAVY, outline=ICE, width=3)
    c.line(61, 72, 101, 101, GOLD, 10)
    c.line(64, 70, 104, 99, ICE, 3)
    c.rect(88, 90, 13, 11, GOLD, outline=BLUE_DARK, width=2)
    c.rect(99, 98, 9, 11, GOLD, outline=BLUE_DARK, width=2)
    c.text_q(45, 57, 15, CYAN)
    c.sparkle(94, 34, 6)
    return c

def qdungeon_relic():
    c = Canvas()
    c.poly([(64, 17), (99, 43), (86, 96), (64, 111), (42, 96), (29, 43)], BLUE_MID, outline=BLUE_DARK, width=5)
    c.poly([(64, 25), (87, 45), (79, 88), (64, 99), (49, 88), (41, 45)], CYAN, outline=ICE, width=2)
    c.line(64, 26, 64, 99, BLUE_DARK, 2)
    c.line(42, 45, 86, 45, ICE, 2)
    c.text_q(64, 66, 28, BLUE_DARK)
    c.sparkle(96, 33, 7)
    c.sparkle(31, 79, 5)
    return c

def qdungeon_monster():
    c = Canvas()
    c.circle(64, 67, 39, BLUE_DARK, outline=CYAN, width=5)
    c.poly([(36, 39), (23, 22), (49, 32)], BLUE_MID, outline=BLUE_DARK, width=3)
    c.poly([(92, 39), (105, 22), (79, 32)], BLUE_MID, outline=BLUE_DARK, width=3)
    c.circle(50, 59, 7, ICE, outline=BLUE, width=2)
    c.circle(78, 59, 7, ICE, outline=BLUE, width=2)
    c.circle(50, 59, 3, RED)
    c.circle(78, 59, 3, RED)
    c.line(48, 83, 80, 83, ICE, 5)
    for x in [56, 64, 72]:
        c.poly([(x - 4, 84), (x + 4, 84), (x, 93)], CYAN, outline=BLUE_DARK, width=1)
    c.text_q(64, 42, 17, CYAN)
    c.sparkle(96, 33, 6)
    return c


def qbirthday():
    c = Canvas()
    c.rect(31, 60, 66, 38, BLUE, outline=BLUE_DARK, width=4)
    c.rect(27, 50, 74, 17, ICE, outline=BLUE_DARK, width=3)
    for x in [42, 64, 86]:
        c.rect(x - 3, 31, 6, 19, GOLD, outline=BLUE_DARK, width=1.5)
        c.poly([(x, 23), (x - 5, 33), (x + 5, 33)], RED, outline=BLUE_DARK, width=1)
    c.text_q(64, 79, 24, ICE)
    c.sparkle(98, 36, 6)
    return c


def qbirthday_cake():
    c = Canvas()
    c.ellipse(64, 96, 45, 12, BLUE_DARK)
    c.rect(28, 58, 72, 35, BLUE, outline=BLUE_DARK, width=4)
    c.rect(24, 45, 80, 18, ICE, outline=BLUE_DARK, width=3)
    c.rect(32, 71, 64, 15, CYAN, outline=BLUE_DARK, width=2)
    for x in [42, 56, 70, 84]:
        c.rect(x - 3, 28, 6, 18, GOLD, outline=BLUE_DARK, width=1)
        c.poly([(x, 18), (x - 5, 30), (x + 5, 30)], RED, outline=BLUE_DARK, width=1)
    c.text_q(64, 80, 20, ICE)
    c.sparkle(102, 35, 7)
    c.sparkle(24, 50, 5)
    return c


def qbirthday_balloons():
    c = Canvas()
    for x, y, color in [(43, 44, BLUE), (66, 34, CYAN), (88, 48, PURPLE)]:
        c.ellipse(x, y, 17, 22, color, outline=BLUE_DARK, width=3)
        c.poly([(x, y + 22), (x - 5, y + 31), (x + 5, y + 31)], color, outline=BLUE_DARK, width=1)
        c.line(x, y + 31, 64, 102, SILVER, 2)
        c.circle(x - 5, y - 7, 4, ICE)
    c.circle(64, 93, 17, BLUE_DARK, outline=CYAN, width=3)
    c.text_q(64, 95, 18, ICE)
    c.sparkle(103, 30, 6)
    c.sparkle(26, 74, 5)
    return c


def qhammer():
    c = Canvas()
    c.rect(36, 27, 48, 20, SILVER, outline=BLUE_DARK, width=3)
    c.rect(75, 33, 21, 12, BLUE_MID, outline=BLUE_DARK, width=2)
    c.line(54, 47, 89, 98, BLUE, 9)
    c.line(58, 50, 93, 101, ICE, 3)
    coin(c, 38, 86, 13)
    c.sparkle(95, 30, 6)
    return c


def qtrash():
    c = Canvas()
    c.rect(39, 43, 50, 57, BLUE_MID, outline=BLUE_DARK, width=4)
    c.rect(34, 34, 60, 12, SILVER, outline=BLUE_DARK, width=3)
    c.rect(53, 24, 22, 10, ICE, outline=BLUE_DARK, width=2)
    for x in [52, 64, 76]:
        c.line(x, 52, x, 91, CYAN, 2)
    c.sparkle(96, 36, 6)
    return c


def qedit():
    c = Canvas()
    c.rect(31, 31, 55, 66, ICE, outline=BLUE_DARK, width=4)
    c.line(45, 51, 73, 51, BLUE_MID, 3)
    c.line(45, 64, 68, 64, BLUE_MID, 3)
    c.line(45, 77, 61, 77, BLUE_MID, 3)
    c.line(70, 88, 101, 57, GOLD, 8)
    c.line(74, 91, 104, 61, ICE, 3)
    c.poly([(98, 51), (108, 61), (102, 67), (92, 57)], RED, outline=BLUE_DARK, width=1.5)
    return c


def qimage():
    c = Canvas()
    c.rect(25, 32, 78, 64, ICE, outline=BLUE_DARK, width=4)
    c.circle(82, 50, 8, GOLD, outline=BLUE_DARK, width=2)
    c.poly([(31, 88), (52, 61), (67, 78), (77, 66), (98, 89)], BLUE, outline=BLUE_DARK, width=2)
    c.sparkle(99, 32, 6)
    return c


def qlock():
    c = Canvas()
    c.rect(34, 57, 60, 42, BLUE_MID, outline=BLUE_DARK, width=4)
    c.ellipse(64, 58, 25, 31, TRANSPARENT, outline=SILVER, width=7)
    c.circle(64, 76, 8, GOLD, outline=BLUE_DARK, width=2)
    c.line(64, 82, 64, 92, GOLD, 4)
    c.sparkle(95, 37, 6)
    return c


def qwarning():
    c = Canvas()
    c.poly([(64, 22), (108, 101), (20, 101)], GOLD, outline=BLUE_DARK, width=5)
    c.line(64, 48, 64, 75, NAVY, 7)
    c.circle(64, 88, 5, NAVY)
    c.sparkle(99, 39, 5)
    return c


def qbroom():
    c = Canvas()
    c.line(34, 24, 88, 91, BLUE, 8)
    c.line(38, 24, 92, 91, ICE, 2)
    c.poly([(76, 75), (107, 91), (86, 111), (63, 88)], GOLD, outline=BLUE_DARK, width=3)
    for x in [75, 83, 91, 99]:
        c.line(x, 88, x - 13, 105, BLUE_DARK, 1.5)
    c.sparkle(95, 40, 6)
    return c


def qattachment():
    c = Canvas()
    c.ellipse(63, 67, 31, 41, TRANSPARENT, outline=BLUE, width=8)
    c.ellipse(70, 66, 18, 27, TRANSPARENT, outline=ICE, width=5)
    c.line(78, 45, 53, 79, BLUE_DARK, 5)
    c.line(81, 46, 56, 80, CYAN, 2)
    c.sparkle(95, 35, 6)
    return c


def qpoll():
    c = Canvas()
    c.rect(28, 27, 72, 74, ICE, outline=BLUE_DARK, width=4)
    for y, w, col in [(48, 44, BLUE), (65, 57, CYAN), (82, 31, GOLD)]:
        c.rect(40, y, w, 9, col, outline=BLUE_DARK, width=1.5)
    c.text_q(64, 34, 13, BLUE_MID)
    c.sparkle(96, 36, 6)
    return c


def qreaction():
    c = Canvas()
    c.circle(64, 63, 39, ICE, outline=BLUE_DARK, width=4)
    c.circle(50, 56, 5, BLUE_DARK)
    c.circle(78, 56, 5, BLUE_DARK)
    c.ellipse(64, 74, 18, 10, TRANSPARENT, outline=BLUE_MID, width=4)
    c.poly([(88, 86), (105, 101), (81, 98)], BLUE, outline=BLUE_DARK, width=2)
    c.sparkle(96, 34, 6)
    return c


def qtimeout():
    c = qtimer(-math.pi / 2)
    c.poly([(78, 83), (103, 108), (109, 102), (84, 77)], RED, outline=BLUE_DARK, width=2)
    c.poly([(103, 77), (78, 102), (84, 108), (109, 83)], RED, outline=BLUE_DARK, width=2)
    return c


def quser_edit():
    c = Canvas()
    c.circle(54, 48, 18, ICE, outline=BLUE_DARK, width=3)
    c.ellipse(54, 84, 31, 23, BLUE, outline=BLUE_DARK, width=3)
    c.line(75, 91, 104, 62, GOLD, 7)
    c.line(78, 94, 107, 65, ICE, 2)
    c.sparkle(96, 35, 6)
    return c


def qroles():
    c = Canvas()
    c.circle(48, 53, 20, BLUE, outline=BLUE_DARK, width=3)
    c.circle(78, 53, 20, PURPLE, outline=BLUE_DARK, width=3)
    c.ellipse(63, 88, 43, 23, ICE, outline=BLUE_DARK, width=3)
    c.text_q(64, 86, 20, BLUE_MID)
    c.sparkle(97, 35, 6)
    return c


def qvoice():
    c = Canvas()
    c.rect(43, 26, 30, 58, BLUE, outline=BLUE_DARK, width=4)
    c.ellipse(58, 84, 25, 13, BLUE, outline=BLUE_DARK, width=3)
    c.line(58, 84, 58, 103, BLUE_DARK, 5)
    for r in [24, 34, 44]:
        c.ellipse(75, 60, r, r, TRANSPARENT, outline=CYAN, width=2)
    c.sparkle(96, 34, 6)
    return c


def qpermissions():
    c = Canvas()
    c.poly([(64, 21), (100, 37), (94, 83), (64, 107), (34, 83), (28, 37)], BLUE_MID, outline=BLUE_DARK, width=4)
    c.line(45, 65, 59, 81, ICE, 7)
    c.line(59, 81, 86, 49, ICE, 7)
    c.text_q(64, 52, 20, CYAN)
    return c


def qaccept():
    c = Canvas()
    c.circle(64, 64, 43, GREEN, outline=BLUE_DARK, width=5)
    c.line(39, 65, 56, 82, ICE, 9)
    c.line(56, 82, 91, 43, ICE, 9)
    c.sparkle(96, 34, 6)
    return c


def qreject():
    c = Canvas()
    c.circle(64, 64, 43, RED, outline=BLUE_DARK, width=5)
    c.line(41, 41, 87, 87, ICE, 9)
    c.line(87, 41, 41, 87, ICE, 9)
    c.sparkle(96, 34, 6)
    return c


def qgame_win():
    c = Canvas()
    c.poly([(64, 21), (72, 47), (100, 47), (77, 63), (87, 94), (64, 75), (41, 94), (51, 63), (28, 47), (56, 47)], GOLD, outline=BLUE_DARK, width=4)
    coin(c, 64, 63, 18)
    c.sparkle(100, 33, 7)
    return c


def qgame_timeout():
    c = qtimeout()
    c.poly([(35, 96), (64, 70), (93, 96)], GOLD, outline=BLUE_DARK, width=3)
    c.sparkle(95, 34, 6)
    return c


def qgame_x():
    c = Canvas()
    c.circle(64, 64, 43, ICE, outline=BLUE_DARK, width=4)
    c.line(40, 40, 88, 88, BLUE_MID, 11)
    c.line(88, 40, 40, 88, BLUE_MID, 11)
    c.sparkle(96, 34, 6)
    return c


def qgame_o():
    c = Canvas()
    c.circle(64, 64, 43, ICE, outline=BLUE_DARK, width=4)
    c.circle(64, 64, 24, TRANSPARENT, outline=BLUE_MID, width=11)
    c.sparkle(96, 34, 6)
    return c


def qconnect_black():
    c = Canvas()
    c.circle(64, 64, 42, NAVY, outline=CYAN, width=5)
    c.text_q(64, 66, 27, CYAN)
    c.sparkle(96, 34, 6)
    return c


def qconnect_white():
    c = Canvas()
    c.circle(64, 64, 42, ICE, outline=BLUE_DARK, width=5)
    c.text_q(64, 66, 27, BLUE_MID)
    c.sparkle(96, 34, 6)
    return c


def qgift():
    c = Canvas()
    c.rect(30, 58, 68, 42, BLUE, outline=BLUE_DARK, width=4)
    c.rect(25, 45, 78, 18, ICE, outline=BLUE_DARK, width=3)
    c.rect(58, 43, 13, 58, GOLD, outline=BLUE_DARK, width=2)
    c.ellipse(50, 39, 16, 10, TRANSPARENT, outline=CYAN, width=5)
    c.ellipse(78, 39, 16, 10, TRANSPARENT, outline=CYAN, width=5)
    c.sparkle(98, 35, 6)
    return c


def qalarm():
    c = qtimer(-math.pi / 4)
    c.circle(36, 33, 12, BLUE, outline=BLUE_DARK, width=2)
    c.circle(92, 33, 12, BLUE, outline=BLUE_DARK, width=2)
    c.sparkle(98, 54, 6)
    return c


def qbell():
    c = Canvas()
    c.ellipse(64, 68, 34, 39, GOLD, outline=BLUE_DARK, width=4)
    c.rect(46, 85, 36, 12, GOLD, outline=BLUE_DARK, width=3)
    c.circle(64, 101, 7, ICE, outline=BLUE_DARK, width=2)
    c.line(38, 28, 27, 40, CYAN, 3)
    c.line(90, 28, 101, 40, CYAN, 3)
    c.sparkle(96, 39, 6)
    return c


def qsleep():
    c = Canvas()
    c.circle(64, 70, 36, NAVY, outline=BLUE_DARK, width=4)
    c.circle(77, 58, 36, TRANSPARENT)
    c.line(46, 69, 60, 69, ICE, 4)
    c.line(70, 59, 86, 59, CYAN, 4)
    c.line(74, 52, 88, 52, CYAN, 3)
    c.sparkle(97, 35, 6)
    return c


def qbook():
    c = Canvas()
    c.poly([(28, 35), (62, 45), (62, 101), (28, 91)], ICE, outline=BLUE_DARK, width=3)
    c.poly([(100, 35), (66, 45), (66, 101), (100, 91)], BLUE, outline=BLUE_DARK, width=3)
    c.line(64, 45, 64, 101, CYAN, 3)
    c.text_q(82, 67, 22, ICE)
    c.sparkle(99, 34, 6)
    return c


def qthinking():
    c = Canvas()
    c.circle(64, 62, 38, ICE, outline=BLUE_DARK, width=4)
    c.circle(50, 55, 4, BLUE_DARK)
    c.circle(74, 55, 4, BLUE_DARK)
    c.line(55, 76, 75, 74, BLUE_MID, 4)
    c.circle(91, 91, 8, CYAN, outline=BLUE_DARK, width=2)
    c.circle(104, 103, 4, CYAN, outline=BLUE_DARK, width=1.5)
    c.sparkle(96, 34, 6)
    return c


def qstreak_fire():
    c = Canvas()
    c.poly([(64, 108), (37, 82), (52, 58), (57, 21), (76, 52), (87, 39), (92, 76)], BLUE, outline=BLUE_DARK, width=4)
    c.poly([(64, 99), (50, 79), (62, 59), (65, 36), (76, 66), (82, 83)], CYAN, outline=ICE, width=2)
    c.text_q(64, 78, 22, ICE)
    c.sparkle(96, 35, 6)
    return c


def qtarget():
    c = Canvas()
    for r, col in [(43, ICE), (32, BLUE), (21, ICE), (10, RED)]:
        c.circle(64, 64, r, col, outline=BLUE_DARK if r == 43 else None, width=4 if r == 43 else 0)
    c.line(64, 19, 64, 109, BLUE_DARK, 3)
    c.line(19, 64, 109, 64, BLUE_DARK, 3)
    c.sparkle(96, 34, 6)
    return c


def qcards():
    c = Canvas()
    c.rect(35, 31, 38, 58, ICE, outline=BLUE_DARK, width=3)
    c.rect(57, 40, 38, 58, BLUE, outline=BLUE_DARK, width=3)
    c.text_q(55, 61, 20, BLUE_MID)
    c.text_q(76, 70, 20, ICE)
    c.sparkle(98, 35, 6)
    return c


def qconfetti():
    c = Canvas()
    coin(c, 64, 73, 27)
    for pts, col in [
        ([(26, 32), (38, 27), (35, 43)], RED),
        ([(93, 28), (108, 35), (92, 44)], GOLD),
        ([(30, 89), (42, 96), (27, 104)], CYAN),
        ([(94, 90), (106, 99), (90, 105)], GREEN),
    ]:
        c.poly(pts, col, outline=BLUE_DARK, width=1)
    c.line(38, 35, 52, 52, CYAN, 3)
    c.line(88, 35, 76, 53, GOLD, 3)
    c.sparkle(99, 34, 7)
    return c

def qchess_piece(kind, white=True):
    c = Canvas()
    fill = ICE if white else BLUE_DARK
    mid = SILVER if white else BLUE
    edge = BLUE_MID if white else CYAN
    accent = CYAN if white else ICE
    shade = SILVER_DARK if white else (2, 14, 34, 255)

    c.ellipse(64, 104, 36, 10, (2, 14, 34, 115))
    c.rect(31, 91, 66, 10, mid, outline=edge, width=3)
    c.ellipse(64, 91, 34, 9, fill, outline=edge, width=3)
    c.rect(45, 70, 38, 24, fill, outline=edge, width=3)
    c.ellipse(64, 70, 22, 8, mid, outline=edge, width=2)

    if kind == "pawn":
        c.circle(64, 43, 18, fill, outline=edge, width=4)
        c.ellipse(64, 62, 20, 10, mid, outline=edge, width=3)
    elif kind == "knight":
        c.poly([(48, 67), (53, 39), (72, 25), (88, 43), (77, 50), (86, 68), (65, 70)], fill, outline=edge, width=4)
        c.poly([(54, 39), (48, 25), (65, 34)], fill, outline=edge, width=3)
        c.circle(70, 42, 3, edge)
        c.line(58, 55, 77, 61, shade, 2)
    elif kind == "bishop":
        c.ellipse(64, 48, 20, 28, fill, outline=edge, width=4)
        c.line(76, 30, 55, 63, accent, 4)
        c.circle(64, 23, 6, accent, outline=edge, width=2)
        c.ellipse(64, 71, 23, 9, mid, outline=edge, width=3)
    elif kind == "rook":
        c.rect(45, 35, 38, 35, fill, outline=edge, width=4)
        for x in (45, 58, 71):
            c.rect(x, 25, 10, 14, fill, outline=edge, width=2)
        c.rect(42, 68, 44, 8, mid, outline=edge, width=3)
    elif kind == "queen":
        c.poly([(40, 64), (46, 33), (56, 55), (64, 26), (72, 55), (82, 33), (88, 64)], fill, outline=edge, width=4)
        for x, y in [(46, 30), (64, 23), (82, 30)]:
            c.circle(x, y, 6, accent, outline=edge, width=2)
        c.rect(43, 63, 42, 9, mid, outline=edge, width=3)
    elif kind == "king":
        c.ellipse(64, 52, 20, 24, fill, outline=edge, width=4)
        c.line(64, 20, 64, 43, accent, 5)
        c.line(53, 30, 75, 30, accent, 5)
        c.rect(45, 65, 38, 8, mid, outline=edge, width=3)

    c.line(46, 95, 82, 95, ICE if not white else CYAN, 2)
    c.sparkle(96, 33, 6)
    return c

def qc4_number(number):
    c = Canvas()
    c.circle(64, 64, 42, BLUE_DARK, outline=NAVY, width=4)
    c.circle(64, 64, 34, BLUE, outline=CYAN, width=3)
    c.circle(50, 46, 5, ICE)

    digit_segments = {
        1: "bc",
        2: "abged",
        3: "abgcd",
        4: "fgbc",
        5: "afgcd",
        6: "afgecd",
        7: "abc",
    }
    segments = digit_segments[number]
    coords = {
        "a": (48, 36, 80, 36),
        "b": (82, 39, 82, 61),
        "c": (82, 67, 82, 89),
        "d": (48, 92, 80, 92),
        "e": (46, 67, 46, 89),
        "f": (46, 39, 46, 61),
        "g": (49, 64, 79, 64),
    }
    for segment in segments:
        c.line(*coords[segment], ICE, 7)
        c.line(*coords[segment], CYAN, 3)
    return c

def qchess_white_pawn():
    return qchess_piece("pawn", True)

def qchess_white_knight():
    return qchess_piece("knight", True)

def qchess_white_bishop():
    return qchess_piece("bishop", True)

def qchess_white_rook():
    return qchess_piece("rook", True)

def qchess_white_queen():
    return qchess_piece("queen", True)

def qchess_white_king():
    return qchess_piece("king", True)

def qchess_black_pawn():
    return qchess_piece("pawn", False)

def qchess_black_knight():
    return qchess_piece("knight", False)

def qchess_black_bishop():
    return qchess_piece("bishop", False)

def qchess_black_rook():
    return qchess_piece("rook", False)

def qchess_black_queen():
    return qchess_piece("queen", False)

def qchess_black_king():
    return qchess_piece("king", False)

def qc4_1():
    return qc4_number(1)

def qc4_2():
    return qc4_number(2)

def qc4_3():
    return qc4_number(3)

def qc4_4():
    return qc4_number(4)

def qc4_5():
    return qc4_number(5)

def qc4_6():
    return qc4_number(6)

def qc4_7():
    return qc4_number(7)

def qgem_symbol(kind):
    c = Canvas()
    colors = {
        "star": (GOLD, (185, 112, 22, 255)),
        "diamond": (CYAN, BLUE_MID),
        "crown": (GOLD, BLUE_DARK),
        "jackpot": (PURPLE, BLUE_DARK),
        "scratch": (SILVER, BLUE_MID),
    }
    fill, edge = colors[kind]
    c.circle(64, 64, 43, BLUE_DARK, outline=NAVY, width=4)
    if kind == "star":
        pts = []
        for i in range(10):
            r = 34 if i % 2 == 0 else 14
            a = -math.pi / 2 + i * math.pi / 5
            pts.append((64 + math.cos(a) * r, 64 + math.sin(a) * r))
        c.poly(pts, fill, outline=edge, width=3)
    elif kind == "diamond":
        c.poly([(64, 25), (99, 61), (64, 103), (29, 61)], fill, outline=edge, width=4)
        c.line(43, 61, 85, 61, ICE, 2)
        c.line(64, 27, 64, 101, ICE, 2)
    elif kind == "crown":
        c.poly([(30, 82), (38, 42), (55, 68), (64, 34), (73, 68), (90, 42), (98, 82)], fill, outline=edge, width=4)
        c.rect(36, 79, 56, 12, fill, outline=edge, width=3)
        for x in (38, 64, 90):
            c.circle(x, 39 if x != 64 else 31, 6, ICE, outline=edge, width=2)
    elif kind == "jackpot":
        coin(c, 64, 64, 31)
        c.text_q(64, 65, 35, fill)
    elif kind == "scratch":
        c.poly([(30, 42), (82, 27), (103, 82), (48, 101)], fill, outline=edge, width=4)
        c.line(43, 57, 85, 45, CYAN, 3)
        c.line(49, 77, 91, 65, BLUE_DARK, 3)
        c.sparkle(95, 36, 7)
    c.sparkle(99, 35, 6)
    return c

def qcolor_disc(fill, label=None):
    c = Canvas()
    c.circle(64, 64, 42, fill, outline=NAVY, width=4)
    c.circle(52, 48, 6, ICE)
    c.circle(64, 64, 28, TRANSPARENT, outline=(255, 255, 255, 90), width=2)
    if label:
        c.text_q(64, 66, 32, ICE if fill != ICE else BLUE)
    return c

def qtile(fill, edge=BLUE_DARK, q=False):
    c = Canvas()
    c.rect(28, 28, 72, 72, fill, outline=edge, width=5)
    c.line(37, 39, 90, 39, ICE, 3)
    if q:
        c.text_q(64, 67, 30, CYAN)
    return c

def qcard_suit(kind):
    c = Canvas()
    c.rect(31, 19, 66, 90, ICE, outline=BLUE_DARK, width=4)
    color = RED if kind in {"heart", "diamond"} else BLUE_DARK
    if kind == "spade":
        c.circle(53, 55, 16, color)
        c.circle(75, 55, 16, color)
        c.poly([(38, 62), (64, 30), (90, 62), (64, 86)], color)
        c.line(64, 70, 56, 91, color, 7)
        c.line(64, 70, 72, 91, color, 7)
    elif kind == "heart":
        c.circle(52, 52, 16, color)
        c.circle(76, 52, 16, color)
        c.poly([(36, 56), (92, 56), (64, 91)], color)
    elif kind == "diamond":
        c.poly([(64, 29), (91, 64), (64, 99), (37, 64)], color)
    elif kind == "club":
        c.circle(64, 43, 14, color)
        c.circle(50, 64, 14, color)
        c.circle(78, 64, 14, color)
        c.line(64, 69, 56, 93, color, 7)
        c.line(64, 69, 72, 93, color, 7)
    c.sparkle(94, 27, 5)
    return c

def qpoll_number(number):
    c = Canvas()
    c.circle(64, 64, 42, BLUE_DARK, outline=NAVY, width=4)
    c.circle(64, 64, 34, BLUE, outline=CYAN, width=3)
    c.circle(50, 46, 5, ICE)

    def draw_digit(digit, x_shift=0, y_shift=0, compact=False):
        if compact:
            coords = {
                "a": (46 + x_shift, 42 + y_shift, 62 + x_shift, 42 + y_shift),
                "b": (64 + x_shift, 44 + y_shift, 64 + x_shift, 60 + y_shift),
                "c": (64 + x_shift, 66 + y_shift, 64 + x_shift, 82 + y_shift),
                "d": (46 + x_shift, 84 + y_shift, 62 + x_shift, 84 + y_shift),
                "e": (44 + x_shift, 66 + y_shift, 44 + x_shift, 82 + y_shift),
                "f": (44 + x_shift, 44 + y_shift, 44 + x_shift, 60 + y_shift),
                "g": (47 + x_shift, 63 + y_shift, 61 + x_shift, 63 + y_shift),
            }
            width_outer, width_inner = 5, 2
        else:
            coords = {
                "a": (48 + x_shift, 36 + y_shift, 80 + x_shift, 36 + y_shift),
                "b": (82 + x_shift, 39 + y_shift, 82 + x_shift, 61 + y_shift),
                "c": (82 + x_shift, 67 + y_shift, 82 + x_shift, 89 + y_shift),
                "d": (48 + x_shift, 92 + y_shift, 80 + x_shift, 92 + y_shift),
                "e": (46 + x_shift, 67 + y_shift, 46 + x_shift, 89 + y_shift),
                "f": (46 + x_shift, 39 + y_shift, 46 + x_shift, 61 + y_shift),
                "g": (49 + x_shift, 64 + y_shift, 79 + x_shift, 64 + y_shift),
            }
            width_outer, width_inner = 7, 3
        digit_segments = {
            0: "abcdef",
            1: "bc",
            2: "abged",
            3: "abgcd",
            4: "fgbc",
            5: "afgcd",
            6: "afgecd",
            7: "abc",
            8: "abcdefg",
            9: "abfgcd",
        }
        for segment in digit_segments[digit]:
            c.line(*coords[segment], ICE, width_outer)
            c.line(*coords[segment], CYAN, width_inner)

    if number == 10:
        draw_digit(1, x_shift=-18, compact=True)
        draw_digit(0, x_shift=18, compact=True)
    else:
        draw_digit(number)
    return c

def qslot_star():
    return qgem_symbol("star")

def qslot_diamond():
    return qgem_symbol("diamond")

def qslot_crown():
    return qgem_symbol("crown")

def qslot_jackpot():
    return qgem_symbol("jackpot")

def qscratch_mark():
    return qgem_symbol("scratch")

def qmine_tile():
    return qtile(BLUE_DARK, NAVY, False)

def qmine_cursor():
    return qtile(GOLD, BLUE_DARK, True)

def qc4_empty_light():
    return qtile(ICE, CYAN, False)

def qc4_empty_dark():
    return qtile(BLUE_DARK, NAVY, False)

def qchess_dark():
    return qtile(BLUE_DARK, NAVY, False)

def qcard_spade():
    return qcard_suit("spade")

def qcard_heart():
    return qcard_suit("heart")

def qcard_diamond():
    return qcard_suit("diamond")

def qcard_club():
    return qcard_suit("club")

def qroulette_red():
    return qcolor_disc(RED)

def qroulette_black():
    return qcolor_disc(NAVY)

def qroulette_green():
    return qcolor_disc(GREEN)

def qwheel_red():
    return qcolor_disc(RED, "Q")

def qwheel_blue():
    return qcolor_disc(BLUE, "Q")

def qwheel_green():
    return qcolor_disc(GREEN, "Q")

def qwheel_orange():
    return qcolor_disc(GOLD, "Q")

def qwheel_purple():
    return qcolor_disc(PURPLE, "Q")

def qwheel_gold():
    return qcolor_disc(GOLD, "Q")

def qwheel_blank():
    return qcolor_disc(SILVER_DARK, "Q")

def qwheel_pink():
    return qcolor_disc((255, 85, 170, 255), "Q")

def qpoll_1():
    return qpoll_number(1)

def qpoll_2():
    return qpoll_number(2)

def qpoll_3():
    return qpoll_number(3)

def qpoll_4():
    return qpoll_number(4)

def qpoll_5():
    return qpoll_number(5)

def qpoll_6():
    return qpoll_number(6)

def qpoll_7():
    return qpoll_number(7)

def qpoll_8():
    return qpoll_number(8)

def qpoll_9():
    return qpoll_number(9)

def qpoll_10():
    return qpoll_number(10)

def qchess_file_letter(letter):
    c = Canvas()
    c.rect(20, 26, 88, 78, (8, 31, 60, 245), outline=NAVY, width=4)
    c.rect(28, 34, 72, 62, BLUE_DARK, outline=CYAN, width=3)
    c.circle(42, 47, 5, ICE)
    c.sparkle(93, 36, 5)

    strokes = {
        "A": [((42, 86), (54, 43)), ((54, 43), (74, 86)), ((49, 69), (67, 69))],
        "B": [((44, 43), (44, 86)), ((44, 43), (67, 43)), ((67, 43), (75, 51)), ((75, 51), (67, 61)), ((44, 61), (67, 61)), ((67, 61), (77, 72)), ((77, 72), (67, 86)), ((44, 86), (67, 86))],
        "C": [((76, 49), (66, 43)), ((66, 43), (48, 48)), ((48, 48), (42, 64)), ((42, 64), (48, 81)), ((48, 81), (66, 86)), ((66, 86), (76, 80))],
        "D": [((44, 43), (44, 86)), ((44, 43), (65, 43)), ((65, 43), (78, 57)), ((78, 57), (78, 72)), ((78, 72), (65, 86)), ((44, 86), (65, 86))],
        "E": [((74, 43), (44, 43)), ((44, 43), (44, 86)), ((44, 86), (76, 86)), ((44, 64), (70, 64))],
        "F": [((44, 43), (44, 86)), ((44, 43), (76, 43)), ((44, 64), (70, 64))],
        "G": [((77, 50), (66, 43)), ((66, 43), (48, 48)), ((48, 48), (42, 64)), ((42, 64), (48, 81)), ((48, 81), (67, 86)), ((67, 86), (78, 76)), ((78, 76), (78, 66)), ((78, 66), (64, 66))],
        "H": [((44, 43), (44, 86)), ((78, 43), (78, 86)), ((44, 64), (78, 64))],
    }
    for x1, y1, x2, y2 in [(a[0], a[1], b[0], b[1]) for a, b in strokes[letter]]:
        c.line(x1, y1, x2, y2, ICE, 8)
        c.line(x1, y1, x2, y2, CYAN, 4)
    return c

def qchess_file_a():
    return qchess_file_letter("A")

def qchess_file_b():
    return qchess_file_letter("B")

def qchess_file_c():
    return qchess_file_letter("C")

def qchess_file_d():
    return qchess_file_letter("D")

def qchess_file_e():
    return qchess_file_letter("E")

def qchess_file_f():
    return qchess_file_letter("F")

def qchess_file_g():
    return qchess_file_letter("G")

def qchess_file_h():
    return qchess_file_letter("H")


STATIC = {
    "QoinBag": qoin_bag,
    "QoinChest": qoin_chest,
    "QTicket": qticket,
    "QTicketMinus": qticket_minus,
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
    "QLuckyCharm": qlucky_charm,
    "QFortuneVial": qfortune_vial,
    "QActivity": qactivity,
    "QXPTonic": qxp_tonic,
    "QQuesoMagnet": qqueso_magnet,
    "QDailySpice": qdaily_spice,
    "QStreakPolish": qstreak_polish,
    "QGoldBadge": qgold_badge,
    "QHighRoller": qhigh_roller,
    "QVelvetFrame": qvelvet_frame,
    "QTicketCharm": qticket_charm,
    "QCooldownClock": qcooldown_clock,
    "QRoyalCrown": qroyal_crown,
    "QBirthday": qbirthday,
    "QBirthdayCake": qbirthday_cake,
    "QBirthdayBalloons": qbirthday_balloons,
    "QHammer": qhammer,
    "QTrash": qtrash,
    "QEdit": qedit,
    "QImage": qimage,
    "QLock": qlock,
    "QWarning": qwarning,
    "QBroom": qbroom,
    "QAttachment": qattachment,
    "QPoll": qpoll,
    "QReaction": qreaction,
    "QTimeout": qtimeout,
    "QUserEdit": quser_edit,
    "QRoles": qroles,
    "QVoice": qvoice,
    "QPermissions": qpermissions,
    "QAccept": qaccept,
    "QReject": qreject,
    "QGameWin": qgame_win,
    "QGameTimeout": qgame_timeout,
    "QGameX": qgame_x,
    "QGameO": qgame_o,
    "QConnectBlack": qconnect_black,
    "QConnectWhite": qconnect_white,
    "QGift": qgift,
    "QAlarm": qalarm,
    "QBell": qbell,
    "QSleep": qsleep,
    "QBook": qbook,
    "QThinking": qthinking,
    "QStreakFire": qstreak_fire,
    "QTarget": qtarget,
    "QCards": qcards,
    "QConfetti": qconfetti,
    "QChessWhitePawn": qchess_white_pawn,
    "QChessWhiteKnight": qchess_white_knight,
    "QChessWhiteBishop": qchess_white_bishop,
    "QChessWhiteRook": qchess_white_rook,
    "QChessWhiteQueen": qchess_white_queen,
    "QChessWhiteKing": qchess_white_king,
    "QChessBlackPawn": qchess_black_pawn,
    "QChessBlackKnight": qchess_black_knight,
    "QChessBlackBishop": qchess_black_bishop,
    "QChessBlackRook": qchess_black_rook,
    "QChessBlackQueen": qchess_black_queen,
    "QChessBlackKing": qchess_black_king,
    "QC4One": qc4_1,
    "QC4Two": qc4_2,
    "QC4Three": qc4_3,
    "QC4Four": qc4_4,
    "QC4Five": qc4_5,
    "QC4Six": qc4_6,
    "QC4Seven": qc4_7,
    "QSlotStar": qslot_star,
    "QSlotDiamond": qslot_diamond,
    "QSlotCrown": qslot_crown,
    "QSlotJackpot": qslot_jackpot,
    "QScratchMark": qscratch_mark,
    "QMineTile": qmine_tile,
    "QMineCursor": qmine_cursor,
    "QC4EmptyLight": qc4_empty_light,
    "QC4EmptyDark": qc4_empty_dark,
    "QChessDark": qchess_dark,
    "QCardSpade": qcard_spade,
    "QCardHeart": qcard_heart,
    "QCardDiamond": qcard_diamond,
    "QCardClub": qcard_club,
    "QRouletteRed": qroulette_red,
    "QRouletteBlack": qroulette_black,
    "QRouletteGreen": qroulette_green,
    "QWheelRed": qwheel_red,
    "QWheelBlue": qwheel_blue,
    "QWheelGreen": qwheel_green,
    "QWheelOrange": qwheel_orange,
    "QWheelPurple": qwheel_purple,
    "QWheelGold": qwheel_gold,
    "QWheelBlank": qwheel_blank,
    "QWheelPink": qwheel_pink,
    "QPollOne": qpoll_1,
    "QPollTwo": qpoll_2,
    "QPollThree": qpoll_3,
    "QPollFour": qpoll_4,
    "QPollFive": qpoll_5,
    "QPollSix": qpoll_6,
    "QPollSeven": qpoll_7,
    "QPollEight": qpoll_8,
    "QPollNine": qpoll_9,
    "QPollTen": qpoll_10,
    "QChessFileA": qchess_file_a,
    "QChessFileB": qchess_file_b,
    "QChessFileC": qchess_file_c,
    "QChessFileD": qchess_file_d,
    "QChessFileE": qchess_file_e,
    "QChessFileF": qchess_file_f,
    "QChessFileG": qchess_file_g,
    "QChessFileH": qchess_file_h,
    "QTower": qtower,
    "QTowerDoor": qtower_door,
    "QTowerTrap": qtower_trap,
    "QVault": qvault,
    "QVaultDial": qvault_dial,
    "QMemory": qmemory,
    "QMemoryTile": qmemory_tile,
    "QHeist": qheist,
    "QDiceDuel": qdice_duel,
    "QCases": qcases,
    "QPlinko": qplinko,
    "QLuckyNumber": qlucky_number,
    "QJackpotSpin": qjackpot_spin,
    "QDoubleNothing": qdouble_nothing,
    "QGameStats": qgame_stats,
    "QBadge": qbadge,
    "QAudit": qaudit,
    "QLimits": qlimits,
    "QHistory": qhistory,
    "QAchievementLocked": qachievement_locked,
    "QAchievementUnlocked": qachievement_unlocked,
    "QRiskLow": qrisk_low,
    "QRiskMedium": qrisk_medium,
    "QRiskHigh": qrisk_high,
    "QRiskExtreme": qrisk_extreme,
    "QPerf": qperf,
    "QFilter": qfilter,
    "QDungeon": qdungeon,
    "QDungeonHeart": qdungeon_heart,
    "QDungeonKey": qdungeon_key,
    "QDungeonRelic": qdungeon_relic,
    "QDungeonMonster": qdungeon_monster,
    "QBank": qbank,
    "QStreakFreeze": qstreak_freeze,
    "QRob": qrob,
    "QTutorial": qtutorial,
    "QRecommend": qrecommend,
    "QSeasonPass": qseason_pass,
    "QRefresh": qrefresh,
    "QQueue": qqueue,
    "QRecovery": qrecovery,
    "QErrorLog": qerror_log,
    "QDatabase": qdatabase,
    "QSnipe": qsnipe,
}

STATIC_CATEGORIES = {
    "general": {
        "QAccept", "QDenied", "QReject", "QSuccess", "QWarning", "QTimer", "QTimeout",
        "QBell", "QBook", "QGift", "QConfetti", "QThinking", "QTarget", "QBirthday",
        "QBirthdayCake", "QBirthdayBalloons", "QAlarm", "QSleep", "QImage", "QAttachment", "QActivity",
        "QTutorial", "QRecommend", "QRefresh", "QQueue", "QRecovery", "QErrorLog", "QDatabase", "QSnipe",
    },
    "quewo": {
        "QoinBag", "QoinChest", "QoinTransfer", "QXP", "QLevelUp", "QQuest", "QShop",
        "QTicket", "QTicketMinus", "QLuckyCharm", "QXPTonic", "QQuesoMagnet", "QDailySpice",
        "QStreakPolish", "QGoldBadge", "QHighRoller", "QVelvetFrame", "QTicketCharm",
        "QCooldownClock", "QRoyalCrown", "QStreakFire", "QFortuneVial",
        "QAudit", "QLimits", "QPerf", "QFilter", "QBank", "QStreakFreeze", "QSeasonPass",
    },
    "moderation": {
        "QHammer", "QTrash", "QEdit", "QLock", "QBroom", "QReaction", "QUserEdit",
        "QRoles", "QVoice", "QPermissions", "QRob",
    },
    "games/common": {
        "QGameWin", "QGameTimeout", "QGameX", "QGameO", "QCards",
        "QDoubleNothing", "QGameStats", "QBadge", "QHistory", "QAchievementLocked",
        "QAchievementUnlocked", "QRiskLow", "QRiskMedium", "QRiskHigh", "QRiskExtreme",
    },
    "games/coinflip": {
        "QFlip",
    },
    "games/connect4": {
        "QConnectBlack", "QConnectWhite", "QC4One", "QC4Two", "QC4Three", "QC4Four",
        "QC4Five", "QC4Six", "QC4Seven", "QC4EmptyLight", "QC4EmptyDark",
    },
    "games/chess": {
        "QChessWhitePawn", "QChessWhiteKnight", "QChessWhiteBishop", "QChessWhiteRook",
        "QChessWhiteQueen", "QChessWhiteKing", "QChessBlackPawn", "QChessBlackKnight",
        "QChessBlackBishop", "QChessBlackRook", "QChessBlackQueen", "QChessBlackKing",
        "QChessDark", "QChessFileA", "QChessFileB", "QChessFileC", "QChessFileD",
        "QChessFileE", "QChessFileF", "QChessFileG", "QChessFileH",
    },
    "games/slots": {
        "QSlots", "QSlotStar", "QSlotDiamond", "QSlotCrown", "QSlotJackpot",
    },
    "games/scratch": {
        "QScratchMark",
    },
    "games/minesweeper": {
        "QMine", "QMineTile", "QMineCursor",
    },
    "games/blackjack": {
        "QCardSpade", "QCardHeart", "QCardDiamond", "QCardClub",
    },
    "games/roulette": {
        "QRouletteRed", "QRouletteBlack", "QRouletteGreen",
    },
    "games/wheel": {
        "QWheel", "QWheelRed", "QWheelBlue", "QWheelGreen", "QWheelOrange",
        "QWheelPurple", "QWheelGold", "QWheelBlank", "QWheelPink",
    },
    "games/tower": {
        "QTower", "QTowerDoor", "QTowerTrap",
    },
    "games/vault": {
        "QVault", "QVaultDial",
    },
    "games/memory": {
        "QMemory", "QMemoryTile",
    },
    "games/heist": {
        "QHeist",
    },
    "games/dice": {
        "QDiceDuel",
    },
    "games/cases": {
        "QCases",
    },
    "games/plinko": {
        "QPlinko",
    },
    "games/lucky_number": {
        "QLuckyNumber",
    },
    "games/jackpot": {
        "QJackpotSpin",
    },
    "games/dungeon": {
        "QDungeon", "QDungeonHeart", "QDungeonKey", "QDungeonRelic", "QDungeonMonster",
    },
    "polls": {
        "QPoll", "QPollOne", "QPollTwo", "QPollThree", "QPollFour", "QPollFive",
        "QPollSix", "QPollSeven", "QPollEight", "QPollNine", "QPollTen",
    },
}


def static_category(name):
    for category, names in STATIC_CATEGORIES.items():
        if name in names:
            return category
    return "general"


def make_static():
    for name, fn in STATIC.items():
        canvas = fn()
        category_dir = os.path.join(UPLOAD_OUT, "png", static_category(name))
        os.makedirs(category_dir, exist_ok=True)
        save_png(canvas, os.path.join(category_dir, f"{name}.png"))


def make_frames(frame_root):
    os.makedirs(frame_root, exist_ok=True)
    animations = {
        "QFlipSpin": lambda i, n: qflip(i / n * math.tau * 2),
        "QWheelSpin": lambda i, n: qwheel(i / n * math.tau),
        "QTimerTick": lambda i, n: qtimer(-math.pi / 2 + i / n * math.tau),
        "QLevelPulse": lambda i, n: qlevel_up(),
        "QMineSpark": lambda i, n: qmine(),
        "QJackpotSpin": lambda i, n: qjackpot_spin(i),
        "QDoubleNothing": lambda i, n: qdouble_nothing(i / n * math.tau * 2),
        "QPlinkoDrop": lambda i, n: qplinko((i * 5) // n),
    }
    for name, fn in animations.items():
        folder = os.path.join(frame_root, name)
        os.makedirs(folder, exist_ok=True)
        for i in range(16):
            canvas = fn(i, 16)
            if name == "QLevelPulse":
                pulse = 4 * math.sin(i / 16 * math.tau)
                canvas.circle(64, 64, 48 + pulse, TRANSPARENT, outline=(128, 221, 255, 80), width=2)
            if name == "QMineSpark" and i % 4 in (0, 1):
                canvas.sparkle(96, 21, 11)
            save_png(canvas, os.path.join(folder, f"{i:03d}.png"))
    return list(animations)


def make_gifs_from_frames(frame_root, names):
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return
    gif_out = os.path.join(UPLOAD_OUT, "animated")
    os.makedirs(gif_out, exist_ok=True)
    for name in names:
        frame_pattern = os.path.join(frame_root, name, "%03d.png")
        out_path = os.path.join(gif_out, f"{name}.gif")
        subprocess.run(
            [
                ffmpeg,
                "-y",
                "-framerate",
                "12",
                "-i",
                frame_pattern,
                "-loop",
                "0",
                "-gifflags",
                "+transdiff",
                out_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


if __name__ == "__main__":
    make_static()
    with tempfile.TemporaryDirectory(prefix="proque_emoji_frames_") as frame_root:
        names = make_frames(frame_root)
        make_gifs_from_frames(frame_root, names)
    print(UPLOAD_OUT)
