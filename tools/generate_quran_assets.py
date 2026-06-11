#!/usr/bin/env python3
import os

from generate_q_emojis import (
    BLUE,
    BLUE_DARK,
    BLUE_MID,
    CYAN,
    GOLD,
    GREEN,
    ICE,
    NAVY,
    SILVER,
    TRANSPARENT,
    Canvas,
    ROOT,
    coin,
    save_png,
)


OUT = os.path.join(ROOT, "assets", "emojis", "upload", "png", "quran")


def book_base(c):
    c.ellipse(64, 102, 43, 14, (1, 14, 30, 110))
    c.rect(25, 31, 40, 66, BLUE, outline=NAVY, width=4)
    c.rect(63, 31, 40, 66, BLUE_MID, outline=NAVY, width=4)
    c.line(64, 34, 64, 96, ICE, 3)
    c.line(35, 46, 55, 46, CYAN, 3)
    c.line(75, 46, 94, 46, CYAN, 3)
    c.line(35, 59, 55, 59, ICE, 2)
    c.line(75, 59, 94, 59, ICE, 2)
    c.line(35, 72, 52, 72, SILVER, 2)
    c.line(75, 72, 91, 72, SILVER, 2)


def qquran():
    c = Canvas()
    c.circle(64, 66, 53, TRANSPARENT, outline=(128, 221, 255, 90), width=3)
    book_base(c)
    c.poly([(64, 63), (72, 73), (84, 76), (74, 84), (70, 96), (64, 86), (54, 96), (58, 84), (48, 76), (60, 73)], GOLD, outline=BLUE_DARK, width=2)
    coin(c, 99, 28, 11, False)
    coin(c, 28, 102, 9, False)
    c.sparkle(101, 73, 6)
    return c


def qreciter():
    c = Canvas()
    book_base(c)
    c.ellipse(64, 46, 22, 28, ICE, outline=NAVY, width=4)
    c.rect(55, 31, 18, 33, BLUE, outline=NAVY, width=3)
    c.line(64, 74, 64, 102, GREEN, 5)
    c.ellipse(64, 76, 37, 29, TRANSPARENT, outline=GREEN, width=5)
    coin(c, 99, 101, 10, False)
    c.sparkle(31, 31, 6)
    return c


def qsurah():
    c = Canvas()
    c.rect(27, 19, 70, 90, ICE, outline=NAVY, width=5)
    c.rect(33, 26, 58, 14, BLUE, outline=BLUE_DARK, width=1.5)
    for y in (53, 66, 79):
        c.line(40, y, 82, y, BLUE_MID, 4)
    c.poly([(64, 88), (72, 94), (70, 105), (64, 99), (56, 105), (58, 94)], GOLD, outline=BLUE_DARK, width=2)
    coin(c, 99, 25, 10, False)
    c.sparkle(31, 105, 5)
    return c


def qquran_queue():
    c = Canvas()
    for i, y in enumerate((30, 52, 74)):
        x = 24 + i * 6
        c.rect(x, y, 72, 16, BLUE_MID if i != 1 else BLUE, outline=NAVY, width=3)
        c.circle(x + 11, y + 8, 4, GOLD, outline=BLUE_DARK, width=1)
        c.line(x + 24, y + 8, x + 57, y + 8, ICE, 2)
    c.poly([(64, 105), (82, 94), (82, 116)], GREEN, outline=NAVY, width=3)
    coin(c, 31, 103, 10, False)
    return c


def save_static():
    os.makedirs(OUT, exist_ok=True)
    for name, canvas in {
        "QQuran": qquran(),
        "QReciter": qreciter(),
        "QSurah": qsurah(),
        "QQuranQueue": qquran_queue(),
    }.items():
        save_png(canvas, os.path.join(OUT, f"{name}.png"))


if __name__ == "__main__":
    save_static()
    print(OUT)
