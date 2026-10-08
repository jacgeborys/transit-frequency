"""Poster header font samples on the dark page: png/previews/header_fonts.png"""
import os
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager

from cities import PROJECT_DIR

DIRS = [Path(os.environ['LOCALAPPDATA']) / 'Microsoft' / 'Windows' / 'Fonts', Path('C:/Windows/Fonts')]


def fp(name, size):
    for d in DIRS:
        if (d / name).exists():
            return font_manager.FontProperties(fname=str(d / name), size=size)
    raise FileNotFoundError(name)


# (label, city font, title font, subtitle font)
SAMPLES = [
    ('A  Inter Black (current)', 'Inter_28pt-Black.ttf', 'Inter_24pt-SemiBold.ttf', 'Inter_18pt-Regular.ttf'),
    ('B  Inter Bold', 'Inter_28pt-Bold.ttf', 'Inter_24pt-Medium.ttf', 'Inter_18pt-Regular.ttf'),
    ('C  Inter SemiBold', 'Inter_28pt-SemiBold.ttf', 'Inter_24pt-Medium.ttf', 'Inter_18pt-Light.ttf'),
    ('D  Inter Medium', 'Inter_28pt-Medium.ttf', 'Inter_24pt-Regular.ttf', 'Inter_18pt-Light.ttf'),
    ('E  Montserrat SemiBold', 'Montserrat-SemiBold.ttf', 'Montserrat-Medium.ttf', 'Montserrat-Light.ttf'),
    ('F  Montserrat Medium', 'Montserrat-Medium.ttf', 'Montserrat-Regular.ttf', 'Montserrat-Light.ttf'),
    ('G  Bahnschrift (DIN-like)', 'bahnschrift.ttf', 'bahnschrift.ttf', 'bahnschrift.ttf'),
]
PAGE, INK, DIM = (14 / 255, 14 / 255, 18 / 255), (225 / 255,) * 2 + (232 / 255,), (0.62, 0.62, 0.66)

fig = plt.figure(figsize=(16, 2.0 * len(SAMPLES)), dpi=110)
fig.patch.set_facecolor(PAGE)
for i, (label, fc, ft, fs) in enumerate(SAMPLES):
    y = 1 - (i + 0.5) / len(SAMPLES)
    fig.text(0.01, y + 0.045, label, color=(0.5, 0.5, 0.55), fontsize=10, va='bottom')
    t = fig.text(0.02, y - 0.03, 'WARSZAWA', fontproperties=fp(fc, 58), color=INK, va='baseline')
    w = t.get_window_extent(fig.canvas.get_renderer()).width / fig.bbox.width
    fig.text(0.04 + w, y + 0.012, 'Ile odjazdów masz w zasięgu 5 minut pieszo?',
             fontproperties=fp(ft, 24), color=INK, va='baseline')
    fig.text(0.04 + w, y - 0.03, 'dzień powszedni · środa 07.10.2026',
             fontproperties=fp(fs, 15), color=DIM, va='baseline')
out = PROJECT_DIR / 'png' / 'previews' / 'header_fonts.png'
fig.savefig(out, facecolor=PAGE)
print(f'Saved {out}')
