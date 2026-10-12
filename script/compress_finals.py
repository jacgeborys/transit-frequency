"""
Compress the 300 dpi finals in png/ to JPEGs under a size limit (default 20 MB) for sharing.

Writes png/compressed/<name>.jpg; the PNGs are not touched. Per file: the highest JPEG quality
(95 -> 90, 4:4:4 chroma) that fits at full resolution; if none does, downscale step by step at
quality 92. A 256-colour PNG was tried first and merged the low legend blues, so JPEG it is.

Usage:
    python compress_finals.py                 # all finals
    python compress_finals.py --city paris    # names containing 'paris'
"""
import argparse
import io
from pathlib import Path

from PIL import Image

from cities import PROJECT_DIR

Image.MAX_IMAGE_PIXELS = None


def encode(im, quality):
    buf = io.BytesIO()
    im.save(buf, 'JPEG', quality=quality, subsampling=0, optimize=True, dpi=im.info.get('dpi', (300, 300)))
    return buf.getvalue()


def compress(src, dst, limit):
    im = Image.open(src)
    dpi = im.info.get('dpi', (300, 300))
    im = im.convert('RGB')
    im.info['dpi'] = dpi
    for q in (95, 92, 90):
        data = encode(im, q)
        if len(data) <= limit:
            return data, q, 1.0, im.size
    scale = 1.0
    while True:
        scale *= 0.9
        size = (round(im.width * scale), round(im.height * scale))
        small = im.resize(size, Image.LANCZOS)
        small.info['dpi'] = tuple(round(d * scale) for d in dpi)
        data = encode(small, 92)
        if len(data) <= limit:
            return data, 92, scale, size


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--city', default='', help='Only files whose name contains this')
    ap.add_argument('--max-mb', type=float, default=20)
    args = ap.parse_args()

    png_dir = PROJECT_DIR / 'png'
    out_dir = png_dir / 'compressed'
    out_dir.mkdir(exist_ok=True)
    limit = int(args.max_mb * 1024 * 1024) - 64 * 1024  # a little margin under the limit
    for src in sorted(png_dir.glob('*_hq.png')):
        if args.city not in src.name:
            continue
        dst = out_dir / (src.stem + '.jpg')
        data, q, scale, size = compress(src, dst, limit)
        dst.write_bytes(data)
        print(f"{src.name}: {src.stat().st_size / 2**20:.1f} MB -> {len(data) / 2**20:.1f} MB "
              f"(q{q}, {size[0]}x{size[1]}{'' if scale == 1 else f', scaled {scale:.2f}'})", flush=True)


if __name__ == '__main__':
    main()
