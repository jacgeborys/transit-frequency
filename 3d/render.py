"""
Render the camera of 3d/scenes/<name>/scene.blend to PNG. Runs inside Blender:

    blender --background 3d/scenes/warsaw_center/scene.blend --python 3d/render.py -- --out 3d/renders/warsaw.png
    ... -- --view mokotow --out 3d/renders/warsaw_mokotow.png      (a view saved by save_view.py)
    ... -- --engine cycles --samples 128 --width 3200 --height 2000 --out 3d/renders/warsaw.png

Cycles on the GPU (CUDA, GTX 1660 Ti) by default: 1600x1000 in ~25 s, 3200x2000 at 128 samples
~2 min; as fast as Eevee here and with soft shadows/occlusion. --engine eevee still works.
The .blend is not modified (the camera you saved in Blender is used as is).
"""
import argparse
import json
import sys
from pathlib import Path

import bpy


def use_cuda():
    prefs = bpy.context.preferences.addons['cycles'].preferences
    prefs.compute_device_type = 'CUDA'
    prefs.refresh_devices()
    n = 0
    for d in prefs.devices:
        d.use = d.type == 'CUDA'
        n += d.use
    return n


def main():
    argv = sys.argv[sys.argv.index('--') + 1:] if '--' in sys.argv else []
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--engine', choices=['eevee', 'cycles'], default='cycles')  # same speed on the 1660 Ti, nicer
    ap.add_argument('--samples', type=int, default=128)
    ap.add_argument('--width', type=int, default=1600)
    ap.add_argument('--height', type=int, default=1000)
    ap.add_argument('--lens', type=float, help='Override the focal length, mm (smaller = wider)')
    ap.add_argument('--fstop', type=float, help="Depth of field (tilt-shift), focused on the empty 'focus_pkin'; "
                    "city scale needs tiny values: ~0.002-0.01 (smaller = blurrier). Default: the view's 'fstop', if any")
    ap.add_argument('--view', help='Use 3d/views/<name>.json (save_view.py) instead of the saved camera')
    args = ap.parse_args(argv)

    sc = bpy.context.scene
    if args.view:
        v = json.loads((Path(__file__).resolve().parent / 'views' / f'{args.view}.json').read_text())
        sc.camera.location = v['location']
        sc.camera.rotation_euler = v['rotation_euler']
        sc.camera.data.lens = v['lens']
    if args.lens:
        sc.camera.data.lens = args.lens
    fstop = args.fstop if args.fstop is not None else (v.get('fstop') if args.view else None)
    if fstop:
        dof = sc.camera.data.dof
        dof.use_dof = True
        dof.focus_object = dof.focus_object or bpy.data.objects.get('focus_pkin')
        dof.aperture_fstop = fstop
        print(f"tilt-shift: f/{fstop} focused on {dof.focus_object.name if dof.focus_object else dof.focus_distance}")
    sc.render.resolution_x, sc.render.resolution_y = args.width, args.height
    sc.render.resolution_percentage = 100
    if args.engine == 'eevee':
        for ident in ('BLENDER_EEVEE', 'BLENDER_EEVEE_NEXT'):  # name differs between versions
            try:
                sc.render.engine = ident
                break
            except TypeError:
                continue
        sc.eevee.taa_render_samples = args.samples
    else:
        sc.render.engine = 'CYCLES'
        n = use_cuda()
        sc.cycles.device = 'GPU' if n else 'CPU'
        sc.cycles.samples = args.samples
        sc.cycles.use_denoising = True
        print(f"cycles on {'GPU (%d CUDA device(s))' % n if n else 'CPU'}")
    out = Path(args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    sc.render.image_settings.file_format = 'PNG'
    sc.render.filepath = str(out)
    bpy.ops.render.render(write_still=True)
    print(f"rendered {out}")


if __name__ == '__main__':
    main()
