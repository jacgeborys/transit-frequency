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


def add_far_blur(sc, start, end, size_frac):
    """Blur only the distance: mix the image with a blurred copy by depth (start..end m from the
    camera). A lens can't do this: blurring the horizon also blurs everything between."""
    sc.view_layers[0].use_pass_z = True
    tree = sc.compositing_node_group
    if tree is None:
        tree = bpy.data.node_groups.new('compositor', 'CompositorNodeTree')
        tree.interface.new_socket(name='Image', in_out='OUTPUT', socket_type='NodeSocketColor')
        rl = tree.nodes.new('CompositorNodeRLayers')
        out = tree.nodes.new('NodeGroupOutput')
        tree.links.new(rl.outputs['Image'], out.inputs['Image'])
        sc.compositing_node_group = tree
        sc.render.use_compositing = True
    out = next(n for n in tree.nodes if n.type == 'GROUP_OUTPUT')
    rl = next(n for n in tree.nodes if n.type == 'R_LAYERS')
    src = out.inputs['Image'].links[0].from_socket
    blur = tree.nodes.new('CompositorNodeBlur')
    px = size_frac * sc.render.resolution_x
    blur.inputs['Size'].default_value[0] = blur.inputs['Size'].default_value[1] = px
    rng = tree.nodes.new('ShaderNodeMapRange')
    rng.inputs['From Min'].default_value, rng.inputs['From Max'].default_value = start, end
    mix = tree.nodes.new('ShaderNodeMix')
    mix.data_type = 'RGBA'
    tree.links.new(src, blur.inputs['Image'])
    tree.links.new(rl.outputs['Depth'], rng.inputs['Value'])
    tree.links.new(rng.outputs['Result'], mix.inputs['Factor'])
    tree.links.new(src, mix.inputs[6])            # A (colour)
    tree.links.new(blur.outputs['Image'], mix.inputs[7])  # B (colour)
    tree.links.new(mix.outputs[2], out.inputs['Image'])
    print(f"far blur: {start:g}-{end:g} m, {px:.0f} px")


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
    ap.add_argument('--far-blur', help="start,end,size: blur only beyond start..end m from the camera, size as a "
                    "fraction of the width (e.g. 9000,18000,0.003). Default: the view's 'far_blur', if any")
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
    far = [float(x) for x in args.far_blur.split(',')] if args.far_blur else (v.get('far_blur') if args.view else None)
    if far:
        add_far_blur(sc, *far)
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
