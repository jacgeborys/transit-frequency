"""
Save a view from a .blend to 3d/views/<name>.json, so it survives rebuilding the scene
(scene coordinates stay local to the export centre). Runs inside Blender:

    blender --background 3d/scenes/warsaw_center/scene.blend --python 3d/save_view.py -- --name center
    blender --background <file>.blend --python 3d/save_view.py -- --name mokotow --from-viewport

Default: the scene camera. --from-viewport: the main 3D view as it was when the file was saved
(handy when the camera wasn't aligned to the view). render.py --view <name> uses the file.
"""
import argparse
import json
import sys
from pathlib import Path

import bpy

HERE = Path(__file__).resolve().parent


def main():
    argv = sys.argv[sys.argv.index('--') + 1:] if '--' in sys.argv else []
    ap = argparse.ArgumentParser()
    ap.add_argument('--name', required=True)
    ap.add_argument('--from-viewport', action='store_true')
    args = ap.parse_args(argv)

    cam = bpy.context.scene.camera
    if args.from_viewport:
        areas = [a for s in bpy.data.screens if s.name == 'Layout' for a in s.areas if a.type == 'VIEW_3D']
        area = max(areas, key=lambda a: a.width * a.height)
        m = area.spaces[0].region_3d.view_matrix.inverted()
        loc, rot = m.translation, m.to_euler()
        lens = area.spaces[0].lens / 2  # the viewport shows twice the camera's field at equal lens
    else:
        loc, rot, lens = cam.location, cam.rotation_euler, cam.data.lens
    view = {'location': [round(v, 2) for v in loc], 'rotation_euler': [round(v, 5) for v in rot],
            'lens': lens, 'source': bpy.data.filepath,
            'from': 'viewport' if args.from_viewport else 'camera'}
    out = HERE / 'views' / f'{args.name}.json'
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(view, indent=1))
    print(f"saved view {out}: {view}")


if __name__ == '__main__':
    main()
