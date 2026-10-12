"""
Build a Blender scene from 3d/scenes/<name>/scene.json (made by export_scene.py).
Runs inside Blender:

    blender --background --python 3d/build_blend.py -- --name warsaw_center

Writes 3d/scenes/<name>/scene.blend: buildings as one mesh with a per-face colour attribute
'col' (poster class colours, linear) and 'glow' (emission strength by class), flat ground
layers (base, green, forest, water, roads, railways) slightly stacked in z, a ground plane out
to the horizon, distance haze in the sky colour, a low sun,
a dark sky and an oblique camera looking at the centre. Open it in Blender to look around;
render with 3d/render.py.
"""
import argparse
import json
import math
import sys
from pathlib import Path

import bpy
from mathutils import Vector
from mathutils.geometry import tessellate_polygon

HERE = Path(__file__).resolve().parent
GROUND_Z = {'base': 0.0, 'green': 0.05, 'forest': 0.06, 'water': 0.08, 'roads': 0.10, 'railways': 0.12}
GROUND_COLOUR = {'base': 'background', 'green': 'green', 'forest': 'forest', 'water': 'water',
                 'roads': 'roads', 'railways': 'railways'}
BASE_RGB = (12, 12, 15)  # ground plane: a touch above the poster's pure black so it reads in 3D
SKY = (0.004, 0.005, 0.009)  # linear; world colour and haze colour


def srgb_to_linear(c):
    c = c / 255
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def lin(rgb):
    return tuple(srgb_to_linear(v) for v in rgb)


def signed_area(ring):
    return 0.5 * sum(x0 * y1 - x1 * y0 for (x0, y0), (x1, y1) in zip(ring, ring[1:] + ring[:1]))


def add_prism(verts, faces, face_cls, poly, z0, z1, c, walls=True):
    """Extrude one polygon [exterior, *holes]: top cap, walls, bottom cap if lifted."""
    rings = []
    for k, r in enumerate(poly):
        if len(r) < 3:
            continue
        ccw = signed_area(r) > 0
        if (k == 0) != ccw:            # exterior CCW, holes CW -> walls face outwards
            r = r[::-1]
        rings.append(r)
    if not rings or len(rings[0]) < 3:
        return
    flat = [p for r in rings for p in r]
    tris = tessellate_polygon([[Vector((x, y, 0)) for x, y in r] for r in rings])
    for z, up in ((z1, True), (z0, False)) if (walls and z0 > 0.05) else ((z1, True),):
        base = len(verts)
        verts.extend((x, y, z) for x, y in flat)
        for a, b, d in tris:
            (ax, ay), (bx, by), (dx, dy) = flat[a], flat[b], flat[d]
            ccw = (bx - ax) * (dy - ay) - (by - ay) * (dx - ax) > 0
            faces.append((base + a, base + b, base + d) if ccw == up else (base + a, base + d, base + b))
            face_cls.append(c)
    if not walls:
        return
    for r in rings:
        n = len(r)
        base = len(verts)
        verts.extend((x, y, z0) for x, y in r)
        verts.extend((x, y, z1) for x, y in r)
        for i in range(n):
            j = (i + 1) % n
            faces.append((base + i, base + j, base + n + j, base + n + i))
            face_cls.append(c)


def make_mesh(name, verts, faces):
    me = bpy.data.meshes.new(name)
    me.from_pydata(verts, [], faces)
    me.validate(clean_customdata=False)
    me.update()
    ob = bpy.data.objects.new(name, me)
    bpy.context.scene.collection.objects.link(ob)
    return ob


def flat_material(name, rgb, roughness=0.9):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    p = m.node_tree.nodes['Principled BSDF']
    p.inputs['Base Color'].default_value = (*lin(rgb), 1)
    p.inputs['Roughness'].default_value = roughness
    return m


def building_material(glow_scale):
    """Base colour + emission from the per-face attributes 'col' and 'glow'."""
    m = bpy.data.materials.new('buildings')
    m.use_nodes = True
    nt = m.node_tree
    p = nt.nodes['Principled BSDF']
    col = nt.nodes.new('ShaderNodeAttribute')
    col.attribute_name = 'col'
    glow = nt.nodes.new('ShaderNodeAttribute')
    glow.attribute_name = 'glow'
    mul = nt.nodes.new('ShaderNodeMath')
    mul.operation = 'MULTIPLY'
    mul.inputs[1].default_value = glow_scale
    nt.links.new(col.outputs['Color'], p.inputs['Base Color'])
    nt.links.new(col.outputs['Color'], p.inputs['Emission Color'])
    nt.links.new(glow.outputs['Fac'], mul.inputs[0])
    nt.links.new(mul.outputs[0], p.inputs['Emission Strength'])
    p.inputs['Roughness'].default_value = 0.7
    return m


def add_haze(m, start, scale, colour):
    """Fade the material towards `colour` with distance from the camera: a cheap fog that
    works the same in Eevee and Cycles (no volumes). fac = 1 - exp(-max(0, d - start) / scale)."""
    nt = m.node_tree
    out = next(n for n in nt.nodes if n.type == 'OUTPUT_MATERIAL')
    surf = out.inputs['Surface'].links[0].from_socket
    cam = nt.nodes.new('ShaderNodeCameraData')
    sub = nt.nodes.new('ShaderNodeMath')
    sub.operation, sub.use_clamp = 'SUBTRACT', False
    sub.inputs[1].default_value = start
    mx = nt.nodes.new('ShaderNodeMath')
    mx.operation = 'MAXIMUM'
    mx.inputs[1].default_value = 0.0
    div = nt.nodes.new('ShaderNodeMath')
    div.operation = 'DIVIDE'
    div.inputs[1].default_value = -scale
    ex = nt.nodes.new('ShaderNodeMath')
    ex.operation = 'EXPONENT'
    inv = nt.nodes.new('ShaderNodeMath')
    inv.operation, inv.use_clamp = 'SUBTRACT', True
    inv.inputs[0].default_value = 1.0
    em = nt.nodes.new('ShaderNodeEmission')
    em.inputs['Color'].default_value = (*colour, 1)
    mix = nt.nodes.new('ShaderNodeMixShader')
    nt.links.new(cam.outputs['View Distance'], sub.inputs[0])
    nt.links.new(sub.outputs[0], mx.inputs[0])
    nt.links.new(mx.outputs[0], div.inputs[0])
    nt.links.new(div.outputs[0], ex.inputs[0])
    nt.links.new(ex.outputs[0], inv.inputs[1])
    nt.links.new(inv.outputs[0], mix.inputs['Fac'])
    nt.links.new(surf, mix.inputs[1])
    nt.links.new(em.outputs[0], mix.inputs[2])
    nt.links.new(mix.outputs[0], out.inputs['Surface'])


def look_at(ob, target):
    d = Vector(target) - ob.location
    ob.rotation_euler = d.to_track_quat('-Z', 'Y').to_euler()


def main():
    argv = sys.argv[sys.argv.index('--') + 1:] if '--' in sys.argv else []
    ap = argparse.ArgumentParser()
    ap.add_argument('--name', required=True)
    ap.add_argument('--glow', type=float, default=0.3, help='Emission strength multiplier (0 = none)')
    ap.add_argument('--sun-elevation', type=float, default=25, help='degrees')
    ap.add_argument('--sun-azimuth', type=float, default=225, help='degrees from north, clockwise (225 = SW)')
    ap.add_argument('--cam-azimuth', type=float, default=200, help='camera direction from centre, deg from north')
    ap.add_argument('--cam-distance', type=float, default=3200, help='m, horizontal')
    ap.add_argument('--cam-height', type=float, default=1800, help='m')
    ap.add_argument('--lens', type=float, default=40, help='mm')
    ap.add_argument('--haze-start', type=float, default=2500, help='m from the camera; no haze closer')
    ap.add_argument('--haze-scale', type=float, default=5000, help='m; 63 %% hazed this far past the start')
    ap.add_argument('--horizon', type=float, default=40000, help='Ground plane size beyond the data, m (0 = none)')
    args = ap.parse_args(argv)

    folder = HERE / 'scenes' / args.name
    scene_data = json.loads((folder / 'scene.json').read_text())
    palette = scene_data['palette']
    colours = scene_data['colours']

    bpy.ops.wm.read_factory_settings(use_empty=True)
    sc = bpy.context.scene

    # Buildings: one mesh, colour per face
    verts, faces, fcls = [], [], []
    for b in scene_data['buildings']:
        add_prism(verts, faces, fcls, b['rings'], b['z0'], b['z1'], b['c'])
    ob = make_mesh('buildings', verts, faces)
    me = ob.data
    n_cls = len(palette) - 1
    cols = [(*lin(palette[c]), 1.0) for c in range(len(palette))]
    # Glow rises with the class: none for uncovered, faint for the lowest, full at the top
    glows = [0.0] + [0.05 + 0.95 * (k / max(1, n_cls - 1)) ** 2 for k in range(n_cls)]
    a = me.color_attributes.new('col', 'FLOAT_COLOR', 'FACE')
    a.data.foreach_set('color', [v for c in fcls for v in cols[c]])
    g = me.attributes.new('glow', 'FLOAT', 'FACE')
    g.data.foreach_set('value', [glows[c] for c in fcls])
    k = me.attributes.new('cls', 'INT', 'FACE')
    k.data.foreach_set('value', fcls)
    me.materials.append(building_material(args.glow))
    print(f"buildings: {len(scene_data['buildings']):,} polygons, {len(verts):,} vertices, {len(faces):,} faces")

    # Ground layers
    for layer, polys in scene_data['ground'].items():
        if not polys:
            continue
        v, f, c = [], [], []
        for poly in polys:
            add_prism(v, f, c, poly, 0.0, GROUND_Z.get(layer, 0.02), 0, walls=False)
        gob = make_mesh(f'ground_{layer}', v, f)
        rgb = BASE_RGB if layer == 'base' else colours.get(GROUND_COLOUR.get(layer, layer), (40, 40, 40))
        gob.data.materials.append(flat_material(f'ground_{layer}', rgb,
                                                roughness=0.25 if layer == 'water' else 0.9))
    print('ground: ' + ', '.join(f'{k} {len(v)}' for k, v in scene_data['ground'].items()))
    if args.horizon:  # ground plane to the horizon, just below the data's base, so there's no edge
        h = args.horizon / 2
        hob = make_mesh('ground_horizon', [(-h, -h, -0.05), (h, -h, -0.05), (h, h, -0.05), (-h, h, -0.05)],
                        [(0, 1, 2, 3)])
        hob.data.materials.append(flat_material('ground_horizon', BASE_RGB))

    # Distance haze in the sky colour on every material
    for m in bpy.data.materials:
        add_haze(m, args.haze_start, args.haze_scale, SKY)

    # World: dark blue-grey sky, so the shadow side isn't pure black
    world = bpy.data.worlds.new('world')
    world.use_nodes = True
    world.node_tree.nodes['Background'].inputs['Color'].default_value = (*SKY, 1)
    world.node_tree.nodes['Background'].inputs['Strength'].default_value = 1.0
    sc.world = world

    # Sun
    sun = bpy.data.lights.new('sun', 'SUN')
    sun.energy = 3.0
    sun.angle = math.radians(1.5)
    sun.color = (1.0, 0.92, 0.82)
    sob = bpy.data.objects.new('sun', sun)
    sc.collection.objects.link(sob)
    el, az = math.radians(args.sun_elevation), math.radians(args.sun_azimuth)
    sob.location = (math.sin(az) * 1000, math.cos(az) * 1000, 1000 * math.tan(el))
    look_at(sob, (0, 0, 0))

    # Camera
    cam = bpy.data.cameras.new('camera')
    cam.lens = args.lens
    cam.clip_start, cam.clip_end = 5, 60000
    cob = bpy.data.objects.new('camera', cam)
    sc.collection.objects.link(cob)
    caz = math.radians(args.cam_azimuth)
    cob.location = (math.sin(caz) * args.cam_distance, math.cos(caz) * args.cam_distance, args.cam_height)
    look_at(cob, (0, 0, 0))
    sc.camera = cob

    # Colour management: keep the poster's sRGB colours as they are (no filmic curve)
    sc.view_settings.view_transform = 'Standard'
    sc.view_settings.look = 'None'
    sc.render.resolution_x, sc.render.resolution_y = 1600, 1000
    sc.unit_settings.system = 'METRIC'

    # Viewport (when the user opens the file): see the whole city, with the scene's own lights
    for screen in bpy.data.screens:
        for area in screen.areas:
            if area.type == 'VIEW_3D':
                for space in area.spaces:
                    if space.type == 'VIEW_3D':
                        space.clip_start, space.clip_end = 1, 60000
                        space.shading.type = 'MATERIAL'
                        space.shading.use_scene_lights = True
                        space.shading.use_scene_world = True
    out = folder / 'scene.blend'
    bpy.ops.wm.save_as_mainfile(filepath=str(out))
    print(f"saved {out}")


if __name__ == '__main__':
    main()
