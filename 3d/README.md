# 3d: transit-frequency city in Blender

Subproject of TransitFrequency (started 2026-10-12). Goal: a **3D render of a city's buildings
coloured by transit frequency** (the same values and palette as the dark 2D posters), made with
**Blender driven entirely from Python scripts**. First target: **central Warsaw** (2–3 km around
the Palace of Culture, PKiN 52.2318 N, 21.0060 E). New York (Midtown/Lower Manhattan) later.

## How to start (for the new chat)

Open Claude Code in `D:\QGIS\TransitFrequency` and say something like:
> "Read 3d/README.md and let's start the 3D subproject: Warsaw first."

The user has never used Blender. Keep it script-first; the user only opens the Blender window to
look around and choose a camera angle.

## Prerequisites

- [ ] **Blender 4.x** installed from blender.org (default path `C:\Program Files\Blender Foundation\
      Blender 4.x\blender.exe`; not installed as of 2026-10-12). Blender ships its own Python, so
      use `blender --background --python <script> -- <args>` and don't import geopandas inside it.
- [ ] GPU: **GTX 1660 Ti, 6 GB** → Cycles via **CUDA** (no OptiX hardware RT). Enable it in
      Edit → Preferences → System, or set it in the script (`cycles` addon prefs). Eevee for drafts.
- 15 GB RAM machine, the user often has PyCharm open: crop the scene, never all 330k Warsaw buildings.

## Data already available (don't refetch)

| What | Where | Notes |
|---|---|---|
| Frequency per building | `_data/warsaw/2026_10_04/buildings_gates.gpkg` (EPSG:4326) | 184k buildings, `osm_id`, `max_trips` (departures/day within a 5-min walk; 0 = not covered), `residents_only` (behind fences: value from the residents-only coverage), `area_m2`, `big`. Made by `script/05_*.py`. |
| Footprints + height tags | `D:\QGIS\osm_basemap\warsaw\buildings.gpkg` | 330k rows: `height` only 2 %, **`building_levels` 86 %**, `building_part` 3 %, `min_height`, `roof_shape`, `name`. |
| Clean 3D heights (Warsaw) | `D:\QGIS\ceny_mieszkan\output\warsaw_prices_viz.gpkg`, layer `buildings_3d` (327k, unified simple buildings + `building:part`, every one has `height_m`); `buildings_all` has `height_m` per footprint | From the user's housing-price project; **reuse it for heights** (join on `osm_id`) rather than re-deriving. |
| Ground layers | `D:\QGIS\osm_basemap\warsaw\{water,parks,forests,grass,roads,railways}.gpkg` | For the ground plane (dark, like the poster). |
| Palette + classes | `script/render_qgis_style.py` (`bmy_dark` with the lightness floor and `--palette-extend`), class breaks from the QGIS project via that renderer; style args in `script/render_poster.py` `STYLE` | 3D colours must match the 2D poster. Factor the class→RGB function out rather than copying numbers. |

Prior art by the user: `D:\QGIS\ceny_mieszkan\make_3d.py` = central Warsaw "floor by floor" (storey
slabs coloured by flat price; matplotlib static render + deck.gl web scene,
`output/analysis/map_07_3d_center.png`). Its height logic (parts inherit the parent's floor grid,
storey height = height / round(height/3 m)) is worth reusing. **Option B** for this subproject:
prices instead of frequency, using the same Blender pipeline.

## Proposed pipeline

1. `3d/export_scene.py` (normal Python, geopandas): `--city warsaw --center lat,lon --radius 1500`
   → crop the buildings, join frequency + height (fallback: `building_levels × 3.2 m`, then 9 m
   default), project to a local metre CRS centred on the crop (EPSG:2180 minus the centre), map
   `max_trips` → RGB via the poster palette, and write the ground layers. Output: an intermediate
   file Blender can read without extra libraries (e.g. JSON / NPZ of rings + heights + colours, or
   PLY/OBJ with vertex colours) in `3d/scenes/<name>/`.
2. `3d/build_blend.py` (runs inside Blender): read the scene, extrude footprints (bmesh, one mesh
   per class or a colour attribute rather than 100k objects), set up materials (emission tied to
   frequency so busy areas glow?), ground plane, sun/sky, camera, fog/mist. Save
   `3d/scenes/<name>/scene.blend` so the user can open it and find a view.
3. `3d/render.py` (Blender, background): render the camera(s) of the .blend → PNG. Drafts with Eevee
   (small), finals with Cycles CUDA.
4. Later: camera fly-over animation, New York, labels/legend composited in Python (PIL) like the poster
   header.

Design questions to settle early, on small drafts: real heights vs **height = frequency**
("frequency skyline"); top-down-ish oblique vs low dramatic angle; how much of the street level
to show (tall buildings hide what's behind them: keep the crop tight); night look with emissive
buildings vs low-sun shadows.

## Conventions (same as the main project)

- Every render/preview is saved as PNG: drafts in `png/previews/` (or `3d/renders/`), finals in
  `3d/renders/`; diagnostic/comparison images in `png/archive/how_it_was_made/3d/` (the user posts
  "how it was made" stories).
- Say which files a command creates/overwrites before running it; commit + push after every change.
- Heavy renders detached (see the main CLAUDE.md "Running long jobs"); check free RAM first.
- `.blend` files and scene caches are large: add `3d/scenes/` to `.gitignore`, commit scripts only.
- Keep this README's **Status** section current.

## Status

- 2026-10-12: folder + plan created. Blender not installed yet. Nothing rendered.
