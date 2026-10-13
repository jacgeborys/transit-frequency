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

- **All 3D output stays in the 3d folder** (user's wish, 2026-10-12): renders in `3d/renders/`,
  drafts and how-it-was-made steps in `3d/renders/drafts/`; nothing in `png/previews` or `png/archive`.
- Say which files a command creates/overwrites before running it; commit + push after every change.
- Heavy renders detached (see the main CLAUDE.md "Running long jobs"); check free RAM first.
- `.blend` files and scene caches are large: add `3d/scenes/` to `.gitignore`, commit scripts only.
- Keep this README's **Status** section current.

## Usage (working since 2026-10-12)

```
python 3d/export_scene.py --city warsaw --center 52.2318,21.0060 --radius 1500 --name warsaw_center
blender --background --factory-startup --python 3d/build_blend.py -- --name warsaw_center
blender --background 3d/scenes/warsaw_center/scene.blend --python 3d/render.py -- --out png/previews/3d_warsaw.png
```
(`blender` = `"C:\Program Files\Blender Foundation\Blender 5.2lender.exe"`; 5.2.2 LTS installed.)

- `export_scene.py` (Python 3.8 with geopandas): crop (square, `--bounds` relative to the centre,
  `--detail-bounds` = full-detail core; outside it outlines are simplified by 1 m). **Buildings + heights
  from the OSM basemap** (`buildings.gpkg` incl. `building:part`): height, else levels x 3.2 m, else 9 m;
  parts start at min_height / building_min_level; roofs/carports without height = slab at 3.5-4 m. A
  building is drawn whole unless its parts cover >= 85 % of it; **parts take their parent's frequency**,
  so a landmark is one colour (PKIN). (Until 2026-10-13 heights came from the housing-price project's
  `buildings_3d`, which drops every building that has any part: Warsaw Spire kept only its crown,
  Politechnika's main building was missing.) Frequency: parent's `osm_id` in `buildings_gates.gpkg`,
  else the footprint it lies in. Colours from `render_poster.poster_colours()` (same code path as the
  poster). **Big buildings are coloured by frequency** (poster greys them; `--big-grey`). Ground layers
  from the poster's QGIS layer stack, dissolved per 250 m tile (a dissolved road network is one polygon
  with thousands of holes that Blender's fill got wrong: grey sheet over the Vistula). **Coverage tint**:
  `coverage_map_gates.gpkg` per class as `coverage_<k>` layers with the poster's composited colour and
  alpha (`poster_colours()['coverage_classes']`). Output `3d/scenes/<name>/scene.json`.
  Gotcha: build helper GeoDataFrames from arrays (`.values`); a dict of Index + a geometry Series are
  aligned differently (by position vs label) and silently mismatch rows.
- `build_blend.py` (inside Blender): one building mesh with face attributes `col` (linear colour),
  `glow` (emission by class), `cls`; flat ground meshes; low sun (`--sun-azimuth/--sun-elevation`),
  dark sky, camera (`--cam-azimuth/--cam-distance/--cam-height/--lens`), view transform Standard
  (keeps the poster colours). `--glow` scales the emission (0.3 default; 1.0 washes out the shading).
- `save_view.py` (inside Blender): saves the camera (or with `--from-viewport` the main 3D view,
  lens halved to match) of a .blend to `3d/views/<name>.json`; `render.py --view <name>` applies it.
  Views stay valid across rebuilds because every export keeps the scene origin at `--center` (PKIN);
  `--bounds xmin,ymin,xmax,ymax` (m from the centre) widens the area without moving the origin.
  Saved views: `center` (from SSW over Śródmieście; the build defaults, i.e. draft 2's framing, which the
  user prefers to their own re-aligned camera), `mokotow` (from above Mokotów looking north),
  `zoliborz` (from Żoliborz looking SE, lens 28 mm: 40 felt narrow). `render.py --lens` overrides the lens. `build_blend.py --view <name>` starts the .blend with that camera.
- `build_blend.py` also adds a 40 km ground plane and a distance haze in the sky colour on every
  material (Camera Data distance, `--haze-start 2500 --haze-scale 5000`), so a wide view has no edge.
- Coverage tint in 3D: transparent ground layers at alpha = poster alpha x `coverage_alpha` (day 0.45,
  night 0.7 + emission 0.25); full poster alpha drowned the buildings.
- **Looks** (`build_blend.py --look day|night`, table `LOOKS`): `day` -> `scene.blend`; `night` ->
  `scene_night.blend`: moonlight (sun 0.25, bluish), near-black sky, buildings glow by class (strength
  1.6 x (0.08 + 0.92 k^1.6)), walls at half the roof glow, streets 0.12 and the Vistula 0.35 faint glow
  (else the river vanishes), compositor **Bloom** (Blender 5: `scene.compositing_node_group`, Glare node
  settings are input sockets). Every build adds the empty `focus_pkin` (0, 0, 90 m) as the focus target.
- **Far blur** (`render.py --far-blur start,end,size` or `"far_blur"` in the view json): compositor mixes
  the image with a blurred copy by the depth pass, so only the distance softens (a lens can't do that:
  blurring the horizon blurs PKIN at 60 %). Żoliborz: 9-18 km, 0.3 % of the width.
- **Tilt-shift**: `render.py --fstop F` (or `"fstop"` in the view json) = depth of field on `focus_pkin`.
  City scale needs absurd f-numbers: from Żoliborz (6.4 km to PKIN) 0.0025+ is invisible, **0.001 = mild**
  at first, 0.0005 = strong miniature. 2026-10-13: user wants it on PKIN in `center` (f/0.004, 3.6 km)
  and `mokotow` (f/0.003, 5 km), and **no lens tilt-shift in Żoliborz** (far blur only).
- Sky: gradient from a dark zenith to a slightly lighter navy horizon (= haze colour); Eevee raytracing on.
- `render.py`: **Cycles on CUDA by default** (1600x1000 ~25 s, 3200x2000 x 128 samples ~2 min; Eevee is
  no faster on this GPU and lacks the soft occlusion). Uses the .blend's camera unless `--view`.

## Status

- 2026-10-12: folder + plan created.
- 2026-10-12: Blender 5.2.2 installed; pipeline works end to end. First drafts of central Warsaw
  (3x3 km around PKIN, 6,876 building polygons, 195k faces): `png/previews/3d_warsaw_draft{1,2}.png`,
  copies in `png/archive/how_it_was_made/3d/`.
- 2026-10-12: the user chose two views in Blender (`3d/views/center.json`, `mokotow.json`) and wants a
  broad perspective, not a cut-out diorama. Scene `warsaw_wide`: `--bounds=-6000,-6500,5000,5500`
  (11 x 12 km, 60k building polygons, 1.26M faces, json 24 MB, export 2.5 min, build 16 s, Eevee
  render ~25 s).
- 2026-10-12: third view `zoliborz`; `center` reset to draft 2's framing. Sky gradient + horizon haze;
  Cycles adopted. Finals 3200x2000: `3d/renders/warsaw_{center,mokotow,zoliborz}.png` (from
  `scenes/warsaw_wide`, rebuilt with `--view zoliborz`). Earlier drafts moved to `3d/renders/drafts/`.
  The user likes the shadows/look.
- 2026-10-12: ground layers tiled (see export) -> the river is visible again and the ground is dark with
  road lines instead of the earlier grey sheets; finals re-rendered.
- 2026-10-12: `warsaw_wide` extended north/east for the wider Żoliborz view: `--bounds=-7000,-6500,7000,9000`
  (14 x 15.5 km, 94k building polygons, 1.85M faces, json 37 MB, export 3 min). Lens comparison in
  `3d/renders/drafts/06_zoliborz_lens{32,28}.png`.
- 2026-10-13: user prefers the **day (shadow) look**; night stays as an option (glow lowered to 1.0, bloom
  0.35). Heights now from OSM (Spire, Politechnika complete; PKIN one colour), coverage tint, background to
  Ursynów and Wawer: `warsaw_wide` = `--bounds=-7000,-15000,15000,9000 --detail-bounds=-7000,-6500,7000,9000`
  (22 x 24 km, 203k buildings, 3.0M faces, json 102 MB, export ~10 min, build 32 s, 1600x1000 render ~50 s).
  Finals `3d/renders/warsaw_{zoliborz,center,mokotow}.png`.
- 2026-10-12: **night look + tilt-shift** (user liked both ideas). Finals `3d/renders/warsaw_night_{zoliborz,
  center,mokotow}.png` (Żoliborz with f/0.001 on PKIN; others without DOF). Drafts `07_night_*`,
  `08_night_tiltshift_f*` in `3d/renders/drafts/`. Next: try
  New York from Brooklyn (above Prospect Park, looking at Manhattan): data is ready except heights:
  `osm_basemap/newyork/buildings.gpkg` kept only osm_id + building, re-read height/levels/parts from
  `osm_basemap/pbf/newyork_new-york.osm.pbf`. Also fly-over or height = frequency animation, labels/legend.
