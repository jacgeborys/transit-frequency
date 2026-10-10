# Transit Frequency Map

Visualizes transit accessibility by combining GTFS schedule data with walking isochrones.
Supports multiple cities — all scripts accept `--city <name>`.

## Supported cities

| City | GTFS source | Notes |
|------|------------|-------|
| **Warsaw** (default) | ZTM + KM + WKD via mkuran.pl | Multi-feed merge |
| **Poznan** | ZTM Poznan via mkuran.pl | Single feed |
| **Krakow** | ZTP Kraków bus + tram + regional trains | Multi-feed merge |
| **Gdansk** | ZTM Gdansk | City proper (bbox excludes Gdynia/Sopot) |
| **Berlin** | VBB GTFS | U/S-Bahn, trams, buses |
| **Manhattan** | MTA subway + buses + LIRR/MNR, PATH, NYC Ferry | Prefixed multi-feed merge, rotated render |
| **Lublin** | ZDiTM via mkuran.pl + regional trains | Prefixed merge (bus, trolleybus, trains) |
| **Amsterdam** | OVapi all-Netherlands feed, clipped to the bbox | Prefixed merge (chunked read), Dutch header |

Project location: `D:\QGIS\TransitFrequency` (since 2026-10-10; previously in OneDrive on C:).

## Status (2026-10-09)

**Current product: dark posters**, 300 dpi, one per city, all for Wednesday 07.10.2026:

| City | File | Data folder |
|---|---|---|
| Warsaw | `png/how_many_rides_in_5_mins_2026_10_07_dark_bmy_bright_hq.png` | `_data/warsaw/2026_10_04` |
| Kraków | `png/how_many_rides_in_5_mins_krakow_2026_10_07_dark_bmy_bright_hq.png` | `_data/krakow/2026_10_04` |
| Berlin | `png/how_many_rides_in_5_mins_berlin_2026_10_07_dark_bmy_bright_hq.png` | `_data/berlin/2026_10_07` |

Render one with `python script/render_poster.py --city <city>` (style + header texts live
there). Pipeline variant: `--gates` isochrones + `05_building_values.py` (buildings coloured
by their best frequency, buildings behind fences painted from the residents-only coverage).
Style: pitch-black page/land (since 2026-10-09; water 26,46,74, parks/forest lifted slightly so they stand out; comparison `png/archive/how_it_was_made/style/style_grid_black.png`), low-class area fills floored at CIE L* 7 (+0.8 per class) on the
background (`--min-fill-lightness`; the QGIS style's low classes are 25-40 % opaque, which
left the 1-80/day fills at L* 1-3 on black, i.e. invisible; Ząbki comparison
`png/archive/how_it_was_made/style/style_grid_fill_zabki.png`), palette `bmy_dark` with a perceived-lightness floor
(`--min-lightness 34`, CIE L*: every class lighter than unserved buildings) and a brighter
top (`--palette-extend "#fff38a,#fffbd6"`), coverage fill 0.42, Bahnschrift header with a
big city name + question + date line, km scale bar opposite the legend.
Since 2026-10-09 (all three 300 dpi finals above re-rendered with it; pre-change versions in
`png/archive/*_no_outline.png`): main roads
get an outer casing only (`--road-edge-mm`: widened major roads minus all road surfaces, no
lines inside junctions), railways darker and thinner (`--line-scale railways=0.6`), and every
building a 0.05 mm outer halo (`--building-outline-mm`, ring outside the footprint, fills
untouched) so small unserved/low-access buildings stay visible. The halo is "paint spill" (`--building-outline-spill 0.35`):
the adjacent building colour lightened 35 % towards white (multi-coloured big buildings: the
nearest part), comparison `png/archive/how_it_was_made/style/style_grid_spill_zoom.png`. Comparison:
`png/archive/how_it_was_made/style/style_grid_outline_halo.png`, full-map preview `png/archive/how_it_was_made/style/warsaw_poster_halo_150.png`.

**Lublin (300 dpi final `png/how_many_rides_in_5_mins_lublin_2026_10_14_dark_bmy_bright_hq.png`;
slightly zoomed `render_frame`)**: Wed **14.10.2026** (the polish_trains feed is a rolling
window from 09.10, 07.10 had 1 train trip). 1,113 stops, 104.9k departures (425 by train); top stops
KUL / Ogród Saski ~790. Render with `--date 14.10.2026`.

**Manhattan (300 dpi final `png/how_many_rides_in_5_mins_manhattan_2026_10_14_dark_bmy_bright_hq.png`,
render with `--date 14.10.2026`)**: underground stations (Times Sq-42 St, Grand Central platforms...)
are hidden in the drawing via `underground_ids.txt`. Template is Kraków's layout (OSM layer stack; Warsaw's
uses its old `roads.shp`, which drew no roads for Manhattan). Water is masked under coloured
buildings (Chelsea Piers' buildings stand on piers over the sea areas from the coastline). Coverage still comes from the run *with* them
(a clean rerun was stopped after 03, so `isochrones_gates*.gpkg` already exclude them while the
coverage maps don't); for a fully clean map rerun 04 gates, 04 gates_residents, 05.: rendered **rotated** so the Hudson is vertical
(`render_crs` = oblique Mercator, `gamma=66`: island upright, Hudson leaning 3°; `render_frame`
in metres of that CRS, 8.6 x 12.5 km from Downtown Brooklyn to Central Park north; renderer uses
both for template cities). New Jersey drops out (no NJ Transit data), LIC/Astoria/Greenpoint come
in. `legend_corner: 'top-right'` (new city option) puts the legend over Queens.
Frame since 13:00: 11.6 x 14.5 km, Park Slope/Red Hook .. Central Park north, Hudson .. Jackson
Heights. Basemap fixes found on the way: water now includes **sea from the coastline** (Harlem
River, Hell Gate, Flushing/Bowery Bay were missing: in OSM they are only coastline, not water
areas), and a failed layer no longer passes silently (parks had come back as "No data").
The bbox was enlarged to contain the rotated frame, so the whole chain reruns; the previous
north-up data is kept in `D:\QGIS\osm_basemap\manhattan_old_bbox`, `network/manhattan_old_bbox`,
`_data/manhattan_old_bbox`. GTFS for **Wed 14.10.2026** (LIRR feed is a rolling
30-day window from 08.10, so 07.10 was impossible). Basemap fetched; walking network, 03-05 and
the preview run as a detached chain (logs in `_data/manhattan/2026_10_09/log_*.txt`, milestones
in `png/log_buildings_final.txt`). Caveats: PATH feed expired 2026-06 (calendar stretched,
`gtfs_extend_calendar`); no NJ Transit (needs a developer login), so Jersey City/Hoboken look
under-served.
Thumbnails of every map: `png/archive/how_it_was_made/thumbs/*_thumb.png`.

**Finals 2026-10-11 (both routing fixes, raster coverage)**:
`png/how_many_rides_in_5_mins_newyork_2026_10_14_dark_bmy_bright_hq.png` (manhattan key, 16.6 x
17.5 km frame, title NEW YORK; isochrones avg 27.9 ha, 8,613/8,659 stops) and
`png/how_many_rides_in_5_mins_amsterdam_2026_10_14_dark_bmy_bright_hq.png` (avg 20.8 ha,
1,543/1,587 stops; skipped: Schiphol terminal platforms, edge stops in Zaandam/Muiden).
Raster coverage: New York 3.3 + 3.3 min (vector ~70 min each), Amsterdam ~0.5 min each.
Overnight: a vector-vs-raster check on New York and a larger New York candidate (`newyork` key,
18.6 x 30.5 km incl. Staten Island buses) via run_city.py; previews
`png/previews/newyork_poster_150.png` (larger) and `newyork_17km_poster_150.png` (current).

**New York + Amsterdam (2026-10-10)**: New York = the Manhattan config with the frame
grown 3 km south + 5 km east (`render_frame` (-3100, -11100, 13500, 6370), 16.6 x 17.5 km,
bbox 40.599-40.814 N, -74.068..-73.792), poster title "NEW YORK"; 8,659 stops, 1.04 M departures
on Wed 14.10. Amsterdam = city core 52.290-52.425 N, 4.755-5.030 E (EPSG:28992, Kraków template,
frame = bbox minus 400 m), Dutch header; OVapi GTFS, 1,587 stops, 183k departures on Wed 14.10
(the feed starts 09.10). Data in `_data/{manhattan,amsterdam}/2026_10_10`, logs in
`log_2026_10_10/`; 150 dpi previews `png/previews/{newyork,amsterdam}_poster_150.png`.
Previous New York basemap/network: `*_old_bbox2`; this afternoon's Overpass layers (replaced by
Geofabrik ones except water + sea): `D:\QGIS\osm_basemap\manhattan_overpass_2026_10_10`.

**OSM data now comes from Geofabrik extracts** (since 2026-10-10; Overpass was overloaded all
afternoon, even trivial queries got 504): `make_osm_extract.py --city X` downloads the regions in
`city['osm_pbf']` once to `D:\QGIS\osm_basemap\pbf\` and cuts the city (bbox + 1 km, complete
ways/relations); with `OSM_LOCAL_PBF=<extract>` set, `overpass_polite.post()` hands every query to
`osm_local.py`, which answers the fetchers' Overpass QL subset from the extract with the same
JSON. Nothing else changed in the fetchers. Validated against Overpass layers in central Manhattan:
railways/water identical, other layers 99.6-100 % (the differences were New Jersey objects, and
New Jersey is intentionally left out). A whole city's basemap: ~10-16 min instead of hours.

**Raster coverage (04_coverage_raster.py, prototype)**: same result as 04 on a 2 m grid aligned to
03's isochrone vertex lattice (per route: best stop per pixel; summed over routes; polygonized
to the 04 schema). Lublin: 100.00 % of pixels identical to the vector map; Kraków 99.67 %
(1-px edge specks, truth split between both). The vector method's geometry repair fills holes
in some invalid isochrones (e.g. 2,450 m² near Oratoryjna, Lublin); the raster keeps them.
Kraków 1.1 min vs 5.9 min. Full-size timing/comparison for New York + Amsterdam queued after
tonight's run (`compare_coverage.py`, previews `compare_raster_vs_vector_*.png`).

Also available: light chroma maps `…_gates_buildings_chroma.png` (all three cities), other
dark palettes for Warsaw `…_dark_<palette>.png`, style comparison sheets
`png/archive/how_it_was_made/style/style_grid_*.png` (`script/_style_grid.py`), header font samples
(`script/_font_samples.py`), diagnostic access sketches `png/archive/how_it_was_made/sketches/sketch_*.png`
(`script/sketch_access.py`).

### Next steps / open ideas

- **Published maps carry a routing error (found 2026-10-10)**: 02 merged its download tiles
  without dropping repeated ways, and 03 built the routing matrix with `csr_matrix`, which *adds
  up* duplicate entries, so every way crossing a tile border was routed at 2-4x its length
  (New York: 6.8 % of node pairs, Lublin 4.9 %). Stops along such streets got too small
  isochrones (E 23 St/1 Av: 3.0 instead of ~12 ha). Fixed in 03 (`_csr_min`: shortest of
  duplicates; tag g7) and 02 (ways deduplicated). New York + Amsterdam are computed with the fix;
  Warsaw, Kraków, Berlin, Lublin finals predate it and need 03 -> 04 -> 04 -> 05 -> render
  to be corrected (not done: waiting for the user's go).
- **Second routing error (found 2026-10-11)**: 02 kept `oneway=yes` (one-way streets, Dutch
  one-way cycle paths) one-directional for walking: 8-9 % of all edges (Amsterdam 8.7 %, New
  York 8.3 %) could be walked one way only, and some stops reached almost nothing
  (Amsterdam: Muiderpoortstation, Noorderpark skipped; average isochrone 17.7 -> 20.8 ha after
  the fix). Fixed in 03 (every edge walkable both ways, tag g8) and 02. Same rerun need for
  the old finals as above.
- **Berlin legend is too small**: Berlin borrows Kraków's layout (page twice as wide), the
  header scales with page width but the legend does not. Make `draw_legend` scale with
  the page (like `draw_headline`) and re-render Berlin.
- **New cities** (e.g. Manhattan): see "Adding a new city" below.
- Readability ideas not done: fewer, wider classes (8-10 instead of 19); faint district
  labels; a legend line explaining residents-only areas.
- Heavy rail as a barrier (like fences, with openings at level crossings; skip
  bridge/tunnel segments) was discussed, not implemented.

### History

- 2026-10-04…06: Warsaw + Kraków recomputed for 07.10.2026 (school-term weekday; Warsaw
  886k departures vs 771k in the August summer timetable). Warsaw walking network rebuilt
  with `02_fetch_walking_network.py` (1.23 M nodes, incl. `access=private` paths); old
  OSMnx network kept as `network/warsaw/walking_network_osmnx_2026_02.*.bak`.
  Variants: baseline (50 m buffer), barriers, gates (Kraków covered area: 16,620 → 15,400
  → 14,015 ha public).
- 2026-10-07: buildings coloured by best frequency; standalone renderer replaces QGIS export.
- 2026-10-08: access-model fixes (fence leak via path cells, gate openings in the fence
  raster, `foot=yes` alone no longer opens a gate, public path islands behind closed gates
  count as private, restrictive `access` beats `opening_hours`, roofs/carports not drawn
  as buildings, coverage noding loss). Berlin added. Incremental coverage cache.
  Light chroma maps; dark mode explored.
- 2026-10-09: dark bmy_bright posters for all three cities; National Stadium fix (big
  buildings show residents-only coverage).

## Architecture

```
00_download_gtfs.py --city <name>       -- Download GTFS (auto-merge for Warsaw/Kraków)
    v
_data/<city>/YYYY_MM_DD/                -- Merged GTFS directory
    v
01_calculate_trip_counts.py --city X YYYYMMDD <folder>  -- Trips & routes per stop (06-22)
    v
02_fetch_walking_network.py --city      -- Tiled Overpass download (cached, retried, refuses
                                           to build with missing tiles) -> walking_network.pkl
    v
03_generate_isochrones_local.py         -- 5-min walking isochrones, parallel (--workers),
      [--barriers] [--gates]               checkpointed every 250 stops
    v
make_osm_extract.py --city C           -- Geofabrik regions -> city extract (for OSM_LOCAL_PBF)
osm_local.py                           -- Local Overpass stand-in used by overpass_polite.post()
04_coverage_raster.py [--variant V]    -- Raster coverage map (prototype; same result, much faster)
04_create_coverage_map.py [--variant V] -- Deduplicated frequency per area: tiled planar
                                           subdivision (STRtree, parallel, RAM guard) +
                                           parallel dissolve. Incremental: tiles on a fixed
                                           grid cached by a fingerprint of their isochrones
                                           in <data>/.coverage_cache_<V>/, so a rerun after a
                                           small rule change only redoes changed tiles
                                           (Kraków: 47 s all-cached vs ~7 min cold)
    v
05_building_values.py --variant V       -- (optional) best frequency per building
    v
render_qgis_style.py                    -- Standalone re-implementation of the QGIS layouts
      [--residents] [--buildings] [--palette] [--crop lon,lat,w,h] [--extend-left/right-m]
      (layer cache in cache/render/, rasterio drawing; a city renders in ~1-7 min;
      PNG written to system temp then copied into png/; was needed while the project was in OneDrive)
render_poster.py --city C [--dpi]      -- Current product: dark bmy_bright poster (wraps the above)
sketch_access.py --city C --stop REGEX  -- Diagnostic access sketch around a stop
      [--recompute] [--name] [--suffix]    (--recompute: current rules, just these stops)
export_layout.py (QGIS python)          -- Exact export of the QGIS layout, project untouched
compare_barriers.py                     -- buffer vs barrier stats + close-ups

access_rules.py                         -- Gate / private-way rules for --gates
D:\QGIS\osm_basemap\fetch_osm_basemap.py -- Basemap fetcher (incl. barriers, gates, private_ways)
```

## Isochrone variants

- **baseline**: reachable network (375 m at 4.5 km/h) buffered by 50 m.
- **`--barriers`**: same routing; the 50 m off-network spread is a 2 m raster cost-distance
  where fences/walls/hedges are impassable and buildings are one-way space: entered like
  open ground (same budget) but never exited, so they are never a shortcut.
  Small holes (< 200 m²) filled, fragments (< 50 m²) dropped.
- **`--gates`** (implies barriers): two-layer routing. Closed gates and private ways form a
  residents-only layer you can enter from public paths but never leave back into public
  space (no shortcuts through estates). Outputs `isochrones_gates` (public) and
  `isochrones_gates_residents` (public + restricted). Rules in `access_rules.py`:
  explicit tags win, except `foot=yes` alone (often just means "pedestrian gate", e.g. on
  private campuses); untagged gates are closed except within 15 m of parks/cemeteries;
  lift gates ignored; ROD allotments count as closed. Gate nodes also cut a ~5 m opening
  in the fence raster (open gates: everyone; closed gates: residents run only), so gates
  without a mapped path through them still connect fenced plots. Path cells never
  override fence or closed-gate cells (sidewalks along fences would leak into plots).
  Public path "islands" (untagged paths reachable only through closed gates or private
  ways, e.g. stadium grounds, gated estates; < 5,000 nodes, not the main network) are
  reclassified private, so residents can walk them and stops never snap onto them.

## Adding a new city

1. **Config** in `script/cities.py`: `name`, `bbox`, `crs_metric` (a *metre*-based CRS: UTM
   zone or national grid), `gtfs` feed URL(s) + `gtfs_merge` ('single' or a merge mode),
   `vehicle_classify` (route -> bus/tram/train/metro), `has_frequencies` (feed uses
   frequencies.txt), `network_tiles` (Overpass tile grid; denser city -> more tiles),
   `template: 'krakow'` (no QGIS layout of its own: borrow Kraków's layout + styles).
2. **Poster texts**: add an entry to `POSTER` in `script/render_poster.py` (city name,
   language; English template `EN` is there).
3. **Run** everything with one command (hidden, outside the session; milestones in
   `png/log_buildings_final.txt`, step logs in `<data folder>/logs/`):

```bash
python run_city.py --city X --date YYYYMMDD --detach          # a school-term Wednesday
python run_city.py --city X --date YYYYMMDD --final --detach  # + 300 dpi poster into png/
```

   Steps: gtfs (00) -> counts (01) -> osm (make_osm_extract) -> basemap (fetch_osm_basemap via
   the local extract + drop_underground_buildings) -> network (02) -> iso (03 --gates) ->
   coverage (04_coverage_raster, gates + gates_residents) -> buildings (05) -> render (150 dpi
   preview, `png/previews/<city>_poster_150.png`) [-> final (300 dpi)] [-> cleanup].
   Make-like: a step whose output is newer than its inputs is skipped, so a rerun continues where
   it stopped; `--from STEP` forces a step and everything after it (e.g. `--from iso` after
   changing access rules), `--to STEP` stops early, `--vector` uses the old vector 04 (check),
   `--cleanup-osm` deletes the city's Geofabrik regional files at the end (the small city extract
   stays for reruns). Regional files are otherwise kept in `D:\QGIS\osm_basemap\pbf\` (~0.2-0.5 GB
   each; `make_osm_extract.py --refresh` downloads newer ones).
   Typical times (2026-10-11): Amsterdam ~25 min from scratch, New York 16.6 x 17.5 km ~1 h.

4. **Check** a few busy stops with `sketch_access.py --city X --stop "^Name$"` before
   trusting the map (access tagging differs between countries).

**Notes for Manhattan / New York** (implemented as `manhattan`, `gtfs_merge: 'prefixed'`:
MTA subway + 5 bus feeds sharing one `bus_` id prefix, LIRR, Metro-North, PATH, NYC Ferry;
stops clipped to the bbox, exact service-id matching):
- GTFS: MTA publishes separate feeds - subway, and buses per borough (Manhattan, Bronx,
  Brooklyn, Queens, Staten Island) plus MTA Bus Company; also PATH, LIRR, Metro-North,
  NYC Ferry. Needs a multi-feed merge like Warsaw (check `route_id`/`stop_id` collisions
  between feeds; prefix ids per feed). Some feeds have high `frequencies.txt`-style
  service; verify trip counts per stop look sane (Times Sq should be very high).
- Bbox: Manhattan alone is narrow; include a margin (Jersey City, Long Island City,
  south Bronx) so edge stops' isochrones aren't cut, then crop the render if wanted.
- CRS: use UTM 18N `EPSG:32618` (metres). Avoid NY State Plane `EPSG:2263` (US feet) -
  the pipeline assumes metres everywhere.
- OSM access tags: US gates/fences are tagged differently; expect many untagged gates in
  parks (rule: untagged gates near parks are open) and `access=private` on building
  courtyards. Check sketches around Central Park, Stuyvesant Town, Battery Park City.
- Basemap: very dense buildings; `fetch_osm_basemap.py` tiles buildings, should be fine.
  Water is large (rivers/harbour): the dark water colour matters for the look.
- Poster header in English (`EN` in `render_poster.py`), US-style date may be preferred.
- No QGIS layout: `template: 'krakow'` gives a landscape page; Manhattan is tall and
  narrow, so a portrait template (`'warsaw'`) is probably the better fit.

## Running on this machine (15 GB RAM)

Run heavy steps one at a time and detached from the terminal session (e.g. a `.cmd`
started with `Start-Process`), since 03/04/renders can each take 2–6 GB. All long steps
checkpoint (03 chunks, 04 tiles, basemap/network tiles), so an interrupted run resumes.

## Directory structure

png/: only the current 300 dpi finals; `png/previews/` = current working previews;
`png/archive/how_it_was_made/{sketches,comparisons,style,thumbs,logs}` = diagnostic sketches,
analytical zooms, comparisons and style experiments (kept for a "how it was made" post);
`png/archive/variants/` = superseded style variants.

```
_data/<city>/YYYY_MM_DD/   -- GTFS data + outputs (isochrones*, coverage_map*, buildings*, logs)
network/<city>/            -- Walking network (pickle) + tile cache
D:\QGIS\osm_basemap\<city>\  -- OSM basemap layers (shared across projects)
styles/                    -- QGIS QML styles
png/                       -- Rendered maps
```

## Key concepts

- **trip_count**: Total departures at a stop (6 AM - 10 PM)
- **unique_routes**: Number of distinct transit lines serving a stop
- **route_ids / route_trip_counts**: per-stop route lists enabling deduplication when
  isochrones from nearby stops overlap (per route, the best stop counts once)
- **Vehicle types**: bus, tram, train, metro — classified by city-specific route_id rules

## Data sources

- **GTFS**: [mkuran.pl](https://mkuran.pl/gtfs/) (Polish cities), ZTP Kraków, VBB (Berlin)
- **Walking network, barriers, gates, basemap**: OpenStreetMap via Overpass API
