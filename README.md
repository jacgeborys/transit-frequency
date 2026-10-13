# Transit Frequency Map

Visualizes transit accessibility by combining GTFS schedule data with walking isochrones.
Supports multiple cities — all scripts accept `--city <name>`.

## Supported cities

| City (key) | GTFS source | Notes |
|------|------------|-------|
| **Warsaw** (`warsaw`, default) | ZTM + KM + WKD via mkuran.pl | Multi-feed merge, own QGIS layout |
| **Poznan** (`poznan`) | ZTM Poznan via mkuran.pl | Single feed |
| **Krakow** (`krakow`) | ZTP Kraków bus + tram + regional trains | Multi-feed merge, own QGIS layout (template for the others) |
| **Gdansk** (`gdansk`) | ZTM Gdansk | City proper (bbox excludes Gdynia/Sopot) |
| **Tricity** (`tricity`) | ZTM Gdańsk + ZKM Gdynia + polish_trains (SKM, PKM, IC) via mkuran.pl, clipped | Gdańsk + Sopot + Gdynia, 16.0 x 30.2 km rotated portrait frame (coast vertical) |
| **Berlin** (`berlin`) | VBB GTFS | U/S-Bahn, trams, buses |
| **Lublin** (`lublin`) | ZDiTM via mkuran.pl + regional trains | Prefixed merge |
| **Amsterdam** (`amsterdam`) | OVapi all-Netherlands feed, clipped | Dutch header |
| **New York** (`newyork`) | MTA subway, 6 bus feeds, LIRR, Metro-North, PATH, NYC Ferry, AirTrain JFK | 25.7 x 25.0 km rotated frame (oblique Mercator, Hudson vertical) |
| Manhattan (`manhattan`) | as New York | Older 16.6 x 17.5 km frame of the same city |
| **Zurich** (`zurich`) | Swiss national GTFS (opentransportdata.swiss), clipped | Slope-aware walking, Swiss German header |
| **San Francisco** (`sanfrancisco`) | 511.org regional feed (all Bay Area operators; key in `.secrets/`) | + Oakland/Berkeley, slope-aware walking |

Project location: `D:\QGIS\TransitFrequency` (since 2026-10-10; previously in OneDrive on C:).

## Status (2026-10-13)

**2026-10-13: Berlin zoomed in** (`render_frame` = 80 % of the bbox, 37.0 x 31.0 km; isochrones
unchanged, still pre-fix) and its 300 dpi final, JPEG and the collage redone. **Sketches** restyled
for the LinkedIn panel (`sketch_access.py --paths none --building-status`): lavender public / pale-lavender
residents-only walk areas, reached buildings deep purple, unreached hatched, red square = closed gate,
ring = open gate, yellow stops, legend box; `--center`, `--cached` (reuses the last `--recompute`).
Panel done: `png/sketch_panel_4.png` / `png/compressed/sketch_panel_4.jpg` (`make_sketch_panel.py`;
Plac Wilsona, Cichy Kącik, Hyde & Lombard with stairs + flat-walk outline, Times Square; tiles
recomputed with the current rules). **Fences on multipolygon relations** were never fetched (e.g. ROD
"Małe Błonia", Kraków): fetcher fixed; `add_barrier_relations.py --city X` patches an existing
basemap (done for Kraków: 25). Maps pick it up only with an iso rerun (not done; all cities affected).

**2026-10-13 evening: poster style "E" adopted** (`STYLE` in render_poster.py; chosen in `png/style_lab/`):
Schwarzplan look: land grey 12, pitch-black roads under the buildings, parks/rails hidden, walk-area fill 0.30
(the visible trace of the path/fence analysis), building saturation 1.35, "paint spill" outlines, **25 classes
to 3500+** (`--breaks`, above 3500 only ~1 %, Berlin), **continuous legend ramp** labelled 1/10/50/100/250/500/
1000/2000/3500+ (`--legend-ticks`, `--legend-compact 0.55`). Headlines say "transit" (EN), ÖPNV/ÖV (DE/CH),
ov (NL), transports en commun (FR), komunikacji (PL). **Boston and Paris zoomed out ~10 %** (rerun from GTFS:
Boston 17.2 x 16.4 km, Paris 24.3 x 16.9 km; Paris now has Stade de France, both bois). 150 dpi previews of all
11 in `png/previews/<city>_poster_150.png`; 300 dpi finals so far: Warsaw (old dark finals archived in
`png/archive/finals_2026_10_13_dark/`). 3D: colours follow `STYLE` via `poster_colours()` (breaks applied there
too); haze pushed back (`--haze-start` 6000 Warsaw / 7000 NYC); NYC final view `brooklyn_tilt8` (8 deg lower:
hides uncoloured Manhattan north of Harlem), no bloom.

**2026-10-13: style lab (`png/style_lab/`)**: looking for a crisper "wow" look like the viral 2023
Schwarzplan rail maps (buildings only, flat colours, 3 classes, white). New renderer options: `--hide LAYER`
(drop basemap layers), `--breaks 25,100,250,600,1500` (merge the 19 classes into fewer, wider ones).
`_style_grid.py --set wow --root ../png/style_lab` renders 12 variants (current poster, dark/light
Schwarzplan, 19/6/3 classes, bmy/plasma/neon/turbo): `png/style_lab/style_grid_boston_wow.png`. Old
Tricity style grids moved to `png/archive/how_it_was_made/style/`. Pick pending.

**2026-10-13: Boston added** (`--city boston`, date 14.10.2026, MBTA feed "Fall 2026"): Boston core +
Cambridge, Somerville, Brookline, Charlestown, East Boston/Logan, south to Dorchester/Jamaica Plain,
15.6 x 14.8 km in UTM 19N, legend over Dorchester Bay, no hills. From scratch in 30 min (osm extract
10, basemap 5, iso 6, preview 3). 3,226 stops, 256k departures 06-22 h. Massachusetts coastline
complete (harbour/Charles checked). Stop check: Government Center in a station island (walk fine,
31.6 ha); **Boston Landing skipped**: its platform paths connect only to `highway=construction`
ways in OSM (38 departures, left as is). 150 dpi preview `png/previews/boston_poster_150.png`;
300 dpi final rendered (2.8 min). Streets under the buildings (`--roads-under-buildings`, render_qgis_style.py:
coloured buildings drawn after roads/railways instead of with the coverage; part of the shared
`STYLE` since 2026-10-13, all 11 finals re-rendered with `script/rerender_all_300.cmd`).
**Water over land fixed**: the 'Gulf of Maine' `natural=bay` relation (coarse CanVec import) cut
through Logan Airport and put 5,300 buildings under water; `fetch_osm_basemap.py` now drops bays
>10x the bbox area (`drop_ocean_bays`; the sea comes from the coastline). Boston's water.gpkg
patched (original in `D:\QGIS\osm_basemaposton\_with_gulf_of_maine`). Other cities: bays hold
only a few shore buildings (NY 32, SF 37, Tricity 22), untouched. Comparison: `png/archive/how_it_was_made/comparisons/boston_roads_under_buildings.png`.

**2026-10-12: Paris added** (`--city paris`, date 14.10.2026, IDFM regional feed): Paris + inner ring
(La Défense .. Vincennes, Saint-Denis .. Ivry), 22.1 x 15.4 km in Lambert-93, French header.
From scratch in 83 min (osm extract 12, basemap 18, iso 18, coverage 9, preview 11). 8,654 stops,
1.29 M departures 06-22 h. Stop check: only the usual station islands; Saint-Denis (SNCF) sits on
the bbox's north edge, outside the frame. 150 dpi preview done; 300 dpi final rendered (7 min).

**2026-10-12: all 9 finals re-rendered at 300 dpi with the unified poster style** (Tricity's style is now
the default in `STYLE`, render_poster.py): low classes lifted by a rising lightness floor
(`--min-lightness 40 --min-lightness-step 1.6`), roads as solid streaks (80,82,94, faint casing
66,68,80 @ 0.06 mm), railways 100,100,112, water 22,39,63; legend swatches = the opaque building
colours actually drawn (before: QGIS class colours at their own near-transparent alpha, so 1-10
looked black), legend scaled with the page (sqrt of page area vs Kraków's layout, 0.7-1.8x) and
always flush in a map corner (bottom-right unless `legend_corner`); scale bar boxes 1 km each;
north arrow to true north on every map. Batch: `script/rerender_all_300.cmd`. Colour check: L*
rises monotonically (CVD-safe ordering); 1-10 vs 10-25 nearly identical (dE 4, few buildings);
the low blue looks brighter than its L* (Helmholtz-Kohlrausch), accepted.

**Current product: dark posters, 300 dpi**, all in `png/` (nothing else lives there; previews,
sketches, comparisons and old variants are in `png/archive/`, see "Directory structure"):

| City | Final | Day | Data folder | Routing fixes |
|---|---|---|---|---|
| Boston | `how_many_rides_in_5_mins_boston_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/boston/2026_10_13` | yes |
| Tricity | `how_many_rides_in_5_mins_tricity_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/tricity/2026_10_12` | yes |
| New York | `how_many_rides_in_5_mins_newyork_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/newyork/2026_10_11` | yes (+ JFK island walk) |
| San Francisco | `how_many_rides_in_5_mins_sanfrancisco_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/sanfrancisco/2026_10_11` | yes, + hills |
| Zurich | `how_many_rides_in_5_mins_zurich_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/zurich/2026_10_11` | yes, + hills |
| Amsterdam | `how_many_rides_in_5_mins_amsterdam_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/amsterdam/2026_10_10` | yes |
| Warsaw | `how_many_rides_in_5_mins_2026_10_07_dark_bmy_bright_hq.png` | Wed 07.10.2026 | `_data/warsaw/2026_10_04` | **no** |
| Kraków | `how_many_rides_in_5_mins_krakow_2026_10_07_dark_bmy_bright_hq.png` | Wed 07.10.2026 | `_data/krakow/2026_10_04` | **no** |
| Berlin | `how_many_rides_in_5_mins_berlin_2026_10_07_dark_bmy_bright_hq.png` | Wed 07.10.2026 | `_data/berlin/2026_10_07` | **no** |
| Lublin | `how_many_rides_in_5_mins_lublin_2026_10_14_dark_bmy_bright_hq.png` | Wed 14.10.2026 | `_data/lublin/2026_10_09` | **no** |

The four older finals predate the two routing fixes of 2026-10-10/11 (see "Known issues"): stops
on streets crossing a download-tile border, or on one-way streets, got too small walk areas. A
rerun is `python script/run_city.py --city X --date YYYYMMDD --from iso --final --detach` (under an
hour each with raster coverage); not done yet, waiting for the user's go.

Key numbers of the new maps (Wed 14.10.2026):
- **New York**: 11,222 stops, 1.34 M departures; Manhattan to ~110th St, Brooklyn to Brighton
  Beach/Coney Island, the Rockaways, past JFK; legend bottom-right over the Atlantic. Isochrones
  avg 27.7 ha; 16 stops skipped (New Jersey PATH, Bronx stops at the bbox edge). AirTrain JFK from
  a 2021 Port Authority feed (calendar stretched like PATH's; frequencies expanded). No NJ Transit,
  so Jersey City/Hoboken show as land without coverage.
- **San Francisco + Oakland/Berkeley**: 5,853 stops, 449k departures, 22 operators; isochrones avg
  24.0 ha (hills: edge cost x1.070; Coit Tower's walk area halves). Legend bottom-right over San
  Leandro Bay (the open Bay would be nicer: needs free legend placement).
- **Zurich**: 1,970 stops, 243k departures (autumn holidays barely change the timetable);
  isochrones avg 25.6 ha (hills: edge cost x1.088); "zu Fuss" (Swiss spelling).
- **Tricity** (2026-10-12, 300 dpi final in `png/`, data `_data/tricity/2026_10_12`): Gdańsk + Sopot + Gdynia, frame rotated 22 deg (oblique Mercator
  gamma 112) so the coast runs up the page; Orunia .. Babie Doły/Cisowa, airport .. Stogi; legend
  over the Gulf. 2,534 stops, 249k departures (196k bus incl. Gdynia trolleybuses, 46k tram, 7k
  train); isochrones median 22.7 ha, 0 stops skipped. ZKM Gdynia GTFS:
  `https://api.zdiz.gdynia.pl/pt/gtfs.zip`. ZTM Gdańsk's feed only covers ~2 weeks ahead
  (to 24.10 here), so pick a date close to the download. Whole run 23 min. North arrow (`'north_arrow': True` in cities.py; minimal split needle above
  the scale bar, rotated to true north, +22 deg). Style test (Tricity only, `POSTER['tricity']['extra']`):
  rising lightness floor `--min-lightness 40 --min-lightness-step 1.6` (the low blues were pinned at
  L* 34: gloomy, blurring into unserved buildings; now every class is a step lighter), roads
  80,82,94 / faint casing 66,68,80 @ 0.06 mm (solid streaks, fill L* ~35 stays below the dimmest
  served building so the buildings lead; a strong casing doubled every road into noisy lines;
  `style_grid_tricity_roads_fill.png`), railways 100,100,112, water one step darker 22,39,63. Comparisons in
  `png/archive/how_it_was_made/style/tricity_blue/`. Other cities keep the old style until told.
- **Amsterdam**: city core, 1,587 stops, 183k departures; avg 20.8 ha; Schiphol terminal
  platforms skipped (an older run, before the island walk).

## How a map is made now (2026-10-11)

One command per city, the same for every city: `python script/run_city.py --city X --date YYYYMMDD
--detach` (`--final` for 300 dpi). Details in "Adding a new city". What changed in the last days:
- **OSM data from Geofabrik extracts** instead of Overpass (which was overloaded for hours):
  `make_osm_extract.py` cuts the city out of a regional .osm.pbf (re-cut automatically when the
  bbox changes); `osm_local.py` answers the fetchers' Overpass queries from it (`OSM_LOCAL_PBF`,
  routed in `overpass_polite.post()`). Validated against Overpass in Manhattan (99.6-100 %
  identical). A city's basemap: minutes instead of hours.
- **Raster coverage** (`04_coverage_raster.py`, default): same result as the vector 04 (New York:
  99.36 % of 71.8 M pixels identical, the rest 1-px edges where the raster is right more often) in
  3.3 instead of 62 min. Vector 04 kept for checks (`run_city.py --vector`).
- **Slope-aware walking** for cities with `'hills': True`: Tobler's hiking function, the same both
  ways (mean of up- and downhill time), DEM from `fetch_dem.py` (swissALTI3D 2 m, USGS 1/3" ~10 m)
  sampled every 10 m; bridges/tunnels flat, stairs at least 25 % slope.
- **Stop check** (`diagnose_stops.py`, run_city step `check`): per stop where its walk starts,
  whether it sits in a closed-off "island", and its walk area; flags + sketches. CLAUDE.md lists
  the known patterns.
- **Islands with a stop inside**: the stop also walks inside the island (JFK terminals, Oakland
  Airport BART); additive, so nothing shrinks.
- Rendering: rail tunnels are drawn below the coverage (under buildings and water: Grand Central
  no longer has tracks across it); legend position per city (`legend_corner`).

## Style

Render one with `python script/render_poster.py --city <city>` (style + header texts live there;
cities without a QGIS layout borrow Kraków's, `template` in cities.py). Pitch-black page/land,
water 26,46,74, parks/forest lifted slightly; buildings coloured by their best frequency (05),
buildings behind fences from the residents-only coverage; palette `bmy_dark` with a
perceived-lightness floor (`--min-lightness 34`) and a brighter top (`--palette-extend
"#fff38a,#fffbd6"`); low-class fills floored at CIE L* 7 (`--min-fill-lightness`); coverage fill
0.42; main roads with an outer casing only; railways darker/thinner; every building a 0.05 mm
"paint spill" halo (adjacent colour lightened 35 %); Bahnschrift header (big city name + question
+ date line); km scale bar opposite the legend. Style comparisons: `png/archive/how_it_was_made/style/`.

## Known issues / open decisions

- **Old finals carry two routing errors** (Warsaw, Kraków, Berlin, Lublin; see Status):
  (1) 02 merged download tiles without dropping repeated ways and 03 *summed* duplicate matrix
  entries, so ways crossing a tile border were routed at 2-4x their length (6.8 % of New York's
  node pairs; E 23 St/1 Av 3.0 instead of ~12 ha); fixed: `_csr_min` (tag g7), 02 dedupes ways.
  (2) `oneway=yes` was one-directional for walking (8-9 % of edges); fixed: all edges two-way (g8).
- **Coastline across a region border**: the sea layer comes from `natural=coastline` in the city's
  extracts. New York state alone lacks the New Jersey shore (Jersey City came out as sea); patched
  for `newyork` by rebuilding water + sea with a New Jersey extract (NJ-less versions in
  `D:\QGIS\osm_basemap\newyork\_nj_less`). A rerun from scratch would bring it back: decide between
  New Jersey in `osm_pbf` (NJ drawn in full, unserved) or a water-only extra region.
- **Legend too small on large pages** (New York, San Francisco, Berlin): the legend does not scale
  with the page like the header does (`draw_legend` vs `draw_headline`); fix before publishing.
- **Legend placement**: only corners (`legend_corner`); Zurich (over Zollikon) and San Francisco
  (over San Leandro Bay) would look better with the legend in the lake/Bay.
- **Amsterdam** predates the island walk (Schiphol terminal platforms skipped); rerun from iso if wanted.
- OSM details seen: the crooked block of Lombard St is tagged private (its sidewalks are public).
- Ideas not done: fewer, wider classes (8-10); faint district labels; a legend line for
  residents-only areas; heavy rail as a barrier (openings at level crossings).

## History

- 2026-10-04…06: Warsaw + Kraków recomputed for 07.10.2026 (school-term weekday; Warsaw 886k
  departures vs 771k in the August summer timetable); Warsaw walking network rebuilt with 02.
- 2026-10-07: buildings coloured by best frequency; standalone renderer replaces QGIS export.
- 2026-10-08: access-model fixes (fence leaks, gate openings, `foot=yes` alone no longer opens a
  gate, public path islands behind closed gates count as private, ...). Berlin added.
- 2026-10-09: dark bmy_bright posters (Warsaw, Kraków, Berlin); National Stadium fix; Manhattan
  (rotated frame, sea from the coastline, underground stations hidden) and Lublin finals.
- 2026-10-10: project moved to D:; polite Overpass access (machine-wide lock, shared backoff);
  Overpass overloaded all day -> Geofabrik extracts + local query answering; raster coverage;
  first routing fix; New York (frame +3 km S, +5 km E) and Amsterdam finals.
- 2026-10-11: second routing fix; `run_city.py` (one normalized pipeline); New York to Brighton
  Beach/JFK with AirTrain and the island walk; rail tunnels below buildings; stop diagnostics;
  slope-aware walking; Zurich and San Francisco + Oakland/Berkeley; png/ cleaned up (finals only).

## Architecture

```
run_city.py --city C --date YYYYMMDD [--detach] [--final] [--from/--to STEP]
                                        -- The whole pipeline below, make-like, one city
00_download_gtfs.py --city C            -- Download + merge GTFS (prefixed merge clips any feed to the
                                           bbox, chunked; calendars/frequencies expanded after clipping)
01_calculate_trip_counts.py --city C YYYYMMDD <folder>  -- Departures & routes per stop (06-22)
make_osm_extract.py --city C            -- Geofabrik regions -> city extract (bbox + 1 km, .bbox stamp)
D:\QGIS\osm_basemapetch_osm_basemap.py -- Basemap layers (incl. sea, barriers, gates, private_ways)
drop_underground_buildings.py --city C  -- underground_ids.txt (subway stations etc. not drawn)
02_fetch_walking_network.py --city C    -- Walking network (ways deduped, bridge/tunnel kept) -> pickle
   (fetchers ask overpass_polite.post(); with OSM_LOCAL_PBF set, osm_local.py answers from the
    extract instead of the Overpass server)
fetch_dem.py --city C                   -- Terrain model for cities with 'hills' (swissALTI3D, USGS)
03_generate_isochrones_local.py --gates -- 5-min walks (375 m flat-equivalent), parallel; fences,
                                           gates, private ways, island walk, slope cost (hills)
diagnose_stops.py --city C <data>       -- Stop check: snapping, islands, walk areas (+ sketches)
04_coverage_raster.py --variant V       -- Coverage map: per route best stop per 2 m pixel, summed
04_create_coverage_map.py --variant V   -- Same on vectors (slow; kept as a check, --out)
05_building_values.py --variant gates   -- Best frequency per building
render_poster.py --city C [--dpi] [--crop=lon,lat,w,h]
                                        -- Dark poster (wraps render_qgis_style.py, the standalone
                                           re-implementation of the QGIS layouts)
sketch_access.py --city C --stop REGEX [--recompute] [--flat]
                                        -- Access sketch around a stop (flat: without hills)
compare_coverage.py --city C A.gpkg B.gpkg  -- Pixel comparison of two coverage maps
access_rules.py                         -- Gate / private-way rules
export_layout.py (QGIS python)          -- Exact export of the QGIS layout, project untouched
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
   zone or national grid), `gtfs` feed URL(s) + `gtfs_merge` (`'prefixed'` clips any number of
   feeds, national ones included, to the bbox), `osm_pbf` (Geofabrik region paths),
   `template: 'krakow'` + `render_frame` (map extent in the render CRS; optional `render_crs`
   for a rotated map, `legend_corner`), optionally `'hills': True` + `dem` (`swissalti3d`,
   `usgs13`; then `python fetch_dem.py --city X`), `gtfs_extend_calendar` (stale feeds),
   `{secret:NAME}` in a URL for API keys (read from the gitignored `.secrets/NAME.txt`).
2. **Poster texts**: add an entry to `POSTER` in `script/render_poster.py` (city name,
   language; English template `EN` is there).
3. **Run** everything with one command (hidden, outside the session; milestones in
   `png/log_buildings_final.txt`, step logs in `<data folder>/logs/`):

```bash
python run_city.py --city X --date YYYYMMDD --detach          # a school-term Wednesday
python run_city.py --city X --date YYYYMMDD --final --detach  # + 300 dpi poster into png/
```

   Steps: gtfs (00) -> counts (01) -> osm (make_osm_extract) -> basemap (fetch_osm_basemap via
   the local extract + drop_underground_buildings) -> network (02) -> iso (03 --gates) -> check
   (diagnose_stops) -> coverage (04_coverage_raster, gates + gates_residents) -> buildings (05) -> render (150 dpi
   preview, `png/previews/<city>_poster_150.png`) [-> final (300 dpi)] [-> cleanup].
   Make-like: a step whose output is newer than its inputs is skipped, so a rerun continues where
   it stopped; `--from STEP` forces that step, later steps follow make-like (e.g. `--from iso`
   after changing access rules; `--from gtfs` with a new date redoes counts..render, not the OSM steps), `--to STEP` stops early, `--vector` uses the old vector 04 (check),
   `--cleanup-osm` deletes the city's Geofabrik regional files at the end (the small city extract
   stays for reruns). Regional files are otherwise kept in `D:\QGIS\osm_basemap\pbf\` (~0.2-0.5 GB
   each; `make_osm_extract.py --refresh` downloads newer ones).
   Typical times (2026-10-11, from scratch): Zurich 23 + 8 min, San Francisco 33 + 20 min, New York
   25.7 x 25 km ~1.5 h (preparation + processing; the extract cut is the longest single step).

4. **Check** the `check` milestone (or `<data>/diagnostics/stops_access.csv`) and sketch the
   busiest flagged stops: `diagnose_stops.py --city X <data> --stop "^Name$"` and
   `sketch_access.py --city X --stop "^Name$" --recompute` (access tagging differs between countries).

**Lessons from the cities so far**:
- Per-platform feeds (Swiss, Dutch, MTA N/S) list every platform as a stop; that is fine, coverage
  counts each route once at its best stop.
- Frequency-based feeds (`frequencies.txt`, e.g. AirTrain JFK, parts of PATH) are expanded into
  trips in 00; stale feeds need `gtfs_extend_calendar`.
- Airports and other areas behind private roads (JFK, Oakland Airport): handled by the island walk.
- A city on a coast shared with another Geofabrik region needs that region's coastline (New York /
  New Jersey); check `sea.gpkg` against a few land and water points.
- School holidays: check trip counts on several Wednesdays (Zurich: no visible difference).

## Running on this machine (15 GB RAM)

Run heavy steps one at a time and detached from the terminal session: `run_city.py --detach`
starts the pipeline hidden via WMI, outside Claude Code's process tree (see CLAUDE.md), since
03/04/renders can each take 2–6 GB. All long steps checkpoint or are make-like, so an
interrupted run resumes.

## Directory structure

`3d/` = subproject (2026-10-12): Blender renders of buildings coloured by frequency; plan and
status in `3d/README.md`.
`png/` holds only the current 300 dpi finals (+ the milestone log `log_buildings_final.txt`);
`png/compressed/` = the finals as JPEGs under 20 MB for sharing (`script/compress_finals.py`; full
resolution except Berlin, scaled 0.66) + `collage_8_cities.jpg` (`script/make_collage.py`: 8 finals in two
justified rows, PNG in `png/`);
`png/previews/` = working previews of the city in progress (empty between projects);
`png/archive/how_it_was_made/{sketches,comparisons,style,previews,thumbs,logs}` = diagnostic
sketches, analytical zooms, raster/vector and flat/hills comparisons, style experiments, 150 dpi
previews and run logs (kept for a "how it was made" post); `png/archive/variants/` = superseded
finals and style variants.

```
_data/<city>/YYYY_MM_DD/      -- GTFS data + outputs (isochrones*, coverage_map*, buildings*),
                                 logs/ (one per step), diagnostics/stops_access.csv
network/<city>/               -- Walking network (pickle) + tile cache
D:\QGIS\osm_basemap\<city>\     -- OSM basemap layers (shared across projects)
D:\QGIS\osm_basemap\pbf\        -- Geofabrik regional files + city extracts (<city>_<region>.osm.pbf + .bbox)
D:\QGIS\osm_basemap\dem\        -- terrain models for hilly cities (<city>.tif)
.secrets/                     -- API keys (gitignored)
styles/                       -- QGIS QML styles
png/                          -- Finals; previews/ and archive/ as above
```

## Key concepts

- **trip_count**: Total departures at a stop (6 AM - 10 PM)
- **unique_routes**: Number of distinct transit lines serving a stop
- **route_ids / route_trip_counts**: per-stop route lists enabling deduplication when
  isochrones from nearby stops overlap (per route, the best stop counts once)
- **Vehicle types**: bus, tram, train, metro — classified by city-specific route_id rules

## Data sources

- **GTFS**: [mkuran.pl](https://mkuran.pl/gtfs/) (Polish cities), ZTP Kraków, VBB (Berlin), MTA,
  PATH, NYC Ferry, Port Authority (AirTrain JFK), OVapi (Netherlands), opentransportdata.swiss
  (Switzerland), 511.org (San Francisco Bay Area)
- **Walking network, barriers, gates, basemap**: OpenStreetMap, via Geofabrik extracts (formerly
  the Overpass API)
- **Terrain**: swisstopo swissALTI3D (Zurich), USGS 3DEP 1/3 arc-second (San Francisco)
