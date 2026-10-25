"""
Jobs reachable by public transport from every built-up 250 m cell of the Warsaw poster area
(run with D:/QGIS/ceny_mieszkan/access/.venv: r5py + its portable Java 21).

Same routing as ceny_mieszkan/access/travel_times.py (read only: its network data, destinations and settings are
reused): OSM walking network + Warsaw GTFS, walk + transit, median travel time over every departure minute of the
window, max 60 min. Only the origins differ: every 250 m cell holding a building of the TransitFrequency basemap
(D:/QGIS/osm_basemap/warsaw/buildings.gpkg), routed from the median position of its buildings, instead of only
the cells with priced flats.

Per cell (cell id = (x // 250) * 100000 + y // 250 in EPSG:2180):
  jobs_30, jobs_45   job equivalents within 30 / 45 min;  jobs_grav  sum of jobs * exp(-t / 15 min)

    D:/QGIS/ceny_mieszkan/access/.venv/Scripts/python.exe jobs/travel_times_all.py [--scenario am_peak]
    -> jobs/data/access_<scenario>_all.csv  (checkpoint: access_<scenario>_all_cells.csv)
"""
import argparse
import os
import sys

ACCESS = "D:/QGIS/ceny_mieszkan/access"
if "--max-memory" not in sys.argv:
    sys.argv += ["--max-memory", "6G"]   # leaves room on the 15 GB machine
sys.path.insert(0, ACCESS)
sys.dont_write_bytecode = True
from travel_times import DATA, MAX_MIN, SCENARIOS, network  # noqa: E402  (sets JAVA_HOME first)

import datetime as dt  # noqa: E402

import geopandas as gpd  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data")
BUILDINGS = "D:/QGIS/osm_basemap/warsaw/buildings.gpkg"
EXCLUDED_BUILDING_TYPES = {"roof", "carport", "service"}
CELL = 250
CHUNK = 500


def origin_cells():
    b = gpd.read_file(BUILDINGS)
    b = b[~b["building"].isin(EXCLUDED_BUILDING_TYPES) & b.geometry.notna()].to_crs(2180)
    p = b.geometry.representative_point()
    cell = (p.x // CELL).astype(int) * 100000 + (p.y // CELL).astype(int)
    xy = pd.DataFrame({"cell": cell, "x": p.x, "y": p.y}).groupby("cell")[["x", "y"]].median()
    return gpd.GeoDataFrame({"id": xy.index.to_numpy()}, geometry=gpd.points_from_xy(xy["x"], xy["y"]),
                            crs=2180).to_crs(4326)


def main():
    import r5py
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=list(SCENARIOS), default="am_peak")
    args, _ = ap.parse_known_args()
    os.makedirs(OUT, exist_ok=True)
    final = os.path.join(OUT, f"access_{args.scenario}_all.csv")
    partial = os.path.join(OUT, f"access_{args.scenario}_all_cells.csv")
    cells = origin_cells()
    dest = gpd.read_file(os.path.join(DATA, "destinations.gpkg"))
    jobs = dest.set_index("id")["jobs"]
    print(f"{len(cells):,} origin cells x {len(dest):,} destinations", flush=True)
    rows = []
    if os.path.exists(partial):
        prev = pd.read_csv(partial, index_col=0)
        rows.append(prev)
        cells = cells[~cells["id"].isin(prev.index)]
        print(f"resuming: {len(prev):,} cells done, {len(cells):,} to go", flush=True)
    net = network()
    dep, win = SCENARIOS[args.scenario]
    t0 = dt.datetime.now()
    for c0 in range(0, len(cells), CHUNK):
        o = cells.iloc[c0:c0 + CHUNK]
        tt = r5py.TravelTimeMatrix(net, origins=o, destinations=dest, departure=dep,
                                   departure_time_window=win,
                                   transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
                                   max_time=dt.timedelta(minutes=MAX_MIN), percentiles=[50])
        tt = pd.DataFrame(tt).dropna(subset=["travel_time"])
        t = tt["travel_time"].to_numpy(float)
        j = tt["to_id"].map(jobs).fillna(0).to_numpy(float)
        tt = tt.assign(j30=j * (t <= 30), j45=j * (t <= 45), grav=j * np.exp(-t / 15))
        r = tt.groupby("from_id")[["j30", "j45", "grav"]].sum()
        r.columns = ["jobs_30", "jobs_45", "jobs_grav"]
        r = r.reindex(o["id"].to_numpy()).fillna(0)   # cells reaching nothing within 60 min: 0
        r.index.name = "from_id"
        rows.append(r)
        pd.concat(rows).to_csv(partial)
        done = min(c0 + CHUNK, len(cells))
        el = (dt.datetime.now() - t0).total_seconds()
        print(f"  {done:,}/{len(cells):,} cells, {el / 60:.0f} min, ~{el / done * (len(cells) - done) / 60:.0f} min left",
              flush=True)
    res = pd.concat(rows)
    res.to_csv(final)
    print(f"Saved {final}: {len(res):,} cells, median jobs within 30 min {res['jobs_30'].median():,.0f}")


if __name__ == "__main__":
    sys.exit(main())
