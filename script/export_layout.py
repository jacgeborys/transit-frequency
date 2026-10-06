"""
Export a QGIS print layout from transit-frequency-map.qgz with a different
coverage map plugged in — headless, via PyQGIS. The project file itself is
never modified (it is loaded read-only in memory).

Must run under QGIS's Python:
    "C:\\Program Files\\QGIS 3.28.3\\bin\\python-qgis.bat" script/export_layout.py \\
        --city warsaw --coverage _data/warsaw/2026_10_04/coverage_map.gpkg \\
        --date 07.10.2026 --out png/how_many_rides_in_5_mins_2026_10_07.png

The city's layer-tree group is made the only visible one, all coverage_map
layers inside it are repointed to --coverage (styles are kept), and the
"stan na DD.MM.YYYY" title is updated.
"""
import argparse
import re
import sys
from pathlib import Path

from qgis.core import (QgsApplication, QgsProject, QgsLayoutExporter,
                       QgsLayoutItemLabel, QgsLayerTreeGroup, QgsLayerTreeLayer)

PROJECT_DIR = Path(__file__).resolve().parent.parent
PROJECT_FILE = PROJECT_DIR / "transit-frequency-map.qgz"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cities import CITIES  # noqa: E402  (plain-Python module, safe under QGIS's interpreter)

CITY_LAYOUTS = {k: (c['qgis_group'], c['qgis_layout']) for k, c in CITIES.items() if 'qgis_group' in c}


def main():
    parser = argparse.ArgumentParser(description='Export a city layout to PNG')
    parser.add_argument('--city', required=True, choices=sorted(CITY_LAYOUTS))
    parser.add_argument('--coverage', required=True, help='coverage_map*.gpkg to show')
    parser.add_argument('--date', required=True, help='Date for the title, DD.MM.YYYY')
    parser.add_argument('--out', required=True, help='Output .png (or .pdf)')
    parser.add_argument('--dpi', type=float, default=None, help='Override layout DPI')
    args = parser.parse_args()

    group_name, layout_name = CITY_LAYOUTS[args.city]
    coverage = Path(args.coverage).resolve()
    out = Path(args.out).resolve()

    qgs = QgsApplication([], False)
    qgs.initQgis()
    project = QgsProject.instance()
    if not project.read(str(PROJECT_FILE)):
        sys.exit(f"Cannot read {PROJECT_FILE}")

    root = project.layerTreeRoot()
    tlo = root.findGroup('Tlo')
    for child in tlo.children():
        if isinstance(child, QgsLayerTreeGroup):
            child.setItemVisibilityChecked(child.name() == group_name)
    group = tlo.findGroup(group_name)

    repointed = 0
    for node in group.findLayers():
        layer = node.layer()
        if layer and 'coverage_map' in layer.source():
            layer.setDataSource(str(coverage), layer.name(), "ogr")  # single-layer gpkg
            repointed += 1
    print(f"Repointed {repointed} coverage layer(s) -> {coverage}")

    layout = project.layoutManager().layoutByName(layout_name)
    if layout is None:
        sys.exit(f"Layout {layout_name} not found")
    for item in layout.items():
        if isinstance(item, QgsLayoutItemLabel) and 'stan na' in item.text():
            item.setText(re.sub(r'stan na [0-9.]+', f'stan na {args.date}', item.text()))

    exporter = QgsLayoutExporter(layout)
    if out.suffix.lower() == '.pdf':
        settings = QgsLayoutExporter.PdfExportSettings()
        if args.dpi:
            settings.dpi = args.dpi
        result = exporter.exportToPdf(str(out), settings)
    else:
        settings = QgsLayoutExporter.ImageExportSettings()
        if args.dpi:
            settings.dpi = args.dpi
        result = exporter.exportToImage(str(out), settings)
    print("Exported" if result == QgsLayoutExporter.Success else f"Export failed ({result})", out)
    qgs.exitQgis()


if __name__ == '__main__':
    main()
