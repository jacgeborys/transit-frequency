"""Generate dark-mode QML styles for all QGIS layers."""
from pathlib import Path

DARK_DIR = Path(__file__).resolve().parent.parent / "styles" / "dark"
DARK_DIR.mkdir(parents=True, exist_ok=True)


def simple_fill_qml(color, outline_color="0,0,0,0", outline_style="no",
                     alpha="1", opacity=None):
    opacity_tag = ""
    if opacity:
        opacity_tag = f"\n  <layerOpacity>{opacity}</layerOpacity>"
    return (
        '<!DOCTYPE qgis PUBLIC \'http://mrcc.com/qgis.dtd\' \'SYSTEM\'>\n'
        '<qgis version="3.34" styleCategories="Symbology|Rendering">\n'
        '  <renderer-v2 type="singleSymbol" symbollevels="0" forceraster="0" enableorderby="0">\n'
        '    <symbols>\n'
        f'      <symbol name="0" type="fill" alpha="{alpha}">\n'
        '        <layer class="SimpleFill">\n'
        '          <Option type="Map">\n'
        f'            <Option name="color" type="QString" value="{color}"/>\n'
        f'            <Option name="outline_color" type="QString" value="{outline_color}"/>\n'
        f'            <Option name="outline_style" type="QString" value="{outline_style}"/>\n'
        '            <Option name="style" type="QString" value="solid"/>\n'
        '          </Option>\n'
        '        </layer>\n'
        '      </symbol>\n'
        '    </symbols>\n'
        '  </renderer-v2>\n'
        '  <blendMode>0</blendMode>'
        f'{opacity_tag}\n'
        '</qgis>\n'
    )


# --- Green areas: pale green -> deep forest green ---
green_layers = {
    "allotments": ("20,45,20,255", "0.5"),
    "grass": ("20,45,20,255", "0.5"),
    "meadow": ("20,45,20,255", "0.5"),
    "leisure": ("20,45,20,255", "0.5"),
    "leisure_relations": ("20,45,20,255", "0.5"),
    "cemeteries": ("20,45,20,255", "0.5"),
    "parks": ("15,40,15,255", "0.7"),
    "forests": ("10,35,10,255", "0.7"),
}
for name, (color, alpha) in green_layers.items():
    (DARK_DIR / f"{name}.qml").write_text(simple_fill_qml(color, alpha=alpha))
    print(f"  {name}.qml")

# --- Water: pale blue -> deep navy ---
(DARK_DIR / "water.qml").write_text(
    simple_fill_qml("10,18,35,255", outline_color="8,15,30,255", outline_style="no"))
print("  water.qml")

(DARK_DIR / "water_copy.qml").write_text(
    simple_fill_qml("10,18,35,255", outline_color="15,25,45,255", outline_style="solid"))
print("  water_copy.qml")

# --- Buildings: mid grey -> near-black ---
(DARK_DIR / "buildings.qml").write_text(
    simple_fill_qml("28,28,28,255", outline_style="no"))
print("  buildings.qml")

(DARK_DIR / "buildings_copy.qml").write_text(
    simple_fill_qml("35,35,35,255", outline_style="no", opacity="0.17"))
print("  buildings_copy.qml")

# --- Railways: warm grey -> dark warm grey ---
rail_qml = (
    '<!DOCTYPE qgis PUBLIC \'http://mrcc.com/qgis.dtd\' \'SYSTEM\'>\n'
    '<qgis version="3.34" styleCategories="Symbology|Rendering">\n'
    '  <renderer-v2 type="singleSymbol" symbollevels="0" forceraster="0" enableorderby="0">\n'
    '    <symbols>\n'
    '      <symbol name="0" type="line" alpha="1">\n'
    '        <layer class="SimpleLine">\n'
    '          <Option type="Map">\n'
    '            <Option name="line_color" type="QString" value="45,40,35,179"/>\n'
    '            <Option name="line_width" type="QString" value="0.26"/>\n'
    '            <Option name="line_style" type="QString" value="solid"/>\n'
    '            <Option name="capstyle" type="QString" value="square"/>\n'
    '            <Option name="joinstyle" type="QString" value="bevel"/>\n'
    '          </Option>\n'
    '        </layer>\n'
    '      </symbol>\n'
    '    </symbols>\n'
    '  </renderer-v2>\n'
    '  <blendMode>0</blendMode>\n'
    '  <layerOpacity>0.7</layerOpacity>\n'
    '</qgis>\n'
)
(DARK_DIR / "railways.qml").write_text(rail_qml)
print("  railways.qml")

# --- Roads (categorized by highway): white lines -> dark grey lines ---
road_categories = [
    ("dojazdowa", "50,50,50,255", "2"),
    ("ekspresowa", "40,40,40,255", "1"),
    ("glownaRuchuPrzyspieszonego", "40,40,40,255", "0.7"),
    ("glowna", "40,40,40,255", "0.5"),
    ("zbiorcza", "40,40,40,255", "0.3"),
    ("inna", "50,50,50,255", "2"),
    ("lokalna", "50,50,50,255", "2"),
    ("living_street", "40,40,40,255", "0.2"),
    ("motorway", "40,40,40,255", "1"),
    ("motorway_link", "40,40,40,255", "0.4"),
    ("primary", "40,40,40,255", "0.7"),
    ("primary_link", "40,40,40,255", "0.4"),
    ("residential", "40,40,40,255", "0.15"),
    ("secondary", "40,40,40,255", "0.5"),
    ("secondary_link", "40,40,40,255", "0.4"),
    ("tertiary", "40,40,40,255", "0.4"),
    ("tertiary_link", "40,40,40,255", "0.2"),
    ("trunk", "40,40,40,255", "0.7"),
    ("trunk_link", "40,40,40,255", "0.4"),
    ("unclassified", "30,60,75,255", "0.15"),
    ("", "20,70,60,255", "0.15"),
]

symbols_parts = []
categories_parts = []
for i, (value, color, width) in enumerate(road_categories):
    symbols_parts.append(
        f'      <symbol name="{i}" type="line" alpha="1">\n'
        f'        <layer class="SimpleLine">\n'
        f'          <Option type="Map">\n'
        f'            <Option name="line_color" type="QString" value="{color}"/>\n'
        f'            <Option name="line_width" type="QString" value="{width}"/>\n'
        f'            <Option name="line_style" type="QString" value="solid"/>\n'
        f'            <Option name="capstyle" type="QString" value="square"/>\n'
        f'            <Option name="joinstyle" type="QString" value="bevel"/>\n'
        f'          </Option>\n'
        f'        </layer>\n'
        f'      </symbol>'
    )
    categories_parts.append(
        f'      <category value="{value}" symbol="{i}" label="{value}" render="true"/>'
    )

roads_qml = (
    '<!DOCTYPE qgis PUBLIC \'http://mrcc.com/qgis.dtd\' \'SYSTEM\'>\n'
    '<qgis version="3.34" styleCategories="Symbology|Rendering">\n'
    '  <renderer-v2 type="categorizedSymbol" attr="highway" symbollevels="0"'
    ' forceraster="0" enableorderby="0">\n'
    '    <categories>\n'
    + "\n".join(categories_parts) + "\n"
    '    </categories>\n'
    '    <symbols>\n'
    + "\n".join(symbols_parts) + "\n"
    '    </symbols>\n'
    '  </renderer-v2>\n'
    '  <blendMode>0</blendMode>\n'
    '  <layerOpacity>0.7</layerOpacity>\n'
    '</qgis>\n'
)
(DARK_DIR / "roads.qml").write_text(roads_qml)
print("  roads.qml")

# --- Coverage map: BMY non-reversed (deep blue -> bright yellow for dark bg) ---
bmy_colors = [
    (0,12,124), (0,18,149), (0,21,166), (66,18,166),
    (116,10,152), (149,3,143), (178,0,136), (205,0,129),
    (230,8,121), (249,40,112), (255,71,100), (255,103,87),
    (255,132,71), (255,158,51), (255,181,35), (255,201,28),
    (255,221,29), (255,241,35),
]
breaks = [
    (1,10), (10,25), (25,50), (50,80), (80,120), (120,170),
    (170,250), (250,350), (350,450), (450,600), (600,800), (800,1000),
    (1000,1300), (1300,1700), (1700,2200), (2200,2800), (2800,3500), (3500,100000),
]

ranges_parts = []
sym_parts = []
for i, ((lo, hi), (r, g, b)) in enumerate(zip(breaks, bmy_colors)):
    label = f"{lo} - {hi}" if hi < 100000 else f"{lo}+"
    ranges_parts.append(
        f'      <range lower="{lo}" upper="{hi}" symbol="{i}" label="{label}" render="true"/>'
    )
    sym_parts.append(
        f'      <symbol name="{i}" type="fill" alpha="1.0">\n'
        f'        <layer class="SimpleFill">\n'
        f'          <Option type="Map">\n'
        f'            <Option name="color" type="QString" value="{r},{g},{b},255"/>\n'
        f'            <Option name="outline_color" type="QString" value="0,0,0,0"/>\n'
        f'            <Option name="outline_style" type="QString" value="no"/>\n'
        f'            <Option name="style" type="QString" value="solid"/>\n'
        f'          </Option>\n'
        f'        </layer>\n'
        f'      </symbol>'
    )

coverage_qml = (
    '<!DOCTYPE qgis PUBLIC \'http://mrcc.com/qgis.dtd\' \'SYSTEM\'>\n'
    '<qgis version="3.34" styleCategories="Symbology|Rendering">\n'
    '  <renderer-v2 type="graduatedSymbol" attr="deduped_trips"'
    ' graduatedMethod="GraduatedColor" symbollevels="0" forceraster="0" enableorderby="0">\n'
    '    <ranges>\n'
    + "\n".join(ranges_parts) + "\n"
    '    </ranges>\n'
    '    <symbols>\n'
    '      <!-- Kovesi BMY for dark mode: deep blue > magenta > bright yellow.\n'
    '           Highest frequency glows brightest on dark background. -->\n'
    + "\n".join(sym_parts) + "\n"
    '    </symbols>\n'
    '  </renderer-v2>\n'
    '  <blendMode>0</blendMode>\n'
    '  <layerOpacity>0.9</layerOpacity>\n'
    '</qgis>\n'
)
(DARK_DIR / "coverage_map.qml").write_text(coverage_qml)
print("  coverage_map.qml")

print(f"\nDone! {len(list(DARK_DIR.glob('*.qml')))} dark-mode styles in styles/dark/")
