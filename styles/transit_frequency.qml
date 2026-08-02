<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.34" styleCategories="Symbology|Rendering">
  <renderer-v2 type="graduatedSymbol" attr="deduped_trips" graduatedMethod="GraduatedColor" symbollevels="0" forceraster="0" enableorderby="0">
    <ranges>
      <range lower="1" upper="10" symbol="0" label="1 - 10" render="true"/>
      <range lower="10" upper="25" symbol="1" label="10 - 25" render="true"/>
      <range lower="25" upper="50" symbol="2" label="25 - 50" render="true"/>
      <range lower="50" upper="80" symbol="3" label="50 - 80" render="true"/>
      <range lower="80" upper="120" symbol="4" label="80 - 120" render="true"/>
      <range lower="120" upper="170" symbol="5" label="120 - 170" render="true"/>
      <range lower="170" upper="250" symbol="6" label="170 - 250" render="true"/>
      <range lower="250" upper="350" symbol="7" label="250 - 350" render="true"/>
      <range lower="350" upper="450" symbol="8" label="350 - 450" render="true"/>
      <range lower="450" upper="600" symbol="9" label="450 - 600" render="true"/>
      <range lower="600" upper="800" symbol="10" label="600 - 800" render="true"/>
      <range lower="800" upper="1000" symbol="11" label="800 - 1000" render="true"/>
      <range lower="1000" upper="1300" symbol="12" label="1000 - 1300" render="true"/>
      <range lower="1300" upper="1700" symbol="13" label="1300 - 1700" render="true"/>
      <range lower="1700" upper="2200" symbol="14" label="1700 - 2200" render="true"/>
      <range lower="2200" upper="2800" symbol="15" label="2200 - 2800" render="true"/>
      <range lower="2800" upper="3500" symbol="16" label="2800 - 3500" render="true"/>
      <range lower="3500" upper="100000" symbol="17" label="3500+" render="true"/>
    </ranges>
    <symbols>
      <!-- 1-10: very pale icy blue, nearly transparent -->
      <symbol name="0" type="fill" alpha="0.25">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="215,235,250,64"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 10-25: pale sky blue -->
      <symbol name="1" type="fill" alpha="0.30">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="190,220,245,77"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 25-50: light blue -->
      <symbol name="2" type="fill" alpha="0.38">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="150,205,235,97"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 50-80: blue-cyan -->
      <symbol name="3" type="fill" alpha="0.45">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="100,190,215,115"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 80-120: cyan-teal -->
      <symbol name="4" type="fill" alpha="0.50">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="55,180,175,128"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 120-170: teal-green -->
      <symbol name="5" type="fill" alpha="0.55">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="40,170,120,140"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 170-250: green -->
      <symbol name="6" type="fill" alpha="0.60">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="70,175,65,153"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 250-350: yellow-green -->
      <symbol name="7" type="fill" alpha="0.65">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="150,195,40,166"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 350-450: yellow -->
      <symbol name="8" type="fill" alpha="0.70">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="230,210,35,179"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 450-600: gold-orange -->
      <symbol name="9" type="fill" alpha="0.73">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="240,175,30,186"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 600-800: orange -->
      <symbol name="10" type="fill" alpha="0.78">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="235,135,25,199"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 800-1000: dark orange-red -->
      <symbol name="11" type="fill" alpha="0.82">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="220,80,30,209"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1000-1300: red -->
      <symbol name="12" type="fill" alpha="0.85">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="200,40,35,217"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1300-1700: dark red -->
      <symbol name="13" type="fill" alpha="0.88">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="170,25,50,224"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1700-2200: crimson -->
      <symbol name="14" type="fill" alpha="0.90">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="150,18,70,230"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 2200-2800: red-purple -->
      <symbol name="15" type="fill" alpha="0.92">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="130,12,95,235"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 2800-3500: purple -->
      <symbol name="16" type="fill" alpha="0.94">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="105,8,115,240"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 3500+: deep purple -->
      <symbol name="17" type="fill" alpha="0.95">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="75,5,120,242"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
    </symbols>
  </renderer-v2>
  <blendMode>0</blendMode>
</qgis>
