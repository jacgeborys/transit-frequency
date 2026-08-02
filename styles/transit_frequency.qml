<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.34" styleCategories="Symbology|Rendering">
  <renderer-v2 type="graduatedSymbol" attr="deduped_trips" graduatedMethod="GraduatedColor" symbollevels="0" forceraster="0" enableorderby="0">
    <ranges>
      <range lower="1" upper="20" symbol="0" label="1 - 20" render="true"/>
      <range lower="20" upper="50" symbol="1" label="20 - 50" render="true"/>
      <range lower="50" upper="100" symbol="2" label="50 - 100" render="true"/>
      <range lower="100" upper="200" symbol="3" label="100 - 200" render="true"/>
      <range lower="200" upper="350" symbol="4" label="200 - 350" render="true"/>
      <range lower="350" upper="500" symbol="5" label="350 - 500" render="true"/>
      <range lower="500" upper="750" symbol="6" label="500 - 750" render="true"/>
      <range lower="750" upper="1000" symbol="7" label="750 - 1000" render="true"/>
      <range lower="1000" upper="1500" symbol="8" label="1000 - 1500" render="true"/>
      <range lower="1500" upper="2000" symbol="9" label="1500 - 2000" render="true"/>
      <range lower="2000" upper="3000" symbol="10" label="2000 - 3000" render="true"/>
      <range lower="3000" upper="100000" symbol="11" label="3000+" render="true"/>
    </ranges>
    <symbols>
      <!-- 1-20: pale blue, very transparent -->
      <symbol name="0" type="fill" alpha="0.35">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="190,220,240,89"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 20-50: steel blue -->
      <symbol name="1" type="fill" alpha="0.45">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="120,180,220,115"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 50-100: teal -->
      <symbol name="2" type="fill" alpha="0.55">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="60,170,180,140"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 100-200: green -->
      <symbol name="3" type="fill" alpha="0.60">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="50,165,90,153"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 200-350: yellow-green -->
      <symbol name="4" type="fill" alpha="0.65">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="160,200,50,166"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 350-500: yellow -->
      <symbol name="5" type="fill" alpha="0.70">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="240,210,40,179"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 500-750: orange -->
      <symbol name="6" type="fill" alpha="0.75">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="240,160,30,191"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 750-1000: dark orange -->
      <symbol name="7" type="fill" alpha="0.80">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="230,110,25,204"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1000-1500: red -->
      <symbol name="8" type="fill" alpha="0.85">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="210,50,35,217"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1500-2000: dark red -->
      <symbol name="9" type="fill" alpha="0.88">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="170,30,50,224"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 2000-3000: crimson-purple -->
      <symbol name="10" type="fill" alpha="0.92">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="140,20,90,235"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 3000+: deep purple -->
      <symbol name="11" type="fill" alpha="0.95">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="100,10,110,242"/>
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
