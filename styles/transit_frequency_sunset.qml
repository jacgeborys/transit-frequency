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
      <!-- CartoColors "Sunset": warm yellow > peach > coral > pink > magenta > purple.
           Alpha ramp 0.25-0.95 for additional light-to-dark graduation. -->

      <!-- 1-10 -->
      <symbol name="0" type="fill" alpha="0.25">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="243,231,155,64"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 10-25 -->
      <symbol name="1" type="fill" alpha="0.30">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="245,218,146,77"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 25-50 -->
      <symbol name="2" type="fill" alpha="0.36">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="247,206,138,92"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 50-80 -->
      <symbol name="3" type="fill" alpha="0.42">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="249,193,131,107"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 80-120 -->
      <symbol name="4" type="fill" alpha="0.48">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="249,181,129,122"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 120-170 -->
      <symbol name="5" type="fill" alpha="0.54">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="248,168,127,138"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 170-250 -->
      <symbol name="6" type="fill" alpha="0.60">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="246,156,126,153"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 250-350 -->
      <symbol name="7" type="fill" alpha="0.65">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="241,144,129,166"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 350-450 -->
      <symbol name="8" type="fill" alpha="0.70">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="237,132,132,179"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 450-600 -->
      <symbol name="9" type="fill" alpha="0.74">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="229,122,136,189"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 600-800 -->
      <symbol name="10" type="fill" alpha="0.78">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="219,113,140,199"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 800-1000 -->
      <symbol name="11" type="fill" alpha="0.82">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="209,104,145,209"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1000-1300 -->
      <symbol name="12" type="fill" alpha="0.85">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="195,98,150,217"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1300-1700 -->
      <symbol name="13" type="fill" alpha="0.87">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="178,94,154,222"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 1700-2200 -->
      <symbol name="14" type="fill" alpha="0.89">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="162,89,159,227"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 2200-2800 -->
      <symbol name="15" type="fill" alpha="0.91">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="139,87,161,232"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 2800-3500 -->
      <symbol name="16" type="fill" alpha="0.93">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="115,85,163,237"/>
            <Option name="outline_color" type="QString" value="0,0,0,0"/>
            <Option name="outline_style" type="QString" value="no"/>
            <Option name="style" type="QString" value="solid"/>
          </Option>
        </layer>
      </symbol>
      <!-- 3500+ -->
      <symbol name="17" type="fill" alpha="0.95">
        <layer class="SimpleFill">
          <Option type="Map">
            <Option name="color" type="QString" value="92,83,165,242"/>
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
