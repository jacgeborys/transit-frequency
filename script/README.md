Run codes in their numbered order.
01 - runs in PyQGIS - fetches the stops (or anything else if you change Overpass API query). It requires a polygon layer (frames) with the area of interest in its extent. Fill the City_Name column with the name of the area that'll be appended to the name of the fetched layer.
02 - runs in PyQGIS (optional) - generates centroids for stops with the same name, so that GraphHopper API is not overloaded.
03 - can be run in PyCharm - calculates isochrones. Requires GraphHopper API key.