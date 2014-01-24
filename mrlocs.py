import json
from mrjob.job import MRJob
from shapely.geometry import * 

class MRLocateBorough(MRJob):
	def mapper_init(self):	
		with open('boroughs.geojson') as f: boroughs = json.loads(f.read())
		#with open('blocks.geojson') as f: blocks = json.loads(f.read())
		#with open('tracts.geojson') as f: tracts = json.loads(f.read())
		with open('neighborhoods.geojson') as f: neighborhoods = json.loads(f.read())
		with open('sdistricts.geojson') as f: sdistricts = json.loads(f.read())
		with open('pprecincts.geojson') as f: pprecincts = json.loads(f.read())
		with open('zctas.geojson') as f: zctas = json.loads(f.read())

		#Extract the coordinates. feature keys: [u'type', u'coordinates']
		self.borough_polygons = [(shape(feature['geometry']), feature['properties']['borough']) for feature in boroughs['features'] ]
		#self.block_polygons =  [(shape(feature['geometry']), feature['properties'][] for feature in blocks['features'] ]
		#self.tract_polygons =  [shape(feature['geometry']) for feature in tracts['features'] ]
		self.neighborhood_polygons =  [(shape(feature['geometry']), feature['properties']['neighborhood']) for feature in neighborhoods['features'] ]
		self.sdistrict_polygons =  [(shape(feature['geometry']), feature['properties']['schoolDistrict']) for feature in sdistricts['features'] ]
		self.pprecinct_polygons =  [(shape(feature['geometry']), feature['properties']['policePrecinct']) for feature in pprecincts['features'] ]	
		self.zcta_polygons =  [(shape(feature['geometry']), feature['properties']['postalCode']) for feature in zctas['features'] ]	

	def mapper(self, key, line):		
		point= line.split(',')[4:6]
	        point = Point(float(point[1]), float(point[0]))	
	
		temp_borough		= (borough[1] for borough in self.borough_polygons if borough[0].contains(point)).next()
		#temp_block   		= (block(1) for block in self.block_polygons) if block[0].contains(point)).next()
		#temp_tract   		= (tract(1) for tract in (self.tract_polygons) if tract[0].contains(point)).next()
		temp_neighborhood 	= (neighborhood[1] for neighborhood in self.neighborhood_polygons if neighborhood[0].contains(point)).next()
		temp_sdistrict 		= (district[1] for district in self.sdistrict_polygons if district[0].contains(point)).next()
		temp_pprecinct 		= (precinct[1] for precinct in	self.pprecinct_polygons if precinct[0].contains(point)).next()	
		temp_zcta 		= (zcta[1] for zcta in self.zcta_polygons if zcta[0].contains(point)).next()	
	
#		print temp_borough,temp_neighborhood, temp_sdistrict, temp_pprecinct, temp_zcta	
		line = [line, temp_borough, temp_neighborhood, temp_sdistrict, temp_pprecinct, temp_zcta]
		#line = ','.join(map(str, line))		
		yield key,line

if __name__ == '__main__':
	 MRLocateBorough.run()
