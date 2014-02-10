import json
from mrjob.job import MRJob
from shapely.geometry import * 

class MRLocateBorough(MRJob):
	def mapper_init(self):	
		#with open('boroughs.geojson') as f: boroughs = json.loads(f.read())
		with open('census.geojson') as f: census = json.loads(f.read()) #blocks, tracts, counties, state fp codes
		#with open('tracts.geojson') as f: tracts = json.loads(f.read())
		with open('neighborhoods.geojson') as f: nhoods = json.loads(f.read())
		with open('sdistricts.geojson') as f: sdistricts = json.loads(f.read())
		with open('pprecincts.geojson') as f: pprecincts = json.loads(f.read())
		with open('zctas.geojson') as f: zctas = json.loads(f.read())

		#Extract the coordinates. feature keys: [u'type', u'coordinates']
		#self.borough_polygons = [(shape(feature['geometry']), feature['properties']['borough']) for feature in boroughs['features']]
		self.census_polygons = [(shape(feature['geometry']), feature['properties']['STATEFP'], feature['properties']['COUNTYFP'], feature['properties']['TRACTCE'], feature['properties']['BLKGRPCE'])  for feature in census['features']]
		#self.tract_polygons =  [shape(feature['geometry']) for feature in tracts['features']]
		self.nhood_polygons =  [(shape(feature['geometry']), feature['properties']['neighborhood']) for feature in nhoods['features'] ]
		self.sdistrict_polygons =  [(shape(feature['geometry']), feature['properties']['schoolDistrict']) for feature in sdistricts['features'] ]
		self.pprecinct_polygons =  [(shape(feature['geometry']), feature['properties']['policePrecinct']) for feature in pprecincts['features'] ]	
		self.zcta_polygons =  [(shape(feature['geometry']), feature['properties']['postalCode']) for feature in zctas['features'] ]	
		print ",state,county,tract,block_group,neighborhood,school_district,police_precinct,zip,"		

	def mapper(self, key, line):		
		point= line.split(',')[4:6]
	        point = Point(float(point[1]), float(point[0]))	
		'''
		try :	
			temp_borough = (borough[1] for borough in self.borough_polygons if borough[0].contains(point)).next()
		except:
			temp_borough = "NA"
		'''
		try:
			temp_state, temp_boro, temp_tract, temp_blkgrp = ((bg[1], bg[2], bg[3], bg[4]) for bg in self.census_polygons if bg[0].contains(point)).next()
		except:
			temp_state, temp_boro, temp_tract, temp_blkgrp = "NA", "NA","NA", "NA"
		#temp_tract   		= (tract(1) for tract in (self.tract_polygons) if tract[0].contains(point)).next()
		try:
			temp_nhood = (nhood[1] for nhood in self.nhood_polygons if nhood[0].contains(point)).next()
		except:
			temp_nhood = "NA"
		try:
			temp_sdistrict = (district[1] for district in self.sdistrict_polygons if district[0].contains(point)).next()
		except:
			temp_sdistrict = "NA"
		try:
			temp_pprecinct = (precinct[1] for precinct in	self.pprecinct_polygons if precinct[0].contains(point)).next()	
		except:
			temp_pprecinct = "NA"
		try:
			temp_zcta = (zcta[1] for zcta in self.zcta_polygons if zcta[0].contains(point)).next()	
		except:
			temp_zcta = "NA"
		
		line = [temp_state, temp_boro, temp_tract, temp_blkgrp, temp_nhood, temp_sdistrict, temp_pprecinct, temp_zcta]
		line = ','.join(map(str, line))
		line = ","+line+","	
		yield key,line

if __name__ == '__main__':
	 MRLocateBorough.run()
