import json
from mrjob.job import MRJob
from shapely.geometry import * #Point, Polygon, MultiPolygon, shape, LinearRing

class MRLocateBorough(MRJob):
	def mapper_init(self):	
		with open('boroughs.geojson') as f: boroughs = json.loads(f.read())
		#Extract the coordinates. feature keys: [u'type', u'coordinates']
		self.borough_polygons = [shape(feature['geometry']) for feature in boroughs['features'] ]

	def mapper(self, _, line):	
		(point,prior_borough)= (line.split(',')[4:6], line.split(',')[25])
	        point = Point(float(point[1]), float(point[0]))	
		try:
			for i,borough in enumerate(self.borough_polygons):
				if borough.contains(point):
					yield  i, prior_borough
		except:
			yield "NA"

#	def reducer(self, key, values):
#        	yield key, sum(values)

if __name__ == '__main__':
	 MRLocateBorough.run()
