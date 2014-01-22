


from pandas import DataFrame, Series
import MySQLdb
import MySQLdb.cursors
import pandas.io.sql as psql
import re
from bs4 import BeautifulSoup
from sklearn import tree
import StringIO

import pandas as pd
import pydot

from sklearn.cross_validation import cross_val_score
from sklearn import preprocessing as preproc
from sklearn import tree


db=MySQLdb.connect(cursorclass=MySQLdb.cursors.DictCursor,passwd=dbparams['passwd'],db=dbparams['db'],host=dbparams['host'],port=dbparams['port'],user=dbparams['user'])
#Fetching the entire table.
df = psql.frame_query(dbparams['query'], con=db)


cases.index = cases.id; cases = cases.drop('id',axis=1)
xrefs.index = np.array(xrefs.index)-1
warrants.index  np.array(warrants.index)-2
#Join

evictions_with_warrants  = evictions.dropna(subset='warr-type')
#Warrants Dummyn
#evictions['warr-type'] = evictions['warr-type'].map(lambda k: 1 if k == 'W' else 0)

	# initialize model (w/ params)
	clf = tree.DecisionTreeClassifier(
	    min_samples_leaf=50,
	    max_depth=2)

	# initialize & fit preprocesser
	lbz = preproc.LabelBinarizer()
	casetype = list(evictions['case-type201'])
	lbz.fit(casetype)
	casetype = pd.DataFrame(lbz.transform(casetype))
	#
	status = evictions['case--status'].map(lambda k: 1 if k == 'A' else 0)
	status = list(status)
	lbz.fit(status)
	status = pd.DataFrame(lbz.transform(status))
	#
	warrant_issued = evictions['warr-type'].copy()
	#Fit
	clf.fit(status, warrant_issued)

	def create_pdf(clf):
	    """Save dec tree graph as pdf."""
	    dot_data = StringIO.StringIO() 
	    tree.export_graphviz(clf, out_file=dot_data)
	    graph = pydot.graph_from_dot_data(dot_data.getvalue())
	    graph.write_pdf('abalone.pdf')


#Duration of the Stay
def duration_stay(data):
	from dateutil import parser
	#Duration of stay
	end=[]
	for date in data['UNITBED_END_DT']:
		try:
			end.append(parser.parse(date))
		except:
			end.append(nan)

	#Duration of stay
	start=[]
	for date in data['UNITBED_START_DT']:
		try:
			start.append(parser.parse(date))

		except:
			start.append(0)


	duration=[]
	for e,s in zip(end,start):
		try:
			duration.append((e-s).days)
		except:
			duration.append(nan)
	data['duration'] = duration
	return(data)




#Bar Chart - Reason and Year. Filter by top 5 reasons
def reason_and_year_group(data):
	primary_reason = data.groupby('HOMELESSNESS_PRIMARY_REASON').size()
	#Top 5 reasons
	primary_reason.sort()
	ix = primary_reason.index[-5:]
	data['TopReasons'] = data['HOMELESSNESS_PRIMARY_REASON'].map(lambda k: 1 if k in ix else 0 )
	data = data[data['TopReasons']==1]
	#Year
	year = []
	for k,v in data.iterrows():
		try:
			year.append( parser.parse(v['UNITBED_START_DT']).year )
		except:
			year.append( NaN ) 
	data['year'] = year
	#Grouped DF
	gdf = data.groupby(['year','HOMELESSNESS_PRIMARY_REASON']).size().reset_index()
	gdf.columns = ['year','group','value']
	#Pivot the DF
	gdf = gdf.pivot(columns='group', index='year', values='value')
	#Replace NAs with 0
	gdf[pd.isnull(gdf)]=0
	return(gdf)


#Transition - Lon/Lats to be used in D3 transition
def transition_convert(data):
	transition = data.groupby(['entry_lat','entry_lon','exit_lat','exit_lon']).size().reset_index()
	#Rid of \N's
	transition = transition[transition['exit_lat'] != '\N']
	transition['exit_lat'] = transition['exit_lat'].astype('float64')
	transition['exit_lon'] = transition['exit_lon'].astype('float64')

	boundary =((40.45, 40.90), (-74.2,-73))

	#Filter Wacky LonLat's. 
	filter_lat_entry = transition['entry_lat'].map(lambda k: 1 if float(k) < boundary[0][1] and float(k) > boundary[0][0] else 0 )
	#transition['filter_lat_exit'] = transition['exit_lat'].map(lambda k: 1 if float(k) < boundary[0][1] and float(k) > boundary[0][0] else 0 )
	filter_lon_entry = transition['entry_lon'].map(lambda k: 1 if k < boundary[1][1] and k > boundary[1][0] else 0 )
	# transition['filter_lon_exit'] = data_sub['exit_lon'].map(lambda k: 1 if k < boundary[1][1] and k > boundary[1][0] else 0 )
	transition['filtered']  = [ a if a+b == 2 else 0 for a,b in zip(filter_lat_entry, filter_lon_entry)  ]
	transition = transition[transition['filtered']==1]
	del transition['filtered']
	#Round
	# transition['entry_lon'] = transition['entry_lon'].map(lambda k: round(k,3))
	# transition['entry_lat'] = transition['entry_lat'].map(lambda k: round(k,3))
	# transition['exit_lon'] = transition['exit_lon'].map(lambda k: round(k,3))
	# transition['exit_lat'] = transition['exit_lat'].map(lambda k: round(k,3))
	#Swtich Lat Lon
	transition.columns = [u'entry_lat', u'entry_lon', u'exit_lon', u'exit_lat',u'size']
	#Sample for illustration
	#transition = transition.reindex(np.random.permutation(transition.index))[:2000]
	return(transition)

#Stacked Bar - Borough and Days Stayed
#Borough is derived from lat&lon's
def borough_and_months_group(data,survival=False):
	#Month
	data['months'] = data['duration'].map(lambda k: int(k/30))
	#Grouped DF
	gdf = data.groupby(['months','borough']).size().reset_index()
	gdf.columns = ['months','group','value']
	#Pivot the DF
	gdf = gdf.pivot(columns='group', index='months', values='value')
	#Replace NAs with 0
	gdf[pd.isnull(gdf)]=0
	#Percentage
	gdf = gdf/gdf.sum().sum()
	gdf.to_csv('dhs_histogram_data.csv')
	#Survival
	if survival:
		survival = gdf.cumsum().ix[51,:] - gdf.cumsum()
		survival.to_csv('dhs_survival_data.csv')
	return(gdf)




##Running Percentage of Re-entries
def reentries_shelter(data,start):
	df = data[['year','month','FAMILY_ID_rcd']].dropna().sort(['year','month'])
	ix = [datetime.datetime(int(ix[0]),int(ix[1]),1) for ix in zip(*[df.year,df.month])]
	df.index = ix
	df = df.drop(['year','month'],axis=1)

	#Set IX
	six = sorted(set(ix))

	rec = set()
	reentry = []
	for c,i in enumerate(six):
		print i
		#Values for the month
		v = set(df.loc[i].values.flatten())
		#Intersection
		if c > 0:
			reentry.append({'total':len(rec.intersection(v)), 'date' : i})
			#reentry.append({'percentage':len(rec.intersection(v)) / float(len(rec))data, 'date' : i})
		#Tally of historical checkins
		rec.update(v)
	reentry = pd.DataFrame(reentry)
	reentry.index = reentry.date
	reentry = reentry.drop('date',axis=1)
	#Partial String Indexing
	reentry = reentry[start:]
	#Plot
	return(reentry)





##########SHAPELY #############
#Find where the point belongs
from shapely.geometry import Point, Polygon, MultiPolygon, asShape, shape, LinearRing
from shapely import speedups

import json

import pandas as pd
#Enable performance enhancements written in C
speedups.enable()

#Look into to speed up the performance
#http://stackoverflow.com/questions/20297977/looking-for-a-fast-way-to-find-the-polygon-a-point-belongs-to-using-shapely

#Approach Explained here 
#http://www.mhermans.net/geojson-shapely-geocoding.html
#GeoJSON
with open('boroughs.geojson') as f: boroughs = json.loads(f.read())
#Extract the coordinates. feature keys: [u'type', u'coordinates']
#Type is a shapely geom object: Point, Line, Polygon, MultiPolygon etc. 
boroughs_polygons = [shape(feature['geometry']) for feature in boroughs['features'] ]


#Shelter Data
body = pd.read_csv('body.csv')
#Lon-Lat's
lonlat = body[['entry_lon','entry_lat']]
match =[]
for k,line in lonlat.iterrows():
	try:
		p = Point(line['entry_lon'], line['entry_lat'])
		for i, borough in enumerate(boroughs_polygons):
			if borough.contains(p):
				print 'Found containing polygon: {}'.format(i)
				match.append({k:i})
				break
	except:
		print k 

#Join borough codes



		



 

	



























































#MYSQL Commands
	#Enrollments
	#Risk
	#Entrance
	#ADD CONSTRAINT author_id_refs FOREIGN KEY (`author_id`) REFERENCES `author` (`id`);
	#Alter Table 'fy13riskassess' ADD CONSTRAINT 'cares_id' FOREIGN KEY ('CASE_NUMBER_recoded') REFERENCES 'Entrants_Exits' ('CASE_NUMBER_rcd')

	# CREATE TABLE hbenrollments_old(Client_ID_rcd int(11) NOT NULL, 
	# 	N int(11) NOT NULL primary key,
	# 	DIVSUB_ABBREV_STR text DEFAULT NULL,
	# 	INTAKE_DT date,
	# 	STATUS text DEFAULT NULL,
	# 	COMPA int(3) DEFAULT NULL,
	# 	COMPC int(3) DEFAULT NULL,
	# 	ADDRESSNUMBER text DEFAULT NULL,
	# 	ADDRESS text DEFAULT NULL,
	# 	APT text DEFAULT NULL,
	# 	BOROUGH text DEFAULT NULL,
	# 	ZIP int(5) DEFAULT NULL,
	# 	GEOX int(11) DEFAULT NULL,
	# 	GEOY int(11) DEFAULT NULL,
	# 	FWC_UBA_AFTER_HB_ENROLL int(5) DEFAULT NULL);

	# CREATE TABLE hbenrollments_new(
	# 	CARES_ID_rcd int(11) NOT NULL, 
	# 	N int(11) NOT NULL primary key,
	# 	ENROLLTYPE text DEFAULT NULL,
	# 	FAMTYPE text DEFAULT NULL,
	# 	RAQSCORE int(11) DEFAULT NULL,
	# 	CASETYPE text DEFAULT NULL,
	# 	DETERMINATION text DEFAULT NULL,	
	# 	STARTDATE date,
	# 	DAYS_IN int(11) DEFAULT NULL,
	# 	CASE_STATUS text DEFAULT NULL,
	# 	ADULTS int(11) DEFAULT NULL,
	# 	CHILDREN int(11) DEFAULT NULL,
	# 	ADDRESS text DEFAULT NULL,
	# 	ZIP int(5) DEFAULT NULL,
	# 	BORO int(5) DEFAULT NULL,
	# 	RANKING int(5) DEFAULT NULL,
	# 	FWC_UBA_AFTER_HB_ENROLL int(5) DEFAULT NULL);


	# LOAD DATA LOCAL INFILE
	# '~/Dropbox/data/DHS/HomebaseEnrollments/HBenrollments2013.csv' INTO TABLE hbenrollments_new
	# FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r' IGNORE 1 LINES
	# (CARES_ID_rcd, N, ENROLLTYPE, FAMTYPE, RAQSCORE, CASETYPE, DETERMINATION, STARTDATE, DAYS_IN, CASE_STATUS, ADULTS, CHILDREN, ADDRESS, ZIP, BORO, RANKING, FWC_UBA_AFTER_HB_ENROLL);

	# LOAD DATA LOCAL INFILE
	# '~/Dropbox/data/DHS/HomebaseEnrollments/HBenrollments_2004_2012.csv' INTO TABLE hbenrollments_old
	# FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r' IGNORE 1 LINES
	# (Client_ID_rcd, N, DIVSUB_ABBREV_STR, INTAKE_DT, STATUS, COMPA, COMPC, ADDRESSNUMBER, ADDRESS, APT, BOROUGH, ZIP, GEOX, GEOY, FWC_UBA_AFTER_HB_ENROLL);
