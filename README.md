DHS
===

DHS Project

The files mr.py and mrlocs.py provide an mrjob implementation to match longitude and latitude to Borough, Neighborhood,School District, Police Precinct, and Zip Code Tabulation Area to the homelessness entries in body.csv. 

Dependencies: mrjob, shapely

Example: python mrlocs.py -r emr ~/example/small_test.csv > out.csv --file boroughs.geojson --file zctas.geojson --file neighborhoods.geojson --file sdistricts.geojson --file pprecincts.geojson

The input is a mrjob command as provided in http://mrjob.readthedocs.org/en/latest/guides/emr-quickstart.html and can be run locally or on EMR. Make sure the config files are set up as per the docs to run on EMR. The command above will create a local file on disk using the given GeoJSON files to find location data. The example output is small_example_output.csv

The output consists of the original input record with concatenated location data.  

Visualization Examples:

http://dhs-visualization.s3-website-us-east-1.amazonaws.com/


