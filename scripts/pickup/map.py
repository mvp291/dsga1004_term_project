#!/usr/bin/python

"""
    map.py Get raw data from yellow and green NYC Taxis and
    1 - Translate pickup latitude longitude to borough
    2 - Sort green taxi data columns to match yellow taxi data
    3 - Include indicator of source
      Y: yellow taxi data
      G: green taxi data

    Author: Ma. Elena Villalobos Ponte
"""

import sys
import json
from shapely.geometry import shape, Point, Polygon

# Read borough polygons from file and create Shapely Polygons
max_size = 200000000

b_file = open('borough_polygons.json', 'r')
b_string = b_file.read(max_size)
borough_json = json.loads(b_string)
b_file.close()

borough_polygons = {}

correct = False
for borough in borough_json['features']:
  poly = shape(borough['geometry'])
  borough_name = borough['properties']['borough']
  borough_polygons[borough['id']] = {'name': borough_name, 'polygon': poly}

# Map each trip with the corresponding borough or none if there is no match

# Preferred order for borough polygon checking
# Manhattan, Queens, Brooklyn, Staten Island, Bronx
borough_ids = range(45, 74) + range(4, 24) + range(24, 44) + range(0, 4) + range(74, 104)

# Indexes of pickup latitude and longitude
lat_ix = 5
long_ix = 6
for line in sys.stdin:
  clean_line = line.replace('\r', '').replace('\n', '')
  splitted = clean_line.split(',')

  try:
    longitude, latitude = float(splitted[lat_ix]), float(splitted[long_ix])
  except: # header line, ignore it
    continue

  if len(splitted) == 19: # yellow taxi data
    col_order = range(0, 19)
    color = 'Y'
  else: # green taxi data
    col_order = [0, 1, 2, 9, 10, 5, 6, 4, 3, 7, 8, 19, 11, 12, 13, 17, 14, 15, 18]
    color = 'G'

  original_data = [splitted[i] for i in col_order]
  output_data = ','.join(original_data)

  for i in borough_ids:
    coordinate = Point(longitude, latitude)
    correct = borough_polygons[i]['polygon'].contains(coordinate)
    if correct:
      print "{0:s}\t{1:.9f},{2:.9f},{3:s},{4:s}".format(borough_polygons[i]['name'], latitude, longitude, output_data, color)
      break

  if not correct:
    print "{0:s}\t{1:.9f},{2:.9f},{3:s},{4:s}".format('None', latitude, longitude, output_data, color)

