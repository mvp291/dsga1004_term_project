from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import datetime
import pytz
from pytz import timezone

sc = SparkContext(appName="BuildDataFrame")
SQLContext = SQLContext(sc)

float_ix = [4, 5, 6, 9, 10, 12, 13, 14, 15, 16, 17, 18, 30, 33, 34, 35, 36, 37,
            38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70]

int_ix = [0, 3, 21, 23, 24, 25, 26, 27, 28, 29]

def str_to_datetime(str_dt):
  unaware_est = datetime.datetime.strptime(str_dt,"%Y-%m-%d %H:%M:%S")
  localtz = timezone('US/Eastern')
  aware_est = localtz.localize(unaware_est)
  return aware_est

def transform_features(feature_list):
  #pickup_datetime
  feature_list[1] = str_to_datetime(feature_list[1])
  feature_list[2] = str_to_datetime(feature_list[2])

  for i in float_ix:
    try:
      feature_list[i] = float(i)
    except TypeError:
      feature_list[i] = None

  for i in int_ix:
    try:
      feature_list[i] = int(i)
    except TypeError:
      feature_list[i] = None

  return tuple(feature_list)

# Load a text file and convert each line to a tuple.
# TODO filter  header
raw_lines = sc.textFile('/Users/mati/Devel/NYU/dsga1004_term_project/data/final_march/test')
lines = raw_lines.filter(lambda l: 'VendorID' not in l)
parts = lines.map(lambda l: l.split(",")[:24] + l.split(",")[25:])
trips = parts.map(transform_features)

schema = StructType([
  StructField("VendorID", IntegerType()),
  StructField("pickup_datetime", DateType()),
  StructField("dropoff_datetime", DateType()),
  StructField("passenger_count", IntegerType()),
  StructField("trip_distance", FloatType()),
  StructField("pickup_longitude", FloatType()),
  StructField("pickup_latitude", FloatType()),
  StructField("RateCodeID", StringType()),
  StructField("store_and_fwd_flag", StringType()),
  StructField("dropoff_longitude", FloatType()),
  StructField("dropoff_latitude", FloatType()),
  StructField("payment_type", StringType()),
  StructField("fare_amount", FloatType()),
  StructField("extra", FloatType()),
  StructField("mta_tax", FloatType()),
  StructField("tip_amount", FloatType()),
  StructField("tolls_amount", FloatType()),
  StructField("improvement_surcharge", FloatType()),
  StructField("total_amount", FloatType()),
  StructField("Data_Source", StringType()),
  StructField("Pickup_Borough", StringType()),
  StructField("Pickup_Zip_Code", IntegerType()),
  StructField("Drop_off_Borough", StringType()),
  StructField("Drop_off_Zip_Code", IntegerType()),
  StructField("SPD", IntegerType()),
  StructField("TEMP", IntegerType()),
  StructField("SD", IntegerType()),
  StructField("PCP24", IntegerType()),
  StructField("MAX", IntegerType()),
  StructField("MIN", IntegerType()),
  StructField("Population", FloatType()),
  StructField("Range_dec_pop", StringType()),
  StructField("range_th_pop", StringType()),
  StructField("pop_male", FloatType()),
  StructField("pop_female", FloatType()),
  StructField("pop_5", FloatType()),
  StructField("pop_5_9", FloatType()),
  StructField("pop_10_14", FloatType()),
  StructField("pop_15_19", FloatType()),
  StructField("pop_20_24", FloatType()),
  StructField("pop_25_34", FloatType()),
  StructField("pop_35_44", FloatType()),
  StructField("pop_45_54", FloatType()),
  StructField("pop_55_59", FloatType()),
  StructField("pop_60_64", FloatType()),
  StructField("pop_65_74", FloatType()),
  StructField("pop_75_84", FloatType()),
  StructField("pop_85", FloatType()),
  StructField("age_median", FloatType()),
  StructField("pop_more18", FloatType()),
  StructField("pop_more21", FloatType()),
  StructField("pop_more_62", FloatType()),
  StructField("pop_more65", FloatType()),
  StructField("pop_more18p", FloatType()),
  StructField("pop_more18_male", FloatType()),
  StructField("pop_more19_female", FloatType()),
  StructField("pop_more65p", FloatType()),
  StructField("pop_more65_male", FloatType()),
  StructField("pop_more65_female", FloatType()),
  StructField("pop_tot", FloatType()),
  StructField("pop_hispanic_tot", FloatType()),
  StructField("pop_nothisp_tot", FloatType()),
  StructField("pop_white", FloatType()),
  StructField("pop_africam", FloatType()),
  StructField("pop_indian", FloatType()),
  StructField("pop_asian", FloatType()),
  StructField("pop_native", FloatType()),
  StructField("pop_otherrace", FloatType()),
  StructField("pop_othertwo", FloatType()),
  StructField("Income", FloatType()),
  StructField("Error_income", FloatType()),
  StructField("Range_decil_inc", StringType()),
  StructField("range_th_inc", StringType())
])

# Infer the schema, and register the DataFrame as a table.
schemaTrips = SQLContext.createDataFrame(trips, schema)

schemaTrips.registerTempTable("trips")

# # SQL can be run over DataFrames that have been registered as a table.
trips = SQLContext.sql("SELECT * FROM trips")

# # The results of SQL queries are RDDs and support all the normal RDD operations.
trip_borough = trips.map(lambda p: "Name: " + p.Pickup_Borough)
for borough_name in trip_borough.collect():
  print borough_name


# [(u'VendorID', u'pickup_datetime', u'dropoff_datetime', u'passenger_count', u'trip_distance', u'pickup_longitude', u'pickup_latitude', u'RateCodeID', u'store_and_fwd_flag', u'dropoff_longitude', u'dropoff_latitude', u'payment_type', u'fare_amount', u'extra', u'mta_tax', u'tip_amount', u'tolls_amount', u'improvement_surcharge', u'total_amount', u'Data_Source', u'Pickup_Borough', u'Pickup_Zip_Code', u'Drop_off_Borough', u'Drop_off_Zip_Code'),
# (u'2015-03-28 23:56:05', u'2', u'2015-03-29 00:19:42', u'1', u'8.70', u'-73.966293334960938', u'40.764869689941406', u'1', u'N', u'-73.921455383300781', u'40.839962005615234', u'1', u'26', u'0.5', u'0.5', u'3', u'0', u'0.3', u'30.3', u'Y', u'Manhattan', u'10065', u'Bronx', u'10452', u'23:51', u'16', u'30', u'', u'*****', u'***', u'***\t')]
