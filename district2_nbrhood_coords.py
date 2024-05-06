import sys
from pyspark.sql import SparkSession, functions 
from pyspark.sql import types
import mysql.connector
from pyspark.sql.functions import *

def mapper(district_dict_brd):
    def district_number(x):
        for k,v in district_dict_brd.value.items():
            if x in v:
                return k
    return functions.udf(district_number)

def main():

    #Displaying the input Clean data
    crime_df = spark.read.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_cleaned_data") \
	.option("user", "admin").option("password", "rsa12345").load()
    crime_df = crime_df.filter( (crime_df['x_neighbourhood'] != 0.0) | (crime_df['y_neighbourhood'] != 0.0) )

    #The following dictionary contains key-value pairs of the neighbourhood and the respective district number
    district_dict={
        1:['west end', 'central business district','stanley park'],
        2:['strathcona', 'grandview-woodland', 'hastings-sunrise'],
        3:['sunset', 'renfrew-collingwood', 'mount pleasant', 'killarney', 'victoria-fraserview', 'kensington-cedar cottage'],
        4:['kitsilano', 'fairview', 'dunbar-southlands', 'arbutus ridge', 'shaughnessy', 'south cambie', 'riley park', 'musqueam', 'kerrisdale', 'oakridge', 'marpole','west point grey']
    }

    #Create a broadcast variable to enable UDF function operations on the dictionary
    district_dict_brd = sc.broadcast(district_dict)

    #Selecting only neighbourhood, type, year before the visualization
    district_df = crime_df.select('neighbourhood', 'hundred_block', 'year', 'x', 'y')
    district_df = district_df.withColumn('District', mapper(district_dict_brd)(col('neighbourhood')))
    district_df = district_df.filter((district_df['year'] == 2020) | (district_df['year'] == 2021) | (district_df['year'] == 2022))
    district_df = district_df.filter(district_df['District'] == 2)
    district_df = district_df.filter((district_df['hundred_block'] == 'e hastings st')| (district_df['hundred_block'] == 'nk_loc st') | (district_df['hundred_block'] == 'commercial dr') )
    district_df = district_df.select('x','y')

    print(district_df.show())

    #Writing the output to the database
    district_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_d2_nbr_hundredblk_coords") \
	.option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    print("Results pushed to database")

    
if __name__ == '__main__':
    spark = SparkSession.builder.appName('District 2 neighbourhood coords').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()