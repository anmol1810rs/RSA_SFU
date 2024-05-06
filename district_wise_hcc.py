import sys
from pyspark.sql import SparkSession, functions 
from pyspark.sql import types
from pyspark.sql import SQLContext
import mysql.connector
import pymysql
from sqlalchemy import create_engine
from pyspark.sql.functions import *

#The following nested UDF function allows to label encode the neighbourhoods present in each district
def mapper(district_dict_brd):
    def district_number(x):
        for k,v in district_dict_brd.value.items():
            if x in v:
                return k
    return functions.udf(district_number)

def main():

    #Creating a dataframe to store the cleaned data
    crime_df = spark.read.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_cleaned_data") \
	.option("user", "admin").option("password", "rsa12345").load()

    #The following dictionary has 'District Number' and 'List of associated neighbourhoods' as a Key-Value Pair
    district_dict={
        1:['west end', 'central business district','stanley park'],
        2:['strathcona', 'grandview-woodland', 'hastings-sunrise'],
        3:['sunset', 'renfrew-collingwood', 'mount pleasant', 'killarney', 'victoria-fraserview', 'kensington-cedar cottage'],
        4:['kitsilano', 'fairview', 'dunbar-southlands', 'arbutus ridge', 'shaughnessy', 'south cambie', 'riley park', 'musqueam', 'kerrisdale', 'oakridge', 'marpole','west point grey']
    }

    #Create a broadcast variable to enable UDF function operations on the dictionary
    district_dict_brd = sc.broadcast(district_dict)

    #Selecting only neighbourhood, type, year before the visualization
    district_df = crime_df.select('type', 'neighbourhood', 'year')
    district_df = district_df.withColumn('District', mapper(district_dict_brd)(col('neighbourhood')))

   
    #Group the entire data by 'District' and then aggregate by sum
    district_df = district_df.withColumn('Count',lit(1))

    #For year 2022
    district_df_1 = district_df.filter( district_df['year'] == 2022)
    district_cases_2022 = district_df_1.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2022')
    print(district_cases_2022.show())

    #For year 2021
    district_df_2 = district_df.filter( district_df['year'] == 2021)
    district_cases_2021 = district_df_2.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2021')

    #For year 2020
    district_df_3 = district_df.filter( district_df['year'] == 2020)
    district_cases_2020 = district_df_3.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2020')
    print(district_cases_2020.show())

    #Joining the tables together to sleect the crimes for each year at once
    district_cases = district_cases_2022.join(district_cases_2021, district_cases_2022['District'] == district_cases_2021['District'],"inner").drop(district_cases_2022['District'])
    district_cases = district_cases.join(district_cases_2020, district_cases['District'] == district_cases_2020['District'],"inner").drop(district_cases_2020['District'])
   
    district_cases = district_cases.select('District','2020','2021','2022').orderBy('District')
    print(district_cases.show())

    district_cases.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_districtwise_highcrimerate") \
	.option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    print("Results pushed to Database")
   
if __name__ == '__main__':
    spark = SparkSession.builder.appName('District wise High Crime Count (2020-2022').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()