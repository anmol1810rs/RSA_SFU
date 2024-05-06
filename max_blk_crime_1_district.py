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
    district_df = crime_df.select('type', 'neighbourhood', 'hundred_block', 'year')
    district_df = district_df.withColumn('District', mapper(district_dict_brd)(col('neighbourhood')))

    #Group the entire data by 'District' and then aggregate by sum
    district_df = district_df.withColumn('Count',lit(1))

    #For year 2022
    district_df_1 = district_df.filter( district_df['year'] == 2022)
    district_cases_2022 = district_df_1.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2022')

    #For year 2021
    district_df_2 = district_df.filter( district_df['year'] == 2021)
    district_cases_2021 = district_df_2.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2021')

    #For year 2020
    district_df_3 = district_df.filter( district_df['year'] == 2020)
    district_cases_2020 = district_df_3.groupBy('District').sum('Count').withColumnRenamed('sum(Count)', '2020')

    district_cases = district_cases_2022.join(district_cases_2021, district_cases_2022['District'] == district_cases_2021['District'],"inner").drop(district_cases_2022['District'])
    district_cases = district_cases.join(district_cases_2020, district_cases['District'] == district_cases_2020['District'],"inner").drop(district_cases_2020['District'])

    #Get the total CrimeSum for the years 2020-2022
    district_cases = district_cases.withColumn('TotalCrime', district_cases['2020'] + district_cases['2021'] + district_cases['2022'])
    district_cases = district_cases.select('District', 'TotalCrime').orderBy('TotalCrime')

    #Finding the district with the least number of TotalCrime count 
    least_crime_district = district_cases.first()['District']
    
    #District 2  has the least number of TotalCrime count
    district_df_new = district_df.filter(district_df['District'] == least_crime_district)
    district_df_new = district_df_new.filter((district_df_new['year'] == 2020) | (district_df_new['year'] == 2021) | (district_df_new['year'] == 2022))
    
    #Find blocks that belong to each neighbourhood in the Least Crime District
    blocks_n1 = district_df_new.select('hundred_block','Count').where(district_df_new['neighbourhood'] == 'strathcona')
    blocks_n2 = district_df_new.select('hundred_block','Count').where(district_df_new['neighbourhood'] == 'grandview-woodland')
    blocks_n3 = district_df_new.select('hundred_block','Count').where(district_df_new['neighbourhood'] == 'hastings-sunrise')
    
    #Find the block with maximum crime in each neighbourhood
    blk_n1 = blocks_n1.groupBy('hundred_block').sum('Count').withColumnRenamed('sum(Count)','total')
    blk_n1 = blk_n1.filter(blk_n1['hundred_block'] != 'e hastings st')
    blk_n1 = blk_n1.orderBy(col('total').desc())

    blk_n2 = blocks_n2.groupBy('hundred_block').sum('Count').withColumnRenamed('sum(Count)','total')
    blk_n2 = blk_n2.orderBy(col('total').desc())

    blk_n3 = blocks_n3.groupBy('hundred_block').sum('Count').withColumnRenamed('sum(Count)','total')
    blk_n3 = blk_n3.orderBy(col('total').desc())

    print("Most Crime Hit Block in strathcona is (2020-2022) - ", blk_n1.first())
    print("Most Crime Hit Block in grandview-woodland is (2020-2022) - ", blk_n2.first())
    print("Most Crime Hit Block in hastings-sunrise is (2020-2022) - ", blk_n3.first())

    block_list = [(least_crime_district,'Strathcona', blk_n1.first()[0], blk_n1.first()[1]),
                 (least_crime_district,'Grandview-Woodland', blk_n2.first()[0], blk_n2.first()[1]),
                (least_crime_district, 'Hastings-Sunrise', blk_n3.first()[0], blk_n3.first()[1])]

    result = spark.createDataFrame(block_list, ["District", "Neighbourhood", "Hundred Block", "Crime Count (2020-2022)"])
    
    #Writing the results to the database
    result.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_d2_blocks_high_crime") \
	.option("user", "admin").option("password", "rsa12345").mode("overwrite").save()
    
    print("Results pushed to Database")

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Max Crime Block').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()