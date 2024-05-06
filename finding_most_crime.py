import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import utm
from pyspark.sql import SparkSession, functions, types, DataFrame
from functools import reduce
from pyspark.sql.functions import col, lit
import mysql.connector

# finding count of the most crime hit block per district 
#(for all neighbourhood) & compare respective crime counts for (2020-2022)


#The following nested UDF function allows to label encode the neighbourhoods present in each district
def district_mapper(district_dict):

    def district_value(ip_district):

        for number,district in district_dict.value.items():

            if ip_district in district:

                return number
                
    return functions.udf(district_value)

def create_district_map():

    district_dict={
        1:['west end', 'central business district','stanley park'],
        2:['strathcona', 'grandview-woodland', 'hastings-sunrise'],
        3:['sunset', 'renfrew-collingwood', 'mount pleasant', 'killarney', 'victoria-fraserview', 'kensington-cedar cottage'],
        4:['kitsilano', 'fairview', 'dunbar-southlands', 'arbutus ridge', 'shaughnessy', 'south cambie', 'riley park', 'musqueam', 'kerrisdale', 'oakridge', 'marpole','west point grey']
    }

    return district_dict


def main():

    # schema for the input
    observation_schema = types.StructType([
                            types.StructField('id', types.StringType()),
                            types.StructField('type', types.StringType()),
                            types.StructField('timestamp', types.TimestampType()),
                            types.StructField('year', types.IntegerType()),
                            types.StructField('month', types.IntegerType()),
                            types.StructField('day', types.IntegerType()),
                            types.StructField('hour', types.IntegerType()),
                            types.StructField('minute', types.IntegerType()),
                            types.StructField('neighbourhood', types.StringType()),
                            types.StructField('x_neighbourhood', types.FloatType()),
                            types.StructField('y_neighbourhood', types.FloatType()),
                            types.StructField('hundred_block', types.StringType()),
                            types.StructField('x', types.FloatType()),
                            types.StructField('y', types.FloatType()),])  

    # Read the json file using the schema to make the spark dataframe
    crime_df = spark.read.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_cleaned_data") \
	.option("user", "admin").option("password", "rsa12345").load()

    # Creating a dictionary of all the districts along with the tagged district number
    district_dict = create_district_map()

    #Create a broadcast variable to enable UDF function operations on the dictionary
    district_dict = sc.broadcast(district_dict)

    #Selecting only neighbourhood, type, year before the visualization
    #district_df = crime_df.select('type', 'neighbourhood', 'year')
    district_df = crime_df.withColumn('district', district_mapper(district_dict)(col('neighbourhood'))).cache()
    
    # Seperating dataframes per district for last 3 years only
    district_1_df = district_df.filter(district_df["district"] == 1) \
                                .filter(district_df["year"] > 2019)

    district_2_df = district_df.filter(district_df["district"] == 2) \
                                .filter(district_df["year"] > 2019)

    district_3_df = district_df.filter(district_df["district"] == 3) \
                                .filter(district_df["year"] > 2019)

    district_4_df = district_df.filter(district_df["district"] == 4) \
                                .filter(district_df["year"] > 2019)


    # District 1 - Most crimes in which hundred_block
    district_1_df = district_1_df.withColumn("counts", lit(1))
    district_1_counts_df  = district_1_df.groupby("year","hundred_block").sum("counts")
    district_1_max_df  = district_1_counts_df.groupby("year").max("sum(counts)")

    # Renaming columns accordingly
    district_1_counts_df = district_1_counts_df.withColumnRenamed("year","district_1_year") \
                    .withColumnRenamed("hundred_block","district_1_hundred_block") \
                    .withColumnRenamed("sum(counts)","count_sum")

    # Declaring a condition for the join to be used
    d1_condition = [district_1_counts_df.district_1_year == district_1_max_df.year,
                    district_1_counts_df.count_sum == district_1_max_df["max(sum(counts))"]]

    district_1_final_df = district_1_counts_df.join(district_1_max_df, d1_condition)

    district_1_final_df = district_1_final_df.select("year","district_1_hundred_block",
                                                    "count_sum")

    district_1_final_df = district_1_final_df.withColumnRenamed("year","year") \
                                            .withColumnRenamed("district_1_hundred_block","hundred_block") 

    district_1_final_df = district_1_final_df.withColumn("district", lit(1))
                                            


    # District 2 - Most crimes in which hundred_block
    district_2_df = district_2_df.withColumn("counts", lit(1))
    district_2_counts_df  = district_2_df.groupby("year","hundred_block").sum("counts")
    district_2_max_df  = district_2_counts_df.groupby("year").max("sum(counts)")

    # Renaming columns accordingly
    district_2_counts_df = district_2_counts_df.withColumnRenamed("year","district_2_year") \
                    .withColumnRenamed("hundred_block","district_2_hundred_block") \
                    .withColumnRenamed("sum(counts)","count_sum")

    # Declaring a condition for the join to be used
    d2_condition = [district_2_counts_df.district_2_year == district_2_max_df.year,
                    district_2_counts_df.count_sum == district_2_max_df["max(sum(counts))"]]

    district_2_final_df = district_2_counts_df.join(district_2_max_df, d2_condition)

    district_2_final_df = district_2_final_df.select("year","district_2_hundred_block",
                                                    "count_sum")

    district_2_final_df = district_2_final_df.withColumnRenamed("year","year") \
                                            .withColumnRenamed("district_2_hundred_block","hundred_block") 
    
    district_2_final_df = district_2_final_df.withColumn("district", lit(2))


    # District 3 - Most crimes in which hundred_block
    district_3_df = district_3_df.withColumn("counts", lit(1))
    district_3_counts_df  = district_3_df.groupby("year","hundred_block").sum("counts")
    district_3_max_df  = district_3_counts_df.groupby("year").max("sum(counts)")


    # Renaming columns accordingly
    district_3_counts_df = district_3_counts_df.withColumnRenamed("year","district_3_year") \
                    .withColumnRenamed("hundred_block","district_3_hundred_block") \
                    .withColumnRenamed("sum(counts)","count_sum")

    # Declaring a condition for the join to be used
    d3_condition = [district_3_counts_df.district_3_year == district_3_max_df.year,
                    district_3_counts_df.count_sum == district_3_max_df["max(sum(counts))"]]

    district_3_final_df = district_3_counts_df.join(district_3_max_df, d3_condition)

    district_3_final_df = district_3_final_df.select("year","district_3_hundred_block",
                                                    "count_sum")

    district_3_final_df = district_3_final_df.withColumnRenamed("year","year") \
                                            .withColumnRenamed("district_3_hundred_block","hundred_block")
    
    district_3_final_df = district_3_final_df.withColumn("district", lit(3))


    # District 4 - Most crimes in which hundred_block
    district_4_df = district_4_df.withColumn("counts", lit(1))
    district_4_counts_df  = district_4_df.groupby("year","hundred_block").sum("counts")
    district_4_max_df  = district_4_counts_df.groupby("year").max("sum(counts)")

    # Renaming columns accordingly
    district_4_counts_df = district_4_counts_df.withColumnRenamed("year","district_4_year") \
                    .withColumnRenamed("hundred_block","district_4_hundred_block") \
                    .withColumnRenamed("sum(counts)","count_sum")

    # Declaring a condition for the join to be used
    d4_condition = [district_4_counts_df.district_4_year == district_4_max_df.year,
                    district_4_counts_df.count_sum == district_4_max_df["max(sum(counts))"]]

    district_4_final_df = district_4_counts_df.join(district_4_max_df, d4_condition)

    district_4_final_df = district_4_final_df.select("year","district_4_hundred_block",
                                                    "count_sum")

    district_4_final_df = district_4_final_df.withColumnRenamed("year","year") \
                                            .withColumnRenamed("district_4_hundred_block","hundred_block")
    
    district_4_final_df = district_4_final_df.withColumn("district", lit(4))


    dataframe_list = [district_1_final_df,
                      district_2_final_df,
                      district_3_final_df,
                      district_4_final_df]

    # Merging all dataframes
    df_merged = reduce(DataFrame.unionAll, dataframe_list)

    df_merged = df_merged.withColumnRenamed("year","Year") \
             .withColumnRenamed("hundred_block","Hundred Block") \
             .withColumnRenamed("count_sum","Total Crime") \
             .withColumnRenamed("district","District")

    df_merged.write.format("jdbc").option("batchsize",1000).option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_mostcrime_data") \
	.option("user", "admin").option("password", "rsa12345").mode("overwrite").save()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('finding-most-crime').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()