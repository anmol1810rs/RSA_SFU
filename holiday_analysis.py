import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import utm
from pyspark.sql import SparkSession, functions, types, DataFrame
from functools import reduce
from pyspark.sql.functions import udf, col, to_timestamp, year, month, dayofmonth, lit,lag, abs, round
from pyspark.sql.window import Window
from holiday_parsing import get_holidays



def main(holiday_ip):

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

    dates, holidays = get_holidays(str(holiday_ip))
    
    holiday_inputs = [list(zipped) for zipped in zip(dates, holidays)]
    holiday_cols = ["date","holiday"]

    holiday_df = spark.createDataFrame(holiday_inputs, holiday_cols)
    
    holiday_df = holiday_df.withColumn("holiday",holiday_df["holiday"].cast(types.StringType()))
    holiday_df = holiday_df.withColumn("date", to_timestamp("date",'MM/dd/yyyy'))

    # Extracting year from holiday dataframe to a seperate column
    holiday_df = holiday_df.withColumn("year",year(holiday_df["date"]))

    # Extracting year from holiday dataframe to a seperate column
    holiday_df = holiday_df.withColumn("month",month(holiday_df["date"]))

    # Extracting year from holiday dataframe to a seperate column
    holiday_df = holiday_df.withColumn("day",dayofmonth(holiday_df["date"]))
    
    # Renaming columns accordingly
    crime_df = crime_df.withColumnRenamed("year","df1_year") \
                    .withColumnRenamed("month","df1_month") \
                    .withColumnRenamed("day","df1_day")

    # Renaming columns accordingly
    holiday_df = holiday_df.withColumnRenamed("year","df2_year") \
                    .withColumnRenamed("month","df2_month") \
                    .withColumnRenamed("day","df2_day")

    # Declaring a condition for the join to be used
    condition = [crime_df.df1_year == holiday_df.df2_year, crime_df.df1_month == holiday_df.df2_month,
                 crime_df.df1_day == holiday_df.df2_day]


    # Joining both dataframes on matching dates
    joined_df = crime_df.join(holiday_df, condition).drop(holiday_df.df2_year) \
                                                    .drop(holiday_df.df2_month) \
                                                    .drop(holiday_df.df2_day).cache()

    # 3 % of the crimes are done on holidays
    #print(crime_df.count(), joined_df.count())

    joined_df = joined_df.select("id","type","df1_year","neighbourhood","hundred_block","holiday").cache()

    ################# Top 3 holidays with most crime in last 5 years  #################
    

    joined_last_5_years_df = joined_df.filter(joined_df["df1_year"] >= 2018)
    joined_count_crime_df  = joined_last_5_years_df.groupby("holiday").count()

    joined_crime_ordered_df = joined_count_crime_df.orderBy(col("count").desc())

    joined_crime_ordered_df = joined_crime_ordered_df.limit(3)
    
    #joined_last_5_years_df = joined_df.filter(joined_df["df1_year"] >= 2018)
    joined_count_crime_df  = joined_df.groupby("holiday").count()

    joined_crime_ordered_df = joined_count_crime_df.orderBy(col("count").desc())

    # Getting the top 3 holidays 
    top_holidays_df = joined_crime_ordered_df.limit(3).collect()
    
    holidays = [top_holidays_df[0][0],top_holidays_df[1][0],top_holidays_df[2][0]]
    
    # Canada Day, New Years Day, Labour Day are found to be having the most crimes 
    
    # Finding for Canada Day
    canada_day_df = joined_df.filter(joined_df["holiday"] == holidays[0])

    canada_day_df = canada_day_df.withColumn("counts", lit(1))

    canada_day_df  = canada_day_df.groupby("df1_year").sum("counts")

    canada_day_df = canada_day_df.orderBy(col("sum(counts)").desc())

    canada_day_df = canada_day_df.withColumn("holiday", lit(str(holidays[0])))

    ## Finding For New Years Day
    new_years_day_df = joined_df.filter(joined_df["holiday"] == holidays[1])

    new_years_day_df = new_years_day_df.withColumn("counts", lit(1))

    new_years_day_df  = new_years_day_df.groupby("df1_year").sum("counts")

    new_years_day_df = new_years_day_df.orderBy(col("sum(counts)").desc())

    new_years_day_df = new_years_day_df.withColumn("holiday", lit(str(holidays[1])))

    # Finding For Labour Day
    labour_day_df = joined_df.filter(joined_df["holiday"] == holidays[2])

    labour_day_df = labour_day_df.withColumn("counts", lit(1))

    labour_day_df = labour_day_df.groupby("df1_year").sum("counts")

    labour_day_df = labour_day_df.orderBy(col("sum(counts)").desc())

    labour_day_df = labour_day_df.withColumn("holiday", lit(str(holidays[2]))) 

    dataframe_list = [canada_day_df,
                      new_years_day_df, labour_day_df] 
    
    df_merged = reduce(DataFrame.unionAll, dataframe_list)

    df_merged = df_merged.withColumnRenamed("df1_year","Year") \
                         .withColumnRenamed("sum(counts)","Count Of Crimes On Holidays") \
                         .withColumnRenamed("holiday","Holiday")
  
      
    df_merged.write.format("jdbc").option("batchsize",1000).option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_holiday_crime_data") \
	.option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

if __name__ == '__main__':
    holiday_ip = sys.argv[1]
    spark = SparkSession.builder.appName('holiday-analysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(holiday_ip)