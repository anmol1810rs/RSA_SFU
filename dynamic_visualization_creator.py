from pyspark.sql import SparkSession, functions, types
import mysql.connector
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main():

    cleaned_df = spark.read.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").load()
    cleaned_df.show(5)

    type_num_df = cleaned_df.groupBy(
        "year", "type").count().alias("count").orderBy("year")
    type_num_df.show(5)
    type_num_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_type_num_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    neighbourhood_num_df = cleaned_df.groupBy(
        "year", "neighbourhood").count().alias("count").orderBy("year")
    neighbourhood_num_df.show(5)
    neighbourhood_num_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_neighbourhood_num_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    month_num_df = cleaned_df.groupBy(
        "year", "month").count().alias("count").orderBy("year")
    month_num_df.show(5)
    month_num_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_month_num_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    hour_num_df = cleaned_df.groupBy(
        "year", "hour").count().alias("count").orderBy("year")
    hour_num_df.show(5)
    hour_num_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_hour_num_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()

    type_neighbourhood_num_df = cleaned_df.groupBy(
        "year", "type", "neighbourhood").count().alias("count").orderBy("year")
    type_neighbourhood_num_df.show(5)
    type_neighbourhood_num_df.write.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_type_neighbourhood_num_df_cleaned_data") \
        .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('tranformations etl').config(
        "spark.jars", "/Users/siddharth/Documents/BD_Project/RSA/mysql-connector-j-8.0.31.jar").getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
