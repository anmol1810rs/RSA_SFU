import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import utm
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import udf, col, to_timestamp
import uuid

## FUNCTIONS TO USE LATER IN CODE


# assigning correct latitude as per the neighbourhood
def assign_lat_dict(mapping_broadcasted):

    def nest(x):

        return mapping_broadcasted.value.get(x)[0]

    return udf(nest)

# assigning correct longitude as per the neighbourhood
def assign_long_dict(mapping_broadcasted):

    def nest(x):

        return mapping_broadcasted.value.get(x)[1]

    return udf(nest)

# UDF Function to convert UTM to latitude and return it
def utm_to_lat(x,y):

    lat, long = utm.to_latlon(x, y, 10, 'U')

    return float(lat)

# # UDF Function to convert UTM to longitude and return it
def utm_to_long(x,y):

    lat, long = utm.to_latlon(x, y, 10, 'U')

    return float(long)

# Convert the given columns to timestamp format and return it
def get_timestamp(year,month,day,hour,minute):
    
    make_time = str(year).zfill(2) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + ' ' + str(hour).zfill(2) + ':' + str(minute).zfill(2) + ':' + '00'
    
    return make_time

#  Removing substrings in hundred_block which have masked values
#  This is done because they do not provide any useful information
@functions.udf(returnType=types.StringType())
def remove_masked_block(hundred_block):
    
    words = hundred_block.split(' ')

    for word in words:

        if len(word) != 0:

            if word[0].isdigit():

                if 'x' in word:

                    words.remove(word)

            elif word == 'x':

                words.remove(word)

    return ' '.join(words)


# Data contains aliases for hundred_block
# Keeping the first instance of hundred_block where the alias exists
@functions.udf(returnType=types.StringType())
def remove_alias_block(hundred_block):
    
    if '/' in hundred_block:

        words = hundred_block.split('/', 1)[0]

        return words

    else:
        return hundred_block

# Further cleaning to ensure all strings are in correct format as per naming the blocks 
@functions.udf(returnType=types.StringType())
def rename_block(hundred_block):

    suf = lambda n: "%d%s"%(n,{1:"st",2:"nd",3:"rd"}.get(n%100 if n%100<20 else n%10,"th")) 

    words = hundred_block.split(' ')

    for i in range(len(words)):

        if words[i] == 'av':

            words[i] = 'ave'

        elif words[i].isdigit():

            words[i] = suf(int(words[i]))

    return ' '.join(words)


# Further cleaning
@functions.udf(returnType=types.StringType())
def remove_hash_block(hundred_block):

    if hundred_block[0] == '''"''':

        hundred_block = hundred_block[1:]

        words = hundred_block.split('''"''',1)[0]

        return words
    else:
        return hundred_block

# Further cleaning
@functions.udf(returnType=types.StringType())
def remove_full_masked_block(hundred_block):

    words = hundred_block.split(' ')

    for word in words:

        if len(word) != 0:

            if word == 'xxxx':
                words.remove(word)

    return ' '.join(words)

# Assigning latitude and longitude as per neighbourhood
# Pre-defined latitude and longitudes were found online as per each neighbourhood
# They were then put in a dictionary that will be used later on
def assign_lat_long(neighbourhood_list):

    lat_longs = [[49.20922, -123.13615],
                [49.26196, -123.13035],
                [49.28247, -123.11863], 
                [49.28413, -123.13179],
                [49.25430, -123.13563],
                [49.21959, -123.09024],
                [49.27955, -123.08998],
                [49.27759, -123.04392],
                [49.27056, -123.06794],
                [49.24744, -123.10297],
                [49.26941, -123.15527],
                [49.26613, -123.20051],
                [49.23083, -123.13113],
                [49.24763, -123.08421],
                [49.26333, -123.09659],
                [49.21842, -123.07329],
                [49.31252, -123.14445],
                [49.23380, -123.15527],
                [49.24621, -123.16126],
                [49.24850, -123.04035],
                [49.22834, -123.19838],
                [49.22427, -123.04625],
                [49.24726, -123.18690],
                [49.24668, -123.12091]]
    
    neighbour_lat_long_dict = {}

    for i in range(len(neighbourhood_list)):

        neighbour_lat_long_dict[neighbourhood_list[i]] = lat_longs[i]
    
    return neighbour_lat_long_dict
    
# Main function 
def main(inputs, outputs):
    
    # schema for the input
    observation_schema = types.StructType([
                        types.StructField('type', types.StringType()),
                        types.StructField('year', types.IntegerType()),
                        types.StructField('month', types.IntegerType()),
                        types.StructField('day', types.IntegerType()),
                        types.StructField('hour', types.IntegerType()),
                        types.StructField('minute', types.IntegerType()),
                        types.StructField('hundred_block', types.StringType()),
                        types.StructField('neighbourhood', types.StringType()),
                        types.StructField('x', types.FloatType()),
                        types.StructField('y', types.FloatType()),])

    # Read the json file using the schema to make the spark dataframe
    crime_df = spark.read.option("header",True) \
            .schema(observation_schema) \
            .csv(str(inputs)).cache()

    # Drop null values 
    crime_df = crime_df.dropna()

    # Drop duplicates
    crime_df1 = crime_df.distinct()

    # Lowercasing all columns with type string
    crime_df2 = crime_df1.withColumn("type",functions.lower(col("type")))
    crime_df3 = crime_df2.withColumn("hundred_block",functions.lower(col("hundred_block")))
    crime_df4 = crime_df3.withColumn("neighbourhood",functions.lower(col("neighbourhood")))

    # Collecting all unique neighbourhoods for assigning latitude and longitude
    neighbourhood_list =[data[0] for data in crime_df4.select('neighbourhood').distinct().collect()]

    # Making a dictionary and broadcasting it
    lat_long_dict = assign_lat_long(neighbourhood_list)
    lat_long_dict_broadcast = sc.broadcast(lat_long_dict)

    # Adding new columns to get latitude and longitude for neighbourhoods only
    crime_df5 = crime_df4.withColumn('x_neighbourhood', assign_lat_dict(lat_long_dict_broadcast)(col('neighbourhood')))
    crime_df6 = crime_df5.withColumn('y_neighbourhood', assign_long_dict(lat_long_dict_broadcast)(col('neighbourhood')))

    #no_conversion_df = crime_df6.filter((crime_df6['x'] == 0.0) | (crime_df6['y'] == 0.0))

    # Excluding data points that are not having any x and y cordinates 
    conversion_df = crime_df6.filter((crime_df6['x'] != 0.0) | (crime_df6['y'] != 0.0))

    # Assigning latitude by converting UTM latitude
    utm_to_lat_conversion = udf(utm_to_lat, types.FloatType())
    crime_df7 = conversion_df.withColumn("lat_x", utm_to_lat_conversion('x','y'))

    # Assigning longitude by converting UTM longitude
    utm_to_long_conversion = udf(utm_to_long, types.FloatType())
    crime_df8 = crime_df7.withColumn("lat_y", utm_to_long_conversion('x','y'))

    # Dropping irrevelant columns x and y as we do not need UTM type cordinates
    crime_df8 = crime_df8.drop('x','y')

    # Renaming latitude and longitude cordinates accordingly
    crime_df8 = crime_df8.withColumnRenamed("lat_x","x") \
    .withColumnRenamed("lat_y","y")

    # Further cleaning on hundred_block
    crime_df8 = crime_df8.withColumn("hundred_block",remove_hash_block(col("hundred_block")))
    crime_df8 = crime_df8.withColumn("hundred_block", remove_masked_block(col("hundred_block")))
    crime_df8 = crime_df8.withColumn("hundred_block", remove_alias_block(col("hundred_block")))
    crime_df8 = crime_df8.withColumn("hundred_block", rename_block(col("hundred_block")))
    crime_df8 = crime_df8.withColumn("hundred_block", remove_full_masked_block(col("hundred_block")))


    # Assigning timestamp column
    timestamp_conversion = udf(get_timestamp, types.StringType())
    crime_df10 = crime_df8.withColumn("timestamp", to_timestamp(timestamp_conversion('year','month','day','hour','minute'),
                                                                'yyyy-MM-dd HH:mm:ss'))

    # Generating unique ID for each row
    UUID_generator = udf(lambda : str(uuid.uuid1()),types.StringType())
    crime_df11 = crime_df10.withColumn("id", UUID_generator())

    # Changing types of columns appropriately
    crime_df11 = crime_df11.withColumn("x_neighbourhood",crime_df11['x_neighbourhood'].cast(types.FloatType()))
    crime_df11 = crime_df11.withColumn("y_neighbourhood",crime_df11['y_neighbourhood'].cast(types.FloatType()))
    crime_df11 = crime_df11.withColumn("x",crime_df11['x'].cast(types.FloatType()))
    crime_df11 = crime_df11.withColumn("y",crime_df11['y'].cast(types.FloatType()))

    # Rearranging the data columns as needed
    crime_df11 = crime_df11.select("id","type","timestamp","year","month","day","hour", "minute", \
                                    "neighbourhood","x_neighbourhood","y_neighbourhood","hundred_block","x","y")

    # Saving the data to our database
    # Note this could take time to save on our database, hence can take hours
    
    # crime_df11.write.format("jdbc").option("batchsize",1000). \
    #     option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	#     .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_clean_data") \
	#     .option("user", "admin").option("password", "rsa12345").mode("overwrite").save()


    # USE THIS COMMAND TO CHECK IF THE DATA IS CORRECTLY BEING STORED LOCALLY 
    crime_df11.repartition(1).write.option("header",True).csv(str(outputs))

if __name__ == '__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    spark = SparkSession.builder.appName('data-cleaning').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,outputs)