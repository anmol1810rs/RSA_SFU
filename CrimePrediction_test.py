import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import date
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('crime_test').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

def main(model_file):
   
    #The test data to check CrimeModel acuracy
    test_list = [(2019, 10, 23, 'hastings-sunrise', 'windermere st', 'other theft')]

    test_dataset = spark.createDataFrame(test_list, ['year','month','hour','neighbourhood','hundred_block','target'])

    model = PipelineModel.load(model_file)
    crime_predictions = model.transform(test_dataset)
    print(crime_predictions.show())

    #Printing the Predicted Crime Type (target) for the test-set
    predicted_crime = crime_predictions.first()['predictedLabel']
    print('Predicted Crime Type is:', predicted_crime)

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)
