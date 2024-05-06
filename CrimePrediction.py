import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import numpy as np
import pickle
np.random.seed(40)

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('crime_train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '3.0' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, NaiveBayes
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

def main(output):

    #Creating a dataframe to store the cleaned data read from database
    crime_df = spark.read.format("jdbc").option("url", "jdbc:mysql://opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com/openvancoverpolicedata") \
	.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "openvancoverpolicedata.opvd_cleaned_data") \
	.option("user", "admin").option("password", "rsa12345").load()

    crime_df = crime_df.withColumnRenamed('type', 'target')
    crime_df = crime_df.dropna()

    #Choosing only 'theft from vehicle', 'mischief' and 'other theft' as the target value set
    crime_df = crime_df.filter((crime_df['target'] == 'theft from vehicle')| (crime_df['target'] == 'mischief') | (crime_df['target'] == 'other theft'))
    crime_df = crime_df.filter(crime_df['x']!='0')
    crime_df = crime_df.filter(crime_df['y']!='0') 
    crime_df = crime_df.select('year','month','hour','neighbourhood','hundred_block','target')
  
    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="target", outputCol="indexedLabel").fit(crime_df)

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer1 = StringIndexer(inputCol="neighbourhood", outputCol="indexedNbr").fit(crime_df)
    featureIndexer2 = StringIndexer(inputCol="hundred_block", outputCol="indexedHb").fit(crime_df)
    featureAssembler = VectorAssembler(inputCols = ['year','month','hour','indexedNbr','indexedHb'], outputCol="indexedFeatures")

    # Split the data into training and test sets (20% held out for testing)
    trainingData, testData = crime_df.randomSplit([0.8, 0.2])

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10, maxBins=1500)

    #Train the Naive bayes Classifer Model (Uncomment this to run the model below)
    #nvb = NaiveBayes(labelCol="indexedLabel", featuresCol="indexedFeatures", smoothing=1.0, modelType="multinomial")

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                labels=labelIndexer.labels)

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer1, featureIndexer2, featureAssembler, rf, labelConverter])

    # Chain indexers and Naive Bayes in a Pipeline (Uncomment to run this particular pipeline)
    #pipeline = Pipeline(stages=[labelIndexer, featureIndexer1, featureIndexer2, featureAssembler, nvb, labelConverter])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("predictedLabel", "target", "year","month","hour","neighbourhood", "hundred_block").show(5)

    #Prediciting the Accuracy of the model
    accuracy = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName= "accuracy")
    model_accuracy = accuracy.evaluate(predictions)

    #Prediciting the precision of the model
    precision = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName= "precisionByLabel")
    model_precision = precision.evaluate(predictions)

    #Prediciting the Recall of the model
    recall = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName= "recallByLabel")
    model_recall = recall.evaluate(predictions)

    #Prediciting the F1-Score of the model
    fscore = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName= "f1")
    model_fscore = fscore.evaluate(predictions)

    #Printing the final results
    print("Test Accuracy = %g" % (model_accuracy))
    print("Test Precision = %g" % (model_precision))
    print("Test Recall = %g" % (model_recall))
    print("Test F1-Score = %g" % (model_fscore))

    #Saving the model to test the model on CrimePrediciton_test.py test-set
    model.write().overwrite().save(output)
    
if __name__ == '__main__':
    output = sys.argv[1]
    main(output)


