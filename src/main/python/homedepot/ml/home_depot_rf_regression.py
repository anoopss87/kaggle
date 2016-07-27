
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
import numpy as np

#inputRDD = sc.textFile("/FileStore/tables/f4jcmp0k1457734475383/train.csv")
inputRDD = sc.textFile("train.csv")
# Product descriptions file
#pdRDD = sc.textFile("/FileStore/tables/aau3zlbl1457735709012/product_descriptions.csv")
pdRDD = sc.textFile("product_descriptions.csv")
# Test Data
#tRDD = sc.textFile("/FileStore/tables/rr0e221h1457734742407/test.csv")
tRDD = sc.textFile("test.csv")
#attributesRDD = sc.textFile("/FileStore/tables/p4n0l9bw1457739200936/attributes.csv")

# rmse calculation
def squaredError(label, prediction):
    return (label - prediction) ** 2.0

def calcRMSE(labelsAndPreds):
    return (float(np.sqrt(labelsAndPreds
           .map(lambda (x, y): squaredError(x, y))
           .mean())))

# remove header
headerTrain = inputRDD.first()
trainCRDD = inputRDD.filter(lambda x:x != headerTrain)
ptrainRDD = trainCRDD.map(lambda x: x.split(',')).map(lambda x:(x[0],x[1],x[-1]))
# In Databricks dont inlcude this step in local use: sqlContext = SQLContext(sc)
sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
#sqlContext = SQLContext(sc)
trainDF = sqlContext.createDataFrame(ptrainRDD, ['id','product_uid','relevance'])

# remove header for product
headerProduct = pdRDD.first()
pdCRDD = pdRDD.filter(lambda x:x != headerProduct)
pDRDD = pdCRDD.map(lambda x: x.split(',"'))
pdDF = sqlContext.createDataFrame(pDRDD, ['product_uid','product_description'])

# join df
joined_df = trainDF.join(pdDF, trainDF.product_uid == pdDF.product_uid, 'inner')

# select relevance, product_description
labelDF = joined_df.select(joined_df.relevance, joined_df.product_description)

# TF-IDF
tokenizer = Tokenizer(inputCol="product_description", outputCol="pwords")
wordsData = tokenizer.transform(labelDF)
hashingTF = HashingTF(inputCol="pwords", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# split into train and test
lpData = rescaledData.map(lambda row: LabeledPoint(float(row[0]), row[4]))

# randomSplit into train and test
weights = [0.9, 0.1]
seed = 42
trainlpData, testlpData = lpData.randomSplit(weights, seed)
testlpData.take(10)

# create the model
#model = RandomForest.trainRegressor(trainlpData, categoricalFeaturesInfo={}, numTrees=10, featureSubsetStrategy="auto", impurity='variance', maxDepth=10, maxBins=100)

# try other machine learning methods if wanted:
model = GradientBoostedTrees.trainRegressor(trainlpData,categoricalFeaturesInfo={}, numIterations=10)

# evaluate model on test instances and compute test error
predictions = model.predict(testlpData.map(lambda x: x.features))
labelsAndPredictions = testlpData.map(lambda lp: lp.label).zip(predictions)

# MSE calculation
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /float(testlpData.count())
print('Test Mean Squared Error = ' + str(testMSE))

# RMSE
RMSE = calcRMSE(labelsAndPredictions)
print('Root Mean Squared Error for Test = ' + str(RMSE))

# create submission file
headertest = tRDD.first()
tcRDD = tRDD.filter(lambda x:x != headertest)
tcdRDD = tcRDD.map(lambda x: x.split(',')).map(lambda x: (x[0],x[1],x[-1]))
tDF = sqlContext.createDataFrame(tcdRDD, ['id','product_uid','search_term'])

# calculate TF-IDF again for search terms:
tDFTest = tDF.select(tDF.id,tDF.search_term)
tokenizerTest = Tokenizer(inputCol="search_term", outputCol="terms")
wordsDataTest = tokenizerTest.transform(tDFTest)
hashingTFTest = HashingTF(inputCol="terms", outputCol="rawFeats", numFeatures=20)

featurizedDataTest = hashingTFTest.transform(wordsDataTest)
idfTest = IDF(inputCol="rawFeats", outputCol="feats")
idfModelTest = idfTest.fit(featurizedDataTest)
rescaledDataTest = idfModelTest.transform(featurizedDataTest)

preds = model.predict(rescaledDataTest.map(lambda x: x.feats))

idsAndPredictions = rescaledDataTest.map(lambda lp: int(lp.id)).zip(preds)

idsAndPredictions.repartition(1).saveAsTextFile('/FileStore/resultFile_jd1')

display(dbutils.fs.ls("/FileStore/resultFile_jd1"))



