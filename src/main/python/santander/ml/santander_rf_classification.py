#pyspark --packages com.databricks:spark-csv_2.11:1.4.0

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils
from pyspark.sql.functions import stddev, stddev_pop, stddev_samp
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
#sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/Users/jshetty/Documents/kaggle/santander/dataset/train.csv")

# drop duplicate rows in a dataframe:
data = df.dropDuplicates()

# count distinct rows
distinctcount = data.distinct.count()

# collect only 1's
unsatisfied = data.filter(data.TARGET == 1)

# collect 6000 rows
rawdata = data.limit(6000)

# union unsatisfied + rawdata to get the train data
train = rawdata.unionAll(unsatisfied)

# drop duplicates
newtrain = train.dropDuplicates()
lprdd = newtrain.map(lambda row: LabeledPoint(row[-1],row[:-1]))

weights = [0.8, 0.2]
seed = 4242
trainlpData, testlpData = lprdd.randomSplit(weights, seed)

#model = LogisticRegressionWithLBFGS.train(trainlpData)
# model 2 RF
model = RandomForest.trainClassifier(trainlpData, numClasses=2, categoricalFeaturesInfo={},numTrees=40, featureSubsetStrategy="auto",impurity='gini', maxDepth=6, maxBins=64)
# model = GradientBoostedTrees.trainClassifier(trainlpData, categoricalFeaturesInfo={}, numIterations=35)
# pred = testlpData.map(lambda p: (float(p.label), float(model.predict(p.features))))

predictions = model.predict(testlpData.map(lambda x: x.features))
labelsAndPredictions = testlpData.map(lambda x: x.label).zip(predictions)

metrics = BinaryClassificationMetrics(labelsAndPredictions)

print("Area under ROC = %s" % metrics.areaUnderROC)
#Area under ROC = 0.720760128142
#RF: Area under ROC = 0.699983307322
#GB : Area under ROC = 0.74

# For submission:
rawTestData = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/Users/jshetty/Documents/kaggle/santander/dataset/test.csv")
#rawTestData.show()

tlpData = rawTestData.map(lambda row: LabeledPoint(row[0],row[:]))
#idAndpreds = tlpData.map(lambda p: (int(p.label), int(model.predict(p.features))))

preds = model.predict(tlpData.map(lambda x: x.features))
pred = preds.map(lambda x: int(x))
labelsAndPreds = tlpData.map(lambda x: int(x.label)).zip(pred)

# Make it as one submission file and add Id, Target
labelsAndPreds.coalesce(1, True).saveAsTextFile("/submission/kaggle_GB1")

# cleanup
#rev part-00000 | cut -c2- | rev | cut -c2- >kaggle_GB1.csv

# remove space after comma:
# sed -e 's/, /,/g' kaggle_GB1.csv > kaggle_gb_sub.csv
