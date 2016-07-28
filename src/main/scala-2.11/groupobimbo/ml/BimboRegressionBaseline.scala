package groupobimbo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


/**
 * Created by jshetty on 7/28/16.
 */
object BimboRegressionBaseline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[8]").appName("Groupo Bimbo ").getOrCreate()
    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val traindf = spark.read.option("header", "true").option("inferSchema", "true").csv("data/groupobimbo/train.csv")

    val newColNames = Seq("week_number",
      "sales_depot_id", "sales_channel_id",
      "route_id", "client_id",
      "product_id", "sales_unit_this_week",
      "sales_this_week", "returns_unit_next_week",
      "returns_next_week", "adjusted_demand")

    val newdf = traindf.toDF(newColNames: _*)

    val cdf = newdf.selectExpr("week_number", "sales_depot_id", "sales_channel_id", "route_id", "client_id", "product_id", "sales_unit_this_week", "sales_this_week", "returns_next_week", "cast(adjusted_demand as double) as label")
    val seed = 2424
    val sample8 = cdf.sample(false, 0.01, seed)

    println("Count of rows in sample = " + sample8.count)
    println("Print Schema of sample " + sample8.printSchema)

    val Array(training, test) = sample8.randomSplit(Array(0.8, 0.2))

    println("Count of training rows = " + training.count)

    println("Count of training rows = " + test.count)

    val featureCols = Array("week_number", "sales_depot_id", "sales_channel_id", "route_id", "client_id", "product_id")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("rawFeatures")

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(5)

    // Train the machine learning algorithm
    val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features").setNumTrees(32)


    val pipeline = new Pipeline().setStages(Array(assembler, featureIndexer, rf))
    val paramGrid = new ParamGridBuilder().addGrid(rf.maxDepth, Array(10)).addGrid(rf.maxBins, Array(10)).build()
    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

    // Cross Validator
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val cvModel = cv.fit(training)

    // Make predictions.
    val predictions = cvModel.transform(test)

    // Evaluation Metric for Test
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    // training error:
    val predictionsTrain = cvModel.transform(training)
    val rmseE = evaluator.evaluate(predictionsTrain)
    println("Root Mean Squared Error (RMSE) on training data = " + rmseE)

    val testdf = spark.read.option("header", "true").option("inferSchema", "true").csv("data/groupobimbo/test.csv")
    val ctestdf = testdf.cache()

    println("Test count = " + ctestdf.count)
    println("Test Schema " + ctestdf.printSchema)

    // Transform column names
    val newTestColNames = Seq("id", "week_number",
      "sales_depot_id", "sales_channel_id",
      "route_id", "client_id",
      "product_id")

    val newTestdf = ctestdf.toDF(newTestColNames: _*)

    println("New dataframe schema " + newTestdf.printSchema)

    // Select cols withoutId
    val crealdf = newTestdf.select("week_number", "sales_depot_id", "sales_channel_id", "route_id", "client_id", "product_id")

    val predictionRealTest = cvModel.transform(crealdf)


    val finaldf = predictionRealTest.select($"prediction".cast("String")).coalesce(1)

    val resultdf = finaldf.withColumn("id", monotonicallyIncreasingId()).select("id", "prediction").withColumnRenamed("prediction", "Demanda_uni_equil")

    resultdf.write.csv("output/groupobimbo/groupoBimboRFR")

  }

}