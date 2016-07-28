package santander.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
 * Created by jshetty on 7/26/16.
 * ML pipeline  to solve Santander Bank satisfaction problem
 * spark 2.0
 */
object SantanderRandomForestClassifier {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[8]").appName("Santander Bank").getOrCreate()
    val sc = spark.sparkContext

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Load train data as a dataframe
    val rawData = spark.read.option("header", "true").option("inferSchema", "true").csv("data/santander/train.csv")
    val rdata = rawData.selectExpr("cast(TARGET as double) as label", "ID", "var3", "var15", "imp_ent_var16_ult1", "imp_op_var39_comer_ult1", "imp_op_var39_comer_ult3", "imp_op_var40_comer_ult1", "imp_op_var40_comer_ult3", "imp_op_var40_efect_ult1", "imp_op_var40_efect_ult3", "imp_op_var40_ult1", "imp_op_var41_comer_ult1", "imp_op_var41_comer_ult3", "imp_op_var41_efect_ult1", "imp_op_var41_efect_ult3", "imp_op_var41_ult1", "imp_op_var39_efect_ult1", "imp_op_var39_efect_ult3", "imp_op_var39_ult1", "imp_sal_var16_ult1", "num_var5", "num_op_var40_ult1", "num_op_var40_ult3", "num_op_var41_hace2", "num_op_var41_hace3", "num_op_var41_ult1", "num_op_var41_ult3", "num_op_var39_hace2", "num_op_var39_hace3", "num_op_var39_ult1", "num_op_var39_ult3", "num_var30_0", "num_var30", "num_var35", "num_var37_med_ult2", "num_var37_0", "num_var37", "num_var39_0", "num_var41_0", "num_var42", "saldo_var1", "saldo_var5", "saldo_var6", "saldo_var8", "saldo_var12", "saldo_var13_corto", "saldo_var13_largo", "saldo_var13_medio", "saldo_var13", "saldo_var14", "saldo_var17", "saldo_var18", "saldo_var20", "saldo_var24", "saldo_var26", "saldo_var25", "saldo_var29", "saldo_var30", "saldo_var31", "saldo_var32", "saldo_var33", "saldo_var34", "saldo_var37", "saldo_var40", "saldo_var42", "saldo_var44", "var36", "delta_imp_amort_var18_1y3", "delta_imp_amort_var34_1y3", "delta_imp_aport_var13_1y3", "delta_imp_aport_var17_1y3", "delta_imp_aport_var33_1y3", "delta_imp_compra_var44_1y3", "delta_imp_reemb_var13_1y3", "delta_imp_reemb_var17_1y3", "delta_imp_reemb_var33_1y3", "delta_imp_trasp_var17_in_1y3", "delta_imp_trasp_var17_out_1y3", "delta_imp_trasp_var33_in_1y3", "delta_imp_trasp_var33_out_1y3", "delta_imp_venta_var44_1y3", "delta_num_aport_var13_1y3", "delta_num_aport_var17_1y3", "delta_num_aport_var33_1y3", "delta_num_compra_var44_1y3", "delta_num_reemb_var13_1y3", "delta_num_reemb_var17_1y3", "delta_num_reemb_var33_1y3", "delta_num_trasp_var17_in_1y3", "delta_num_trasp_var17_out_1y3", "delta_num_trasp_var33_in_1y3", "delta_num_trasp_var33_out_1y3", "delta_num_venta_var44_1y3", "imp_amort_var18_ult1", "imp_amort_var34_ult1", "imp_aport_var13_hace3", "imp_aport_var13_ult1", "imp_aport_var17_hace3", "imp_aport_var17_ult1", "imp_aport_var33_hace3", "imp_aport_var33_ult1", "imp_var7_emit_ult1", "imp_var7_recib_ult1", "imp_compra_var44_hace3", "imp_compra_var44_ult1", "imp_reemb_var13_ult1", "imp_reemb_var17_hace3", "imp_reemb_var17_ult1", "imp_reemb_var33_ult1", "imp_var43_emit_ult1", "imp_trans_var37_ult1", "imp_trasp_var17_in_hace3", "imp_trasp_var17_in_ult1", "imp_trasp_var17_out_ult1", "imp_trasp_var33_in_hace3", "imp_trasp_var33_in_ult1", "imp_trasp_var33_out_ult1", "imp_venta_var44_hace3", "imp_venta_var44_ult1", "var21", "num_var22_hace2", "num_var22_hace3", "num_var22_ult1", "num_var22_ult3", "num_med_var22_ult3", "num_med_var45_ult3", "num_meses_var5_ult3", "num_op_var39_comer_ult1", "num_op_var39_comer_ult3", "num_op_var40_comer_ult1", "num_op_var40_comer_ult3", "num_op_var41_comer_ult1", "num_op_var41_comer_ult3", "num_op_var41_efect_ult1", "num_op_var41_efect_ult3", "num_op_var39_efect_ult1", "num_op_var39_efect_ult3", "num_var43_emit_ult1", "num_var43_recib_ult1", "num_trasp_var11_ult1", "num_var45_hace2", "num_var45_hace3", "num_var45_ult1", "num_var45_ult3", "saldo_medio_var5_hace2", "saldo_medio_var5_hace3", "saldo_medio_var5_ult1", "saldo_medio_var5_ult3", "saldo_medio_var8_hace2", "saldo_medio_var8_hace3", "saldo_medio_var8_ult1", "saldo_medio_var8_ult3", "saldo_medio_var12_hace2", "saldo_medio_var12_hace3", "saldo_medio_var12_ult1", "saldo_medio_var12_ult3", "saldo_medio_var13_corto_hace2", "saldo_medio_var13_corto_hace3", "saldo_medio_var13_corto_ult1", "saldo_medio_var13_corto_ult3", "saldo_medio_var13_largo_hace2", "saldo_medio_var13_largo_hace3", "saldo_medio_var13_largo_ult1", "saldo_medio_var13_largo_ult3", "saldo_medio_var13_medio_hace2", "saldo_medio_var13_medio_ult1", "saldo_medio_var13_medio_ult3", "saldo_medio_var17_hace2", "saldo_medio_var17_hace3", "saldo_medio_var17_ult1", "saldo_medio_var17_ult3", "saldo_medio_var29_hace2", "saldo_medio_var29_ult1", "saldo_medio_var29_ult3", "saldo_medio_var33_hace2", "saldo_medio_var33_hace3", "saldo_medio_var33_ult1", "saldo_medio_var33_ult3", "saldo_medio_var44_hace2", "saldo_medio_var44_hace3", "saldo_medio_var44_ult1", "saldo_medio_var44_ult3", "var38")
    val cdf = rdata.filter("label > 0.0")

    val firstdf = rdata.limit(3000)

    val joineddf = firstdf.union(cdf)

    val data = joineddf.dropDuplicates()

    // Use meta to create numClasses for label column
    val meta = NominalAttribute.defaultAttr.withName("label").withValues("0.0", "1.0").toMetadata

    val dataWithMeta = data.withColumn("label", $"label".as("label", meta))

    val Array(training, test) = dataWithMeta.randomSplit(Array(0.8, 0.2))

    // Create an array of all required columns without ID
    val featureCols = Array("var3", "var15", "imp_ent_var16_ult1", "imp_op_var39_comer_ult1", "imp_op_var39_comer_ult3", "imp_op_var40_comer_ult1", "imp_op_var40_comer_ult3", "imp_op_var40_efect_ult1", "imp_op_var40_efect_ult3", "imp_op_var40_ult1", "imp_op_var41_comer_ult1", "imp_op_var41_comer_ult3", "imp_op_var41_efect_ult1", "imp_op_var41_efect_ult3", "imp_op_var41_ult1", "imp_op_var39_efect_ult1", "imp_op_var39_efect_ult3", "imp_op_var39_ult1", "imp_sal_var16_ult1", "num_var5", "num_op_var40_ult1", "num_op_var40_ult3", "num_op_var41_hace2", "num_op_var41_hace3", "num_op_var41_ult1", "num_op_var41_ult3", "num_op_var39_hace2", "num_op_var39_hace3", "num_op_var39_ult1", "num_op_var39_ult3", "num_var30_0", "num_var30", "num_var35", "num_var37_med_ult2", "num_var37_0", "num_var37", "num_var39_0", "num_var41_0", "num_var42", "saldo_var1", "saldo_var5", "saldo_var6", "saldo_var8", "saldo_var12", "saldo_var13_corto", "saldo_var13_largo", "saldo_var13_medio", "saldo_var13", "saldo_var14", "saldo_var17", "saldo_var18", "saldo_var20", "saldo_var24", "saldo_var26", "saldo_var25", "saldo_var29", "saldo_var30", "saldo_var31", "saldo_var32", "saldo_var33", "saldo_var34", "saldo_var37", "saldo_var40", "saldo_var42", "saldo_var44", "var36", "delta_imp_amort_var18_1y3", "delta_imp_amort_var34_1y3", "delta_imp_aport_var13_1y3", "delta_imp_aport_var17_1y3", "delta_imp_aport_var33_1y3", "delta_imp_compra_var44_1y3", "delta_imp_reemb_var13_1y3", "delta_imp_reemb_var17_1y3", "delta_imp_reemb_var33_1y3", "delta_imp_trasp_var17_in_1y3", "delta_imp_trasp_var17_out_1y3", "delta_imp_trasp_var33_in_1y3", "delta_imp_trasp_var33_out_1y3", "delta_imp_venta_var44_1y3", "delta_num_aport_var13_1y3", "delta_num_aport_var17_1y3", "delta_num_aport_var33_1y3", "delta_num_compra_var44_1y3", "delta_num_reemb_var13_1y3", "delta_num_reemb_var17_1y3", "delta_num_reemb_var33_1y3", "delta_num_trasp_var17_in_1y3", "delta_num_trasp_var17_out_1y3", "delta_num_trasp_var33_in_1y3", "delta_num_trasp_var33_out_1y3", "delta_num_venta_var44_1y3", "imp_amort_var18_ult1", "imp_amort_var34_ult1", "imp_aport_var13_hace3", "imp_aport_var13_ult1", "imp_aport_var17_hace3", "imp_aport_var17_ult1", "imp_aport_var33_hace3", "imp_aport_var33_ult1", "imp_var7_emit_ult1", "imp_var7_recib_ult1", "imp_compra_var44_hace3", "imp_compra_var44_ult1", "imp_reemb_var13_ult1", "imp_reemb_var17_hace3", "imp_reemb_var17_ult1", "imp_reemb_var33_ult1", "imp_var43_emit_ult1", "imp_trans_var37_ult1", "imp_trasp_var17_in_hace3", "imp_trasp_var17_in_ult1", "imp_trasp_var17_out_ult1", "imp_trasp_var33_in_hace3", "imp_trasp_var33_in_ult1", "imp_trasp_var33_out_ult1", "imp_venta_var44_hace3", "imp_venta_var44_ult1", "var21", "num_var22_hace2", "num_var22_hace3", "num_var22_ult1", "num_var22_ult3", "num_med_var22_ult3", "num_med_var45_ult3", "num_meses_var5_ult3", "num_op_var39_comer_ult1", "num_op_var39_comer_ult3", "num_op_var40_comer_ult1", "num_op_var40_comer_ult3", "num_op_var41_comer_ult1", "num_op_var41_comer_ult3", "num_op_var41_efect_ult1", "num_op_var41_efect_ult3", "num_op_var39_efect_ult1", "num_op_var39_efect_ult3", "num_var43_emit_ult1", "num_var43_recib_ult1", "num_trasp_var11_ult1", "num_var45_hace2", "num_var45_hace3", "num_var45_ult1", "num_var45_ult3", "saldo_medio_var5_hace2", "saldo_medio_var5_hace3", "saldo_medio_var5_ult1", "saldo_medio_var5_ult3", "saldo_medio_var8_hace2", "saldo_medio_var8_hace3", "saldo_medio_var8_ult1", "saldo_medio_var8_ult3", "saldo_medio_var12_hace2", "saldo_medio_var12_hace3", "saldo_medio_var12_ult1", "saldo_medio_var12_ult3", "saldo_medio_var13_corto_hace2", "saldo_medio_var13_corto_hace3", "saldo_medio_var13_corto_ult1", "saldo_medio_var13_corto_ult3", "saldo_medio_var13_largo_hace2", "saldo_medio_var13_largo_hace3", "saldo_medio_var13_largo_ult1", "saldo_medio_var13_largo_ult3", "saldo_medio_var13_medio_hace2", "saldo_medio_var13_medio_ult1", "saldo_medio_var13_medio_ult3", "saldo_medio_var17_hace2", "saldo_medio_var17_hace3", "saldo_medio_var17_ult1", "saldo_medio_var17_ult3", "saldo_medio_var29_hace2", "saldo_medio_var29_ult1", "saldo_medio_var29_ult3", "saldo_medio_var33_hace2", "saldo_medio_var33_hace3", "saldo_medio_var33_ult1", "saldo_medio_var33_ult3", "saldo_medio_var44_hace2", "saldo_medio_var44_hace3", "saldo_medio_var44_ult1", "saldo_medio_var44_ult3", "var38")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    // Train a GBT classifier
    //val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(30)
    val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(10).setMaxBins(32)

    // Setup Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler, rf))

    //val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(10, 30)).build()
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    // Cross validator to choose the best hyper parameters for rf
    // val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5) // Use 3+ in practice
    val cvModel = pipeline.fit(training)

    // Fit against best model
    //val cvModel = cv.fit(training)
    // Save model
    cvModel.save("models/santander/rf")


    // and load it back in during production
    //val GBTModel = PipelineModel.load("santander/model/mlpipeline/GBTModel")
    // Calculate Area under ROC for training data
    println("Area under ROC for training data = " + evaluator.evaluate(cvModel.transform(training)))

    // Calculate for test data
    println("Area under ROC for test data = " + evaluator.evaluate(cvModel.transform(test)))


    // For submission
    val rawTest = spark.read.option("header", "true").option("inferSchema", "true").csv("data/santander/test.csv")

    val tdata = rawTest.select("ID", "var3", "var15", "imp_ent_var16_ult1", "imp_op_var39_comer_ult1", "imp_op_var39_comer_ult3", "imp_op_var40_comer_ult1", "imp_op_var40_comer_ult3", "imp_op_var40_efect_ult1", "imp_op_var40_efect_ult3", "imp_op_var40_ult1", "imp_op_var41_comer_ult1", "imp_op_var41_comer_ult3", "imp_op_var41_efect_ult1", "imp_op_var41_efect_ult3", "imp_op_var41_ult1", "imp_op_var39_efect_ult1", "imp_op_var39_efect_ult3", "imp_op_var39_ult1", "imp_sal_var16_ult1", "num_var5", "num_op_var40_ult1", "num_op_var40_ult3", "num_op_var41_hace2", "num_op_var41_hace3", "num_op_var41_ult1", "num_op_var41_ult3", "num_op_var39_hace2", "num_op_var39_hace3", "num_op_var39_ult1", "num_op_var39_ult3", "num_var30_0", "num_var30", "num_var35", "num_var37_med_ult2", "num_var37_0", "num_var37", "num_var39_0", "num_var41_0", "num_var42", "saldo_var1", "saldo_var5", "saldo_var6", "saldo_var8", "saldo_var12", "saldo_var13_corto", "saldo_var13_largo", "saldo_var13_medio", "saldo_var13", "saldo_var14", "saldo_var17", "saldo_var18", "saldo_var20", "saldo_var24", "saldo_var26", "saldo_var25", "saldo_var29", "saldo_var30", "saldo_var31", "saldo_var32", "saldo_var33", "saldo_var34", "saldo_var37", "saldo_var40", "saldo_var42", "saldo_var44", "var36", "delta_imp_amort_var18_1y3", "delta_imp_amort_var34_1y3", "delta_imp_aport_var13_1y3", "delta_imp_aport_var17_1y3", "delta_imp_aport_var33_1y3", "delta_imp_compra_var44_1y3", "delta_imp_reemb_var13_1y3", "delta_imp_reemb_var17_1y3", "delta_imp_reemb_var33_1y3", "delta_imp_trasp_var17_in_1y3", "delta_imp_trasp_var17_out_1y3", "delta_imp_trasp_var33_in_1y3", "delta_imp_trasp_var33_out_1y3", "delta_imp_venta_var44_1y3", "delta_num_aport_var13_1y3", "delta_num_aport_var17_1y3", "delta_num_aport_var33_1y3", "delta_num_compra_var44_1y3", "delta_num_reemb_var13_1y3", "delta_num_reemb_var17_1y3", "delta_num_reemb_var33_1y3", "delta_num_trasp_var17_in_1y3", "delta_num_trasp_var17_out_1y3", "delta_num_trasp_var33_in_1y3", "delta_num_trasp_var33_out_1y3", "delta_num_venta_var44_1y3", "imp_amort_var18_ult1", "imp_amort_var34_ult1", "imp_aport_var13_hace3", "imp_aport_var13_ult1", "imp_aport_var17_hace3", "imp_aport_var17_ult1", "imp_aport_var33_hace3", "imp_aport_var33_ult1", "imp_var7_emit_ult1", "imp_var7_recib_ult1", "imp_compra_var44_hace3", "imp_compra_var44_ult1", "imp_reemb_var13_ult1", "imp_reemb_var17_hace3", "imp_reemb_var17_ult1", "imp_reemb_var33_ult1", "imp_var43_emit_ult1", "imp_trans_var37_ult1", "imp_trasp_var17_in_hace3", "imp_trasp_var17_in_ult1", "imp_trasp_var17_out_ult1", "imp_trasp_var33_in_hace3", "imp_trasp_var33_in_ult1", "imp_trasp_var33_out_ult1", "imp_venta_var44_hace3", "imp_venta_var44_ult1", "var21", "num_var22_hace2", "num_var22_hace3", "num_var22_ult1", "num_var22_ult3", "num_med_var22_ult3", "num_med_var45_ult3", "num_meses_var5_ult3", "num_op_var39_comer_ult1", "num_op_var39_comer_ult3", "num_op_var40_comer_ult1", "num_op_var40_comer_ult3", "num_op_var41_comer_ult1", "num_op_var41_comer_ult3", "num_op_var41_efect_ult1", "num_op_var41_efect_ult3", "num_op_var39_efect_ult1", "num_op_var39_efect_ult3", "num_var43_emit_ult1", "num_var43_recib_ult1", "num_trasp_var11_ult1", "num_var45_hace2", "num_var45_hace3", "num_var45_ult1", "num_var45_ult3", "saldo_medio_var5_hace2", "saldo_medio_var5_hace3", "saldo_medio_var5_ult1", "saldo_medio_var5_ult3", "saldo_medio_var8_hace2", "saldo_medio_var8_hace3", "saldo_medio_var8_ult1", "saldo_medio_var8_ult3", "saldo_medio_var12_hace2", "saldo_medio_var12_hace3", "saldo_medio_var12_ult1", "saldo_medio_var12_ult3", "saldo_medio_var13_corto_hace2", "saldo_medio_var13_corto_hace3", "saldo_medio_var13_corto_ult1", "saldo_medio_var13_corto_ult3", "saldo_medio_var13_largo_hace2", "saldo_medio_var13_largo_hace3", "saldo_medio_var13_largo_ult1", "saldo_medio_var13_largo_ult3", "saldo_medio_var13_medio_hace2", "saldo_medio_var13_medio_ult1", "saldo_medio_var13_medio_ult3", "saldo_medio_var17_hace2", "saldo_medio_var17_hace3", "saldo_medio_var17_ult1", "saldo_medio_var17_ult3", "saldo_medio_var29_hace2", "saldo_medio_var29_ult1", "saldo_medio_var29_ult3", "saldo_medio_var33_hace2", "saldo_medio_var33_hace3", "saldo_medio_var33_ult1", "saldo_medio_var33_ult3", "saldo_medio_var44_hace2", "saldo_medio_var44_hace3", "saldo_medio_var44_ult1", "saldo_medio_var44_ult3", "var38")

    val tModel = cvModel.transform(tdata)

    val finaldf = tModel.selectExpr("ID", "cast(prediction as int) as prediction").repartition(1)

    // Convert to rdd and save the dataframe
    finaldf.rdd.saveAsTextFile("output/santander/rf1")


    spark.stop()
    // Process result output format is [ID, prediction]
    //rev part-00000 | cut -c2- | rev | cut -c2- >ml_gbt1.csv
  }
}
