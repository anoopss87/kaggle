package redhat.ml

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}


/**
 * Created by jshetty on 8/16/16.
 * Predict Business value outcome of Redhat users using behavioral analysis
 */
object RedHatBaseline {

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: <train_file> <people_file> <test_file> <output_file>")
      System.exit(1)
    }
    val trainFile = args(0)
    val peopleFile = args(1)
    val testFile = args(2)
    val outputFile = args(3)

    val spark = SparkSession.builder.master("local[8]").appName("Groupo Bimbo Baseline").getOrCreate()
    val train = spark.read.option("header", "true").option("inferSchema", "true").csv(trainFile).cache() // 1.3GB to 70 MB in memory
    val people = spark.read.option("header", "true").option("inferSchema", "true").csv(peopleFile).cache() // 47 MB to 8.7 MB

    // Clean Data
   // val dateTrimmer = new DateTrimmer().setInputCol("date").setoutputCol("date_trimmed")

    val train = spark.read.option("header", "true").option("inferSchema", "true").csv(trainFile).cache()
    val people = spark.read.option("header", "true").option("inferSchema", "true").csv(peopleFile).cache()

    // 1. Rename columns in train
    val newcols = train.columns.map{x => ( x + "_train")}
    val newtrain = train.toDF(newcols: _*)

    // Replace true false columns with binary 0 and 1
    val peopleTFColumns = Array("char_10","char_11","char_12","char_13","char_14","char_15","char_16","char_17","char_18","char_19","char_20","char_21","char_22","char_23","char_24","char_25","char_26","char_27","char_28","char_29","char_30","char_31","char_32","char_33","char_34","char_35","char_36","char_37")
    var newpeople = people
    for(col <- peopleTFColumns){
      newpeople = newpeople.withColumn(col,trueFalseReplace(people(col)))
    }

    // Join two datasets using people_id
    val train_people = newtrain.join(newpeople, newtrain("people_id_train") === newpeople("people_id")).drop(newtrain("people_id_train"))

    train_people.count

    // Still not tested form here
    // Apply StringIndexers to multiple columns:
    val activityColumns = Array("","","")
    val stringIndexers: Array[PipelineStage] = activityColumns.map{
      x => new StringIndexer().setInputCol(x).setOutputCol( x + "categoryIndex")
    }

    val pipeline1 = new Pipeline().setStages(stringIndexers)
    val indexerModel = pipeline1.fit(train).transform(train)

    // one hot encoders for multiple columns
    val oneHotColumns = Array("",)
    val oneHotEncoders: Array[PipelineStage] = oneHotColumns.map{
      x => new StringIndexer().setInputCol(x).setOutputCol( x + "Encode")
    }

    val pipeline2 = new Pipeline().setStages(oneHotEncoders)
    val oneHotEncoderModel = pipeline2.fit(indexerModel).transform(indexerModel)

}
