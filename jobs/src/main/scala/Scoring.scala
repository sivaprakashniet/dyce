import ReadWriteImplicits._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, GBTClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel, RandomForestRegressionModel}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json

object Scoring {
  var spark: SparkSession = null
  var request_id: String = null

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val new_dataset_path = getValue(arguments(1))
    val modified_dataset_path = getValue(arguments(2))
    val s3_bucketname = getValue(arguments(3))

    val job_name = "Scoring"
    spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, new_dataset_path).cache()

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[ScoreMetadata](request_body).get
    println("s3a://" + s3_bucketname + "/" + modified_dataset_path)
    val scored_dataset = scoreDataset(s3_bucketname, df, request_body_as_json)
    scored_dataset.write.parquet("s3a://" + s3_bucketname + "/" + modified_dataset_path)
    spark.stop()
  }

  def constructDataFrame(dataframe: DataFrame,
                         independent_columns: List[String]): DataFrame = {

    dataframe.select(independent_columns.map(c => col(c)): _*).na.drop()
  }

  def scoreDataset(s3_bucketname: String, df: DataFrame, metadata: ScoreMetadata) = {
    val independent_columns = metadata.model_meta.input_column.split(",").toList
    var transformed_dataframe = constructDataFrame(df, independent_columns)
    val model_path = "s3a://" + s3_bucketname + "/" + metadata.model_meta.model_path
    val label_column = metadata.model_meta.output_column

    val model = CrossValidatorModel.load(model_path)
    val stages_list = model.bestModel.asInstanceOf[PipelineModel].stages


    val new_df = metadata.model_meta.model_type match {
      case "DecisionTree Classification Model" => {
        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("DecisionTreeClassificationModel"))
        val i =  stages_list.indexOf(gmodel(0))

        val tmp_df = model.transform(transformed_dataframe)
        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
          .asInstanceOf[DecisionTreeClassificationModel]

        val trans_df = splitProbabilityColumn(tmp_df, tree_model.getLabelCol, metadata.model_meta.output_column)

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        trans_df.withColumnRenamed("prediction", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }

      case "DecisionTree Regression Model" => {
        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("DecisionTreeRegressionModel"))
        val i =  stages_list.indexOf(gmodel(0))
        val trans_df = model.transform(transformed_dataframe)

        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
          .asInstanceOf[DecisionTreeRegressionModel]

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        trans_df.withColumnRenamed("prediction", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }


      case "GBT Classification Model" => {
        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("GBTClassificationModel"))
        val i =  stages_list.indexOf(gmodel(0))

        val tmp_df = model.transform(transformed_dataframe)
        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
                          .asInstanceOf[GBTClassificationModel]

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        //val trans_df = splitProbabilityColumn(tmp_df, tree_model.getLabelCol, metadata.model_meta.output_column)
        tmp_df.withColumnRenamed("predictedLabel", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }

      case "GBT Regression Model" => {

        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("GBTRegressionModel"))
        val i =  stages_list.indexOf(gmodel(0))

        val trans_df = model.transform(transformed_dataframe)
        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
          .asInstanceOf[GBTRegressionModel]

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        trans_df.withColumnRenamed("prediction", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }

      case "RandomForest Classification Model" => {
        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("RandomForestClassificationModel"))
        val i =  stages_list.indexOf(gmodel(0))

        val tmp_df = model.transform(transformed_dataframe)
        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
          .asInstanceOf[RandomForestClassificationModel]

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        val trans_df = splitProbabilityColumn(tmp_df, tree_model.getLabelCol, metadata.model_meta.output_column)
        trans_df.withColumnRenamed("prediction", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }

      case "RandomForest Regression Model" => {
        val gmodel = stages_list.toList.filter(x => x.toString.startsWith("RandomForestRegressionModel"))
        val i =  stages_list.indexOf(gmodel(0))

        val trans_df = model.transform(transformed_dataframe)
        val tree_model = model.bestModel.asInstanceOf[PipelineModel].stages(i)
          .asInstanceOf[RandomForestRegressionModel]

        val output_cols = stages_list.filter(x => x.toString()
          .startsWith("strIdx")).map(y => y.asInstanceOf[StringIndexerModel].getOutputCol)

        trans_df.withColumnRenamed("prediction", metadata.model_meta.output_column)
          .drop("features").drop(tree_model.getFeaturesCol).drop("indexedFeatures").drop(output_cols: _*)
      }
    }

    new_df
  }

  def splitProbabilityColumn(input_df: DataFrame,
                             output_column_name: String, dependent_col_name: String): DataFrame = {

    val extractValue = udf { (x: DenseVector, i: Int) => x(i) }

    val number_of_categories = input_df.select("probability").head
      .toSeq(0).asInstanceOf[DenseVector].values.length

    val cnt: Int = if(number_of_categories <= 2)  number_of_categories else 2

    val cols = (1 to cnt).toList.map(c => dependent_col_name + "_" + c)
    var custom_added_cols_dataframe  = input_df

    def addProbabilityColumns(cols: List[String], extractValue: UserDefinedFunction, i: Int): Unit = {
      if(cols.nonEmpty){
        custom_added_cols_dataframe = custom_added_cols_dataframe.withColumn(cols.head, extractValue(col("probability"),lit(i)))
        addProbabilityColumns(cols.tail, extractValue, i+1)
      }
    }
    addProbabilityColumns(cols, extractValue, 0)

    custom_added_cols_dataframe.drop("rawPrediction").drop("probability")

  }


}