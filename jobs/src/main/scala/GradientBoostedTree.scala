import java.util.UUID
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import play.api.libs.json.Json
import ReadWriteImplicits._


object GradientBoostedTree {
  var spark: SparkSession = null

  val limit_of_distinct_values_for_categorical_columns = 3

  def current_date_time: String = System.currentTimeMillis().toString

  val s3bucket = "s3n://" + S3Config.bucket_name + "/"

  var pipeline_stages: Array[PipelineStage with MLWritable] = Array()
  var categorical_column_labels: Map[String, List[String]] = Map()


  def main(args: Array[String]): Unit = {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val s3_bucketname = getValue(arguments(3))

    val model_path = s3bucket + getValue(arguments(4))

    val job_name = "GradientBoostedTrees"
    spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[GBTMetadata](request_body).get
    val gbt = buildGBT(df, request_body_as_json, model_path)
    val response_body = Json.toJson(gbt).toString
    StorageReadWrite.saveResponse(response_filepath, response_body)
    spark.stop()
  }

  // Duplicate
  def prepareDataframe(dataframe: DataFrame,
                       independent_columns: List[String],
                       independent_features_column: String,
                       dependent_column_old: String,
                       dependent_column: String): DataFrame = {
    val new_df = dataframe.withColumnRenamed(dependent_column_old, dependent_column)
    val assembler = new VectorAssembler()
      .setInputCols(independent_columns.toArray)
      .setOutputCol(independent_features_column)

    pipeline_stages = pipeline_stages :+ assembler
    new_df
    //assembler.transform(new_df)
  }

  def randomNum() = UUID.randomUUID().toString

  def createNewColumnNames(input_columns: List[String]): (List[String], String, String) =
    (input_columns.map(i => i + randomNum()), "features" + randomNum(), "label" + randomNum())


  //Indexes categorical variables in a dataframe
  def categoricalIndexer(df: DataFrame, old_categorical_column_names: List[String],
                         new_categorical_column_names: List[String], max_categories: Int): Unit = {
    if (!old_categorical_column_names.isEmpty) {
      val current_name = old_categorical_column_names.head
      val new_name = new_categorical_column_names.head

      val datatype = df.schema(current_name).dataType.typeName
      val num_distinct_values = df.select(current_name).distinct().count()

      val catIndexer = new StringIndexer()
        .setInputCol(current_name)
        .setOutputCol(new_name)
        .fit(df)

      categorical_column_labels = categorical_column_labels + (current_name -> catIndexer.labels.toList)

      pipeline_stages = pipeline_stages :+ catIndexer

      categoricalIndexer(df, old_categorical_column_names.tail, new_categorical_column_names.tail, max_categories)
    }
  }

  def preprocessDataframe(dataframe: DataFrame, column_names: List[String]) =
    dataframe.select(column_names.map(col(_)): _*).na.drop()

  def getCategoricalAndContinuousColumns(df: DataFrame, input_columns: List[String]):
  (List[String], List[String]) = {
    val categorical_columns = input_columns filter { c =>
      val datatype = df.schema(c).dataType.typeName
      val num_distinct_values = df.select(c).distinct().count()

      ((datatype == "double" || datatype == "integer") &&
        num_distinct_values < limit_of_distinct_values_for_categorical_columns) ||
        (datatype != "double" && datatype != "integer")
    }

    (categorical_columns, input_columns diff categorical_columns)
  }

  //code duplicate


  def buildGBT(dataframe: DataFrame, metadata: GBTMetadata, model_path: String): GBTResult = {
    val parameters = metadata.parameters
    val training_percentage = parameters.training_data / 100.0
    val test_percentage = parameters.test_data / 100.00
    val nfolds:Int = if(parameters.nfolds.get < 2) 2 else parameters.nfolds.get

    val preprocessed_dataframe = preprocessDataframe(dataframe, metadata.input_column :+ metadata.output_column)

    val (old_categorical_columns, old_continuous_columns) = getCategoricalAndContinuousColumns(preprocessed_dataframe, metadata.input_column)

    val (categorical_columns, independent_features_column, dependent_column) =
      createNewColumnNames(old_categorical_columns)

    val continuous_columns = old_continuous_columns

    val independent_columns = categorical_columns ++ continuous_columns
    val list_original_cols = old_categorical_columns ++ old_continuous_columns

    categoricalIndexer(preprocessed_dataframe, old_categorical_columns, categorical_columns,
      parameters.max_bins)

    val prepared_dataframe = prepareDataframe(preprocessed_dataframe,
      independent_columns, independent_features_column, metadata.output_column,
      dependent_column)

    val Array(trainingData, testData) = prepared_dataframe.randomSplit(Array(training_percentage,
      test_percentage))

    val count_distinct_values_in_output = prepared_dataframe.select(dependent_column).distinct.count

    val output_datatype = prepared_dataframe.select(dependent_column).schema.toList(0).dataType.typeName

    if (count_distinct_values_in_output >= limit_of_distinct_values_for_categorical_columns &&
      (output_datatype == "integer" || output_datatype == "double"))
      buildGBTForRegression(prepared_dataframe, metadata,
        trainingData, testData, list_original_cols, model_path, Some(nfolds),
        independent_columns, independent_features_column, dependent_column,
        test_percentage)
    else
      buildGBTForClassification(prepared_dataframe, metadata,
        trainingData, testData, list_original_cols, model_path, Some(nfolds),
        independent_columns, independent_features_column, dependent_column,
        test_percentage)
  }

  def buildGBTForClassification(dataframe: DataFrame, metadata: GBTMetadata,
                                trainingData: Dataset[Row], testData: Dataset[Row],
                                input_columns: List[String], model_path: String,
                                nfolds: Option[Int],
                                independent_columns: List[String], independent_features_column: String,
                                dependent_column: String,
                                test_percentage: Double): GBTResult = {

    val indexed_dependent_column = "Indexed" + dependent_column

    //Indexing the dependent column
    val parameters = metadata.parameters
    val labelIndexer = new StringIndexer()
      .setInputCol(dependent_column)
      .setOutputCol(indexed_dependent_column)
      .fit(dataframe)

    val output_labels = labelIndexer.labels.toList

    val gbt = new GBTClassifier()
      .setLabelCol(indexed_dependent_column)
      .setFeaturesCol(independent_features_column)
      .setMaxBins(parameters.max_bins)
      .setMaxDepth(parameters.max_depth)
      .setMinInstancesPerNode(parameters.min_instances_per_node)
      .setMaxIter(parameters.num_iterations)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    pipeline_stages = pipeline_stages :+ labelIndexer
    val model_index = pipeline_stages.length
    pipeline_stages = pipeline_stages ++ Array(gbt, labelConverter)

    val pipeline = new Pipeline()
      .setStages(pipeline_stages)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(indexed_dependent_column)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val default_folds = 5
    val paramGrid = new ParamGridBuilder().build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nfolds.getOrElse(default_folds))

    val model = cv.fit(trainingData)
        model.save(model_path)
    val predictions_training = model.transform(trainingData)


    val error_training = (1.0 - evaluator.evaluate(predictions_training)).toString


    val test_metrics = if(test_percentage != 0.0) {
      val predictions_test = model.transform(testData)
      val error_test = (1.0 - evaluator.evaluate(predictions_test)).toString
      calcMetrics(predictions_test, error_test, indexed_dependent_column,
        output_labels)
    } else {
      new GBTSummary(List(), Map(), List(), Map(), "0.0", List())
    }

    val gbtModel = model.bestModel.asInstanceOf[PipelineModel].stages(model_index)
      .asInstanceOf[GBTClassificationModel]
    //gbtModel.save(model_path)

    val feature_importances = gbtModel.featureImportances
    val variable_importance: Map[String, String] = (input_columns
      zip feature_importances.toArray.map(_.toString)).toMap

    val training_metrics = calcMetrics(predictions_training, error_training,
      indexed_dependent_column, output_labels)

    val transformed_debug_string = transformDebugString(gbtModel.toDebugString,
      input_columns, "Classification", labelIndexer.labels.toList)

    new GBTResult(metadata.dataset_id, metadata.name, metadata,
      training_metrics, test_metrics, "Classification Model",
      transformed_debug_string, variable_importance, current_date_time,
      testData.count(), trainingData.count())

  }

  def buildGBTForRegression(dataframe: DataFrame, metadata: GBTMetadata,
                            trainingData: Dataset[Row], testData: Dataset[Row],
                            input_columns: List[String], model_path: String,
                            nfolds: Option[Int],
                            independent_columns: List[String], independent_features_column: String,
                            dependent_column: String,
                            test_percentage: Double): GBTResult = {

    val parameters = metadata.parameters

    val gbt = new GBTRegressor()
      .setLabelCol(dependent_column)
      .setFeaturesCol(independent_features_column)
      .setMaxBins(parameters.max_bins)
      .setMaxDepth(parameters.max_depth)
      .setMinInstancesPerNode(parameters.min_instances_per_node)

    val model_index = pipeline_stages.length
    pipeline_stages = pipeline_stages ++ Array(gbt)

    val pipeline = new Pipeline()
      .setStages(pipeline_stages)

    //Evaluation of rmse
    val evaluator = new RegressionEvaluator()
      .setLabelCol(dependent_column)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val default_folds = 5
    val paramGrid = new ParamGridBuilder().build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nfolds.getOrElse(5))

    val model = cv.fit(trainingData)
    model.save(model_path)
    val predictions_training = model.transform(trainingData)

    val test_metrics = if(test_percentage != 0.0) {
      val predictions_test = model.transform(testData)
      val rmse_test = evaluator.evaluate(predictions_test).toString
      new GBTSummary(List(), Map(),List(), Map(), rmse_test, List())
    } else {
      new GBTSummary(List(), Map(),List(), Map(), "0.0", List())
    }


    val gbtModel = model.bestModel.asInstanceOf[PipelineModel].stages(model_index).asInstanceOf[GBTRegressionModel]

    val rmse_training = evaluator.evaluate(predictions_training).toString

    val feature_importances = gbtModel.featureImportances
    val variable_importance: Map[String, String] = (input_columns
      zip feature_importances.toArray.map(_.toString)).toMap

    val training_metrics = new GBTSummary(List(), Map(),List(), Map(), rmse_training, List())

    val transformed_debug_string = transformDebugString(gbtModel.toDebugString,
      input_columns, "Regression")

    new GBTResult(metadata.dataset_id, metadata.name, metadata,
      training_metrics, test_metrics, "Regression Model",
      transformed_debug_string, variable_importance, current_date_time,
      testData.count(), trainingData.count())

  }

  def calcMetrics(predictions: DataFrame, error: String, indexed_dependent_column: String,
                  output_labels: List[String]): GBTSummary = {
    val prediction_and_label = predictions.select("prediction", indexed_dependent_column)
    val predictionModel = prediction_and_label.withColumn("prediction",
      prediction_and_label("prediction").cast(DoubleType)).withColumn(indexed_dependent_column,
      prediction_and_label(indexed_dependent_column).cast(DoubleType))
    val data_model = predictionModel.rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val metrics = new MulticlassMetrics(data_model)
    val labels = metrics.labels

    val labels_metrics = labels map { case label =>
      Map("label" -> label.toString,
        "precision" -> metrics.precision(label).toString,
        "recall" -> metrics.recall(label).toString,
        "FPR" -> metrics.falsePositiveRate(label).toString,
        "f1_score" -> metrics.fMeasure(label).toString
      )
    } toList

    val confusion_matrix = metrics.confusionMatrix

    var cal_weighted_stats: Map[String, String] = Map(
      "confusion_matrix" -> confusion_matrix.rowIter.toList.map(x => x.toArray.toList.mkString(" ")).mkString("\\n"),
      "weighted_precision" -> metrics.weightedPrecision.toString,
      "weighted_recall" -> metrics.weightedRecall.toString,
      "weighted_f1_score" -> metrics.weightedFMeasure.toString,
      "weighted_false_positive_rate" -> metrics.weightedFalsePositiveRate.toString)

    var summary: Map[String, Double] = Map(
      "precision" -> metrics.precision,
      "recall" -> metrics.recall,
      "F1_Score" -> metrics.fMeasure)

    new GBTSummary(labels_metrics, summary, labels.toList, cal_weighted_stats, error,
      output_labels)
  }

  def transformDebugString(debug_string: String, clm_names: List[String],
                           tree_type: String,
                           output_labels: List[String] = List()) = {
    val lines = debug_string.split("\n") map { r =>
      var x = r.split(" ").toList
      val features_info_parsed = if(x.contains("(feature")) {
        val i1 = x.indexOf("(feature")
        val feature_clm_name = clm_names(x(i1 + 1).toInt)
        val feature_indices_replaced = r.replaceAll("feature " + x(i1 + 1).trim, feature_clm_name)

        val feature_value_indices_replaced = if(x.contains("in")) {
          val i2 = x.indexOf("in")
          val transformed_values = x(i2+1).replace("{","").replace("})","")
            .split(",").map(v => {
            val feature_clm_labels = categorical_column_labels.getOrElse(feature_clm_name, List())
            if(!feature_clm_labels.isEmpty)
              feature_clm_labels(v.toDouble.toInt)
            else ""
          }).mkString(",")
          feature_indices_replaced.replace(x(i2+1),"{" + transformed_values + "})")
        } else feature_indices_replaced

        feature_value_indices_replaced
      }
      else r

      if(x.contains("Predict:") && tree_type == "Classification") {
        val i3 = x.indexOf("Predict:")
        features_info_parsed.replace(x(i3+1), output_labels(x(i3+1).toDouble.toInt))
      } else features_info_parsed
    }

    val result_str = lines.mkString("\n")
    result_str
  }

}