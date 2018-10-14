import java.util.UUID

import org.apache.spark.ml.{PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json
import ReadWriteImplicits._

// New Decision tree lib
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object DecisionTree {
  //Spark instance
  val threshold_for_numerical_columns: Int = 20
  val s3bucket = "s3n://" + S3Config.bucket_name + "/"
  var spark: SparkSession = null
  var request_id: String = null
  var number_of_instances_per_node: Map[Int, Long] = Map()
  var mean_and_median_per_node: Map[Int, RegDetailsPerNode] = Map()
  var number_of_instances_per_node_per_class: Map[Int, List[ClassDetailsPerNode]] = Map()
  var split_condition_for_nodes: Map[Int, String] = Map()
  var connector_length: Map[Int, Double] = Map()
  var categorical_column_labels: Map[String, List[String]] = Map()
  var pipeline_stages: Array[PipelineStage with MLWritable] = Array()

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val s3_bucketname = getValue(arguments(3))

    val model_path = s3bucket + getValue(arguments(4))
    val model_temp_path = model_path + "_desicion_tree"

    val job_name = "DecisionTree"
    spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[DecisionTreeMeta](request_body).get
    val tree = BuildDecisionTree(df, request_body_as_json, model_path, model_temp_path)
    val response_body = Json.toJson(tree).toString
    StorageReadWrite.saveResponse(response_filepath, response_body)
    spark.stop()
  }

  def current_date_time: Long = System.currentTimeMillis()

  def currentDateTime = System.currentTimeMillis().toString

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
                         new_categorical_column_names: List[String],
                         max_categories: Int): Unit = {
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

      categoricalIndexer(df, old_categorical_column_names.tail,
        new_categorical_column_names.tail, max_categories)
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
        num_distinct_values < threshold_for_numerical_columns) ||
        (datatype != "double" && datatype != "integer")
    }

    (categorical_columns, input_columns diff categorical_columns)
  }

  // Build DecitionTree
  def BuildDecisionTree(dataframe: DataFrame,
                        metadata: DecisionTreeMeta, model_path: String,
                        model_temp_path: String): DecisionTreeResult = {
    val count = dataframe.select(metadata.output_column).distinct.count
    val params: Parameters = metadata.parameters
    var train_sample = params.training_data / 100.00
    var test_sample = params.test_data / 100.00
    var nFolds = 2

    if (params.nfolds.getOrElse(2) > 1) {
      nFolds = params.nfolds.getOrElse(2)
    }
    val preprocessed_dataframe = preprocessDataframe(dataframe,
      metadata.input_column :+ metadata.output_column)

    val (old_categorical_columns, old_continuous_columns) =
      getCategoricalAndContinuousColumns(preprocessed_dataframe, metadata.input_column)

    val (categorical_columns, independent_features_column, dependent_column) =
      createNewColumnNames(old_categorical_columns)

    val continuous_columns = old_continuous_columns

    val independent_columns = categorical_columns ++ continuous_columns

    val list_original_cols = old_categorical_columns ++ old_continuous_columns

    categoricalIndexer(preprocessed_dataframe, old_categorical_columns,
      categorical_columns, params.max_bins)
    val prepared_dataframe = prepareDataframe(preprocessed_dataframe,
      independent_columns, independent_features_column,
      metadata.output_column, dependent_column)

    val Array(trainingData, testData) = prepared_dataframe.randomSplit(Array(train_sample, test_sample))

    val output_datatype = prepared_dataframe.select(dependent_column).schema.toList(0).dataType.typeName

    if (count > 10 && (output_datatype == "integer" || output_datatype == "double")) {

      val indexed_independent_feature_column = "Indexed" + independent_features_column

      val dt = new DecisionTreeRegressor()
        .setLabelCol(dependent_column)
        .setFeaturesCol(independent_features_column)
        .setMaxBins(params.max_bins)
        .setMaxDepth(params.max_depth)
        .setMinInstancesPerNode(params.min_instances_per_node)

      val dt_index_in_pipeline = pipeline_stages.length

      pipeline_stages = pipeline_stages :+ dt

      val pipeline = new Pipeline()
        .setStages(pipeline_stages)

      val evaluator = new RegressionEvaluator()
        .setLabelCol(dependent_column)
        .setPredictionCol("prediction")
        .setMetricName("rmse")


      val paramGrid = new ParamGridBuilder().build()
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(nFolds)

      val model = cv.fit(trainingData)
      model.save(model_path)

      // Make predictions.
      val tr_predictions = model.transform(trainingData)


      var predictionAndlabel_tr = tr_predictions.select("prediction", dependent_column)

      predictionAndlabel_tr = predictionAndlabel_tr
        .withColumn("predictedLabel", predictionAndlabel_tr("prediction")
          .cast(DoubleType))


      val rmse_tr = evaluator.evaluate(tr_predictions)

      val treeModel = model.bestModel.asInstanceOf[PipelineModel].stages(dt_index_in_pipeline)
        .asInstanceOf[DecisionTreeRegressionModel]

      val mfeature = treeModel.featureImportances
      var variable_importance: Map[String, String] = Map()
      for (index <- 0 until mfeature.size) {
        variable_importance += list_original_cols(index) -> mfeature(index).toString
      }

      treeModel.save(model_temp_path)

      var read = spark.sqlContext.read.parquet(model_temp_path + "/data")
      var node_df = read.toDF()
      var nodes = node_df.toJSON.take(node_df.count().toInt).toList

      getDecisionTreeInformation(dataframe, node_df, list_original_cols,
        metadata.output_column, "Regression")

      nodes = nodes.map { n =>
        val node_info = Json.fromJson[Node](Json.parse(n)).get
        val new_info = node_info.copy(node_instances =
          number_of_instances_per_node.get(node_info.id),
          split_condition = split_condition_for_nodes.get(node_info.id),
          reg_node_details = mean_and_median_per_node.get(node_info.id),
          connector_length = connector_length.get(node_info.id))
        Json.toJson(new_info).toString
      }
      var str = treeModel.toDebugString

      var (js_string, header, depth, node, rules) = treeJson(str, list_original_cols,
        "Regression")

      val model_get_parameters: Map[String, String] = Map(
        "tree_rules" -> rules.replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n"),
        "json_string" -> js_string,
        "number_of_nodes" -> node.toString,
        "decision_tree_type" -> "Regression Model",
        "header" -> header,
        "depth" -> depth.toString,
        "training_sample" -> trainingData.count().toString,
        "test_sample" -> testData.count().toString,
        "min_info_gain" -> (treeModel.getMinInfoGain).toString
      )


      val tree_summary = if(test_sample != 0.0) {
        val predictions = model.transform(testData)
        var predictionAndlabel = predictions.select("prediction", dependent_column)

        predictionAndlabel = predictionAndlabel
          .withColumn("predictedLabel", predictionAndlabel("prediction")
            .cast(DoubleType))

        val rmse = evaluator.evaluate(predictions)
        new DecisionTreeSummary(nodes,variable_importance, List(),
          Map(), List(), Map(), rmse.toDouble, List())
      } else {
        new DecisionTreeSummary(nodes,variable_importance, List(),
          Map(), List(), Map(), 0.0, List())
      }

      val tr_tree_summary = new DecisionTreeSummary(nodes, variable_importance, List(), Map(),
        List(), Map(), rmse_tr.toDouble, List())

      val result_params = new Parameters(treeModel.getImpurity,
        treeModel.getMaxBins, treeModel.getMaxDepth, treeModel.getMinInstancesPerNode,
        metadata.parameters.training_data, metadata.parameters.test_data,metadata.parameters.nfolds)

      val result_meta = new DecisionTreeMeta(metadata.name, list_original_cols,
        metadata.output_column, metadata.dataset_id, metadata.dataset_name, result_params)


      new DecisionTreeResult(metadata.dataset_id, metadata.name, result_meta,
        tr_tree_summary, tree_summary, model_get_parameters, currentDateTime)

    }
    else {

      val indexed_dependent_column = "Indexed" + dependent_column

      val labelIndexer = new StringIndexer()
        .setInputCol(dependent_column)
        .setOutputCol(indexed_dependent_column)
        .fit(prepared_dataframe)

      val dt = new DecisionTreeClassifier()
        .setLabelCol(indexed_dependent_column)
        .setFeaturesCol(independent_features_column)
        .setImpurity(params.impurity)
        .setMaxBins(params.max_bins)
        .setMaxDepth(params.max_depth)
        .setMinInstancesPerNode(params.min_instances_per_node)

      // Convert indexed labels back to original labels.
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      val pipeline_stages_index = pipeline_stages.length + 1

      pipeline_stages = pipeline_stages ++ Array(labelIndexer, dt,
        labelConverter)

      val pipeline = new Pipeline()
        .setStages(pipeline_stages)

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol(indexed_dependent_column)
        .setPredictionCol("prediction")
        .setMetricName("accuracy")

      val paramGrid = new ParamGridBuilder().build()
      val cv = new CrossValidator()
        .setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(nFolds)

      val model = cv.fit(trainingData)
      model.save(model_path)

      // Make predictions.
      val predictions_training = model.transform(trainingData)


      // Select (prediction, true label) and compute test error.

      val (tr_cal_labels_metric, tr_summary, tr_labels, tr_stats) =
        calMetrics(predictions_training.select("prediction", indexed_dependent_column), indexed_dependent_column)



      val accuracy_tr = evaluator.evaluate(predictions_training)

      val treeModel = model.bestModel.asInstanceOf[PipelineModel].stages(pipeline_stages_index)
        .asInstanceOf[DecisionTreeClassificationModel]
      val mfeature = treeModel.featureImportances
      var variable_importance: Map[String, String] = Map()
      for (index <- 0 until mfeature.size) {
        variable_importance += list_original_cols(index) -> mfeature(index).toString
      }
      var str = treeModel.toDebugString

      //val local_path = "tmp/myDecisionTreeClassificationModel/" + current_date_time
      treeModel.save(model_temp_path)
      var read = spark.sqlContext.read.parquet(model_temp_path + "/data")

      var nodes_df = read.toDF()
      var nodes = nodes_df.toJSON.take(nodes_df.count().toInt).toList

      getDecisionTreeInformation(dataframe, nodes_df, list_original_cols, metadata.output_column,
        "Classification")

      nodes = nodes.map { n =>
        val node_info = Json.fromJson[Node](Json.parse(n)).get
        val nc = number_of_instances_per_node_per_class.get(node_info.id)
        val total_node_instances = number_of_instances_per_node.get(node_info.id).get
        val new_info = node_info.copy(node_instances =
          Option(total_node_instances),
          node_instances_per_class = Option(nc.get.map(x =>
            x.copy(percentage = x.percentage / total_node_instances * 100))),
          split_condition = split_condition_for_nodes.get(node_info.id),
          connector_length = connector_length.get(node_info.id),
          classes = Some(labelIndexer.labels.toList))
        Json.toJson(new_info).toString
      }

      var (js_string, header, depth, node, rules) = treeJson(str, list_original_cols,
        "Classification",labelIndexer.labels.toList)

      val model_get_parameters: Map[String, String] = Map(
        "tree_rules" -> rules.replaceAll("(\\r|\\n|\\r\\n)+", "\\\\n"),
        "json_string" -> js_string,
        "number_of_nodes" -> node.toString,
        "decision_tree_type" -> "Classification Model",
        "header" -> header,
        "depth" -> depth.toString,
        "created_date" -> currentDateTime,
        "training_sample" -> trainingData.count().toString,
        "test_sample" -> testData.count().toString,
        "min_info_gain" -> (treeModel.getMinInfoGain).toString
      )

      val test_summary = if(test_sample != 0.0) {
        val predictions = model.transform(testData)
        val predictionAndlabel = predictions.select("prediction", indexed_dependent_column)
        val (cal_labels_metric, summary, labels, stats) =
          calMetrics(predictionAndlabel, indexed_dependent_column)

        val accuracy = evaluator.evaluate(predictions)

        new DecisionTreeSummary(nodes,
          variable_importance,
          cal_labels_metric,
          summary, labels, stats, (1.0 - accuracy).toDouble,
          labelIndexer.labels.toList)
      } else {
        new DecisionTreeSummary(nodes,
          variable_importance,List(),Map(),List(),Map(),0.0, List())
      }

      val tree_summary_tr = new DecisionTreeSummary(nodes,
        variable_importance,
        tr_cal_labels_metric,
        tr_summary, tr_labels, tr_stats, (1.0 - accuracy_tr).toDouble,
        labelIndexer.labels.toList)

      val result_params = new Parameters(treeModel.getImpurity,
        treeModel.getMaxBins, treeModel.getMaxDepth, treeModel.getMinInstancesPerNode,
        metadata.parameters.training_data, metadata.parameters.test_data, metadata.parameters.nfolds)

      val result_meta = new DecisionTreeMeta(metadata.name, list_original_cols,
        metadata.output_column, metadata.dataset_id, metadata.dataset_name, result_params)

      new DecisionTreeResult(metadata.dataset_id, metadata.name, result_meta,
        tree_summary_tr, test_summary, model_get_parameters, currentDateTime)
    }
  }

  def treeJson(debug_string: String, clm: List[String], tree_type: String,
               output_labels: List[String] = List()): (String, String,
    Integer, Integer, String) = {
    var cmt = debug_string.split("nodes")
    var l = debug_string.indexOf("nodes\n")
    var str = debug_string.substring(l + 6)
    var lines = str.split("\n")
    lines = lines map { r =>
      var x = r.split(" ").toList
      val features_info_parsed = if(x.contains("(feature")) {
        val i1 = x.indexOf("(feature")
        val feature_clm_name = clm(x(i1 + 1).toInt)
        val feature_indices_replaced = r.replaceAll("feature " + x(i1 + 1).trim, feature_clm_name)

        val feature_value_indices_replaced = if(x.contains("in")) {
          val i2 = x.indexOf("in")
          val transformed_values = x(i2+1).replace("{","").replace("})","")
            .split(",").map(v => {
            val feature_clm_labels = categorical_column_labels.getOrElse(feature_clm_name,List())
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
    var metaList = cmt(0).split(" ").toList
    var n = metaList.indexOf("depth")
    val result_str = cmt(0) + " nodes \n" + lines.mkString("\n")
    (lines.mkString("||"), cmt(0) + "nodes", metaList(n + 1).toInt, metaList(n + 3).toInt, result_str)
  }

  def calMetrics(predictionAndlabel: DataFrame, dependent_column: String): (List[Map[String, String]],
    Map[String, Double], List[Double],
    Map[String, String]) = {
    val predictionModel = predictionAndlabel
      .withColumn("prediction", predictionAndlabel("prediction").cast(DoubleType))
      .withColumn(dependent_column, predictionAndlabel(dependent_column).cast(DoubleType))

    val datalist = predictionModel.rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val metrics = new MulticlassMetrics(datalist)
    val labels = metrics.labels
    var cal_labels_metric: List[Map[String, String]] = List()
    labels.foreach { l =>
      var m: Map[String, String] = Map("label" -> l.toString,
        "precision" -> metrics.precision(l).toString,
        "recall" -> metrics.recall(l).toString,
        "FPR" -> metrics.falsePositiveRate(l).toString,
        "f1_score" -> metrics.fMeasure(l).toString
      )
      cal_labels_metric = cal_labels_metric :+ m
    }
    val metric = metrics.confusionMatrix
    var cal_weighted_stats: Map[String, String] = Map(
      "confusion_matrix" -> metric.rowIter.toList.map(x => x.toArray
        .toList.mkString(" ")).mkString("\\n"),
      "weighted_precision" -> metrics.weightedPrecision.toString,
      "weighted_recall" -> metrics.weightedRecall.toString,
      "weighted_f1_score" -> metrics.weightedFMeasure.toString,
      "weighted_false_positive_rate" -> metrics.weightedFalsePositiveRate.toString)

    var summary: Map[String, Double] = Map(
      "precision" -> metrics.precision,
      "recall" -> metrics.recall,
      "F1_Score" -> metrics.fMeasure)

    (cal_labels_metric, summary, labels.toList, cal_weighted_stats)
  }

  def getDecisionTreeInformation(df: DataFrame, dt_df: DataFrame,
                                 feature_columns: List[String], output_column: String,
                                 tree_type: String) = {
    val node_data = getNodeInformation(dt_df)
    val temp_df_name = "dataset"
    df.createOrReplaceTempView(temp_df_name)
    val reg_mean_query_prefix = "SELECT AVG(" + output_column + ") FROM " + temp_df_name + " WHERE 1 = 1"
    val reg_select_query_prefix = "SELECT " + output_column + " FROM " + temp_df_name + " WHERE 1 = 1"

    val reg_count_query_prefix = "SELECT COUNT(*) FROM " + temp_df_name + " WHERE 1 = 1"
    val class_query_prefix = "SELECT " + output_column + ", COUNT(*) FROM " + temp_df_name + " WHERE 1 = 1"
    val query_suffix = " GROUP BY 1"
    countDetailsForNodes(reg_count_query_prefix, reg_mean_query_prefix,
      reg_select_query_prefix,
      class_query_prefix, query_suffix, df, node_data.filter(_.id == 0)(0),
      node_data, feature_columns, "", tree_type, output_column)

    findConnectorLength(node_data, 0, 1.0)
  }

  def getNodeInformation(dt_df: DataFrame) = {
    val json = dt_df.toJSON
    val all_node_details = json.collectAsList().toArray.toList
    all_node_details map { node_details =>
      Json.fromJson[Node](Json.parse(node_details.toString)).get
    }
  }

  def calculateMeanAndMedianPerNode(node_id: Int, reg_mean_query_prefix: String,
                                    reg_select_query_prefix: String,
                                    where_clause: String, node_instance_count: Long,
                                    dependent_column: String) = {
    val calc_mean_query = reg_mean_query_prefix + where_clause
    val mean = spark.sql(calc_mean_query)
      .rdd.map(x => x.toSeq.toList).collect()
      .toList(0)(0).asInstanceOf[Double]

    val get_records_query = reg_select_query_prefix + where_clause
    val records = spark.sql(get_records_query)
    var median:Double = 0.0
    if(records.count() > 0){
      val stats = records.stat.approxQuantile(dependent_column, Array(0.5), 0.25)
      if(stats.length > 0){
        median = records.stat.approxQuantile(dependent_column, Array(0.5), 0.25)(0)
      }
    }

    mean_and_median_per_node = mean_and_median_per_node +
      (node_id -> new RegDetailsPerNode(mean, median))
  }

  def countDetailsForNodes(reg_query_prefix: String, reg_mean_query_prefix: String,
                           reg_select_query_prefix: String,
                           class_query_prefix: String, query_suffix: String,
                           df: DataFrame, node: Node, node_details: List[Node],
                           feature_columns: List[String], where_clause: String,
                           tree_type: String, dependent_column: String): Unit = {
    if (tree_type == "Classification") {
      val query = class_query_prefix + where_clause + query_suffix

      val class_details_in_this_node =
        spark.sql(query).rdd.map(x => x.toSeq.toList).collect()

      val class_details_for_this_node = class_details_in_this_node.map { case c =>
        val l = if(c(0) == null) " " else c(0).toString

        new ClassDetailsPerNode(l, c(1).asInstanceOf[Long])
      }.toList

      number_of_instances_per_node_per_class = number_of_instances_per_node_per_class +
        (node.id -> class_details_for_this_node)

      val instance_count_in_this_node = class_details_for_this_node
        .foldLeft(0)((a, b) => a + b.percentage.toInt)

      number_of_instances_per_node = number_of_instances_per_node +
        (node.id -> instance_count_in_this_node)

    } else {
      val query = reg_query_prefix + where_clause
      val instance_count_in_this_node =
        spark.sql(query).rdd.map(x => x.toSeq.toList)
          .collect().toList(0)(0).asInstanceOf[Long]

      calculateMeanAndMedianPerNode(node.id, reg_mean_query_prefix,
        reg_select_query_prefix,
        where_clause, instance_count_in_this_node, dependent_column)

      number_of_instances_per_node = number_of_instances_per_node +
        (node.id -> instance_count_in_this_node)
    }

    val index_of_feature_used_for_splitting = node.split.featureIndex
    val threshold = node.split.leftCategoriesOrThreshold
    val number_of_categories = node.split.numCategories
    val feature_for_splitting = if (index_of_feature_used_for_splitting != -1)
      feature_columns(index_of_feature_used_for_splitting) else ""
    val feature_labels = if (index_of_feature_used_for_splitting != -1)
      categorical_column_labels.get(feature_columns(index_of_feature_used_for_splitting)).getOrElse(List()) else List()
    val threshold_for_splitting = if (threshold.isEmpty) 0 else threshold(0)

    if (node.leftChild != -1) {
      val left_node = node_details.filter(_.id == node.leftChild)(0)
      val split_condition =
        if (number_of_categories > -1){
          val list_arr =
            threshold.filter(c => feature_labels.length > c.toInt)
              .map(c => "'" + feature_labels(c.toInt) + "'")
          if (list_arr.length > 0)  feature_for_splitting+" IN (" + list_arr.mkString(",") + ")"  else " 1 = 1 "



        } else feature_for_splitting + " <= " + threshold_for_splitting
      val new_clause = where_clause + " AND " + split_condition
      split_condition_for_nodes = split_condition_for_nodes + (left_node.id -> split_condition)
      countDetailsForNodes(reg_query_prefix, reg_mean_query_prefix,
        reg_select_query_prefix, class_query_prefix, query_suffix, df,
        left_node, node_details, feature_columns, new_clause, tree_type, dependent_column)
    }

    if (node.rightChild != -1) {
      val right_node = node_details.filter(_.id == node.rightChild)(0)
      val split_condition =
        if (number_of_categories > -1){
          val list_arr =
            threshold.filter(c => feature_labels.length > c.toInt)
              .map(c => "'" + feature_labels(c.toInt) + "'")

          if (list_arr.length > 0)  feature_for_splitting+" NOT IN (" + list_arr.mkString(",") + ")"
          else " 1 = 1 "
        }
        else feature_for_splitting + " > " + threshold_for_splitting
      val new_clause = where_clause + " AND " + split_condition
      split_condition_for_nodes = split_condition_for_nodes + (right_node.id -> split_condition)
      countDetailsForNodes(reg_query_prefix, reg_mean_query_prefix,
        reg_select_query_prefix, class_query_prefix, query_suffix, df,
        right_node, node_details, feature_columns, new_clause, tree_type, dependent_column)
    }
  }

  def findConnectorLength(node_data: List[Node], node_id: Int,
                          previous_node_count: Double): Unit = {
    val current_node = node_data.filter(_.id == node_id)(0)
    val current_node_count = number_of_instances_per_node(node_id).toDouble

    connector_length = connector_length +
      (if (previous_node_count == 0) (node_id -> 0)
      else (node_id -> current_node_count / previous_node_count))
    if (current_node.leftChild != -1) {
      findConnectorLength(node_data, current_node.leftChild, current_node_count)
    }

    if (current_node.rightChild != -1) {
      findConnectorLength(node_data, current_node.rightChild, current_node_count)
    }
  }
}