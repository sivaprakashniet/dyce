case class Metrics(mean:String, stddev:String, min:String, max:String, distinct:String, median: String)

case class ColumnDetails(column_id: String, column_name: String, position: Int, column_datatype: String, number_of_bins: Int,
                         bins: Option[Array[String]], metrics: Option[Metrics], decimal: Int, ends: Option[Array[Double]])

case class UnivariateRequest(dataset_id: String, dataset_name: String, dataset_path : String,
                             columns: List[ColumnDetails])

case class Histogram(x: List[String], y: List[Long])

case class UnivariateSummary(dataset_name: String, dataset_id: String, column_id: String,position: Int,
                            column_name: String, column_datatype: String, bins: List[String], histogram: Histogram,
                            metrics: Metrics, missing: String, decimal: Int, number_of_bins: Int)

case class BivariatePair(column_1_id: String, column_2_id: String)

case class BivariateRequest(dataset_id: String, dataset_name: String,
                            columns: List[ColumnDetails], pairs: List[BivariatePair])

case class ColumnMetadata(id: Option[String], name: String, description: Option[String], position: Long,
                          datatype: String, format: String, display_format: String, separator: Boolean,
                          decimal: Long, dataset_id: Option[String], visibility: Boolean,
                          calculated: Boolean, formula: String, metrics: String,
                          created_date: Option[String], modified_date: Option[String],
                          new_name: Option[String], status: Option[String], raw_formula: Option[String])

case class FileMetadata(id: Option[String], file_path: String, dataset_name: String,
                        created_date: Option[String], updated_date: Option[String],
                        user_id: Option[String],number_of_rows: Option[Long],
                        number_of_columns: Option[Long], file_size: Option[Long],
                        parquet_file_path: Option[String], dataset_status: Option[String],
                        decision_tree_count: Option[Long], anomaly_count: Option[Long],
                        gbt_count: Option[Long], random_forest_count: Option[Long])

case class PreProcessMetaData(dataset: FileMetadata, columns: List[ColumnMetadata])

case class MosaicPlotDatapoint(x: String, y: String, count: Long, xpercent: Double, ypercent: Double)

case class BivariateSummary(dataset_id: String, column_1_id: String, column_2_id: String,
                            column_1_name: String, column_2_name: String, covariance: String,
                            correlation: String, mosaic_plot_data: Array[MosaicPlotDatapoint])

case class MosaicPlotData(dataset_id: String, column_1_id: String, column_2_id: String,
                          column_1_name: String, column_2_name: String,

                          mosaic_plot_data: Array[MosaicPlotDatapoint])

case class CorrelationSummary(dataset_id: String, column_1_id: String, column_2_id: String,
                          column_1_name: String, column_2_name: String, correlation: String)

case class UpdateDatasetRequest(schema_list: List[ColumnMetadata])

case class Parameters(impurity: String, max_bins: Int, max_depth: Int, min_instances_per_node: Int,
                      training_data: Int, test_data: Int, nfolds: Option[Int])

case class DecisionTreeMeta(name: String, input_column: List[String], output_column:String,
                            dataset_id: String, dataset_name: String, parameters:Parameters)

//case class ConfusionMatrix(matrix: List[Double], num_rows: Int, num_cols: Int)

case class DecisionTreeSummary(nodes: List[String], variable_importance:Map[String, String],
                               cal_labels_metric:List[Map[String, String]], summary:Map[String, Double],
                               labels: List[Double], stats:Map[String, String], error: Double,
                               output_labels: List[String])

case class DecisionTreeResult(dataset_id:String, name:String, meta_data:DecisionTreeMeta,
                              summary:DecisionTreeSummary, test_summary: DecisionTreeSummary,
                              model_parameters:Map[String, String], created_date: String)

case class AnomalyMeta(name: String, dataset_id: String, dataset_name: String, trees:Int,
                       sample_proportions: Int, no_of_anomalies: Int, features: Int,
                       feature_columns: List[String],bootstrap: Boolean, replacement:Boolean,
                       random_split: Int, path: Option[String], created_date: Option[Long])

case class GBTParameters(max_bins: Int, max_depth: Int,
                         min_instances_per_node: Int,
                         num_iterations: Int, training_data: Int, test_data: Int,
                         nfolds: Option[Int])

case class GBTMetadata(name: String, input_column: List[String], output_column: String,
                       dataset_id: String, dataset_name: String, parameters: GBTParameters)

case class GBTSummary(cal_labels_metric:List[Map[String, String]], summary:Map[String, Double],
                      labels: List[Double], stats:Map[String, String], error: String,
                      output_labels: List[String])

case class GBTResult(dataset_id:String, name:String, meta_data:GBTMetadata,
                     summary: GBTSummary, test_summary: GBTSummary,
                     model_type: String, rules: String, variable_importance: Map[String, String],
                     created_date: String, test_sample:Long, training_sample:Long)

case class RandomForestParameters(impurity: String, max_bins: Int, max_depth: Int,
                                  min_instances_per_node: Int, max_classes: Int, num_trees: Int,
                                  feature_subset_strategy: String, training_data: Int, test_data: Int,
                                  nfolds: Option[Int])

case class RandomForestMetadata(name: String, input_column: List[String],
                                output_column: String, dataset_id: String,
                                dataset_name: String, parameters: RandomForestParameters)

case class RandomForestSummary(cal_labels_metric:List[Map[String, String]],
                      summary:Map[String, Double], labels: List[Double],
                      stats:Map[String, String], error:String,
                               output_labels: List[String])

case class RandomForestResult(dataset_id:String, name:String, meta_data:RandomForestMetadata,
                              summary: RandomForestSummary,
                              test_summary: RandomForestSummary,
                              model_type: String, rules: String,
                              variable_importance:Map[String, String],
                              created_date: String,
                              test_sample:Long, training_sample:Long
                             )

case class ModelMetadata(id:Option[String], model_name: String, dataset_id: String, user_id: String,
                         dataset_name: String,model_type: String, input_column:String,
                         output_column: String, model_path: String, created_date: Option[Long])

case class ScoreMetadata(model_meta: ModelMetadata, dataset: FileMetadata)

case class Split(featureIndex: Int, leftCategoriesOrThreshold: List[Double], numCategories: Int)

case class ClassDetailsPerNode(label: String, percentage: Double)

case class RegDetailsPerNode(mean: Double, median: Double)

case class DatasetResult(num_of_rows: Long, num_of_cols:Int)

case class Node(id: Int, prediction: Double, impurity: Double, impurityStats: List[Double],
                gain: Double, leftChild: Int, rightChild: Int, split: Split,
                node_instances: Option[Long], split_condition: Option[String],
                node_instances_per_class: Option[List[ClassDetailsPerNode]],
                reg_node_details: Option[RegDetailsPerNode],
                connector_length: Option[Double], classes: Option[List[String]])

case class DBConnection(id: Option[String], connection_name: String, connection_method: String,
                        host_name:String, port: Option[Int], username: String, password: String,
                        default_schema: Option[String], user_id: Option[String],
                        connection_db_type:Option[String], created_date: Option[Long],
                        modified_date: Option[Long])

case class IngestParameters(query: String, db_conn: DBConnection, tables: List[String])