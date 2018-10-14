package entities

import play.api.libs.json.{JsObject, JsValue}

// For access control list
case class Acl(id: Option[String], user_id: String, object_id: String,
               resource_type: String, privilege: String,
               permission: String)

// For user
case class User(id: Option[String], first_name: String, last_name: String,
                username: String, email: String, created_date: Option[Long],
                modified_date: Option[Long], profile_image: Option[String], role: String)

// For dataset
case class ColumnMetadata(id: Option[String], name: String, description:Option[String], position: Long,
                          datatype: String, format: String, display_format: String, separator: Boolean,
                          decimal: Long, dataset_id: Option[String], visibility: Boolean,
                          calculated: Boolean, formula: String, metrics: String,
                          created_date: Option[String], modified_date: Option[String],
                          new_name: Option[String], status: Option[String],raw_formula:Option[String])


case class FileMetadata(id: Option[String], file_path: String, dataset_name: String,
                        created_date: Option[String], updated_date: Option[String],
                        user_id: Option[String], number_of_rows: Option[Long],
                        number_of_columns: Option[Long], file_size: Option[Long],
                        parquet_file_path: Option[String], download_file_path:Option[String],
                        dataset_status: Option[String], decision_tree_count: Option[Long],
                        anomaly_count: Option[Long], gbt_count: Option[Long],
                        random_forest_count: Option[Long], model_count: Option[Long],
                        download_status: Option[String])

case class PreviewData(preview_data: List[List[String]], schema_list: List[ColumnMetadata])

case class SchemaList(schema_list: List[ColumnMetadata])

case class Metrics(mean:String, stddev:String, min:String, max:String, distinct:String, median: String)

case class Columns(column_id: String, column_name: String, position: Int,
                   column_datatype: String, number_of_bins: Int, decimal: Int)

//For univariate
case class UnivariateMeta(dataset_id: String, dataset_path: String,
                          dataset_name: String, columns: List[Columns])

case class UnivariateSummary(id: Option[String], dataset_id: String, column_id: String,
                             column_name: String, data_type: String, missing: Int,
                             histogram: Option[JsValue])

// For job controller
case class Job(job_id: Long, user_id: String, dataset_id: String, dataset_name: String, job_name: String,
               start_time: Long, end_time: Long, status: String,
               created_date: Long, request_json: Option[String], response_json: Option[String])

// For pre-processing
case class PreProcessMetaData(dataset: FileMetadata, columns: List[ColumnMetadata])

// For bivariate
case class ColumnDetails(column_id: String, name: String, datatype: String,
                         number_of_bins: Option[Int],
                         bins: Option[Array[String]], decimal: Option[Int])

case class BivariatePair(column_1_id: String, column_2_id: String)

case class BivariateRequest(id: String, dataset_id: String, dataset_name: String,
                            columns: List[JsObject], pairs: List[BivariatePair])

case class BivariateMetaRequest(dataset_id: String, column_1_id: String, column_2_id: String)

case class MosaicPlotDatapoint(x: String, y: String, count: Long, xpercent: Double, ypercent: Double)

case class BivariateSummary(dataset_id: String, column_1_id: String, column_2_id: String,
                            column_1_name: String, column_2_name: String, covariance: String,
                            correlation: String, mosaic_plot_data: Array[MosaicPlotDatapoint])

case class CorrelationSummary(columns: List[String], correlation_summary: List[List[String]])

case class DatasetView(schema: List[ColumnMetadata],
                       preview_data: List[Map[String, String]], dataset: FileMetadata,
                       number_of_rows: Long, number_of_cols: Int)

case class Parameters(impurity: String, max_bins: Int, max_depth: Int,
                      min_instances_per_node: Int,
                      training_data: Int, test_data: Int, nfolds: Int)

case class DecisionTreeMeta(name: String, input_column: List[String], output_column: String,
                            dataset_id: String, dataset_name: String, parameters: Parameters)

//Random forest meta data
case class RandomForestParameters(impurity: String, max_bins: Int, max_depth: Int,
                                  min_instances_per_node: Int, max_classes: Int, num_trees: Int,
                                  feature_subset_strategy: String,
                                  training_data: Int, test_data: Int, nfolds: Int)

case class RandomForestMeta(name: String, input_column: List[String], output_column: String,
                            dataset_id: String, dataset_name: String,
                            parameters: RandomForestParameters)

//GBT's meta data
case class GBTParameters(max_bins: Int, max_depth: Int,
                         min_instances_per_node: Int, num_iterations: Int,
                         training_data: Int, test_data: Int, nfolds: Int)

case class GBTMeta(name: String, input_column: List[String], output_column: String,
                   dataset_id: String, dataset_name: String, parameters: GBTParameters)


case class AnomalyMeta(name: String, dataset_id: String, dataset_name: String, trees: Int,
                       sample_proportions: Int, no_of_anomalies: Int, features: Int,
                       feature_columns: List[String], bootstrap: Boolean, replacement: Boolean,
                       random_split: Int, path: Option[String], created_date: Option[Long],
                       feature_column_ids: Option[List[String]])


case class AnomalySummary(id: Option[String], name: String, dataset_id: String, dataset_name: String, trees: Int,
                          sample_proportions: Int, no_of_anomalies: Int, features: Int,
                          bootstrap: Boolean, replacement: Boolean, random_split: Int,
                          path: Option[String], created_date: Option[Long], feature_column_ids: String)


case class AnomalyView(schema: List[ColumnMetadata],
                       preview_data: List[Map[String, String]],
                       anomaly: AnomalySummary, number_of_rows: Long, number_of_cols: Int)

// Save Model data's
case class ModelMeta(id:Option[String], model_name: String, dataset_id: String,
                     model_result_id: String, user_id: String, dataset_name: String,
                     model_type: String, input_column:String,output_column: String,
                     model_path: String, created_date: Option[Long])

case class ModelColumnMetadata(id: Option[String], model_meta_id: String,
                               name: String, description:Option[String], position: Int,
                               datatype: String, format: String, separator: Boolean,
                               decimal: Long, dataset_id: Option[String],calculated:Boolean, is_dv: Boolean)

case class ModelMetaSchema(meta: ModelMeta, input_column: List[ModelColumnMetadata],
                            output_column: ModelColumnMetadata)

case class ModelScoreMeta(model_id: String, name: String)

case class ModelScoreJob(name: String, model_meta: ModelMeta, dataset: FileMetadata)

case class ScoreSummary(id:Long, name:String, user_id:String,
                        dataset_id:String, dataset_name:String, score_path: String, created_date:Long)

case class ModelView(name: String, dataset_id: String,
                     dataset_name: String,schema: List[ColumnMetadata],
                     preview_data: List[Map[String, String]], number_of_rows: Long, number_of_cols: Int)

case class PrepareDataset(name: String, dataset_ids:List[String], query: String)

case class DataView(schema: List[ColumnMetadata],
                    preview_data: List[Map[String, String]])

case class CheckDuplicateName(dataset_id: Option[String], name: String)

case class CheckDuplidateModelName(tree_id: Option[String], name: String)


case class QueryMeta(schema_name: String, query: String, name: Option[String])

// Ingest from SQL Datasource

case class Field(position:Int, name: String, data_type: String)

case class QueryResult(sql_schema: List[Field], preview_data: List[List[String]], schema:List[ColumnMetadata])

case class DBConnection(id: Option[String],connection_name: String, connection_method: String,
                        host_name:String,port: Option[Int], username: String, password: String,
                         default_schema: Option[String], user_id: Option[String],
                         connection_db_type:Option[String], created_date: Option[Long],
                         modified_date: Option[Long])

case class FieldSchema(name: String, data_type: String, column_size: Option[Int],
                       is_nullable: Option[Boolean], decimal_digits:Option[Int], remarks: Option[String],
                       is_autoincrement:Option[Boolean])

case class TableInfo(table_name: String, columns: List[FieldSchema])

case class DBSchema(db_config:DBConnection, tables: List[TableInfo])

case class IngestRequest(query_meta: QueryMeta, tables:List[String],
                         columns: Option[List[ColumnMetadata]])

case class IngestParameters(query: String, name: Option[String], db_conn: DBConnection,
                            tables:List[String], columns: Option[List[ColumnMetadata]])


/**
  * Parameters for specifying numeric-binning analysis requests
  *
  */

case class ColumnBins(column_id: String, column_name: String, position: Int,
                      column_datatype: String, number_of_bins: Int, decimal: Int,
                      bins: Option[Array[String]], bin_ends: Option[Array[Double]])

case class NumericBinMeta(dataset_id: String, dataset_path: String,
                          dataset_name: String, columns:List[ColumnBins])

/**
*  Parameters for specifying Ad-hoc analysis requests
*
*   @param dataset_id Id of the dataset (the id in the table)
*   @param sql_query The query to be executed
*
*/
case class AdhocQueryParam(dataset_id: String,
                           sql_query: String
                          )
