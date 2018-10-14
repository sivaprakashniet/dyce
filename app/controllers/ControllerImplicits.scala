package controllers

import entities.{PreProcessMetaData, _}
import play.api.libs.json._


object ControllerImplicits {

  implicit val userWrites = Json.writes[User]
  implicit val userReads = Json.reads[User]

  implicit val aclWrites = Json.writes[Acl]
  implicit val aclReads = Json.reads[Acl]

  implicit val file_metadata_writes = Json.writes[FileMetadata]
  implicit val file_metadata_reads = Json.reads[FileMetadata]

  implicit val column_metadata_writes = Json.writes[ColumnMetadata]
  implicit val column_metadata_reads = Json.reads[ColumnMetadata]

  implicit val preview_data_writes = Json.writes[PreviewData]
  implicit val preview_data_reads = Json.reads[PreviewData]

  implicit val schema_list_writes = Json.writes[SchemaList]
  implicit val schema_list_reads = Json.reads[SchemaList]

  implicit val column_writes = Json.writes[Columns]
  implicit val column_reads = Json.reads[Columns]

  implicit val univariate_meta_writes = Json.writes[UnivariateMeta]
  implicit val univariate_meta_reads = Json.reads[UnivariateMeta]

  implicit val univariate_summary_writes = Json.writes[UnivariateSummary]
  implicit val univariate_summary_reads = Json.reads[UnivariateSummary]

  implicit val pre_process_meta_write = Json.writes[PreProcessMetaData]
  implicit val pre_process_meta_read = Json.reads[PreProcessMetaData]

  implicit val bivariate_pair_meta_writes = Json.writes[BivariatePair]
  implicit val bivariate__pair_meta_reads = Json.reads[BivariatePair]

  implicit val bivariate_meta_writes = Json.writes[BivariateRequest]
  implicit val bivariate_meta_reads = Json.reads[BivariateRequest]

  implicit val job_writes = Json.writes[Job]
  implicit val job_reads = Json.reads[Job]

  implicit val bi_meta_writes = Json.writes[BivariateMetaRequest]
  implicit val bi_meta_reads = Json.reads[BivariateMetaRequest]


  implicit val mosaic_plot_data_write = Json.writes[MosaicPlotDatapoint]
  implicit val mosaic_plot_data_read = Json.reads[MosaicPlotDatapoint]

  implicit val bivariate_summary_write = Json.writes[BivariateSummary]
  implicit val bivariate_summary_read = Json.reads[BivariateSummary]

  implicit val correlation_summary_write = Json.writes[CorrelationSummary]
  implicit val correlation_summary_read = Json.reads[CorrelationSummary]

  implicit val dataset_view_write = Json.writes[DatasetView]
  implicit val dataset_view_read = Json.reads[DatasetView]

  implicit val parameter_write = Json.writes[Parameters]
  implicit val parameter_read  = Json.reads[Parameters]

  implicit val decision_tree_write = Json.writes[DecisionTreeMeta]
  implicit val decision_tree_read  = Json.reads[DecisionTreeMeta]

  implicit val anomaly_meta_write = Json.writes[AnomalyMeta]
  implicit val anomaly_meta_read  = Json.reads[AnomalyMeta]

  implicit val anomaly_sum_write = Json.writes[AnomalySummary]
  implicit val anomaly_sum_read  = Json.reads[AnomalySummary]

  implicit val anomaly_view_write = Json.writes[AnomalyView]
  implicit val anomaly_view_read  = Json.reads[AnomalyView]

  implicit val rf_parameter_write = Json.writes[RandomForestParameters]
  implicit val rf_parameter_read  = Json.reads[RandomForestParameters]

  implicit val random_forest_write = Json.writes[RandomForestMeta]
  implicit val random_forest_read  = Json.reads[RandomForestMeta]

  implicit val gbt_parameter_write = Json.writes[GBTParameters]
  implicit val gbt_parameter_read  = Json.reads[GBTParameters]

  implicit val gbt_write = Json.writes[GBTMeta]
  implicit val gbt_read  = Json.reads[GBTMeta]

  implicit val model_meta_write = Json.writes[ModelMeta]
  implicit val model_meta_read  = Json.reads[ModelMeta]

  implicit val model_req_write  = Json.writes[ModelScoreMeta]
  implicit val model_req_read  = Json.reads[ModelScoreMeta]

  implicit val model_job_write  = Json.writes[ModelScoreJob]
  implicit val model_job_read  = Json.reads[ModelScoreJob]

  implicit val model_score_write  = Json.writes[ScoreSummary]
  implicit val model_score_read  = Json.reads[ScoreSummary]

  implicit val model_view_write  = Json.writes[ModelView]
  implicit val model_view_read  = Json.reads[ModelView]

  implicit val prepare_write  = Json.writes[PrepareDataset]
  implicit val prepare_read  = Json.reads[PrepareDataset]

  implicit val data_view_write  = Json.writes[DataView]
  implicit val data_view_read  = Json.reads[DataView]

  implicit val data_duplicate_write  = Json.writes[CheckDuplicateName]
  implicit val data_duplicate_read  = Json.reads[CheckDuplicateName]

  implicit val model_column_meta_write  = Json.writes[ModelColumnMetadata]
  implicit val model_column_meta_read  = Json.reads[ModelColumnMetadata]

  implicit val model_column_schema_write  = Json.writes[ModelMetaSchema]
  implicit val model_column_schema_read  = Json.reads[ModelMetaSchema]

  implicit val data_model_duplicate_write  = Json.writes[CheckDuplidateModelName]
  implicit val data_model_duplicate_read  = Json.reads[CheckDuplidateModelName]

  implicit val db_connection_write  = Json.writes[DBConnection]
  implicit val db_connection_read  = Json.reads[DBConnection]

  implicit val db_field_schema_write  = Json.writes[FieldSchema]
  implicit val db_field_schema_read  = Json.reads[FieldSchema]

  implicit val db_table_info_write  = Json.writes[TableInfo]
  implicit val db_table_info_read  = Json.reads[TableInfo]

  implicit val db_schema_write  = Json.writes[DBSchema]
  implicit val db_schema_read  = Json.reads[DBSchema]

  implicit val db_query_meta_write  = Json.writes[QueryMeta]
  implicit val db_query_meta_read  = Json.reads[QueryMeta]

  implicit val db_field_write  = Json.writes[Field]
  implicit val db_field_read  = Json.reads[Field]

  implicit val db_query_result_write  = Json.writes[QueryResult]
  implicit val db_query_result_read  = Json.reads[QueryResult]

  implicit val ingest_parameter_write  = Json.writes[IngestParameters]
  implicit val ingest_parameter_read  = Json.reads[IngestParameters]

  implicit val ingest_req_parameter_write  = Json.writes[IngestRequest]
  implicit val ingest_req_parameter_read  = Json.reads[IngestRequest]

  implicit val numeric_bin_param_write  = Json.writes[ColumnBins]
  implicit val numeric_bin_param_read  = Json.reads[ColumnBins]

  implicit val numeric_param_write  = Json.writes[NumericBinMeta]
  implicit val numeric_param_read  = Json.reads[NumericBinMeta]

  implicit val adhoc_query_param_write  = Json.writes[AdhocQueryParam]
  implicit val adhoc_query_param_read  = Json.reads[AdhocQueryParam]

}
