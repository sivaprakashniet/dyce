import play.api.libs.json.Json

object ReadWriteImplicits {

  implicit val metrics_writes = Json.writes[Metrics]
  implicit val metrics_reads = Json.reads[Metrics]

  implicit val column_details_writes = Json.writes[ColumnDetails]
  implicit val column_details_reads = Json.reads[ColumnDetails]

  implicit val univariate_request_writes = Json.writes[UnivariateRequest]
  implicit val univariate_request_reads = Json.reads[UnivariateRequest]

  implicit val histogram_writes = Json.writes[Histogram]
  implicit val histogram_reads = Json.reads[Histogram]

  implicit val univariate_summary_writes = Json.writes[UnivariateSummary]
  implicit val univariate_summary_reads = Json.reads[UnivariateSummary]

  implicit val file_metadata_writes = Json.writes[FileMetadata]
  implicit val file_metadata_reads = Json.reads[FileMetadata]

  implicit val column_metadata_writes = Json.writes[ColumnMetadata]
  implicit val column_metadata_reads = Json.reads[ColumnMetadata]

  implicit val pre_process_meta_write = Json.writes[PreProcessMetaData]
  implicit val pre_process_meta_read = Json.reads[PreProcessMetaData]

  implicit val bivariate_pair_write = Json.writes[BivariatePair]
  implicit val bivariate_pair_read = Json.reads[BivariatePair]

  implicit val bivariate_request_write = Json.writes[BivariateRequest]
  implicit val bivariate_request_read = Json.reads[BivariateRequest]

  implicit val mosaic_plot_datapoint_write = Json.writes[MosaicPlotDatapoint]
  implicit val mosaic_plot_datapoint_read = Json.reads[MosaicPlotDatapoint]

  implicit val bivariate_summary_write = Json.writes[BivariateSummary]
  implicit val bivariate_summary_read = Json.reads[BivariateSummary]

  implicit val mosaic_plot_data_write = Json.writes[MosaicPlotData]
  implicit val mosaic_plot_data_read = Json.reads[MosaicPlotData]

  implicit val correlation_summary_write = Json.writes[CorrelationSummary]
  implicit val correlation_summary_read = Json.reads[CorrelationSummary]

  implicit val update_dataset_request_write = Json.writes[UpdateDatasetRequest]
  implicit val update_dataset_request_read = Json.reads[UpdateDatasetRequest]

  implicit val parameter_write = Json.writes[Parameters]
  implicit val parameter_read  = Json.reads[Parameters]

  implicit val decision_tree_write = Json.writes[DecisionTreeMeta]
  implicit val decision_tree_read  = Json.reads[DecisionTreeMeta]

  implicit val decision_sum_write = Json.writes[DecisionTreeSummary]
  implicit val decision_sum_read  = Json.reads[DecisionTreeSummary]

  implicit val decision_result_write = Json.writes[DecisionTreeResult]
  implicit val decision_result_read  = Json.reads[DecisionTreeResult]

  implicit val anomaly_write = Json.writes[AnomalyMeta]
  implicit val anomaly_read  = Json.reads[AnomalyMeta]


  implicit val gbt_param_write = Json.writes[GBTParameters]
  implicit val gbt_param_read  = Json.reads[GBTParameters]

  implicit val gbt_meta_write = Json.writes[GBTMetadata]
  implicit val gbt_meta_read  = Json.reads[GBTMetadata]

  implicit val gbt_sum_write = Json.writes[GBTSummary]
  implicit val gbt_sum_read  = Json.reads[GBTSummary]

  implicit val gbt_result_write = Json.writes[GBTResult]
  implicit val gbt_result_read  = Json.reads[GBTResult]

  implicit val rf_param_write = Json.writes[RandomForestParameters]
  implicit val rf_param_read  = Json.reads[RandomForestParameters]

  implicit val rf_meta_write = Json.writes[RandomForestMetadata]
  implicit val rf_meta_read  = Json.reads[RandomForestMetadata]

  implicit val rf_sum_write = Json.writes[RandomForestSummary]
  implicit val rf_sum_read  = Json.reads[RandomForestSummary]

  implicit val rf_result_write = Json.writes[RandomForestResult]
  implicit val rf_result_read  = Json.reads[RandomForestResult]

  implicit val model_metadata_write = Json.writes[ModelMetadata]
  implicit val model_metadata_read  = Json.reads[ModelMetadata]

  implicit val score_metadata_write = Json.writes[ScoreMetadata]
  implicit val score_metadata_read  = Json.reads[ScoreMetadata]

  implicit val split_writes = Json.writes[Split]
  implicit val split_reads = Json.reads[Split]

  implicit val class_details_per_node_writes = Json.writes[ClassDetailsPerNode]
  implicit val class_details_per_node_reads = Json.reads[ClassDetailsPerNode]

  implicit val reg_details_per_node_writes = Json.writes[RegDetailsPerNode]
  implicit val reg_details_per_node_reads = Json.reads[RegDetailsPerNode]

  implicit val node_writes = Json.writes[Node]
  implicit val node_reads = Json.reads[Node]

  implicit val dataset_writes = Json.writes[DatasetResult]
  implicit val dataset_reads = Json.reads[DatasetResult]

  implicit val db_conn_writes = Json.writes[DBConnection]
  implicit val db_conn_reads = Json.reads[DBConnection]

  implicit val ingest_param_writes = Json.writes[IngestParameters]
  implicit val ingest_param_reads = Json.reads[IngestParameters]

}