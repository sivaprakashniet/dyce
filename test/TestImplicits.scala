import play.api.libs.json.Json

case class ColumnMetadata(name: String, description:Option[String], position: Long, datatype: String,
                          format: String, display_format: String,decimal: Long, separator: Boolean,
                          visibility: Boolean,calculated: Boolean, formula: String, metrics: String)

case class ColumnMetadataExtended(id: String, name: String, position: Long, datatype: String,
                                format: String, decimal: Long, separator: Boolean,
                                visibility: Boolean, calculated: Boolean, formula: String,
                                metrics: String, created_date: String, modified_date: String,
                                  status: String, dataset_id: String)

case class AddColumnMetadata(dataset_id: String, id: String, name: String, new_name: Option[String],
                             position: Long, datatype: String, format: String, raw_formula: Option[String],
                             display_format: String, decimal: Long, separator: Boolean, visibility: Boolean,
                             calculated: Boolean, created_date: String, modified_date: String, formula: String,
                             metrics: String, description: Option[String], status: String)

/*
case class AddColumnMetadata(id: String, name: String, new_name: String, position: Long, datatype: String,
                                 format: String, raw_formula: String, display_format: String,
                                 decimal: Long, separator: Boolean, visibility: Boolean,
                                 calculated: Boolean, formula: String,
                                 metrics: String, description: String, status: String)*/

case class InferSchemaRequest(preview_data: List[List[String]], schema_list: List[ColumnMetadata])

case class FileMetadata(file_path: String, dataset_name: String, file_size: Long)

case class ColumnSchema(schema_list: List[ColumnMetadata])

case class ColumnSchemaExtended(dataset_id: String, schema_list: List[ColumnMetadataExtended])

case class AddColumnSchema(dataset_id: String, schema_list: List[AddColumnMetadata])

case class AnomalyRequest(name: String, trees: Long, sample_proportions: Long, no_of_anomalies: Long,
                          features: Long, dataset_id: String, dataset_name: String, bootstrap: Boolean,
                          replacement: Boolean, feature_columns: List[String], feature_column_ids: List[String],
                          random_split: Long)

object TestImplicits {

  implicit val column_metadata_writes = Json.writes[ColumnMetadata]
  implicit val column_metadata_reads = Json.reads[ColumnMetadata]

  implicit val infer_schema_writes = Json.writes[InferSchemaRequest]
  implicit val infer_schema_reads = Json.reads[InferSchemaRequest]

  implicit val file_metadata_writes = Json.writes[FileMetadata]
  implicit val file_metadata_reads = Json.reads[FileMetadata]

  implicit val column_schema_writes = Json.writes[ColumnSchema]
  implicit val column_schema_reads = Json.reads[ColumnSchema]

  implicit val column_metadata_extended_writes = Json.writes[ColumnMetadataExtended]
  implicit val column_metadata_extended_reads = Json.reads[ColumnMetadataExtended]

  implicit val column_schema_extended_writes = Json.writes[ColumnSchemaExtended]
  implicit val column_schema_extended_reads = Json.reads[ColumnSchemaExtended]

  implicit val anomaly_request_extended_writes = Json.writes[AnomalyRequest]
  implicit val anomaly_request_extended_reads = Json.reads[AnomalyRequest]

  implicit val add_column_schema_writes = Json.writes[AddColumnMetadata]
  implicit val add_column_schema_reads = Json.reads[AddColumnMetadata]

  implicit val add_column_metadata_extended_writes = Json.writes[AddColumnSchema]
  implicit val add_column_metadata_extended_reads = Json.reads[AddColumnSchema]
}