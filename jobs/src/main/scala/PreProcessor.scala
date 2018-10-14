import java.text.DecimalFormat
import java.util.UUID

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import play.api.libs.json.Json
import ReadWriteImplicits._

object PreProcessor {

  val maximum_number_of_decimal_places = 45

  val decimal_formatter = new DecimalFormat("0."+
    "0"*maximum_number_of_decimal_places)
  val emptyType = ""
  val stringType = ""
  val dateType = "0000-00-00"

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val bucket_name = getValue(arguments(3))

    val result_path = getValue(arguments(4))

    val s3bucket = "s3n://"+ bucket_name +"/"
    val job_name = "Pre-processing dataset"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromFile(bucket_name, spark, dataset_path)
    val request_body = StorageReadWrite.getRequest(request_filepath)

    val request_body_as_json = Json.fromJson[PreProcessMetaData](request_body).get

    val columns_renamed:Seq[String] = request_body_as_json.columns map (x => x.name)
    val df_columns_renamed = df.toDF(columns_renamed: _*)
    val formatted_df = castColumns(df_columns_renamed, request_body_as_json.columns zip columns_renamed)

    val selected_columns = request_body_as_json.columns.filter(_.visibility == false).map(_.name)
    val final_df = if(selected_columns.isEmpty)
      spark.emptyDataFrame
    else
      formatted_df.select(selected_columns.head, selected_columns.tail: _*)

    val new_result = new DatasetResult(final_df.count(), final_df.columns.length)
    val response_body = Json.toJson(new_result).toString
      StorageReadWrite.saveResponse(result_path, response_body)
      final_df.write.parquet(s3bucket+response_filepath)
      spark.stop()
  }


  def isNumeric(str:String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")
  def toDouble = udf { (m: String) =>if(isNumeric(m.trim)) m.trim else emptyType}
  def isDouble = udf { (m: String) => if(m.trim.contains(".")) "true" else "false"}


  def toStringCol = udf { (m: String) =>
    m match {
      case "NA" => m.toString
      case null  => "_MISSING_"
      case _ =>m.toString
    }
  }

  def uuid() = UUID.randomUUID().toString()

  def castColumns(df: DataFrame, cols: List[(ColumnMetadata,String)]): DataFrame = {
    if (cols.isEmpty == true) return df
    else{
      val col = cols.head
      val df2:DataFrame = col._1.datatype match {
        case "Percentage" | "Number" => {
          val new_df = df.withColumn(col._2, toDouble(df(col._2)))
          val dummy_col_name = "count_doubles"+uuid()
          val new_df2 = new_df.withColumn(dummy_col_name, isDouble(new_df(col._2)))
          var col_type = if(new_df2.filter(new_df2(dummy_col_name) === "true").count() > 0) "double"
          else "integer"
          val old_type = df.select(col._2).schema.toList(0).dataType.typeName
          if(col_type == "integer" && old_type == "long"){
            col_type = "long"
          }
          new_df.withColumn(col._2, new_df(col._2).cast(col_type))

        }
        case "Text" | "Category" | "Date" => df.withColumn(col._2, toStringCol(df(col._2)))
        case _ => df
      }
      castColumns(df2, cols.tail)
    }
  }
}
