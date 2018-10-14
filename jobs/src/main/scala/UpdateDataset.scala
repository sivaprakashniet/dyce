import java.util.UUID
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import play.api.libs.json.Json
import ReadWriteImplicits._
import InferSchema._

object UpdateDataset {

  val s3bucket = "s3n://" + S3Config.bucket_name + "/"

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val new_dataset_path = getValue(arguments(3))
    val s3_bucketname = getValue(arguments(4))

    val job_name = "Update or Add column for a dataset"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)
    val request_body = StorageReadWrite.getRequest(request_filepath)
    //val request_body = Json.parse(SampleString.json)

    val all_column_details = Json.fromJson[UpdateDatasetRequest](request_body).get

    val final_df_with_details = modifyDataframe(df, all_column_details.schema_list)

    final_df_with_details._1.write.parquet(s3bucket + new_dataset_path)

    StorageReadWrite.saveResponse(response_filepath,
      Json.toJson(final_df_with_details._2).toString)

    spark.stop()
  }

  def modifyDataframe(df: DataFrame, all_column_details: List[ColumnMetadata]):
  (DataFrame,List[ColumnMetadata]) = {
    val final_column_details = all_column_details.filter(_.status.get != "deleted")
    var new_column_details = all_column_details.filter(_.status.get == "new")

    //Apply formulae to columns
    val df_with_new_columns = applyFormulaeToColumns(df, all_column_details)

    //Select the final columns
    val tmp_col_names = Seq.fill(final_column_details.length)("_cu")
      .zipWithIndex.map { case (x, i) => x + i }
    val df_with_final_columns = df_with_new_columns.select(tmp_col_names.map(c => col(c)): _*)

    //Rename all the columns
    val new_column_names = final_column_details.map(_.new_name.get)
    val renamed_df = renameColumns(df_with_final_columns, new_column_names)

    //Format all the columns
    val final_df = formatColumns(renamed_df, final_column_details)

    final_df.show()

    (final_df, autoDetectType(final_df, new_column_details))
  }

  def applyFormulaeToColumns(df: DataFrame,
                             all_column_details: List[ColumnMetadata]): DataFrame = {
    var modified_df = df
    var j = 0
    all_column_details.zipWithIndex.map { case (c, i) =>
      val formula = c.formula
      val old_col_name = c.name
      if (!(c.status.get == "deleted")) {
        if ((c.status.get == "modified" && c.calculated == true) || c.status.get == "new")
          modified_df = ExprParser.calculator(modified_df, "_cu" + j, formula).get
        else
          modified_df = modified_df.withColumn("_cu" + j, col(old_col_name))
        j = j + 1
      }
    }
    modified_df
  }

  def renameColumns(df: DataFrame, new_column_names: List[String]): DataFrame =
    df.toDF(new_column_names: _*)

  val toDoubleCol = udf { (m: String) =>
    try {
      m.toDouble.toString
    } catch {
      case e: Exception => {
        " "
      }
    }
  }

  def toStringCol = udf { (m: String) =>
    m match {
      case null  => "_MISSING_"
      case _ => m.toString
    }
  }

  def uuid() = UUID.randomUUID().toString()

  def isDouble = udf { (m: String) =>
    if(m.contains(".")) "true" else "false"
  }

  def formatColumns(df: DataFrame, final_column_details: List[ColumnMetadata]): DataFrame = {
    if (final_column_details.isEmpty == true) return df
    else{
      val column_details = final_column_details.head
      val new_name = column_details.new_name.get
      val formatted_df:DataFrame = column_details.datatype match {
        case "Percentage" | "Number" => {
          val new_df = df.withColumn(new_name, toDoubleCol(df(new_name)))
          val dummy_col_name = "count_doubles"+uuid()
          val new_df2 = new_df.withColumn(dummy_col_name, isDouble(new_df(new_name)))
          val col_type =
            if(new_df2.filter(new_df2(dummy_col_name) === "true").count() > 0) "double"
            else "integer"
          new_df.withColumn(new_name, new_df(new_name).cast(col_type))
        }
        case "Text" | "Category" | "Date" => df.withColumn(new_name, toStringCol(df(new_name)))
        case _ => df
      }
      formatColumns(formatted_df, final_column_details.tail)
    }
  }

  def autoDetectType(df: DataFrame, new_column_details: List[ColumnMetadata]): List[ColumnMetadata] = {
    new_column_details map {
      column_details => {
        val col_name = column_details.new_name.get
        val col_type = column_details.datatype
        if(col_type == "AutoDetect") {

          val values = df.select(col_name).take(100).map { row =>
            val value = row.toSeq(0)
            value match {
              case null => ""
              case _ => value.toString
            }
          }

          val col_schema = infer_schema(List(values.toList).transpose)
          column_details.copy(datatype = col_schema(0).get("datatype").getOrElse("Text").toString)
        } else column_details
      }
    }
  }

}