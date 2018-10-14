import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json
import ReadWriteImplicits._

object Ingest {
  var spark: SparkSession = null
  val s3bucket = "s3n://" + S3Config.bucket_name + "/"

  def getDataframeFromDatabase(db_conn: DBConnection, table: String): DataFrame = {
    spark.sqlContext.read.format("jdbc").options(Map(
      "url" -> ("jdbc:mysql://" + db_conn.host_name + ":" + db_conn.port.get +"/"+ db_conn.default_schema.get),
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> table,
      "user" -> db_conn.username,
      "password" -> db_conn.password)).load()
  }

  def runQuery(query: String): DataFrame = spark.sql(query)

  def createAllDataframes(db_conn: DBConnection, tables: List[String]) = {
    tables foreach { t =>
      getDataframeFromDatabase(db_conn, t).createOrReplaceTempView(t)
    }
  }

  def main(args: Array[String]): Unit = {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'","")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val s3_bucketname = getValue(arguments(2))

    val job_name = "Ingest"
    spark = SparkConfig.createSparkSession(job_name)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[IngestParameters](request_body).get

    val df = createAllDataframes(request_body_as_json.db_conn,
      request_body_as_json.tables)
    val new_df = runQuery(request_body_as_json.query)
    println(new_df.show())
    new_df.write.option("header", true).csv(s3bucket+response_filepath)
    spark.stop()
  }
}