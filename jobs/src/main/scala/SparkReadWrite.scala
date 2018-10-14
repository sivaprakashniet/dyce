import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReadWrite {

  def getDataframeFromFile(bucket_name: String, spark: SparkSession,
                           dataset_path: String):DataFrame = {
    val full_dataset_path = "s3a://" + bucket_name + "/" + dataset_path
    spark.read.option("header", true).option("inferSchema",true).csv(full_dataset_path)
  }

  def getDataframeFromParquetFile(bucket_name: String, spark: SparkSession,
                                  dataset_path: String):DataFrame = {
    val full_dataset_path = "s3a://" + bucket_name + "/" + dataset_path
    spark.read.option("header", true).option("inferSchema",true).parquet(full_dataset_path)
  }

  def getDataframeFromParquetFileNA(bucket_name: String, spark: SparkSession,
                                  dataset_path: String):DataFrame = {
    val full_dataset_path = "s3a://" + bucket_name + "/" + dataset_path
    spark.read.option("header", true).option("nullValue", "NA").option("inferSchema",true).parquet(full_dataset_path)
  }
}
