object DownloadDataset {

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val dataset_path = getValue(arguments(0))
    val new_dataset_path = getValue(arguments(1))
    val s3_bucketname = getValue(arguments(2))

    val job_name = "Download dataset"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)
    val s3bucket = "s3n://"+ s3_bucketname +"/"
    df.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true").csv(s3bucket + new_dataset_path)
    spark.stop()
  }
}