package services

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val access_key = "AKIAIHBBE5LFMW3KEGGQ"
  val secret_key = "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"
  val spark = SparkSession.builder
    .config("spark.driver.allowMultipleContexts",true)
    .master("local[*]")
    .appName("Amplifyr").getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", access_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret_key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.multipart.size","104857600")
    spark.sparkContext.setLogLevel("ERROR")
}
