import S3Config.{access_key, secret_key}
import org.apache.spark.sql.SparkSession

object SparkConfig {
  var spark: SparkSession = null

  def createSparkSession(job_name: String): SparkSession = {
    spark = SparkSession.builder.appName(job_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.access.key", access_key)
    hadoopConf.set("fs.s3a.secret.key", secret_key)
    hadoopConf.set("fs.s3n.awsAccessKeyId", access_key)
    hadoopConf.set("fs.s3n.awsSecretAccessKey", secret_key)
    spark
  }

  def getSparkSession = spark
}
