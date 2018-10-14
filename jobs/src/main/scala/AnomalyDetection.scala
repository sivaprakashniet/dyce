import ReadWriteImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.Random
import play.api.libs.json.Json

object AnomalyDetection {
  var spark: SparkSession = null
  var request_id: String = null
  val s3bucket = "s3n://"+ S3Config.bucket_name +"/"

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val s3_bucketname = getValue(arguments(3))

    val job_name = "Anomaly Detection"
    spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val meta = Json.fromJson[AnomalyMeta](request_body).get

    anomalyDetection(df, dataset_path, response_filepath, meta)
    spark.stop()
    }

  def anomalyDetection(dataframe: DataFrame, dataset_path: String,
                       result_storage_path: String, meta: AnomalyMeta): Unit = {
     Random.setSeed(1337)

    val numTrees = meta.trees
    val samples = meta.sample_proportions
    val list_of_column = meta.feature_columns

    val sub_sample: Double = (samples / 100) * numTrees
    val sub_sample_size = math.ceil(sub_sample).toInt

    val auxSchema = StructType(Array(StructField("anomaly_score", DoubleType, false)))
    val df = dataframe.select(list_of_column.map(c => col(c).cast(DoubleType)): _*)

    val rows: RDD[Array[Double]] = df.rdd.map(x => (x.toSeq.toArray).map(_.asInstanceOf[Double]))
    val forest = buildForest(rows, numTrees)
    var rdd_data: List[Seq[Double]] = List()
    for (row <- rows.take(rows.count.toInt)) {
      var x = row.toList
      x = forest.predict(row) :: x
      rdd_data = rdd_data :+ x
    }
    var rdds = spark.sparkContext.parallelize(rdd_data)
    val rowRdd = rdds.map(v => Row(v: _*))
    val anamoly_df = spark.sqlContext.createDataFrame(rowRdd, StructType(auxSchema ++ df.schema))
    writeAnomalyResult(anamoly_df, result_storage_path)
  }

  def buildForest(data: RDD[Array[Double]], numTrees: Int = 2,
                  subSampleSize: Int = 256, seed: Long = Random.nextLong): IsolationForest = {
    val numSamples = data.count()
    val numColumns = data.take(1)(0).size
    var sampleRatio = subSampleSize / numSamples.toDouble
    if (sampleRatio > 1) {
      sampleRatio = 0.1
    }
    val maxHeight = math.ceil(math.log(subSampleSize)).toInt
    val trees = Array.fill[ITree](numTrees)(ITreeLeaf(1))
    val trainedTrees = trees.map(s => growTree(getRandomSubsample(data, sampleRatio, seed), maxHeight, numColumns))
    IsolationForest(numSamples, trainedTrees)
  }

  def growTree(data: RDD[Array[Double]], maxHeight: Int, numColumns: Int, currentHeight: Int = 0): ITree = {
    val numSamples = data.count()
    if (currentHeight >= maxHeight || numSamples <= 1) {
      return new ITreeLeaf(numSamples)
    }

    val split_column = Random.nextInt(numColumns)
    val column = data.map(s => s(split_column))

    val col_min = column.min()
    val col_max = column.max()
    val split_value = col_min + Random.nextDouble() * (col_max - col_min)

    val X_left = data.filter(s => s(split_column) < split_value).cache()
    val X_right = data.filter(s => s(split_column) >= split_value).cache()
    new ITreeBranch(growTree(X_left, maxHeight, numColumns, currentHeight + 1),
      growTree(X_right, maxHeight, numColumns, currentHeight + 1),
      split_column,
      split_value)
  }

  //For anomaly detection code
  def getRandomSubsample(data: RDD[Array[Double]], sampleRatio: Double, seed: Long = Random.nextLong): RDD[Array[Double]] = {
    data.sample(false, sampleRatio, seed = seed)
  }

  def writeAnomalyResult(dataframe: DataFrame, result_storage_path: String) = {
    val df = dataframe.orderBy(desc("anomaly_score"))
      df.write.parquet(s3bucket+result_storage_path)
  }

}