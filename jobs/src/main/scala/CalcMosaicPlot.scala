import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import play.api.libs.json.Json
import ReadWriteImplicits._

object CalcMosaicPlot {

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_id = getValue(arguments(0))
    val dataset_path = getValue(arguments(1))
    val s3_bucketname = getValue(arguments(2))

    val job_name = "Mosaic Plot"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val connection = DBReadWrite.createDBConnection()
    val request_body = DBReadWrite.executeSelectQuery(connection, request_id)

    val request_body_as_json = Json.fromJson[BivariateRequest](Json.parse(request_body))

    val bivariate_summaries = calcBivariateSummaries(df, request_body_as_json.get, dataset_path)
    val response_body = Json.toJson(bivariate_summaries).toString

    println(response_body)
    DBReadWrite.executeUpdateQuery(connection, request_id, response_body)

    connection.close
    spark.stop()

  }

  def calcBivariateSummaries(df: DataFrame, request_body_as_json: BivariateRequest,
                             dataset_path: String): List[BivariateSummary] = {
    def getColumnDetails(column_id: String) =
      request_body_as_json.columns.filter(column_id == _.column_id)(0)

    val dataset_id = request_body_as_json.dataset_id

    request_body_as_json.pairs map { pair =>
      val c1 = getColumnDetails(pair.column_1_id)
      val c2 = getColumnDetails(pair.column_2_id)
      new BivariateSummary(dataset_id, pair.column_1_id, pair.column_2_id,
        c1.column_name,c2.column_name,
        calcCov(df, c1, c2), calcCorr(df, c1, c2),
        calcMosaicPlot(df, c1, c2))
    }
  }

  def calcCorr(df: DataFrame, c1: ColumnDetails, c2: ColumnDetails): String = if (isNumericalPair(c1, c2)) {
    var new_df = df.withColumn(c1.column_name, df(c1.column_name).cast(DoubleType))
    new_df = new_df.withColumn(c2.column_name, new_df(c2.column_name).cast(DoubleType))
    new_df.stat.corr(c1.column_name, c2.column_name).toString
  } else "null"

  def isNumericalPair(c1: ColumnDetails, c2: ColumnDetails) =
    if ((c1.column_datatype == "Number" || c1.column_datatype == "Percentage")
      && (c1.column_datatype == "Number" || c1.column_datatype == "Percentage")) true
    else false

  def calcCov(df: DataFrame, c1: ColumnDetails, c2: ColumnDetails): String = if (isNumericalPair(c1, c2)) {
    var new_df = df.withColumn(c1.column_name, df(c1.column_name).cast(DoubleType))
    new_df = new_df.withColumn(c2.column_name, new_df(c2.column_name).cast(DoubleType))
    new_df.stat.cov(c1.column_name, c2.column_name).toString
  } else "null"

  def calcMosaicPlot(df: DataFrame, c1: ColumnDetails,
                     c2: ColumnDetails): Array[MosaicPlotDatapoint] = {
    if(c1.column_id != c2.column_id) {
      val binned_c1_name = "binned_" + c1.column_name
      val binned_c2_name = "binned_" + c2.column_name
      var binned_df = binColumn(binColumn(df, c1, binned_c1_name),
        c2, binned_c2_name)
      binned_df = binned_df.select(binned_c1_name, binned_c2_name)

      val count_df = binned_df.rdd.map(x => (x, 1)).reduceByKey(_ + _)
      val count_list = count_df.map(x => (x._1.toSeq.toArray :+ x._2).map(_.toString)).collect()
      println("Computing for: "+c1.column_name+", "+c2.column_name)
      getMosaicPlotData(count_list, c1.bins.get, c2.bins.get)
    } else Array()
  }

  def getMosaicPlotData(count_list: Array[Array[String]], c1_bins: Array[String],
                        c2_bins: Array[String]): Array[MosaicPlotDatapoint] = {
    val calc_pairs = count_list.map(x => new MosaicPlotDatapoint(x(0), x(1), x(2).toInt, 0, 0))
    var all_pairs = for (x <- c1_bins; y <- c2_bins) yield new MosaicPlotDatapoint(x, y, 0, 0, 0)


    all_pairs = all_pairs.map { p1 =>
      val similar_pair = calc_pairs.filter(p2 => (p2.x == p1.x) && (p2.y == p1.y))
      if (similar_pair.isEmpty) p1 else similar_pair(0)
    }


    def getBinCountSum(pairs: Array[MosaicPlotDatapoint], b: String) =
      pairs.foldLeft[Long](0)((a, p) => if (p.x == b) (a + p.count) else a)

    val total_count_for_each_bin = Map(c1_bins.map { b =>
      (b, getBinCountSum(all_pairs, b))
    }: _*)

    val total_count_for_all_bins = total_count_for_each_bin.foldLeft[Long](0) {
      case (a, (k, v)) => a + v
    }

    all_pairs = all_pairs.map { p =>
      val yper = if (total_count_for_each_bin.get(p.x).get == 0) 0
      else p.count.toDouble / (total_count_for_each_bin.get(p.x).get)
      val xper = if (total_count_for_all_bins == 0) 0
      else (total_count_for_each_bin.get(p.x).get).toDouble / total_count_for_all_bins

      new MosaicPlotDatapoint(p.x, p.y, p.count, xper, yper)
    }

    all_pairs
  }

  def binColumn(df: DataFrame, c: ColumnDetails, binned_column_name: String): DataFrame = {
    val bins = c.bins.getOrElse(Array())
    c.column_datatype match {
      case "Number" | "Percentage" =>
        val num_bins = bins.map(x => (x, x.split(" - ")(0).toDouble))
        binNumericalColumn(df, c.column_name, binned_column_name, num_bins)
      case _ => binStringColumn(df, c.column_name, binned_column_name, bins)
    }
  }

  def binNumericalColumn(df: DataFrame, column_name: String, binned_column_name: String,
                         bins: Array[(String, Double)]): DataFrame = {
    val num_bins = bins.map(x => x._2)

    def bin = udf[String, Double] { x =>
      val bin_index = num_bins.filter(_ <= x).size
      if (bin_index > 0) bins(bin_index - 1)._1 else bins(0)._1
    }

    df.withColumn(binned_column_name, bin(col(column_name)))
  }

  def binStringColumn(df: DataFrame, column_name: String, binned_column_name: String,
                      bins: Array[String]): DataFrame = {
    def bin = udf[String, String] { x => if (bins.contains(x)) x else "Others" }

    df.withColumn(binned_column_name, bin(col(column_name)))
  }

}
