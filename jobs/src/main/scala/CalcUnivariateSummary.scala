
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import play.api.libs.json.Json
import ReadWriteImplicits._
import scala.util.Try

object CalcUnivariateSummary {

  val default_bins = 22
  val default_ends = Array(0.05, 0.95)

  def main(args: Array[String]) {
    def getValue(s: String) = s.replaceAll("&nbsp", " ").replaceAll("'", "")

    val arguments = args.toList
    val request_filepath = getValue(arguments(0))
    val response_filepath = getValue(arguments(1))
    val dataset_path = getValue(arguments(2))
    val s3_bucketname = getValue(arguments(3))

    val job_name = "Univariate Summary"
    val spark = SparkConfig.createSparkSession(job_name)
    val df = SparkReadWrite.getDataframeFromParquetFile(s3_bucketname, spark, dataset_path)

    val request_body = StorageReadWrite.getRequest(request_filepath)
    val request_body_as_json = Json.fromJson[UnivariateRequest](request_body).get
    val univariate_summaries = calcUnivariateSummaries(df, request_body_as_json, dataset_path)
    val response_body = Json.toJson(univariate_summaries).toString
    StorageReadWrite.saveResponse(response_filepath, response_body)

    spark.stop()
  }

  def calcUnivariateSummaries(df: DataFrame, request_body: UnivariateRequest, dataset_path: String) = {
    val columns = request_body.columns

    val univariate_summaries = columns map { case column =>
      calcSummary(df, request_body.dataset_name, request_body.dataset_id, column.column_id,
        column.position, column.bins, column.column_name, column.column_datatype,
        column.number_of_bins, column.decimal, column.ends)
    }
    univariate_summaries
  }

  def calcSummary(df: DataFrame, dataset_name: String, dataset_id: String,
                  column_id: String, position: Int, col_bins: Option[Array[String]],
                  column_name: String, column_datatype: String,
                  number_of_bins: Int, decimal: Int,
                  ends: Option[Array[Double]]): UnivariateSummary = {
    val metrics = calcMetrics(df, column_name, column_datatype)
    val missing = calcMissing(df, column_name)
    val num_distinct_values = metrics.distinct.toLong
    val histogram = calcHistogram(df, column_name, column_datatype, num_distinct_values,
      number_of_bins, ends, col_bins)
    val bins = histogram.x
    new UnivariateSummary(dataset_name, dataset_id, column_id, position, column_name, column_datatype,
      bins, histogram, metrics, missing, decimal, bins.size)
  }

  def calcMissing(df: DataFrame, x: String): String = {
    df.select(sum(isnull(df.col(x)).cast("integer"))).head.toSeq(0).asInstanceOf[Long].toString
  }

  def calcMetrics(df: DataFrame, x: String, column_type: String): Metrics = {
    val cnt = df.select(x).count()
    //println(cnt)
    if (cnt > 0)
      column_type match {
        case "Number" | "Percentage" => getMetricsForNumericalColumns(df, x)
        case _ => getMetricsForStringColumns(df, x)
      }
    else new Metrics("null", "null", "null", "null", "null", "null")
  }


  def getMetricsForStringColumns(df: DataFrame, x: String): Metrics = {
    val num_distinct_values = df.select(x).distinct().count().toString
    new Metrics("null", "null", "null", "null", num_distinct_values, "null")
  }

  def getMetricsForNumericalColumns(df: DataFrame, x: String): Metrics = {
    val df2 = df.select(col(x).cast("double"))
    if(df2.count() > 0) {
      val metrics_list = df2.describe(x).select(x).collect().toList.map(x =>
        if (x.toSeq(0) == null) "null" else x.toSeq(0).toString)

      val num_distinct_values = df.select(x).distinct().count().toString

      val median_get_or_else = Try(df.stat.approxQuantile(x, Array(0.5), 0.25)).getOrElse(Array())
      var median = "null"
      if(median_get_or_else.length >0)
        median = median_get_or_else(0).toString


      new Metrics(metrics_list(1), metrics_list(2), metrics_list(3), metrics_list(4),
        num_distinct_values, median)
    }else{
      new Metrics("null", "null", "null", "null", "null", "null")
    }
  }

  def calcHistogram(df: DataFrame, column_name: String, column_datatype: String,
                    num_distinct_values: Long, num_bins: Int, ends: Option[Array[Double]],
                    col_bins: Option[Array[String]]): Histogram =
    column_datatype match {
      case "Number" | "Percentage" => {
        val max_num_of_distinct_values_for_number = 20
        if (num_distinct_values <= max_num_of_distinct_values_for_number) {
          val rdd = df.select(column_name).na.drop.rdd map { x => x(0).toString }
          val count = rdd.map(k => (k, 1)).countByKey()
          new Histogram(count.keys.toList, count.values.toList)
        } else {
          val toDouble = udf[Double, String] { x => if (x == "NA") 0 else x.toDouble }
          val subset: DataFrame = df.select(column_name)
            .na.drop.withColumn(column_name, toDouble(df(column_name)))
          val rdd = subset.rdd map { x => x(0).toString.toDouble }
          val buckets = if(col_bins == None) getBuckets(subset,
            num_bins, ends.getOrElse(default_ends))
          else col_bins.get.map(_.toDouble)
          new Histogram(formatBuckets(buckets), rdd.histogram(buckets).toList)
        }
      }

      case "Category" | "Text" => {
        //val max_num_of_distinct_values_for_string = 1000
        //if(num_distinct_values <= max_num_of_distinct_values_for_string) {
        val max_categories_to_consider = 100
        val non_empty_df = df.select(column_name).na.drop
        val rdd = non_empty_df.rdd map { x => x(0).toString }
        val count = rdd.map(k => (k, 1)).countByKey()
        val new_count = count.toSeq.sortBy(_._2).slice(0,
          max_categories_to_consider - 1).toList

        val others_count = non_empty_df.count() -
          new_count.foldLeft(0.toLong){case (a,b) => a + b._2}
        val (buckets, count_values) = if(others_count != 0) {
          (new_count.map(_._1) :+ "Others", new_count.map(_._2) :+ others_count)
        } else {
          (new_count.map(_._1), new_count.map(_._2))
        }

        new Histogram(buckets, count_values)
        //new Histogram(count.keys.toList, count.values.toList)
        //} else
        //  new Histogram(List(), List())
      }

      case _ =>
        new Histogram(List(), List())
    }


  def formatBuckets(buckets: Array[Double]) =
    1 to buckets.length - 1 map (x => buckets(x - 1) + " - " + buckets(x)) toList


  private def getBuckets(data: DataFrame, bins: Int,
                         ends: Array[Double], error: Double = 0.01): Array[Double] = {
    val name = data.schema.fieldNames(0)
    val percentiles = data.stat.approxQuantile(name, ends, error)
    val minMax = data.agg(min(name), max(name)).collect().toList(0).toSeq.map {
      x => x.toString.toDouble
    }
    val adjBins = if (ends.length == 0) bins
    else if (ends(0) == 0 && ends(1) == 1) bins
    else if ((ends(0) == 0) || (ends(1) == 1)) bins - 1
    else bins - 2
    val p2 = percentiles(1)
    val p1 = percentiles(0)

    val step = if(adjBins != 0) (p2 - p1) / adjBins else 0
    if (step > 0) minMax(0) +: ((0 to adjBins).toArray.map(x => (x * step) + p1.toDouble)) :+
      minMax(1)
    else Array(minMax(0), minMax(1))
  }

}