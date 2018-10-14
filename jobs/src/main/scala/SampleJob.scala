import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.util.parsing.json.JSON

object SampleJob {

  var request_id: String = null
  var value: Any = null
  var value2: Any = null

  def main(args: Array[String]) {
    val arguments = args.toList
    println(arguments)
    println(arguments(0))
    println(arguments(1))
    request_id = arguments(0)
    set_parameters(arguments(1))
    val conf = new SparkConf().setAppName("SampleJob")
    val sc = new SparkContext(conf)
    val response = run_job(sc)
  }

  def run_job(sc: SparkContext): String = {
    val rdd_count = sc.parallelize("r_count").count()
    val response = create_ok_response(rdd_count)
    response
  }

  def create_ok_response(rdd_count: Long): String = {
    """ {"rdd_count": """ + rdd_count + "}"
  }

  def set_parameters(argument_string: String) = {
    println(argument_string)
    val argument_json = JSON.parseFull(argument_string).get
    println(argument_json)
    value = argument_json.asInstanceOf[Map[String, Any]]("string")
    value2 = argument_json.asInstanceOf[Map[String, Any]]("array")
  }
}
