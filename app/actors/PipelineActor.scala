package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import controllers.ControllerImplicits._
import actors.MasterActor.JobDAG
import akka.actor.Actor
import com.amazonaws.services.s3.model.PutObjectResult
import entities.{PreProcessMetaData, UnivariateMeta}
import filesystem.{FilesystemService, S3Config}
import job.{JobResult, PipelineService}
import play.api.Configuration
import play.api.libs.json.{JsValue, Json}
import scala.sys.process._
import scala.concurrent.Future
import scala.sys.process.ProcessIO
import scala.concurrent.ExecutionContext.Implicits.global

object PipelineActor {

  case class PreProcessPipeline(user_id: String, dataset_id: String)

  case class UnivariatePipeline(user_id: String, dataset_id: String)

  case class BivariatePipeline(user_id: String, dataset_id: String)

  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}

class PipelineActor @Inject()(s3Config: S3Config,
                              jobResult: JobResult,
                              filesystemService: FilesystemService,
                              configuration: Configuration,
                              pipelineService: PipelineService
                             ) extends Actor {

  import PipelineActor._

  def receive = {
    case PreProcessPipeline(user_id: String, dataset_id: String) => {
      pipelineService.preProcessDataset(user_id, dataset_id) map { request_json =>
        val pre_process_meta: PreProcessMetaData = request_json._2
        val job_id = request_json._3
        val request_file_path = getFolderName(user_id) + "/" + unique_id + "_request"
        val result_path = getFolderName(user_id) + "/" + unique_id + "_response"
        val response_file_path = "/parquet_datasets/" + pre_process_meta.dataset.dataset_name + "_" + unique_id + ".parquet"
        val dataset_path = getFolderName(user_id) + pre_process_meta.dataset.file_path
        val result = saveRequestToS3(request_file_path, Json.toJson(pre_process_meta))
        checkInS3(request_file_path)
        val job_status = run_spark_submit_preprocessor(dataset_path,
          request_file_path, getFolderName(user_id) + response_file_path, result_path)

        //println("Pre - Processing job status ->" + job_status)
        jobResult.updateJobStatus(job_id, job_status) map { res =>
          if (job_status == 0) {
            jobResult.savePreProcessFile(result_path, pre_process_meta.dataset.id.get,
              response_file_path, job_status) map { res =>
              Thread.sleep(100)
              context.parent ! JobDAG("Univariate", user_id, dataset_id, None)
            }
          } else context.stop(self)
        }
      }
    }
    case UnivariatePipeline(user_id: String, dataset_id: String) => {
      pipelineService.computeUnivariate(user_id, dataset_id) map { request_json =>
        val uni_meta: UnivariateMeta = request_json._2
        val job_id = request_json._3

        val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
        val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"
        val dataset_path = getFolderName(user_id) + uni_meta.dataset_path

        //println(Json.toJson(uni_meta))

        val result = saveRequestToS3(request_filepath, Json.toJson(uni_meta))

        checkInS3(request_filepath)

        val job_status = run_spark_submit_command_univariate(dataset_path,
          request_filepath, response_filepath)
        //println("Univariate pipeline ->" + job_status)
        jobResult.updateJobStatus(job_id, job_status) map { res =>
          if (job_status == 0) {
            jobResult.saveUnivariateFromS3(response_filepath) map (res => {
              Thread.sleep(100)
              context.parent ! JobDAG("Bivariate", user_id, dataset_id, None)
            })
          } else context.stop(self)

        }
      }
    }
    case BivariatePipeline(user_id: String, dataset_id: String) => {
      pipelineService.computeBivariate(user_id, dataset_id) map { req_json =>
        val request_json: JsValue = req_json._4
        val dataset_path: String = req_json._2.get
        val job_id: Long = req_json._3

        val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
        val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"

        val result = saveRequestToS3(request_filepath, request_json)
        checkInS3(request_filepath)

        val dataset_parquet_path = getFolderName(user_id) + dataset_path
        val job_status = run_spark_submit_command_bivariate(dataset_parquet_path, request_filepath, response_filepath)
        //println("Bivariate pipeline ->" + job_status)
        jobResult.saveBivariateFromS3(response_filepath) map { result =>
          jobResult.updateJobStatus(job_id, job_status) map { res =>
            Thread.sleep(100)
            context.stop(self)
          }
        }
      }
    }

    case _ => {
      throw new Exception
    }
  }

  // General Functions
  def getFolderName(userid: String): String = {
    def encrypt_with_md5(text: String): String = {
      val encrypted_string = MessageDigest.getInstance("MD5").digest(text.getBytes)
        .map(0xFF & _).map {
        "%02x".format(_)
      }.foldLeft("") {
        _ + _
      }
      encrypted_string
    }

    val encrypted_value = encrypt_with_md5(userid)
    encrypted_value.toString
  }

  def checkInS3(request_filepath: String) = {
    var expiration_time = 5
    while (expiration_time > 0 && !(filesystemService.checkInResultS3(request_filepath).get)) {
      expiration_time = expiration_time - 1
      Thread.sleep(5000)
    }
  }
  // Spark shell commands
  def run_spark_submit_preprocessor(result_path: String, dataset_path: String,
                                    request_file_path: String, response_file_path: String): Int = {
    var spark_submit_command = "spark-submit --jars " +
      getConfiguration("spark.additional_jars") +
      " --class PreProcessor" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_file_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + response_file_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ", "&nbsp") + "'"+
      " '" + s3Config.bucket_name.replaceAll(" ", "&nbsp") + "'" +
      " '" + result_path.replaceAll(" ", "&nbsp") + "'"


    println(spark_submit_command)

    val io = new ProcessIO(
      stdin => stdin.close(),
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines foreach { line =>
          println("> " + line)
        }
        stdout.close()
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines foreach { line =>
          println(">> " + line)
        }
        stderr.close()
      })

    val process = spark_submit_command.run(io)
    process.exitValue().toInt
  }
  def getConfiguration(s: String) = configuration.underlying.getString(s)
  def run_spark_submit_command_univariate(dataset_path: String,
                                          request_filepath: String, response_filepath: String) = {
    var spark_submit_command = "spark-submit --jars " +
      getConfiguration("spark.additional_jars") +
      " --class CalcUnivariateSummary" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_filepath.replaceAll(" ", "&nbsp") + "'" +
      " '" + response_filepath.replaceAll(" ", "&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ", "&nbsp") + "'"

    //println(spark_submit_command)
    val io = new ProcessIO(
      stdin => stdin.close(),
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines foreach { line =>
          //println("> " + line)
        }
        stdout.close()
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines foreach { line =>
          //println(">> " + line)
        }
        stderr.close()
      })

    val process = spark_submit_command.run(io)
    //println(process.exitValue().toString)
    process.exitValue()
  }
  def run_spark_submit_command_bivariate(dataset_path: String,
                                         request_filepath: String, response_filepath: String) = {

    var spark_submit_command = "spark-submit --jars " +
      getConfiguration("spark.additional_jars") +
      " --class CalcBivariateSummary" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_filepath.replaceAll(" ", "&nbsp") + "'" +
      " '" + response_filepath.replaceAll(" ", "&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ", "&nbsp") + "'"

    //println(spark_submit_command)

    val io = new ProcessIO(
      stdin => stdin.close(),
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines foreach { line =>
          //println("> " + line)
        }
        stdout.close()
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines foreach { line =>
          //println(">> " + line)
        }
        stderr.close()
      })

    val process = spark_submit_command.run(io)
    //println(process.exitValue().toString)
    process.exitValue()

  }
  def saveRequestToS3(request_filepath: String,
                      metadata: JsValue): Future[PutObjectResult] = Future {

    s3Config.s3client.putObject(getConfiguration("s3config.bucket_name_for_job_results"),
      request_filepath, metadata.toString)
  }
  def unique_id = UUID.randomUUID().toString

}