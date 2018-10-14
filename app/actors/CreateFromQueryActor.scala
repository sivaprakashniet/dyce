package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import actors.MasterActor.JobDAG
import akka.actor.{Actor, ActorContext}
import com.amazonaws.services.s3.model.PutObjectResult
import entities.{IngestParameters}
import filesystem.{FilesystemService, S3Config}
import job.JobResult
import play.api.Configuration
import play.api.libs.json.{JsValue, Json}
import controllers.ControllerImplicits._

import scala.concurrent.Future
import scala.sys.process._
import scala.concurrent.ExecutionContext.Implicits.global

object CreateFormQueryActor {

  case class CreateFromQuerySubmit(user_id: String, query_meta: IngestParameters,
                                   job_id: Long, actor_context: Option[ActorContext])

  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}


class CreateFormQueryActor @Inject()(s3Config: S3Config, jobResult: JobResult, filesystemService: FilesystemService, configuration: Configuration) extends Actor {

  import CreateFormQueryActor._

  def receive = {
    case CreateFromQuerySubmit(user_id: String, query_meta: IngestParameters,
    job_id: Long, actor_context: Option[ActorContext]) => {

      val unique_id = createUUID
      val request_file_path = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_file_path = getFolderName(user_id) + "/" + unique_id + "_response"

      val dataset_result_path = "/dataset/" + query_meta.name.get + "_"+unique_id+".csv"

      val result = saveRequestToS3(request_file_path, Json.toJson(query_meta))
      checkInS3(request_file_path)

      val job_status = run_spark_submit_create_from_query( request_file_path,
        getFolderName(user_id) + dataset_result_path, response_file_path)

        if(job_status == 0) {
          jobResult.saveFileColumnMeta(user_id, dataset_result_path, query_meta) map { dataset_id =>
            jobResult.updateJobStatus(job_id, job_status) map { res =>
              if (actor_context == None) context.stop(self)
              else {
                context.parent ! JobDAG("Pre-Processing", user_id, dataset_id, None)
                context.stop(self)
              }
            }
          }
        }else{
          context.stop(self)
        }
    }

    case "Stop" => throw StopException
    case _ => throw new Exception
  }

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

  def run_spark_submit_create_from_query(request_file_path: String,
                                         dataset_path: String,
                                         response_file_path: String): Int = {
    var spark_submit_command = "spark-submit --jars " +
      getConfiguration("spark.additional_jars") +
      " --class Ingest" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_file_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + response_file_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + s3Config.bucket_name.replaceAll(" ", "&nbsp") + "'"


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
    println(process.exitValue().toInt)
    process.exitValue().toInt

  }

  def getConfiguration(s: String) = configuration.underlying.getString(s)

  def saveRequestToS3(request_filepath: String, metadata: JsValue): Future[PutObjectResult] = Future {

    s3Config.s3client.putObject(getConfiguration("s3config.bucket_name_for_job_results"), request_filepath, metadata.toString)
  }

  def createUUID = UUID.randomUUID().toString
}