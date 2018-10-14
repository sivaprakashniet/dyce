package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import actors.MasterActor.{CustomJobDAG, JobDAG}
import akka.actor.{Actor, ActorContext}
import com.amazonaws.services.s3.model.PutObjectResult
import dao.FileMetadataDAO
import entities.{ColumnMetadata, FileMetadata, UnivariateMeta}
import filesystem.{FilesystemService, S3Config}
import job.JobResult
import play.api.Configuration
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

object  UnivariateActor {

  case class ComputeUnivariate(user_id: String, metadata: JsValue,
                               univariateMeta: UnivariateMeta, job_id:
                               Long, actor_context: Option[ActorContext])

  case class ComputeUnivariateForUpdatedColumns(user_id: String, metadata: JsValue,
                                                univariateMeta: UnivariateMeta, job_id:
                                                Long, actor_context: Option[ActorContext],
                                                dataset: FileMetadata,columns: List[ColumnMetadata])
  case class ComputeNumericBinUnivariate(user_id: String, metadata: JsValue,
                                         dataset_path:String, job_id: Long)
  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}


class UnivariateActor @Inject()(s3Config: S3Config, jobResult: JobResult,
                                filesystemService: FilesystemService,
                                configuration: Configuration,
                                fileMetadataDAO: FileMetadataDAO) extends Actor {

  import UnivariateActor._

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
    while(expiration_time > 0 && !(filesystemService.checkInResultS3(request_filepath).get)) {
      expiration_time = expiration_time - 1
      Thread.sleep(5000)
    }

  }

  def receive = {

    case ComputeUnivariate(user_id: String, metadata: JsValue,
    univariateMeta :UnivariateMeta, job_id: Long,
    actor_context:Option[ActorContext]) => {

      val unique_id = createUUID
      val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"
      val dataset_path = getFolderName(user_id) + univariateMeta.dataset_path
      val result = saveRequestToS3(request_filepath, metadata)
      checkInS3(request_filepath)

      val job_status = run_spark_submit_command(dataset_path, request_filepath, response_filepath)
      jobResult.updateJobStatus(job_id, job_status) map { res =>
        jobResult.saveUnivariateFromS3(response_filepath) map (res => {
          if (actor_context == None) context.stop(self)
          else{
            context.parent ! JobDAG("Bivariate", user_id, univariateMeta.dataset_id, None)
          }
        })
      }
    }

    case ComputeUnivariateForUpdatedColumns(user_id: String, metadata: JsValue,
      univariateMeta: UnivariateMeta, job_id:
        Long, actor_context: Option[ActorContext], dataset: FileMetadata,columns: List[ColumnMetadata]) =>{
      val unique_id = createUUID
      val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"
      val dataset_path = getFolderName(user_id) + univariateMeta.dataset_path
      val result = saveRequestToS3(request_filepath, metadata)
      checkInS3(request_filepath)

      val job_status = run_spark_submit_command(dataset_path, request_filepath, response_filepath)
      jobResult.updateJobStatus(job_id, job_status) map { res =>
        jobResult.saveUnivariateFromS3(response_filepath) map (res => {
          context.parent ! CustomJobDAG ("Bivariate", dataset, columns)
          context.stop(self)
        })
      }

    }
    case ComputeNumericBinUnivariate(user_id: String, metadata: JsValue,
    file_path: String, job_id: Long) => {

      val unique_id = createUUID
      val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"
      val dataset_path = getFolderName(user_id) + file_path
      val result = saveRequestToS3(request_filepath, metadata)
      checkInS3(request_filepath)

      val job_status = run_spark_submit_command(dataset_path, request_filepath, response_filepath)
      jobResult.updateJobStatus(job_id, job_status) map { res =>
        jobResult.saveUnivariateFromS3(response_filepath) map (res => {
          context.stop(self)
        })
      }
    }
    case "Stop" => throw StopException
    case _ => throw new Exception
  }

  def run_spark_submit_command(dataset_path: String, request_filepath: String, response_filepath: String) = {
    var spark_submit_command = "spark-submit --jars "+
      getConfiguration("spark.additional_jars") +
      " --class CalcUnivariateSummary" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_filepath.replaceAll(" ","&nbsp") + "'" +
      " '" + response_filepath.replaceAll(" ","&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ","&nbsp") + "'" +
      " '" + s3Config.bucket_name.replaceAll(" ", "&nbsp") + "'"

    //println(spark_submit_command)
    val io = new ProcessIO(
      stdin => stdin.close(),
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines foreach { line =>
          //println("> "+line)
        }
        stdout.close()
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines foreach { line =>
          //println(">> "+line)
        }
        stderr.close()
      })

    val process = spark_submit_command.run(io)
    //println(process.exitValue().toString)
    process.exitValue()

  }

  def saveRequestToS3(request_filepath: String, metadata: JsValue):Future[PutObjectResult] = Future {

    s3Config.s3client.putObject(getConfiguration("s3config.bucket_name_for_job_results"), request_filepath, metadata.toString)
  }

  def getConfiguration(s: String) = configuration.underlying.getString(s)

  def createUUID = UUID.randomUUID().toString
}