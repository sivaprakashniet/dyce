package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import akka.actor.Actor
import com.amazonaws.services.s3.model.PutObjectResult
import dao.FileMetadataDAO
import filesystem.{FilesystemService, S3Config}
import job.JobResult
import play.api.Configuration
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

object  BivariateActor {

  case class ComputeBivariate(user_id: String,
                              dataset_path: String, job_id: Long,
                              request_json: JsValue)

  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}


class BivariateActor @Inject()(s3Config: S3Config, jobResult: JobResult,
                               filesystemService: FilesystemService,
                               configuration: Configuration,
                               fileMetadataDAO: FileMetadataDAO) extends Actor {

  import BivariateActor._

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
    case ComputeBivariate(user_id: String, dataset_path: String,
    job_id: Long, request_json: JsValue) => {

      val unique_id = createUUID
      val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"

      val result = saveRequestToS3(request_filepath, request_json)
      checkInS3(request_filepath)

      val dataset_parquet_path = getFolderName(user_id) + dataset_path
      val job_status = run_spark_submit_command(dataset_parquet_path, request_filepath, response_filepath)
      //println("Bivariate job status"+ job_status)
      jobResult.updateJobStatus(job_id, job_status) map { res =>
        if(job_status == 0) {
          jobResult.saveBivariateFromS3(response_filepath) map { result =>
            context.stop(self)
          }
        }else context.stop(self)

      }
    }
    case "Stop" => throw StopException
    case _ => throw new Exception
  }

  def run_spark_submit_command(dataset_path: String, request_filepath: String,response_filepath: String) = {

    var spark_submit_command = "spark-submit --jars "+
      getConfiguration("spark.additional_jars") +
      " --class CalcBivariateSummary" +
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

  def saveRequestToS3(request_filepath: String,
                      metadata: JsValue):Future[PutObjectResult] = Future {

    s3Config.s3client.putObject(
      getConfiguration("s3config.bucket_name_for_job_results"),
      request_filepath, metadata.toString)
  }

  def getConfiguration(s: String) = configuration.underlying.getString(s)

  def createUUID = UUID.randomUUID().toString
}