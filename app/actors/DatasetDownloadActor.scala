package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import akka.actor.Actor
import com.amazonaws.services.s3.model.PutObjectResult
import dao.FileMetadataDAO
import entities.FileMetadata
import filesystem.{FilesystemService, S3Config}
import job.JobResult
import play.api.Configuration
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process._

object DatasetDownloadActor {

  case class Downloaddataset(user_id: String, dataset: FileMetadata, job_id: Long)

  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}


class DatasetDownloadActor @Inject()(s3Config: S3Config, jobResult: JobResult, filesystemService: FilesystemService, configuration: Configuration, fileMetadataDAO:FileMetadataDAO) extends Actor {

  import DatasetDownloadActor._

  def receive = {
    case Downloaddataset(user_id: String, dataset: FileMetadata, job_id: Long) => {
      val unique_id = createUUID

      val download_dataset_path = "/download/" + dataset.dataset_name+"_"+unique_id + "_download"
      val download_path_with_user = getFolderName(user_id) + download_dataset_path

      val dataset_path = getFolderName(user_id) + dataset.parquet_file_path.get

      val job_status = run_spark_submit_preprocessor(dataset_path,download_path_with_user)

      filesystemService.listFiles(download_path_with_user, download_path_with_user+".csv")

      jobResult.updateJobStatus(job_id, job_status) map { res =>
        if(job_status == 0) {
          jobResult.updateFileMetaData(dataset,download_dataset_path+".csv") map { res =>
            context.stop(self)
          }
        }else{
          jobResult.updateErrorInFileMetaData(dataset) map { res =>
            context.stop(self)
          }
        }
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

  def run_spark_submit_preprocessor(dataset_parquet_path: String, download_path_with_user: String): Int = {
    var spark_submit_command = "spark-submit --jars " +
      getConfiguration("spark.additional_jars") +
      " --class DownloadDataset" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + dataset_parquet_path.replaceAll(" ", "&nbsp") + "'" +
      " '" + download_path_with_user.replaceAll(" ", "&nbsp") + "'" +
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
    process.exitValue().toInt
  }

  def getConfiguration(s: String) = configuration.underlying.getString(s)

  def saveRequestToS3(request_filepath: String, metadata: JsValue): Future[PutObjectResult] = Future {

    s3Config.s3client.putObject(getConfiguration("s3config.bucket_name_for_job_results"), request_filepath, metadata.toString)
  }

  def createUUID = UUID.randomUUID().toString
}