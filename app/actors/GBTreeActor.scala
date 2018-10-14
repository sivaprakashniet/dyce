package actors

import java.security.MessageDigest
import java.util.UUID
import javax.inject.Inject

import akka.actor.Actor
import com.amazonaws.services.s3.model.PutObjectResult
import dao.FileMetadataDAO
import entities.{GBTMeta, ModelMeta}
import filesystem.{FilesystemService, S3Config}
import job.JobResult
import play.api.Configuration
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContext.Implicits.global
import controllers.ControllerImplicits._

import scala.concurrent.Future
import scala.sys.process._

object  GBTreeActor {

  case class BuildGBTrees(user_id: String, tree_id: Option[String], dataset_path: String,
                               gbt_meta: GBTMeta, job_id: Long)

  case object ResumeException extends Exception

  case object StopException extends Exception

  case object RestartException extends Exception

}


class GBTreeActor @Inject()(s3Config: S3Config, jobResult: JobResult,
                                  filesystemService: FilesystemService,
                                  configuration: Configuration,
                                  fileMetadataDAO: FileMetadataDAO) extends Actor {

  import GBTreeActor._

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
    case BuildGBTrees(user_id: String, tree_id: Option[String],
                      dataset_path: String,gbt_meta: GBTMeta, job_id: Long) => {

      val unique_id = createUUID
      val request_filepath = getFolderName(user_id) + "/" + unique_id + "_request"
      val response_filepath = getFolderName(user_id) + "/" + unique_id + "_response"
      val dataset_path_parquet = getFolderName(user_id) + dataset_path
      // Model Path
      val model_path = getFolderName(user_id) + "/models/"+unique_id

      val result = saveRequestToS3(request_filepath, Json.toJson(gbt_meta))

      checkInS3(request_filepath)

      val job_status = run_spark_submit_command(dataset_path_parquet,
        request_filepath, response_filepath, model_path)
      jobResult.updateJobStatus(job_id, job_status) map { res =>
        if(job_status == 0) {
          jobResult.saveGBTreeFromS3(response_filepath, tree_id, gbt_meta.dataset_id) map (res => {
            val new_model = new ModelMeta(Some(unique_id),
              gbt_meta.name,
              gbt_meta.dataset_id,
              res._2,
              user_id,
              gbt_meta.dataset_name,
              res._1,
              gbt_meta.input_column.mkString(","),
              gbt_meta.output_column,
              model_path, None)
            jobResult.saveModel(new_model, tree_id) map { s =>
              context.stop(self)
            }
          })
        }else
          context.stop(self)
      }
    }
    case "Stop" => throw StopException
    case _ => throw new Exception
  }

  def run_spark_submit_command(dataset_path: String, request_filepath: String,
                               response_filepath: String, model_path: String):Int= {
    var spark_submit_command = "spark-submit --jars "+
      getConfiguration("spark.additional_jars") +
      " --class GradientBoostedTree" +
      " --master " + getConfiguration("spark.master") +
      " --deploy-mode " + getConfiguration("spark.deploy_mode") +
      " " + getConfiguration("spark.jars") +
      " '" + request_filepath.replaceAll(" ","&nbsp") + "'" +
      " '" + response_filepath.replaceAll(" ","&nbsp") + "'" +
      " '" + dataset_path.replaceAll(" ","&nbsp") + "'" +
      " '" + s3Config.bucket_name.replaceAll(" ", "&nbsp") + "'" +
      " '" + model_path.replaceAll(" ","&nbsp") + "'"


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