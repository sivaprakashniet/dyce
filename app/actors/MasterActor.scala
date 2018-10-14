package actors

import javax.inject.Inject

import actors.BivariateActor.ComputeBivariate
import actors.CreateFormQueryActor.CreateFromQuerySubmit
import actors.PreProccesActor.PreProcessorSubmit
import actors.UnivariateActor.{ComputeUnivariate, ComputeUnivariateForUpdatedColumns}
import akka.actor.{Actor, Props}
import controllers.ControllerImplicits._
import dao.FileMetadataDAO
import entities.{ColumnMetadata, FileMetadata, IngestParameters}
import filesystem.{FilesystemService, S3Config}
import job.{JobResult, PipelineService}
import play.api.Configuration
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
object MasterActor {
  case class JobDAG(job_type: String, user_id: String, dataset_id: String, meta:Option[IngestParameters])
  case class CustomJobDAG(job_type: String, dataset: FileMetadata,
                          columns: List[ColumnMetadata])
}
class MasterActor@Inject()(s3Config: S3Config,
                           jobResult: JobResult,
                           filesystemService: FilesystemService,
                           configuration: Configuration,
                           pipelineService: PipelineService,
                           fileMetadataDAO: FileMetadataDAO
                          ) extends Actor {
  import MasterActor._


  def receive = {
    case JobDAG(job_type: String, user_id: String, dataset_id: String, meta:Option[IngestParameters]) => {
      job_type match {
        case "Create-From-Query" =>{
          val create_from_query_pipeline = context.actorOf(Props(classOf[CreateFormQueryActor], s3Config,
            jobResult, filesystemService, configuration))
           pipelineService.createFromQuery(user_id, meta) map { res =>
             create_from_query_pipeline ! CreateFromQuerySubmit(user_id, meta.get, res, Some(context))
           }
        }
        case "Pre-Processing" =>{
          val pre_processor_pipeline = context.actorOf(Props(classOf[PreProccesActor], s3Config,
            jobResult, filesystemService, configuration))
          pipelineService.preProcessDataset(user_id, dataset_id) map { res =>
            pre_processor_pipeline ! PreProcessorSubmit(user_id, res._2, res._3, Some(context))
          }
        }
        case "Univariate" =>{
          val univariate_actor_pipeline = context.actorOf(Props(classOf[UnivariateActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO))
          pipelineService.computeUnivariate(user_id, dataset_id) map { res =>
            univariate_actor_pipeline ! ComputeUnivariate(user_id,
              Json.toJson(res._2), res._2,res._3, Some(context))
          }
        }
        case "Bivariate" =>{
          val bivariate_actor_pipeline = context.actorOf(Props(classOf[BivariateActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO))
          pipelineService.computeBivariate(user_id, dataset_id) map { res =>
            bivariate_actor_pipeline ! ComputeBivariate(user_id, res._2.get, res._3, res._4)
          }
        }
        case _ => context.stop(self)
      }

    }
    case CustomJobDAG(job_type: String, dataset: FileMetadata,
                                  columns: List[ColumnMetadata]) => {
      job_type match {
        case "Updated_columns_univariate" => {
          val univariate_actor_pipeline = context.actorOf(Props(classOf[UnivariateActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO))
          pipelineService.constructUnivariateMeta(dataset, columns) map { meta_data =>
            univariate_actor_pipeline ! ComputeUnivariateForUpdatedColumns(dataset.user_id.get,
              Json.toJson(meta_data._1), meta_data._1, meta_data._2, Some(context), dataset, columns)
          }
        }
        case "Bivariate" => {
          val bivariate_actor_pipeline = context.actorOf(Props(classOf[BivariateActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO))
          pipelineService.constructBivariateMeta(dataset, columns) map { res =>
            bivariate_actor_pipeline ! ComputeBivariate(dataset.user_id.get, res._2.get, res._3, res._4)
          }
        }
      }
    }
    case _ => {
      throw new Exception
    }
  }
}
