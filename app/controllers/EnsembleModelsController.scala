package controllers

import javax.inject._

import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities.{GBTMeta, RandomForestMeta}
import job.{JobResult, JobService}
import services.FileService
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import util.Secured
import play.api.libs.json.{Json, _}

import scala.concurrent.Future

class EnsembleModelsController @Inject()(jobService: JobService,
                                  jobResult: JobResult,
                                  fileService: FileService,
                                  secured: Secured) extends Controller {
  // Gradient-Boosted Trees

  def buildGBTSecured(user_id: String,
                               dataset_id: String) = Action.async(parse.json) {
    implicit request =>
      val decision_tree_meta = Json.fromJson[GBTMeta](request.body)
      decision_tree_meta match {
        case JsSuccess(u: GBTMeta, _) => {
          jobService.buildGBT(user_id, u, None) map { result =>
            Status(200)(Json.obj("status" -> "Building GBT"))
          }
        }
        case e: JsError => handleValidationError(e)
      }
  }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))

    )

  def deleteGBTSecured(user_id: String,
                                dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, DeletePrivilege) {
      Action.async {
        jobResult.deleteGBT(dataset_id, id).map { p =>
          Status(200)(Json.obj("status" -> "GBT deleted"))
        }
      }
  }

  def getOneGBTSecured(user_id: String,
                                dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getOneGBT(dataset_id, id).map { p =>
          Ok(p)
        }
    }
  }

  def getGBTSecured(user_id: String, dataset_id: String, page:Int, limit: Int, q:Option[String]) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getGBT(dataset_id, page, limit, q).map { p =>
          Ok(Json.obj("total_count" ->p._1, "summaries" -> p._2))
        }
    }
  }

  def updateGBTSecured(user_id: String,
                                dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, UpdatePrivilege) {
      Action.async(parse.json) {
        implicit request =>
          val decision_tree_meta = Json.fromJson[GBTMeta](request.body)
          decision_tree_meta match {
            case JsSuccess(u: GBTMeta, _) => {
              jobService.buildGBT(user_id, u, Some(id)) map { result =>
                Status(200)(Json.obj("status" -> "Building GBT"))
              }
            }
            case e: JsError => handleValidationError(e)
          }
      }
  }
  // Random Forest tree's
  def buildRandomForestSecured(user_id: String,
                      dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, CreatePrivilege) {
      Action.async(parse.json) {
        implicit request =>
          val rf_meta = Json.fromJson[RandomForestMeta](request.body)
          rf_meta match {
            case JsSuccess(u: RandomForestMeta, _) => {
              jobService.buildRandomForest(user_id, u, None) map { result =>
                Status(200)(Json.obj("status" -> "Building Random Forest tree's"))
              }
            }
            case e: JsError => handleValidationError(e)
          }
      }
  }

  def deleteRandomForestSecured(user_id: String,
                       dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, DeletePrivilege) {
    Action.async {
      jobResult.deleteRandomForest(dataset_id, id).map { p =>
        Status(200)(Json.obj("status" -> "Random Forest tree deleted"))
      }
    }
  }

  def getOneRandomForestSecured(user_id: String,
                       dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getOneRandomForest(dataset_id, id).map { p =>
          Ok(p)
        }
      }
  }

  def getRandomForestSecured(user_id: String,
                             dataset_id: String, page:Int, limit: Int, q:Option[String]) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getRandomForest(dataset_id, page, limit, q).map { p =>
          Ok(Json.obj("total_count" ->p._1, "summaries" -> p._2))
        }
      }
  }


  def updateRandomForestSecured(user_id: String,
                       dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, UpdatePrivilege) {
      Action.async(parse.json) {
        implicit request =>
          val decision_tree_meta = Json.fromJson[RandomForestMeta](request.body)
          decision_tree_meta match {
            case JsSuccess(u: RandomForestMeta, _) => {
              jobService.buildRandomForest(user_id, u, Some(id)) map { result =>
                Status(200)(Json.obj("status" -> "Building Random Forest tree's"))
              }
            }
            case e: JsError => handleValidationError(e)
          }
      }
  }
}