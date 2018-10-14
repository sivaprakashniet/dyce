package controllers

import javax.inject._

import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities.PrepareDataset
import job.{JobResult, JobService}
import services.DataPreparationService
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import util.Secured
import play.api.libs.json.{Json, _}

import scala.concurrent.Future


class DataPreparationController @Inject()(jobService: JobService,
                                          jobResult: JobResult,
                                          dataPreparationService: DataPreparationService,
                                          secured: Secured) extends Controller {

  // Create new dataset from existing ones

  def prepareDatasetSecured(user_id: String) = secured.withUser(user_id) {
    Action.async(parse.json) { implicit request =>
      val meta = Json.fromJson[PrepareDataset](request.body)
      meta match {
        case JsSuccess(u: PrepareDataset, _) => {
          dataPreparationService.prepareDataset(user_id, u) map { result =>
            if(result.isLeft)
              Status(500)(Json.obj("status" -> result.left.get))
            else
              Status(200)(Json.toJson(result.right.get))
          }
        }
        case e: JsError => handleValidationError(e)
      }
    }
  }

  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))

    )

}