package controllers


import javax.inject._

import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities._
import job.{JobResult, JobService}
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.TABLE
import services.{AdhocQueryService, FileService}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import util.{Secured, AppUtils}
import play.api.libs.json.{Json, _}

import scala.concurrent.Future

/**
  * Created by ramesh on 9/13/17.
  */
class AdhocQueryController @Inject()(fileService: FileService,
                                     secured: Secured,
                                     adhocQueryService: AdhocQueryService,
                                     utils: AppUtils
                                     ) extends Controller {


  /**
    * Executes the query submitted from the front end. Before using this,
    * make sure the context is set correctly
    * @param user_id
    * @return
    */
  def executeQuery(user_id: String, dataset_id: String) =
    Action.async(parse.json) { implicit request =>
      val q = Json.fromJson[AdhocQueryParam](request.body)

      q match {
        case JsSuccess(adhoc_query_params, path) =>

          adhocQueryService.executeAdhocQuery(adhoc_query_params, user_id) map {
            dataView =>  Ok(Json.toJson(dataView))
          }

        case e: JsError => handleValidationError(e)
      }
    }


  /**
    * When the user comes to the explore screen, the context needs to be
    * initialized. This will ensure that an external table for the dataset is
    * defined
    * @param user_id
    * @param dataset_id
    * @return
    */
  def initCtx(user_id: String, dataset_id: String) = {
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {

      Action.async {
        val result = for {
          fileMetadata <- fileService.getFileMetadataById(dataset_id)
          columnMetadata <- fileService.getColumnMetadata(dataset_id)

        } yield adhocQueryService.initAdhocQueryCtx(fileMetadata.get,
          columnMetadata.toList, user_id)

        result map {
          x => Ok(Json.toJson(x))
        }

      } //end of Action.async
    } //end of secured.withProtectedAcl
  }


  private def handleValidationError(e: JsError): Future[Result] =
    Future.successful(
      Status(400)(
        Json.obj("errors" -> JsError.toJson(e).toString()))
    )




}
