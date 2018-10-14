package controllers

import javax.inject._

import acl.ACLConfig._
import controllers.ControllerImplicits._
import entities._
import job.{JobResult, JobService}
import services.{FileService, ModelScoringService}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.mvc._
import util.Secured
import play.api.libs.json.{Json, _}

import scala.concurrent.Future

class SummaryController @Inject()(jobService: JobService,
                                  jobResult: JobResult,
                                  fileService: FileService,
                                  modelService: ModelScoringService,
                                  secured: Secured) extends Controller {

  // Getting univariate summary for a dataset
  def getUnivariateSecured(user_id: String,
                           dataset_id: String, page: Int, limit: Int) =
    secured.withProtectedAcl(user_id, dataset_id,
      DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getUnivariateSummary(dataset_id, page, limit).map { p =>
          Ok(Json.obj("total_count" -> p._1, "summary" -> p._2))
        }
      }
    }


  def getUnivariateSummarySecured(user_id: String,
                                  dataset_id: String, column_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getUnivariateSumaryByCol(dataset_id, column_id).map { p =>
          Ok(p)
        }
      }
    }

  def getUnivariateSummaryMaxBinSecured(user_id: String, dataset_id: String) = {
    Action.async(parse.json) { implicit request =>
      val metadata = request.body
      val cols = (metadata \ "columns").as[List[String]]
        jobResult.getMaxBins(dataset_id, cols).map { p =>
          Ok(Json.obj("max_bin" -> p))
        }
      }
  }

  def computeUnivariateSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, CreatePrivilege) {
      computeUnivariate(user_id, dataset_id)
    }

  def computeUnivariate(user_id: String,
                        dataset_id: String) = Action.async(parse.json) { implicit request =>
    val metadata = request.body
    val univariate_meta_data = Json.fromJson[UnivariateMeta](request.body)

    univariate_meta_data match {
      case JsSuccess(u: UnivariateMeta, _) => {
        jobService.computeUnivariate(user_id, metadata, u) map { result =>
          Status(200)(Json.obj("status" -> "Univariate computation started"))
        }
      }
      case e: JsError => handleValidationError(e)
    }
  }

  def getBivariateSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      getBivariate(user_id, dataset_id)
    }

  def getBivariate(user_id: String, dataset_id: String) = Action.async {
    jobResult.getBivariateSummaries(dataset_id).map { p =>
      Ok(p)
    }
  }

  def getBivariateSummarySecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      getBivariateSummary(user_id, dataset_id)
    }

  def getBivariateSummary(user_id: String, dataset_id: String) =
    Action.async(parse.json) { implicit request =>
      val metadata = request.body
      val bi_meta_data = Json.fromJson[BivariateMetaRequest](request.body)

      bi_meta_data match {
        case JsSuccess(u: BivariateMetaRequest, _) => {
          jobResult.getBivariateSummary(u) map { result =>
            Ok(result)
          }
        }
        case e: JsError => handleValidationError(e)
      }
    }

  def getCorrelationSummarySecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      getCorrelation(user_id, dataset_id)
    }

  def getCorrelation(user_id: String, dataset_id: String) = Action.async {
    jobResult.getCorrelationSummary(dataset_id).map { p =>
      Ok(Json.toJson(p))
    }
  }

  def computeBivariateSecured(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, CreatePrivilege) {
      computeBivariate(user_id, dataset_id)
    }

  def computeBivariate(user_id: String, dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        implicit request =>
          jobService.computeBivariate(user_id, dataset_id) map { result =>
            Status(200)(Json.obj("status" -> "Bivariate computation started"))
          }
      }
    }

  def getDatasetSatus(user_id: String,
                      dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id,
      DatasetResource, ReadPrivilege) {
      Action.async {
        implicit request =>
          jobService.getDatasetJob(dataset_id) map { jobs =>
            Ok(if (jobs == Nil) new JsArray(List()) else Json.toJson(jobs))
          }
      }
    }

  def buildDecisiontreeSecured(user_id: String,
                               dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async(parse.json) { implicit request =>
        val decision_tree_meta = Json.fromJson[DecisionTreeMeta](request.body)
        decision_tree_meta match {
          case JsSuccess(u: DecisionTreeMeta, _) => {
            jobService.buildDecisionTree(user_id, u, None) map { result =>
              Status(200)(Json.obj("status" -> "Building decision tree"))
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

  def buildAnomalySecured(user_id: String,
                          dataset_id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async(parse.json) { implicit request =>
        val decision_tree_meta = Json.fromJson[AnomalyMeta](request.body)
        decision_tree_meta match {
          case JsSuccess(u: AnomalyMeta, _) => {
            jobService.buildIsolationForestModel(user_id, u) map { result =>
              Status(200)(Json.obj("status" -> "Building anomaly"))
            }
          }
          case e: JsError => handleValidationError(e)
        }
      }
    }

  def deleteDecisiontreeSecured(user_id: String,
                                dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, DeletePrivilege) {
      Action.async {
        jobResult.deleteDecisiontree(dataset_id, id).map { p =>
          Status(200)(Json.obj("status" -> "Decision Tree deleted"))
        }
      }
    }

  def getOneDecisiontreeSecured(user_id: String,
                                dataset_id: String, id: String) = Action.async {
    jobResult.getOneDecisiontree(dataset_id, id).map { p =>
      Ok(p)
    }
  }

  def getDecisiontreeSecured(user_id: String, dataset_id: String,
                             page: Int, limit: Int, q: Option[String]) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async {
        jobResult.getDecisiontree(dataset_id, page, limit, q).map { p =>
          Ok(Json.obj("total_count" -> p._1, "summaries" -> p._2))
        }
      }
    }

  def updateDecisiontreeSecured(user_id: String,
                                dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async(parse.json) { implicit request =>
        val decision_tree_meta = Json.fromJson[DecisionTreeMeta](request.body)
        decision_tree_meta match {
          case JsSuccess(u: DecisionTreeMeta, _) => {
            jobService.buildDecisionTree(user_id, u, Some(id)) map { result =>
              Status(200)(Json.obj("status" -> "Building decision tree"))
            }
          }
          case e: JsError => handleValidationError(e)
        }
      }
    }


  def getJob(user_id: String, page: Int, limit: Int) = secured.withUser(user_id) {
    Action.async {
      implicit request =>
        jobService.getjobs(user_id, page, limit) map { response =>
          Ok(if (response._2 == Nil) new JsArray(List())
          else Json.obj("total_count" -> response._1, "jobs" -> Json.toJson(response._2)))
        }
    }
  }

  def getAnomaliesSecured(user_id: String, dataset_id: String,
                          page: Int, limit: Int, q: Option[String]) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async { implicit request =>
        fileService.getAnomalies(user_id, dataset_id, page, limit, q.get) map { response =>
          Ok(if (response._2 == Nil) new JsArray(List())
          else Json.obj("total_count" -> response._1, "anomalies" -> Json.toJson(response._2)))
        }
      }
    }

  def getAnomalyByIdSecured(user_id: String, dataset_id: String, id: String,
                            page: Int, limit: Int) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async { implicit request =>
        fileService.getAnomalyById(user_id, id, limit, page) map { a =>
          Ok(if (a == None) new JsArray(List()) else Json.toJson(a))
        }
      }
    }


  def deleteAnomalyByIdSecured(user_id: String,
                               dataset_id: String, id: String) =
    secured.withProtectedAcl(user_id, dataset_id, DatasetResource, ReadPrivilege) {
      Action.async { implicit request =>
        fileService.deleteAnomalyById(user_id, dataset_id, id) map { res =>
          Status(200)(Json.obj("status" -> "Decision Tree deleted"))
        }
      }
    }

  // Model scoring API
  def getModel(user_id: String,
              page_no: Option[Int], limit: Option[Int]) = Action.async {
    modelService.findByUserID(user_id, page_no, limit) map {  res =>
      Ok(if (res._2 == Nil) new JsArray(List())
      else Json.obj("total_count" -> res._1, "models" -> Json.toJson(res._2)))
    }
  }

  def scoreModel(user_id: String, dataset_id: String) = secured.withUser(user_id) {
    Action.async(parse.json) { implicit request =>
      val meta = Json.fromJson[ModelScoreMeta](request.body)
      meta match {
        case JsSuccess(u: ModelScoreMeta, _) => {
          jobService.scoreModel(user_id,dataset_id, u) map { result =>
            Status(200)(Json.obj("status" -> "scoring model"))
          }
        }
        case e: JsError => handleValidationError(e)
      }
    }
  }

  def getModelScore(user_id: String, dataset_id: String,
                    page_no: Option[Int], limit: Option[Int], q:Option[String]) = Action.async {
    modelService.getScores(dataset_id, page_no, limit, q) map {  res =>
      Ok(if (res._2 == Nil) new JsArray(List())
      else Json.obj("total_count" -> res._1, "models" -> Json.toJson(res._2)))
    }
  }

  def validateModelName(user_id: String, dataset_id: String) = Action.async(parse.json) { implicit request =>
    val json = request.body
    val json_to_map = Json.fromJson[CheckDuplidateModelName](request.body)
    json_to_map match {
      case JsSuccess(u, path) => {
        modelService.checkModelName(user_id, dataset_id, u) map { result =>
          if(result)
            Status(409)(Json.obj("status" -> "Model name already exist"))
          else
            Status(200)(Json.obj("status" -> "valid name"))
        }
      }
      case e: JsError => handleValidationError(e)
    }
  }



  def getScore(user_id: String, dataset_id: String,
               id: Long,page_no: Int, limit: Int) = Action.async { implicit request =>
    fileService.previewScore(id, limit, page_no) map { a =>
      Ok(if (a == None) new JsArray(List()) else Json.toJson(a))
    }
  }

  def deleteModel(user_id: String, id: String) = Action.async {
    modelService.deleteById(user_id, id) map {  res =>
      Status(200)(Json.obj("status" -> "Model deleted success"))
    }
  }


  def deleteScoreById(user_id: String, dataset_id: String, id: Long) = Action.async {
    modelService.deleteScoreById(id, dataset_id) map {  res =>
      Status(200)(Json.obj("status" -> "Model score deleted success"))
    }
  }

}