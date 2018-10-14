package job

import java.util.UUID
import javax.inject.Inject

import actors._
import actors.BivariateActor._
import actors.UnivariateActor._
import UpdateDatasetActor._
import actors.AnomalyDetectionActor.BuildIsolationForest
import actors.CreateFormQueryActor.CreateFromQuerySubmit
import actors.DatasetDownloadActor.Downloaddataset
import actors.DecisionTreeActor.BuildDecisionTree
import actors.GBTreeActor.BuildGBTrees
import actors.MasterActor.JobDAG
import actors.ModelScoreActor.BuildModelScore
import actors.PreProccesActor._
import actors.RandomForestActor.BuildRandomForest
import controllers.ControllerImplicits._
import entities._
import akka.actor.{ActorSystem, Props}
import dao.{ColumnMetadataDAO, FileMetadataDAO, JobDAO, ModelDAO}
import filesystem.{FilesystemService, S3Config}
import play.api.Configuration
import play.api.libs.json.{JsObject, JsValue, Json}
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONObjectID
import services.FileService

// For MongoDB
import reactivemongo.api.Cursor
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.play.json.collection.JSONCollection

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JobService @Inject()(val reactiveMongoApi: ReactiveMongoApi, s3Config: S3Config,
                           jobResult: JobResult, filesystemService: FilesystemService,
                           configuration: Configuration, fileMetadataDAO: FileMetadataDAO,
                           columnMetadataDAO: ColumnMetadataDAO, jobDAO: JobDAO,
                           modelDAO: ModelDAO,pipelineService:PipelineService,
                           fileService: FileService) {

  import play.modules.reactivemongo.json._

  lazy val db = reactiveMongoApi.db
  lazy val univariate_collection = db.collection[JSONCollection]("univariate")

  val _system = ActorSystem("Amplifyr-spark-jobs")

  def createUUID = UUID.randomUUID().toString
  def currentDateTime = System.currentTimeMillis()

  def preProcessDataset(user_id: String, dataset_id: String): Future[String] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Pre - Processing",
        currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))

      jobDAO.create(new_job) flatMap { job =>
        getFileAndColumnMeta(dataset_id).map { pre_process_meta =>
          val asynProcessor = _system.actorOf(Props(classOf[PreProccesActor], s3Config, jobResult,
            filesystemService, configuration), "job_" + createUUID)
          asynProcessor ! PreProcessorSubmit(user_id, pre_process_meta, job.job_id, None)
          dataset_id
        }
      }
    }
  }

  def buildScoreModel(model: ModelMeta, user_id: String,
                      dataset_id: String, name: String): Future[String] = {
      fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
        val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Model Scoring",
          currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
        jobDAO.create(new_job) map { job =>
          val score_meta = new ModelScoreJob(name, model, dataset.get)

          val asynProcessor = _system.actorOf(Props(classOf[ModelScoreActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
            asynProcessor ! BuildModelScore(user_id, dataset.get.parquet_file_path.get, dataset.get.id.get, score_meta, job.job_id)
            user_id
        }

      }
  }

  def scoreModel(user_id: String, dataset_id: String, req_model: ModelScoreMeta):Future[String] = {
    modelDAO.get(req_model.model_id) flatMap { model =>
      buildScoreModel(model.get,user_id,dataset_id, req_model.name)
    }

  }


  def getFileAndColumnMeta(dataset_id: String): Future[PreProcessMetaData] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { file_meta =>
      columnMetadataDAO.getColumnMetadata(dataset_id) map { columns =>
        new PreProcessMetaData(file_meta.get, columns.toList)
      }
    }
  }

  def computeUnivariate(user_id: String, metadata: JsValue,
                        univariate: UnivariateMeta): Future[UnivariateMeta] = {
    val new_job = new Job(0, user_id, univariate.dataset_id,univariate.dataset_name, "Univariate",
      currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
    jobDAO.create(new_job) map { job =>
      val asynProcessor = _system.actorOf(Props(classOf[UnivariateActor], s3Config,
        jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
      asynProcessor ! ComputeUnivariate(user_id, metadata, univariate, job.job_id, None)
      univariate
    }
  }

  def buildActorForGBT(user_id: String,
                       gbt_meta:GBTMeta,
                                job_id: Long, tree_id:Option[String]):Future[GBTMeta] = {
    fileMetadataDAO.getFileMetadataById(gbt_meta.dataset_id) map { dataset =>
      val asynProcessor = _system.actorOf(Props(classOf[GBTreeActor], s3Config,
        jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
      asynProcessor ! BuildGBTrees(user_id, tree_id, dataset.get.parquet_file_path.get, gbt_meta, job_id)
      gbt_meta
    }
  }

  def buildGBT(user_id: String,
               gbt_meta: GBTMeta, id:Option[String]):Future[GBTMeta] = {

    val new_job = new Job(0, user_id, gbt_meta.dataset_id, gbt_meta.dataset_name,
      "Gradient-Boosted Trees", currentDateTime, 0.toLong, "PROCESSING", currentDateTime,
      Some(""), Some(""))
    jobDAO.create(new_job) flatMap { job =>
      buildActorForGBT(user_id, gbt_meta, job.job_id, id)
    }
  }

  def buildActorForRandomForest(user_id: String,
                                rf_meta:RandomForestMeta,
                       job_id: Long, tree_id:Option[String]):Future[RandomForestMeta] = {
    fileMetadataDAO.getFileMetadataById(rf_meta.dataset_id) map { dataset =>
      val asynProcessor = _system.actorOf(Props(classOf[RandomForestActor], s3Config,
        jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
      asynProcessor ! BuildRandomForest(user_id, tree_id, dataset.get.parquet_file_path.get, rf_meta, job_id)
      rf_meta
    }
  }

  def buildRandomForest(user_id: String,
                        rf_meta: RandomForestMeta, id:Option[String]):Future[RandomForestMeta] ={

    val new_job = new Job(0, user_id, rf_meta.dataset_id,rf_meta.dataset_name, "Random Forest",
      currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
    jobDAO.create(new_job) flatMap { job =>
      buildActorForRandomForest(user_id, rf_meta, job.job_id, id)
    }
  }


  def buildActorForDecisionTree(user_id: String,
                                decision_tree_meta:DecisionTreeMeta,
                                job_id: Long, tree_id:Option[String]):Future[DecisionTreeMeta] = {
    fileMetadataDAO.getFileMetadataById(decision_tree_meta.dataset_id) map { dataset =>
      val asynProcessor = _system.actorOf(Props(classOf[DecisionTreeActor], s3Config,
        jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
        asynProcessor ! BuildDecisionTree(user_id, tree_id, dataset.get.parquet_file_path.get, decision_tree_meta, job_id)
        decision_tree_meta
    }
  }

  def buildDecisionTree(user_id: String,
                        decision_tree_meta:DecisionTreeMeta, id:Option[String]): Future[DecisionTreeMeta] = {
    val new_job = new Job(0, user_id, decision_tree_meta.dataset_id,decision_tree_meta.dataset_name, "DecisionTree",
      currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
    jobDAO.create(new_job) flatMap { job =>
      buildActorForDecisionTree(user_id, decision_tree_meta, job.job_id, id)
    }
  }





  def buildActorForIsolationForest(user_id: String,
                                   anomaly_meta:AnomalyMeta,
                                   job_id: Long):Future[String] = {
    fileMetadataDAO.getFileMetadataById(anomaly_meta.dataset_id) map { dataset =>
      val asynProcessor = _system.actorOf(Props(classOf[AnomalyDetectionActor], s3Config,
        jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
      asynProcessor ! BuildIsolationForest(user_id, dataset.get.parquet_file_path.get, anomaly_meta, job_id)
      user_id
    }
  }


  def buildIsolationForestModel(user_id: String, u: AnomalyMeta): Future[String] = {
    val new_job = new Job(0, user_id, u.dataset_id,u.dataset_name, "Anomaly",
      currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
    jobDAO.create(new_job) flatMap { job =>
      buildActorForIsolationForest(user_id, u, job.job_id)
    }
  }


  def downloadDataset(user_id: String, dataset_id: String): Future[String] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val update_status = dataset.get.copy(download_status = Some("PROCESSING"))
      fileMetadataDAO.updateFileMeta(dataset_id, update_status) flatMap { res =>
        val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Prepare Dataset Download",
          currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
        jobDAO.create(new_job) map { job =>
          val asynProcessor = _system.actorOf(Props(classOf[DatasetDownloadActor], s3Config,
            jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
          asynProcessor ! Downloaddataset(user_id, dataset.get, job.job_id)
          user_id
        }
      }
    }
  }


 def getDatasetAndupdateOrAddColumns(user_id:String, dataset_id: String,
                                     columns: SchemaList): Future[String] = {
   fileMetadataDAO.getFileMetadataById(dataset_id) flatMap {
     dataset => updateOrAddColumn(user_id, dataset.get, columns)
   }
 }
 def updateOrAddColumn(user_id:String, dataset:FileMetadata, columns: SchemaList):Future[String] = {
   val new_job = new Job(0, user_id, dataset.id.get,dataset.dataset_name, "Update or Add Columns",
     currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
   jobDAO.create(new_job) map { job =>
     val asynProcessor = _system.actorOf(Props(classOf[UpdateDatasetActor], s3Config,
       jobResult, filesystemService, configuration,fileService), "job_" + createUUID)
     asynProcessor ! UpdateDatasetWithColumns(user_id, dataset, columns, job.job_id)
     dataset.id.get
   }
 }

  def saveRequestDB(collection: JSONCollection,
                    request: JsObject): Future[WriteResult] = collection.insert(request)

  def computeBivariate(user_id: String, dataset_id: String): Future[String] = {
    val request_json_id = BSONObjectID.generate().stringify

    columnDetails(dataset_id) flatMap { cols =>

      val columns = cols map (c => (c \ "column_id").get.as[String])

      var pairs: List[BivariatePair] = List()
      for (a <- columns; b <- columns) yield
        if (!pairs.exists(p => (p.column_1_id == a) && (p.column_2_id == b) ||
          (p.column_1_id == b) && (p.column_2_id == a))) {
          pairs = pairs :+ new BivariatePair(a, b)
        }

      fileMetadataDAO.getFileMetadataById(dataset_id) flatMap (
        dataset => {
          val dataset_name = dataset.get.dataset_name
          val dataset_path = dataset.get.parquet_file_path
          val bivariateRequest = new BivariateRequest(request_json_id, dataset_id, dataset_name, cols, pairs)
          val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Bivariate",
            currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
          jobDAO.create(new_job) map (job => {
            val asynProcessor = _system.actorOf(Props(classOf[BivariateActor], s3Config,
              jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
            asynProcessor ! ComputeBivariate(user_id, dataset_path.get,
              job.job_id, Json.toJson(bivariateRequest))
            request_json_id
          })
        })
    }
  }

  def columnDetails(dataset_id: String): Future[List[JsObject]] = {
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id)).cursor[JsObject]
    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureUnivariateList map (res =>
      res map (x => x - ("dataset_id") - ("_id") - ("dataset_name") - ("missing") - ("histogram"))
      )
  }

  def getjobs(user_id: String, page:Int, limit:Int): Future[(Int, Seq[Job])] = {
    jobDAO.getJobCount(user_id) flatMap { count =>
      getJobWithCount(count, user_id, page, limit)
    }
  }

  def getJobWithCount(count: Int, user_id:String,
                      page: Int, limit: Int): Future[(Int, Seq[Job])] = {
    jobDAO.getJobs(user_id, page, limit) map { result =>
      (count, result)
    }
  }
  def getDatasetJob(dataset_id: String):Future[Seq[Job]] =  {
    jobDAO.getDatasetJob(dataset_id)
  }

  def createFromQueryJob(user_id: String, params: IngestParameters): Future[String] = Future {

    val master = _system.actorOf(Props(classOf[MasterActor],s3Config,
      jobResult, filesystemService, configuration, pipelineService, fileMetadataDAO), "master_with_create_query_"+createUUID)
      master ! JobDAG("Create-From-Query", user_id, "", Some(params))
      user_id
  }

  def numericBinColumn(user_id: String, dataset_id: String,
                       column_bin: List[ColumnBins]): Future[String] ={
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val meta = new NumericBinMeta(dataset_id, dataset.get.parquet_file_path.get,
        dataset.get.dataset_name, column_bin)
      val asynProcessor = _system.actorOf(Props(classOf[UnivariateActor], s3Config,
      jobResult, filesystemService, configuration, fileMetadataDAO), "job_" + createUUID)
      val new_job = new Job(0, user_id, dataset_id, dataset.get.dataset_name, "Numeric Binning",
        currentDateTime, 0.toLong, "PROCESSING", currentDateTime, Some(""), Some(""))
      jobDAO.create(new_job) map (job => {
        asynProcessor ! ComputeNumericBinUnivariate(user_id, Json.toJson(meta), dataset.get.parquet_file_path.get, job.job_id)
        user_id
      })
    }
  }

}