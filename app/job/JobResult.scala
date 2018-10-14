package job

//Mongodb
import java.util.UUID
import javax.inject.Inject

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsArray, JsObject, Json}
import reactivemongo.api.{Cursor, QueryOpts, ReadPreference}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.play.json.collection.JSONCollection
import filesystem.FilesystemService
import dao._
import entities._
import controllers.ControllerImplicits._
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONObjectID
import services.UserService

import scala.concurrent.Future

class JobResult @Inject()(val reactiveMongoApi: ReactiveMongoApi,
                          filesystemService: FilesystemService,
                          fileMetadataDAO: FileMetadataDAO,
                          columnMetadataDAO: ColumnMetadataDAO,
                          modelColumnMetaDAO: ModelColumnMetaDAO,
                          anomalyDAO: AnomalyDAO,
                          scoreDAO: ScoreDAO,
                          modelDAO: ModelDAO,
                          userService:UserService,
                          jobDAO: JobDAO) {



  import play.modules.reactivemongo.json._

  lazy val db = reactiveMongoApi.db
  lazy val univariate_collection = db.collection[JSONCollection]("univariate")
  lazy val bivariate_collection = db.collection[JSONCollection]("bivariate")
  lazy val decision_collection = db.collection[JSONCollection]("decisiontree")
  lazy val gbt_collection = db.collection[JSONCollection]("gradient_boosted_trees")
  lazy val rf_collection = db.collection[JSONCollection]("random_forest")

  // GET univariate API with pagination
  def getUnivariateSummary(dataset_id: String, page_no: Int, limit: Int): Future[(Int, JsArray)] = {
    val offset = (limit * (page_no - 1))-(page_no - 1)+(page_no-1)
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id))
      .sort(Json.obj("position" ->1))
      .options(QueryOpts(skipN = offset))
      .cursor[JsObject](ReadPreference.primaryPreferred)

    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List](limit)
    futureUnivariateList.flatMap { p =>
      getUnivariateWithTotal(Json.arr(p), dataset_id)
    }
  }

  def getMaxBins(dataset_id:String, list_columns:List[String]):Future[Int] = {
    ///val list_columns = List("c68a6f39-2177-46bf-a9ca-c57aa0f75643","3316b91c-ef6e-4316-ab4a-6f9b7dfe7db8")
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id, "column_id" -> Json.obj("$in" -> list_columns)), Json.obj("column_datatype"->1, "metrics" ->1))
      .cursor[JsObject](ReadPreference.primaryPreferred)

    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
    val list_of_dist = List()
   val x = futureUnivariateList map {  objs =>
     objs map { ob =>
       val dist = (ob \ "metrics" \ "distinct").get.as[String].toInt
       if((ob \ "column_datatype").get.as[String] == "Number" && dist < 20)
         dist :: list_of_dist
       else if ((ob \ "column_datatype").get.as[String] == "Category" && dist < 100)
          dist::list_of_dist
       else
         0::list_of_dist
     }
    }
    val list = x.map { res =>
      res map ( x => x(0))
    }
    list map  { y =>
      if(y.isEmpty) 32 else y.max
    }
  }

  def getUnivariateSumaryByCol(dataset_id: String,
                                column_id: String):Future[JsObject]  = {
    val cursor: Cursor[JsObject] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id, "column_id" -> column_id), Json.obj("metrics" ->1))
      .cursor[JsObject](ReadPreference.primaryPreferred)

    val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
     val x =  futureUnivariateList map {  res =>
        res(0)
      }
    x
    }



  def getUnivariateWithTotal(arr: JsArray,
                             dataset_id: String): Future[(Int, JsArray)] = {

    val cursor_total: Future[List[JsObject]] = univariate_collection
      .find(Json.obj("dataset_id" -> dataset_id))
      .cursor[JsObject](ReadPreference.primaryPreferred).collect[List]()
    cursor_total map { x =>
      (x.length, arr)
    }
  }



  def removeColumnsSummary(dataset_id: String,
                           list_of_cols: List[String]): Future[WriteResult] = {
    univariate_collection
      .remove(Json.obj("dataset_id" -> dataset_id,
        "column_id" -> Json.obj("$in" -> list_of_cols))).flatMap { res =>
      bivariate_collection
        .remove(Json.obj("dataset_id" -> dataset_id,
          "column_1_id" -> Json.obj("$in" -> list_of_cols))).flatMap { res1 =>
        bivariate_collection
          .remove(Json.obj("dataset_id" -> dataset_id,
            "column_2_id" -> Json.obj("$in" -> list_of_cols)))
      }
    }
  }


  // GET Decision API with pagination
  def getDecisiontree(dataset_id: String,
                      page:Int, limit:Int, q: Option[String]): Future[(Int, JsArray)] = {

    val offset = (limit * (page - 1))-(page - 1)+(page-1)
    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"), "$options" -> "i"))

    val cursor: Cursor[JsObject] = decision_collection
      .find(whr, Json.obj("name" -> 1, "created_date" -> 2))
      .sort(Json.obj("created_date" -> -1))
      .options(QueryOpts(skipN = offset))
      .cursor[JsObject]
    val futureDecisionTreeList: Future[List[JsObject]] = cursor.collect[List](limit)

    futureDecisionTreeList.flatMap { p =>
      getDecisionWithTotal(Json.arr(p), q, dataset_id)
    }
  }

  def getDecisionWithTotal(arr: JsArray, q:Option[String],
                             dataset_id: String): Future[(Int, JsArray)] = {

    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"), "$options" -> "i"))

    val cursor_total: Future[List[JsObject]] = decision_collection
      .find(whr)
      .cursor[JsObject](ReadPreference.primaryPreferred).collect[List]()

    cursor_total map { x =>
      (x.length, arr)
    }
  }

  def updateErrorInFileMetaData(dataset: FileMetadata): Future[Int] = {
    val update_status = dataset.copy(download_status = Some("MODIFIED"))
    fileMetadataDAO.updateFileMeta(update_status.id.get, update_status)
  }

  def getOneDecisiontree(dataset_id: String, object_id: String): Future[JsArray] = {
    val cursor: Cursor[JsObject] = decision_collection
      .find(Json.obj("_id" -> Json.obj("$oid" -> object_id))).cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.map { p => Json.arr(p) }
  }


  def deleteDecisiontree(dataset_id: String, id: String): Future[Boolean] =
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(decision_tree_count = Some(dataset.get.decision_tree_count.get - 1))
      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) map { status =>
        decision_collection.findAndRemove(Json.obj("_id" -> Json.obj("$oid" -> id))).isCompleted
      }
    }

  def getBivariateSummaries(dataset_id: String): Future[JsArray] = {
    val cursor: Cursor[JsObject] = bivariate_collection
      .find(Json.obj("dataset_id" -> dataset_id)).cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.map { p => Json.arr(p) }
  }

  // Need to refactor code
  def getBivariateSummary(u: BivariateMetaRequest): Future[JsArray] = {
    val cursor: Cursor[JsObject] = bivariate_collection
      .find(Json.obj("dataset_id" -> u.dataset_id, "column_1_id" -> u.column_1_id, "column_2_id" -> u.column_2_id)).cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.flatMap {
      p =>
        if (p.length > 0) Future {
          Json.arr(p)
        } else {
          val cursor_alt: Cursor[JsObject] = bivariate_collection
            .find(Json.obj("dataset_id" -> u.dataset_id,
              "column_1_id" -> u.column_2_id, "column_2_id" -> u.column_1_id)).cursor[JsObject]
          val futureBivariateList: Future[List[JsObject]] = cursor_alt.collect[List]()
          futureBivariateList map {
            p => Json.arr(p)
          }
        }
    }
  }

  def getCorrelationSummary(dataset_id: String): Future[CorrelationSummary] = {
    val cursor: Cursor[JsObject] = bivariate_collection
      .find(Json.obj("dataset_id" -> dataset_id))
      .sort(Json.obj("column_1_name" ->1))
      .cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.map { x =>
      val list_details = x
      //x.map(y => y - "_id")
      val biv = list_details.map(x => Json.fromJson[BivariateSummary](Json.toJson(x)).get)

      val column_ids_and_names = biv.map(p => (p.column_1_id, p.column_1_name)).distinct
      val column_ids = column_ids_and_names.map(c => c._1)
      val column_names: List[String] = column_ids_and_names.map(c => c._2)
      val corr = column_ids.map(x => column_ids.map(y => {
        val filtered_biv = biv.filter(bi => ((bi.column_1_id == x) && (bi.column_2_id == y))
          || ((bi.column_1_id == y) && (bi.column_2_id == x)))

        if(x == y) "1.0" else if(filtered_biv.length == 0 ) "0.0"  else  filtered_biv(0).correlation
      }))
      new CorrelationSummary(column_names, corr)
    }
  }

  def saveUnivariateFromS3(file_path: String): Future[List[Boolean]] = {
    filesystemService.readJson(file_path) flatMap { result =>
      Future.sequence({
        Json.parse(result.get).as[List[JsObject]].map { obj =>
          univariateSummary(obj)
        }
      })
    }
  }

  def saveNumericBinFromS3(response_filepath: String):Future[List[Boolean]] = {
    filesystemService.readJson(response_filepath) map { result =>
      Json.parse(result.get).as[List[JsObject]].map { obj =>
        univariateNumericBinSummary(obj)
      }
    }
  }

  def univariateSummary(n: JsObject): Future[Boolean] = {
    univariate_collection.remove(Json.obj("dataset_id" -> (n \ "dataset_id").get,
      "column_id" -> (n \ "column_id").get)) map { res =>
      univariate_collection.insert(n).isCompleted
    }
  }

  def univariateNumericBinSummary(n: JsObject):Boolean = {
    univariate_collection.insert(n).isCompleted
  }

  def saveBivariateFromS3(file_path: String): Future[List[Boolean]] = {
    filesystemService.readJson(file_path) flatMap { result =>
      Future.sequence({
        Json.parse(result.get).as[List[JsObject]].map { obj =>
          bivariateSummary(obj)
        }
      })
    }
  }


  def bivariateSummary(n: JsObject): Future[Boolean] = {
    bivariate_collection.remove(Json.obj("dataset_id" -> (n \ "dataset_id").get.as[String],
      "column_1_id" -> (n \ "column_1_id").get.as[String],
      "column_2_id" -> (n \ "column_2_id").get.as[String])) flatMap { res =>
      bivariate_collection.remove(Json.obj("dataset_id" -> (n \ "dataset_id").get.as[String],
        "column_2_id" -> (n \ "column_1_id").get.as[String],
        "column_1_id" -> (n \ "column_2_id").get.as[String])) map { res =>
        bivariate_collection.insert(n).isCompleted
      }
    }
  }

  def updateJobStatus(job_id: Long, job_staus: Int): Future[Long] = {
    val status = if (job_staus == 0) "FINISHED" else "ERROR"
    jobDAO.updateJob(job_id, status)
  }

  def savePreProcessFile(result_path:String, dataset_id: String,
                         response_file_path: String, status: Int): Future[Int] = {
    val process_status = if (status == 0) "PROCESSED" else "ERROR"
    filesystemService.readJson(result_path) flatMap { result =>
      val metrics = Json.parse(result.get).as[JsObject]

      val rows = (metrics \ "num_of_rows").get.as[Long]
      val cols = (metrics \ "num_of_cols").get.as[Int]

      columnMetadataDAO.ignoreColumnMetadataByDataset(dataset_id) flatMap { res =>
        fileMetadataDAO.updateParquetFilePath(dataset_id, response_file_path,
          process_status, rows, cols)
      }
    }
  }

  def saveUpdatedParquetFile(dataset_id: String, response_file_path: String,
                             status: Int, cols: Int): Future[Int] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset=>
      fileMetadataDAO.updateParquetFilePath(dataset_id, response_file_path,
        "PROCESSED", dataset.get.number_of_rows.get, cols)
    }
  }



  def updateFileMetaData(dataset: FileMetadata,
                         download_dataset_path: String):Future[Int] = {
    fileMetadataDAO.updateDownloadPath(dataset.id, download_dataset_path)
  }

  def saveDecisionTreeFromS3(response_filepath: String,
                             tree_id: Option[String],
                             dataset_id: String): Future[(String, String)] = {
    filesystemService.readJson(response_filepath) flatMap { result =>
      val decision_tree_obj = Json.parse(result.get).as[JsObject]
      updateTreeCountAndSave(decision_tree_obj, dataset_id, tree_id)
    }
  }

  def updateTreeCountAndSave(decision_tree_obj: JsObject,
                             data_id: String, tree_id: Option[String]): Future[(String, String)] = {
    fileMetadataDAO.getFileMetadataById(data_id) flatMap { dataset =>
      val cnt = if (tree_id != None) dataset.get.decision_tree_count.get
      else dataset.get.decision_tree_count.get + 1
      val file_meta = dataset.get.copy(decision_tree_count = Some(cnt))
      fileMetadataDAO.updateFileMeta(data_id, file_meta) map { status =>
        saveDecisonTree(decision_tree_obj, tree_id)
      }
    }
  }

  def saveDecisonTree(obj: JsObject, tree_id: Option[String]): (String, String) = {
    val id = BSONObjectID.generate

    val model = "DecisionTree " + (obj \ "model_parameters" \ "decision_tree_type").get.as[String]
    if (tree_id != None){
      decision_collection.findAndUpdate(
        Json.obj("_id" -> Json.obj("$oid" -> tree_id)), obj).isCompleted
      (model, tree_id.get)

    } else{
      val x =
        decision_collection.insert(obj ++ Json.obj("_id" -> id))
      (model, id.stringify)
    }

  }


  def saveAnomalyDataset(meta: AnomalyMeta): Future[AnomalySummary] = {
    val columns_ids = meta.feature_column_ids.get mkString ","
    val a = new AnomalySummary(Some(createUUID), meta.name, meta.dataset_id,
      meta.dataset_name, meta.trees, meta.sample_proportions, meta.no_of_anomalies,
      meta.features, meta.bootstrap, meta.replacement, meta.random_split,
      meta.path, Some(currentDateTime), columns_ids)

    updateAnomalyCountAndSave(meta.dataset_id, a)
  }

  // Save Anomaly detections
  def updateAnomalyCountAndSave(dataset_id: String,
                                meta: AnomalySummary): Future[AnomalySummary] = {
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(anomaly_count = Some(dataset.get.anomaly_count.get + 1))
      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) flatMap { status =>
        anomalyDAO.create(meta)
      }
    }
  }

  def createUUID = UUID.randomUUID().toString

  def currentDateTime = System.currentTimeMillis()

  // GBT's
  def saveGBTreeFromS3(response_filepath: String,
                       tree_id: Option[String], dataset_id: String): Future[(String, String)] = {
    filesystemService.readJson(response_filepath) flatMap { result =>
      val tree_obj = Json.parse(result.get).as[JsObject]
      updateGBTCountAndSave(tree_obj, dataset_id, tree_id)
    }
  }

  def updateGBTCountAndSave(obj: JsObject,
                            data_id: String, tree_id: Option[String]): Future[(String, String)] = {
    fileMetadataDAO.getFileMetadataById(data_id) flatMap { dataset =>
      val cnt = if (tree_id != None) dataset.get.gbt_count.get
      else dataset.get.gbt_count.get + 1
      val file_meta = dataset.get.copy(gbt_count = Some(cnt))
      fileMetadataDAO.updateFileMeta(data_id, file_meta) map { status =>
        saveGBT(obj, tree_id)
      }
    }
  }

  def saveGBT(obj: JsObject, tree_id: Option[String]): (String, String) = {
    val model_type = "GBT " + (obj \ "model_type").get.as[String]
    val id = BSONObjectID.generate

    if (tree_id != None) {
      gbt_collection.findAndUpdate(
        Json.obj("_id" -> Json.obj("$oid" -> tree_id)), obj).isCompleted
      (model_type, tree_id.get)
    }else{
      val x = gbt_collection.insert(obj ++ Json.obj("_id" -> id))
      (model_type, id.stringify)
    }

  }

  def deleteGBT(dataset_id: String, id: String): Future[Boolean] =
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(gbt_count = Some(dataset.get.gbt_count.get - 1))
      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) map { status =>
        gbt_collection.findAndRemove(Json.obj("_id" -> Json.obj("$oid" -> id))).isCompleted
      }
    }

  def getOneGBT(dataset_id: String, object_id: String): Future[JsArray] = {
    val cursor: Cursor[JsObject] = gbt_collection
      .find(Json.obj("_id" -> Json.obj("$oid" -> object_id))).cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.map { p => Json.arr(p) }
  }


  def getGBT(dataset_id: String, page:Int, limit:Int,
             q: Option[String]): Future[(Int, JsArray)] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)

    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"), "$options" -> "i"))

    val cursor: Cursor[JsObject] = gbt_collection
      .find(whr, Json.obj("name" -> 1, "created_date" -> 2))
      .sort(Json.obj("created_date" -> -1))
      .options(QueryOpts(skipN = offset))
      .cursor[JsObject]
    val futureGBTList: Future[List[JsObject]] = cursor.collect[List](limit)
    futureGBTList.flatMap { p =>
      getGBTListWithTotal(Json.arr(p),q, dataset_id)
    }
  }
  def getGBTListWithTotal(arr: JsArray,q: Option[String],
                           dataset_id: String): Future[(Int, JsArray)] = {

    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"),"$options" -> "i"))

    val cursor_total: Future[List[JsObject]] = gbt_collection
      .find(whr)
      .cursor[JsObject](ReadPreference.primaryPreferred).collect[List]()
    cursor_total map { x =>
      (x.length, arr)
    }
  }
  //Random Forest
  def saveRandomForestFromS3(response_filepath: String,
                             tree_id: Option[String], dataset_id: String): Future[(String, String)] = {
    filesystemService.readJson(response_filepath) flatMap { result =>
      val tree_obj = Json.parse(result.get).as[JsObject]
      updateRandomForestCountAndSave(tree_obj, dataset_id, tree_id)
    }
  }

  def updateRandomForestCountAndSave(obj: JsObject,
                                     data_id: String, tree_id: Option[String]): Future[(String, String)] = {
    fileMetadataDAO.getFileMetadataById(data_id) flatMap { dataset =>
      val cnt = if (tree_id != None) dataset.get.random_forest_count.get
      else dataset.get.random_forest_count.get + 1
      val file_meta = dataset.get.copy(random_forest_count = Some(cnt))
      fileMetadataDAO.updateFileMeta(data_id, file_meta) map { status =>
        saveRandomForest(obj, tree_id)
      }
    }
  }

  def saveRandomForest(obj: JsObject, tree_id: Option[String]): (String, String) = {
    val model_type = "RandomForest " + (obj \ "model_type").get.as[String]
    val id = BSONObjectID.generate
    if (tree_id != None) {
      rf_collection.findAndUpdate(
        Json.obj("_id" -> Json.obj("$oid" -> tree_id)), obj).isCompleted
      (model_type, tree_id.get)
    }else {
      rf_collection.insert(obj).isCompleted
      (model_type, id.stringify)
    }
  }

  def deleteRandomForest(dataset_id: String, id: String): Future[Boolean] =
    fileMetadataDAO.getFileMetadataById(dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(random_forest_count = Some(dataset.get.random_forest_count.get - 1))

      fileMetadataDAO.updateFileMeta(dataset_id, file_meta) map { status =>
        rf_collection.findAndRemove(Json.obj("_id" -> Json.obj("$oid" -> id))).isCompleted
      }
    }

  def getOneRandomForest(dataset_id: String, object_id: String): Future[JsArray] = {
    val cursor: Cursor[JsObject] = rf_collection
      .find(Json.obj("_id" -> Json.obj("$oid" -> object_id))).cursor[JsObject]
    val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()
    futureBivariateList.map { p => Json.arr(p) }
  }

  def getRandomForest(dataset_id: String, page:Int, limit:Int, q:Option[String]): Future[(Int, JsArray)] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)
    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"), "$options" -> "i"))

    val cursor: Cursor[JsObject] = rf_collection
      .find(whr, Json.obj("name" -> 1, "created_date" -> 2))
      .sort(Json.obj("created_date" -> -1))
      .options(QueryOpts(skipN = offset))
      .cursor[JsObject]
    val futureRFList: Future[List[JsObject]] = cursor.collect[List](limit)
    futureRFList.flatMap { p =>
      getRFListWithTotal(Json.arr(p), q, dataset_id)
    }
  }


  def getRFListWithTotal(arr: JsArray, q:Option[String],
                          dataset_id: String): Future[(Int, JsArray)] = {
    var whr = Json.obj("dataset_id" -> dataset_id)
    if(q != None)
      whr = Json.obj("dataset_id" -> dataset_id,
        "name"->Json.obj("$regex" ->  (".*" + q.get + ".*"), "$options" -> "i"))

    val cursor_total: Future[List[JsObject]] = rf_collection
      .find(whr)
      .cursor[JsObject](ReadPreference.primaryPreferred).collect[List]()
    cursor_total map { x =>
      (x.length, arr)
    }
  }

  def saveModelScoreInS3(response_filepath: String,
                         meta: ModelScoreJob): Future[(String, String)] = {
    filesystemService.readJson(response_filepath) flatMap { result =>
      val tree_obj = Json.parse(result.get).as[JsObject]
      if (meta.model_meta.model_type == "DecisionTree Regression Model" || meta.model_meta.model_type == "DecisionTree Classification Model")
        updateTreeCountAndSave(tree_obj, meta.dataset.id.get, None)
      else if (meta.model_meta.model_type == "GBT Regression Model" || meta.model_meta.model_type == "GBT Classification Model")
        updateGBTCountAndSave(tree_obj, meta.dataset.id.get, None)
      else
        updateRandomForestCountAndSave(tree_obj, meta.dataset.id.get, None)
    }
  }


  def updateModelColumnMeta(columns: Seq[ColumnMetadata],
                            output_column: ColumnMetadata, model_id: String): Future[Option[Int]] =  {
    val output_data:ModelColumnMetadata = new ModelColumnMetadata(Some(createUUID), model_id, output_column.name, output_column.description, output_column.position.toInt, output_column.datatype,
      output_column.format, output_column.separator, output_column.decimal,
      output_column.dataset_id,output_column.calculated, true)

    val input_data:List[ModelColumnMetadata] = columns.map(x => new ModelColumnMetadata(Some(createUUID),
      model_id, x.name, x.description, x.position.toInt, x.datatype,
      x.format, x.separator, x.decimal, x.dataset_id,x.calculated, false)).toList
    val list_metas = output_data :: input_data
    modelColumnMetaDAO.saveColumnMetadata(list_metas)
  }


  def saveColumnModelMeta(model: ModelMeta, model_id: String):Future[Option[Int]] = {
    val input_columns:List[String] = model.input_column.split(",").map(_.trim).toList
    columnMetadataDAO.getColumnMetadataInName(input_columns, model.dataset_id) flatMap { columns =>
      columnMetadataDAO.getColumnMetadataInName(List(model.output_column), model.dataset_id) flatMap {
        output_column =>
        updateModelColumnMeta(columns, output_column(0), model_id)
      }
    }
  }

  def saveModel(model: ModelMeta, tree_id: Option[String]):Future[Option[Int]] = {
    if(tree_id == None)
      modelDAO.create(model.copy(created_date = Some(currentDateTime))) flatMap { model_id =>
        saveColumnModelMeta(model, model_id)
      }
    else
      modelDAO.update(tree_id.get, model.model_path) map(x => Some(x))
  }


  def saveModelScore(score_path: String, scored_dataset_id: String, meta: ModelScoreJob): Future[ScoreSummary] = {
    val new_score = new ScoreSummary(0, meta.name, meta.dataset.user_id.get,
      meta.dataset.id.get, meta.dataset.dataset_name, score_path, currentDateTime)
    fileMetadataDAO.getFileMetadataById(scored_dataset_id) flatMap { dataset =>
      val file_meta = dataset.get.copy(model_count = Some(dataset.get.model_count.get + 1))
      fileMetadataDAO.updateFileMeta(scored_dataset_id, file_meta) flatMap { status =>
        scoreDAO.create(new_score)
      }
    }
  }

  def saveFileColumnMeta(user_id: String, dataset_result_path: String,
                         query_meta: IngestParameters):Future[String] = {

   val file = new FileMetadata(Some(createUUID),dataset_result_path, query_meta.name.get,
      Some(currentDateTime.toString), Some(currentDateTime.toString),
     Some(user_id),Some(0),Some(0),Some(0),Some(""),Some(""),Some("UPLOADED"), Some(0),
      Some(0), Some(0),Some(0), Some(0),Some("FINISHED"))

    fileMetadataDAO.saveFileMetadata(file) flatMap { res =>
      val columns: List[ColumnMetadata] = query_meta.columns.get map(
        x=> x.copy(id = Some(createUUID),created_date = Some(""), modified_date=Some(""),
          new_name = Some(x.name), status = Some(""),raw_formula = Some(""),format="",
          display_format ="",formula ="", metrics = "", description = Some(x.name),dataset_id = Some(res)))
      userService.setACLPermission(Acl(None, user_id, res, "DatasetAPI", "*", "allow")) flatMap { acl =>
          columnMetadataDAO.saveColumnMetadata(columns) map { result =>
            res
          }
      }
    }
  }

}
