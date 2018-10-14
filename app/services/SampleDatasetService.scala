  package services

  import javax.inject.Inject

  import dao._
  import entities.{Acl, ColumnMetadata, FileMetadata, User, Job}
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import play.api.libs.json.{JsObject, Json, Writes}
  import reactivemongo.api.{Cursor, ReadPreference}
  import play.modules.reactivemongo.ReactiveMongoApi
  import reactivemongo.play.json.collection.JSONCollection
  import play.modules.reactivemongo.ReactiveMongoApi
  import filesystem.FilesystemService
  import scala.concurrent.Future

  class SampleDatasetService @Inject()(val reactiveMongoApi: ReactiveMongoApi,
                                        userDAO: UserDAO,
                                       fileMetadataDAO: FileMetadataDAO,
                                       columnMetadataDAO: ColumnMetadataDAO,
                                       anomalyDAO: AnomalyDAO,
                                       filesystemService: FilesystemService,
                                       jobDAO: JobDAO,
                                       aclDAO: AclDAO) {


  //MongoDB  collections
    import play.modules.reactivemongo.json._
    lazy val db = reactiveMongoApi.db
    lazy val univariate_collection = db.collection[JSONCollection]("univariate")
    lazy val bivariate_collection = db.collection[JSONCollection]("bivariate")
    lazy val decision_collection = db.collection[JSONCollection]("decisiontree")
    lazy val gbt_collection = db.collection[JSONCollection]("gradient_boosted_trees")
    lazy val rf_collection = db.collection[JSONCollection]("random_forest")




    val sampel_dataset_id = "5ac9b629-0be7-4fa6-8062-dbc68c746f86";
    
    def currentDateTime = System.currentTimeMillis()
    def createUUID: String = java.util.UUID.randomUUID.toString


    def createUser(u: User): Future[List[Boolean]] = {
      val new_user = u.copy(id = Some(createUUID),created_date = Some(currentDateTime),
        modified_date = Some(currentDateTime), role = "USER")
      userDAO.create(new_user) flatMap { user =>
        findAndCreateDataset(user)
      }
    }

    def getAndSaveColumnMeta(user: User, file_meta: FileMetadata,
                             sampel_dataset_id: String):Future[List[Boolean]]= {
      columnMetadataDAO.getColumnMetadata(sampel_dataset_id) flatMap { cls =>
        val cols:List[ColumnMetadata] = cls.map(x => x.copy(id = Some(createUUID),dataset_id = file_meta.id)).toList
        updateColumnMeta(file_meta, cols, user)
      }
    }

    def findAndCreateDataset(user: User):Future[List[Boolean]] = {
      fileMetadataDAO.getFileMetadataById(sampel_dataset_id) flatMap
        { dataset =>
          val file_meta = dataset.get.copy(id = Some(createUUID), user_id = user.id)
          //save files in s3
          val status = filesystemService.copyFileOrFolder(dataset.get.parquet_file_path.get,
            dataset.get.parquet_file_path.get,
            dataset.get.user_id.get, user.id.get)

          println(status)

          fileMetadataDAO.saveFileMetadata(file_meta) flatMap { data =>
            val new_acl = new Acl(None, user.id.get, file_meta.id.get, "DatasetAPI", "*", "allow")
            aclDAO.createACL(new_acl) flatMap { acl =>
              getAndSaveColumnMeta(user, file_meta, sampel_dataset_id)
            }
          }
      }
   }

    def getIdWithList(name: String, meta: List[ColumnMetadata]):String =
      meta.filter(_.name==name)(0).id.get

    def updateBiveriate(file_meta:FileMetadata, sampel_dataset_id: String,
                        metadatas: List[ColumnMetadata]):Future[List[Boolean]] = {

      val cursor: Cursor[JsObject] = bivariate_collection
        .find(Json.obj("dataset_id" -> sampel_dataset_id)).cursor[JsObject]
      val futureBivariateList: Future[List[JsObject]] = cursor.collect[List]()

     futureBivariateList.map { p =>
          p.map (x => x - ("_id")).map { obj =>
            var modified_obj = withField(obj, "dataset_id",file_meta.id.get)
            val  col_1_name= (obj \ "column_1_name").get.as[String]
            val  col_2_name= (obj \ "column_2_name").get.as[String]

            modified_obj = withField(modified_obj,
              "column_1_id", getIdWithList(col_1_name, metadatas))
            modified_obj = withField(modified_obj,
              "column_2_id", getIdWithList(col_2_name, metadatas))


            bivariate_collection.insert(modified_obj).isCompleted
          }
      }
    }


    def getAndUpdateUnivariate(file_meta:FileMetadata,sampel_dataset_id: String,
                               metadatas: List[ColumnMetadata], user: User):Future[List[Boolean]]= {

      cloneAnomaly(sampel_dataset_id, file_meta.id.get, user) flatMap { result =>
      updateBiveriate(file_meta, sampel_dataset_id, metadatas) flatMap { res =>
        Future.sequence(metadatas.map { colmns =>
          val cursor: Cursor[JsObject] = univariate_collection
            .find(Json.obj("dataset_id" -> sampel_dataset_id, "column_name" -> colmns.name))
            .cursor[JsObject](ReadPreference.primaryPreferred)

          val futureUnivariateList: Future[List[JsObject]] = cursor.collect[List]()
          futureUnivariateList.map { res =>
            val list_objs = res map (x => x - ("_id"))
            list_objs.map { x =>
              println(colmns.dataset_id)
              var modified_obj = withField(x, "dataset_id", colmns.dataset_id)
              modified_obj = withField(modified_obj, "column_id", colmns.id.get)
              println(modified_obj)
              univariateSummary(modified_obj)
            }
            true
          }
        })
      }
    }
    }

    def univariateSummary(n: JsObject): Future[Boolean] = {
      univariate_collection.remove(Json.obj("dataset_id" -> (n \ "dataset_id").get,
        "column_id" -> (n \ "column_id").get)) map { res =>
        univariate_collection.insert(n).isCompleted
      }
    }

    def updateColumnMeta(file_meta: FileMetadata, metadatas: List[ColumnMetadata],
                         user: User): Future[List[Boolean]] = {
      columnMetadataDAO.saveColumnMetadata(metadatas) flatMap
        { colms =>
          getAndUpdateUnivariate(file_meta, sampel_dataset_id, metadatas, user)
      }
    }

    def cloneDecisionTree(sample_dataset_id: String,
                          detaset_id: String):Future[Boolean] = {

      println("Decision Tree Called")
      cloneRandomForestTree(sample_dataset_id, detaset_id) flatMap { res =>
        val cursor: Cursor[JsObject] = decision_collection
          .find(Json.obj("dataset_id" -> sample_dataset_id)).cursor[JsObject]
        val futureDT: Future[List[JsObject]] = cursor.collect[List]()
        futureDT.map { p =>
          var dt_tree_obj = p(0) - ("_id")
          dt_tree_obj = withField(dt_tree_obj, "dataset_id", detaset_id)
          decision_collection.insert(dt_tree_obj).isCompleted
        }
      }
    }

    def cloneRandomForestTree(sample_dataset_id: String,
                          detaset_id: String):Future[Boolean] = {
      println("cloneRandomForestTree")

      cloneGBT(sample_dataset_id, detaset_id) flatMap { res =>
        val cursor: Cursor[JsObject] = rf_collection
          .find(Json.obj("dataset_id" -> sample_dataset_id)).cursor[JsObject]
        val futureRF: Future[List[JsObject]] = cursor.collect[List]()
        futureRF.map { p =>
          var dt_tree_obj = p(0) - ("_id")
          dt_tree_obj = withField(dt_tree_obj, "dataset_id", detaset_id)
          rf_collection.insert(dt_tree_obj).isCompleted
        }
      }
    }

    def cloneGBT(sample_dataset_id: String,
                          detaset_id: String):Future[Boolean] = {

      println("cloneGBT")
      val cursor: Cursor[JsObject] = gbt_collection
        .find(Json.obj("dataset_id" -> sample_dataset_id)).cursor[JsObject]
      val futureGBT: Future[List[JsObject]] = cursor.collect[List]()
      futureGBT.map { p =>
        var dt_tree_obj = p(0) - ("_id")
        dt_tree_obj = withField(dt_tree_obj, "dataset_id", detaset_id)
        gbt_collection.insert(dt_tree_obj).isCompleted
      }
    }

    def updateJobTable(sampel_dataset_id: String, datasetid: String, userid: String):Future[Option[Int]] = {
      jobDAO.getDatasetJob(sampel_dataset_id) flatMap { list =>
        val job_list: List[Job] = list.toList map (x =>x.copy(job_id = 0, dataset_id = datasetid, user_id = userid))
          jobDAO.bulkCreate(job_list)
      }
    }

    def cloneAnomalyMeta(get_sample_id: String,
                         dataset_id: String, user: User): Future[Option[Int]] = {
      updateJobTable(sampel_dataset_id, dataset_id, user.id.get) flatMap { jobs =>
        cloneDecisionTree(sampel_dataset_id, dataset_id) flatMap { res =>
          columnMetadataDAO.getColumnMetadata(get_sample_id) flatMap { metas =>
            val seq_columns = metas.toList.map(x => x.copy(id = Some(createUUID), dataset_id = Some(dataset_id)))
            columnMetadataDAO.saveColumnMetadata(seq_columns)
          }
        }
      }
    }

    def cloneAnomaly(sample_dataset_id: String,
                     dataset_id: String,  user: User): Future[Option[Int]] = {
      anomalyDAO.findById(sample_dataset_id, 1, 1, "") flatMap {a =>
        val x = a(0)
        val new_x = x.copy(id = Some(createUUID), dataset_id = dataset_id)
        // save anomaly file
        fileMetadataDAO.getFileMetadataById(sampel_dataset_id) flatMap { dataset =>

          filesystemService.copyFileOrFolder(x.path.get,x.path.get,
            dataset.get.user_id.get, user.id.get)

          anomalyDAO.create(new_x) flatMap { an  =>
            cloneAnomalyMeta(sampel_dataset_id, dataset_id, user)
          }
        }
      }
    }



    def withField[A](j: JsObject, key: String, value: A)(implicit w: Writes[A]) =
      j ++ Json.obj(key -> value)

  }