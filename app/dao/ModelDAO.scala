package dao

import javax.inject.Inject

import entities.{CheckDuplidateModelName, ModelMeta}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ModelDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {

  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class ModelMetaTable(tag: Tag)
    extends Table[ModelMeta](tag, "model") {

    def id = column[Option[String]]("id", O.PrimaryKey)

    def model_name = column[String]("model_name")

    def dataset_id = column[String]("dataset_id")

    def model_result_id = column[String]("model_result_id")

    def user_id = column[String]("user_id")

    def dataset_name = column[String]("dataset_name")

    def model_type = column[String]("model_type")

    def input_column = column[String]("input_column")

    def output_column = column[String]("output_column")

    def model_path = column[String]("model_path")

    def created_date = column[Option[Long]]("created_date")



    override def * =
      (id, model_name, dataset_id, model_result_id, user_id, dataset_name, model_type, input_column,
        output_column, model_path, created_date) <> ((ModelMeta.apply _).tupled, ModelMeta.unapply)
  }

  implicit val models = TableQuery[ModelMetaTable]

  def create(model: ModelMeta): Future[String] = {
    db.run(models += model) map { result =>
      model.id.get
    }
  }


  def findById(dataset_id: String, page: Int,
    limit: Int, q: String): Future[Seq[ModelMeta]] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)

    db.run(models
      .filter(_.dataset_id === dataset_id)
      .filter(x =>(x.model_name).toLowerCase startsWith q.toLowerCase)
      .sortBy(_.created_date.desc)
      .drop(offset).take(limit).result
    )
  }

  def findByUserID(user_id: String, page: Option[Int],
                   limit: Option[Int]): Future[Seq[ModelMeta]] = {

    if(limit == None)
      db.run(models
        .filter(_.user_id === user_id)
        .sortBy(_.created_date.desc).result
      )
    else{
      val offset = (limit.get * (page.get - 1))-(page.get - 1)+(page.get-1)
      db.run(models
        .filter(_.user_id === user_id)
        .sortBy(_.created_date.desc)
        .drop(offset).take(limit.get).result
      )
    }
  }


  def get(id: String):Future[Option[ModelMeta]] = {
    db.run(models.filter(_.id === id).result.headOption)
  }

  def delete(id: String): Future[Int] = {
    db.run(models.filter(_.id === id).delete)
  }


  def update(tree_id: String, model_path: String):Future[Int] = {
    db.run(models.filter(_.model_result_id === tree_id).map(x => (x.model_path))
      .update(model_path))
  }

  def deleteByDatasetId(dataset_id: String): Future[Int] = {
    db.run(models.filter(_.dataset_id === dataset_id).delete)
  }

  def getCount(user_id: String):Future[Int] = {
    db.run(models
      .filter(_.user_id === user_id)
      .result) map { q =>q.length}
  }

  def checkCheckDuplicate(user_id: String,
                     dataset_id: String, data:CheckDuplidateModelName): Future[Boolean] = {

    if(data.tree_id == None)
      db.run(models.filter(_.user_id === user_id).filter(_.dataset_id === dataset_id)
        .filter(_.model_name === data.name).exists.result)
    else
      db.run(models.filter(_.user_id === user_id).filter(_.dataset_id === dataset_id)
        .filter(_.model_name === data.name).filter(_.model_result_id =!= data.tree_id.get).exists.result)

  }
}
