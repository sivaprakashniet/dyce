package dao

import javax.inject.Inject

import entities.AnomalySummary
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AnomalyDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {



  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class AnomalyTable(tag: Tag)
    extends Table[AnomalySummary](tag, "anomaly") {
    def id = column[Option[String]]("id", O.PrimaryKey)

    def name = column[String]("name")

    def dataset_id = column[String]("dataset_id")

    def dataset_name = column[String]("dataset_name")

    def trees = column[Int]("trees")

    def sample_proportions = column[Int]("sample_proportions")

    def no_of_anomalies = column[Int]("no_of_anomalies")

    def features = column[Int]("features")

    def bootstrap = column[Boolean]("bootstrap")

    def replacement = column[Boolean]("replacement")

    def random_split = column[Int]("random_split")

    def path = column[Option[String]]("path")

    def created_date = column[Option[Long]]("created_date")

    def feature_column_ids = column[String]("feature_column_ids")

    override def * =
      (id, name, dataset_id, dataset_name, trees,
        sample_proportions, no_of_anomalies, features,
        bootstrap, replacement, random_split, path,
        created_date, feature_column_ids) <> ((AnomalySummary.apply _).tupled, AnomalySummary.unapply)
  }

  implicit val anomalies = TableQuery[AnomalyTable]

  def create(job: AnomalySummary): Future[AnomalySummary] = {
    db.run(anomalies += job) map { result =>
      println(result)
      job
    }
  }


  def findById(dataset_id: String, page: Int,
               limit: Int, q: String): Future[Seq[AnomalySummary]] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)

    db.run(anomalies
      .filter(_.dataset_id === dataset_id)
      .filter(x =>(x.name).toLowerCase startsWith q.toLowerCase)
      .sortBy(_.created_date.desc)
      .drop(offset).take(limit).result
    )
  }

  def getAnomaly(id: String):Future[Option[AnomalySummary]] = {
    db.run(anomalies.filter(_.id === id).result.headOption)
  }

  def delete(id: String): Future[Int] = {
    db.run(anomalies.filter(_.id === id).delete)
  }

  def getAnomalyCount(dataset_id: String, q: String):Future[Int] = {
    db.run(anomalies
      .filter(_.dataset_id === dataset_id)
      .filter(x =>(x.name).toLowerCase startsWith q.toLowerCase)
      .result) map { q =>q.length}
  }
}
