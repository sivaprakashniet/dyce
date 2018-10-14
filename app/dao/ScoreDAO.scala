package dao


import javax.inject.Inject

import entities.{ScoreSummary}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ScoreDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {


  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class ScoreTable(tag: Tag)
    extends Table[ScoreSummary](tag, "model_score") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def user_id = column[String]("user_id")

    def dataset_id = column[String]("dataset_id")

    def dataset_name = column[String]("dataset_name")

    def score_path = column[String]("score_path")

    def created_date = column[Long]("created_date")

    override def * =
      (id, name, user_id, dataset_id, dataset_name,
        score_path, created_date) <> ((ScoreSummary.apply _).tupled, ScoreSummary.unapply)
  }

  implicit val scores = TableQuery[ScoreTable]

  def current_date_time: Long = System.currentTimeMillis()


  def create(score: ScoreSummary): Future[ScoreSummary] = {
    db.run((scores returning scores
      .map(_.id) into ((job, id) => score.copy(id = id))) += score)
  }

  def getScores(dataset_id: String, page: Option[Int],
                limit: Option[Int], q:Option[String]): Future[Seq[ScoreSummary]] = {
    if(page == None)
      db.run(scores
        .filter(_.dataset_id === dataset_id)
        .filter(x =>(x.name).toLowerCase startsWith q.get.toLowerCase)
        .sortBy(_.id.desc).result)
    else{
      val offset = (limit.get * (page.get - 1))-(page.get - 1)+(page.get-1)
      db.run(scores.filter(_.dataset_id === dataset_id)
        .filter(x =>(x.name).toLowerCase startsWith q.get.toLowerCase)
        .sortBy(_.id.desc).drop(offset).take(limit.get).result)
    }

  }

  def getScoreCount(dataset_id: String, q:Option[String]): Future[Int] = {
    db.run(scores
      .filter(_.dataset_id === dataset_id)
      .filter(x =>(x.name).toLowerCase startsWith q.get.toLowerCase)
      .result) map { q =>q.length}
  }


  def getScore(id: Long):Future[Option[ScoreSummary]] = {
    db.run(scores.filter(_.id === id).result.headOption)
  }

  def delete(id: Long): Future[Int] = {
    db.run(scores.filter(_.id === id).delete)
  }

}
