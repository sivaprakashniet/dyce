package dao

import javax.inject.Inject

import entities.{ColumnMetadata, Job}
import play.api.db.slick.DatabaseConfigProvider
import slick.driver.JdbcProfile

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class JobDAO @Inject()(dbConfigProvider: DatabaseConfigProvider) {


  val dbConfig = dbConfigProvider.get[JdbcProfile]

  import dbConfig._
  import driver.api._

  class JobTable(tag: Tag)
    extends Table[Job](tag, "job") {
    def job_id = column[Long]("job_id", O.PrimaryKey, O.AutoInc)

    def user_id = column[String]("user_id")

    def dataset_id = column[String]("dataset_id")

    def dataset_name = column[String]("dataset_name")

    def job_name = column[String]("job_name")

    def start_time = column[Long]("start_time")

    def end_time = column[Long]("end_time")

    def status = column[String]("status")

    def created_date = column[Long]("created_date")

    def request_json = column[Option[String]]("request_json")

    def response_json = column[Option[String]]("response_json")


    override def * =
      (job_id, user_id, dataset_id, dataset_name,  job_name, start_time,
        end_time, status, created_date,
        request_json, response_json) <> ((Job.apply _).tupled, Job.unapply)
  }

  implicit val jobs = TableQuery[JobTable]

  def current_date_time: Long = System.currentTimeMillis()

  def bulkCreate(jobslist: List[Job]): Future[Option[Int]] = {
    db.run(jobs ++= jobslist)
  }

  def create(job: Job): Future[Job] = {
    db.run((jobs returning jobs
      .map(_.job_id) into ((job, id) => job.copy(job_id = id))) += job)
  }

  def getJobs(user_id: String, page: Int, limit: Int): Future[Seq[Job]] = {
    val offset = (limit * (page - 1))-(page - 1)+(page-1)
    db.run(jobs.filter(_.user_id === user_id)
      .sortBy(_.job_id.desc).drop(offset).take(limit).result)
  }

  def getJobCount(user_id: String): Future[Int] = {
    db.run(jobs.filter(_.user_id === user_id).result) map { q =>q.length}
  }


  def getJob(job_id: Long):Future[Option[Job]] = {
    db.run(jobs.filter(_.job_id === job_id).result.headOption)
  }

  def getDatasetJob(dataset_id: String):Future[Seq[Job]] = {
    db.run(jobs.filter(_.dataset_id === dataset_id)
      .sortBy(_.job_id.desc)
      .distinctOn(_.job_name)
      .result)
  }

  def updateJob(id: Long, status: String): Future[Long] = {
    db.run(jobs.filter(_.job_id === id).map(x => (x.status, x.end_time))
      .update(status, current_date_time)).map(_.toLong)
  }

  def delete(id: Long): Future[Int] = {
    db.run(jobs.filter(_.job_id === id).delete)
  }

}
