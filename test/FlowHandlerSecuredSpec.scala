import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.amazonaws.services.s3.model.S3ObjectSummary
import filesystem.S3Config
import org.junit.runner.RunWith
import org.specs2.mutable._
import org.specs2.runner._
import play.api.Application
import play.api.libs.json.{JsNull, JsValue, Json}
import play.api.mvc.Result
import play.api.test.Helpers._
import play.api.test._
import TestImplicits._

import scala.concurrent.Future
import scala.io.Source
import scala.sys.process.Process

@RunWith(classOf[JUnitRunner])
class FlowHandlerSecuredSpec extends Specification with Inject {

  lazy val configuration = inject[play.api.Configuration]

  def getConfiguration(s: String) = configuration.underlying.getString(s)

  val user_id = getConfiguration("authconfig.userid")
  val username = getConfiguration("authconfig.username")
  val user_firstname = getConfiguration("authconfig.firstname")
  val user_lastname = getConfiguration("authconfig.lastname")
  val user_email = getConfiguration("authconfig.email")
  val test_bucket = configuration.underlying.getString("s3config.bucket_name")
  val test_token = configuration.underlying.getString("authconfig.test_token")
  lazy val s3config = inject[S3Config]
  val s3client = s3config.s3client
  val local_filepath = "test/res/sample_dataset_2.csv"
  val s3path = "/sample_dataset_2.csv"
  val dataset_name = "test_sample_2"
  val dataset_size = 100
  val waiting_time_in_minutes = 2
  val waiting_time = waiting_time_in_minutes * 60 * 1000

  def fixture = {
    var result: String = null
    val driver = getConfiguration("slick.dbs.default.db.driver")
    val url = getConfiguration("slick.dbs.default.db.url")
    val username = getConfiguration("slick.dbs.default.db.user")
    val password = getConfiguration("slick.dbs.default.db.password")
    val dbname = getConfiguration("dbname")

    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, username, password)
      val select_statement = connection.createStatement()
      val truncate_tables = connection.createStatement()
      val rs = select_statement.executeQuery("SELECT TABLE_NAME as result FROM " +
        "INFORMATION_SCHEMA.TABLES" +
        " where table_schema in ('" + dbname + "')")
      while (rs.next()) {
        var sql_query: String = "truncate table " + rs.getString("result")
        truncate_tables.execute(sql_query)
      }

      select_statement.executeUpdate("INSERT INTO  user" +
        " (id, username, first_name, last_name, email," +
        " created_date, modified_date, profile_image)" +
        " VALUES('" + user_id + "','" + username + "','" + user_firstname + "','" + user_lastname
        + "','" + user_email + "',23452345, 21341234,'');")

    } catch {
      case e: Exception => None
    }
    connection.close()
  }

  def getColumnNames(): List[String] = {
    val iterator = Source.fromFile(local_filepath).getLines
    if (iterator.hasNext) {
      val header = iterator.next()
      header.split(",").toList
    } else List()
  }

  val column_names = getColumnNames()

  def getPreview(): List[List[String]] = {
    val window_size = 100
    val iterator = Source.fromFile(local_filepath).getLines
    val preview = iterator sliding window_size
    val data_preview = preview.toList(0).map(line =>
      line.split(",").toList).toList.tail
    data_preview
  }

  var schema: JsValue = null
  var schema_list: AddColumnSchema = null
  var dataset_id: String = null

  "FlowHandlerSecured" should {
    fixture

    // Read the various parameters of a dataset, for example, the column names, the column types - Done
    // Infer schema from the dataset - [Need to do for 100 rows]
    // Upload the dataset - Done
    // Save file metadata and column metadata for the dataset - Done
    // Preprocess the dataset - Done
    // Calculate univariate summary from the dataset - Done
    // Calculate bivariate summary from the dataset - Done
    // Create a decision tree
    // Create gradient boosted trees from the dataset
    // Create random forests from the dataset
    // Find anomalies in the dataset

    def getResponse(app: Application, method: String, url: String, body: JsValue,
                    token: String = test_token): Future[Result] = {
      val fake_request_without_body = FakeRequest(method, url)
        .withHeaders("Authorization" -> token)

      if(body != null)
        route(app, fake_request_without_body.withJsonBody(body)).get
      else
        route(app, fake_request_without_body).get
    }

    "clear the test bucket" in new WithApplication {
      val command = Process("aws s3 rm s3://" + test_bucket + " --recursive")
      val execute_command = command.!
      (execute_command) mustEqual (0)
    }

    "check if there is no file in the test bucket" in new WithApplication {
      val objects_list = s3client.listObjects(test_bucket).getObjectSummaries.toArray.toList
      objects_list mustEqual (List())
    }

    "upload a file in the test bucket" in new WithApplication {
      val json_body = Json.parse("""{"file_path":"""" + s3path +""""}""")
      val url = "/" + user_id + "/datasets/upload/presignedurl"

      val response = getResponse(app, POST, url, json_body)
      status(response) must equalTo(OK)
      contentType(response) must beSome.which(_ == "application/json")

      val presignedurl_list = contentAsJson(response) \\ "presignedurl"
      presignedurl_list mustNotEqual (List())

      val command = Process("curl -v -X PUT -T " + local_filepath + " "
        + presignedurl_list(0).as[String])
      val execute_command = command.!
      (execute_command) mustEqual (0)
    }

    "check if there is a file in the test bucket with the same name" in new WithApplication {
      val objects_list = s3client.listObjects(test_bucket).getObjectSummaries.toArray.toList
      objects_list mustNotEqual (List())

      if (!(objects_list.isEmpty)) {
        val key = (objects_list(0).asInstanceOf[S3ObjectSummary].getKey.split("/")) (1)
        "/" + key mustEqual (s3path)
      }
    }

    "infer schema from the dataset" in new WithApplication {
      val fake_schema = column_names.zipWithIndex.map { case (n, i) =>
        new ColumnMetadata(n, None, i, "Category", "Category", "", 0, false, false, false, "", "")
      }
      val data_preview = getPreview()
      val infer_schema_request = Json.toJson(new InferSchemaRequest(data_preview, fake_schema))

      val url = "/" + user_id + "/datasets/inferschema"

      val response = getResponse(app, POST, url, infer_schema_request)
      status(response) must equalTo(OK)
      schema = (contentAsJson(response) \ "schema_list").as[JsValue]
    }

    "save file metadata in database" in new WithApplication {
      val url = "/" + user_id + "/datasets/metadata"
      val file_details = new FileMetadata(s3path, dataset_name, dataset_size)
      val response = getResponse(app, POST, url, Json.toJson(file_details))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      val dataset_id_opt = response_body \\ "dataset_id"
      dataset_id_opt mustNotEqual(List())
      if(!dataset_id_opt.isEmpty) {
        dataset_id = dataset_id_opt(0).as[String]
      }
    }

    "save column metadata in database" in new WithApplication {
      val url = "/" + user_id + "/datasets/" + dataset_id + "/column-metadata"
      val response = getResponse(app, POST, url, Json.obj("schema_list" -> schema))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      Thread.sleep(waiting_time)
    }

    "check that the jobs have completed" in new WithApplication {
      val url = "/" + user_id + "/jobs"
      val response = getResponse(app, GET, url, null)
      status(response) must equalTo(OK)
      println((contentAsJson(response) \\ "status"))
      println((contentAsJson(response) \\ "status").forall(x => List("PROCESSING",
        "FINISHED").contains(x.as[String])))
      (contentAsJson(response) \\ "status").forall(x => List("PROCESSING",
        "FINISHED").contains(x.as[String])) mustEqual(true)
    }

    "get column metadata from database" in new WithApplication {
      val url = "/" + user_id + "/datasets/" + dataset_id + "/column-metadata"
      val response = getResponse(app, GET, url, null)
      status(response) must equalTo(OK)
      schema_list = Json.fromJson[AddColumnSchema](contentAsJson(response)).getOrElse(null)
      schema_list mustNotEqual null
      Thread.sleep(waiting_time)
    }

    "testing logical functions" in new WithApplication {
      var updated_schema = schema_list.schema_list.map(c => c.copy(new_name = Some(c.name),
        raw_formula = Some(c.formula)))
      updated_schema :+= create_new_column(formula = "LIKE([Sex],'F')")
      updated_schema :+= create_new_column(formula = "IN([Sex],{'F','M'})")
      updated_schema :+= create_new_column(formula = "AND(1,1)")
      updated_schema :+= create_new_column(formula = "OR(0,1)")
      updated_schema :+= create_new_column(formula = "NOT(0)")
      updated_schema :+= create_new_column(formula = "IFNULL([Diameter],0)")
      updated_schema :+= create_new_column(formula = "ISNULL([Length])")
      updated_schema :+= create_new_column(formula = "IF [Length] > 2 THEN 1 ELSE -1 END")
      updated_schema :+= create_new_column(formula = "CASE [Length] WHEN 0 THEN -1 END")

      val url = "/" + user_id + "/datasets/" + dataset_id + "/view"
      val response = getResponse(app, PUT, url, Json.toJson(new AddColumnSchema(dataset_id = dataset_id,
        schema_list = updated_schema)))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      Thread.sleep(waiting_time)
    }

    "testing string functions" in new WithApplication {
      var updated_schema = schema_list.schema_list.map(c => c.copy(new_name = Some(c.name),
        raw_formula = Some(c.formula)))
      updated_schema :+= create_new_column(formula = "TRIM([Sex])")
      updated_schema :+= create_new_column(formula = "CONCAT([Sex],'-',[Length])")
      updated_schema :+= create_new_column(formula = "FIND(Diameter,'4')")
      updated_schema :+= create_new_column(formula = "REPLACE([Diameter],'0','1')")
      updated_schema :+= create_new_column(formula = "LEFT([Diameter],1)")
      updated_schema :+= create_new_column(formula = "RIGHT([Diameter],2)")
      updated_schema :+= create_new_column(formula = "LENGTH([Length])")
      updated_schema :+= create_new_column(formula = "LOWER([Sex])")
      updated_schema :+= create_new_column(formula = "UPPER([Sex])")
      updated_schema :+= create_new_column(formula = "SUBSTR([Sex],1,2)")

      val url = "/" + user_id + "/datasets/" + dataset_id + "/view"
      val response = getResponse(app, PUT, url, Json.toJson(new AddColumnSchema(dataset_id = dataset_id,
        schema_list = updated_schema)))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      Thread.sleep(waiting_time)
    }

    "testing math functions" in new WithApplication {
      var updated_schema = schema_list.schema_list.map(c => c.copy(new_name = Some(c.name),
        raw_formula = Some(c.formula)))
      updated_schema :+= create_new_column(formula = "SUM([Length],[Diameter])")
      updated_schema :+= create_new_column(formula = "MIN([Length],[Diameter])")
      updated_schema :+= create_new_column(formula = "MAX([Length],[Diameter])")
      updated_schema :+= create_new_column(formula = "CEIL([Length])")
      updated_schema :+= create_new_column(formula = "FLOOR([Diameter])")
      updated_schema :+= create_new_column(formula = "ABS([Diameter])")
      updated_schema :+= create_new_column(formula = "LOG([Diameter])")
      updated_schema :+= create_new_column(formula = "LN([Diameter])")
      updated_schema :+= create_new_column(formula = "MOD([Length],1)")
      updated_schema :+= create_new_column(formula = "POW([Length],2)")
      updated_schema :+= create_new_column(formula = "ROUND([Length])")
      updated_schema :+= create_new_column(formula = "SQRT([Length])")
      updated_schema :+= create_new_column(formula = "NORMALIZE([Length],1,2)")
      updated_schema :+= create_new_column(formula = "ISOUTLIER([Length])")
      updated_schema :+= create_new_column(formula = "REPLACEOUTLIER([Length],1)")

      val url = "/" + user_id + "/datasets/" + dataset_id + "/view"
      val response = getResponse(app, PUT, url, Json.toJson(new AddColumnSchema(dataset_id = dataset_id,
        schema_list = updated_schema)))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      Thread.sleep(waiting_time)
    }

    "testing operators" in new WithApplication {
      var updated_schema = schema_list.schema_list.map(c => c.copy(new_name = Some(c.name),
        raw_formula = Some(c.formula)))
      updated_schema :+= create_new_column(formula = "[Length] + [Diameter]")
      updated_schema :+= create_new_column(formula = "[Length] - [Diameter]")
      updated_schema :+= create_new_column(formula = "[Length] * [Diameter] * [Height]")
      updated_schema :+= create_new_column(formula = "[Length] / [Diameter]")
      updated_schema :+= create_new_column(formula = "[Length] ^ 2")
      updated_schema :+= create_new_column(formula = "[Length] % [Diameter]")
      updated_schema :+= create_new_column(formula = "[Diameter] < [Length]")
      updated_schema :+= create_new_column(formula = "[Diameter] > [Length]")
      updated_schema :+= create_new_column(formula = "[Diameter] <= [Length]")
      updated_schema :+= create_new_column(formula = "[Diameter] >= [Length]")
      updated_schema :+= create_new_column(formula = "[Diameter] == [Length]")
      updated_schema :+= create_new_column(formula = "[Diameter] <> [Length]")

      val url = "/" + user_id + "/datasets/" + dataset_id + "/view"
      val response = getResponse(app, PUT, url, Json.toJson(new AddColumnSchema(dataset_id = dataset_id,
        schema_list = updated_schema)))
      status(response) must equalTo(OK)
      val response_body = contentAsJson(response)
      Thread.sleep(waiting_time)
    }

  }

  def create_new_column(datatype: String = "AutoDetect", formula: String,
                        decimal: Long = 0) = {
    val name = "New_column_" + UUID.randomUUID().toString

    new AddColumnMetadata(dataset_id = "", id = "", name = name, new_name = Some(name),
      position = 0, datatype = datatype, format = "", raw_formula = Some(""),
      display_format = "", decimal = decimal, separator = false,
      visibility = false, calculated = true, created_date = "",
      modified_date = "", metrics = "",formula = formula, description = Some(""),
      status = "new")
  }
}