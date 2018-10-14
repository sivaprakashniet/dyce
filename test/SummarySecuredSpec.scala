import java.sql.{Connection, DriverManager}

import org.junit.runner._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test._

import scala.sys.process.Process

@RunWith(classOf[JUnitRunner])
class SummarySecuredSpec extends Specification with Inject {
  lazy val configuration = inject[play.api.Configuration]
  val waiting_time_in_minutes = 5
  val waiting_time = waiting_time_in_minutes * 60 * 1000

  val test_bucket = configuration.underlying.getString("s3config.bucket_name")
  val user_bucket = configuration.underlying.getString("s3config.bucket_name_user")

  def fixture = {
    var result: String = null
    val driver = configuration.underlying.getString("slick.dbs.default.db.driver")
    val url = configuration.underlying.getString("slick.dbs.default.db.url")
    val username = configuration.underlying.getString("slick.dbs.default.db.user")
    val password = configuration.underlying.getString("slick.dbs.default.db.password")
    val dbname = configuration.underlying.getString("dbname")
    var connection: Connection = null
    val user_id = "1"
    val dataset_id = "fccd6c44-c319-4fa1-8aa6-7c4c6ec817ab"
    val model_id = "be0fab7a-351f-4994-bace-02dacacf07e2"

    try {
      connection = DriverManager.getConnection(url, username, password)
      val select_statement = connection.createStatement()
      val create_perm_statement = connection.createStatement()
      val create_model_meta_statement = connection.createStatement()
      val create_dataset_statement = connection.createStatement()

      val trucate_tables = connection.createStatement()
      val rs = select_statement.executeQuery("SELECT TABLE_NAME as result FROM INFORMATION_SCHEMA.TABLES" +
        " where table_schema in ('"+dbname+"')")
      while (rs.next()) {
        var sql_query: String = "truncate table " + rs.getString("result")
        trucate_tables.execute(sql_query)
      }
      select_statement.executeUpdate("INSERT INTO  user (id, username, first_name, last_name, email, created_date, modified_date, profile_image)" +
        " VALUES('1','idealabs','idealabs','latentview','"+configuration.underlying.getString("authconfig.email")+"',2345234523,234523452,'');")
      create_perm_statement.executeUpdate("INSERT INTO acl (id, user_id, object_id, resource_type, privilege, permission) VALUES ('1', '1', '1', 'DatasetAPI', '*', 'allow'),('2', '1', '2', 'DatasetAPI', 'read', 'allow');")

      create_dataset_statement.executeUpdate("insert into file_metadata(id, file_path, parquet_file_path, download_file_path, dataset_name, created_date, updated_date, user_id, number_of_rows, number_of_columns, file_size, dataset_status, status, new_name, decision_tree_count, anomaly_count, gbt_count, random_forest_count, model_count, download_status) VALUES('"+dataset_id+"', '/datasets/sample_dataset_1.csv', '/parquet_datasets/sample_dataset_less_columns_6261df4f-d766-40f1-9bad-3a0b991f2c83.parquet', '/datasets/sample_dataset_1.csv', 'sample_dataset_1', '1501045037006', '1501045037006', ' "+user_id+"', 0, 0, 3652, 'PROCESSED', NULL, NULL, 0, 0, 0, 0, 0, 'PROCESSED');")

      create_model_meta_statement.executeUpdate("insert into model(id, model_name, dataset_id, user_id, dataset_name, model_type, input_column, output_column, model_path, created_date) values('"+model_id+"', 'dt_sample_tree', '"+dataset_id+"', '"+user_id+"', 'sample_dataset', 'DecisionTree Classification Model', 'segment,MOB', 'SegmentType', 'testcases/be0fab7a-351f-4994-bace-02dacacf07e2', '1504163646402');")

      ///aws s3 cp MyFile.txt s3://my-bucket/path/

      val clear = Process("aws s3 rm s3://" + test_bucket + " --recursive")
      val clear_execute_command = clear.!
      (clear_execute_command) mustEqual (0)

      val command = Process("aws s3 cp s3://"+user_bucket+"/a87ff679a2f3e71d9181a67b7542122c/parquet_datasets/sample_dataset_less_columns_6261df4f-d766-40f1-9bad-3a0b991f2c83.parquet/ s3://" + test_bucket + "/c4ca4238a0b923820dcc509a6f75849b/parquet_datasets/sample_dataset_less_columns_6261df4f-d766-40f1-9bad-3a0b991f2c83.parquet/ --recursive" )

      val execute_command = command.!
      (execute_command) mustEqual(0)



      val command1 = Process("aws s3 cp s3://"+user_bucket+"/testcases/ s3://" + test_bucket + "/testcases/ --recursive")
      val execute_command1 = command1.!
      (execute_command1) mustEqual(0)

    } catch {
      case e: Exception => None
    }
    connection.close()
  }


  "Dataset models and Summary controller" should {
    val f = fixture
    var token = ""
    var cookie: play.api.mvc.Cookie = null
    val sample_token_user = configuration.underlying.getString("authconfig.test_token")
    val user_id = "1"
    val dataset_id = "fccd6c44-c319-4fa1-8aa6-7c4c6ec817ab"
    val model_id = "be0fab7a-351f-4994-bace-02dacacf07e2"

    // Model management
    "Returns a 400 on a request with an correct token and invalid request json" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/"+user_id+"/datasets/"+dataset_id+"/models"))
        .withJsonBody(Json.parse(""" { "model_id": "be0fab7a-351f-4994-bace-02dacacf07e2"} """))
        .withHeaders("Authorization"->sample_token_user)).get
      status(response) must equalTo(400)
    }


    "Returns a 401 on a request with an incorrect token" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/"+user_id+"/datasets/"+dataset_id+"/models"))
        .withJsonBody(Json.parse(""" { "model_id": "be0fab7a-351f-4994-bace-02dacacf07e2", "name": "mc_score_1"} """))
        .withHeaders("Authorization"->"Invalid")).get
      status(response) must equalTo(401)
    }

    "Returns a 200 on a request with valid token and json" in new WithApplication {
      val response = route(app, FakeRequest(POST, ("/"+user_id+"/datasets/"+dataset_id+"/models"))
        .withJsonBody(Json.parse(""" { "model_id": "be0fab7a-351f-4994-bace-02dacacf07e2", "name": "mc_score_1"} """))
        .withHeaders("Authorization"->sample_token_user)).get
      status(response) must equalTo(200)
    }

  }
}